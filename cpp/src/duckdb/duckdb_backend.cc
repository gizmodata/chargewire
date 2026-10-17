#include "duckdb_backend.hpp"

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/scalar.h>
#include <arrow/table.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <array>
#include <filesystem>
#include <vector>

namespace warpduckdb {

namespace json = boost::json;
using warpsql::Error;
using warpsql::ErrorCode;

namespace {

std::string QuoteIdentifier(std::string_view name) {
  std::string out = "\"";
  for (char c : name) {
    out += c;
    if (c == '"') {
      out += '"';
    }
  }
  return out + "\"";
}

// The rows of a DuckDB result, kept together with the statement they came from.
class StatementReader : public arrow::RecordBatchReader {
 public:
  StatementReader(SqlStatement statement, std::shared_ptr<arrow::RecordBatchReader> rows)
      : statement_(std::move(statement)), rows_(std::move(rows)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return rows_->schema(); }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    return rows_->ReadNext(batch);
  }
  arrow::Status Close() override { return rows_->Close(); }

 private:
  SqlStatement statement_;
  std::shared_ptr<arrow::RecordBatchReader> rows_;
};

std::vector<SqlStatement> Parse(duckdb_v2_connection_handle connection, std::string_view sql) {
  const std::string text(sql);
  StatementIterator iterator;
  Call([&](auto* err) { return duckdb_v2_parse_sql(connection, text.c_str(), iterator.out(), err); });
  std::vector<SqlStatement> statements;
  for (;;) {
    SqlStatement statement;
    Call([&](auto* err) {
      return duckdb_v2_statement_iterator_next(iterator.get(), statement.out(), err);
    });
    if (!statement) {
      return statements;
    }
    statements.push_back(std::move(statement));
  }
}

// Runs one parsed statement to completion, discarding rows; returns rows changed.
idx_t RunToEnd(duckdb_v2_connection_handle connection, duckdb_v2_sql_statement_handle statement) {
  Result result;
  Call([&](auto* err) {
    return duckdb_v2_statement_execute(connection, statement, nullptr, nullptr, 0, result.out(),
                                       err);
  });
  idx_t changed = 0;
  Call([&](auto* err) { return duckdb_v2_result_drain(result.get(), &changed, err); });
  return changed;
}

void RunSql(duckdb_v2_connection_handle connection, std::string_view sql) {
  for (auto& statement : Parse(connection, sql)) {
    RunToEnd(connection, statement.get());
  }
}

// The first column of the first row of a query, as text.
std::string QueryText(duckdb_v2_connection_handle connection, std::string_view sql) {
  auto statements = Parse(connection, sql);
  ArrowResult result;
  Call([&](auto* err) {
    return duckdb_v2_statement_execute_arrow(connection, statements.at(0).get(), nullptr, nullptr,
                                             0, 0, result.out(), err);
  });
  ArrowArrayStream stream{};
  duckdb_v2_arrow_result_handle handle = result.get();
  Call([&](auto* err) { return duckdb_v2_arrow_result_to_arrow_c_stream(&handle, &stream, err); });
  result.release();
  auto reader = arrow::ImportRecordBatchReader(&stream);
  if (!reader.ok()) {
    throw Error(ErrorCode::kExecutionError, reader.status().message());
  }
  auto table = (*reader)->ToTable();
  if (!table.ok() || (*table)->num_rows() == 0) {
    return {};
  }
  auto scalar = (*table)->column(0)->GetScalar(0);
  return scalar.ok() && (*scalar)->is_valid ? (*scalar)->ToString() : std::string();
}

class DuckDBSession : public warpsql::EngineSession {
 public:
  explicit DuckDBSession(duckdb_v2_instance_handle instance) {
    Call([&](auto* err) { return duckdb_v2_connection_create(instance, connection_.out(), err); });
    Call([&](auto* err) {
      return duckdb_v2_connection_get_factory(connection_.get(), &factory_, err);
    });
    // Arrow extension types keep UUID, JSON, TIMETZ and HUGEINT exact; Arrow JS and
    // every Arrow library read them (see spikes/duckdb-arrow-ipc).
    RunSql(connection_.get(), "SET arrow_lossless_conversion = true");
  }

  warpsql::StatementOutcome Execute(std::string_view sql, const json::value& parameters) override {
    auto statements = Parse(connection_.get(), sql);
    if (statements.empty()) {
      throw Error(ErrorCode::kSqlError, "no SQL statement given");
    }
    if (!parameters.is_null() && statements.size() > 1) {
      throw Error(ErrorCode::kSqlError, "bind parameters need a single SQL statement");
    }
    // Every statement but the last runs to completion; the last one's result is returned.
    for (std::size_t i = 0; i + 1 < statements.size(); i++) {
      RunToEnd(connection_.get(), statements[i].get());
    }
    SqlStatement last = std::move(statements.back());

    std::vector<Value> values;
    std::vector<duckdb_v2_value_handle> value_handles;
    std::vector<std::string> names;
    std::vector<duckdb_v2_identifier_t> name_handles;
    BindParameters(last.get(), parameters, values, names);
    for (auto& value : values) {
      value_handles.push_back(value.get());
    }
    for (auto& name : names) {
      name_handles.push_back(Str(name));
    }

    ArrowResult result;
    Call([&](auto* err) {
      return duckdb_v2_statement_execute_arrow(
          connection_.get(), last.get(), name_handles.empty() ? nullptr : name_handles.data(),
          value_handles.empty() ? nullptr : value_handles.data(), value_handles.size(), 0,
          result.out(), err);
    });
    DUCKDB_V2_RESULT_TYPE type{};
    Call([&](auto* err) { return duckdb_v2_arrow_result_get_result_type(result.get(), &type, err); });

    warpsql::StatementOutcome outcome;
    if (type == DUCKDB_V2_RESULT_TYPE_QUERY_RESULT) {
      ArrowArrayStream stream{};
      duckdb_v2_arrow_result_handle handle = result.get();
      Call([&](auto* err) {
        return duckdb_v2_arrow_result_to_arrow_c_stream(&handle, &stream, err);
      });
      result.release();  // the stream owns it now
      auto reader = arrow::ImportRecordBatchReader(&stream);
      if (!reader.ok()) {
        throw Error(ErrorCode::kExecutionError, reader.status().message());
      }
      outcome.rows = std::make_shared<StatementReader>(std::move(last), std::move(*reader));
    } else {
      idx_t changed = 0;
      Call([&](auto* err) { return duckdb_v2_arrow_result_drain(result.get(), &changed, err); });
      if (type == DUCKDB_V2_RESULT_TYPE_CHANGED_ROWS) {
        outcome.rows_affected = static_cast<int64_t>(changed);
      }
    }
    return outcome;
  }

  void Interrupt() override { duckdb_v2_connection_interrupt(connection_.get(), nullptr); }

  std::optional<double> Progress() override {
    double percentage = -1;
    uint64_t processed = 0;
    uint64_t total = 0;
    if (duckdb_v2_connection_progress_get(connection_.get(), &percentage, &processed, &total,
                                          nullptr) != DUCKDB_V2_ERROR_NONE ||
        percentage < 0) {
      return std::nullopt;
    }
    return percentage / 100.0;
  }

  warpsql::OptionErrors SetOptions(const json::object& options) override {
    warpsql::OptionErrors errors;
    auto apply = [&](std::string_view key, auto&& fn) {
      const json::value* value = options.if_contains(key);
      if (value == nullptr) {
        return;
      }
      try {
        fn(*value);
      } catch (const Error& error) {
        errors.emplace(std::string(key), Error(ErrorCode::kInvalidOptionValue, error.what()));
      }
    };
    auto text = [](const json::value& value, std::string_view key) {
      if (!value.is_string()) {
        throw Error(ErrorCode::kInvalidOptionValue, std::string(key) + " must be a string");
      }
      return std::string(value.as_string());
    };
    // Catalog before schema, so {"catalog": a, "schema": b} means a.b.
    apply("catalog", [&](const json::value& value) {
      RunSql(connection_.get(), "USE " + QuoteIdentifier(text(value, "catalog")));
    });
    apply("schema", [&](const json::value& value) {
      const std::string catalog = QueryText(connection_.get(), "SELECT current_database()");
      RunSql(connection_.get(), "USE " + QuoteIdentifier(catalog) + "." +
                                    QuoteIdentifier(text(value, "schema")));
    });
    apply("autocommit", [&](const json::value& value) {
      if (!value.is_bool()) {
        throw Error(ErrorCode::kInvalidOptionValue, "autocommit must be true or false");
      }
      const bool on = value.as_bool();
      if (!on && autocommit_) {
        RunSql(connection_.get(), "BEGIN TRANSACTION");
      } else if (on && !autocommit_) {
        RunSql(connection_.get(), "COMMIT");
      }
      autocommit_ = on;
    });
    apply("session_tag", [&](const json::value& value) { session_tag_ = value; });
    for (const auto& option : options) {
      const std::string_view key = option.key();
      if (key != "catalog" && key != "schema" && key != "autocommit" && key != "session_tag") {
        errors.emplace(std::string(key),
                       Error(ErrorCode::kUnknownOption, "unknown session option"));
      }
    }
    return errors;
  }

  json::object Options() override {
    json::object options{
        {"catalog", QueryText(connection_.get(), "SELECT current_database()")},
        {"schema", QueryText(connection_.get(), "SELECT current_schema()")},
        {"autocommit", autocommit_},
    };
    if (!session_tag_.is_null()) {
      options["session_tag"] = session_tag_;
    }
    return options;
  }

  void Commit() override { EndTransaction("COMMIT"); }
  void Rollback() override { EndTransaction("ROLLBACK"); }

 private:
  void EndTransaction(std::string_view sql) {
    RunSql(connection_.get(), sql);
    if (!autocommit_) {
      RunSql(connection_.get(), "BEGIN TRANSACTION");  // the next statement starts a new one
    }
  }

  // JSON bind parameters (spec §6.1) as DuckDB values: an array binds positionally,
  // an object by name.
  void BindParameters(duckdb_v2_sql_statement_handle statement, const json::value& parameters,
                      std::vector<Value>& values, std::vector<std::string>& names) {
    // JSON null has no type, and DuckDB's untyped NULL exists only inside its binder, so
    // a NULL takes the type DuckDB infers for that parameter.
    ParameterTypes types;
    auto null_type = [&](std::size_t index, std::string_view name) {
      if (!types.loaded) {
        types = LoadParameterTypes(statement);
      }
      return types.For(index, name);
    };
    if (parameters.is_array()) {
      std::size_t index = 0;
      for (const auto& item : parameters.as_array()) {
        values.push_back(item.is_null() ? MakeNull(null_type(index, {})) : MakeValue(item));
        index++;
      }
    } else if (parameters.is_object()) {
      for (const auto& item : parameters.as_object()) {
        names.emplace_back(item.key());
        values.push_back(item.value().is_null() ? MakeNull(null_type(0, item.key()))
                                                : MakeValue(item.value()));
      }
    }
  }

  struct ParameterTypes {
    bool loaded = false;
    std::vector<std::string> names;
    std::vector<LogicalType> types;

    // The type of a parameter by position (or by name, when given); VARCHAR when DuckDB
    // can't tell.
    duckdb_v2_logical_type_handle For(std::size_t index, std::string_view name) const {
      for (std::size_t i = 0; i < types.size(); i++) {
        const bool match = name.empty() ? i == index : names[i] == name;
        if (match && types[i]) {
          return types[i].get();
        }
      }
      return nullptr;
    }
  };

  ParameterTypes LoadParameterTypes(duckdb_v2_sql_statement_handle statement) {
    using Schema = Owned<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
    ParameterTypes out;
    out.loaded = true;
    Schema result;
    Schema parameters;
    duckdb_v2_error_info_handle err = nullptr;
    if (duckdb_v2_statement_bind(connection_.get(), statement, result.out(), parameters.out(),
                                 &err) != DUCKDB_V2_ERROR_NONE) {
      if (err != nullptr) duckdb_v2_error_info_destroy(&err);
      return out;  // execution reports the real error
    }
    idx_t count = 0;
    Call([&](auto* e) { return duckdb_v2_schema_get_count(parameters.get(), &count, e); });
    for (idx_t i = 0; i < count; i++) {
      duckdb_v2_identifier_t name{};
      LogicalType type;
      Call([&](auto* e) {
        return duckdb_v2_schema_get_field(parameters.get(), i, &name, type.out(), e);
      });
      DUCKDB_V2_LOGICAL_TYPE_ID id = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID;
      duckdb_v2_logical_type_get_id(type.get(), &id, nullptr);
      if (id == DUCKDB_V2_LOGICAL_TYPE_ID_INVALID || id == DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL ||
          id == DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN || id == DUCKDB_V2_LOGICAL_TYPE_ID_ANY) {
        type.reset();
      }
      out.names.push_back(ToString(name));
      out.types.push_back(std::move(type));
    }
    return out;
  }

  Value MakeNull(duckdb_v2_logical_type_handle known) {
    LogicalType varchar;
    if (known == nullptr) {
      Call([&](auto* err) {
        return duckdb_v2_factory_create_type_from_id(factory_, DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
                                                     nullptr, nullptr, 0, varchar.out(), err);
      });
      known = varchar.get();
    }
    Value value;
    Call([&](auto* err) { return duckdb_v2_value_create_null(factory_, known, value.out(), err); });
    return value;
  }

  Value MakeValue(const json::value& item) {
    Value value;
    switch (item.kind()) {
      case json::kind::null:
        return MakeNull(nullptr);
      case json::kind::bool_:
        Call([&](auto* err) {
          return duckdb_v2_value_create_bool(factory_, item.as_bool(), value.out(), err);
        });
        break;
      case json::kind::int64:
        Call([&](auto* err) {
          return duckdb_v2_value_create_bigint(factory_, item.as_int64(), value.out(), err);
        });
        break;
      case json::kind::uint64:
        Call([&](auto* err) {
          return duckdb_v2_value_create_ubigint(factory_, item.as_uint64(), value.out(), err);
        });
        break;
      case json::kind::double_:
        Call([&](auto* err) {
          return duckdb_v2_value_create_double(factory_, item.as_double(), value.out(), err);
        });
        break;
      case json::kind::string: {
        const duckdb_v2_str text = Str(item.as_string());
        Call([&](auto* err) {
          return duckdb_v2_value_create_varchar(factory_, &text, value.out(), err);
        });
        break;
      }
      default:
        throw Error(ErrorCode::kBadRequest,
                    "bind parameters must be JSON scalars; use a prepared statement with Arrow "
                    "parameters for nested values");
    }
    return value;
  }

  Connection connection_;
  duckdb_v2_factory_handle factory_ = nullptr;
  bool autocommit_ = true;
  json::value session_tag_ = nullptr;
};

}  // namespace

DuckDBBackend::DuckDBBackend(Options options) {
  Call([&](auto* err) { return duckdb_v2_environment_create(environment_.out(), err); });
  if (options.read_only) {
    const duckdb_v2_str names[] = {Str("access_mode")};
    const duckdb_v2_str values[] = {Str("READ_ONLY")};
    Call([&](auto* err) {
      return duckdb_v2_instance_create_with_options(environment_.get(), names, values, 1,
                                                    instance_.out(), err);
    });
  } else {
    Call([&](auto* err) {
      return duckdb_v2_instance_create(environment_.get(), instance_.out(), err);
    });
  }
  // The catalog is named as DuckDB names it: the file's stem, or "memory".
  const std::string catalog = options.database == ":memory:"
                                  ? std::string("memory")
                                  : std::filesystem::path(options.database).stem().string();
  const duckdb_v2_identifier_t name = Str(catalog);
  const duckdb_v2_str path = Str(options.database);
  Call([&](auto* err) {
    return duckdb_v2_instance_attach(instance_.get(), &name, &path, nullptr, true, err);
  });

  duckdb_v2_str version{};
  Call([&](auto* err) { return duckdb_v2_library_version(&version, err); });
  version_ = ToString(version);

  Call([&](auto* err) {
    return duckdb_v2_connection_create(instance_.get(), health_connection_.out(), err);
  });
}

DuckDBBackend::~DuckDBBackend() {
  health_connection_.reset();
  instance_.reset();
  environment_.reset();
}

warpsql::ServerIdentity DuckDBBackend::Identity() {
  return {"warp-duckdb", "0.1.0 (DuckDB " + version_ + ")"};
}

std::unique_ptr<warpsql::EngineSession> DuckDBBackend::OpenSession(
    const warpsql::Principal&, const json::object& options) {
  auto session = std::make_unique<DuckDBSession>(instance_.get());
  if (!options.empty()) {
    auto errors = session->SetOptions(options);
    if (!errors.empty()) {
      throw Error(ErrorCode::kInvalidOptionValue,
                  errors.begin()->first + ": " + errors.begin()->second.what());
    }
  }
  return session;
}

void DuckDBBackend::CheckHealth() {
  std::lock_guard lock(health_mutex_);
  QueryText(health_connection_.get(), "SELECT 1");
}

warpsql::Principal StaticPasswordAuthenticator::AuthenticatePassword(std::string_view username,
                                                                     std::string_view password) {
  // Compare digests so neither the time taken nor the comparison length depends on
  // where the inputs differ.
  auto digest = [](std::string_view text) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> out{};
    unsigned int length = 0;
    EVP_Digest(text.data(), text.size(), out.data(), &length, EVP_sha256(), nullptr);
    return out;
  };
  const auto given_user = digest(username);
  const auto want_user = digest(username_);
  const auto given_password = digest(password);
  const auto want_password = digest(password_);
  const bool user_ok = CRYPTO_memcmp(given_user.data(), want_user.data(), 32) == 0;
  const bool password_ok = CRYPTO_memcmp(given_password.data(), want_password.data(), 32) == 0;
  if (!user_ok || !password_ok) {
    throw Error(ErrorCode::kInvalidCredentials, "the user name or password is not correct");
  }
  return warpsql::Principal{std::string(username)};
}

}  // namespace warpduckdb
