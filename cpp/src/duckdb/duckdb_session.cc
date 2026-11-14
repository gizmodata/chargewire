#include "duckdb_session.hpp"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/writer.h>

#include <algorithm>
#include <cctype>
#include <map>

#include "duckdb_describe.hpp"
#include "duckdb_sql.hpp"

namespace chargewire_duckdb {

namespace json = boost::json;
using chargewire::Error;
using chargewire::ErrorCode;
using chargewire::MetadataKind;

namespace {

using PreparedHandle =
    Owned<duckdb_v2_prepared_statement_handle, duckdb_v2_prepared_statement_destroy>;

// What executing a prepared statement returns, learned before it ever runs.
struct Described {
  std::optional<bool> returns_rows;
  std::shared_ptr<arrow::Schema> schema;  // when it returns rows
};

class DuckDBPrepared : public chargewire::PreparedStatement {
 public:
  DuckDBPrepared(SqlStatement statement, PreparedHandle prepared, ParameterTypes types,
                 Described described, std::shared_ptr<arrow::Schema> parameter_schema)
      : statement_(std::move(statement)),
        prepared_(std::move(prepared)),
        types_(std::move(types)),
        described_(std::move(described)),
        parameter_schema_(std::move(parameter_schema)) {}

  std::shared_ptr<arrow::Schema> ParameterSchema() override { return parameter_schema_; }
  std::shared_ptr<arrow::Schema> ResultSchema() override { return described_.schema; }
  std::optional<bool> ReturnsRows() override { return described_.returns_rows; }

  duckdb_v2_sql_statement_handle statement() const { return statement_.get(); }
  duckdb_v2_prepared_statement_handle prepared() const { return prepared_.get(); }
  const ParameterTypes& types() const { return types_; }

 private:
  SqlStatement statement_;
  PreparedHandle prepared_;
  ParameterTypes types_;
  Described described_;
  std::shared_ptr<arrow::Schema> parameter_schema_;
};

DUCKDB_V2_STATEMENT_TYPE StatementType(duckdb_v2_sql_statement_handle statement) {
  DUCKDB_V2_STATEMENT_TYPE type = DUCKDB_V2_STATEMENT_TYPE_INVALID;
  Call([&](auto* err) { return duckdb_v2_sql_statement_get_type(statement, &type, err); });
  return type;
}

using TokenIterator = Owned<duckdb_v2_token_iterator_handle, duckdb_v2_token_iterator_destroy>;

// Whether the statement's own text has a RETURNING clause outside any parentheses (a
// RETURNING inside a CTE or a subquery belongs to that inner statement). Tokens, not raw
// text: a string, a quoted name or a comment that says RETURNING is no clause.
bool HasTopLevelReturning(duckdb_v2_connection_handle connection,
                          duckdb_v2_sql_statement_handle statement) {
  duckdb_v2_str text{};
  Call([&](auto* e) { return duckdb_v2_sql_statement_get_text(statement, &text, e); });
  const std::string_view sql(text.ptr == nullptr ? "" : text.ptr, text.len);
  TokenIterator tokens;
  Call([&](auto* e) { return duckdb_v2_tokenize_sql(connection, &text, tokens.out(), e); });
  int depth = 0;
  for (;;) {
    DUCKDB_V2_TOKEN_TYPE type = DUCKDB_V2_TOKEN_TYPE_INVALID;
    idx_t start = 0;
    idx_t length = 0;
    Call([&](auto* e) { return duckdb_v2_token_iterator_next(tokens.get(), &type, &start, &length, e); });
    if (type == DUCKDB_V2_TOKEN_TYPE_END_OF_INPUT) return false;
    const std::string_view token = sql.substr(start, length);
    if (type == DUCKDB_V2_TOKEN_TYPE_OPERATOR) {
      for (const char c : token) depth += c == '(' ? 1 : c == ')' ? -1 : 0;
    } else if (type == DUCKDB_V2_TOKEN_TYPE_KEYWORD && depth == 0 && token.size() == 9) {
      std::string word(token);
      std::transform(word.begin(), word.end(), word.begin(),
                     [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
      if (word == "RETURNING") return true;
    }
  }
}

// A write, described by binding it (read-only, even inside the session's transaction;
// a write is never executed to describe it). Without RETURNING DuckDB reports one BIGINT
// column named Count, the changed rows; with RETURNING, the returned columns, converted
// as a query's are (ResultArrowSchema). A RETURNING of one BIGINT column named Count
// looks the same as the changed-rows count, so the statement's tokens decide that one.
Described DescribeWrite(duckdb_v2_connection_handle connection,
                        duckdb_v2_sql_statement_handle statement) {
  using Schema = Owned<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
  Schema output;
  duckdb_v2_error_info_handle err = nullptr;
  if (duckdb_v2_statement_bind(connection, statement, output.out(), nullptr, &err) !=
      DUCKDB_V2_ERROR_NONE) {
    if (err != nullptr) duckdb_v2_error_info_destroy(&err);
    return {};
  }
  try {
    idx_t count = 0;
    Call([&](auto* e) { return duckdb_v2_schema_get_count(output.get(), &count, e); });
    if (count == 1) {
      duckdb_v2_identifier_t name{};
      duckdb_v2_logical_type_handle type = nullptr;  // borrowed from the schema
      Call([&](auto* e) { return duckdb_v2_schema_get_field(output.get(), 0, &name, &type, e); });
      DUCKDB_V2_LOGICAL_TYPE_ID id{};
      Call([&](auto* e) { return duckdb_v2_logical_type_get_id(type, &id, e); });
      if (ToString(name) == "Count" && id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT &&
          !HasTopLevelReturning(connection, statement)) {
        return Described{false, nullptr};
      }
    }
    return Described{true, ResultArrowSchema(connection, output.get())};
  } catch (const Error&) {
    return {};
  }
}

// A prepared query describes itself without running: binding gives its result columns'
// DuckDB types, and DuckDB's Arrow exporter the schema an execution would send
// (ResultArrowSchema). Executing to learn the schema was not safe: dropping the result
// unstepped did not always run nothing, since DuckDB's threads could already have
// picked the plan up, and the drop then waited for the whole query. Writes are
// described by binding too (DescribeWrite). DDL and the like return no rows; the rest
// (PRAGMA, a statement whose parameter types DuckDB can't tell, ...) stay unknown.
Described Describe(duckdb_v2_connection_handle connection,
                   duckdb_v2_sql_statement_handle statement, const ParameterTypes& types) {
  switch (StatementType(statement)) {
    case DUCKDB_V2_STATEMENT_TYPE_SELECT:
    case DUCKDB_V2_STATEMENT_TYPE_EXPLAIN:
      break;
    case DUCKDB_V2_STATEMENT_TYPE_INSERT:
    case DUCKDB_V2_STATEMENT_TYPE_UPDATE:
    case DUCKDB_V2_STATEMENT_TYPE_DELETE:
    case DUCKDB_V2_STATEMENT_TYPE_MERGE_INTO:
      return DescribeWrite(connection, statement);
    case DUCKDB_V2_STATEMENT_TYPE_CREATE:
    case DUCKDB_V2_STATEMENT_TYPE_DROP:
    case DUCKDB_V2_STATEMENT_TYPE_ALTER:
    case DUCKDB_V2_STATEMENT_TYPE_TRANSACTION:
    case DUCKDB_V2_STATEMENT_TYPE_SET:
    case DUCKDB_V2_STATEMENT_TYPE_VARIABLE_SET:
    case DUCKDB_V2_STATEMENT_TYPE_CREATE_FUNC:
    case DUCKDB_V2_STATEMENT_TYPE_ATTACH:
    case DUCKDB_V2_STATEMENT_TYPE_DETACH:
    case DUCKDB_V2_STATEMENT_TYPE_LOAD:
    case DUCKDB_V2_STATEMENT_TYPE_VACUUM:
    case DUCKDB_V2_STATEMENT_TYPE_ANALYZE:
      return Described{false, nullptr};
    default:
      return {};
  }
  // A parameter whose type DuckDB can't tell (`SELECT ?`) leaves the result's types
  // open until the values arrive.
  for (std::size_t i = 0; i < types.size(); i++) {
    if (types.For(i, types.named() ? types.name(i) : "") == nullptr) return {};
  }
  using Schema = Owned<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
  Schema output;
  duckdb_v2_error_info_handle err = nullptr;
  if (duckdb_v2_statement_bind(connection, statement, output.out(), nullptr, &err) !=
      DUCKDB_V2_ERROR_NONE) {
    // e.g. a statement that expands into a group: its schema is known only at execution
    if (err != nullptr) duckdb_v2_error_info_destroy(&err);
    return {};
  }
  try {
    return Described{true, ResultArrowSchema(connection, output.get())};
  } catch (const Error&) {
    return {};
  }
}

// What a statement does to the session's transaction, from its type and its first
// keyword (tokens, so a leading comment is no keyword): BEGIN and START open one;
// COMMIT, END, ROLLBACK and ABORT end it.
enum class TransactionEffect { kNone, kBegin, kCommit, kRollback };

TransactionEffect TransactionEffectOf(duckdb_v2_connection_handle connection,
                                      duckdb_v2_sql_statement_handle statement) {
  if (StatementType(statement) != DUCKDB_V2_STATEMENT_TYPE_TRANSACTION) {
    return TransactionEffect::kNone;
  }
  duckdb_v2_str text{};
  Call([&](auto* e) { return duckdb_v2_sql_statement_get_text(statement, &text, e); });
  const std::string_view sql(text.ptr == nullptr ? "" : text.ptr, text.len);
  TokenIterator tokens;
  Call([&](auto* e) { return duckdb_v2_tokenize_sql(connection, &text, tokens.out(), e); });
  for (;;) {
    DUCKDB_V2_TOKEN_TYPE type = DUCKDB_V2_TOKEN_TYPE_INVALID;
    idx_t start = 0;
    idx_t length = 0;
    Call([&](auto* e) {
      return duckdb_v2_token_iterator_next(tokens.get(), &type, &start, &length, e);
    });
    if (type == DUCKDB_V2_TOKEN_TYPE_END_OF_INPUT) return TransactionEffect::kNone;
    if (type == DUCKDB_V2_TOKEN_TYPE_COMMENT) continue;
    std::string word(sql.substr(start, length));
    std::transform(word.begin(), word.end(), word.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    if (word == "BEGIN" || word == "START") return TransactionEffect::kBegin;
    if (word == "COMMIT" || word == "END") return TransactionEffect::kCommit;
    if (word == "ROLLBACK" || word == "ABORT") return TransactionEffect::kRollback;
    return TransactionEffect::kNone;
  }
}

// Whether DuckDB has aborted the connection's open transaction. Most failed statements
// abort it, but not all (a parser error leaves it usable), so DuckDB is asked: in an
// aborted transaction even SELECT 1 is refused, with a transaction error.
bool TransactionAborted(duckdb_v2_connection_handle connection) {
  auto statements = Parse(connection, "SELECT 1");
  Result result;
  duckdb_v2_error_info_handle err = nullptr;
  const DUCKDB_V2_ERROR code = duckdb_v2_statement_execute(
      connection, statements.at(0).get(), nullptr, nullptr, 0, result.out(), &err);
  if (code == DUCKDB_V2_ERROR_NONE) {
    idx_t changed = 0;
    Call([&](auto* e) { return duckdb_v2_result_drain(result.get(), &changed, e); });
    return false;
  }
  DUCKDB_V2_ERROR detailed = code;
  if (err != nullptr) {
    duckdb_v2_error_info_get_code(err, &detailed);
    duckdb_v2_error_info_destroy(&err);
  }
  return detailed == DUCKDB_V2_ERROR_DATABASE_TRANSACTION;
}

bool HasNull(const json::value& parameters) {
  if (parameters.is_array()) {
    for (const auto& item : parameters.as_array()) {
      if (item.is_null()) return true;
    }
  } else if (parameters.is_object()) {
    for (const auto& item : parameters.as_object()) {
      if (item.value().is_null()) return true;
    }
  }
  return false;
}

}  // namespace

DuckDBSession::DuckDBSession(duckdb_v2_instance_handle instance, std::size_t batch_rows)
    : batch_rows_(batch_rows) {
  static const auto kDefaultPolicy = std::make_shared<const ReadOnlyPolicy>(ReadOnlyPolicy::Default());
  read_only_policy_ = kDefaultPolicy;
  Call([&](auto* err) { return duckdb_v2_connection_create(instance, connection_.out(), err); });
  Call([&](auto* err) { return duckdb_v2_connection_get_factory(connection_.get(), &factory_, err); });
  values_ = std::make_unique<ValueFactory>(factory_);
  // DuckDB's default Arrow mapping, as GizmoSQL 1.x uses: HUGEINT (every integer SUM)
  // arrives as decimal128(38, 0), which clients can compute with. The lossless mode
  // keeps UUID, JSON and TIMETZ exact but turns HUGEINT into opaque bytes; a session can
  // opt in with the duckdb_arrow_lossless_conversion option.
}

DuckDBSession::~DuckDBSession() {
  connection_.reset();
}

// ---- Statements ----

chargewire::StatementOutcome DuckDBSession::Execute(std::string_view sql,
                                                 const json::value& parameters) {
  auto statements = Parse(connection_.get(), sql);
  if (statements.empty()) {
    throw Error(ErrorCode::kSqlError, "no SQL statement given");
  }
  if (!parameters.is_null() && statements.size() > 1) {
    throw Error(ErrorCode::kSqlError, "bind parameters need a single SQL statement");
  }
  for (const auto& statement : statements) {
    switch (StatementType(statement.get())) {
      case DUCKDB_V2_STATEMENT_TYPE_SET:  // USE, SET schema/search_path
      case DUCKDB_V2_STATEMENT_TYPE_ATTACH:
      case DUCKDB_V2_STATEMENT_TYPE_DETACH:
        options_changed_ = true;
        break;
      default:
        break;
    }
  }
  // Every statement but the last runs to completion; the last one's result is returned.
  BeginWork();
  work_in_transaction_ = true;  // also when the statement then fails; BEGIN, COMMIT and ROLLBACK reset it (Ran)
  for (std::size_t i = 0; i + 1 < statements.size(); i++) {
    if (read_only_) {
      const Gated gated = GateReadOnly(statements[i].get(), BoundValues{});
      if (!gated.handled) {
        if (gated.replacement.empty()) {
          RunToEnd(connection_.get(), statements[i].get());
        } else {
          RunSql(connection_.get(), gated.replacement);
        }
      }
      EnsureReadOnlyTransaction();  // a COMMIT or ROLLBACK just ended one: the next statement has its own
      continue;
    }
    RunToEnd(connection_.get(), statements[i].get());
    Ran(statements[i].get());
    BeginOwedTransaction();
  }
  auto last = std::make_shared<SqlStatement>(std::move(statements.back()));

  const ParameterTypes types =
      HasNull(parameters) ? ParameterTypes::Load(connection_.get(), last->get()) : ParameterTypes();
  const BoundValues bound = values_->FromJson(parameters, types);
  if (read_only_) {
    const Gated gated = GateReadOnly(last->get(), bound);
    if (gated.handled) return {};  // a transaction statement: no rows, no count
    if (!gated.replacement.empty()) {
      auto replaced = Parse(connection_.get(), gated.replacement);
      last = std::make_shared<SqlStatement>(std::move(replaced.at(0)));
    }
  }
  const auto handles = bound.Handles();
  const auto names = bound.NameHandles();
  ArrowResult result;
  Call([&](auto* err) {
    return duckdb_v2_statement_execute_arrow(connection_.get(), last->get(),
                                             names.empty() ? nullptr : names.data(),
                                             handles.empty() ? nullptr : handles.data(),
                                             handles.size(), batch_rows_, result.out(), err);
  });
  if (!read_only_) Ran(last->get());
  return Outcome(result, last);
}

std::unique_ptr<chargewire::PreparedStatement> DuckDBSession::Prepare(std::string_view sql) {
  auto statements = Parse(connection_.get(), sql);
  if (statements.size() != 1) {
    throw Error(ErrorCode::kSqlError, "a prepared statement is exactly one SQL statement");
  }
  BeginWork();
  // What can be told without the values: the kind. The plan is checked at every execution.
  if (read_only_) CheckReadOnlyKind(statements[0].get());
  PreparedHandle prepared;
  Call([&](auto* err) {
    return duckdb_v2_prepared_statement_create(connection_.get(), statements[0].get(), false,
                                               prepared.out(), err);
  });
  ParameterTypes types = ParameterTypes::Load(connection_.get(), statements[0].get());
  Described described = Describe(connection_.get(), statements[0].get(), types);
  auto parameter_schema = types.ArrowSchema(connection_.get());
  return std::make_unique<DuckDBPrepared>(std::move(statements[0]), std::move(prepared),
                                          std::move(types), std::move(described),
                                          std::move(parameter_schema));
}


chargewire::StatementOutcome DuckDBSession::ExecutePrepared(chargewire::PreparedStatement& statement,
                                                         const chargewire::Parameters& parameters) {
  auto& prepared = static_cast<DuckDBPrepared&>(statement);
  auto run = [&](const BoundValues& bound) {
    BeginWork();
    if (read_only_) {
      // Every execution is checked as it runs (spec §7.2), also of a statement prepared
      // while the session was read-write. A prepared statement is never one the adapter
      // runs itself or rewrites: those kinds cannot be prepared.
      const Gated gated = GateReadOnly(prepared.statement(), bound);
      if (gated.handled || !gated.replacement.empty()) {
        throw ReadOnlyRefusal(std::string("a prepared ") + std::string(StatementKindName(StatementType(prepared.statement()))));
      }
    }
    const auto handles = bound.Handles();
    const auto names = bound.NameHandles();
    ArrowResult result;
    Call([&](auto* err) {
      return duckdb_v2_prepared_statement_execute_arrow(
          prepared.prepared(), names.empty() ? nullptr : names.data(),
          handles.empty() ? nullptr : handles.data(), handles.size(), batch_rows_, result.out(), err);
    });
    work_in_transaction_ = true;
    if (!read_only_) Ran(prepared.statement());
    // The prepared statement outlives its executions: the SDK destroys it on the
    // session's strand, after any statement that uses it.
    return Outcome(result, nullptr);
  };

  if (!parameters.rows) {
    return run(values_->FromJson(parameters.json, prepared.types()));
  }

  // One execution per parameter row; rows from all executions in order (spec §7.2).
  arrow::RecordBatchVector batches;
  std::shared_ptr<arrow::Schema> schema;
  std::optional<int64_t> changed;
  for (;;) {
    auto next = parameters.rows->Next();
    if (!next.ok()) {
      throw Error(ErrorCode::kBadRequest, next.status().message());
    }
    auto parameter_batch = std::move(next).ValueUnsafe();
    if (parameter_batch == nullptr) {
      break;
    }
    for (int64_t row = 0; row < parameter_batch->num_rows(); row++) {
      auto outcome = run(values_->FromArrowRow(*parameter_batch, row, prepared.types()));
      if (outcome.rows) {
        schema = outcome.rows->schema();
        auto all = outcome.rows->ToRecordBatches();
        if (!all.ok()) {
          throw Error(ErrorCode::kExecutionError, all.status().message());
        }
        for (auto& batch : *all) batches.push_back(std::move(batch));
      } else if (outcome.rows_affected) {
        changed = changed.value_or(0) + *outcome.rows_affected;
      }
    }
  }
  if (!schema) {
    schema = prepared.ResultSchema();  // a query run for no parameter rows: no rows, not a count
  }
  chargewire::StatementOutcome outcome;
  if (schema) {
    auto reader = arrow::RecordBatchReader::Make(std::move(batches), schema);
    if (!reader.ok()) {
      throw Error(ErrorCode::kExecutionError, reader.status().message());
    }
    outcome.rows = *reader;
  } else {
    outcome.rows_affected = changed.value_or(0);
  }
  return outcome;
}

int64_t DuckDBSession::Ingest(const chargewire::IngestTarget& target,
                              std::shared_ptr<arrow::RecordBatchReader> rows) {
  // The SDK refuses ingest in a read-only session before it reads the body; this holds for
  // an ingest that was already on its way when the session became read-only.
  if (read_only_) throw ReadOnlyRefusal("bulk ingest");
  BeginOwedTransaction();
  work_in_transaction_ = true;
  return chargewire_duckdb::Ingest(connection_.get(), target, std::move(rows));
}

// ---- Metadata (spec §10), with Arrow Flight SQL's result schemas ----

namespace {

// The types DuckDB offers clients (Flight SQL's GetXdbcTypeInfo, JDBC's getTypeInfo),
// with JDBC type codes as GizmoSQL 1.x reports them, ordered by data_type as JDBC asks.
std::shared_ptr<arrow::RecordBatchReader> TypeInfo(std::optional<int32_t> data_type) {
  struct Type {
    const char* name;
    int32_t code;
    std::optional<int32_t> size;
    bool quoted;
    std::vector<const char*> params;
    bool fixed_scale;
  };
  // java.sql.Types: BIT -7, TINYINT -6, BIGINT -5, LONGVARCHAR -1, VARBINARY -3, DECIMAL 3,
  // INTEGER 4, SMALLINT 5, FLOAT 6, DOUBLE 8, VARCHAR 12, BOOLEAN 16, DATE 91, TIME 92,
  // TIMESTAMP 93, OTHER 1111, TIMESTAMP_WITH_TIMEZONE 2014.
  static const std::vector<Type> kTypes = {
      {"bigint", -5, 19, false, {}, false},    {"hugeint", -5, 38, false, {}, false},
      {"blob", -3, std::nullopt, true, {}, false},
      {"tinyint", -6, 3, false, {}, false},    {"text", -1, std::nullopt, true, {}, false},
      {"decimal", 3, 38, false, {"precision", "scale"}, true},
      {"integer", 4, 10, false, {}, false},    {"smallint", 5, 5, false, {}, false},
      {"float", 6, 7, false, {}, false},       {"double", 8, 15, false, {}, false},
      {"varchar", 12, std::nullopt, true, {}, false},
      {"boolean", 16, 1, false, {}, false},    {"date", 91, 10, true, {}, false},
      {"time", 92, 15, true, {}, false},       {"timestamp", 93, 26, true, {}, false},
      {"interval", 1111, std::nullopt, true, {}, false},
      {"uuid", 1111, 36, true, {}, false},
      {"timestamp with time zone", 2014, 32, true, {}, false},
  };
  const auto schema = chargewire::MetadataSchema(MetadataKind::kTypeInfo);
  std::unique_ptr<arrow::RecordBatchBuilder> builder =
      arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool()).ValueOrDie();
  auto text = [&](int i) { return builder->GetFieldAs<arrow::StringBuilder>(i); };
  auto i32 = [&](int i) { return builder->GetFieldAs<arrow::Int32Builder>(i); };
  auto flag = [&](int i) { return builder->GetFieldAs<arrow::BooleanBuilder>(i); };
  auto check = [](const arrow::Status& status) {
    if (!status.ok()) throw Error(ErrorCode::kExecutionError, status.ToString());
  };
  auto maybe = [&](arrow::Int32Builder* b, std::optional<int32_t> v) {
    check(v ? b->Append(*v) : b->AppendNull());
  };
  std::vector<Type> types = kTypes;
  std::stable_sort(types.begin(), types.end(),
                   [](const Type& a, const Type& b) { return a.code < b.code; });
  for (const Type& type : types) {
    if (data_type && *data_type != type.code) continue;
    check(text(0)->Append(type.name));
    check(i32(1)->Append(type.code));
    maybe(i32(2), type.size);
    check(type.quoted ? text(3)->Append("'") : text(3)->AppendNull());
    check(type.quoted ? text(4)->Append("'") : text(4)->AppendNull());
    auto* params = builder->GetFieldAs<arrow::ListBuilder>(5);
    check(params->Append());
    for (const char* param : type.params) {
      check(static_cast<arrow::StringBuilder*>(params->value_builder())->Append(param));
    }
    check(i32(6)->Append(1));                         // nullable: typeNullable
    check(flag(7)->Append(type.quoted && type.code != 91 && type.code != 92 && type.code != 93 &&
                          type.code != 2014 && type.code != 1111));  // text compares by case
    check(i32(8)->Append(3));                         // searchable: typeSearchable
    check(type.quoted ? flag(9)->AppendNull() : flag(9)->Append(false));
    check(flag(10)->Append(type.fixed_scale));
    check(flag(11)->Append(false));
    check(text(12)->Append(type.name));
    maybe(i32(13), type.fixed_scale ? std::optional<int32_t>(0) : std::nullopt);
    maybe(i32(14), type.fixed_scale ? std::optional<int32_t>(38) : std::nullopt);
    check(i32(15)->Append(type.code));
    check(i32(16)->AppendNull());
    maybe(i32(17), type.quoted ? std::nullopt : std::optional<int32_t>(10));
    check(i32(18)->AppendNull());
  }
  auto batch = builder->Flush().ValueOrDie();
  return arrow::RecordBatchReader::Make({batch}, schema).ValueOrDie();
}

}  // namespace

// Every schema by catalog and oid, with the name the metadata calls give it (spec §10):
// a schema's own name, or, for a schema nested in another (DuckDB 2.0), its path, each
// part written quoted when it contains a dot or a double quote. DuckDB's listings give a
// schema's leaf name and its parent's oid only, so the path is built from the root down.
// The listing is read once (MATERIALIZED): an attached database of another kind may list
// its schemas with a round trip.
constexpr const char* kSchemaPaths =
    "WITH RECURSIVE schemas AS MATERIALIZED ("
    "SELECT database_name, oid, parent_schema_oid, "
    "CASE WHEN schema_name LIKE '%.%' OR schema_name LIKE '%\"%' "
    "THEN '\"' || replace(schema_name, '\"', '\"\"') || '\"' ELSE schema_name END AS part "
    "FROM duckdb_schemas()), "
    "schema_paths AS ("
    "SELECT database_name, oid, part AS path FROM schemas WHERE parent_schema_oid IS NULL "
    "UNION ALL SELECT s.database_name, s.oid, p.path || '.' || s.part FROM schemas s "
    "JOIN schema_paths p ON s.database_name = p.database_name AND s.parent_schema_oid = p.oid) ";

std::shared_ptr<arrow::RecordBatchReader> DuckDBSession::Metadata(
    MetadataKind kind, const chargewire::MetadataFilter& filter) {
  if (kind == MetadataKind::kTypeInfo) {
    return TypeInfo(filter.data_type);
  }
  if (read_only_) BeginWork();
  // Filters are bound as parameters, never spliced into the SQL: one VARCHAR value (or
  // VARCHAR NULL) per named parameter.
  std::vector<std::string> names;
  std::vector<Value> values;
  auto text = [&](std::string name, const std::optional<std::string>& value) {
    names.push_back(std::move(name));
    json::array one;
    one.push_back(value ? json::value(*value) : json::value(nullptr));
    values.push_back(std::move(values_->FromJson(one, {}).values[0]));
  };

  std::string sql;
  switch (kind) {
    case MetadataKind::kTypeInfo:
      break;  // answered above
    case MetadataKind::kCatalogs:
      // duckdb_databases() names the attached catalogs without opening them;
      // information_schema.schemata would list every schema of every catalog, which for
      // remote catalogs (DuckLake on PostgreSQL, say) means a connection to each.
      sql = "SELECT DISTINCT database_name AS catalog_name FROM duckdb_databases() ORDER BY 1";
      break;
    case MetadataKind::kDbSchemas:
      text("catalog", filter.catalog);
      text("schema", filter.db_schema);
      sql = std::string(kSchemaPaths) +
            "SELECT database_name AS catalog_name, path AS db_schema_name FROM schema_paths "
            "WHERE ($catalog IS NULL OR database_name = $catalog) "
            "AND ($schema IS NULL OR path LIKE $schema ESCAPE '\\') ORDER BY 1, 2";
      break;
    case MetadataKind::kTables: {
      std::optional<std::string> types;
      for (const auto& type : filter.table_types) {
        types = types ? *types + "," + type : type;
      }
      text("catalog", filter.catalog);
      text("schema", filter.db_schema);
      text("table", filter.table);
      text("types", types);
      // information_schema.tables, with each schema's path (nested schemas) in its place.
      // Like information_schema.tables, it leaves out the engine's internal tables and views
      // (the system catalog's information_schema, pg_catalog and duckdb_* views): 2.1.0
      // listed them, so every client's table browser showed DuckDB's own catalog.
      sql = std::string(kSchemaPaths) +
            ", tables AS (SELECT database_name, schema_oid, table_name, "
            "CASE WHEN temporary THEN 'LOCAL TEMPORARY' ELSE 'BASE TABLE' END AS table_type "
            "FROM duckdb_tables() WHERE NOT internal "
            "UNION ALL SELECT database_name, schema_oid, view_name, 'VIEW' "
            "FROM duckdb_views() WHERE NOT internal) "
            "SELECT t.database_name AS catalog_name, p.path AS db_schema_name, t.table_name, "
            "t.table_type FROM tables t JOIN schema_paths p "
            "ON p.database_name = t.database_name AND p.oid = t.schema_oid "
            "WHERE ($catalog IS NULL OR t.database_name = $catalog) "
            "AND ($schema IS NULL OR p.path LIKE $schema ESCAPE '\\') "
            "AND ($table IS NULL OR t.table_name LIKE $table ESCAPE '\\') "
            "AND ($types IS NULL OR list_contains(string_split($types, ','), t.table_type)) "
            "ORDER BY 1, 2, 3, 4";
      break;
    }
    case MetadataKind::kTableTypes:
      sql = "SELECT table_type FROM (VALUES ('BASE TABLE'), ('LOCAL TEMPORARY'), ('VIEW')) "
            "t(table_type) ORDER BY 1";
      break;
    case MetadataKind::kPrimaryKeys:
      text("catalog", filter.catalog);
      text("schema", filter.db_schema);
      text("table", filter.table);
      sql = std::string(kSchemaPaths) +
            "SELECT catalog_name, db_schema_name, table_name, column_name, key_name, "
            "CAST(key_sequence AS INTEGER) AS key_sequence FROM ("
            "SELECT c.database_name AS catalog_name, p.path AS db_schema_name, c.table_name, "
            "unnest(c.constraint_column_names) AS column_name, c.constraint_name AS key_name, "
            "unnest(range(1, len(c.constraint_column_names) + 1)) AS key_sequence "
            "FROM duckdb_constraints() c JOIN schema_paths p "
            "ON p.database_name = c.database_name AND p.oid = c.schema_oid "
            "WHERE c.constraint_type = 'PRIMARY KEY' "
            "AND ($table IS NULL OR c.table_name = $table) "
            "AND ($catalog IS NULL OR c.database_name = $catalog) "
            "AND ($schema IS NULL OR p.path = $schema)) ORDER BY 1, 2, 3, 5, 6";
      break;
    case MetadataKind::kImportedKeys:
    case MetadataKind::kExportedKeys:
    case MetadataKind::kCrossReference: {
      std::string where;
      if (kind == MetadataKind::kImportedKeys) {
        text("catalog", filter.catalog);
        text("schema", filter.db_schema);
        text("table", filter.table);
        where = "($table IS NULL OR c.table_name = $table) "
                "AND ($catalog IS NULL OR c.database_name = $catalog) "
                "AND ($schema IS NULL OR p.path = $schema)";
      } else if (kind == MetadataKind::kExportedKeys) {
        text("catalog", filter.catalog);
        text("schema", filter.db_schema);
        text("table", filter.table);
        where = "c.referenced_table = $table AND ($catalog IS NULL OR c.database_name = $catalog) "
                "AND ($schema IS NULL OR p.path = $schema)";
      } else {
        text("pk_table", filter.table);
        text("fk_table", filter.fk_table);
        text("pk_catalog", filter.catalog);
        text("pk_schema", filter.db_schema);
        where = "c.referenced_table = $pk_table AND c.table_name = $fk_table "
                "AND ($pk_catalog IS NULL OR c.database_name = $pk_catalog) "
                "AND ($pk_schema IS NULL OR p.path = $pk_schema)";
      }
      const std::string order =
          kind == MetadataKind::kImportedKeys ? "1, 2, 3, 11, 9" : "5, 6, 7, 10, 9";
      sql = std::string(kSchemaPaths) +
            "SELECT pk_catalog_name, pk_db_schema_name, pk_table_name, pk_column_name, "
            "fk_catalog_name, fk_db_schema_name, fk_table_name, fk_column_name, "
            "CAST(key_sequence AS INTEGER) AS key_sequence, fk_key_name, "
            "NULL::VARCHAR AS pk_key_name, CAST(3 AS UTINYINT) AS update_rule, "
            "CAST(3 AS UTINYINT) AS delete_rule FROM ("
            "SELECT c.database_name AS pk_catalog_name, p.path AS pk_db_schema_name, "
            "c.referenced_table AS pk_table_name, "
            "unnest(c.referenced_column_names) AS pk_column_name, "
            "c.database_name AS fk_catalog_name, p.path AS fk_db_schema_name, "
            "c.table_name AS fk_table_name, unnest(c.constraint_column_names) AS fk_column_name, "
            "unnest(range(1, len(c.constraint_column_names) + 1)) AS key_sequence, "
            "c.constraint_name AS fk_key_name FROM duckdb_constraints() c JOIN schema_paths p "
            "ON p.database_name = c.database_name AND p.oid = c.schema_oid "
            "WHERE c.constraint_type = 'FOREIGN KEY' AND " + where + ") ORDER BY " + order;
      break;
    }
  }

  std::vector<duckdb_v2_value_handle> handles;
  for (const auto& value : values) handles.push_back(value.get());
  auto rows = QueryArrow(connection_.get(), sql, names, handles);
  if (kind == MetadataKind::kTables && filter.include_schema) {
    return TablesWithSchemas(std::move(rows));
  }
  return rows;
}

std::shared_ptr<arrow::RecordBatchReader> DuckDBSession::TablesWithSchemas(
    std::shared_ptr<arrow::RecordBatchReader> tables) {
  auto all = tables->ToTable();
  tables.reset();  // a connection runs one result at a time: release it first
  if (!all.ok()) {
    throw Error(ErrorCode::kExecutionError, all.status().message());
  }
  auto combined = (*all)->CombineChunksToBatch();
  if (!combined.ok()) {
    throw Error(ErrorCode::kExecutionError, combined.status().message());
  }
  const auto& batch = **combined;
  auto column = [&](int i) { return std::static_pointer_cast<arrow::StringArray>(batch.column(i)); };
  auto catalogs = column(0), schemas = column(1), names = column(2);

  arrow::BinaryBuilder builder;
  for (int64_t row = 0; row < batch.num_rows(); row++) {
    const std::string catalog = catalogs->GetString(row);
    const std::string schema = schemas->GetString(row);
    const std::string table = names->GetString(row);
    // The table's Arrow schema, with the Flight SQL per-field metadata drivers read. The
    // schema's name is already written as SQL takes it (a nested schema's path).
    auto table_schema = QueryArrow(connection_.get(), "SELECT * FROM " + QualifiedName({catalog}) + "." +
                                                          schema + "." + QualifiedName({table}) + " LIMIT 0")
                            ->schema();
    // What the catalog says about each column, beyond its Arrow type.
    struct Column {
      std::string type_name;
      bool nullable = true;
      std::string precision, scale;  // decimal digits; empty when not numeric
      bool auto_increment = false;
      std::string remarks, default_sql;
    };
    std::map<std::string, Column> columns;
    {
      std::vector<Value> bound;
      for (const auto& part : {catalog, schema, table}) {
        bound.push_back(std::move(values_->FromJson(json::array{json::value(part)}, {}).values[0]));
      }
      std::vector<duckdb_v2_value_handle> handles;
      for (const auto& value : bound) handles.push_back(value.get());
      auto described = QueryArrow(
          connection_.get(),
          std::string(kSchemaPaths) +
              "SELECT c.column_name, c.data_type, CASE WHEN c.is_nullable THEN 'YES' ELSE 'NO' END, "
              "c.numeric_precision, c.numeric_scale, c.column_default, c.numeric_precision_radix, "
              "c.comment FROM duckdb_columns() c JOIN schema_paths p "
              "ON p.database_name = c.database_name AND p.oid = c.schema_oid "
              "WHERE c.database_name = $c AND p.path = $s AND c.table_name = $t ORDER BY c.column_index",
          {"c", "s", "t"}, handles);
      auto rows = described->ToTable();
      described.reset();
      if (rows.ok()) {
        auto flat = (*rows)->CombineChunksToBatch();
        if (flat.ok()) {
          auto text = [&](int column, int64_t row) -> std::string {
            auto scalar = (*flat)->column(column)->GetScalar(row);
            return scalar.ok() && (*scalar)->is_valid ? (*scalar)->ToString() : std::string();
          };
          for (int64_t i = 0; i < (*flat)->num_rows(); i++) {
            Column& column = columns[text(0, i)];
            column.type_name = text(1, i);
            // The base type, as Flight SQL drivers report it: DECIMAL(10,2) -> DECIMAL (the
            // digits are PRECISION and SCALE).
            if (const auto paren = column.type_name.find('('); paren != std::string::npos) {
              column.type_name.resize(paren);
            }
            column.nullable = text(2, i) != "NO";
            column.scale = text(4, i);
            if (text(6, i) == "10") {
              column.precision = text(3, i);  // DECIMAL: already digits
            } else {
              // Binary precision (bits) as the decimal digits JDBC and ODBC report.
              static const std::map<std::string, std::string, std::less<>> kDigits{
                  {"TINYINT", "3"},   {"SMALLINT", "5"},   {"INTEGER", "10"},  {"BIGINT", "19"},
                  {"HUGEINT", "39"},  {"UTINYINT", "3"},   {"USMALLINT", "5"}, {"UINTEGER", "10"},
                  {"UBIGINT", "20"},  {"UHUGEINT", "39"},  {"FLOAT", "7"},     {"DOUBLE", "15"}};
              auto digits = kDigits.find(column.type_name);
              column.precision = digits == kDigits.end() ? std::string() : digits->second;
              if (digits == kDigits.end()) column.scale.clear();
            }
            column.default_sql = text(5, i);
            column.auto_increment = column.default_sql.starts_with("nextval(");
            column.remarks = text(7, i);
          }
        }
      }
    }
    const std::vector<std::string> default_keys = ColumnDefaultMetadataKeys();
    arrow::FieldVector fields;
    for (const auto& field : table_schema->fields()) {
      auto it = columns.find(field->name());
      const Column column =
          it == columns.end() ? Column{field->type()->ToString(), true, {}, {}, false, {}, {}} : it->second;
      std::vector<std::string> keys{"ARROW:FLIGHT:SQL:CATALOG_NAME", "ARROW:FLIGHT:SQL:DB_SCHEMA_NAME",
                                    "ARROW:FLIGHT:SQL:TABLE_NAME", "ARROW:FLIGHT:SQL:TYPE_NAME",
                                    "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT"};
      std::vector<std::string> values{catalog, schema, table, column.type_name,
                                      column.auto_increment ? "1" : "0"};
      if (!column.precision.empty()) {
        keys.push_back("ARROW:FLIGHT:SQL:PRECISION");
        values.push_back(column.precision);
      }
      if (!column.scale.empty()) {
        keys.push_back("ARROW:FLIGHT:SQL:SCALE");
        values.push_back(column.scale);
      }
      if (!column.remarks.empty()) {
        keys.push_back("ARROW:FLIGHT:SQL:REMARKS");
        values.push_back(column.remarks);
      }
      if (!column.default_sql.empty()) {
        for (const auto& key : default_keys) {  // Flight SQL has no key for it (spec §10)
          keys.push_back(key);
          values.push_back(column.default_sql);
        }
      }
      fields.push_back(field->WithNullable(column.nullable)
                           ->WithMetadata(arrow::key_value_metadata(keys, values)));
    }
    auto serialized = arrow::ipc::SerializeSchema(*arrow::schema(fields));
    if (!serialized.ok()) {
      throw Error(ErrorCode::kExecutionError, serialized.status().message());
    }
    if (!builder.Append((*serialized)->data(), (*serialized)->size()).ok()) {
      throw Error(ErrorCode::kExecutionError, "could not build table schemas");
    }
  }
  std::shared_ptr<arrow::Array> schemas_column;
  if (!builder.Finish(&schemas_column).ok()) {
    throw Error(ErrorCode::kExecutionError, "could not build table schemas");
  }
  auto columns = batch.columns();
  columns.push_back(schemas_column);
  auto fields = batch.schema()->fields();
  fields.push_back(arrow::field("table_schema", arrow::binary(), false));
  auto out = arrow::RecordBatch::Make(arrow::schema(fields), batch.num_rows(), columns);
  auto reader = arrow::RecordBatchReader::Make({out});
  if (!reader.ok()) {
    throw Error(ErrorCode::kExecutionError, reader.status().message());
  }
  return *reader;
}

// ---- Control and options ----

void DuckDBSession::Interrupt() { duckdb_v2_connection_interrupt(connection_.get(), nullptr); }

std::optional<double> DuckDBSession::Progress() {
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

namespace {
// A SQL string literal: 'it''s'.
std::string SqlLiteral(std::string_view text) {
  std::string out = "'";
  for (const char c : text) {
    if (c == '\'') out += '\'';
    out += c;
  }
  return out + "'";
}
}  // namespace

chargewire::OptionErrors DuckDBSession::SetOptions(const json::object& options) {
  const OwnReadOnlyWork own(*this);
  BeginWork();
  // In a transaction, a USE that fails (a catalog or schema that isn't there) aborts the
  // transaction: one mistyped option threw away the work in it, and the answer said only that
  // the transaction was aborted. There, a missing one is reported without running USE.
  const bool in_transaction = transaction_ != chargewire::TransactionState::kNone;
  auto missing = [&](const std::string& name) {
    return Error(ErrorCode::kInvalidOptionValue,
                 "Catalog Error: SET schema: No catalog + schema named \"" + name + "\" found.");
  };
  chargewire::OptionErrors errors;
  auto apply = [&](std::string_view key, auto&& fn) {
    const json::value* value = options.if_contains(key);
    if (value == nullptr) return;
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
    const std::string catalog = text(value, "catalog");
    if (in_transaction && QueryText(connection_.get(), "SELECT count(*) FROM duckdb_databases() WHERE "
                                                       "lower(database_name) = lower(" +
                                                           SqlLiteral(catalog) + ")") == "0") {
      throw missing(catalog);
    }
    RunSql(connection_.get(), "USE " + QualifiedName({catalog}));
  });
  apply("schema", [&](const json::value& value) {
    const std::string catalog = QueryText(connection_.get(), "SELECT current_database()");
    const std::string schema = text(value, "schema");
    if (in_transaction && QueryText(connection_.get(), "SELECT count(*) FROM duckdb_schemas() WHERE "
                                                       "lower(database_name) = lower(" +
                                                           SqlLiteral(catalog) + ") AND lower(schema_name) = lower(" +
                                                           SqlLiteral(schema) + ")") == "0") {
      throw missing(catalog + "." + schema);
    }
    RunSql(connection_.get(), "USE " + QualifiedName({catalog, schema}));
  });
  apply("autocommit", [&](const json::value& value) {
    if (!value.is_bool()) {
      throw Error(ErrorCode::kInvalidOptionValue, "autocommit must be true or false");
    }
    const bool on = value.as_bool();
    if (read_only_) {
      // The adapter starts a read-only session's transactions as they are needed, READ ONLY:
      // manual commit only means the next one stays open until the client ends it.
      if (on && !autocommit_) EndReadOnlyTransaction(true);
      autocommit_ = on;
      transaction_ = on ? chargewire::TransactionState::kNone : chargewire::TransactionState::kOpen;
      return;
    }
    if (!on && autocommit_) {
      RunSql(connection_.get(), "BEGIN TRANSACTION");
    } else if (on && !autocommit_) {
      RunSql(connection_.get(), "COMMIT");
    }
    autocommit_ = on;
    transaction_ = on ? chargewire::TransactionState::kNone : chargewire::TransactionState::kOpen;
    work_in_transaction_ = false;
  });
  apply("session_tag", [&](const json::value& value) {
    if (!value.is_object() && !value.is_null()) {
      throw Error(ErrorCode::kInvalidOptionValue, "session_tag must be a JSON object");
    }
    session_tag_ = value;
  });
  // DuckDB's arrow_lossless_conversion setting, as a session option of this engine's own
  // (spec §2.7).
  apply("duckdb_arrow_lossless_conversion", [&](const json::value& value) {
    if (!value.is_bool()) {
      throw Error(ErrorCode::kInvalidOptionValue, "duckdb_arrow_lossless_conversion must be true or false");
    }
    RunSql(connection_.get(), value.as_bool() ? "SET SESSION arrow_lossless_conversion = true"
                                              : "SET SESSION arrow_lossless_conversion = false");
  });
  for (const auto& option : options) {
    const std::string_view key = option.key();
    if (key != "catalog" && key != "schema" && key != "autocommit" && key != "session_tag" &&
        key != "duckdb_arrow_lossless_conversion") {
      errors.emplace(std::string(key), Error(ErrorCode::kUnknownOption, "unknown session option"));
    }
  }
  return errors;
}

json::object DuckDBSession::Options() {
  const OwnReadOnlyWork own(*this);
  if (read_only_) EnsureReadOnlyTransaction();
  json::object options{
      {"catalog", QueryText(connection_.get(), "SELECT current_database()")},
      {"schema", QueryText(connection_.get(), "SELECT current_schema()")},
      {"autocommit", autocommit_},
      {"duckdb_arrow_lossless_conversion", Lossless()},
  };
  if (!session_tag_.is_null()) {
    options["session_tag"] = session_tag_;
  }
  return options;
}

bool DuckDBSession::Lossless() {
  // The value this session's results use: its own (SET SESSION, or the option above), else
  // the instance's (a plain SET of this option is GLOBAL in DuckDB, so an administrator's
  // `SET arrow_lossless_conversion = true` reaches every session that has none of its own).
  // Read from what a result does, the value that matters whatever scope set it (in this
  // DuckDB, current_setting() read right after a SQL SET SESSION did not always agree).
  // The SDK keeps the session document's options and reads them again after this
  // session's SET/USE/ATTACH statements and option changes, so another session's
  // SET GLOBAL shows at the next of those.
  const auto schema = QueryArrow(connection_.get(), "SELECT NULL::HUGEINT AS v")->schema();
  return schema->num_fields() == 1 && schema->field(0)->type()->id() != arrow::Type::DECIMAL128;
}

void DuckDBSession::Commit() { EndTransaction("COMMIT"); }

void DuckDBSession::Rollback() { EndTransaction("ROLLBACK"); }

void DuckDBSession::EndTransaction(std::string_view sql) {
  if (read_only_) {
    if (transaction_ == chargewire::TransactionState::kNone) {
      throw Error(ErrorCode::kExecutionError,
                  std::string("TransactionContext Error: cannot ") + (sql == "COMMIT" ? "commit" : "rollback") +
                      " - no transaction is active");
    }
    EndReadOnlyTransaction(sql == "COMMIT");
    transaction_ = autocommit_ ? chargewire::TransactionState::kNone : chargewire::TransactionState::kOpen;
    return;
  }
  BeginOwedTransaction();
  RunSql(connection_.get(), sql);
  transaction_ = chargewire::TransactionState::kNone;
  if (!autocommit_) {
    RunSql(connection_.get(), "BEGIN TRANSACTION");  // the next statement starts a new one
    transaction_ = chargewire::TransactionState::kOpen;
  }
  work_in_transaction_ = false;
}

void DuckDBSession::Ran(duckdb_v2_sql_statement_handle statement) {
  switch (TransactionEffectOf(connection_.get(), statement)) {
    case TransactionEffect::kBegin:
      transaction_ = chargewire::TransactionState::kOpen;
      work_in_transaction_ = false;
      break;
    case TransactionEffect::kCommit:
    case TransactionEffect::kRollback:
      transaction_ = chargewire::TransactionState::kNone;
      transaction_owed_ = !autocommit_;
      work_in_transaction_ = false;
      break;
    case TransactionEffect::kNone:
      work_in_transaction_ = true;
      break;
  }
}

void DuckDBSession::BeginOwedTransaction() {
  if (!std::exchange(transaction_owed_, false)) return;
  RunSql(connection_.get(), "BEGIN TRANSACTION");
  transaction_ = chargewire::TransactionState::kOpen;
  work_in_transaction_ = false;
}

chargewire::TransactionState DuckDBSession::Transaction() {
  if (read_only_) {
    // The SDK asks after every piece of work, once its rows are released: the READ ONLY
    // transaction that work ran in ends here unless it is the client's own.
    if (transaction_ == chargewire::TransactionState::kNone) {
      EndReadOnlyTransaction(true);
    } else if (transaction_ == chargewire::TransactionState::kOpen && read_only_transaction_open_ &&
               TransactionAborted(connection_.get())) {
      transaction_ = chargewire::TransactionState::kAborted;
    }
    return transaction_;
  }
  BeginOwedTransaction();
  if (transaction_ == chargewire::TransactionState::kOpen && TransactionAborted(connection_.get())) {
    transaction_ = chargewire::TransactionState::kAborted;
  }
  return transaction_;
}

// ---- Read-only sessions (spec §5.8; duckdb_read_only.hpp) ----

namespace {

std::string StatementText(duckdb_v2_sql_statement_handle statement) {
  duckdb_v2_str text{};
  Call([&](auto* e) { return duckdb_v2_sql_statement_get_text(statement, &text, e); });
  return ToString(text);
}

std::string Lower(std::string_view text) {
  std::string out(text);
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return out;
}

bool HasParenthesis(const SqlToken& token) {
  return token.type == DUCKDB_V2_TOKEN_TYPE_OPERATOR && token.text.find_first_of("()") != std::string::npos;
}

}  // namespace

void DuckDBSession::SetReadOnly() {
  if (read_only_) return;
  if (transaction_ != chargewire::TransactionState::kNone) {
    // DuckDB cannot turn an open read-write transaction into a read-only one, and whether
    // it has written is not for the adapter to guess: the client ends it first. One in
    // which nothing has run (autocommit just went off, or a commit just started the next)
    // holds nothing, and is replaced by a READ ONLY one.
    if (work_in_transaction_) {
      throw Error(ErrorCode::kInvalidOptionValue,
                  "read_only cannot be set while a transaction with work in it is open: commit or roll back first");
    }
    RunSql(connection_.get(), "ROLLBACK");
  }
  transaction_owed_ = false;
  read_only_transaction_open_ = false;
  if (!autocommit_) transaction_ = chargewire::TransactionState::kOpen;
  read_only_ = true;
}

void DuckDBSession::BeginWork() {
  if (!read_only_) {
    BeginOwedTransaction();
    return;
  }
  if (transaction_ == chargewire::TransactionState::kNone) EndReadOnlyTransaction(true);
  EnsureReadOnlyTransaction();
}

void DuckDBSession::EnsureReadOnlyTransaction() {
  if (read_only_transaction_open_) return;
  try {
    RunSql(connection_.get(), "BEGIN TRANSACTION READ ONLY");
  } catch (const Error&) {
    // DuckDB had one open that the adapter did not know of: nothing of a read-only session
    // is lost with it, and the statement must not run in it.
    RunSql(connection_.get(), "ROLLBACK");
    RunSql(connection_.get(), "BEGIN TRANSACTION READ ONLY");
  }
  read_only_transaction_open_ = true;
}

void DuckDBSession::EndReadOnlyTransaction(bool commit) {
  if (!read_only_transaction_open_) return;
  try {
    RunSql(connection_.get(), commit ? "COMMIT" : "ROLLBACK");
  } catch (const Error&) {
    // e.g. the commit of an aborted transaction. Whatever DuckDB did with it, none stays
    // open that the adapter believes closed or the other way round.
    try {
      RunSql(connection_.get(), "ROLLBACK");
    } catch (const Error&) {
    }
  }
  read_only_transaction_open_ = false;
}

void DuckDBSession::CheckReadOnlyKind(duckdb_v2_sql_statement_handle statement) const {
  const DUCKDB_V2_STATEMENT_TYPE kind = StatementType(statement);
  if (!read_only_policy_->statement_kinds.contains(kind)) throw ReadOnlyRefusal(StatementKindName(kind));
}

DuckDBSession::Gated DuckDBSession::GateReadOnly(duckdb_v2_sql_statement_handle statement,
                                                 const BoundValues& bound) {
  CheckReadOnlyKind(statement);
  switch (StatementType(statement)) {
    case DUCKDB_V2_STATEMENT_TYPE_TRANSACTION:
      RunReadOnlyTransactionStatement(statement);
      return {.handled = true, .replacement = {}};
    case DUCKDB_V2_STATEMENT_TYPE_SET:
      return {.handled = false, .replacement = GateReadOnlySet(statement)};
    case DUCKDB_V2_STATEMENT_TYPE_PRAGMA:
      GateReadOnlyPragma(statement);
      return {};
    case DUCKDB_V2_STATEMENT_TYPE_EXPLAIN:
      GateReadOnlyExplain(statement, bound);
      return {};
    default:
      CheckReadOnlyPlan(statement, bound);
      return {};
  }
}

// The client's transaction statements never reach DuckDB in a read-only session: the
// adapter starts every transaction itself, READ ONLY, so no statement can open one that
// writes, and a COMMIT followed by a write finds the write in a READ ONLY transaction too.
void DuckDBSession::RunReadOnlyTransactionStatement(duckdb_v2_sql_statement_handle statement) {
  using chargewire::TransactionState;
  const TransactionEffect effect = TransactionEffectOf(connection_.get(), statement);
  if (effect == TransactionEffect::kBegin) {
    for (const SqlToken& token : SqlTokens(connection_.get(), StatementText(statement))) {
      if (token.Is("WRITE")) throw ReadOnlyRefusal("a read-write transaction");
    }
    if (transaction_ != TransactionState::kNone) {
      throw Error(ErrorCode::kExecutionError,
                  "TransactionContext Error: cannot start a transaction within a transaction");
    }
    EnsureReadOnlyTransaction();
    transaction_ = TransactionState::kOpen;
    return;
  }
  if (effect != TransactionEffect::kCommit && effect != TransactionEffect::kRollback) {
    throw ReadOnlyRefusal("this transaction statement");
  }
  const bool commit = effect == TransactionEffect::kCommit;
  if (transaction_ == TransactionState::kNone) {
    throw Error(ErrorCode::kExecutionError, std::string("TransactionContext Error: cannot ") +
                                                (commit ? "commit" : "rollback") + " - no transaction is active");
  }
  EndReadOnlyTransaction(commit);
  transaction_ = autocommit_ ? TransactionState::kNone : TransactionState::kOpen;
}

// USE, SET VARIABLE, and SET / RESET of a setting the policy lists, which is applied at
// session scope whatever the statement wrote. The words are DuckDB's tokens; a SET that is
// not exactly one of these shapes is refused.
std::string DuckDBSession::GateReadOnlySet(duckdb_v2_sql_statement_handle statement) {
  const std::string text = StatementText(statement);
  const std::vector<SqlToken> tokens = SqlTokens(connection_.get(), text);
  const Error shape = ReadOnlyRefusal("this form of SET");
  if (tokens.empty()) throw shape;
  if (tokens[0].Is("USE")) return {};
  const bool pragma = tokens[0].Is("PRAGMA");  // PRAGMA name = value is a SET
  const bool reset = tokens[0].Is("RESET");
  if (!pragma && !reset && !tokens[0].Is("SET")) throw shape;
  std::size_t i = 1;
  std::string scope;
  if (!pragma && i < tokens.size()) {
    for (const char* word : {"SESSION", "LOCAL", "GLOBAL", "VARIABLE"}) {
      if (tokens[i].Is(word)) scope = word;
    }
    if (!scope.empty()) i++;
  }
  if (i >= tokens.size() || (tokens[i].type != DUCKDB_V2_TOKEN_TYPE_IDENTIFIER &&
                             tokens[i].type != DUCKDB_V2_TOKEN_TYPE_KEYWORD)) {
    throw shape;
  }
  const SqlToken& name = tokens[i++];
  if (scope == "GLOBAL") throw ReadOnlyRefusal("SET GLOBAL");
  // After the name: nothing (RESET), or = / TO and a value.
  std::string value;
  if (reset) {
    if (i != tokens.size()) throw shape;
  } else {
    if (i >= tokens.size() || !(tokens[i].IsOperator("=") || tokens[i].Is("TO"))) throw shape;
    if (++i >= tokens.size()) throw shape;
    value = text.substr(tokens[i].start, tokens.back().end - tokens[i].start);
  }
  if (scope == "VARIABLE") {
    // The session's own variable. Its value may be a query: that query is held to the plan
    // check as any other.
    if (!reset) {
      auto query = Parse(connection_.get(), "SELECT " + value);
      if (query.size() != 1 || StatementType(query[0].get()) != DUCKDB_V2_STATEMENT_TYPE_SELECT) throw shape;
      CheckReadOnlyPlan(query[0].get(), BoundValues{});
    }
    return {};
  }
  if (!read_only_policy_->settings.contains(Lower(name.text))) {
    throw ReadOnlyRefusal("SET " + name.text.substr(0, 64));
  }
  if (reset) return "RESET SESSION " + name.text;
  // A setting's value is a constant or a word: nothing in it may call or query.
  for (std::size_t v = i; v < tokens.size(); v++) {
    if (HasParenthesis(tokens[v])) throw shape;
  }
  return "SET SESSION " + name.text + " = " + value;
}

void DuckDBSession::GateReadOnlyPragma(duckdb_v2_sql_statement_handle statement) {
  // COPY FROM DATABASE and IMPORT DATABASE are of the PRAGMA kind too: only PRAGMA <name>.
  const std::vector<SqlToken> tokens = SqlTokens(connection_.get(), StatementText(statement));
  if (tokens.size() < 2 || !tokens[0].Is("PRAGMA") ||
      (tokens[1].type != DUCKDB_V2_TOKEN_TYPE_IDENTIFIER && tokens[1].type != DUCKDB_V2_TOKEN_TYPE_KEYWORD)) {
    throw ReadOnlyRefusal("this statement");
  }
  if (!read_only_policy_->pragmas.contains(Lower(tokens[1].text))) {
    throw ReadOnlyRefusal("PRAGMA " + tokens[1].text.substr(0, 64));
  }
}

// EXPLAIN x and EXPLAIN ANALYZE x run when x may run: EXPLAIN ANALYZE executes x, and
// planning x is what the plan check does anyway.
void DuckDBSession::GateReadOnlyExplain(duckdb_v2_sql_statement_handle statement, const BoundValues& bound) {
  const std::string text = StatementText(statement);
  const std::vector<SqlToken> tokens = SqlTokens(connection_.get(), text);
  const Error shape = ReadOnlyRefusal("this form of EXPLAIN");
  if (tokens.empty() || !tokens[0].Is("EXPLAIN")) throw shape;
  std::size_t i = 1;
  if (i < tokens.size() && tokens[i].Is("ANALYZE")) i++;
  if (i < tokens.size() && tokens[i].type == DUCKDB_V2_TOKEN_TYPE_OPERATOR && tokens[i].text.starts_with("(")) {
    // the option list: to its closing parenthesis
    int depth = 0;
    for (; i < tokens.size(); i++) {
      if (tokens[i].type != DUCKDB_V2_TOKEN_TYPE_OPERATOR) continue;
      for (const char c : tokens[i].text) depth += c == '(' ? 1 : c == ')' ? -1 : 0;
      if (depth <= 0) break;
    }
    if (depth != 0) throw shape;
    i++;
  }
  if (i >= tokens.size()) throw shape;
  auto inner = Parse(connection_.get(), text.substr(tokens[i].start));
  if (inner.size() != 1) throw shape;
  const DUCKDB_V2_STATEMENT_TYPE kind = StatementType(inner[0].get());
  if (kind != DUCKDB_V2_STATEMENT_TYPE_SELECT && kind != DUCKDB_V2_STATEMENT_TYPE_MULTI) {
    throw ReadOnlyRefusal("EXPLAIN of " + std::string(StatementKindName(kind)));
  }
  CheckReadOnlyKind(inner[0].get());
  CheckReadOnlyPlan(inner[0].get(), bound);
}

void DuckDBSession::CheckReadOnlyPlan(duckdb_v2_sql_statement_handle statement, const BoundValues& bound) {
  const Error unreadable = ReadOnlyRefusal("a statement whose plan the server cannot read");
  auto explain = Parse(connection_.get(), "EXPLAIN (FORMAT JSON) " + StatementText(statement));
  if (explain.size() != 1) throw unreadable;
  const auto handles = bound.Handles();
  const auto names = bound.NameHandles();
  ArrowResult result;
  Call([&](auto* err) {
    return duckdb_v2_statement_execute_arrow(connection_.get(), explain[0].get(),
                                             names.empty() ? nullptr : names.data(),
                                             handles.empty() ? nullptr : handles.data(), handles.size(), 0,
                                             result.out(), err);
  });
  auto outcome = Outcome(result, nullptr);
  if (!outcome.rows) throw unreadable;
  auto table = outcome.rows->ToTable();
  outcome.rows.reset();
  if (!table.ok() || (*table)->num_columns() < 2) throw unreadable;
  std::optional<std::string> plan;
  for (int64_t row = 0; row < (*table)->num_rows(); row++) {
    auto key = (*table)->column(0)->GetScalar(row);
    auto value = (*table)->column(1)->GetScalar(row);
    if (key.ok() && value.ok() && (*key)->is_valid && (*value)->is_valid && (*key)->ToString() == "physical_plan") {
      plan = (*value)->ToString();
    }
  }
  if (!plan) throw unreadable;
  const std::vector<std::string> refused = PlanNamesNotAllowed(*plan, *read_only_policy_);
  if (!refused.empty()) {
    throw Error(ErrorCode::kReadOnly,
                refused.front() + " is not on this server's list of what a read-only session may run: a read-only "
                                  "session runs only the operations and table functions the server knows to read",
                "25006");
  }
}

}  // namespace chargewire_duckdb
