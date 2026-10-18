#include "duckdb_session.hpp"

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/ipc/writer.h>

#include <algorithm>
#include <map>

#include "duckdb_describe.hpp"
#include "duckdb_sql.hpp"

namespace warpduckdb {

namespace json = boost::json;
using warpsql::Error;
using warpsql::ErrorCode;
using warpsql::MetadataKind;

namespace {

using PreparedHandle =
    Owned<duckdb_v2_prepared_statement_handle, duckdb_v2_prepared_statement_destroy>;

// What executing a prepared statement returns, learned before it ever runs.
struct Described {
  std::optional<bool> returns_rows;
  std::shared_ptr<arrow::Schema> schema;  // when it returns rows
};

class DuckDBPrepared : public warpsql::PreparedStatement {
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

// A write, described by binding it (read-only, even inside the session's transaction):
// without RETURNING DuckDB reports one BIGINT column named Count, the changed rows.
// With RETURNING it returns rows, whose Arrow schema binding doesn't give.
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
  idx_t count = 0;
  Call([&](auto* e) { return duckdb_v2_schema_get_count(output.get(), &count, e); });
  if (count == 1) {
    duckdb_v2_identifier_t name{};
    duckdb_v2_logical_type_handle type = nullptr;  // borrowed from the schema
    Call([&](auto* e) { return duckdb_v2_schema_get_field(output.get(), 0, &name, &type, e); });
    DUCKDB_V2_LOGICAL_TYPE_ID id{};
    Call([&](auto* e) { return duckdb_v2_logical_type_get_id(type, &id, e); });
    if (ToString(name) == "Count" && id == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT) {
      return Described{false, nullptr};
    }
  }
  return Described{true, nullptr};
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
  Call([&](auto* err) { return duckdb_v2_connection_create(instance, connection_.out(), err); });
  Call([&](auto* err) { return duckdb_v2_connection_get_factory(connection_.get(), &factory_, err); });
  values_ = std::make_unique<ValueFactory>(factory_);
  // DuckDB's default Arrow mapping, as GizmoSQL 1.x uses: HUGEINT (every integer SUM)
  // arrives as decimal128(38, 0), which clients can compute with. The lossless mode
  // keeps UUID, JSON and TIMETZ exact but turns HUGEINT into opaque bytes; a session can
  // opt in with the arrow_lossless_conversion option.
}

DuckDBSession::~DuckDBSession() {
  connection_.reset();
}

// ---- Statements ----

warpsql::StatementOutcome DuckDBSession::Execute(std::string_view sql,
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
  for (std::size_t i = 0; i + 1 < statements.size(); i++) {
    RunToEnd(connection_.get(), statements[i].get());
  }
  auto last = std::make_shared<SqlStatement>(std::move(statements.back()));

  const ParameterTypes types =
      HasNull(parameters) ? ParameterTypes::Load(connection_.get(), last->get()) : ParameterTypes();
  const BoundValues bound = values_->FromJson(parameters, types);
  const auto handles = bound.Handles();
  const auto names = bound.NameHandles();
  ArrowResult result;
  Call([&](auto* err) {
    return duckdb_v2_statement_execute_arrow(connection_.get(), last->get(),
                                             names.empty() ? nullptr : names.data(),
                                             handles.empty() ? nullptr : handles.data(),
                                             handles.size(), batch_rows_, result.out(), err);
  });
  return Outcome(result, last);
}

std::unique_ptr<warpsql::PreparedStatement> DuckDBSession::Prepare(std::string_view sql) {
  auto statements = Parse(connection_.get(), sql);
  if (statements.size() != 1) {
    throw Error(ErrorCode::kSqlError, "a prepared statement is exactly one SQL statement");
  }
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


warpsql::StatementOutcome DuckDBSession::ExecutePrepared(warpsql::PreparedStatement& statement,
                                                         const warpsql::Parameters& parameters) {
  auto& prepared = static_cast<DuckDBPrepared&>(statement);
  auto run = [&](const BoundValues& bound) {
    const auto handles = bound.Handles();
    const auto names = bound.NameHandles();
    ArrowResult result;
    Call([&](auto* err) {
      return duckdb_v2_prepared_statement_execute_arrow(
          prepared.prepared(), names.empty() ? nullptr : names.data(),
          handles.empty() ? nullptr : handles.data(), handles.size(), batch_rows_, result.out(), err);
    });
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
  warpsql::StatementOutcome outcome;
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

int64_t DuckDBSession::Ingest(const warpsql::IngestTarget& target,
                              std::shared_ptr<arrow::RecordBatchReader> rows) {
  return warpduckdb::Ingest(connection_.get(), target, std::move(rows));
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
  const auto schema = warpsql::MetadataSchema(MetadataKind::kTypeInfo);
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

std::shared_ptr<arrow::RecordBatchReader> DuckDBSession::Metadata(
    MetadataKind kind, const warpsql::MetadataFilter& filter) {
  if (kind == MetadataKind::kTypeInfo) {
    return TypeInfo(filter.data_type);
  }
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
      sql = "SELECT catalog_name, schema_name AS db_schema_name "
            "FROM information_schema.schemata "
            "WHERE ($catalog IS NULL OR catalog_name = $catalog) "
            "AND ($schema IS NULL OR schema_name LIKE $schema ESCAPE '\\') ORDER BY 1, 2";
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
      sql = "SELECT table_catalog AS catalog_name, table_schema AS db_schema_name, table_name, "
            "table_type FROM information_schema.tables "
            "WHERE ($catalog IS NULL OR table_catalog = $catalog) "
            "AND ($schema IS NULL OR table_schema LIKE $schema ESCAPE '\\') "
            "AND ($table IS NULL OR table_name LIKE $table ESCAPE '\\') "
            "AND ($types IS NULL OR list_contains(string_split($types, ','), table_type)) "
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
      sql = "SELECT catalog_name, db_schema_name, table_name, column_name, key_name, "
            "CAST(key_sequence AS INTEGER) AS key_sequence FROM ("
            "SELECT database_name AS catalog_name, schema_name AS db_schema_name, table_name, "
            "unnest(constraint_column_names) AS column_name, constraint_name AS key_name, "
            "unnest(range(1, len(constraint_column_names) + 1)) AS key_sequence "
            "FROM duckdb_constraints() WHERE constraint_type = 'PRIMARY KEY' "
            "AND ($table IS NULL OR table_name = $table) "
            "AND ($catalog IS NULL OR database_name = $catalog) "
            "AND ($schema IS NULL OR schema_name = $schema)) ORDER BY 1, 2, 3, 5, 6";
      break;
    case MetadataKind::kImportedKeys:
    case MetadataKind::kExportedKeys:
    case MetadataKind::kCrossReference: {
      std::string where;
      if (kind == MetadataKind::kImportedKeys) {
        text("catalog", filter.catalog);
        text("schema", filter.db_schema);
        text("table", filter.table);
        where = "($table IS NULL OR table_name = $table) "
                "AND ($catalog IS NULL OR database_name = $catalog) "
                "AND ($schema IS NULL OR schema_name = $schema)";
      } else if (kind == MetadataKind::kExportedKeys) {
        text("catalog", filter.catalog);
        text("schema", filter.db_schema);
        text("table", filter.table);
        where = "referenced_table = $table AND ($catalog IS NULL OR database_name = $catalog) "
                "AND ($schema IS NULL OR schema_name = $schema)";
      } else {
        text("pk_table", filter.table);
        text("fk_table", filter.fk_table);
        text("pk_catalog", filter.catalog);
        text("pk_schema", filter.db_schema);
        where = "referenced_table = $pk_table AND table_name = $fk_table "
                "AND ($pk_catalog IS NULL OR database_name = $pk_catalog) "
                "AND ($pk_schema IS NULL OR schema_name = $pk_schema)";
      }
      const std::string order =
          kind == MetadataKind::kImportedKeys ? "1, 2, 3, 11, 9" : "5, 6, 7, 10, 9";
      sql = "SELECT pk_catalog_name, pk_db_schema_name, pk_table_name, pk_column_name, "
            "fk_catalog_name, fk_db_schema_name, fk_table_name, fk_column_name, "
            "CAST(key_sequence AS INTEGER) AS key_sequence, fk_key_name, "
            "NULL::VARCHAR AS pk_key_name, CAST(3 AS UTINYINT) AS update_rule, "
            "CAST(3 AS UTINYINT) AS delete_rule FROM ("
            "SELECT database_name AS pk_catalog_name, schema_name AS pk_db_schema_name, "
            "referenced_table AS pk_table_name, "
            "unnest(referenced_column_names) AS pk_column_name, "
            "database_name AS fk_catalog_name, schema_name AS fk_db_schema_name, "
            "table_name AS fk_table_name, unnest(constraint_column_names) AS fk_column_name, "
            "unnest(range(1, len(constraint_column_names) + 1)) AS key_sequence, "
            "constraint_name AS fk_key_name FROM duckdb_constraints() "
            "WHERE constraint_type = 'FOREIGN KEY' AND " + where + ") ORDER BY " + order;
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
    // The table's Arrow schema, with the Flight SQL per-field metadata drivers read.
    auto table_schema = QueryArrow(connection_.get(), "SELECT * FROM " +
                                                          QualifiedName({catalog, schema, table}) +
                                                          " LIMIT 0")
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
          "SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale, "
          "column_default, numeric_precision_radix, COLUMN_COMMENT "
          "FROM information_schema.columns WHERE table_catalog = $c "
          "AND table_schema = $s AND table_name = $t ORDER BY ordinal_position",
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

warpsql::OptionErrors DuckDBSession::SetOptions(const json::object& options) {
  warpsql::OptionErrors errors;
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
    RunSql(connection_.get(), "USE " + QualifiedName({text(value, "catalog")}));
  });
  apply("schema", [&](const json::value& value) {
    const std::string catalog = QueryText(connection_.get(), "SELECT current_database()");
    RunSql(connection_.get(), "USE " + QualifiedName({catalog, text(value, "schema")}));
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
  apply("arrow_lossless_conversion", [&](const json::value& value) {
    if (!value.is_bool()) {
      throw Error(ErrorCode::kInvalidOptionValue, "arrow_lossless_conversion must be true or false");
    }
    RunSql(connection_.get(), value.as_bool() ? "SET SESSION arrow_lossless_conversion = true"
                                              : "SET SESSION arrow_lossless_conversion = false");
  });
  for (const auto& option : options) {
    const std::string_view key = option.key();
    if (key != "catalog" && key != "schema" && key != "autocommit" && key != "session_tag" &&
        key != "arrow_lossless_conversion") {
      errors.emplace(std::string(key), Error(ErrorCode::kUnknownOption, "unknown session option"));
    }
  }
  return errors;
}

json::object DuckDBSession::Options() {
  json::object options{
      {"catalog", QueryText(connection_.get(), "SELECT current_database()")},
      {"schema", QueryText(connection_.get(), "SELECT current_schema()")},
      {"autocommit", autocommit_},
      {"arrow_lossless_conversion", Lossless()},
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
  RunSql(connection_.get(), sql);
  if (!autocommit_) {
    RunSql(connection_.get(), "BEGIN TRANSACTION");  // the next statement starts a new one
  }
}

}  // namespace warpduckdb
