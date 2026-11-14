#include "duckdb_read_only.hpp"

#include <boost/json.hpp>

#include <algorithm>
#include <cctype>

namespace chargewire_duckdb {

namespace json = boost::json;
using chargewire::Error;
using chargewire::ErrorCode;

namespace {

std::string Upper(std::string_view text) {
  std::string out(text);
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return out;
}

using TokenIterator = Owned<duckdb_v2_token_iterator_handle, duckdb_v2_token_iterator_destroy>;

}  // namespace

ReadOnlyPolicy ReadOnlyPolicy::Default() {
  ReadOnlyPolicy policy;
  policy.statement_kinds = {
      DUCKDB_V2_STATEMENT_TYPE_SELECT,       // queries; also DESCRIBE, SHOW, SUMMARIZE, VALUES
      DUCKDB_V2_STATEMENT_TYPE_EXPLAIN,      // of a statement that may itself run
      DUCKDB_V2_STATEMENT_TYPE_TRANSACTION,  // run by the adapter: BEGIN, COMMIT, ROLLBACK
      DUCKDB_V2_STATEMENT_TYPE_SET,          // USE, SET VARIABLE, and the settings below
      DUCKDB_V2_STATEMENT_TYPE_PRAGMA,       // the pragmas below
      // A PIVOT whose values come from the data: DuckDB expands it into a temporary enum type
      // and a query. The one statement of a read-only session that leaves something in its
      // temporary catalog; its plan is checked like any query's.
      DUCKDB_V2_STATEMENT_TYPE_MULTI,
  };
  policy.settings = {
      // how values are read, compared, ordered and shown
      "timezone", "calendar", "default_collation", "default_null_order", "null_order", "default_order",
      "integer_division", "ieee_floating_point_ops", "error_on_division_by_zero", "order_by_non_integer_literal",
      "scalar_subquery_error_on_multiple_rows", "preserve_identifier_case", "disable_timestamptz_casts",
      "errors_as_json", "max_expression_depth", "pivot_limit", "pivot_filter_threshold",
      // where unqualified names resolve
      "search_path", "schema",
      // the Arrow shape of results
      "arrow_lossless_conversion", "arrow_large_buffer_size", "arrow_output_list_view", "arrow_output_version",
      "binary_as_string",
      // progress, and how a query is planned
      "enable_progress_bar", "enable_progress_bar_print", "progress_bar_time", "preserve_insertion_order",
      "ordered_aggregate_threshold", "perfect_ht_threshold", "prefer_range_joins", "merge_join_threshold",
      "nested_loop_join_threshold", "asof_loop_join_threshold",
  };
  policy.pragmas = {
      "table_info", "show", "show_tables", "show_tables_expanded", "show_databases", "database_list",
      "database_size", "storage_info", "metadata_info", "version", "platform", "functions", "collations",
      "user_agent", "extension_versions",
  };
  policy.plan_operators = {
      "SEQ_SCAN", "DUMMY_SCAN", "CHUNK_SCAN", "COLUMN_DATA_SCAN", "DELIM_SCAN", "EXPRESSION_SCAN", "POSITIONAL_SCAN",
      "EMPTY_RESULT", "ORDER_BY", "LIMIT", "LIMIT_PERCENT", "STREAMING_LIMIT", "RESERVOIR_SAMPLE",
      "STREAMING_SAMPLE", "TOP_N", "WINDOW", "STREAMING_WINDOW", "UNNEST", "UNGROUPED_AGGREGATE",
      "HASH_GROUP_BY", "PERFECT_HASH_GROUP_BY", "PARTITIONED_AGGREGATE", "LIMITED_DISTINCT", "FILTER",
      "PROJECTION", "LEFT_DELIM_JOIN", "RIGHT_DELIM_JOIN", "BLOCKWISE_NL_JOIN", "NESTED_LOOP_JOIN", "HASH_JOIN",
      "PIECEWISE_MERGE_JOIN", "IE_JOIN", "ASOF_JOIN", "CROSS_PRODUCT", "POSITIONAL_JOIN", "UNION",
      "REC_CTE", "REC_KEY_CTE", "CTE", "REC_CTE_SCAN", "REC_REC_CTE_SCAN", "CTE_SCAN", "RECURSIVE_KEY_JOIN",
      "PIVOT", "INOUT_FUNCTION", "EXPLAIN", "EXPLAIN_ANALYZE", "RESULT_COLLECTOR",
  };
  policy.table_functions = {
      // generators
      "RANGE", "GENERATE_SERIES", "UNNEST", "REPEAT", "REPEAT_ROW",
      // files, local or in object storage (the same functions read https:// and s3:// paths)
      "READ_CSV", "READ_CSV_AUTO", "SNIFF_CSV", "READ_PARQUET", "PARQUET_SCAN", "READ_JSON", "READ_JSON_AUTO",
      "READ_JSON_OBJECTS", "READ_JSON_OBJECTS_AUTO", "READ_NDJSON", "READ_NDJSON_AUTO", "READ_NDJSON_OBJECTS",
      "READ_TEXT", "READ_BLOB", "GLOB", "READ_DUCKDB", "READ_AVRO", "READ_XLSX",
      "PARQUET_METADATA", "PARQUET_FILE_METADATA", "PARQUET_FULL_METADATA", "PARQUET_KV_METADATA",
      "PARQUET_SCHEMA", "PARQUET_BLOOM_PROBE",
      // JSON
      "JSON_EACH", "JSON_TREE",
      // the catalog (information_schema and pg_catalog are views over these)
      "DUCKDB_COLUMNS", "DUCKDB_CONSTRAINTS", "DUCKDB_DATABASES", "DUCKDB_DEPENDENCIES", "DUCKDB_EXTENSIONS",
      "DUCKDB_FUNCTIONS", "DUCKDB_INDEXES", "DUCKDB_KEYWORDS", "DUCKDB_OPTIMIZERS", "DUCKDB_SCHEMAS",
      "DUCKDB_SEQUENCES", "DUCKDB_SETTINGS", "DUCKDB_TABLES", "DUCKDB_TRIGGERS", "DUCKDB_TYPES", "DUCKDB_VARIABLES",
      "DUCKDB_VIEWS", "DUCKDB_COORDINATE_SYSTEMS", "DUCKDB_DIALECTS", "DUCKDB_SECRET_TYPES",
      "DUCKDB_SECRET_TYPE_PARAMETERS", "DUCKDB_PREPARED_STATEMENTS", "DUCKDB_TEMPORARY_FILES", "DUCKDB_MEMORY",
      "PRAGMA_COLLATIONS", "PRAGMA_DATABASE_SIZE", "PRAGMA_METADATA_INFO", "PRAGMA_PLATFORM", "PRAGMA_SHOW",
      "PRAGMA_STORAGE_INFO", "PRAGMA_TABLE_INFO", "PRAGMA_USER_AGENT", "PRAGMA_VERSION", "SUMMARY",
      "SQL_AUTO_COMPLETE", "ICU_CALENDAR_NAMES", "PG_TIMEZONE_NAMES",
      // attached catalogs of other systems, scanned through DuckDB SQL (their pass-through
      // functions, which send SQL text to the other system, are deliberately not here)
      "POSTGRES_SCAN", "POSTGRES_SCAN_PUSHDOWN", "SQLITE_SCAN", "MYSQL_SCAN",
      // lakes
      "DUCKLAKE_SCAN", "DUCKLAKE_SNAPSHOTS", "DUCKLAKE_TABLE_INFO", "DUCKLAKE_LIST_FILES",
      "DUCKLAKE_TABLE_INSERTIONS", "DUCKLAKE_TABLE_DELETIONS", "DUCKLAKE_TABLE_CHANGES",
      "DUCKLAKE_CURRENT_SNAPSHOT", "DUCKLAKE_LAST_COMMITTED_SNAPSHOT", "DUCKLAKE_OPTIONS", "DUCKLAKE_SETTINGS",
      "ICEBERG_SCAN", "ICEBERG_METADATA", "ICEBERG_SNAPSHOTS", "DELTA_SCAN",
      // spatial files
      "ST_READ", "ST_READ_META", "ST_READOSM", "ST_READSHP", "SHAPEFILE_META", "ST_DRIVERS",
  };
  return policy;
}

std::string_view StatementKindName(DUCKDB_V2_STATEMENT_TYPE kind) {
  switch (kind) {
#define KIND(x) case DUCKDB_V2_STATEMENT_TYPE_##x: return #x;
    KIND(SELECT) KIND(INSERT) KIND(UPDATE) KIND(CREATE) KIND(DELETE) KIND(PREPARE) KIND(EXECUTE)
    KIND(ALTER) KIND(TRANSACTION) KIND(COPY) KIND(ANALYZE) KIND(VARIABLE_SET) KIND(CREATE_FUNC)
    KIND(EXPLAIN) KIND(DROP) KIND(EXPORT) KIND(PRAGMA) KIND(VACUUM) KIND(CALL) KIND(SET) KIND(LOAD)
    KIND(RELATION) KIND(EXTENSION) KIND(LOGICAL_PLAN) KIND(ATTACH) KIND(DETACH) KIND(MULTI)
    KIND(COPY_DATABASE) KIND(UPDATE_EXTENSIONS) KIND(MERGE_INTO) KIND(CONNECT) KIND(DISCONNECT)
    KIND(EXTERNAL_RESOURCE) KIND(PASSTHROUGH)
#undef KIND
    default:
      return "this kind of statement";
  }
}

chargewire::Error ReadOnlyRefusal(std::string_view what) {
  return Error(ErrorCode::kReadOnly, std::string(what) + " is not allowed in a read-only session", "25006");
}

bool SqlToken::Is(std::string_view word) const {
  if (type != DUCKDB_V2_TOKEN_TYPE_KEYWORD && type != DUCKDB_V2_TOKEN_TYPE_IDENTIFIER) return false;
  return text.size() == word.size() &&
         std::equal(text.begin(), text.end(), word.begin(), [](unsigned char a, unsigned char b) {
           return std::toupper(a) == std::toupper(b);
         });
}

std::vector<SqlToken> SqlTokens(duckdb_v2_connection_handle connection, std::string_view sql) {
  const std::string text(sql);
  const duckdb_v2_str str = Str(text);
  TokenIterator iterator;
  Call([&](auto* e) { return duckdb_v2_tokenize_sql(connection, &str, iterator.out(), e); });
  std::vector<SqlToken> tokens;
  for (;;) {
    DUCKDB_V2_TOKEN_TYPE type = DUCKDB_V2_TOKEN_TYPE_INVALID;
    idx_t start = 0;
    idx_t length = 0;
    Call([&](auto* e) { return duckdb_v2_token_iterator_next(iterator.get(), &type, &start, &length, e); });
    if (type == DUCKDB_V2_TOKEN_TYPE_END_OF_INPUT) return tokens;
    if (type == DUCKDB_V2_TOKEN_TYPE_COMMENT || type == DUCKDB_V2_TOKEN_TYPE_TERMINATOR) continue;
    tokens.push_back({type, text.substr(start, length), start, start + length});
  }
}

namespace {

void CollectNotAllowed(const json::value& node, const ReadOnlyPolicy& policy, std::vector<std::string>& out) {
  if (node.is_array()) {
    for (const auto& item : node.as_array()) CollectNotAllowed(item, policy, out);
    return;
  }
  if (!node.is_object()) throw ReadOnlyRefusal("a statement whose plan the server cannot read");
  const json::object& object = node.as_object();
  const json::value* name = object.if_contains("name");
  if (name == nullptr || !name->is_string()) throw ReadOnlyRefusal("a statement whose plan the server cannot read");
  auto check = [&](std::string_view raw) {
    std::string upper = Upper(raw);
    while (!upper.empty() && upper.back() == ' ') upper.pop_back();
    if (!policy.plan_operators.contains(upper) && !policy.table_functions.contains(upper) &&
        std::find(out.begin(), out.end(), upper) == out.end()) {
      out.push_back(std::move(upper));
    }
  };
  check(name->as_string());
  if (const json::value* extra = object.if_contains("extra_info"); extra != nullptr && extra->is_object()) {
    if (const json::value* function = extra->as_object().if_contains("Function");
        function != nullptr && function->is_string()) {
      check(function->as_string());
    }
  }
  if (const json::value* children = object.if_contains("children"); children != nullptr) {
    CollectNotAllowed(*children, policy, out);
  }
}

}  // namespace

std::vector<std::string> PlanNamesNotAllowed(std::string_view plan_json, const ReadOnlyPolicy& policy) {
  boost::system::error_code ec;
  const json::value plan = json::parse(plan_json, ec);
  if (ec) throw ReadOnlyRefusal("a statement whose plan the server cannot read");
  std::vector<std::string> out;
  CollectNotAllowed(plan, policy, out);
  return out;
}

}  // namespace chargewire_duckdb
