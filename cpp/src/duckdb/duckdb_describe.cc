#include "duckdb_describe.hpp"

#include <arrow/c/bridge.h>

#include <string>
#include <vector>

#include "duckdb_sql.hpp"

namespace warpduckdb {

using warpsql::Error;
using warpsql::ErrorCode;

namespace {

constexpr std::string_view kFunction = "warp_result_schema";

using Schema = Owned<duckdb_v2_schema_handle, duckdb_v2_schema_destroy>;
using TableFunction = Owned<duckdb_v2_table_function_handle, duckdb_v2_table_function_destroy>;
using ArrowExporter = Owned<duckdb_v2_arrow_exporter_handle, duckdb_v2_arrow_exporter_destroy>;

// The description in progress: the columns to convert, and the Arrow schema made.
struct Slot {
  duckdb_v2_schema_handle columns = nullptr;  // borrowed
  ArrowSchema schema{};
  ~Slot() {
    if (schema.release != nullptr) schema.release(&schema);
  }
};

// DuckDB binds a statement on the calling thread, and ResultArrowSchema() binds its
// one statement on its own: this is the description that bind may make. Any other
// statement, on any other thread (a user calling warp_result_schema()), finds none.
thread_local Slot* current_slot = nullptr;

// Callbacks run inside DuckDB: report errors through the slot DuckDB hands over, never
// by letting an exception escape.
template <typename Fn>
void Guarded(duckdb_v2_error_info_handle* err, Fn&& fn) {
  try {
    fn();
  } catch (const std::exception& e) {
    duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
    const duckdb_v2_str text = Str(e.what());
    duckdb_v2_error_info_set_text(*err, &text);
  }
}

// Converts the slot's columns with DuckDB's own exporter: the conversion, settings and
// extension types (UUID, JSON, HUGEINT under lossless, ...) every executed result uses
// (both build the schema with ArrowConverter::ToArrowSchema from the context's
// GetClientProperties()). The function itself returns one column and no rows.
void Bind(duckdb_v2_function_bind_info_handle, duckdb_v2_table_function_bind_info_handle result,
          duckdb_v2_context_handle context, duckdb_v2_error_info_handle* err) {
  Guarded(err, [&] {
    Slot* slot = current_slot;
    if (slot == nullptr || slot->columns == nullptr) {
      throw Error(ErrorCode::kSqlError, "warp_result_schema() only describes a prepared statement's result");
    }
    idx_t count = 0;
    Call([&](auto* e) { return duckdb_v2_schema_get_count(slot->columns, &count, e); });
    std::vector<duckdb_v2_logical_type_handle> types(count);  // borrowed from the schema
    std::vector<duckdb_v2_str> names(count);
    for (idx_t i = 0; i < count; i++) {
      Call([&](auto* e) { return duckdb_v2_schema_get_field(slot->columns, i, &names[i], &types[i], e); });
    }
    ArrowExporter exporter;
    Call([&](auto* e) {
      return duckdb_v2_arrow_exporter_create(context, types.data(), names.data(), count, 0,
                                             exporter.out(), e);
    });
    if (slot->schema.release != nullptr) slot->schema.release(&slot->schema);  // bound again
    Call([&](auto* e) { return duckdb_v2_arrow_exporter_get_schema(exporter.get(), &slot->schema, e); });
    if (count == 0) {
      throw Error(ErrorCode::kExecutionError, "warp_result_schema(): a result without columns");
    }
    const duckdb_v2_identifier_t column = Str("described");
    Call([&](auto* e) { return duckdb_v2_table_function_bind_add_result_column(result, &column, types[0], e); });
  });
}

void Exec(duckdb_v2_table_function_exec_info_handle, duckdb_v2_context_handle,
          duckdb_v2_error_info_handle*) {
  // No rows (nothing ever executes it: ResultArrowSchema() only binds it).
}

}  // namespace

void RegisterResultSchemaFunction(duckdb_v2_connection_handle connection) {
  duckdb_v2_factory_handle factory = nullptr;
  Call([&](auto* err) { return duckdb_v2_connection_get_factory(connection, &factory, err); });
  TableFunction function;
  Call([&](auto* err) { return duckdb_v2_table_function_create(factory, function.out(), err); });
  const duckdb_v2_str name = Str(kFunction);
  Call([&](auto* err) { return duckdb_v2_table_function_set_name(function.get(), &name, err); });
  Call([&](auto* err) { return duckdb_v2_table_function_set_bind_callback(function.get(), Bind, err); });
  Call([&](auto* err) { return duckdb_v2_table_function_set_exec_callback(function.get(), Exec, err); });
  Call([&](auto* err) {
    return duckdb_v2_connection_register_table_function(connection, function.get(), err);
  });
}

std::shared_ptr<arrow::Schema> ResultArrowSchema(duckdb_v2_connection_handle connection,
                                                 duckdb_v2_schema_handle columns) {
  static const std::string sql = "SELECT * FROM " + std::string(kFunction) + "()";
  auto statements = Parse(connection, sql);
  Slot slot;
  slot.columns = columns;
  {
    struct Scope {
      explicit Scope(Slot* slot) { current_slot = slot; }
      ~Scope() { current_slot = nullptr; }
    } scope(&slot);
    Schema output;
    Call([&](auto* err) {
      return duckdb_v2_statement_bind(connection, statements.at(0).get(), output.out(), nullptr, err);
    });
  }
  if (slot.schema.release == nullptr) {
    throw Error(ErrorCode::kExecutionError, "warp_result_schema() was not bound");
  }
  auto schema = arrow::ImportSchema(&slot.schema);  // takes the schema over
  if (!schema.ok()) {
    throw Error(ErrorCode::kExecutionError, schema.status().message());
  }
  return std::move(*schema);
}

}  // namespace warpduckdb
