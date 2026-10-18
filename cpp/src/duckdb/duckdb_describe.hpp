// A query's result schema without running it (spec §7.1: a prepare describes its
// result). duckdb_v2_statement_bind gives the result columns' DuckDB types without
// executing anything; DuckDB's Arrow exporter turns them into the Arrow schema a result
// would carry, under the session's settings (arrow_lossless_conversion, string and
// list views, large buffers, the Arrow format version, TimeZone). The exporter needs a
// client context, which the v2 C API hands out only inside DuckDB's callbacks, so the
// conversion runs in the bind callback of a table function, warp_result_schema(),
// bound (never executed) on the session's own connection.
#pragma once

#include <arrow/type.h>

#include <memory>

#include "duckdb_v2.hpp"

namespace warpduckdb {

// Registers warp_result_schema() on the instance. Call once, on any connection.
void RegisterResultSchemaFunction(duckdb_v2_connection_handle connection);

// The Arrow schema of a result with `columns` (an output schema from
// duckdb_v2_statement_bind), exactly as `connection` would send it. Runs nothing.
std::shared_ptr<arrow::Schema> ResultArrowSchema(duckdb_v2_connection_handle connection,
                                                 duckdb_v2_schema_handle columns);

}  // namespace warpduckdb
