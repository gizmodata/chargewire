// Running SQL on a v2 connection: parse, execute, drain, read a single value.
#pragma once

#include <arrow/record_batch.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "duckdb_v2.hpp"
#include "warpsql/backend.hpp"

namespace warpduckdb {

std::vector<SqlStatement> Parse(duckdb_v2_connection_handle connection, std::string_view sql);

// Runs a parsed statement to its end, discarding rows; returns rows changed.
idx_t RunToEnd(duckdb_v2_connection_handle connection, duckdb_v2_sql_statement_handle statement);

// Runs every statement in `sql`, discarding rows.
void RunSql(duckdb_v2_connection_handle connection, std::string_view sql);

// Runs `sql` with bound values and returns all rows as one reader.
std::shared_ptr<arrow::RecordBatchReader> QueryArrow(
    duckdb_v2_connection_handle connection, std::string_view sql,
    const std::vector<std::string>& names = {},
    const std::vector<duckdb_v2_value_handle>& values = {});

// The first column of the first row, as text ("" for none or NULL).
std::string QueryText(duckdb_v2_connection_handle connection, std::string_view sql);

// The outcome of an executed Arrow result: rows, or the count of rows changed. Takes
// over `result`; `statement` is kept alive as long as the rows are.
warpsql::StatementOutcome Outcome(ArrowResult& result, std::shared_ptr<void> keep_alive);

// "a"."b"."c" from the given parts, skipping empty ones, quoted as DuckDB needs.
std::string QualifiedName(const std::vector<std::string>& parts);

}  // namespace warpduckdb
