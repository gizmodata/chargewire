// Bulk ingest into DuckDB (spec §8.1): the upload is read by a table function,
// warp_upload(), so an ingest is one SQL statement (atomic), and creating the table
// takes DuckDB's own type mapping (CREATE TABLE … AS SELECT * FROM warp_upload()).
#pragma once

#include <arrow/record_batch.h>

#include <memory>

#include "duckdb_v2.hpp"
#include "warpsql/backend.hpp"

namespace warpduckdb {

// Registers warp_upload() on the instance. Call once, on any connection.
void RegisterUploadFunction(duckdb_v2_connection_handle connection);

// Writes every row of `rows` into `target` on `connection`, in one statement, and
// returns how many.
int64_t Ingest(duckdb_v2_connection_handle connection, const warpsql::IngestTarget& target,
               std::shared_ptr<arrow::RecordBatchReader> rows);

}  // namespace warpduckdb
