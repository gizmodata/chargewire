// A DuckDB database seen the way a Chargewire server sees it: SQL in, Arrow streams
// out, Arrow streams in for bulk ingest. Uses only DuckDB's v2 C API.
#pragma once

#include <nanoarrow/nanoarrow.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "duckdb_v2.hpp"

namespace chargewire {

enum class IngestMode {
  kCreate,        // fail if the table exists
  kAppend,        // fail if it does not
  kReplace,       // drop and recreate
  kCreateAppend,  // create if missing, then append
};

// The stream a bulk ingest is reading, shared with the chargewire_upload() table function.
struct Upload;

class Database {
 public:
  // `path` is a database file, or ":memory:".
  explicit Database(const std::string& path);
  ~Database();
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  // Runs every statement in `sql`, discarding any rows.
  void Execute(std::string_view sql);

  // Runs one statement that returns rows. Each array holds at most `batch_rows`
  // rows (0: DuckDB's default).
  nanoarrow::UniqueArrayStream Query(std::string_view sql, int64_t batch_rows = 0);

  // Writes every row of `rows` into `table` in one statement, and returns how many.
  int64_t Ingest(std::string_view table, IngestMode mode, ArrowArrayStream* rows);

 private:
  SqlStatement ParseOne(std::string_view sql);
  bool TableExists(std::string_view table);
  void RegisterUploadFunction();

  Environment environment_;
  Instance instance_;
  Connection connection_;
  std::unique_ptr<Upload> upload_;
};

}  // namespace chargewire
