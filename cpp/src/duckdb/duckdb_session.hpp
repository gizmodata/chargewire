// One Warp SQL session on its own DuckDB connection.
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "duckdb_upload.hpp"
#include "duckdb_v2.hpp"
#include "duckdb_values.hpp"
#include "warpsql/backend.hpp"

namespace warpduckdb {

class DuckDBSession : public warpsql::EngineSession {
 public:
  // `batch_rows`: most rows per Arrow batch in results (0: DuckDB's default).
  explicit DuckDBSession(duckdb_v2_instance_handle instance, std::size_t batch_rows = 0);
  ~DuckDBSession() override;

  warpsql::StatementOutcome Execute(std::string_view sql,
                                    const boost::json::value& parameters) override;
  std::unique_ptr<warpsql::PreparedStatement> Prepare(std::string_view sql) override;
  warpsql::StatementOutcome ExecutePrepared(warpsql::PreparedStatement& statement,
                                            const warpsql::Parameters& parameters) override;
  int64_t Ingest(const warpsql::IngestTarget& target,
                 std::shared_ptr<arrow::RecordBatchReader> rows) override;
  std::shared_ptr<arrow::RecordBatchReader> Metadata(warpsql::MetadataKind kind,
                                                     const warpsql::MetadataFilter& filter) override;

  void Interrupt() override;
  std::optional<double> Progress() override;
  warpsql::OptionErrors SetOptions(const boost::json::object& options) override;
  boost::json::object Options() override;
  bool TakeOptionsChanged() override { return std::exchange(options_changed_, false); }
  void Commit() override;
  void Rollback() override;

 protected:
  duckdb_v2_connection_handle connection() const { return connection_.get(); }
  // The field metadata keys that carry a column's default in table schemas
  // (metadata/tables?include_schema=true; Flight SQL has none). A server may add the key
  // its older clients read; none: not reported.
  virtual std::vector<std::string> ColumnDefaultMetadataKeys() const { return {"WARP:SQL:COLUMN_DEFAULT"}; }
  // Rows of the tables metadata (catalog, schema, table, type) with each table's schema
  // added (include_schema), so a server that leaves some tables out (spec §9.4) can
  // describe only the ones it keeps.
  std::shared_ptr<arrow::RecordBatchReader> TablesWithSchemas(
      std::shared_ptr<arrow::RecordBatchReader> tables);

 private:
  void EndTransaction(std::string_view sql);

  Connection connection_;
  idx_t batch_rows_ = 0;
  duckdb_v2_factory_handle factory_ = nullptr;
  std::unique_ptr<ValueFactory> values_;
  bool autocommit_ = true;
  bool lossless_ = false;
  boost::json::value session_tag_ = nullptr;
  bool options_changed_ = false;  // a statement ran that can move the current catalog/schema
};

}  // namespace warpduckdb
