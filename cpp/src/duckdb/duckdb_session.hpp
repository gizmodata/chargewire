// One Warp SQL session on its own DuckDB connection.
#pragma once

#include <memory>
#include <utility>

#include "duckdb_upload.hpp"
#include "duckdb_v2.hpp"
#include "duckdb_values.hpp"
#include "warpsql/backend.hpp"

namespace warpduckdb {

class DuckDBSession : public warpsql::EngineSession {
 public:
  explicit DuckDBSession(duckdb_v2_instance_handle instance);
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

 private:
  void EndTransaction(std::string_view sql);
  std::shared_ptr<arrow::RecordBatchReader> TablesWithSchemas(
      std::shared_ptr<arrow::RecordBatchReader> tables);

  Connection connection_;
  duckdb_v2_factory_handle factory_ = nullptr;
  std::unique_ptr<ValueFactory> values_;
  bool autocommit_ = true;
  bool lossless_ = false;
  boost::json::value session_tag_ = nullptr;
  bool options_changed_ = false;  // a statement ran that can move the current catalog/schema
};

}  // namespace warpduckdb
