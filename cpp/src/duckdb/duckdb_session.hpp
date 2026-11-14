// One Chargewire session on its own DuckDB connection.
#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "duckdb_upload.hpp"
#include "duckdb_read_only.hpp"
#include "duckdb_v2.hpp"
#include "duckdb_values.hpp"
#include "chargewire/backend.hpp"

namespace chargewire_duckdb {

class DuckDBSession : public chargewire::EngineSession {
 public:
  // `batch_rows`: most rows per Arrow batch in results (0: DuckDB's default).
  explicit DuckDBSession(duckdb_v2_instance_handle instance, std::size_t batch_rows = 0);
  ~DuckDBSession() override;

  chargewire::StatementOutcome Execute(std::string_view sql,
                                    const boost::json::value& parameters) override;
  std::unique_ptr<chargewire::PreparedStatement> Prepare(std::string_view sql) override;
  chargewire::StatementOutcome ExecutePrepared(chargewire::PreparedStatement& statement,
                                            const chargewire::Parameters& parameters) override;
  int64_t Ingest(const chargewire::IngestTarget& target,
                 std::shared_ptr<arrow::RecordBatchReader> rows) override;
  std::shared_ptr<arrow::RecordBatchReader> Metadata(chargewire::MetadataKind kind,
                                                     const chargewire::MetadataFilter& filter) override;

  void Interrupt() override;
  std::optional<double> Progress() override;
  // Read-only sessions (spec §5.8; duckdb_read_only.hpp says how DuckDB is held to it).
  void SetReadOnly() override;
  chargewire::OptionErrors SetOptions(const boost::json::object& options) override;
  boost::json::object Options() override;
  bool TakeOptionsChanged() override { return std::exchange(options_changed_, false); }
  void Commit() override;
  void Rollback() override;
  chargewire::TransactionState Transaction() override;

 protected:
  duckdb_v2_connection_handle connection() const { return connection_.get(); }

  // Whether SetReadOnly() made this session read-only.
  bool read_only() const { return read_only_; }
  // The lists a read-only session is held to (ReadOnlyPolicy::Default() unless set). A server
  // that embeds this adapter adds the table functions it knows to read, or its own settings.
  void set_read_only_policy(std::shared_ptr<const ReadOnlyPolicy> policy) { read_only_policy_ = std::move(policy); }
  const ReadOnlyPolicy& read_only_policy() const { return *read_only_policy_; }
  // The plan check of a read-only session: called, inside the session's READ ONLY
  // transaction, for every statement that is about to run and whose kind lets it read (a
  // query, the statement of an EXPLAIN, a SET VARIABLE's value as a query), with the values
  // it will run with. Throws read_only unless the statement only reads. The default plans
  // the statement with EXPLAIN (FORMAT JSON) and refuses a plan with an operator or a table
  // function the policy does not list, which costs a second planning of every statement. A
  // server that sees DuckDB's bound plan overrides this with its own check.
  virtual void CheckReadOnlyPlan(duckdb_v2_sql_statement_handle statement, const BoundValues& bound);
  // The field metadata keys that carry a column's default in table schemas
  // (metadata/tables?include_schema=true; Flight SQL has none). A server may add the key
  // its older clients read; none: not reported.
  virtual std::vector<std::string> ColumnDefaultMetadataKeys() const { return {"CHARGEWIRE:SQL:COLUMN_DEFAULT"}; }
  // Rows of the tables metadata (catalog, schema, table, type) with each table's schema
  // added (include_schema), so a server that leaves some tables out (spec §9.4) can
  // describe only the ones it keeps.
  std::shared_ptr<arrow::RecordBatchReader> TablesWithSchemas(
      std::shared_ptr<arrow::RecordBatchReader> tables);

 private:
  // What the gate of a read-only session decided for one statement.
  struct Gated {
    bool handled = false;        // the adapter ran it itself (a transaction statement)
    std::string replacement;     // run this instead (a SET, at session scope); empty: the statement
  };
  // Holds one statement to the read-only policy before it runs: its kind, then what its
  // kind needs (the plan, the setting's name, the pragma's name). Throws read_only.
  Gated GateReadOnly(duckdb_v2_sql_statement_handle statement, const BoundValues& bound);
  // The kind alone: what Prepare() can tell before the values are there.
  void CheckReadOnlyKind(duckdb_v2_sql_statement_handle statement) const;
  std::string GateReadOnlySet(duckdb_v2_sql_statement_handle statement);
  void GateReadOnlyPragma(duckdb_v2_sql_statement_handle statement);
  void GateReadOnlyExplain(duckdb_v2_sql_statement_handle statement, const BoundValues& bound);
  void RunReadOnlyTransactionStatement(duckdb_v2_sql_statement_handle statement);
  // Before any work in the session. Read-write: starts the transaction a SQL COMMIT or
  // ROLLBACK left owed. Read-only: ends the READ ONLY transaction the last work ran in, if
  // the client has none open, and starts the one this work runs in.
  void BeginWork();
  void EnsureReadOnlyTransaction();
  // For the adapter's own queries (the session's options): ends, when it goes, the READ ONLY
  // transaction that was started for them, so a session in manual commit holds no
  // transaction, and no snapshot, before its client has run anything in it.
  class OwnReadOnlyWork {
   public:
    explicit OwnReadOnlyWork(DuckDBSession& session)
        : session_(session), was_open_(session.read_only_transaction_open_) {}
    ~OwnReadOnlyWork() {
      if (session_.read_only_ && !was_open_) session_.EndReadOnlyTransaction(true);
    }

   private:
    DuckDBSession& session_;
    bool was_open_;
  };
  // Ends the read-only transaction DuckDB has open, if any; a commit that fails is a rollback.
  void EndReadOnlyTransaction(bool commit);

  void EndTransaction(std::string_view sql);
  // Notes a transaction statement (BEGIN, COMMIT, ...) that just ran. With autocommit off
  // a SQL COMMIT or ROLLBACK starts the next transaction, as the commit and rollback
  // calls do (spec §5.5), so the session never leaves manual-commit mode by accident.
  void Ran(duckdb_v2_sql_statement_handle statement);
  // Starts the transaction a SQL COMMIT or ROLLBACK left owed (Ran()), once the statement's
  // result is gone: DuckDB runs one result at a time on a connection.
  void BeginOwedTransaction();
  // The arrow_lossless_conversion value this session's results use (its own, else the
  // instance's).
  bool Lossless();

  Connection connection_;
  idx_t batch_rows_ = 0;
  duckdb_v2_factory_handle factory_ = nullptr;
  std::unique_ptr<ValueFactory> values_;
  bool autocommit_ = true;
  // The transaction as the statements that ran left it (spec §5.2); Transaction() asks
  // DuckDB whether an open one has been aborted.
  chargewire::TransactionState transaction_ = chargewire::TransactionState::kNone;
  bool transaction_owed_ = false;  // autocommit off, and a SQL COMMIT/ROLLBACK just ended one
  // Read-only (SetReadOnly): one-way, never cleared.
  bool read_only_ = false;
  std::shared_ptr<const ReadOnlyPolicy> read_only_policy_;
  // Read-only: whether DuckDB has a transaction open now. Only the adapter starts and ends
  // DuckDB's transactions then, always READ ONLY, so this is exact.
  bool read_only_transaction_open_ = false;
  // Read-write: a statement ran in the transaction that is open (SetReadOnly refuses then).
  bool work_in_transaction_ = false;
  boost::json::value session_tag_ = nullptr;
  bool options_changed_ = false;  // a statement ran that can move the current catalog/schema
};

}  // namespace chargewire_duckdb
