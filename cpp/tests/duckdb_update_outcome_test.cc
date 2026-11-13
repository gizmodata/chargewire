// What update_outcome_test.cc plays, with DuckDB: `CREATE TABLE ... AS`, INSERT, SET, USE,
// BEGIN and COMMIT, each started over every path with the engine thread held after the
// statement's work (update_outcome.hpp). No document carries an update result before the
// statement has succeeded (spec §6.2), and a cancel or timeout that reaches DuckDB once the
// statement's change is made leaves the statement succeeded, the change in place and the
// session working (spec §6.8).
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "duckdb_backend.hpp"
#include "duckdb_session.hpp"
#include "chargewire/server.hpp"
#include "update_outcome.hpp"

namespace {

namespace json = boost::json;

using update_outcome::CancelBy;
using update_outcome::Gate;
using update_outcome::Kind;
using update_outcome::Path;
using update_outcome::Wire;

class HeldSession : public chargewire_duckdb::DuckDBSession {
 public:
  HeldSession(duckdb_v2_instance_handle instance, std::size_t batch_rows, std::shared_ptr<Gate> gate)
      : DuckDBSession(instance, batch_rows), gate_(std::move(gate)) {}

  chargewire::StatementOutcome Execute(std::string_view sql, const json::value& parameters) override {
    return Work([&] { return DuckDBSession::Execute(sql, parameters); });
  }
  chargewire::StatementOutcome ExecutePrepared(chargewire::PreparedStatement& statement,
                                               const chargewire::Parameters& parameters) override {
    return Work([&] { return DuckDBSession::ExecutePrepared(statement, parameters); });
  }
  void Cancel(std::string_view reason) override {
    gate_->Cancelled(reason);
    DuckDBSession::Cancel(reason);
  }
  json::object Options() override {
    gate_->Hold();
    return DuckDBSession::Options();
  }
  bool TakeOptionsChanged() override {
    gate_->Hold();
    return DuckDBSession::TakeOptionsChanged();
  }

 private:
  template <class Run>
  chargewire::StatementOutcome Work(Run&& run) {
    gate_->WorkStarted();
    try {
      chargewire::StatementOutcome outcome = run();
      if (outcome.rows) {
        gate_->NothingToHold();
      } else {
        gate_->WorkDone();
      }
      return outcome;
    } catch (...) {
      gate_->NothingToHold();
      throw;
    }
  }

  std::shared_ptr<Gate> gate_;
};

class HeldDuckDB : public chargewire_duckdb::DuckDBBackend {
 public:
  explicit HeldDuckDB(std::shared_ptr<Gate> gate) : DuckDBBackend(Options{}), gate_(std::move(gate)) {}

 protected:
  std::unique_ptr<chargewire::EngineSession> NewSession(const chargewire::Principal&, const std::string&) override {
    return std::make_unique<HeldSession>(instance(), result_batch_rows(), gate_);
  }

 private:
  std::shared_ptr<Gate> gate_;
};

const json::object kOneRow{{"kind", "update"}, {"rows_affected", 1}};
const json::object kNoCount{{"kind", "update"}};

const std::vector<Kind>& Kinds() {
  static const std::vector<Kind> kinds = {
      // The statement of issue gizmodata/hardcharger#42.
      {"CTAS", "CREATE TABLE notes_{n} AS SELECT 1 AS id, 'hello' AS body", kOneRow, ""},
      {"INSERT", "INSERT INTO held VALUES ({n})", kOneRow, ""},
      {"SET", "SET threads = 2", kNoCount, ""},
      {"USE", "USE memory.main", kNoCount, ""},
      {"BEGIN", "BEGIN", kNoCount, "open"},
      {"COMMIT", "COMMIT", kNoCount, "none"},
  };
  return kinds;
}

class DuckDBUpdateOutcome : public ::testing::Test {
 protected:
  void SetUp() override {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(options, std::make_shared<HeldDuckDB>(gate_),
                                                   std::make_shared<update_outcome::AnyPassword>());
    server_->Start();
    wire_ = Wire::Open(server_->port());
    wire_.Run("CREATE TABLE held (v INTEGER)");
  }
  void TearDown() override {
    gate_->Disarm();
    server_->Stop();
  }
  // The rows in `held`: an UPDATE of every row counts them.
  int64_t RowsHeld() {
    const json::object doc = wire_.Run("UPDATE held SET v = v");
    return doc.at("result").as_object().at("rows_affected").to_number<int64_t>();
  }

  std::shared_ptr<Gate> gate_ = std::make_shared<Gate>();
  std::unique_ptr<chargewire::Server> server_;
  Wire wire_;
};

TEST_F(DuckDBUpdateOutcome, NoPathShowsAnUpdateResultBeforeTheStatementHasSucceeded) {
  int inserts = 0;
  for (const Path path : {Path::kStreamedSubmit, Path::kStreamedPrepared, Path::kStatusSubmit, Path::kRespondAsync}) {
    for (const Kind& kind : Kinds()) {
      update_outcome::ExpectUpdateOnlyWithSucceeded(wire_, *gate_, kind, path);
      if (kind.name == "INSERT") inserts += 1;
    }
  }
  EXPECT_EQ(RowsHeld(), inserts);
}

TEST_F(DuckDBUpdateOutcome, ACancellationAfterTheWorkIsDoneChangesNothing) {
  int inserts = 0;
  for (const CancelBy by : {CancelBy::kStatementCancel, CancelBy::kSessionCancel, CancelBy::kTimeout}) {
    for (const Kind& kind : Kinds()) {
      // Each timeout is waited out: the INSERT shows it.
      if (by == CancelBy::kTimeout && kind.name != "INSERT") continue;
      update_outcome::ExpectALateCancelChangesNothing(wire_, *gate_, kind, by);
      if (kind.name == "INSERT") inserts += 1;
      // DuckDB was interrupted with nothing running: the session's next statement is not
      // the one that pays for it, and its transaction is where the statements left it.
      EXPECT_EQ(wire_.Run("SELECT 42").at("result").as_object().at("kind"), json::value("rows"));
      if (!kind.transaction.empty()) {
        EXPECT_EQ(wire_.Session().at("transaction"), json::value(kind.transaction)) << kind.name;
      }
    }
  }
  // Every INSERT whose answer said one row is there.
  EXPECT_EQ(RowsHeld(), inserts);
}

}  // namespace
