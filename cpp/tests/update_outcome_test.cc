// A statement without rows gets its result and its end in one step (spec §6.2): no request
// finds `"state": "running"` with an update result, and a cancellation or timeout that comes
// once the engine has done the work changes nothing (spec §6.8). A server started in this
// process, with an engine that plays statements and stops after each one's work until the
// test lets it go (update_outcome.hpp says why).
#include <gtest/gtest.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>

#include "chargewire/server.hpp"
#include "update_outcome.hpp"

namespace {

namespace json = boost::json;

using chargewire::Error;
using chargewire::ErrorCode;
using chargewire::Principal;
using update_outcome::CancelBy;
using update_outcome::Gate;
using update_outcome::Kind;
using update_outcome::Path;
using update_outcome::Wire;

// What the played statements did, for the test to read.
struct Played {
  Gate gate;
  std::mutex mutex;
  std::condition_variable changed;
  int writes = 0;       // "ctas" and "insert" statements whose work was done
  bool cancel = false;  // Cancel() came: "slow" stops
};

class NoRows : public chargewire::PreparedStatement {
 public:
  explicit NoRows(std::string sql) : sql(std::move(sql)) {}
  std::shared_ptr<arrow::Schema> ParameterSchema() override { return arrow::schema({}); }
  std::shared_ptr<arrow::Schema> ResultSchema() override { return nullptr; }
  std::optional<bool> ReturnsRows() override { return false; }
  const std::string sql;
};

// Plays a statement by its first word: "ctas" and "insert" change one row, "set" changes a
// session option, "begin" and "commit" the transaction, and "slow" works until it is
// cancelled.
class PlayedSession : public chargewire::EngineSession {
 public:
  explicit PlayedSession(std::shared_ptr<Played> played) : played_(std::move(played)) {}

  chargewire::StatementOutcome Execute(std::string_view sql, const json::value&) override {
    played_->gate.WorkStarted();
    chargewire::StatementOutcome outcome;
    if (sql.starts_with("slow")) {
      std::unique_lock lock(played_->mutex);
      played_->changed.wait_for(lock, update_outcome::kPatience, [&] { return played_->cancel; });
      played_->gate.NothingToHold();
      throw Error(ErrorCode::kExecutionError, "interrupted");
    }
    if (sql.starts_with("ctas") || sql.starts_with("insert")) {
      std::lock_guard lock(played_->mutex);
      played_->writes += 1;
      outcome.rows_affected = 1;
    } else if (sql.starts_with("set")) {
      options_["played_option"] = std::string(sql);
      options_changed_ = true;
    } else if (sql.starts_with("begin")) {
      transaction_ = chargewire::TransactionState::kOpen;
    } else if (sql.starts_with("commit")) {
      transaction_ = chargewire::TransactionState::kNone;
    } else {
      played_->gate.NothingToHold();
      throw Error(ErrorCode::kSqlError, "this engine doesn't play that");
    }
    played_->gate.WorkDone();
    return outcome;
  }
  std::unique_ptr<chargewire::PreparedStatement> Prepare(std::string_view sql) override {
    return std::make_unique<NoRows>(std::string(sql));
  }
  chargewire::StatementOutcome ExecutePrepared(chargewire::PreparedStatement& statement,
                                               const chargewire::Parameters&) override {
    return Execute(static_cast<NoRows&>(statement).sql, nullptr);
  }
  void Cancel(std::string_view reason) override {
    played_->gate.Cancelled(reason);
    {
      std::lock_guard lock(played_->mutex);
      played_->cancel = true;
    }
    played_->changed.notify_all();
  }
  chargewire::OptionErrors SetOptions(const json::object&) override { return {}; }
  json::object Options() override {
    played_->gate.Hold();
    return options_;
  }
  bool TakeOptionsChanged() override {
    played_->gate.Hold();
    return std::exchange(options_changed_, false);
  }
  chargewire::TransactionState Transaction() override { return transaction_; }

 private:
  std::shared_ptr<Played> played_;
  json::object options_;
  bool options_changed_ = false;
  chargewire::TransactionState transaction_ = chargewire::TransactionState::kNone;
};

class PlayedEngine : public chargewire::Backend {
 public:
  explicit PlayedEngine(std::shared_ptr<Played> played) : played_(std::move(played)) {}
  chargewire::ServerIdentity Identity() override { return {"update-outcome-test", "0"}; }
  chargewire::BackendCapabilities Capabilities() override { return {.prepared_statements = true}; }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const Principal&, const std::string&,
                                                         const json::object&) override {
    return std::make_unique<PlayedSession>(played_);
  }

 private:
  std::shared_ptr<Played> played_;
};

const std::vector<Kind>& Kinds() {
  static const std::vector<Kind> kinds = {
      {"CTAS", "ctas {n}", json::object{{"kind", "update"}, {"rows_affected", 1}}, ""},
      {"INSERT", "insert {n}", json::object{{"kind", "update"}, {"rows_affected", 1}}, ""},
      {"SET", "set {n}", json::object{{"kind", "update"}}, ""},
      {"BEGIN", "begin", json::object{{"kind", "update"}}, "open"},
      {"COMMIT", "commit", json::object{{"kind", "update"}}, "none"},
  };
  return kinds;
}

class UpdateOutcome : public ::testing::Test {
 protected:
  void SetUp() override {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(options, std::make_shared<PlayedEngine>(played_),
                                                   std::make_shared<update_outcome::AnyPassword>());
    server_->Start();
    wire_ = Wire::Open(server_->port());
  }
  void TearDown() override {
    played_->gate.Disarm();
    server_->Stop();
  }
  int Writes() {
    std::lock_guard lock(played_->mutex);
    return played_->writes;
  }

  std::shared_ptr<Played> played_ = std::make_shared<Played>();
  std::unique_ptr<chargewire::Server> server_;
  Wire wire_;
};

// Issue gizmodata/hardcharger#42: a streamed `CREATE TABLE ... AS` answered
// {"state":"running","result":{"kind":"update","rows_affected":1}}.
TEST_F(UpdateOutcome, NoPathShowsAnUpdateResultBeforeTheStatementHasSucceeded) {
  for (const Path path : {Path::kStreamedSubmit, Path::kStreamedPrepared, Path::kStatusSubmit, Path::kRespondAsync}) {
    for (const Kind& kind : Kinds()) {
      update_outcome::ExpectUpdateOnlyWithSucceeded(wire_, played_->gate, kind, path);
    }
  }
}

TEST_F(UpdateOutcome, ACancellationAfterTheWorkIsDoneChangesNothing) {
  int expected_writes = 0;
  for (const CancelBy by : {CancelBy::kStatementCancel, CancelBy::kSessionCancel, CancelBy::kTimeout}) {
    for (const Kind& kind : Kinds()) {
      update_outcome::ExpectALateCancelChangesNothing(wire_, played_->gate, kind, by);
      if (kind.result.as_object().contains("rows_affected")) expected_writes += 1;
      if (by == CancelBy::kTimeout) break;  // each waits out its timeout: one kind shows it
    }
  }
  EXPECT_EQ(Writes(), expected_writes);
  // The session goes on: the next statement runs as usual.
  EXPECT_EQ(wire_.Run("insert after the cancels").at("result"),
            (json::object{{"kind", "update"}, {"rows_affected", 1}}));
}

// The other side: a cancellation that reaches the engine while it works stops the work,
// and the statement is cancelled, as before.
TEST_F(UpdateOutcome, ACancellationWhileTheEngineWorksStillCancels) {
  update_outcome::Started started = update_outcome::Start(wire_, {"slow", "slow", nullptr, ""}, Path::kRespondAsync);
  const update_outcome::Answer accepted = started.answer.get();
  ASSERT_EQ(accepted.status, 201) << accepted.body;
  const std::string id(accepted.Json().at("statement_id").as_string());
  const auto deadline = std::chrono::steady_clock::now() + update_outcome::kPatience;
  while (Wire::StateOf(wire_.Status(id)) != "running") {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "the statement never started";
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  EXPECT_EQ(wire_.Post(wire_.session + "/statements/" + id + "/cancel", "").status, 202);
  json::object doc = wire_.Status(id, {{"Prefer", "wait=30"}});
  while (!update_outcome::Terminal(Wire::StateOf(doc))) {
    ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "the statement never ended";
    doc = wire_.Status(id, {{"Prefer", "wait=30"}});
  }
  EXPECT_EQ(Wire::StateOf(doc), "cancelled") << json::serialize(doc);
  EXPECT_EQ(doc.at("cancel_reason"), json::value("client"));
  EXPECT_TRUE(doc.at("result").is_null());
}

}  // namespace
