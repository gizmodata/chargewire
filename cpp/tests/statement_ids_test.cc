// EngineSession::SetStatementId: the engine is told the id the client holds for what it
// runs (a statement's q_..., a prepared statement's p_...), and untold after. A server
// started in this process with an engine that records what it was told.
#include <gtest/gtest.h>

#include <boost/json.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "warpsql/client.hpp"
#include "warpsql/server.hpp"

namespace {

namespace json = boost::json;

using warpsql::Error;
using warpsql::ErrorCode;
using warpsql::Principal;

// What the engine saw: each call with the id it had been told at the time.
struct Seen {
  std::mutex mutex;
  std::vector<std::string> calls;  // "execute q_...", "prepare p_...", "execute_prepared q_..."
  std::string last_told = "(never)";
  void Add(const std::string& call) {
    std::lock_guard lock(mutex);
    calls.push_back(call);
  }
};

class NoRows : public warpsql::PreparedStatement {
 public:
  std::shared_ptr<arrow::Schema> ParameterSchema() override { return arrow::schema({}); }
  std::shared_ptr<arrow::Schema> ResultSchema() override { return nullptr; }
  std::optional<bool> ReturnsRows() override { return false; }
};

class RecordingSession : public warpsql::EngineSession {
 public:
  explicit RecordingSession(std::shared_ptr<Seen> seen) : seen_(std::move(seen)) {}
  void SetStatementId(std::string_view id) override {
    id_ = std::string(id);
    std::lock_guard lock(seen_->mutex);
    seen_->last_told = id_;
  }
  warpsql::StatementOutcome Execute(std::string_view sql, const json::value&) override {
    if (sql == "fail") throw Error(ErrorCode::kSqlError, "no such thing");
    seen_->Add("execute " + id_);
    return {.rows_affected = 0};
  }
  std::unique_ptr<warpsql::PreparedStatement> Prepare(std::string_view) override {
    seen_->Add("prepare " + id_);
    return std::make_unique<NoRows>();
  }
  warpsql::StatementOutcome ExecutePrepared(warpsql::PreparedStatement&, const warpsql::Parameters&) override {
    seen_->Add("execute_prepared " + id_);
    return {.rows_affected = 0};
  }

 private:
  std::shared_ptr<Seen> seen_;
  std::string id_;
};

class RecordingEngine : public warpsql::Backend {
 public:
  explicit RecordingEngine(std::shared_ptr<Seen> seen) : seen_(std::move(seen)) {}
  warpsql::ServerIdentity Identity() override { return {"statement-ids-test", "0"}; }
  warpsql::BackendCapabilities Capabilities() override { return {.prepared_statements = true}; }
  std::unique_ptr<warpsql::EngineSession> OpenSession(const Principal&, const std::string&,
                                                      const json::object&) override {
    return std::make_unique<RecordingSession>(seen_);
  }

 private:
  std::shared_ptr<Seen> seen_;
};

class AnyPassword : public warpsql::Authenticator {
 public:
  Principal AuthenticatePassword(std::string_view username, std::string_view) override {
    return Principal{.name = std::string(username), .role = "admin"};
  }
  json::array GrantTypes() override { return {"password"}; }
};

class StatementIds : public ::testing::Test {
 protected:
  void SetUp() override {
    warpsql::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<warpsql::Server>(options, std::make_shared<RecordingEngine>(seen_),
                                                std::make_shared<AnyPassword>());
    server_->Start();
    client_ = std::make_unique<warpsql::client::Client>(
        warpsql::client::ConnectOptions{.host = "127.0.0.1", .port = server_->port()});
    client_->SignInWithPassword(/*username=*/"alice", /*password=*/"x");
    session_ = client_->CreateSession();
  }
  void TearDown() override {
    session_.reset();
    client_.reset();
    server_->Stop();
  }
  std::vector<std::string> Calls() {
    std::lock_guard lock(seen_->mutex);
    return seen_->calls;
  }
  std::string LastTold() {
    std::lock_guard lock(seen_->mutex);
    return seen_->last_told;
  }

  std::shared_ptr<Seen> seen_ = std::make_shared<Seen>();
  std::unique_ptr<warpsql::Server> server_;
  std::unique_ptr<warpsql::client::Client> client_;
  std::unique_ptr<warpsql::client::Session> session_;
};

TEST_F(StatementIds, AStatementRunsUnderTheIdItsClientHolds) {
  const auto streamed = session_->Execute("one");
  const auto submitted = session_->Submit("two", /*wait_s=*/5);
  ASSERT_TRUE(streamed.statement_id.starts_with("q_")) << streamed.statement_id;
  ASSERT_TRUE(submitted.statement_id.starts_with("q_")) << submitted.statement_id;
  EXPECT_EQ(Calls(), (std::vector<std::string>{"execute " + streamed.statement_id,
                                               "execute " + submitted.statement_id}));
  EXPECT_EQ(LastTold(), "");  // untold once it returned
}

TEST_F(StatementIds, AFailedStatementWasToldItsIdToo) {
  // The engine throws before recording; what matters is that it was told, then untold.
  EXPECT_THROW(session_->Execute("fail"), warpsql::client::Error);
  EXPECT_EQ(LastTold(), "");
  EXPECT_TRUE(Calls().empty());
}

TEST_F(StatementIds, APreparedStatementIsPreparedUnderItsIdAndEachExecutionUnderItsOwn) {
  auto prepared = session_->Prepare("three");
  ASSERT_TRUE(prepared->id().starts_with("p_")) << prepared->id();
  const auto first = prepared->Execute();
  const auto second = prepared->Execute();
  ASSERT_TRUE(first.statement_id.starts_with("q_")) << first.statement_id;
  ASSERT_NE(first.statement_id, second.statement_id);
  EXPECT_EQ(Calls(), (std::vector<std::string>{"prepare " + prepared->id(),
                                               "execute_prepared " + first.statement_id,
                                               "execute_prepared " + second.statement_id}));
  EXPECT_EQ(LastTold(), "");
}

TEST(NewId, HasThePrefixAndThirtyTwoBase32Characters) {
  const std::string id = warpsql::NewId("x");
  ASSERT_EQ(id.size(), 2u + 32u) << id;
  EXPECT_TRUE(id.starts_with("x_")) << id;
  EXPECT_EQ(id.find_first_not_of("abcdefghijklmnopqrstuvwxyz234567", 2), std::string::npos) << id;
  EXPECT_NE(warpsql::NewId("x"), id);
}

}  // namespace
