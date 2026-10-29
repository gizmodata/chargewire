// EngineSession::SetStatementId: the engine is told the id the client holds for what it
// runs (a statement's q_..., a prepared statement's p_...), and untold after; and
// SetStatementSubmitted: when the statement it runs was submitted. A server started in
// this process with an engine that records what it was told.
#include <gtest/gtest.h>

#include <boost/json.hpp>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "chargewire/client.hpp"
#include "chargewire/server.hpp"

namespace {

namespace json = boost::json;

using chargewire::Error;
using chargewire::ErrorCode;
using chargewire::Principal;
using SteadyClock = std::chrono::steady_clock;

// What the engine saw: each call with the id it had been told at the time.
struct Seen {
  std::mutex mutex;
  std::vector<std::string> calls;  // "execute q_...", "prepare p_...", "execute_prepared q_..."
  std::string last_told = "(never)";
  // Each call's submission time as told, and when the call came: by id.
  struct Timing {
    SteadyClock::time_point submitted;
    SteadyClock::time_point called;
  };
  std::map<std::string, Timing> timings;
  SteadyClock::time_point last_submitted = SteadyClock::time_point::max();  // max: never told
  // "hold" waits for this.
  std::condition_variable released_changed;
  bool released = false;
  void Add(const std::string& call) {
    std::lock_guard lock(mutex);
    calls.push_back(call);
  }
};

class NoRows : public chargewire::PreparedStatement {
 public:
  std::shared_ptr<arrow::Schema> ParameterSchema() override { return arrow::schema({}); }
  std::shared_ptr<arrow::Schema> ResultSchema() override { return nullptr; }
  std::optional<bool> ReturnsRows() override { return false; }
};

class RecordingSession : public chargewire::EngineSession {
 public:
  explicit RecordingSession(std::shared_ptr<Seen> seen) : seen_(std::move(seen)) {}
  void SetStatementId(std::string_view id) override {
    id_ = std::string(id);
    std::lock_guard lock(seen_->mutex);
    seen_->last_told = id_;
  }
  void SetStatementSubmitted(SteadyClock::time_point submitted) override {
    submitted_ = submitted;
    std::lock_guard lock(seen_->mutex);
    seen_->last_submitted = submitted;
  }
  chargewire::StatementOutcome Execute(std::string_view sql, const json::value&) override {
    Time();
    if (sql == "fail") throw Error(ErrorCode::kSqlError, "no such thing");
    if (sql == "hold") {
      std::unique_lock lock(seen_->mutex);
      seen_->released_changed.wait_for(lock, std::chrono::seconds(30), [&] { return seen_->released; });
    }
    seen_->Add("execute " + id_);
    return {.rows_affected = 0};
  }
  std::unique_ptr<chargewire::PreparedStatement> Prepare(std::string_view) override {
    Time();
    seen_->Add("prepare " + id_);
    return std::make_unique<NoRows>();
  }
  chargewire::StatementOutcome ExecutePrepared(chargewire::PreparedStatement&, const chargewire::Parameters&) override {
    Time();
    seen_->Add("execute_prepared " + id_);
    return {.rows_affected = 0};
  }

 private:
  void Time() {
    std::lock_guard lock(seen_->mutex);
    seen_->timings[id_] = {submitted_, SteadyClock::now()};
  }
  std::shared_ptr<Seen> seen_;
  std::string id_;
  SteadyClock::time_point submitted_{};
};

class RecordingEngine : public chargewire::Backend {
 public:
  explicit RecordingEngine(std::shared_ptr<Seen> seen) : seen_(std::move(seen)) {}
  chargewire::ServerIdentity Identity() override { return {"statement-ids-test", "0"}; }
  chargewire::BackendCapabilities Capabilities() override { return {.prepared_statements = true}; }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const Principal&, const std::string&,
                                                      const json::object&) override {
    return std::make_unique<RecordingSession>(seen_);
  }

 private:
  std::shared_ptr<Seen> seen_;
};

class AnyPassword : public chargewire::Authenticator {
 public:
  Principal AuthenticatePassword(std::string_view username, std::string_view) override {
    return Principal{.name = std::string(username), .role = "admin"};
  }
  json::array GrantTypes() override { return {"password"}; }
};

class StatementIds : public ::testing::Test {
 protected:
  void SetUp() override {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(options, std::make_shared<RecordingEngine>(seen_),
                                                std::make_shared<AnyPassword>());
    server_->Start();
    client_ = std::make_unique<chargewire::client::Client>(
        chargewire::client::ConnectOptions{.host = "127.0.0.1", .port = server_->port()});
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
  Seen::Timing TimingOf(const std::string& id) {
    std::lock_guard lock(seen_->mutex);
    return seen_->timings.at(id);
  }
  SteadyClock::time_point LastSubmittedTold() {
    std::lock_guard lock(seen_->mutex);
    return seen_->last_submitted;
  }
  void Release() {
    {
      std::lock_guard lock(seen_->mutex);
      seen_->released = true;
    }
    seen_->released_changed.notify_all();
  }

  std::shared_ptr<Seen> seen_ = std::make_shared<Seen>();
  std::unique_ptr<chargewire::Server> server_;
  std::unique_ptr<chargewire::client::Client> client_;
  std::unique_ptr<chargewire::client::Session> session_;
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
  EXPECT_THROW(session_->Execute("fail"), chargewire::client::Error);
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

// A statement's timeout counts from its submission, so the engine is told that instant,
// not the one it was handed the statement: here the second statement waits behind the
// first for the session's engine thread, and is told when it was submitted.
TEST_F(StatementIds, AStatementIsToldWhenItWasSubmittedNotWhenTheEngineTookItUp) {
  const auto first = session_->Submit("hold");
  const auto before = SteadyClock::now();
  const auto second = session_->Submit("two");
  const auto after = SteadyClock::now();
  const auto queued_for = std::chrono::milliseconds(300);
  std::this_thread::sleep_for(queued_for);
  Release();
  EXPECT_EQ(session_->Wait(first.statement_id).state, "succeeded");
  EXPECT_EQ(session_->Wait(second.statement_id).state, "succeeded");
  const Seen::Timing timing = TimingOf(second.statement_id);
  EXPECT_GE(timing.submitted, before);
  EXPECT_LE(timing.submitted, after);
  EXPECT_GE(timing.called - timing.submitted, queued_for);
  EXPECT_EQ(LastSubmittedTold(), SteadyClock::time_point{});  // untold once it returned
}

TEST_F(StatementIds, EachExecutionOfAPreparedStatementIsToldItsOwnSubmissionAndThePrepareNone) {
  auto prepared = session_->Prepare("three");
  const auto before = SteadyClock::now();
  const auto execution = prepared->Execute();
  const auto after = SteadyClock::now();
  EXPECT_EQ(TimingOf(prepared->id()).submitted, SteadyClock::time_point{});
  const Seen::Timing timing = TimingOf(execution.statement_id);
  EXPECT_GE(timing.submitted, before);
  EXPECT_LE(timing.submitted, after);
  EXPECT_LE(timing.submitted, timing.called);
  EXPECT_EQ(LastSubmittedTold(), SteadyClock::time_point{});
}

TEST(NewId, HasThePrefixAndThirtyTwoBase32Characters) {
  const std::string id = chargewire::NewId("x");
  ASSERT_EQ(id.size(), 2u + 32u) << id;
  EXPECT_TRUE(id.starts_with("x_")) << id;
  EXPECT_EQ(id.find_first_not_of("abcdefghijklmnopqrstuvwxyz234567", 2), std::string::npos) << id;
  EXPECT_NE(chargewire::NewId("x"), id);
}

}  // namespace
