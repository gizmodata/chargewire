// Session lifetime against requests (spec §5.6, §5.7): a request that found a session
// either runs in it or gets session_not_found; it never runs in a session that was closed
// under it. The idle sweep's check and close are one step, ordered against Touch(): a
// conformance run on Windows saw a SELECT 1 whose result stream was cut mid-body
// ("incomplete chunked read") because the sweep found the session idle, the request then
// found and touched it, and the sweep closed it with the statement running.
#include <gtest/gtest.h>

#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <memory>

#include <arrow/api.h>

#include "sessions.hpp"
#include "warpsql/errors.hpp"

namespace {

using warpsql::Error;
using warpsql::ErrorCode;
using warpsql::Principal;
using warpsql::detail::CancelReason;
using warpsql::detail::Clock;
using warpsql::detail::IngestOperation;
using warpsql::IngestTarget;
using warpsql::detail::Session;
using warpsql::detail::SessionRegistry;
using warpsql::detail::Statement;
using warpsql::detail::StatementState;

class SessionLifetime : public ::testing::Test {
 protected:
  std::shared_ptr<Session> NewSession() {
    return std::make_shared<Session>("s_test", Principal{.name = "alice"}, boost::asio::make_strand(pool_.get_executor()));
  }
  static std::shared_ptr<Statement> NewStatement(const std::string& id) {
    return std::make_shared<Statement>(id, Statement::Config{});
  }
  // A statement that ran to its end with one batch of rows, its result kept or not.
  static std::shared_ptr<Statement> FinishedWithRows(const std::string& id, bool retain) {
    Statement::Config config;
    config.retain = retain;
    auto statement = std::make_shared<Statement>(id, config);
    EXPECT_TRUE(statement->BeginRunning());
    auto schema = arrow::schema({arrow::field("v", arrow::int64())});
    statement->SetRows(schema);
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.AppendValues({1, 2, 3}).ok());
    std::shared_ptr<arrow::Array> values;
    EXPECT_TRUE(builder.Finish(&values).ok());
    EXPECT_TRUE(statement->AddBatch(arrow::RecordBatch::Make(schema, 3, {values})));
    statement->Finish();
    return statement;
  }
  static void ExpectNotFound(const std::function<void()>& call) {
    try {
      call();
      FAIL() << "expected session_not_found";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::kSessionNotFound);
    }
  }

  boost::asio::thread_pool pool_{1};
};

// The interleaving the Windows run hit, in the order that must win: the request touches
// the session first, so the sweep that follows finds it busy and leaves it open.
TEST_F(SessionLifetime, ASweepAfterATouchLeavesTheSessionOpen) {
  auto session = NewSession();
  const auto cutoff = Clock::now();  // the sweep's cutoff, computed before the request arrived
  session->Touch();
  EXPECT_FALSE(session->CloseIfIdle(cutoff, CancelReason::kSessionClosed));
  EXPECT_FALSE(session->closed());
  session->AddStatement(NewStatement("q_1"));
}

// The other order: the sweep closes the idle session first, so the request finds it gone.
TEST_F(SessionLifetime, ARequestAfterAnIdleCloseGetsSessionNotFound) {
  auto session = NewSession();
  EXPECT_TRUE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
  EXPECT_TRUE(session->closed());
  ExpectNotFound([&] { session->Touch(); });
}

// Whatever closed it (a sweep, KILL SESSION, DELETE), a session closed after the request
// touched it takes no new statement or ingest: they would run in a session being torn
// down, never cancelled by its close.
TEST_F(SessionLifetime, AClosedSessionTakesNoNewStatementOrIngest) {
  auto session = NewSession();
  session->Touch();
  session->Close(CancelReason::kSessionClosed);
  ExpectNotFound([&] { session->Touch(); });
  ExpectNotFound([&] { session->AddStatement(NewStatement("q_late")); });
  ExpectNotFound([&] { session->AddIngest(std::make_shared<IngestOperation>("i_late", IngestTarget{})); });
  EXPECT_TRUE(session->Statements().empty());
  EXPECT_TRUE(session->Ingests().empty());
}

// A statement that is still running keeps an otherwise idle session open (§5.7).
TEST_F(SessionLifetime, ARunningStatementKeepsTheSessionOpen) {
  auto session = NewSession();
  session->AddStatement(NewStatement("q_running"));
  EXPECT_FALSE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
  EXPECT_FALSE(session->closed());
}

// A finished statement whose result is still being read keeps the session open: an
// unretained result's last batches (up to unretained_buffer_bytes) are sent after the
// statement has finished, and a slow reader's stream was cut by the idle sweep. Once the
// read ends, the session is idle again.
TEST_F(SessionLifetime, AReadInProgressKeepsTheSessionOpen) {
  auto session = NewSession();
  auto statement = FinishedWithRows("q_read", /*retain=*/false);
  session->AddStatement(statement);
  statement->ClaimRead(0, 0);
  EXPECT_FALSE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
  EXPECT_FALSE(session->closed());
  statement->EndRead(0);
  EXPECT_TRUE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
}

// A retained result keeps an otherwise idle session open until it expires (§5.7, §6.5),
// so it can be read again until its expires_at; then the session is idle.
TEST_F(SessionLifetime, ARetainedResultKeepsTheSessionOpenUntilItExpires) {
  auto session = NewSession();
  auto statement = FinishedWithRows("q_kept", /*retain=*/true);
  session->AddStatement(statement);
  EXPECT_FALSE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
  ASSERT_TRUE(statement->ExpireIfDue(*statement->finished_at() + std::chrono::hours(2)));
  EXPECT_TRUE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
}

}  // namespace

// Server::EndSession (KILL SESSION): an ended session refuses everything with its own
// error at once, but cancels nothing itself (the engine cancels its work first, then ends
// it); Close() then tears it down.
TEST_F(SessionLifetime, AnEndedSessionSaysWhyAndCancelsNothingUntilClosed) {
  auto session = NewSession();
  auto running = NewStatement("q_running");
  session->AddStatement(running);
  ASSERT_TRUE(running->BeginRunning());
  const Error why = Error::SessionEnded("killed", "killed by an administrator");
  ASSERT_TRUE(session->End(why));
  EXPECT_FALSE(session->End(why));  // once
  for (const auto& call : std::vector<std::function<void()>>{
           [&] { session->Touch(); }, [&] { session->AddStatement(NewStatement("q_late")); },
           [&] { session->AddIngest(std::make_shared<IngestOperation>("i_late", IngestTarget{})); }}) {
    try {
      call();
      FAIL() << "expected session_ended";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ErrorCode::kSessionEnded);
      EXPECT_EQ(error.reason(), "killed");
    }
  }
  EXPECT_EQ(running->state(), StatementState::kRunning);  // not cancelled by the end itself
  EXPECT_FALSE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
  session->Close(CancelReason::kKilled);
  EXPECT_TRUE(running->CancelRequested());
  EXPECT_EQ(running->cancel_reason(), CancelReason::kKilled);
}

// After its teardown the registry remembers an ended session for its owner only, until a
// DELETE (or the time is up).
TEST_F(SessionLifetime, ARetiredSessionAnswersItsOwnerWithItsError) {
  SessionRegistry registry;
  auto session = NewSession();
  registry.Add(session);
  const Error why = Error::SessionEnded("killed", "killed by an administrator");
  registry.Retire(session->id(), session->owner(), why, Clock::now() + std::chrono::minutes(1));
  EXPECT_EQ(registry.Get(session->id()), nullptr);
  try {
    registry.Find(session->id(), Principal{.name = "alice"});
    FAIL() << "expected session_ended";
  } catch (const Error& error) {
    EXPECT_EQ(error.code(), ErrorCode::kSessionEnded);
  }
  ExpectNotFound([&] { registry.Find(session->id(), Principal{.name = "mallory"}); });
  EXPECT_FALSE(registry.ForgetEnded(session->id(), Principal{.name = "mallory"}));
  EXPECT_TRUE(registry.ForgetEnded(session->id(), Principal{.name = "alice"}));
  ExpectNotFound([&] { registry.Find(session->id(), Principal{.name = "alice"}); });
  // And the sweep forgets one whose time is up.
  registry.Retire("s_old", session->owner(), why, Clock::now() - std::chrono::seconds(1));
  registry.SweepEnded(Clock::now());
  ExpectNotFound([&] { registry.Find("s_old", Principal{.name = "alice"}); });
}

// What Server::Sessions() reads of a statement.
TEST(StatementDescribe, ItSaysStateRowsPartitionsReadersAndWhyItWasCancelled) {
  Statement::Config config;
  config.retain = true;
  auto statement = std::make_shared<Statement>("q_d", config);
  EXPECT_EQ(statement->Describe().state, StatementState::kQueued);
  ASSERT_TRUE(statement->BeginRunning());
  auto schema = arrow::schema({arrow::field("v", arrow::int64())});
  statement->SetRows(schema);
  arrow::Int64Builder builder;
  ASSERT_TRUE(builder.AppendValues({1, 2, 3}).ok());
  std::shared_ptr<arrow::Array> values;
  ASSERT_TRUE(builder.Finish(&values).ok());
  ASSERT_TRUE(statement->AddBatch(arrow::RecordBatch::Make(schema, 3, {values})));
  auto running = statement->Describe();
  EXPECT_EQ(running.state, StatementState::kRunning);
  EXPECT_TRUE(running.has_rows && running.started_at.has_value());
  EXPECT_EQ(running.batches, 1);
  EXPECT_EQ(running.rows, 3);
  statement->Finish();
  statement->ClaimRead(0, 0);
  auto done = statement->Describe();
  EXPECT_EQ(done.active_readers, 1);
  EXPECT_TRUE(done.holds_retained_result);
  EXPECT_EQ(done.partitions, std::optional<std::size_t>(1));
  auto cancelled = std::make_shared<Statement>("q_c", Statement::Config{});
  EXPECT_FALSE(cancelled->RequestCancel(CancelReason::kCancelled));  // queued: cancelled at once
  EXPECT_EQ(cancelled->cancel_reason(), CancelReason::kCancelled);
  EXPECT_EQ(cancelled->Describe().state, StatementState::kCancelled);
  EXPECT_EQ(warpsql::detail::CancelReasonName(CancelReason::kKilled), "killed");
  EXPECT_EQ(warpsql::detail::CancelReasonName(CancelReason::kCancelled), "cancelled");
}
