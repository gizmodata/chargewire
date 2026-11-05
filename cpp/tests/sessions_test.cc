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
#include <format>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>

#include <arrow/api.h>

#include "sessions.hpp"
#include "chargewire/errors.hpp"

namespace {

using chargewire::Error;
using chargewire::ErrorCode;
using chargewire::Principal;
using chargewire::detail::CancelReason;
using chargewire::detail::Clock;
using chargewire::detail::IngestOperation;
using chargewire::IngestTarget;
using chargewire::detail::Session;
using chargewire::detail::SessionRegistry;
using chargewire::detail::Statement;
using chargewire::detail::StatementState;

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

// A request waiting on a running statement (a submit or status long-poll) keeps an
// otherwise idle session open: the client is there (§5.7). Once no request waits on the
// statement and nobody reads it, the statement itself keeps nothing: the client is gone
// (its connection dropped mid-wait, its process killed), and a write it left running must
// not hold the session, and the write's transaction, open for as long as it runs. The
// close cancels the statement.
TEST_F(SessionLifetime, ARunningStatementKeepsTheSessionOpenOnlyWhileARequestWaitsOnIt) {
  auto session = NewSession();
  auto statement = NewStatement("q_running");
  session->AddStatement(statement);
  EXPECT_TRUE(statement->BeginRunning());
  statement->BeginWait();
  EXPECT_FALSE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
  EXPECT_FALSE(session->closed());
  statement->EndWait();
  EXPECT_FALSE(session->CloseIfIdle(Clock::now() - std::chrono::hours(1), CancelReason::kSessionClosed))
      << "a request was made within the window";
  EXPECT_TRUE(session->CloseIfIdle(Clock::now() + std::chrono::seconds(1), CancelReason::kSessionClosed));
  EXPECT_TRUE(session->closed());
  EXPECT_TRUE(statement->CancelRequested()) << "the abandoned statement is cancelled with its session";
}

// An engine whose session holds a transaction, for the idle-in-transaction rule.
class FakeEngine : public chargewire::EngineSession {
 public:
  chargewire::TransactionState transaction = chargewire::TransactionState::kNone;
  std::optional<std::chrono::seconds> idle_in_transaction;
  std::string* ending = nullptr;  // outlives the engine, which DestroyEngine destroys
  chargewire::TransactionState Transaction() override { return transaction; }
  std::optional<std::chrono::seconds> IdleInTransactionTimeout() override { return idle_in_transaction; }
  void Ending(std::string_view reason) override {
    if (ending) *ending = std::string(reason);
  }
};

// A session that holds a transaction open with nothing running and no request since the
// cutoff is ended with the error given (§5.7): its next request gets it, and the engine
// hears why it is destroyed. Work under way, or a request waiting on it, keeps it.
TEST_F(SessionLifetime, IdleInATransactionEndsTheSessionWithItsReason) {
  auto session = NewSession();
  auto engine = std::make_unique<FakeEngine>();
  std::string ending;
  engine->ending = &ending;
  engine->transaction = chargewire::TransactionState::kOpen;
  session->SetEngine(std::move(engine));
  session->RefreshTransaction();
  const Error why = Error::SessionEnded(chargewire::reason::kIdleInTransaction, "idle in a transaction");
  const auto past = Clock::now() + std::chrono::seconds(1);  // every request was before this
  EXPECT_FALSE(session->EndIfIdleInTransaction(Clock::now() - std::chrono::hours(1), why))
      << "a request was made within the window";
  auto statement = NewStatement("q_running");
  session->AddStatement(statement);
  EXPECT_TRUE(statement->BeginRunning());
  EXPECT_FALSE(session->EndIfIdleInTransaction(past, why)) << "a statement is running";
  statement->Finish();
  statement->BeginWait();
  EXPECT_FALSE(session->EndIfIdleInTransaction(past, why)) << "a request waits on a statement";
  statement->EndWait();
  EXPECT_FALSE(session->CloseIfIdle(Clock::now() - std::chrono::hours(1), CancelReason::kSessionClosed));
  EXPECT_TRUE(session->EndIfIdleInTransaction(past, why));
  EXPECT_TRUE(session->closed());
  try {
    session->Touch();
    FAIL() << "expected session_ended";
  } catch (const Error& error) {
    EXPECT_EQ(error.code(), ErrorCode::kSessionEnded);
    EXPECT_EQ(error.reason(), chargewire::reason::kIdleInTransaction);
  }
  EXPECT_FALSE(session->EndIfIdleInTransaction(past, why)) << "ended once";
  session->DestroyEngine("idle_in_transaction");
  EXPECT_EQ(ending, "idle_in_transaction");
}

// A session in no transaction is never idle in one, however long it has been quiet; the
// idle rule (CloseIfIdle) is the one for it. Aborted transactions count as open ones: they
// hold what an open one holds until they are rolled back.
TEST_F(SessionLifetime, OnlyASessionInATransactionIsIdleInOne) {
  for (const auto [transaction, ends] : {std::pair{chargewire::TransactionState::kNone, false},
                                         std::pair{chargewire::TransactionState::kOpen, true},
                                         std::pair{chargewire::TransactionState::kAborted, true}}) {
    auto session = NewSession();
    auto engine = std::make_unique<FakeEngine>();
    engine->transaction = transaction;
    session->SetEngine(std::move(engine));
    session->RefreshTransaction();
    const Error why = Error::SessionEnded(chargewire::reason::kIdleInTransaction, "idle in a transaction");
    EXPECT_EQ(session->EndIfIdleInTransaction(Clock::now() + std::chrono::seconds(1), why), ends)
        << chargewire::TransactionStateName(transaction);
  }
}

// An idle session that held a transaction open (SQL BEGIN, which the client can't see) is
// ended with the error given, not only closed (§5.7, issue #29): its next request gets
// session_ended, so a client that replaces lost sessions learns its transaction was rolled
// back instead of running its next statement alone. One in no transaction is only closed:
// session_not_found, as before, and without the error it is only closed either way.
TEST_F(SessionLifetime, AnIdleSessionInATransactionIsEndedWithAReason) {
  const Error why = Error::SessionEnded(chargewire::reason::kIdleInTransaction, "idle while in a transaction");
  const auto past = Clock::now() + std::chrono::seconds(1);
  for (const auto [transaction, given, ended] :
       {std::tuple{chargewire::TransactionState::kNone, true, false},
        std::tuple{chargewire::TransactionState::kOpen, true, true},
        std::tuple{chargewire::TransactionState::kAborted, true, true},
        std::tuple{chargewire::TransactionState::kOpen, false, false}}) {
    auto session = NewSession();
    auto engine = std::make_unique<FakeEngine>();
    engine->transaction = transaction;
    session->SetEngine(std::move(engine));
    session->RefreshTransaction();
    const auto label = std::format("{} error {}", chargewire::TransactionStateName(transaction),
                                   given ? "given" : "not given");
    EXPECT_FALSE(session->CloseIfIdle(Clock::now() - std::chrono::hours(1), CancelReason::kSessionClosed,
                                      given ? &why : nullptr))
        << label << ": a request was made within the window";
    EXPECT_FALSE(session->ended().has_value()) << label;
    EXPECT_TRUE(session->CloseIfIdle(past, CancelReason::kSessionClosed, given ? &why : nullptr)) << label;
    EXPECT_TRUE(session->closed()) << label;
    EXPECT_EQ(session->ended().has_value(), ended) << label;
    try {
      session->Touch();
      FAIL() << label << ": expected the closed session to refuse a request";
    } catch (const Error& error) {
      EXPECT_EQ(error.code(), ended ? ErrorCode::kSessionEnded : ErrorCode::kSessionNotFound) << label;
      if (ended) EXPECT_EQ(error.reason(), chargewire::reason::kIdleInTransaction) << label;
    }
  }
}

// The engine's per-session timeout is the session's; without an engine (or an answer)
// the server's default applies (nullopt).
TEST_F(SessionLifetime, TheEngineSetsTheSessionsIdleInTransactionTimeout) {
  auto session = NewSession();
  EXPECT_EQ(session->IdleInTransactionTimeout(), std::nullopt);
  auto engine = std::make_unique<FakeEngine>();
  engine->idle_in_transaction = std::chrono::seconds(42);
  session->SetEngine(std::move(engine));
  EXPECT_EQ(session->IdleInTransactionTimeout(), std::chrono::seconds(42));
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

// A session belongs to its owner's name and authority (spec §9.2): the same name with
// another authority_key (a weaker token of the user) is told nothing, live or ended, and
// can't forget it; the same name and authority can.
TEST_F(SessionLifetime, ASessionBelongsToItsOwnersAuthorityToo) {
  SessionRegistry registry;
  auto session = std::make_shared<Session>("s_auth", Principal{.name = "alice", .authority_key = "full"},
                                           boost::asio::make_strand(pool_.get_executor()));
  registry.Add(session);
  const Principal weaker{.name = "alice", .authority_key = "readonly"};
  const Principal unset{.name = "alice"};
  const Principal same{.name = "alice", .role = "other fields don't matter", .authority_key = "full"};
  ExpectNotFound([&] { registry.Find(session->id(), weaker); });
  ExpectNotFound([&] { registry.Find(session->id(), unset); });
  EXPECT_EQ(registry.Find(session->id(), same), session);
  registry.Retire(session->id(), session->owner(), Error::SessionEnded("killed", "killed"),
                  Clock::now() + std::chrono::minutes(1));
  ExpectNotFound([&] { registry.Find(session->id(), weaker); });
  EXPECT_FALSE(registry.ForgetEnded(session->id(), weaker));
  try {
    registry.Find(session->id(), same);
    FAIL() << "expected session_ended";
  } catch (const Error& error) {
    EXPECT_EQ(error.code(), ErrorCode::kSessionEnded);
  }
  EXPECT_TRUE(registry.ForgetEnded(session->id(), same));
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
  EXPECT_EQ(chargewire::detail::CancelReasonName(CancelReason::kKilled), "killed");
  EXPECT_EQ(chargewire::detail::CancelReasonName(CancelReason::kCancelled), "cancelled");
}
