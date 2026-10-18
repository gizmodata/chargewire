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
using warpsql::detail::Statement;

class SessionLifetime : public ::testing::Test {
 protected:
  std::shared_ptr<Session> NewSession() {
    return std::make_shared<Session>("s_test", Principal{.name = "alice"}, boost::asio::make_strand(pool_.get_executor()));
  }
  static std::shared_ptr<Statement> NewStatement(const std::string& id) {
    return std::make_shared<Statement>(id, Statement::Config{});
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

}  // namespace
