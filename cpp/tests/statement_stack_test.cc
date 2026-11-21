// The threads that run statements have an 8 MB stack on every platform. Engines recurse once
// per level of a statement's nesting on the thread that runs it, and size their limits for
// Linux's default 8 MB (DuckDB's max_expression_depth, 1000); a boost::asio::thread_pool gave
// its threads the platform default, 512 KB on macOS and 1 MB on Windows, where a statement
// the engine accepts overflowed the stack and stopped the server.
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "chargewire/server.hpp"
#include "engine_pool.hpp"
#include "platform.hpp"
#include "update_outcome.hpp"

namespace {

namespace json = boost::json;

using chargewire::detail::CurrentThreadStackBytes;
using chargewire::detail::EnginePool;

constexpr std::size_t kEightMegabytes = 8 * 1024 * 1024;

// Uses `frames` x 64 KB of the calling thread's stack, as a deep parse does: 96 frames (6 MB)
// overflow a 512 KB or 1 MB stack and stop the process.
#if defined(_MSC_VER)
__declspec(noinline)
#else
__attribute__((noinline))
#endif
int UseStack(int frames) {
  volatile char block[64 * 1024];
  std::memset(const_cast<char*>(block), frames, sizeof(block));
  if (frames == 0) return block[sizeof(block) - 1];
  return UseStack(frames - 1) + block[frames];  // (not a tail call: the frame stays)
}

TEST(StatementStack, EachThreadOfThePoolHasAnEightMegabyteStack) {
  EnginePool pool(3);
  std::mutex mutex;
  std::set<std::thread::id> threads;
  std::set<std::size_t> stacks;
  std::atomic<int> ran = 0;
  for (int i = 0; i < 60; i++) {
    boost::asio::post(pool.context(), [&] {
      const std::size_t bytes = CurrentThreadStackBytes();
      UseStack(96);
      std::lock_guard lock(mutex);
      threads.insert(std::this_thread::get_id());
      stacks.insert(bytes);
      ran += 1;
    });
  }
  // join() lets what is queued finish, as thread_pool::join() did.
  pool.join();
  EXPECT_EQ(ran.load(), 60);
  EXPECT_LE(threads.size(), 3u);  // the number of threads asked for, and no more
  ASSERT_FALSE(stacks.empty());
  for (const std::size_t bytes : stacks) EXPECT_GE(bytes, kEightMegabytes);
}

TEST(StatementStack, AJoinedPoolAcceptsWorkItNeverRunsAndAStoppedOneEnds) {
  {
    EnginePool pool(2);
    pool.join();
    std::atomic<bool> ran = false;
    boost::asio::post(pool.context(), [&] { ran = true; });
    pool.join();  // a second join is harmless
    EXPECT_FALSE(ran.load());
  }
  // stop(): the threads end after the handler each is running; what is still queued is
  // abandoned, as with thread_pool::stop().
  std::atomic<bool> started = false;
  std::atomic<bool> release = false;
  std::atomic<int> ran = 0;
  {
    EnginePool pool(1);
    boost::asio::post(pool.context(), [&] {
      started = true;
      while (!release.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
      ran += 1;
    });
    while (!started.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    boost::asio::post(pool.context(), [&] { ran += 10; });
    pool.stop();
    release = true;
  }
  EXPECT_EQ(ran.load(), 1);
}

// The server's own: a statement's Execute runs on a thread with the stack.
class StackReportingSession : public chargewire::EngineSession {
 public:
  explicit StackReportingSession(std::shared_ptr<std::atomic<std::size_t>> seen) : seen_(std::move(seen)) {}
  chargewire::StatementOutcome Execute(std::string_view, const json::value&) override {
    seen_->store(CurrentThreadStackBytes());
    UseStack(96);  // a 512 KB stack ends the test process here
    chargewire::StatementOutcome outcome;
    outcome.rows_affected = 1;
    return outcome;
  }
  std::unique_ptr<chargewire::PreparedStatement> Prepare(std::string_view) override {
    throw chargewire::Error(chargewire::ErrorCode::kNotImplemented, "no prepare");
  }
  chargewire::StatementOutcome ExecutePrepared(chargewire::PreparedStatement&, const chargewire::Parameters&) override {
    throw chargewire::Error(chargewire::ErrorCode::kNotImplemented, "no prepare");
  }
  void Cancel(std::string_view) override {}
  chargewire::OptionErrors SetOptions(const json::object&) override { return {}; }
  json::object Options() override { return {}; }
  chargewire::TransactionState Transaction() override { return chargewire::TransactionState::kNone; }

 private:
  std::shared_ptr<std::atomic<std::size_t>> seen_;
};

class StackReportingEngine : public chargewire::Backend {
 public:
  explicit StackReportingEngine(std::shared_ptr<std::atomic<std::size_t>> seen) : seen_(std::move(seen)) {}
  chargewire::ServerIdentity Identity() override { return {"statement-stack-test", "0"}; }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const chargewire::Principal&, const std::string&,
                                                         const json::object&) override {
    return std::make_unique<StackReportingSession>(seen_);
  }

 private:
  std::shared_ptr<std::atomic<std::size_t>> seen_;
};

TEST(StatementStack, AStatementRunsOnAThreadWithAnEightMegabyteStack) {
  auto seen = std::make_shared<std::atomic<std::size_t>>(0);
  chargewire::ServerOptions options;
  options.host = "127.0.0.1";
  options.engine_threads = 2;
  chargewire::Server server(options, std::make_shared<StackReportingEngine>(seen),
                            std::make_shared<update_outcome::AnyPassword>());
  server.Start();
  const update_outcome::Wire wire = update_outcome::Wire::Open(server.port());
  EXPECT_EQ(wire.Run("statement").at("result"), (json::object{{"kind", "update"}, {"rows_affected", 1}}));
  EXPECT_GE(seen->load(), kEightMegabytes);
  server.Stop();
}

}  // namespace
