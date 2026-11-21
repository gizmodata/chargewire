// The threads statements run on: a fixed number, each with an 8 MB stack on every platform.
//
// Engines recurse once per level of a statement's nesting while they parse, bind and plan it
// on the thread that runs it, and size their limits for the 8 MB stack Linux gives a thread
// (DuckDB's max_expression_depth, 1000, assumes it). A boost::asio::thread_pool starts its
// threads with the platform's default stack, 512 KB on macOS and 1 MB on Windows, where
// statements the engine accepts by default overflowed it and stopped the server. This pool
// runs an io_context on threads it starts itself with the stack size explicit, and is
// otherwise used as the thread_pool was: work is posted to its io_context, join() lets the
// work already queued finish and then waits for the threads, stop() abandons it.
#pragma once

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "platform.hpp"

namespace chargewire::detail {

class EnginePool {
 public:
  // The stack of each thread: what Linux gives a thread by default, and what engines size
  // their recursion limits for.
  static constexpr std::size_t kStackBytes = 8 * 1024 * 1024;

  explicit EnginePool(std::size_t threads, std::size_t stack_bytes = kStackBytes)
      // (The concurrency hint thread_pool gives its scheduler.)
      : io_(threads == 1 ? 1 : 0), work_(io_.get_executor()) {
    threads_.reserve(threads);
    for (std::size_t i = 0; i < threads; i++) {
      threads_.push_back(std::make_unique<StackThread>(stack_bytes, [this] { io_.run(); }));
    }
  }

  // As thread_pool's destructor: what has not run by now is abandoned.
  ~EnginePool() {
    stop();
    join();
  }

  EnginePool(const EnginePool&) = delete;
  EnginePool& operator=(const EnginePool&) = delete;

  boost::asio::io_context& context() { return io_; }

  // As thread_pool::join(): no more work keeps the threads; they finish what is queued (and
  // what that queues in turn), then end.
  void join() {
    work_.reset();
    for (auto& thread : threads_) thread->join();
  }

  // As thread_pool::stop(): the threads end after the handler each is running.
  void stop() { io_.stop(); }

 private:
  boost::asio::io_context io_;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
  std::vector<std::unique_ptr<StackThread>> threads_;
};

}  // namespace chargewire::detail
