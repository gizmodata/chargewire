// The threads statements run on: a fixed number, each with an 8 MB stack on every platform.
//
// Engines recurse once per level of a statement's nesting while they parse, bind and plan it
// on the thread that runs it, and size their limits for the 8 MB stack Linux gives a thread
// (DuckDB's max_expression_depth, 1000, assumes it). A boost::asio::thread_pool starts its
// threads with the platform's default stack, 512 KB on macOS and 1 MB on Windows, where
// statements the engine accepts by default overflowed it and stopped the server. Attach
// threads with explicit stacks to a thread_pool that starts none of its own. Its scheduler
// stops before dequeuing more work, including on Windows where an io_context's IOCP can run
// queued completions before it receives its stop packet.
#pragma once

#include <boost/asio/thread_pool.hpp>

#include <cstddef>
#include <memory>
#include <vector>

#include "platform.hpp"

namespace chargewire::detail {

class EnginePool {
 public:
  // The stack of each thread: what Linux gives a thread by default, and what engines size
  // their recursion limits for.
  static constexpr std::size_t kStackBytes = 8 * 1024 * 1024;

  explicit EnginePool(std::size_t threads, std::size_t stack_bytes = kStackBytes) {
    threads_.reserve(threads);
    try {
      for (std::size_t i = 0; i < threads; i++) {
        threads_.push_back(std::make_unique<StackThread>(stack_bytes, [this] { pool_.attach(); }));
      }
    } catch (...) {
      // An attached thread waits for work even if a later thread failed to start.
      stop();
      join();
      throw;
    }
  }

  // As thread_pool's destructor: what has not run by now is abandoned.
  ~EnginePool() {
    stop();
    join();
  }

  EnginePool(const EnginePool&) = delete;
  EnginePool& operator=(const EnginePool&) = delete;

  boost::asio::thread_pool& context() { return pool_; }

  // Release the pool's work guard, then join the threads we attached. They finish what is
  // queued (and what that queues in turn), unless stop() was called first.
  void join() {
    pool_.join();
    for (auto& thread : threads_) thread->join();
  }

  // As thread_pool::stop(): the threads end after the handler each is running.
  void stop() { pool_.stop(); }

 private:
  boost::asio::thread_pool pool_{0};
  std::vector<std::unique_ptr<StackThread>> threads_;
};

}  // namespace chargewire::detail
