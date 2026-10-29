// A "something changed" signal that engine threads raise and coroutines await.
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "common.hpp"

namespace chargewire::detail {

class Signal {
 public:
  uint64_t version() const;

  // Bumps the version and wakes every waiter. Safe from any thread.
  void Notify();

  // Returns once the version differs from `seen` or `deadline` passes, with the
  // version at that point. The calling coroutine must run on a strand (each
  // connection does), so a Notify() can't slip in between the check and the wait.
  Task<uint64_t> WaitChange(uint64_t seen, Clock::time_point deadline);

 private:
  mutable std::mutex mutex_;
  uint64_t version_ = 0;
  std::vector<std::weak_ptr<net::steady_timer>> waiters_;
};

}  // namespace chargewire::detail
