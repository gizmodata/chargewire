#include "signal.hpp"

namespace chargewire::detail {

uint64_t Signal::version() const {
  std::lock_guard lock(mutex_);
  return version_;
}

void Signal::Notify() {
  std::vector<std::weak_ptr<net::steady_timer>> waiters;
  {
    std::lock_guard lock(mutex_);
    ++version_;
    waiters.swap(waiters_);
  }
  for (auto& weak : waiters) {
    if (auto timer = weak.lock()) {
      // Cancel on the timer's own strand: timers aren't safe across threads.
      net::post(timer->get_executor(), [timer] { timer->cancel(); });
    }
  }
}

Task<uint64_t> Signal::WaitChange(uint64_t seen, Clock::time_point deadline) {
  auto timer = std::make_shared<net::steady_timer>(co_await net::this_coro::executor, deadline);
  {
    std::lock_guard lock(mutex_);
    if (version_ != seen) {
      co_return version_;
    }
    waiters_.push_back(timer);
  }
  boost::system::error_code ignored;
  co_await timer->async_wait(net::redirect_error(net::use_awaitable, ignored));
  co_return version();
}

}  // namespace chargewire::detail
