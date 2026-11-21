#include "platform.hpp"

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>
#include <process.h>
#else
#include <poll.h>
#include <pthread.h>
#include <sys/socket.h>

#include <cstring>
#endif

namespace chargewire::detail {

bool PeerClosed(boost::asio::ip::tcp::socket& socket) {
  const auto fd = socket.native_handle();
#if defined(POLLRDHUP)
  // Linux reports the peer's shutdown even when bytes (a TLS close_notify) are pending.
  pollfd entry{fd, POLLRDHUP, 0};
  return ::poll(&entry, 1, 0) > 0 && (entry.revents & (POLLRDHUP | POLLHUP | POLLERR)) != 0;
#elif defined(_WIN32)
  // WSAPoll reports a shutdown or reset as POLLHUP/POLLERR, even with bytes (a TLS
  // close_notify) pending. Otherwise peek as below, but only once WSAPoll says a read
  // won't block: Asio leaves Windows sockets blocking.
  WSAPOLLFD entry{fd, POLLRDNORM, 0};
  if (::WSAPoll(&entry, 1, 0) <= 0) {
    return false;
  }
  if ((entry.revents & (POLLHUP | POLLERR | POLLNVAL)) != 0) {
    return true;
  }
  char byte = 0;
  const int got = ::recv(fd, &byte, 1, MSG_PEEK);
  if (got != SOCKET_ERROR) {
    return got == 0;
  }
  const int error = ::WSAGetLastError();
  return error != WSAEWOULDBLOCK && error != WSAEINTR;
#else
  char byte = 0;
  const ssize_t got = ::recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
  return got == 0 || (got < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
#endif
}

void PrepareListener(boost::asio::ip::tcp::acceptor& acceptor) {
#ifndef _WIN32
  acceptor.set_option(boost::asio::socket_base::reuse_address(true));
#else
  (void)acceptor;
#endif
}

boost::asio::awaitable<void> EndConnection(boost::beast::tcp_stream& stream) {
  boost::system::error_code ec;
  constexpr auto kLinger = std::chrono::seconds(2);
  constexpr std::size_t kMaxDrained = 256 * 1024 * 1024;
  stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
  if (!ec) {
    std::array<char, 64 * 1024> sink;
    std::size_t drained = 0;
    stream.expires_after(kLinger);
    while (drained < kMaxDrained) {
      const std::size_t got = co_await stream.async_read_some(
          boost::asio::buffer(sink), boost::asio::redirect_error(boost::asio::use_awaitable, ec));
      if (ec) {
        break;  // end of stream, reset, or the linger time is up
      }
      drained += got;
    }
  }
  stream.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
  co_return;
}

std::tm UtcTime(std::time_t seconds) {
  std::tm utc{};
#ifdef _WIN32
  gmtime_s(&utc, &seconds);
#else
  gmtime_r(&seconds, &utc);
#endif
  return utc;
}

namespace {

// What the new thread runs: its body, with an escaping exception ending the process.
void RunBody(std::function<void()>* body) {
  const std::unique_ptr<std::function<void()>> owned(body);
  try {
    (*owned)();
  } catch (...) {
    std::terminate();
  }
}

}  // namespace

#ifdef _WIN32

struct StackThread::Native {
  HANDLE handle = nullptr;
};

namespace {
unsigned __stdcall StackThreadMain(void* body) {
  RunBody(static_cast<std::function<void()>*>(body));
  return 0;
}
}  // namespace

StackThread::StackThread(std::size_t stack_bytes, std::function<void()> body) : native_(new Native) {
  auto* owned = new std::function<void()>(std::move(body));
  // STACK_SIZE_PARAM_IS_A_RESERVATION: the size is the stack's reserve, committed as it is
  // used (without it, it would be committed up front and the reserve stay the linker's).
  const uintptr_t handle = _beginthreadex(nullptr, static_cast<unsigned>(stack_bytes), &StackThreadMain, owned,
                                          STACK_SIZE_PARAM_IS_A_RESERVATION, nullptr);
  if (handle == 0) {
    const int error = errno;
    delete owned;
    delete native_;
    throw std::runtime_error("cannot start a thread with a " + std::to_string(stack_bytes) +
                             "-byte stack (errno " + std::to_string(error) + ")");
  }
  native_->handle = reinterpret_cast<HANDLE>(handle);
}

void StackThread::join() {
  if (native_->handle == nullptr) return;
  WaitForSingleObject(native_->handle, INFINITE);
  CloseHandle(native_->handle);
  native_->handle = nullptr;
}

std::size_t CurrentThreadStackBytes() {
  ULONG_PTR low = 0;
  ULONG_PTR high = 0;
  GetCurrentThreadStackLimits(&low, &high);
  return static_cast<std::size_t>(high - low);
}

#else

struct StackThread::Native {
  pthread_t thread{};
  bool joinable = false;
};

namespace {
void* StackThreadMain(void* body) {
  RunBody(static_cast<std::function<void()>*>(body));
  return nullptr;
}
}  // namespace

StackThread::StackThread(std::size_t stack_bytes, std::function<void()> body) : native_(new Native) {
  pthread_attr_t attributes;
  int error = pthread_attr_init(&attributes);
  // (macOS takes only a multiple of the page size: callers ask for whole megabytes.)
  if (error == 0) error = pthread_attr_setstacksize(&attributes, stack_bytes);
  auto* owned = new std::function<void()>(std::move(body));
  if (error == 0) error = pthread_create(&native_->thread, &attributes, &StackThreadMain, owned);
  pthread_attr_destroy(&attributes);
  if (error != 0) {
    delete owned;
    delete native_;
    throw std::runtime_error("cannot start a thread with a " + std::to_string(stack_bytes) +
                             "-byte stack: " + std::strerror(error));
  }
  native_->joinable = true;
}

void StackThread::join() {
  if (!native_->joinable) return;
  pthread_join(native_->thread, nullptr);
  native_->joinable = false;
}

std::size_t CurrentThreadStackBytes() {
#if defined(__APPLE__)
  return pthread_get_stacksize_np(pthread_self());
#elif defined(__linux__)
  pthread_attr_t attributes;
  if (pthread_getattr_np(pthread_self(), &attributes) != 0) return 0;
  std::size_t bytes = 0;
  pthread_attr_getstacksize(&attributes, &bytes);
  pthread_attr_destroy(&attributes);
  return bytes;
#else
  return 0;
#endif
}

#endif

StackThread::~StackThread() {
  join();
  delete native_;
}

}  // namespace chargewire::detail
