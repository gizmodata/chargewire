#include "platform.hpp"

#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <array>
#include <chrono>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
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

}  // namespace chargewire::detail
