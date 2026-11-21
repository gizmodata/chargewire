// Operating-system differences the SDK hides, kept apart from the protocol code.
#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <cstddef>
#include <ctime>
#include <functional>

namespace chargewire::detail {

// Whether the peer of a connected socket has gone (shut down its side, reset, or
// errored), without consuming anything. Never blocks.
bool PeerClosed(boost::asio::ip::tcp::socket& socket);

// Options a listening socket takes before bind: on POSIX SO_REUSEADDR, so a restarted
// server rebinds a port with connections in TIME_WAIT. Nothing on Windows: there
// SO_REUSEADDR would let a second server bind a port this one listens on, and no option
// is needed to rebind past TIME_WAIT.
void PrepareListener(boost::asio::ip::tcp::acceptor& acceptor);

// Ends a connection (a TLS one after its close_notify). The server first stops sending,
// then reads and drops what the client still sends (for up to 2 s, as nginx's lingering
// close does): closing a socket with unread bytes resets the connection, and the client
// then loses a response it hasn't read yet (Windows discards it; Linux and macOS clients
// still writing the body get a broken pipe or a reset instead of the answer). So a client
// uploading a body the server answers without reading (a retried part or upload answered
// from its first send, a 413, an ingest error) would not see the answer.
boost::asio::awaitable<void> EndConnection(boost::beast::tcp_stream& stream);

// `seconds` as UTC calendar time (gmtime_r; gmtime_s on Windows).
std::tm UtcTime(std::time_t seconds);

// A thread that runs `body` on a stack of `stack_bytes`, whatever stack the platform gives a
// thread by default (macOS 512 KB, Windows 1 MB, Linux the soft RLIMIT_STACK, 8 MB as a rule).
// std::thread cannot say: POSIX threads get the size in their attributes, Windows threads as
// the stack's reserve. An exception that leaves `body` ends the process, as one leaving a
// std::thread's does. Joined by join() or, if not yet, by the destructor.
class StackThread {
 public:
  StackThread(std::size_t stack_bytes, std::function<void()> body);
  ~StackThread();
  StackThread(const StackThread&) = delete;
  StackThread& operator=(const StackThread&) = delete;
  void join();

 private:
  struct Native;
  Native* native_;
};

// The size of the calling thread's stack in bytes (the reserve on Windows), or 0 where the
// platform cannot say.
std::size_t CurrentThreadStackBytes();

}  // namespace chargewire::detail
