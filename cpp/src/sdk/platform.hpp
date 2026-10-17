// Operating-system differences the SDK hides, kept apart from the protocol code.
#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <ctime>

namespace warpsql::detail {

// Whether the peer of a connected socket has gone (shut down its side, reset, or
// errored), without consuming anything. Never blocks.
bool PeerClosed(boost::asio::ip::tcp::socket& socket);

// Options a listening socket takes before bind: on POSIX SO_REUSEADDR, so a restarted
// server rebinds a port with connections in TIME_WAIT. Nothing on Windows: there
// SO_REUSEADDR would let a second server bind a port this one listens on, and no option
// is needed to rebind past TIME_WAIT.
void PrepareListener(boost::asio::ip::tcp::acceptor& acceptor);

// Ends a plain connection. On Windows the server first stops sending, then reads and
// drops what the client still sends (for a moment, as nginx's lingering close does):
// closing a socket with unread bytes resets the connection, and a Windows client
// discards a response it hasn't read yet when the reset arrives, so a client still
// uploading a body the server refused (413) would never see the refusal. Elsewhere it
// shuts both directions down at once, as before.
boost::asio::awaitable<void> EndConnection(boost::beast::tcp_stream& stream);

// `seconds` as UTC calendar time (gmtime_r; gmtime_s on Windows).
std::tm UtcTime(std::time_t seconds);

}  // namespace warpsql::detail
