// Operating-system differences the SDK hides, kept apart from the protocol code.
#pragma once

#include <boost/asio/ip/tcp.hpp>

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

// `seconds` as UTC calendar time (gmtime_r; gmtime_s on Windows).
std::tm UtcTime(std::time_t seconds);

}  // namespace warpsql::detail
