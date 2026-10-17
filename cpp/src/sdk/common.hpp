// Names shared by the SDK's internals.
#pragma once

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/json.hpp>

#include <chrono>

namespace warpsql::detail {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace json = boost::json;

using Clock = std::chrono::steady_clock;
using WallClock = std::chrono::system_clock;

template <typename T>
using Task = net::awaitable<T>;

}  // namespace warpsql::detail
