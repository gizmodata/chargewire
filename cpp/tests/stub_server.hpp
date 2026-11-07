// Tests only: a Chargewire stand-in on 127.0.0.1 that answers each request with whatever the
// test's handler says, and records the requests it got.
#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

#include "chargewire/client.hpp"

namespace chargewire_test_stub {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;

struct Seen {
  std::string method;
  std::string target;
  std::string body;
  std::string authorization;  // the Authorization header, if any
};

struct Answer {
  int status = 200;
  std::string content_type = "application/json";
  std::string body;
  std::map<std::string, std::string> headers;
};

// One problem document of server_resuming, as a stand-in sends it; `retry_after` empty: no
// Retry-After header.
inline Answer Resuming(const std::string& retry_after = "1") {
  Answer answer;
  answer.status = 503;
  answer.content_type = "application/problem+json";
  const std::string seconds = retry_after.empty() ? "3" : retry_after;
  answer.body = boost::json::serialize(boost::json::object{
      {"type", "urn:chargewire:error:server_resuming"},
      {"title", "Server resuming"},
      {"status", 503},
      {"code", "server_resuming"},
      {"detail", "the server is resuming; retry after " + seconds + " s"},
      {"request_id", "r_resume"}});
  if (!retry_after.empty()) answer.headers["Retry-After"] = retry_after;
  return answer;
}

inline Answer Discovery() {
  return {200, "application/json",
          R"({"protocol":"chargewire","protocol_versions":["1"],"server":{"name":"stub","version":"0"},)"
          R"("capabilities":["ingest"],"auth":{"grant_types":["password"]}})",
          {}};
}

inline Answer Token() { return {200, "application/json", R"({"access_token":"t0k","token_type":"Bearer","expires_in":3600})", {}}; }

inline Answer Created() {
  return {201, "application/json", R"({"session_id":"s1","idle_timeout_s":900,"options":{},"transaction":"none"})", {}};
}

// A Chargewire stand-in on 127.0.0.1: every request goes to `handler` (by method and path,
// with how many such requests came before), and is recorded. Connections are kept alive
// as the client asks.
class StubServer {
 public:
  using Handler = std::function<Answer(const Seen& request, int nth)>;

  explicit StubServer(Handler handler)
      : handler_(std::move(handler)), acceptor_(io_, {net::ip::make_address("127.0.0.1"), 0}) {
    port_ = acceptor_.local_endpoint().port();
    thread_ = std::thread([this] { Run(); });
  }

  ~StubServer() {
    stopping_ = true;
    {
      std::lock_guard lock(mutex_);
      for (auto* socket : open_) {
        boost::system::error_code ignored;
        socket->shutdown(tcp::socket::shutdown_both, ignored);
      }
    }
    try {  // wake the accept
      net::io_context io;
      tcp::socket poke(io);
      poke.connect({net::ip::make_address("127.0.0.1"), port_});
    } catch (...) {
    }
    thread_.join();
    for (auto& connection : connections_) connection.join();
  }

  uint16_t port() const { return port_; }

  chargewire::client::ConnectOptions Options() const {
    chargewire::client::ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = port_;
    return options;
  }

  std::vector<Seen> Requests() {
    std::lock_guard lock(mutex_);
    return seen_;
  }
  // How many requests came with this method and path (query ignored).
  int Count(const std::string& method, const std::string& path) {
    std::lock_guard lock(mutex_);
    int n = 0;
    for (const auto& s : seen_) {
      if (s.method == method && s.target.substr(0, s.target.find('?')) == path) n++;
    }
    return n;
  }

 private:
  // Each connection on a thread of its own: a client keeps several open (its sign-in's,
  // each session's).
  void Run() {
    while (!stopping_) {
      auto socket = std::make_shared<tcp::socket>(io_);
      boost::system::error_code ec;
      acceptor_.accept(*socket, ec);
      if (ec || stopping_) break;
      std::lock_guard lock(mutex_);
      open_.push_back(socket.get());
      connections_.emplace_back([this, socket] {
        Serve(*socket);
        std::lock_guard lock(mutex_);
        std::erase(open_, socket.get());
      });
    }
  }

  void Serve(tcp::socket& socket) {
    beast::flat_buffer buffer;
    while (!stopping_) {
      http::request<http::string_body> request;
      boost::system::error_code ec;
      http::read(socket, buffer, request, ec);
      if (ec) return;
      Seen seen{std::string(request.method_string()), std::string(request.target()), request.body(),
                std::string(request[http::field::authorization])};
      int nth = 0;
      {
        std::lock_guard lock(mutex_);
        for (const auto& s : seen_) {
          if (s.method == seen.method && s.target.substr(0, s.target.find('?')) ==
                                             seen.target.substr(0, seen.target.find('?'))) {
            nth++;
          }
        }
        seen_.push_back(seen);
      }
      Answer answer = handler_(seen, nth);
      http::response<http::string_body> response(static_cast<http::status>(answer.status), 11);
      response.set(http::field::content_type, answer.content_type);
      for (const auto& [name, value] : answer.headers) response.set(name, value);
      response.body() = answer.body;
      response.keep_alive(request.keep_alive());
      response.prepare_payload();
      http::write(socket, response, ec);
      if (ec || !request.keep_alive()) return;
    }
  }

  Handler handler_;
  net::io_context io_;
  tcp::acceptor acceptor_;
  uint16_t port_ = 0;
  std::atomic<bool> stopping_ = false;
  std::mutex mutex_;
  std::vector<tcp::socket*> open_;
  std::vector<Seen> seen_;
  std::thread thread_;
  std::vector<std::thread> connections_;
};

}  // namespace chargewire_test_stub
