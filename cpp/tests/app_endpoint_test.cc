// ServerOptions::app_endpoints: an application's own endpoints beside the protocol's, with
// streamed request bodies and streamed answers (Hardcharger's web UI relays other servers
// through one). Each test starts the SDK's server in this process, with a backend that runs
// nothing, and talks plain HTTP/1.1 to it.
#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "chargewire/server.hpp"

namespace {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;

class NoEngine : public chargewire::Backend {
 public:
  chargewire::ServerIdentity Identity() override { return {"app-endpoint-test", "0"}; }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const chargewire::Principal&, const std::string&,
                                                         const boost::json::object&) override {
    throw chargewire::Error(chargewire::ErrorCode::kNotImplemented, "no engine");
  }
};

class NoSignIn : public chargewire::Authenticator {
 public:
  boost::json::array GrantTypes() override { return {"password"}; }
};

struct Answer {
  int status = 0;
  std::map<std::string, std::string> headers;  // lower-case names
  std::string body;
  bool has(const std::string& name) const { return headers.contains(name); }
};

Answer Send(uint16_t port, http::verb method, const std::string& target,
            const std::map<std::string, std::string>& headers = {}, const std::string& body = {},
            bool chunked = false) {
  net::io_context io;
  beast::tcp_stream stream(io);
  stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  http::request<http::string_body> request{method, target, 11};
  request.set(http::field::host, "127.0.0.1");
  for (const auto& [k, v] : headers) request.set(k, v);
  request.body() = body;
  if (chunked) {
    request.chunked(true);
  } else {
    request.prepare_payload();
  }
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response_parser<http::string_body> parser;
  parser.body_limit(256 * 1024 * 1024);
  http::read(stream, buffer, parser);
  Answer answer;
  const auto& message = parser.get();
  answer.status = static_cast<int>(message.result_int());
  for (const auto& field : message) {
    std::string name(field.name_string());
    for (auto& c : name) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    answer.headers[name] = std::string(field.value());
  }
  answer.body = message.body();
  beast::error_code ec;
  stream.socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
  return answer;
}

struct Running {
  std::unique_ptr<chargewire::Server> server;
  uint16_t port = 0;
  ~Running() {
    if (server) server->Stop();
  }
};

std::unique_ptr<Running> Start(chargewire::ServerOptions options) {
  options.host = "127.0.0.1";
  auto running = std::make_unique<Running>();
  running->server = std::make_unique<chargewire::Server>(std::move(options), std::make_shared<NoEngine>(),
                                                         std::make_shared<NoSignIn>());
  running->server->Start();
  running->port = running->server->port();
  return running;
}

// An endpoint that records what it saw, streams a POST's body back as it arrives, and
// answers a GET with a small body.
struct Echo {
  std::mutex mutex;
  std::vector<std::string> seen;  // "METHOD target [principal]"
  std::size_t pieces = 0;

  chargewire::AppEndpoint Endpoint(std::string prefix, bool requires_token) {
    chargewire::AppEndpoint endpoint;
    endpoint.prefix = std::move(prefix);
    endpoint.requires_token = requires_token;
    endpoint.handler = [this](chargewire::AppRequest& request) -> net::awaitable<chargewire::AppResponse> {
      {
        std::lock_guard lock(mutex);
        seen.push_back(request.method + " " + request.target +
                       (request.principal ? " [" + request.principal->name + "]" : ""));
      }
      chargewire::AppResponse response;
      response.content_type = "application/octet-stream";
      response.headers.emplace_back("X-App", "yes");
      if (request.method == "POST" && request.body) {
        // Read whole first, as any HTTP answer follows its request's body, then stream
        // back in the pieces it arrived in.
        auto received = std::make_shared<std::vector<std::string>>();
        while (auto piece = co_await request.body->Next()) received->push_back(std::move(*piece));
        {
          std::lock_guard lock(mutex);
          pieces += received->size();
        }
        response.stream = [received](chargewire::AppBodyWriter& writer) -> net::awaitable<bool> {
          for (const auto& piece : *received) co_await writer.Write(piece);
          co_return true;
        };
      } else {
        response.body = "hello from " + request.path;
      }
      co_return response;
    };
    return endpoint;
  }
};

TEST(AppEndpoint, AnswersUnderItsPrefixAndStreamsAPostBodyBack) {
  Echo echo;
  chargewire::ServerOptions options;
  options.app_endpoints.push_back(echo.Endpoint("/ui/v1/remotes/", false));
  auto running = Start(std::move(options));

  const Answer get = Send(running->port, http::verb::get, "/ui/v1/remotes/r1/chargewire?wait=5&x=a%20b");
  EXPECT_EQ(get.status, 200);
  EXPECT_EQ(get.body, "hello from /ui/v1/remotes/r1/chargewire");
  EXPECT_EQ(get.headers.at("x-app"), "yes");
  EXPECT_TRUE(get.has("chargewire-request-id"));

  // A body larger than the JSON limit, sent with Content-Length and chunked: both stream.
  const std::string big(3 * 1024 * 1024, 'z');
  const Answer whole = Send(running->port, http::verb::post, "/ui/v1/remotes/r1/chargewire/v1/sessions", {},
                            big);
  EXPECT_EQ(whole.status, 200);
  EXPECT_EQ(whole.body.size(), big.size());
  EXPECT_EQ(whole.headers.at("transfer-encoding"), "chunked");
  const Answer chunked = Send(running->port, http::verb::post, "/ui/v1/remotes/r1/chargewire/v1/sessions", {},
                              big, true);
  EXPECT_EQ(chunked.status, 200);
  EXPECT_EQ(chunked.body, big);
  {
    std::lock_guard lock(echo.mutex);
    EXPECT_GT(echo.pieces, 2u);
    ASSERT_EQ(echo.seen.size(), 3u);
    EXPECT_EQ(echo.seen[0], "GET /ui/v1/remotes/r1/chargewire?wait=5&x=a%20b");
    EXPECT_EQ(echo.seen[1], "POST /ui/v1/remotes/r1/chargewire/v1/sessions");
  }
}

TEST(AppEndpoint, TheTokenRuleAndWhatStaysTheServers) {
  Echo open;
  Echo closed;
  chargewire::ServerOptions options;
  options.app_endpoints.push_back(closed.Endpoint("/ui/v1/remotes/", true));
  options.app_endpoints.push_back(open.Endpoint("/ui/v1/open/", false));
  options.plain_handler = [](const chargewire::PlainRequest& request) -> std::optional<chargewire::PlainResponse> {
    if (!request.path.starts_with("/ui/")) return std::nullopt;
    chargewire::PlainResponse response;
    response.body = "page";
    return response;
  };
  auto running = Start(std::move(options));

  // No token where one is required: refused before the handler, as the protocol refuses.
  const Answer none = Send(running->port, http::verb::get, "/ui/v1/remotes/r1/chargewire");
  EXPECT_EQ(none.status, 401);
  EXPECT_NE(none.body.find("\"invalid_token\""), std::string::npos);
  // A bad token is refused everywhere, whatever the endpoint's rule.
  for (const char* path : {"/ui/v1/remotes/r1/chargewire", "/ui/v1/open/x"}) {
    const Answer bad = Send(running->port, http::verb::get, path, {{"Authorization", "Bearer nope"}});
    EXPECT_EQ(bad.status, 401) << path;
  }
  EXPECT_EQ(Send(running->port, http::verb::get, "/ui/v1/open/x").status, 200);
  EXPECT_EQ(Send(running->port, http::verb::delete_, "/ui/v1/open/x").status, 200);
  {
    std::lock_guard lock(closed.mutex);
    EXPECT_TRUE(closed.seen.empty());
  }
  // Outside the prefixes: the plain handler, the protocol, and the server's own 404 and
  // preflight answers, as before.
  EXPECT_EQ(Send(running->port, http::verb::get, "/ui/index.html").body, "page");
  EXPECT_EQ(Send(running->port, http::verb::get, "/ui/v1/remote/x").body, "page");
  EXPECT_EQ(Send(running->port, http::verb::get, "/chargewire/v1/health").status, 200);
  EXPECT_EQ(Send(running->port, http::verb::post, "/nope").status, 404);
  EXPECT_EQ(Send(running->port, http::verb::options, "/ui/v1/open/x").status, 204);
  {
    std::lock_guard lock(open.mutex);
    EXPECT_EQ(open.seen, (std::vector<std::string>{"GET /ui/v1/open/x", "DELETE /ui/v1/open/x"}));
  }
}

}  // namespace
