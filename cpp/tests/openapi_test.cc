// GET /chargewire/v1/openapi.json (spec §3.3): the SDK serves spec/openapi.yaml as JSON, converted
// at build time, without authentication, cacheable, and advertised in discovery. Each test
// starts the SDK's server in this process with a backend that runs nothing.
#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <algorithm>
#include <cctype>
#include <map>
#include <memory>
#include <string>

#include "openapi_document.hpp"
#include "chargewire/server.hpp"

namespace {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace json = boost::json;

class NoEngine : public chargewire::Backend {
 public:
  chargewire::ServerIdentity Identity() override { return {"openapi-test", "0"}; }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const chargewire::Principal&, const std::string&,
                                                      const json::object&) override {
    throw chargewire::Error(chargewire::ErrorCode::kNotImplemented, "no engine");
  }
};

class NoSignIn : public chargewire::Authenticator {
 public:
  json::array GrantTypes() override { return {"password"}; }
};

struct Answer {
  int status = 0;
  std::map<std::string, std::string> headers;  // lower-case names
  std::string body;
};

Answer Send(uint16_t port, http::verb method, const std::string& target,
            const std::map<std::string, std::string>& headers = {}) {
  net::io_context io;
  beast::tcp_stream stream(io);
  stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  http::request<http::string_body> request{method, target, 11};
  request.set(http::field::host, "127.0.0.1");
  for (const auto& [name, value] : headers) request.set(name, value);
  request.prepare_payload();
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response_parser<http::string_body> parser;
  parser.body_limit(16 * 1024 * 1024);
  http::read(stream, buffer, parser);
  Answer answer;
  answer.status = static_cast<int>(parser.get().result_int());
  for (const auto& field : parser.get()) {
    std::string name(field.name_string());
    for (auto& c : name) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    answer.headers[name] = std::string(field.value());
  }
  answer.body = parser.get().body();
  beast::error_code ec;
  stream.socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
  return answer;
}

class OpenApi : public ::testing::Test {
 protected:
  void SetUp() override {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(std::move(options), std::make_shared<NoEngine>(),
                                                std::make_shared<NoSignIn>());
    server_->Start();
    port_ = server_->port();
  }
  void TearDown() override { server_->Stop(); }

  std::unique_ptr<chargewire::Server> server_;
  uint16_t port_ = 0;
};

TEST_F(OpenApi, TheDocumentIsServedWithoutCredentials) {
  const Answer answer = Send(port_, http::verb::get, "/chargewire/v1/openapi.json");
  ASSERT_EQ(answer.status, 200) << answer.body;
  EXPECT_EQ(answer.headers.at("content-type"), "application/json");
  EXPECT_EQ(answer.headers.at("x-content-type-options"), "nosniff");
  EXPECT_NE(answer.headers.at("cache-control").find("max-age="), std::string::npos);
  EXPECT_EQ(answer.headers.at("cache-control").find("no-store"), std::string::npos);
  EXPECT_EQ(answer.headers.at("etag"), chargewire::OpenApiEtag());
  EXPECT_TRUE(answer.headers.contains("chargewire-request-id"));
  EXPECT_EQ(answer.body, chargewire::OpenApiJson());
}

TEST_F(OpenApi, TheDocumentIsOpenApi31WithEveryEndpoint) {
  const json::object doc = json::parse(chargewire::OpenApiJson()).as_object();
  EXPECT_TRUE(doc.at("openapi").as_string().starts_with("3.1."));
  EXPECT_EQ(doc.at("info").at("title").as_string(), "Chargewire");
  const json::object& paths = doc.at("paths").as_object();
  // Appendix B's 38 endpoints, on 34 paths; the conformance suite compares them one by one.
  EXPECT_EQ(paths.size(), 34u);
  std::size_t operations = 0;
  for (const auto& path : paths) {
    for (const auto& item : path.value().as_object()) {
      if (item.key() != "parameters") operations++;
    }
  }
  EXPECT_EQ(operations, 38u);
  for (const char* path : {"/chargewire", "/chargewire/v1/health", "/chargewire/v1/openapi.json", "/chargewire/v1/auth/token",
                           "/chargewire/v1/sessions/{session_id}/statements"}) {
    EXPECT_TRUE(paths.contains(path)) << path;
  }
  EXPECT_EQ(doc.at("components").at("securitySchemes").at("bearerAuth").at("scheme").as_string(), "bearer");
  // The unauthenticated endpoints say so.
  EXPECT_TRUE(paths.at("/chargewire/v1/openapi.json").at("get").at("security").as_array().empty());
}

TEST_F(OpenApi, AMatchingEtagGets304) {
  const std::string etag(chargewire::OpenApiEtag());
  const Answer same = Send(port_, http::verb::get, "/chargewire/v1/openapi.json", {{"If-None-Match", etag}});
  EXPECT_EQ(same.status, 304);
  EXPECT_TRUE(same.body.empty());
  EXPECT_EQ(same.headers.at("etag"), etag);
  const Answer listed = Send(port_, http::verb::get, "/chargewire/v1/openapi.json",
                             {{"If-None-Match", "\"other\", " + etag}});
  EXPECT_EQ(listed.status, 304);
  const Answer other = Send(port_, http::verb::get, "/chargewire/v1/openapi.json", {{"If-None-Match", "\"other\""}});
  EXPECT_EQ(other.status, 200);
  EXPECT_EQ(other.body, chargewire::OpenApiJson());
}

TEST_F(OpenApi, OnlyGet) {
  const Answer answer = Send(port_, http::verb::post, "/chargewire/v1/openapi.json");
  EXPECT_EQ(answer.status, 405);
  EXPECT_NE(answer.body.find("\"method_not_allowed\""), std::string::npos);
}

TEST_F(OpenApi, DiscoveryAdvertisesIt) {
  const Answer answer = Send(port_, http::verb::get, "/chargewire");
  ASSERT_EQ(answer.status, 200);
  const json::object doc = json::parse(answer.body).as_object();
  const json::array& capabilities = doc.at("capabilities").as_array();
  EXPECT_NE(std::find(capabilities.begin(), capabilities.end(), json::value("openapi")), capabilities.end());
  EXPECT_EQ(doc.at("links").at("openapi").as_string(), "/chargewire/v1/openapi.json");
}

}  // namespace
