// Handles belong to a principal's name AND its authority (spec §9.2, Principal::authority_key):
// a token of the same name whose authority differs (a readonly or scoped token of the user,
// say) gets session_not_found on every route of another authority's session, as another user
// would; a token with the same name and authority (a new sign-in) reaches it. Against a
// server in this process, whose Authenticator gives each password its own authority.
#include <gtest/gtest.h>

#include <arrow/api.h>
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "chargewire/server.hpp"

namespace {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace json = boost::json;

using chargewire::Principal;

class OneRowSession : public chargewire::EngineSession {
 public:
  chargewire::StatementOutcome Execute(std::string_view, const json::value&) override {
    arrow::Int64Builder builder;
    (void)builder.Append(1);
    std::shared_ptr<arrow::Array> values;
    (void)builder.Finish(&values);
    auto schema = arrow::schema({arrow::field("v", arrow::int64())});
    auto batch = arrow::RecordBatch::Make(schema, 1, {values});
    return {.rows = *arrow::RecordBatchReader::Make({batch}, schema)};
  }
};

class Engine : public chargewire::Backend {
 public:
  chargewire::ServerIdentity Identity() override { return {"session-authority-test", "0"}; }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const Principal&, const std::string&,
                                                      const json::object&) override {
    return std::make_unique<OneRowSession>();
  }
};

// The password names the authority: "full" and "readonly" are two authorities of one user;
// "legacy" sets none (a backend that predates authority_key).
class AuthorityByPassword : public chargewire::Authenticator {
 public:
  json::array GrantTypes() override { return {"password"}; }
  Principal AuthenticatePassword(std::string_view username, std::string_view password) override {
    Principal principal{.name = std::string(username), .role = std::string(password)};
    if (password != "legacy") principal.authority_key = "authority-of-" + std::string(password);
    return principal;
  }
};

struct Answer {
  int status = 0;
  std::string body;
  std::string code() const {
    boost::system::error_code failed;
    const auto parsed = json::parse(body, failed);  // a result or a 204 is not JSON
    const auto* code = parsed.is_object() ? parsed.as_object().if_contains("code") : nullptr;
    return code && code->is_string() ? std::string(code->as_string()) : std::string();
  }
};

class SessionAuthority : public ::testing::Test {
 protected:
  void SetUp() override {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(options, std::make_shared<Engine>(),
                                                std::make_shared<AuthorityByPassword>());
    server_->Start();
  }
  void TearDown() override {
    server_->Stop();
    server_->Wait();
  }

  Answer Send(http::verb method, const std::string& target, const std::string& token, const std::string& body = {},
              const std::vector<std::pair<std::string, std::string>>& headers = {}) {
    net::io_context io;
    beast::tcp_stream stream(io);
    stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), server_->port()));
    http::request<http::string_body> request{method, target, 11};
    request.set(http::field::host, "127.0.0.1");
    if (!token.empty()) request.set(http::field::authorization, "Bearer " + token);
    for (const auto& [name, value] : headers) request.insert(name, value);
    if (!body.empty()) {
      request.set(http::field::content_type, "application/json");
      request.body() = body;
    }
    request.prepare_payload();
    http::write(stream, request);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    http::read(stream, buffer, response);
    beast::error_code ec;
    stream.socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
    return {static_cast<int>(response.result_int()), response.body()};
  }

  std::string SignIn(const std::string& password) {
    const Answer answer = Send(http::verb::post, "/chargewire/v1/auth/token", "",
                               json::serialize(json::object{{"grant_type", "password"},
                                                            {"username", "alice"},
                                                            {"password", password}}));
    EXPECT_EQ(answer.status, 200) << answer.body;
    return std::string(json::parse(answer.body).at("access_token").as_string());
  }
  std::string OpenSession(const std::string& token) {
    const Answer answer = Send(http::verb::post, "/chargewire/v1/sessions", token, "{}");
    EXPECT_EQ(answer.status, 201) << answer.body;
    return std::string(json::parse(answer.body).at("session_id").as_string());
  }
  // A statement of the session, its result retained (a JSON submission's default).
  std::string Submit(const std::string& token, const std::string& session, const std::string& key = {}) {
    std::vector<std::pair<std::string, std::string>> headers;
    if (!key.empty()) headers.emplace_back("Idempotency-Key", key);
    const Answer answer = Send(http::verb::post, "/chargewire/v1/sessions/" + session + "/statements", token,
                               R"({"sql": "SELECT 1"})", headers);
    EXPECT_TRUE(answer.status == 200 || answer.status == 201 || answer.status == 202) << answer.body;
    return std::string(json::parse(answer.body).at("statement_id").as_string());
  }

  // Every route of `session` (statement `statement` of it): what `token` gets on each.
  std::vector<std::pair<std::string, Answer>> EveryRoute(const std::string& token, const std::string& session,
                                                         const std::string& statement) {
    const std::string s = "/chargewire/v1/sessions/" + session;
    const std::string q = s + "/statements/" + statement;
    std::vector<std::pair<std::string, Answer>> out;
    auto add = [&](std::string name, Answer answer) { out.emplace_back(std::move(name), std::move(answer)); };
    add("GET session", Send(http::verb::get, s, token));
    add("PATCH session", Send(http::verb::patch, s, token, R"({"options": {}})"));
    add("submit", Send(http::verb::post, s + "/statements", token, R"({"sql": "SELECT 1"})"));
    add("GET statement", Send(http::verb::get, q, token));
    add("GET result", Send(http::verb::get, q + "/result", token, {},
                           {{"Accept", "application/vnd.apache.arrow.stream"}}));
    add("cancel statement", Send(http::verb::post, q + "/cancel", token, "{}"));
    add("prepare", Send(http::verb::post, s + "/prepared", token, R"({"sql": "SELECT 1"})"));
    add("metadata", Send(http::verb::get, s + "/metadata/catalogs", token));
    add("open ingest", Send(http::verb::post, s + "/ingests", token, R"({"table": "t", "mode": "create"})"));
    add("commit", Send(http::verb::post, s + "/commit", token, "{}"));
    add("cancel session", Send(http::verb::post, s + "/cancel", token, "{}"));
    add("DELETE statement", Send(http::verb::delete_, q, token));
    add("DELETE session", Send(http::verb::delete_, s, token));
    return out;
  }

  std::unique_ptr<chargewire::Server> server_;
};

TEST_F(SessionAuthority, ATokenOfTheSameNameWithAnotherAuthorityGetsSessionNotFoundOnEveryRoute) {
  const std::string full = SignIn("full");
  const std::string readonly = SignIn("readonly");
  const std::string session = OpenSession(full);
  const std::string statement = Submit(full, session);
  for (const auto& [route, answer] : EveryRoute(readonly, session, statement)) {
    EXPECT_EQ(answer.status, 404) << route << ": " << answer.body;
    EXPECT_EQ(answer.code(), "session_not_found") << route << ": " << answer.body;
  }
  // And the session, its statement and its result are all still there for their owner.
  EXPECT_EQ(Send(http::verb::get, "/chargewire/v1/sessions/" + session, full).status, 200);
  const Answer result = Send(http::verb::get, "/chargewire/v1/sessions/" + session + "/statements/" + statement + "/result",
                             full, {}, {{"Accept", "application/vnd.apache.arrow.stream"}});
  EXPECT_EQ(result.status, 200) << result.body;
}

// The positive twin: a second sign-in with the same authority (a new token, as after a
// refresh) reaches every route of the session, so the 404s above are the authority's.
TEST_F(SessionAuthority, ANewTokenWithTheSameAuthorityReachesTheSession) {
  const std::string first = SignIn("full");
  const std::string second = SignIn("full");
  ASSERT_NE(first, second);
  const std::string session = OpenSession(first);
  const std::string statement = Submit(first, session);
  for (const auto& [route, answer] : EveryRoute(second, session, statement)) {
    // Past the ownership check: the route's own answer (this engine has no prepared
    // statements, metadata, ingest or transactions, so those are not_implemented).
    EXPECT_NE(answer.status, 404) << route << ": " << answer.body;
    EXPECT_NE(answer.code(), "session_not_found") << route << ": " << answer.body;
  }
}

// A backend that sets no authority_key keeps handles by name, as before it existed.
TEST_F(SessionAuthority, WithoutAnAuthorityKeyHandlesBelongToTheName) {
  const std::string session = OpenSession(SignIn("legacy"));
  EXPECT_EQ(Send(http::verb::get, "/chargewire/v1/sessions/" + session, SignIn("legacy")).status, 200);
  // ... but an empty key is an authority of its own: a token that has one doesn't match it.
  const Answer other = Send(http::verb::get, "/chargewire/v1/sessions/" + session, SignIn("full"));
  EXPECT_EQ(other.status, 404) << other.body;
  EXPECT_EQ(other.code(), "session_not_found");
}

// Idempotency keys (spec §6.7) are the principal's: another authority's request with the
// same key and body runs as a new statement in its own session, rather than being answered
// with (or refused over) the other authority's statement.
TEST_F(SessionAuthority, AnIdempotencyKeyIsScopedToTheAuthority) {
  const std::string full = SignIn("full");
  const std::string readonly = SignIn("readonly");
  const std::string full_session = OpenSession(full);
  const std::string readonly_session = OpenSession(readonly);
  const std::string original = Submit(full, full_session, "key-1");
  EXPECT_EQ(Submit(full, full_session, "key-1"), original);  // the owner's retry: replayed
  const Answer other = Send(http::verb::post, "/chargewire/v1/sessions/" + readonly_session + "/statements", readonly,
                            R"({"sql": "SELECT 1"})", {{"Idempotency-Key", "key-1"}});
  ASSERT_LT(other.status, 300) << other.body;
  EXPECT_NE(std::string(json::parse(other.body).at("statement_id").as_string()), original) << other.body;
}

}  // namespace
