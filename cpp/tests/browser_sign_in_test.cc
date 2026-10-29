// Browser sign-in (spec §4.3): the flow table on its own, then the endpoints against a
// server started in this process with a provider that stands in for an identity provider.
#include <gtest/gtest.h>
#include <openssl/evp.h>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <boost/url.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "browser_sign_in.hpp"
#include "chargewire/client.hpp"
#include "chargewire/server.hpp"

namespace {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace json = boost::json;

using chargewire::Error;
using chargewire::ErrorCode;
using chargewire::Principal;
using chargewire::detail::BrowserFlows;
using chargewire::detail::ValidReturnTo;

ErrorCode CodeOf(const std::function<void()>& work) {
  try {
    work();
  } catch (const Error& e) {
    return e.code();
  }
  ADD_FAILURE() << "no error";
  return ErrorCode::kExecutionError;
}

// ---- The flow table ----

TEST(ReturnTo, AllowsPathsOnTheServersOwnOrigin) {
  for (const char* ok : {"/", "/ui/", "/ui/?tab=2&x=%2F", "/a/b/c.html", "/ui/;x=1:2@3!$'()*+,~"}) {
    EXPECT_TRUE(ValidReturnTo(ok)) << ok;
  }
}

TEST(ReturnTo, RefusesEverythingABrowserCouldReadAsAnotherHost) {
  for (const char* bad :
       {"", "https://evil.example/", "//evil.example/", "/\\evil.example/", "\\\\evil.example", "/ui\\x",
        "evil.example", "javascript:alert(1)", "/ui/#frag", "/\t/evil.example", "/\n/evil.example",
        "/ui /x", "/ui/\x7f", "/ui/\xc3\xa9", "ui/", "///evil.example"}) {
    EXPECT_FALSE(ValidReturnTo(bad)) << bad;
  }
  EXPECT_FALSE(ValidReturnTo("/" + std::string(2048, 'a')));
  EXPECT_TRUE(ValidReturnTo("/" + std::string(2047, 'a')));
}

TEST(BrowserFlowTable, CreateRefusesABadReturnTo) {
  BrowserFlows flows({});
  EXPECT_EQ(CodeOf([&] { flows.Create("1.2.3.4", "//evil.example/", ""); }), ErrorCode::kInvalidReturnTo);
  EXPECT_EQ(flows.size(), 0u);
}

TEST(BrowserFlowTable, SecretsStateAndPkceAreDistinctAndFresh) {
  BrowserFlows flows({});
  const auto a = flows.Create("1.2.3.4", "", "replica-7.");
  const auto b = flows.Create("1.2.3.4", "", "");
  EXPECT_TRUE(a.flow_id.starts_with("f_"));
  EXPECT_NE(a.flow_id, b.flow_id);
  EXPECT_NE(a.poll_secret, b.poll_secret);
  EXPECT_GE(a.poll_secret.size(), 26u);  // >= 128 bits in base32
  EXPECT_TRUE(a.state.starts_with("replica-7."));
  EXPECT_EQ(a.state.find(a.flow_id), std::string::npos);
  EXPECT_EQ(a.state.find(a.poll_secret), std::string::npos);
  EXPECT_NE(a.nonce, b.nonce);
  EXPECT_EQ(a.code_challenge.size(), 43u);  // base64url SHA-256, no padding

  BrowserFlows::Exchange exchange;
  ASSERT_EQ(flows.BeginCallback(a.state, &exchange), BrowserFlows::CallbackOutcome::kStarted);
  // The challenge is S256 of the verifier the callback gets (RFC 7636 §4.2).
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int length = 0;
  EVP_Digest(exchange.code_verifier.data(), exchange.code_verifier.size(), digest, &length, EVP_sha256(), nullptr);
  std::string encoded(4 * ((length + 2) / 3), '\0');
  encoded.resize(static_cast<std::size_t>(EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()), digest,
                                                          static_cast<int>(length))));
  for (char& c : encoded) c = c == '+' ? '-' : c == '/' ? '_' : c;
  while (encoded.ends_with('=')) encoded.pop_back();
  EXPECT_EQ(encoded, a.code_challenge);
  EXPECT_EQ(exchange.nonce, a.nonce);
  EXPECT_EQ(exchange.flow_id, a.flow_id);
}

TEST(BrowserFlowTable, TheSecretIsRequiredAndTheTokenIsHandedOutOnce) {
  BrowserFlows flows({});
  const auto created = flows.Create("1.2.3.4", "/ui/", "");
  EXPECT_EQ(flows.Collect(created.flow_id, created.poll_secret).state, BrowserFlows::Poll::State::kPending);
  BrowserFlows::Exchange exchange;
  ASSERT_EQ(flows.BeginCallback(created.state, &exchange), BrowserFlows::CallbackOutcome::kStarted);
  EXPECT_EQ(exchange.return_to, "/ui/");
  // While the code is exchanged, still pending; a second callback changes nothing.
  EXPECT_EQ(flows.Collect(created.flow_id, created.poll_secret).state, BrowserFlows::Poll::State::kPending);
  EXPECT_EQ(flows.BeginCallback(created.state, &exchange), BrowserFlows::CallbackOutcome::kNotPending);
  flows.Complete(created.flow_id, Principal{.name = "alice"});

  // Without the right secret it is the same as no flow at all.
  for (const std::string wrong : {std::string(), std::string("nope"), created.flow_id, created.state}) {
    EXPECT_EQ(CodeOf([&] { flows.Collect(created.flow_id, wrong); }), ErrorCode::kFlowNotFound) << wrong;
  }
  EXPECT_EQ(CodeOf([&] { flows.Collect("f_unknown", created.poll_secret); }), ErrorCode::kFlowNotFound);

  // A refusal by `accept` keeps it for the right client.
  EXPECT_EQ(CodeOf([&] {
              flows.Collect(created.flow_id, created.poll_secret,
                            [](const Principal&) { throw Error(ErrorCode::kCertificateIdentityMismatch, "no"); });
            }),
            ErrorCode::kCertificateIdentityMismatch);
  const auto poll = flows.Collect(created.flow_id, created.poll_secret);
  EXPECT_EQ(poll.state, BrowserFlows::Poll::State::kComplete);
  EXPECT_EQ(poll.principal.name, "alice");
  EXPECT_EQ(CodeOf([&] { flows.Collect(created.flow_id, created.poll_secret); }), ErrorCode::kFlowNotFound);
  EXPECT_EQ(flows.BeginCallback(created.state, &exchange), BrowserFlows::CallbackOutcome::kUnknown);
  EXPECT_EQ(flows.size(), 0u);
}

TEST(BrowserFlowTable, AFailureIsReportedOnce) {
  BrowserFlows flows({});
  const auto created = flows.Create("1.2.3.4", "", "");
  BrowserFlows::Exchange exchange;
  ASSERT_EQ(flows.BeginCallback(created.state, &exchange), BrowserFlows::CallbackOutcome::kStarted);
  flows.Fail(created.flow_id, "access_denied", "the user said no");
  flows.Complete(created.flow_id, Principal{.name = "late"});  // too late: stays failed
  const auto poll = flows.Collect(created.flow_id, created.poll_secret);
  EXPECT_EQ(poll.state, BrowserFlows::Poll::State::kFailed);
  EXPECT_EQ(poll.error_code, "access_denied");
  EXPECT_EQ(poll.error_detail, "the user said no");
  EXPECT_EQ(CodeOf([&] { flows.Collect(created.flow_id, created.poll_secret); }), ErrorCode::kFlowNotFound);
}

TEST(BrowserFlowTable, BoundedInAllAndPerAddress) {
  BrowserFlows flows({.lifetime = std::chrono::seconds(600), .max_flows = 5, .max_per_address = 3});
  for (int i = 0; i < 3; i++) flows.Create("10.0.0.1", "", "");
  EXPECT_EQ(CodeOf([&] { flows.Create("10.0.0.1", "", ""); }), ErrorCode::kRateLimited);
  flows.Create("10.0.0.2", "", "");
  const auto last = flows.Create("10.0.0.3", "", "");
  EXPECT_EQ(CodeOf([&] { flows.Create("10.0.0.4", "", ""); }), ErrorCode::kRateLimited);
  // Collecting a finished flow frees its place.
  BrowserFlows::Exchange exchange;
  flows.BeginCallback(last.state, &exchange);
  flows.Fail(last.flow_id, "access_denied", "no");
  flows.Collect(last.flow_id, last.poll_secret);
  flows.Create("10.0.0.4", "", "");
  EXPECT_EQ(flows.size(), 5u);
}

TEST(BrowserFlowTable, FlowsExpire) {
  BrowserFlows flows({.lifetime = std::chrono::seconds(1), .max_flows = 10, .max_per_address = 0});
  const auto created = flows.Create("1.2.3.4", "", "");
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  BrowserFlows::Exchange exchange;
  EXPECT_EQ(flows.BeginCallback(created.state, &exchange), BrowserFlows::CallbackOutcome::kUnknown);
  EXPECT_EQ(CodeOf([&] { flows.Collect(created.flow_id, created.poll_secret); }), ErrorCode::kFlowNotFound);
  EXPECT_EQ(flows.size(), 0u);
}

// ---- The endpoints ----

class NoEngine : public chargewire::Backend {
 public:
  chargewire::ServerIdentity Identity() override { return {"browser-sign-in-test", "0"}; }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const Principal&, const std::string&,
                                                      const json::object&) override {
    throw Error(ErrorCode::kNotImplemented, "no engine");
  }
};

class NoSignIn : public chargewire::Authenticator {
 public:
  json::array GrantTypes() override { return {"password"}; }
};

// Stands in for an identity provider: the "authorize URL" carries what the SDK gave it, and
// the code decides the outcome ("good": alice, "refused": invalid_credentials, else a
// failure to reach the provider).
class FakeProvider : public chargewire::BrowserSignInProvider {
 public:
  std::string DisplayName() override { return "Fake IdP"; }
  std::string AuthorizeUrl(const Authorization& a) override {
    return "https://idp.example/authorize?response_type=code&state=" + a.state +
           "&code_challenge=" + a.code_challenge + "&code_challenge_method=S256&nonce=" + a.nonce;
  }
  Principal CompleteSignIn(const Callback& callback) override {
    std::lock_guard lock(mutex);
    calls.push_back(callback);
    if (callback.code == "good") return Principal{.name = "alice@example.com", .role = "admin"};
    if (callback.code == "refused") throw Error(ErrorCode::kInvalidCredentials, "User 'mallory' is not authorized");
    throw std::runtime_error("the identity provider could not be reached");
  }
  std::string PublicOrigin() override { return origin; }
  std::mutex mutex;
  std::vector<Callback> calls;
  std::string origin;
};

struct Answer {
  int status = 0;
  std::map<std::string, std::string> headers;  // lower-case names
  std::string body;
  json::object Json() const { return json::parse(body).as_object(); }
};

Answer Send(uint16_t port, http::verb method, const std::string& target, const std::string& body = "") {
  net::io_context io;
  beast::tcp_stream stream(io);
  stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  http::request<http::string_body> request{method, target, 11};
  request.set(http::field::host, "127.0.0.1");
  if (!body.empty()) {
    request.set(http::field::content_type, "application/json");
    request.body() = body;
  }
  request.prepare_payload();
  http::write(stream, request);
  beast::flat_buffer buffer;
  http::response<http::string_body> response;
  http::read(stream, buffer, response);
  Answer answer;
  answer.status = static_cast<int>(response.result_int());
  for (const auto& field : response) {
    std::string name(field.name_string());
    for (auto& c : name) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    answer.headers[name] = std::string(field.value());
  }
  answer.body = response.body();
  beast::error_code ec;
  stream.socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
  return answer;
}

std::string QueryParam(const std::string& url, const std::string& name) {
  auto parsed = boost::urls::parse_uri(url);
  auto it = parsed->params().find(name);
  return it == parsed->params().end() ? "" : std::string((*it).value);
}

class BrowserSignInServer : public ::testing::Test {
 protected:
  void SetUp() override {
    chargewire::ServerOptions options;
    options.browser_sign_in = provider_;
    server_ = std::make_unique<chargewire::Server>(options, std::make_shared<NoEngine>(), std::make_shared<NoSignIn>());
    server_->Start();
    port_ = server_->port();
  }
  void TearDown() override {
    server_->Stop();
    server_->Wait();
  }
  json::object NewFlow(const std::string& body = "") {
    const Answer created = Send(port_, http::verb::post, "/chargewire/v1/auth/oauth/flows", body);
    EXPECT_EQ(created.status, 201) << created.body;
    EXPECT_EQ(created.headers.at("cache-control"), "no-store");
    return created.Json();
  }
  Answer Poll(const json::object& flow, const std::string& secret) {
    return Send(port_, http::verb::post, "/chargewire/v1/auth/oauth/flows/" + std::string(flow.at("flow_id").as_string()),
                json::serialize(json::object{{"poll_secret", secret}}));
  }
  Answer Callback(const json::object& flow, const std::string& query) {
    const std::string state = QueryParam(std::string(flow.at("authorize_url").as_string()), "state");
    return Send(port_, http::verb::get, "/chargewire/v1/auth/oauth/callback?state=" + state + query);
  }

  std::shared_ptr<FakeProvider> provider_ = std::make_shared<FakeProvider>();
  std::unique_ptr<chargewire::Server> server_;
  uint16_t port_ = 0;
};

TEST_F(BrowserSignInServer, DiscoveryAdvertisesItWithTheProvidersName) {
  const auto doc = Send(port_, http::verb::get, "/chargewire").Json();
  const auto& capabilities = doc.at("capabilities").as_array();
  EXPECT_NE(std::find(capabilities.begin(), capabilities.end(), json::value("oauth_browser")), capabilities.end());
  EXPECT_EQ(doc.at("auth").at("oauth_browser").at("provider").as_string(), "Fake IdP");
}

TEST_F(BrowserSignInServer, CompleteFlowWithoutReturnTo) {
  const auto flow = NewFlow();
  const std::string secret(flow.at("poll_secret").as_string());
  const std::string url(flow.at("authorize_url").as_string());
  EXPECT_EQ(url.find(secret), std::string::npos);
  EXPECT_EQ(flow.at("poll_interval_s").as_int64(), 2);
  EXPECT_EQ(Poll(flow, secret).Json().at("state").as_string(), "pending");

  const Answer page = Callback(flow, "&code=good");
  EXPECT_EQ(page.status, 200);
  EXPECT_NE(page.body.find("signed in to browser-sign-in-test as alice@example.com"), std::string::npos) << page.body;
  EXPECT_EQ(page.headers.at("cache-control"), "no-store");
  EXPECT_EQ(page.headers.at("referrer-policy"), "no-referrer");
  EXPECT_EQ(page.headers.at("x-frame-options"), "DENY");
  EXPECT_TRUE(page.headers.at("content-security-policy").starts_with("default-src 'none'"));
  EXPECT_EQ(page.body.find("<script"), std::string::npos);
  {
    std::lock_guard lock(provider_->mutex);
    ASSERT_EQ(provider_->calls.size(), 1u);
    EXPECT_EQ(provider_->calls[0].nonce, QueryParam(url, "nonce"));
  }

  const Answer wrong = Poll(flow, "not-the-secret");
  EXPECT_EQ(wrong.status, 404);
  EXPECT_EQ(wrong.Json().at("code").as_string(), "flow_not_found");
  const Answer done = Poll(flow, secret);
  EXPECT_EQ(done.status, 200) << done.body;
  EXPECT_EQ(done.headers.at("cache-control"), "no-store");
  EXPECT_EQ(done.Json().at("state").as_string(), "complete");
  EXPECT_EQ(done.Json().at("token_type").as_string(), "Bearer");
  EXPECT_FALSE(done.Json().at("access_token").as_string().empty());
  EXPECT_EQ(Poll(flow, secret).status, 404);
  // A second callback (a replayed redirect) finds nothing.
  EXPECT_EQ(Callback(flow, "&code=good").status, 400);
}

TEST_F(BrowserSignInServer, ReturnToGetsTheFlowIdInTheFragmentOnly) {
  const auto flow = NewFlow(R"({"return_to": "/ui/?tab=1"})");
  const Answer redirect = Callback(flow, "&code=good");
  EXPECT_EQ(redirect.status, 303);
  EXPECT_EQ(redirect.headers.at("location"),
            "/ui/?tab=1#chargewire_oauth_flow=" + std::string(flow.at("flow_id").as_string()));
  EXPECT_EQ(redirect.headers.at("referrer-policy"), "no-referrer");
  EXPECT_EQ(Poll(flow, std::string(flow.at("poll_secret").as_string())).Json().at("state").as_string(), "complete");
}

TEST_F(BrowserSignInServer, ReturnToIsAbsoluteOnAConfiguredPublicOrigin) {
  provider_->origin = "https://db.example.com";
  const auto flow = NewFlow(R"({"return_to": "/ui/"})");
  EXPECT_EQ(Callback(flow, "&code=good").headers.at("location"),
            "https://db.example.com/ui/#chargewire_oauth_flow=" + std::string(flow.at("flow_id").as_string()));
}

TEST_F(BrowserSignInServer, OpenRedirectsAreRefused) {
  for (const char* bad : {"https://evil.example/", "//evil.example", "/\\evil.example", "/ui/#x", "javascript:x"}) {
    const Answer refused =
        Send(port_, http::verb::post, "/chargewire/v1/auth/oauth/flows", json::serialize(json::object{{"return_to", bad}}));
    EXPECT_EQ(refused.status, 400) << bad;
    EXPECT_EQ(refused.Json().at("code").as_string(), "invalid_return_to") << bad;
  }
}

TEST_F(BrowserSignInServer, FailuresAreReportedThroughThePoll) {
  struct Case {
    std::string query, code, detail;
  };
  for (const auto& c : {Case{"&error=access_denied&error_description=%3Cscript%3Ealert(1)%3C/script%3E",
                             "access_denied", "access_denied: <script>alert(1)</script>"},
                        Case{"&code=refused", "invalid_credentials", "User 'mallory' is not authorized"},
                        Case{"&code=other", "identity_provider_error", "the identity provider could not be reached"},
                        Case{"", "identity_provider_error", "the identity provider's redirect carried no authorization code"}}) {
    const auto flow = NewFlow();
    const Answer page = Callback(flow, c.query);
    EXPECT_EQ(page.body.find("<script>"), std::string::npos) << page.body;
    const Answer poll = Poll(flow, std::string(flow.at("poll_secret").as_string()));
    const auto doc = poll.Json();
    EXPECT_EQ(doc.at("state").as_string(), "failed") << c.query;
    EXPECT_EQ(doc.at("error").at("code").as_string(), c.code) << c.query;
    EXPECT_EQ(doc.at("error").at("detail").as_string(), c.detail) << c.query;
  }
}

TEST_F(BrowserSignInServer, MalformedPollsAndWrongMethods) {
  const auto flow = NewFlow();
  const std::string target = "/chargewire/v1/auth/oauth/flows/" + std::string(flow.at("flow_id").as_string());
  EXPECT_EQ(Send(port_, http::verb::post, target, "{}").Json().at("code").as_string(), "bad_request");
  EXPECT_EQ(Send(port_, http::verb::get, target).status, 405);
  EXPECT_EQ(Send(port_, http::verb::get, "/chargewire/v1/auth/oauth/flows").status, 405);
  EXPECT_EQ(Send(port_, http::verb::post, "/chargewire/v1/auth/oauth/callback").status, 405);
  EXPECT_EQ(Callback(flow, "&code=good").status, 200);
}

// The client library's browser sign-in (libchargewire_client): `open` plays the browser.
TEST_F(BrowserSignInServer, ClientSignsInWithTheBrowser) {
  chargewire::client::Client client({.host = "127.0.0.1", .port = port_});
  EXPECT_EQ(chargewire::client::Client::BrowserSignInProviderName(client.Discover()), "Fake IdP");
  std::string opened;
  client.SignInWithBrowser([&](const std::string& url) {
    opened = url;
    const std::string state = QueryParam(url, "state");
    EXPECT_EQ(Send(port_, http::verb::get, "/chargewire/v1/auth/oauth/callback?code=good&state=" + state).status, 200);
  });
  EXPECT_TRUE(opened.starts_with("https://idp.example/authorize?"));
  EXPECT_TRUE(client.SignedIn());

  // Step by step: pending, then the provider's refusal as an error with its code.
  chargewire::client::Client refused({.host = "127.0.0.1", .port = port_});
  const auto flow = refused.StartBrowserSignIn();
  EXPECT_FALSE(refused.PollBrowserSignIn(flow));
  Send(port_, http::verb::get, "/chargewire/v1/auth/oauth/callback?code=refused&state=" + QueryParam(flow.authorize_url, "state"));
  try {
    refused.PollBrowserSignIn(flow);
    ADD_FAILURE() << "no error";
  } catch (const chargewire::client::Error& e) {
    EXPECT_EQ(e.code(), "invalid_credentials");
    EXPECT_EQ(e.status(), 401);
  }
  try {
    refused.PollBrowserSignIn(flow);
    ADD_FAILURE() << "no error";
  } catch (const chargewire::client::Error& e) {
    EXPECT_EQ(e.code(), "flow_not_found");
  }
  EXPECT_FALSE(refused.SignedIn());
}

TEST(BrowserSignInOff, NotImplementedWithoutAProvider) {
  chargewire::Server server(chargewire::ServerOptions{}, std::make_shared<NoEngine>(), std::make_shared<NoSignIn>());
  server.Start();
  const Answer answer = Send(server.port(), http::verb::post, "/chargewire/v1/auth/oauth/flows", "{}");
  EXPECT_EQ(answer.status, 501);
  EXPECT_EQ(answer.Json().at("code").as_string(), "not_implemented");
  const auto doc = Send(server.port(), http::verb::get, "/chargewire").Json();
  EXPECT_FALSE(doc.at("auth").as_object().contains("oauth_browser"));
  server.Stop();
  server.Wait();
}

}  // namespace
