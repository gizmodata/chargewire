// Client addresses behind trusted proxies (ServerOptions::trusted_proxies): parsing the
// networks, reading X-Forwarded-For and Forwarded (RFC 7239), and, against a server in this
// process, that only a trusted peer's headers count, for session activity and the
// per-address bound on browser sign-ins.
#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "forwarded.hpp"
#include "security_events.hpp"
#include "warpsql/server.hpp"

namespace {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace json = boost::json;

using warpsql::IpNetwork;
using warpsql::ParseIpNetwork;
using warpsql::detail::ForwardedClient;
using warpsql::detail::PeerHost;

std::vector<IpNetwork> Networks(const std::vector<std::string>& texts) {
  std::vector<IpNetwork> out;
  for (const auto& text : texts) out.push_back(ParseIpNetwork(text));
  return out;
}

// The forwarded client for these headers (name, value; repeated names are separate
// instances), or "" when the TCP peer stands.
std::string Client(const std::vector<std::pair<std::string, std::string>>& headers,
                   const std::vector<std::string>& trusted = {"10.0.0.0/8", "fd00::/8"}) {
  http::fields fields;
  for (const auto& [name, value] : headers) fields.insert(name, value);
  return ForwardedClient(fields, Networks(trusted)).value_or("");
}

TEST(IpNetwork, ParsesBlocksAndBareAddresses) {
  EXPECT_EQ(ParseIpNetwork("10.0.0.0/8").ToString(), "10.0.0.0/8");
  EXPECT_EQ(ParseIpNetwork("192.0.2.7").ToString(), "192.0.2.7/32");
  EXPECT_EQ(ParseIpNetwork("2001:db8::/32").ToString(), "2001:db8::/32");
  EXPECT_EQ(ParseIpNetwork("::1").ToString(), "::1/128");
  EXPECT_EQ(ParseIpNetwork("0.0.0.0/0").ToString(), "0.0.0.0/0");
  EXPECT_EQ(ParseIpNetwork(" 10.0.0.0/8 ").ToString(), "10.0.0.0/8");
  // An IPv4-mapped block is the IPv4 one.
  const IpNetwork mapped = ParseIpNetwork("::ffff:10.0.0.0/104");
  EXPECT_FALSE(mapped.v6);
  EXPECT_EQ(mapped.ToString(), "10.0.0.0/8");
}

TEST(IpNetwork, RefusesWhatIsNotANetworkWithAClearMessage) {
  for (const char* bad : {"", "10.0.0.0/", "10.0.0.0/33", "2001:db8::/129", "10.0.0.0/-1", "10.0.0.0/8/8",
                          "10.0.0/8", "example.com", "10.0.0.0 /8", "fe80::1%eth0", "10.0.0.0/+8", "1.2.3.4:80",
                          "[::1]", "10.0.0.0/08x", "127.1", "2130706433", "0x7f000001", "010.0.0.0/8",
                          "256.0.0.1", "1.2.3.4.", "1..2.3", "::ffff:127.1", "::ffff:192.000.2.1"}) {
    EXPECT_THROW(ParseIpNetwork(bad), std::invalid_argument) << bad;
  }
  try {
    ParseIpNetwork("10.1.2.3/8");
    FAIL();
  } catch (const std::invalid_argument& e) {
    EXPECT_NE(std::string(e.what()).find("did you mean 10.0.0.0/8?"), std::string::npos) << e.what();
  }
  try {
    ParseIpNetwork("10.0.0.0/40");
    FAIL();
  } catch (const std::invalid_argument& e) {
    EXPECT_NE(std::string(e.what()).find("'10.0.0.0/40'"), std::string::npos) << e.what();
    EXPECT_NE(std::string(e.what()).find("0-32"), std::string::npos) << e.what();
  }
}

TEST(IpNetwork, Contains) {
  const IpNetwork v4 = ParseIpNetwork("10.0.0.0/8");
  EXPECT_TRUE(v4.Contains("10.255.1.2"));
  EXPECT_TRUE(v4.Contains("::ffff:10.1.2.3"));  // as a dual-stack socket reports it
  EXPECT_FALSE(v4.Contains("11.0.0.1"));
  EXPECT_FALSE(v4.Contains("::1"));
  EXPECT_FALSE(v4.Contains("not an address"));
  const IpNetwork odd = ParseIpNetwork("192.168.4.0/22");
  EXPECT_TRUE(odd.Contains("192.168.7.255"));
  EXPECT_FALSE(odd.Contains("192.168.8.0"));
  const IpNetwork v6 = ParseIpNetwork("2001:db8:1::/48");
  EXPECT_TRUE(v6.Contains("2001:db8:1:ffff::1"));
  EXPECT_FALSE(v6.Contains("2001:db8:2::1"));
  EXPECT_FALSE(v6.Contains("10.0.0.1"));
  EXPECT_TRUE(ParseIpNetwork("0.0.0.0/0").Contains("203.0.113.9"));
  EXPECT_TRUE(ParseIpNetwork("::1").Contains("::1"));
}

TEST(XForwardedFor, TheRightmostHopThatIsNotATrustedProxy) {
  EXPECT_EQ(Client({{"X-Forwarded-For", "203.0.113.9"}}), "203.0.113.9");
  EXPECT_EQ(Client({{"X-Forwarded-For", "198.51.100.7, 10.1.2.3"}}), "198.51.100.7");
  // What the client wrote itself (left) is not believed past the first untrusted hop.
  EXPECT_EQ(Client({{"X-Forwarded-For", "1.1.1.1, 198.51.100.7, 10.1.2.3, 10.9.9.9"}}), "198.51.100.7");
  // Every instance counts, in order.
  EXPECT_EQ(Client({{"X-Forwarded-For", "1.1.1.1"}, {"X-Forwarded-For", "198.51.100.7,10.1.2.3"}}),
            "198.51.100.7");
  EXPECT_EQ(Client({{"X-Forwarded-For", "198.51.100.7"}, {"X-Forwarded-For", "10.1.2.3"}}), "198.51.100.7");
}

TEST(XForwardedFor, AllTrustedIsTheLeftmost) {
  EXPECT_EQ(Client({{"X-Forwarded-For", "10.0.0.5, 10.1.2.3"}}), "10.0.0.5");
  EXPECT_EQ(Client({{"X-Forwarded-For", "10.0.0.5"}}), "10.0.0.5");
}

TEST(XForwardedFor, PortsAreDroppedAndIPv6IsBracketed) {
  EXPECT_EQ(Client({{"X-Forwarded-For", "203.0.113.9:51234"}}), "203.0.113.9");  // Azure Application Gateway
  EXPECT_EQ(Client({{"X-Forwarded-For", "2001:db8::7"}}), "[2001:db8::7]");
  EXPECT_EQ(Client({{"X-Forwarded-For", "[2001:db8::7]"}}), "[2001:db8::7]");
  EXPECT_EQ(Client({{"X-Forwarded-For", "[2001:DB8:0::7]:443, fd00::1"}}), "[2001:db8::7]");
  EXPECT_EQ(Client({{"X-Forwarded-For", "2001:db8::7, fd12:3456::1"}}), "[2001:db8::7]");
  EXPECT_EQ(Client({{"X-Forwarded-For", "::ffff:203.0.113.9"}}), "203.0.113.9");
  EXPECT_EQ(Client({{"X-Forwarded-For", "203.0.113.9, ::ffff:10.1.2.3"}}), "203.0.113.9");
}

TEST(XForwardedFor, MalformedOrUnknownLeavesTheTcpPeer) {
  for (const char* bad : {"", " ", "unknown", "203.0.113.9,", "203.0.113.9, garbage",
                          "example.com", "203.0.113.9:0", "203.0.113.9:99999", "203.0.113.9:http", "[2001:db8::7",
                          "[203.0.113.9]", "fe80::1%eth0", "203.0.113.9 198.51.100.7", "1.2.3", "_hidden",
                          "203.0.113.9, 10.1.2.3, unknown", "127.1", "2130706433", "010.0.0.1",
                          "1.2.3:80", "::ffff:127.1", "::ffff:192.000.2.1"}) {
    EXPECT_EQ(Client({{"X-Forwarded-For", bad}}), "") << bad;
  }
  // Garbage to the left of the client is never read.
  EXPECT_EQ(Client({{"X-Forwarded-For", "garbage, 203.0.113.9, 10.1.2.3"}}), "203.0.113.9");
  EXPECT_EQ(Client({{"X-Forwarded-For", ",203.0.113.9"}}), "203.0.113.9");
}

TEST(Forwarded, Rfc7239Forms) {
  EXPECT_EQ(Client({{"Forwarded", "for=203.0.113.9"}}), "203.0.113.9");
  EXPECT_EQ(Client({{"Forwarded", "For=\"203.0.113.9:4711\";proto=https;by=10.0.0.1"}}), "203.0.113.9");
  EXPECT_EQ(Client({{"Forwarded", "for=\"[2001:db8:cafe::17]:4711\""}}), "[2001:db8:cafe::17]");
  EXPECT_EQ(Client({{"Forwarded", "for=\"[2001:db8:cafe::17]\""}}), "[2001:db8:cafe::17]");
  EXPECT_EQ(Client({{"Forwarded", "for=[2001:db8:cafe::17]"}}), "[2001:db8:cafe::17]");  // unquoted slip
  EXPECT_EQ(Client({{"Forwarded", "for=192.0.2.43, for=198.51.100.17;by=10.0.0.1, for=10.1.2.3"}}),
            "198.51.100.17");
  EXPECT_EQ(Client({{"Forwarded", "for=192.0.2.43"}, {"Forwarded", "for=10.1.2.3"}}), "192.0.2.43");
  EXPECT_EQ(Client({{"Forwarded", "for=\"203.0.113.9:_p1\""}}), "203.0.113.9");  // obfuscated port
  EXPECT_EQ(Client({{"Forwarded", "for=\"a,b\\\"c\", for=203.0.113.9"}}), "203.0.113.9");  // quoted , and "
  EXPECT_EQ(Client({{"Forwarded", "for=203.0.113.9,,"}}), "203.0.113.9");  // empty list elements
  EXPECT_EQ(Client({{"Forwarded", "for=10.0.0.4, for=10.1.2.3"}}), "10.0.0.4");  // all trusted
}

TEST(Forwarded, UnknownObfuscatedOrMalformedLeavesTheTcpPeer) {
  for (const char* bad : {"for=unknown", "for=_hidden", "for=\"_SEVKISEK\"", "proto=https", "for=192.0.2.43, by=1.2.3.4",
                          "for=", "for", "for=\"203.0.113.9", "for=203.0.113.9;for=198.51.100.7",
                          "for=203.0.113.9 junk", "=203.0.113.9", "for=example.com", "for=192.0.2.43; ;",
                          "for=\"[2001:db8::1]:x\"", "for=203.0.113.9, for=UNKNOWN"}) {
    EXPECT_EQ(Client({{"Forwarded", bad}}), "") << bad;
  }
}

TEST(ForwardingHeaders, XForwardedForWinsOverForwarded) {
  // The common proxies append to X-Forwarded-For and pass a client's Forwarded on.
  EXPECT_EQ(Client({{"Forwarded", "for=1.1.1.1"}, {"X-Forwarded-For", "203.0.113.9"}}), "203.0.113.9");
  // And a malformed X-Forwarded-For doesn't fall through to Forwarded.
  EXPECT_EQ(Client({{"Forwarded", "for=1.1.1.1"}, {"X-Forwarded-For", "garbage"}}), "");
  EXPECT_EQ(Client({}), "");
  EXPECT_EQ(Client({{"X-Real-IP", "203.0.113.9"}}), "");
}

TEST(PeerHost, EveryFormThePeerTakes) {
  EXPECT_EQ(PeerHost("127.0.0.1:5000"), "127.0.0.1");
  EXPECT_EQ(PeerHost("[::1]:5000"), "::1");
  EXPECT_EQ(PeerHost("[2001:db8::7]"), "2001:db8::7");
  EXPECT_EQ(PeerHost("203.0.113.9"), "203.0.113.9");
  EXPECT_EQ(PeerHost("::1:5"), "::1");  // unbracketed: address:port
  EXPECT_EQ(warpsql::detail::FormatPeer(net::ip::make_address("::1"), 5), "[::1]:5");
  EXPECT_EQ(warpsql::detail::FormatPeer(net::ip::make_address("10.0.0.1"), 5), "10.0.0.1:5");
  // The security-event limiter keys a forwarded IPv6 client by its /64, as a direct one.
  warpsql::SecurityEvent direct, forwarded;
  direct.code = forwarded.code = "client_certificate_refused";
  direct.peer = "[2001:db8:1:2::7]:443";
  forwarded.peer = "[2001:db8:1:2::8]";
  EXPECT_EQ(warpsql::detail::SourceKey(direct), "2001:db8:1:2::/64");
  EXPECT_EQ(warpsql::detail::SourceKey(forwarded), "2001:db8:1:2::/64");
}

// ---- Against a server ----

class Engine : public warpsql::Backend {
 public:
  warpsql::ServerIdentity Identity() override { return {"forwarded-test", "0"}; }
  std::unique_ptr<warpsql::EngineSession> OpenSession(const warpsql::Principal&, const std::string&,
                                                      const json::object&) override {
    return std::make_unique<warpsql::EngineSession>();
  }
};

class AnyPassword : public warpsql::Authenticator {
 public:
  json::array GrantTypes() override { return {"password"}; }
  warpsql::Principal AuthenticatePassword(std::string_view username, std::string_view) override {
    return warpsql::Principal{.name = std::string(username)};
  }
};

class FakeProvider : public warpsql::BrowserSignInProvider {
 public:
  std::string DisplayName() override { return "Fake IdP"; }
  std::string AuthorizeUrl(const Authorization& a) override { return "https://idp.example/authorize?state=" + a.state; }
  warpsql::Principal CompleteSignIn(const Callback&) override { throw std::runtime_error("unused"); }
};

struct Answer {
  int status = 0;
  std::string body;
};

Answer Send(uint16_t port, http::verb method, const std::string& target, const std::string& body,
            const std::vector<std::pair<std::string, std::string>>& headers) {
  net::io_context io;
  beast::tcp_stream stream(io);
  stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  http::request<http::string_body> request{method, target, 11};
  request.set(http::field::host, "127.0.0.1");
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

class ProxiedServer {
 public:
  explicit ProxiedServer(std::vector<std::string> trusted) {
    warpsql::ServerOptions options;
    options.trusted_proxies = Networks(trusted);
    options.browser_sign_in = std::make_shared<FakeProvider>();
    options.max_browser_sign_ins_per_address = 2;
    server = std::make_unique<warpsql::Server>(options, std::make_shared<Engine>(), std::make_shared<AnyPassword>());
    server->Start();
    port = server->port();
  }
  ~ProxiedServer() {
    server->Stop();
    server->Wait();
  }
  // Signs in and opens a session with these extra headers; returns the session's activity.
  warpsql::SessionActivity OpenSession(const std::vector<std::pair<std::string, std::string>>& headers) {
    const Answer token = Send(port, http::verb::post, "/warp/v1/auth/token",
                              R"({"grant_type": "password", "username": "alice", "password": "x"})", headers);
    EXPECT_EQ(token.status, 200) << token.body;  // a forwarding header changes nothing about signing in
    auto with_auth = headers;
    with_auth.emplace_back("Authorization",
                           "Bearer " + std::string(json::parse(token.body).at("access_token").as_string()));
    const Answer session = Send(port, http::verb::post, "/warp/v1/sessions", "{}", with_auth);
    EXPECT_EQ(session.status, 201) << session.body;
    const std::string id(json::parse(session.body).at("session_id").as_string());
    for (auto& activity : server->Sessions()) {
      if (activity.session_id == id) return activity;
    }
    ADD_FAILURE() << "no activity for " << id;
    return {};
  }
  int NewFlow(const std::vector<std::pair<std::string, std::string>>& headers) {
    return Send(port, http::verb::post, "/warp/v1/auth/oauth/flows", "", headers).status;
  }

  std::unique_ptr<warpsql::Server> server;
  uint16_t port = 0;
};

TEST(TrustedProxies, AnUntrustedPeersHeadersAreIgnored) {
  ProxiedServer server({"10.0.0.0/8"});  // 127.0.0.1 is not in it
  const auto activity = server.OpenSession({{"X-Forwarded-For", "203.0.113.9"}, {"Forwarded", "for=203.0.113.9"}});
  EXPECT_EQ(PeerHost(activity.client_address), "127.0.0.1") << activity.client_address;
  EXPECT_TRUE(activity.client_address.starts_with("127.0.0.1:")) << activity.client_address;
  EXPECT_EQ(activity.proxy_address, "");
}

TEST(TrustedProxies, NoneTrustedByDefault) {
  ProxiedServer server({});
  const auto activity = server.OpenSession({{"X-Forwarded-For", "203.0.113.9"}});
  EXPECT_TRUE(activity.client_address.starts_with("127.0.0.1:")) << activity.client_address;
  EXPECT_EQ(activity.proxy_address, "");
}

TEST(TrustedProxies, ATrustedPeersHeadersNameTheClient) {
  ProxiedServer server({"127.0.0.0/8", "10.0.0.0/8"});
  auto activity = server.OpenSession({{"X-Forwarded-For", "198.51.100.7, 10.1.2.3"}});
  EXPECT_EQ(activity.client_address, "198.51.100.7");
  EXPECT_TRUE(activity.proxy_address.starts_with("127.0.0.1:")) << activity.proxy_address;
  activity = server.OpenSession({{"Forwarded", "for=\"[2001:db8::7]:4711\""}});
  EXPECT_EQ(activity.client_address, "[2001:db8::7]");
  // Malformed: the TCP peer.
  activity = server.OpenSession({{"X-Forwarded-For", "not-an-address"}});
  EXPECT_TRUE(activity.client_address.starts_with("127.0.0.1:")) << activity.client_address;
  EXPECT_EQ(activity.proxy_address, "");
}

TEST(TrustedProxies, BrowserSignInsAreBoundedPerForwardedAddress) {
  ProxiedServer trusted({"127.0.0.1"});
  // Two flows per address: each forwarded client has its own two.
  EXPECT_EQ(trusted.NewFlow({{"X-Forwarded-For", "203.0.113.1"}}), 201);
  EXPECT_EQ(trusted.NewFlow({{"X-Forwarded-For", "203.0.113.1"}}), 201);
  EXPECT_EQ(trusted.NewFlow({{"X-Forwarded-For", "203.0.113.1"}}), 429);
  EXPECT_EQ(trusted.NewFlow({{"X-Forwarded-For", "203.0.113.2"}}), 201);
  EXPECT_EQ(trusted.NewFlow({{"X-Forwarded-For", "[2001:db8::1]:5"}}), 201);
  EXPECT_EQ(trusted.NewFlow({{"X-Forwarded-For", "2001:db8::1"}}), 201);
  EXPECT_EQ(trusted.NewFlow({{"X-Forwarded-For", "2001:db8::1"}}), 429);

  // Untrusted: a client can't escape the bound by inventing addresses.
  ProxiedServer untrusted({"10.0.0.0/8"});
  EXPECT_EQ(untrusted.NewFlow({{"X-Forwarded-For", "203.0.113.1"}}), 201);
  EXPECT_EQ(untrusted.NewFlow({{"X-Forwarded-For", "203.0.113.2"}}), 201);
  EXPECT_EQ(untrusted.NewFlow({{"X-Forwarded-For", "203.0.113.3"}}), 429);
}

}  // namespace
