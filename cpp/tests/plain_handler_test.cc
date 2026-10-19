// ServerOptions::plain_handler: an application's answers to GET and HEAD outside /warp
// (Hardcharger serves its web UI with it). Each test starts the SDK's server in this
// process, with a backend that runs nothing, and talks plain HTTP/1.1 (and TLS) to it.
#include <gtest/gtest.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "warpsql/server.hpp"

namespace {

namespace fs = std::filesystem;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
namespace http = boost::beast::http;

class NoEngine : public warpsql::Backend {
 public:
  warpsql::ServerIdentity Identity() override { return {"plain-handler-test", "0"}; }
  std::unique_ptr<warpsql::EngineSession> OpenSession(const warpsql::Principal&, const std::string&,
                                                      const boost::json::object&) override {
    throw warpsql::Error(warpsql::ErrorCode::kNotImplemented, "no engine");
  }
};

class NoSignIn : public warpsql::Authenticator {
 public:
  boost::json::array GrantTypes() override { return {"password"}; }
};

// The handler the tests install: it records every path it is asked about, answers
// /ui/... with a small page, and leaves everything else to the server (nullopt).
struct Recorder {
  std::mutex mutex;
  std::vector<std::string> calls;

  warpsql::PlainHandler Handler() {
    return [this](const warpsql::PlainRequest& request) -> std::optional<warpsql::PlainResponse> {
      {
        std::lock_guard lock(mutex);
        calls.push_back(request.method + " " + request.path);
      }
      if (!request.path.starts_with("/ui/")) return std::nullopt;
      warpsql::PlainResponse response;
      response.content_type = "text/plain; charset=utf-8";
      response.headers.emplace_back("X-Plain", "yes");
      response.body = "hello from " + request.path;
      return response;
    };
  }
  std::vector<std::string> Calls() {
    std::lock_guard lock(mutex);
    return calls;
  }
};

struct Answer {
  int status = 0;
  std::map<std::string, std::string> headers;  // lower-case names
  std::string body;
  bool has(const std::string& name) const { return headers.contains(name); }
};

// `buffer` carries what was read past one answer to the next on the same connection.
template <class Stream>
Answer Exchange(Stream& stream, http::verb method, const std::string& target,
                const std::map<std::string, std::string>& headers, beast::flat_buffer& buffer) {
  http::request<http::string_body> request{method, target, 11};
  request.set(http::field::host, "127.0.0.1");
  for (const auto& [k, v] : headers) request.set(k, v);
  request.prepare_payload();
  http::write(stream, request);
  http::response_parser<http::string_body> parser;
  parser.body_limit(64 * 1024 * 1024);
  parser.skip(method == http::verb::head);  // a HEAD answer has headers only
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
  return answer;
}

template <class Stream>
Answer Exchange(Stream& stream, http::verb method, const std::string& target,
                const std::map<std::string, std::string>& headers) {
  beast::flat_buffer buffer;
  return Exchange(stream, method, target, headers, buffer);
}

Answer Send(uint16_t port, http::verb method, const std::string& target,
            const std::map<std::string, std::string>& headers = {}) {
  net::io_context io;
  beast::tcp_stream stream(io);
  stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
  Answer answer = Exchange(stream, method, target, headers);
  beast::error_code ec;
  stream.socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
  return answer;
}

struct Running {
  std::unique_ptr<warpsql::Server> server;
  uint16_t port = 0;
  ~Running() {
    if (server) server->Stop();
  }
};

std::unique_ptr<Running> Start(warpsql::ServerOptions options) {
  options.host = "127.0.0.1";
  auto running = std::make_unique<Running>();
  running->server = std::make_unique<warpsql::Server>(std::move(options), std::make_shared<NoEngine>(),
                                                      std::make_shared<NoSignIn>());
  running->server->Start();
  running->port = running->server->port();
  return running;
}

TEST(PlainHandler, WithoutOneEverythingOutsideWarpIs404) {
  auto running = Start({});
  for (const char* path : {"/ui/", "/", "/index.html"}) {
    const Answer answer = Send(running->port, http::verb::get, path);
    EXPECT_EQ(answer.status, 404) << path;
    EXPECT_NE(answer.body.find("\"not_found\""), std::string::npos) << path;
  }
}

TEST(PlainHandler, AnswersGetAndHeadWithoutTheBody) {
  Recorder recorder;
  warpsql::ServerOptions options;
  options.plain_handler = recorder.Handler();
  auto running = Start(std::move(options));

  const Answer get = Send(running->port, http::verb::get, "/ui/page");
  EXPECT_EQ(get.status, 200);
  EXPECT_EQ(get.body, "hello from /ui/page");
  EXPECT_EQ(get.headers.at("content-type"), "text/plain; charset=utf-8");
  EXPECT_EQ(get.headers.at("x-plain"), "yes");
  EXPECT_EQ(get.headers.at("content-length"), std::to_string(get.body.size()));

  const Answer head = Send(running->port, http::verb::head, "/ui/page");
  EXPECT_EQ(head.status, 200);
  EXPECT_EQ(head.headers.at("content-length"), std::to_string(get.body.size()));
  EXPECT_EQ(head.headers.at("x-plain"), "yes");
  EXPECT_TRUE(head.body.empty());
  EXPECT_FALSE(head.has("transfer-encoding"));

  // HEAD sends no body: on the same connection (and the same read buffer), the next answer
  // starts right after the HEAD answer's headers.
  net::io_context io;
  beast::tcp_stream stream(io);
  stream.connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), running->port));
  beast::flat_buffer buffer;
  EXPECT_TRUE(Exchange(stream, http::verb::head, "/ui/a", {}, buffer).body.empty());
  EXPECT_EQ(Exchange(stream, http::verb::get, "/ui/b", {}, buffer).body, "hello from /ui/b");
  EXPECT_EQ(buffer.size(), 0u);

  EXPECT_EQ(recorder.Calls(), (std::vector<std::string>{"GET /ui/page", "HEAD /ui/page", "HEAD /ui/a", "GET /ui/b"}));
}

TEST(PlainHandler, SeesTheRequestsHeaders) {
  std::mutex mutex;
  std::vector<std::string> seen;
  warpsql::ServerOptions options;
  options.plain_handler = [&](const warpsql::PlainRequest& request) -> std::optional<warpsql::PlainResponse> {
    std::lock_guard lock(mutex);
    seen.emplace_back(request.Header("accept-encoding"));
    seen.emplace_back(request.Header("IF-NONE-MATCH"));
    seen.emplace_back(request.Header("x-absent"));
    return warpsql::PlainResponse{};
  };
  auto running = Start(std::move(options));
  EXPECT_EQ(Send(running->port, http::verb::get, "/ui/",
                 {{"Accept-Encoding", "gzip, br"}, {"If-None-Match", "\"abc\""}}).status,
            200);
  std::lock_guard lock(mutex);
  EXPECT_EQ(seen, (std::vector<std::string>{"gzip, br", "\"abc\"", ""}));
}

TEST(PlainHandler, NulloptLeavesTheServers404) {
  Recorder recorder;
  warpsql::ServerOptions options;
  options.plain_handler = recorder.Handler();
  auto running = Start(std::move(options));
  for (const char* path : {"/", "/nope", "/uix/page"}) {
    const Answer answer = Send(running->port, http::verb::get, path);
    EXPECT_EQ(answer.status, 404) << path;
    EXPECT_NE(answer.body.find("\"not_found\""), std::string::npos) << path;
  }
  EXPECT_EQ(recorder.Calls().size(), 3u);
}

TEST(PlainHandler, ProtocolPathsNeverReachIt) {
  Recorder recorder;
  warpsql::ServerOptions options;
  options.plain_handler = recorder.Handler();
  auto running = Start(std::move(options));
  // The protocol's own answers, however the path is spelled: the server decodes it, and
  // empty segments don't count, before deciding.
  for (const char* path : {"/warp", "/%77arp", "//warp", "/warp/", "/%77%61%72%70"}) {
    const Answer answer = Send(running->port, http::verb::get, path);
    EXPECT_EQ(answer.status, 200) << path;
    EXPECT_NE(answer.body.find("\"warp-sql\""), std::string::npos) << path;
  }
  for (const char* path : {"/warp/v1/health", "/%77arp/v1/health", "//warp//v1/health", "/warp%2Fv1%2Fhealth"}) {
    EXPECT_EQ(Send(running->port, http::verb::get, path).status, 200) << path;
  }
  for (const char* path : {"/warp/v1/no-such-thing", "/%77arp/ui/page", "//warp/ui/page"}) {
    const Answer answer = Send(running->port, http::verb::get, path);
    EXPECT_EQ(answer.status, 404) << path;
    EXPECT_FALSE(answer.has("x-plain")) << path;
  }
  EXPECT_EQ(Send(running->port, http::verb::head, "/warp/v1/health").status, 405);
  EXPECT_TRUE(recorder.Calls().empty()) << recorder.Calls().front();
}

TEST(PlainHandler, OtherMethodsNeverReachIt) {
  Recorder recorder;
  warpsql::ServerOptions options;
  options.plain_handler = recorder.Handler();
  auto running = Start(std::move(options));
  for (http::verb method : {http::verb::post, http::verb::put, http::verb::delete_, http::verb::patch}) {
    const Answer answer = Send(running->port, method, "/ui/page");
    EXPECT_EQ(answer.status, 404) << http::to_string(method);
    EXPECT_FALSE(answer.has("x-plain")) << http::to_string(method);
  }
  // A CORS preflight is the server's, as for any path.
  EXPECT_EQ(Send(running->port, http::verb::options, "/ui/page").status, 204);
  EXPECT_TRUE(recorder.Calls().empty()) << recorder.Calls().front();
}

TEST(PlainHandler, CorsHeadersAsForEveryOtherAnswer) {
  Recorder recorder;
  warpsql::ServerOptions options;
  options.plain_handler = recorder.Handler();
  options.cors_allowed_origins = {"https://app.example"};
  auto running = Start(std::move(options));

  const Answer allowed = Send(running->port, http::verb::get, "/ui/page", {{"Origin", "https://app.example"}});
  EXPECT_EQ(allowed.status, 200);
  EXPECT_EQ(allowed.headers.at("access-control-allow-origin"), "https://app.example");
  EXPECT_EQ(allowed.headers.at("vary"), "Origin");
  const Answer protocol = Send(running->port, http::verb::get, "/warp", {{"Origin", "https://app.example"}});
  EXPECT_EQ(protocol.headers.at("access-control-allow-origin"), allowed.headers.at("access-control-allow-origin"));
  EXPECT_EQ(protocol.headers.at("access-control-expose-headers"), allowed.headers.at("access-control-expose-headers"));

  const Answer other = Send(running->port, http::verb::get, "/ui/page", {{"Origin", "https://evil.example"}});
  EXPECT_EQ(other.status, 200);
  EXPECT_FALSE(other.has("access-control-allow-origin"));
  const Answer none = Send(running->port, http::verb::get, "/ui/page");
  EXPECT_FALSE(none.has("access-control-allow-origin"));
}

TEST(PlainHandler, TheHealthPortNeverConsultsIt) {
  Recorder recorder;
  warpsql::ServerOptions options;
  options.plain_handler = recorder.Handler();
  net::io_context io;
  net::ip::tcp::acceptor probe(io, {net::ip::make_address("127.0.0.1"), 0});
  options.health_port = probe.local_endpoint().port();
  probe.close();
  auto running = Start(std::move(options));
  EXPECT_EQ(Send(options.health_port, http::verb::get, "/ui/page").status, 404);
  EXPECT_EQ(Send(options.health_port, http::verb::get, "/health").status, 200);
  EXPECT_TRUE(recorder.Calls().empty());
  // Positive twin: the main port does.
  EXPECT_EQ(Send(running->port, http::verb::get, "/ui/page").status, 200);
}

// ---- Over mutual TLS ----

struct Issued {
  X509* cert;
  EVP_PKEY* key;
};

Issued Issue(const std::string& common_name, const Issued* issuer, const std::vector<std::string>& extensions,
             const fs::path& cert_path, const fs::path& key_path) {
  EVP_PKEY* key = EVP_RSA_gen(2048);
  X509* cert = X509_new();
  X509_set_version(cert, 2);
  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1 + std::rand() % 100000);
  X509_gmtime_adj(X509_getm_notBefore(cert), -60);
  X509_gmtime_adj(X509_getm_notAfter(cert), 24 * 3600);
  X509_set_pubkey(cert, key);
  X509_NAME_add_entry_by_txt(X509_get_subject_name(cert), "CN", MBSTRING_ASC,
                             reinterpret_cast<const unsigned char*>(common_name.c_str()), -1, -1, 0);
  X509* issuer_cert = issuer != nullptr ? issuer->cert : cert;
  X509_set_issuer_name(cert, X509_get_subject_name(issuer_cert));
  X509V3_CTX ctx;
  X509V3_set_ctx_nodb(&ctx);
  X509V3_set_ctx(&ctx, issuer_cert, cert, nullptr, nullptr, 0);
  for (const auto& extension : extensions) {
    const auto eq = extension.find('=');
    X509_EXTENSION* ext = X509V3_EXT_conf(nullptr, &ctx, extension.substr(0, eq).c_str(), extension.substr(eq + 1).c_str());
    EXPECT_NE(ext, nullptr) << extension;
    if (ext != nullptr) {
      X509_add_ext(cert, ext, -1);
      X509_EXTENSION_free(ext);
    }
  }
  EXPECT_GT(X509_sign(cert, issuer != nullptr ? issuer->key : key, EVP_sha256()), 0);
  FILE* out = std::fopen(cert_path.string().c_str(), "w");
  PEM_write_X509(out, cert);
  std::fclose(out);
  out = std::fopen(key_path.string().c_str(), "w");
  PEM_write_PrivateKey(out, key, nullptr, nullptr, 0, nullptr, nullptr);
  std::fclose(out);
  return {cert, key};
}

class PlainHandlerMtls : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    dir_ = fs::temp_directory_path() / ("warpsql-plain-handler-" + std::to_string(std::rand()));
    fs::create_directories(dir_);
    const std::vector<std::string> ca_ext{"basicConstraints=critical,CA:TRUE", "keyUsage=critical,keyCertSign,cRLSign",
                                          "subjectKeyIdentifier=hash"};
    Issued ca = Issue("test-ca", nullptr, ca_ext, dir_ / "ca.pem", dir_ / "ca.key");
    Issued server = Issue("server", &ca,
                          {"basicConstraints=CA:FALSE", "keyUsage=critical,digitalSignature,keyEncipherment",
                           "extendedKeyUsage=serverAuth", "subjectAltName=IP:127.0.0.1", "authorityKeyIdentifier=keyid"},
                          dir_ / "server.pem", dir_ / "server.key");
    Issued client = Issue("client", &ca,
                          {"basicConstraints=CA:FALSE", "keyUsage=critical,digitalSignature,keyEncipherment",
                           "extendedKeyUsage=clientAuth", "subjectAltName=email:client@example.com",
                           "authorityKeyIdentifier=keyid"},
                          dir_ / "client.pem", dir_ / "client.key");
    for (Issued* issued : {&ca, &server, &client}) {
      X509_free(issued->cert);
      EVP_PKEY_free(issued->key);
    }
  }
  static void TearDownTestSuite() {
    std::error_code ec;
    fs::remove_all(dir_, ec);
  }

  // GET over TLS, presenting the client certificate when `with_certificate`.
  static Answer GetTls(uint16_t port, const std::string& target, bool with_certificate) {
    net::io_context io;
    ssl::context context(ssl::context::tls_client);
    context.load_verify_file((dir_ / "ca.pem").string());
    context.set_verify_mode(ssl::verify_peer);
    if (with_certificate) {
      context.use_certificate_chain_file((dir_ / "client.pem").string());
      context.use_private_key_file((dir_ / "client.key").string(), ssl::context::pem);
    }
    beast::ssl_stream<beast::tcp_stream> stream(io, context);
    beast::get_lowest_layer(stream).connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
    stream.handshake(ssl::stream_base::client);
    return Exchange(stream, http::verb::get, target, {});
  }

  static fs::path dir_;
};

fs::path PlainHandlerMtls::dir_;

TEST_F(PlainHandlerMtls, AnswersOverTlsOnlyToAClientWithACertificate) {
  Recorder recorder;
  warpsql::ServerOptions options;
  options.plain_handler = recorder.Handler();
  options.tls = warpsql::TlsOptions{};
  options.tls->certificate_chain_file = (dir_ / "server.pem").string();
  options.tls->private_key_file = (dir_ / "server.key").string();
  options.tls->client_ca_file = (dir_ / "ca.pem").string();
  options.tls->require_client_certificate = true;
  auto running = Start(std::move(options));

  const Answer answer = GetTls(running->port, "/ui/page", true);
  EXPECT_EQ(answer.status, 200);
  EXPECT_EQ(answer.body, "hello from /ui/page");
  EXPECT_EQ(recorder.Calls(), std::vector<std::string>{"GET /ui/page"});

  // Without a certificate the connection never gets to HTTP (the handshake or the first
  // read fails), and the handler is never asked.
  bool refused = false;
  try {
    GetTls(running->port, "/ui/page", false);
  } catch (const std::exception&) {
    refused = true;
  }
  EXPECT_TRUE(refused);
  EXPECT_EQ(recorder.Calls().size(), 1u);
}

}  // namespace
