#include "transport.hpp"

#include "legacy_dialect.hpp"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <utility>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/write.hpp>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <wincrypt.h>
#pragma comment(lib, "crypt32.lib")
#endif

namespace chargewire::client::detail {

namespace {

bool Readable(const char* path) { return std::ifstream(path).good(); }

std::string Lower(std::string_view text) {
  std::string out(text);
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::tolower(c); });
  return out;
}

std::string MediaType(std::string_view value) {
  auto semicolon = value.find(';');
  std::string type = Lower(value.substr(0, semicolon));
  while (!type.empty() && std::isspace(static_cast<unsigned char>(type.back()))) type.pop_back();
  return type;
}

bool IsIpLiteral(const std::string& host) {
  boost::system::error_code ec;
  net::ip::make_address(host, ec);
  return !ec;
}

#ifdef _WIN32
// The Windows certificate store's roots, added to the context's trust store.
bool AddWindowsRoots(ssl::context& context) {
  HCERTSTORE store = CertOpenSystemStoreA(0, "ROOT");
  if (!store) return false;
  X509_STORE* trust = SSL_CTX_get_cert_store(context.native_handle());
  bool any = false;
  PCCERT_CONTEXT cert = nullptr;
  while ((cert = CertEnumCertificatesInStore(store, cert)) != nullptr) {
    const unsigned char* data = cert->pbCertEncoded;
    X509* x509 = d2i_X509(nullptr, &data, static_cast<long>(cert->cbCertEncoded));
    if (x509 != nullptr) {
      any = X509_STORE_add_cert(trust, x509) == 1 || any;
      X509_free(x509);
    }
  }
  CertCloseStore(store, 0);
  return any;
}
#endif

// A TLS failure as the error the client reports. An alert the server sent about the
// client's certificate is the refusal it stands for (spec §4.4): certificate_revoked, or
// certificate_required when the client sent none. With TLS 1.3 the client's side
// of the handshake is over before the server checks the certificate, so such an alert
// arrives with the first read, not in the handshake; it is still not a failure to connect.
// Anything else at the TLS layer is tls_failed.
Error TlsFailure(const boost::system::error_code& ec, const std::string& origin, std::string_view during) {
  if (ec.category() == net::error::get_ssl_category()) {
    const char* refused = nullptr;
    switch (ERR_GET_REASON(static_cast<unsigned long>(ec.value()))) {
      case SSL_R_SSLV3_ALERT_CERTIFICATE_REVOKED:
        return Error(0, "certificate_revoked", "the server at " + origin + " refused the client certificate: certificate revoked");
      case SSL_R_TLSV13_ALERT_CERTIFICATE_REQUIRED:
        return Error(0, "certificate_required",
                     "the server at " + origin + " requires a client certificate, and none was sent");
      case SSL_R_SSLV3_ALERT_CERTIFICATE_EXPIRED: refused = "certificate expired"; break;
      case SSL_R_TLSV1_ALERT_UNKNOWN_CA: refused = "its issuer is not a certificate authority the server trusts"; break;
      case SSL_R_SSLV3_ALERT_BAD_CERTIFICATE: refused = "bad certificate"; break;
      case SSL_R_SSLV3_ALERT_UNSUPPORTED_CERTIFICATE: refused = "unsupported certificate"; break;
      case SSL_R_SSLV3_ALERT_CERTIFICATE_UNKNOWN: refused = "certificate unknown"; break;
      default: break;
    }
    if (refused != nullptr) {
      return Error(0, "client_tls_failed", "the server at " + origin + " refused the client certificate: " + refused);
    }
  }
  return Error(0, "client_tls_failed", std::string(during) + " " + origin + " failed: " + ec.message());
}

bool IsTlsError(const boost::system::error_code& ec) {
  return ec.category() == net::error::get_ssl_category();
}

}  // namespace

std::optional<std::string> FindSystemCaCertFile() {
  if (const char* env = std::getenv("SSL_CERT_FILE"); env != nullptr && *env != '\0' && Readable(env)) {
    return std::string(env);
  }
  static constexpr const char* kBundles[] = {
      "/etc/ssl/certs/ca-certificates.crt",    // Debian, Ubuntu, Alpine
      "/etc/pki/tls/certs/ca-bundle.crt",      // RHEL, CentOS, Fedora, Amazon Linux
      "/etc/ssl/ca-bundle.pem",                // OpenSUSE
      "/etc/pki/tls/cacert.pem",               // OpenELEC
      "/etc/ssl/cert.pem",                     // macOS, Alpine, BSD
      "/usr/local/etc/openssl@3/cert.pem",     // Homebrew (Intel)
      "/opt/homebrew/etc/openssl@3/cert.pem",  // Homebrew (Apple Silicon)
  };
  for (const char* path : kBundles) {
    if (Readable(path)) return std::string(path);
  }
  return std::nullopt;
}

std::shared_ptr<ssl::context> MakeTlsContext(const TlsOptions& options) {
  auto context = std::make_shared<ssl::context>(ssl::context::tls_client);
  SSL_CTX_set_min_proto_version(context->native_handle(), TLS1_2_VERSION);
  auto load = [&](const char* what, const std::string& path, auto&& action) {
    if (!Readable(path.c_str())) throw Error(0, "client_tls_failed", std::string("cannot read the ") + what + " " + path);
    boost::system::error_code ec;
    action(ec);
    if (ec) throw Error(0, "client_tls_failed", std::string("cannot load the ") + what + " " + path + ": " + ec.message());
  };
  if (options.skip_verify) {
    context->set_verify_mode(ssl::verify_none);
  } else {
    context->set_verify_mode(ssl::verify_peer);
    if (!options.root_certs_file.empty()) {
      load("root certificates", options.root_certs_file,
           [&](auto& ec) { context->load_verify_file(options.root_certs_file, ec); });
    } else if (auto bundle = FindSystemCaCertFile()) {
      boost::system::error_code ec;
      context->load_verify_file(*bundle, ec);
    } else {
#ifdef _WIN32
      if (!AddWindowsRoots(*context)) context->set_default_verify_paths();
#else
      context->set_default_verify_paths();
#endif
    }
  }
  if (!options.cert_file.empty() || !options.key_file.empty()) {
    if (options.cert_file.empty() || options.key_file.empty()) {
      throw Error(0, "client_tls_failed", "a client certificate needs both its certificate and its private key");
    }
    load("client certificate", options.cert_file,
         [&](auto& ec) { context->use_certificate_chain_file(options.cert_file, ec); });
    load("client private key", options.key_file,
         [&](auto& ec) { context->use_private_key_file(options.key_file, ssl::context::pem, ec); });
  }
  return context;
}

std::string Reply::Header(std::string_view name) const {
  auto it = headers.find(legacy ? LegacyHeaderName(name) : std::string(name));
  return it == headers.end() ? std::string() : std::string(it->value());
}

std::string Reply::ContentType() const { return MediaType(Header("Content-Type")); }

std::string UrlEncode(std::string_view text) {
  static constexpr char kHex[] = "0123456789ABCDEF";
  std::string out;
  for (unsigned char c : text) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      out += static_cast<char>(c);
    } else {
      out += '%';
      out += kHex[c >> 4];
      out += kHex[c & 15];
    }
  }
  return out;
}

// ---- BodyReader ----

BodyReader::BodyReader(Transport& transport, uint64_t generation)
    : transport_(transport), generation_(generation),
      parser_(std::make_unique<http::response_parser<http::buffer_body>>()) {
  parser_->body_limit(boost::none);
  parser_->header_limit(64 * 1024);
}

BodyReader::~BodyReader() {
  if (transport_.active_ == this) {
    transport_.active_ = nullptr;
    if (!done_) transport_.Drop();
  }
}

int BodyReader::status() const { return static_cast<int>(parser_->get().result_int()); }
const http::fields& BodyReader::headers() const { return parser_->get().base(); }

std::string BodyReader::Header(std::string_view name) const {
  auto it = headers().find(transport_.Legacy() ? LegacyHeaderName(name) : std::string(name));
  return it == headers().end() ? std::string() : std::string(it->value());
}

std::string BodyReader::ContentType() const { return MediaType(Header("Content-Type")); }

std::size_t BodyReader::Read(char* out, std::size_t size) {
  if (done_ || size == 0) return 0;
  if (transport_.generation_ != generation_ || transport_.active_ != this) {
    throw Error(0, "client_connection_failed", "the response was abandoned before its end");
  }
  while (true) {
    if (parser_->is_done()) {
      done_ = true;
      transport_.active_ = nullptr;
      if (!parser_->keep_alive()) transport_.Drop();
      return 0;
    }
    parser_->get().body().data = out;
    parser_->get().body().size = size;
    boost::system::error_code ec;
    transport_.WithStream([&](auto& stream) {
      ec = transport_.Run([&](auto handler) {
        http::async_read_some(stream, transport_.buffer_, *parser_, std::move(handler));
      });
    });
    if (ec == http::error::need_buffer) ec = {};
    const std::size_t got = size - parser_->get().body().size;
    if (ec) {
      transport_.active_ = nullptr;
      transport_.Drop();
      done_ = true;
      if (IsTlsError(ec)) throw TlsFailure(ec, transport_.Origin(), "the TLS connection to");
      throw Error(0, "client_connection_failed",
                  "the connection to " + transport_.Origin() + " ended in the middle of a response: " + ec.message());
    }
    if (got > 0) {
      if (parser_->is_done()) {
        done_ = true;
        transport_.active_ = nullptr;
        if (!parser_->keep_alive()) transport_.Drop();
      }
      return got;
    }
  }
}

std::string BodyReader::ReadAll() {
  std::string body;
  char buffer[16 * 1024];
  while (std::size_t n = Read(buffer, sizeof(buffer))) body.append(buffer, n);
  return body;
}

// ---- Transport ----

Transport::Transport(std::string host, uint16_t port, std::shared_ptr<ssl::context> tls,
                     std::chrono::milliseconds connect_timeout, std::shared_ptr<const std::atomic<bool>> legacy)
    : host_(std::move(host)), port_(port), tls_(std::move(tls)), connect_timeout_(connect_timeout),
      legacy_(std::move(legacy)) {
  if (host_.size() > 2 && host_.front() == '[' && host_.back() == ']') host_ = host_.substr(1, host_.size() - 2);
}

Transport::~Transport() { Drop(); }

std::string Transport::Origin() const {
  const bool v6 = host_.find(':') != std::string::npos;
  return std::string(tls_ ? "https://" : "http://") + (v6 ? "[" + host_ + "]" : host_) + ":" + std::to_string(port_);
}

template <class Op>
boost::system::error_code Transport::Run(Op&& op) {
  boost::system::error_code result = net::error::operation_aborted;
  bool done = false;
  op([&](boost::system::error_code ec, auto&&...) {
    result = ec;
    done = true;
  });
  io_.restart();
  io_.run();
  if (!done) result = net::error::operation_aborted;
  return result;
}

template <class F>
void Transport::WithStream(F&& f) {
  if (secure_) {
    f(*secure_);
  } else {
    f(*plain_);
  }
}

void Transport::SetTimeout(std::chrono::milliseconds timeout) {
  beast::tcp_stream& lowest = secure_ ? beast::get_lowest_layer(*secure_) : *plain_;
  if (deadline_ != std::chrono::steady_clock::time_point{}) {
    lowest.expires_at(deadline_);
  } else if (timeout.count() > 0) {
    lowest.expires_after(timeout);
  } else {
    lowest.expires_never();
  }
}

void Transport::Fail(std::string_view what, const boost::system::error_code& ec) {
  Drop();
  if (IsTlsError(ec)) throw TlsFailure(ec, Origin(), "the TLS connection to");
  if (ec == beast::error::timeout) {
    throw Error(0, "client_connection_failed", std::string(what) + " " + Origin() + " timed out");
  }
  throw Error(0, "client_connection_failed", std::string(what) + " " + Origin() + " failed: " + ec.message());
}

void Transport::Connect() {
  Drop();
  tcp::resolver resolver(io_);
  tcp::resolver::results_type endpoints;
  boost::system::error_code ec = Run([&](auto handler) {
    resolver.async_resolve(host_, std::to_string(port_),
                           [&endpoints, handler = std::move(handler)](boost::system::error_code e,
                                                                      tcp::resolver::results_type r) mutable {
                             endpoints = std::move(r);
                             handler(e);
                           });
  });
  if (ec) throw Error(0, "client_connection_failed", "could not resolve host '" + host_ + "': " + ec.message());

  if (tls_) {
    secure_ = std::make_unique<beast::ssl_stream<beast::tcp_stream>>(io_, *tls_);
  } else {
    plain_ = std::make_unique<beast::tcp_stream>(io_);
  }
  beast::tcp_stream& lowest = secure_ ? beast::get_lowest_layer(*secure_) : *plain_;
  lowest.expires_after(connect_timeout_);
  // IPv4 addresses first on Windows: there a refused connection takes about two seconds
  // (Windows retries the SYN), and "localhost" resolves to ::1 before 127.0.0.1, so every
  // connection to a server listening on 127.0.0.1 (the default) waited that long first.
  // Elsewhere the resolver's order, which a refusal costs nothing to walk.
  std::vector<tcp::endpoint> ordered(endpoints.begin(), endpoints.end());
#ifdef _WIN32
  std::stable_partition(ordered.begin(), ordered.end(), [](const tcp::endpoint& e) { return e.address().is_v4(); });
#endif
  ec = Run([&](auto handler) {
    lowest.async_connect(ordered,
                       [handler = std::move(handler)](boost::system::error_code e, const tcp::endpoint&) mutable {
                         handler(e);
                       });
  });
  if (ec) {
    Drop();
    if (ec == beast::error::timeout) throw Error(0, "client_connection_failed", "connecting to " + Origin() + " timed out");
    throw Error(0, "client_connection_failed", "could not connect to " + Origin() + ": " + ec.message());
  }
  lowest.socket().set_option(tcp::no_delay(true), ec);
  if (secure_) {
    if (!IsIpLiteral(host_) && !SSL_set_tlsext_host_name(secure_->native_handle(), host_.c_str())) {
      Drop();
      throw Error(0, "client_tls_failed", "cannot set the TLS server name for " + host_);
    }
    if (SSL_CTX_get_verify_mode(tls_->native_handle()) != SSL_VERIFY_NONE) {
      secure_->set_verify_callback(ssl::host_name_verification(host_));
    }
    lowest.expires_after(connect_timeout_);
    ec = Run([&](auto handler) { secure_->async_handshake(ssl::stream_base::client, std::move(handler)); });
    if (ec) {
      Drop();
      if (ec == beast::error::timeout) throw Error(0, "client_tls_failed", "the TLS handshake with " + Origin() + " timed out");
      throw TlsFailure(ec, Origin(), "the TLS handshake with");
    }
  }
  lowest.expires_never();
  buffer_.clear();
  connected_ = true;
}

void Transport::Drop() {
  generation_++;
  active_ = nullptr;
  connected_ = false;
  boost::system::error_code ignored;
  if (secure_) {
    beast::get_lowest_layer(*secure_).socket().shutdown(tcp::socket::shutdown_both, ignored);
    beast::get_lowest_layer(*secure_).close();
  }
  if (plain_) {
    plain_->socket().shutdown(tcp::socket::shutdown_both, ignored);
    plain_->close();
  }
  // Let the closed streams' cancelled handlers run before the streams go.
  io_.restart();
  io_.poll();
  secure_.reset();
  plain_.reset();
  buffer_.clear();
}

void Transport::Close() { Drop(); }

std::unique_ptr<BodyReader> Transport::Open(http::request<http::string_body> request, const ChunkSource* chunks,
                                            std::chrono::milliseconds timeout) {
  if (active_ != nullptr) Drop();  // the last response wasn't read to its end
  deadline_ = timeout.count() > 0 ? std::chrono::steady_clock::now() + timeout
                                  : std::chrono::steady_clock::time_point{};
  struct ResetDeadline {
    Transport& t;
    ~ResetDeadline() { t.deadline_ = {}; }
  } reset{*this};
  if (request.find(http::field::host) == request.end()) request.set(http::field::host, host_);
  if (Legacy()) ToLegacyHeaders(request);
  if (chunks != nullptr) {
    request.body().clear();
    request.chunked(true);
  } else {
    request.prepare_payload();
  }

  for (int attempt = 0;; attempt++) {
    const bool reused = connected_;
    if (!connected_) Connect();
    bool sent_chunk = false;
    boost::system::error_code ec;
    try {
      WithStream([&](auto& stream) {
      SetTimeout(timeout);
      if (chunks == nullptr) {
        ec = Run([&](auto handler) { http::async_write(stream, request, std::move(handler)); });
        return;
      }
      http::request_serializer<http::string_body> serializer(request);
      ec = Run([&](auto handler) { http::async_write_header(stream, serializer, std::move(handler)); });
      while (!ec) {
        std::optional<std::string> piece = (*chunks)();
        if (!piece) break;
        if (piece->empty()) continue;
        sent_chunk = true;
        ec = Run([&](auto handler) {
          net::async_write(stream, http::make_chunk(net::buffer(*piece)), std::move(handler));
        });
      }
      if (!ec) {
        ec = Run([&](auto handler) { net::async_write(stream, http::make_chunk_last(), std::move(handler)); });
      }
      });
    } catch (...) {
      Drop();  // a body producer that failed leaves a half-sent request
      throw;
    }
    // A write that failed on a connection the server had closed: send again on a new one
    // (only before any of a chunked body was used up).
    const bool can_retry = reused && attempt == 0 && !sent_chunk;
    if (ec && can_retry) {
      Drop();
      continue;
    }
    // On a failed write, the server may still have answered (an upload refused early).
    auto reader = std::make_unique<BodyReader>(*this, generation_);
    boost::system::error_code read_ec;
    WithStream([&](auto& stream) {
      SetTimeout(timeout);
      read_ec = Run([&](auto handler) {
        http::async_read_header(stream, buffer_, *reader->parser_, std::move(handler));
      });
    });
    if (read_ec) {
      const bool nothing_read = !reader->parser_->got_some();
      if (nothing_read && can_retry && !ec) {
        Drop();
        continue;
      }
      Fail("the request to", ec ? ec : read_ec);
    }
    active_ = reader.get();
    if (reader->parser_->is_done()) {
      reader->done_ = true;
      active_ = nullptr;
      if (!reader->parser_->keep_alive()) Drop();
    }
    if (timeout.count() == 0) {
      WithStream([&](auto&) { SetTimeout(std::chrono::milliseconds(0)); });
    }
    return reader;
  }
}

Reply Transport::Exchange(http::request<http::string_body> request, const ChunkSource* chunks,
                          std::chrono::milliseconds timeout) {
  const auto deadline = timeout.count() > 0 ? std::chrono::steady_clock::now() + timeout
                                            : std::chrono::steady_clock::time_point{};
  auto reader = Open(std::move(request), chunks, timeout);
  Reply reply;
  reply.legacy = Legacy();
  reply.status = reader->status();
  reply.headers = reader->headers();
  deadline_ = deadline;
  struct ResetDeadline {
    Transport& t;
    ~ResetDeadline() { t.deadline_ = {}; }
  } reset{*this};
  if (connected_ && deadline_ != std::chrono::steady_clock::time_point{}) {
    WithStream([&](auto&) { SetTimeout(timeout); });
  }
  reply.body = reader->ReadAll();
  return reply;
}

}  // namespace chargewire::client::detail
