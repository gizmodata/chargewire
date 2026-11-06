// One HTTP/1.1 connection (plain or TLS) on Boost.Beast, kept alive between requests and
// reopened when the server has closed it. It knows one thing about Chargewire: a server that
// is resuming (spec §11.2) answers 503 server_resuming, and the request is sent again. Not
// thread-safe: each user of the protocol that may run concurrently (a cancel from another
// thread) opens its own Transport.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <atomic>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include "chargewire/client.hpp"

namespace chargewire::client::detail {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

// The system's CA bundle: $SSL_CERT_FILE when it names a readable file, else the first
// well-known bundle that exists; nullopt when there is none (OpenSSL's compiled-in
// directory is a build-time path, so a portable build can't rely on it).
std::optional<std::string> FindSystemCaCertFile();

// A TLS client context for these options (verification, roots, client certificate).
// Throws Error (code tls_failed) when a file can't be loaded.
std::shared_ptr<ssl::context> MakeTlsContext(const TlsOptions& options);

struct Reply {
  int status = 0;
  http::fields headers;
  std::string body;
  bool legacy = false;  // from a server in the pre-rename spelling (legacy_dialect.hpp)
  std::string Header(std::string_view name) const;
  std::string ContentType() const;  // without parameters, lower case
};

// Produces a chunked request body one piece at a time; nullopt ends it.
using ChunkSource = std::function<std::optional<std::string>()>;

class Transport;

// A response whose body is read as it arrives.
class BodyReader {
 public:
  BodyReader(Transport& transport, uint64_t generation);
  ~BodyReader();  // a body not read to its end drops the connection
  BodyReader(const BodyReader&) = delete;
  BodyReader& operator=(const BodyReader&) = delete;

  int status() const;
  const http::fields& headers() const;
  std::string Header(std::string_view name) const;
  std::string ContentType() const;

  // Up to `size` bytes of the body into `out`, waiting for at least one; 0 at the body's
  // end. Throws Error (connection_failed) when the connection fails before the end.
  std::size_t Read(char* out, std::size_t size);
  std::string ReadAll();
  bool done() const { return done_ && replay_at_ >= replay_.size(); }

 private:
  friend class Transport;
  Transport& transport_;
  uint64_t generation_;
  std::unique_ptr<http::response_parser<http::buffer_body>> parser_;
  bool done_ = false;
  // A body already read (to see whether a 503 was server_resuming), handed out again.
  std::string replay_;
  std::size_t replay_at_ = 0;
};

// How long to wait before repeating a request answered server_resuming (spec §11.2): the
// answer's Retry-After in seconds, 3 s when it is missing or not an integer, at most 60 s.
std::chrono::milliseconds ResumeWait(std::string_view retry_after);

// Tests only: replaces the wait between server_resuming attempts (nullptr: sleep).
void SetResumeSleepForTesting(std::function<void(std::chrono::milliseconds)> sleep);

class Transport {
 public:
  // `legacy`, when set and true, makes requests and header lookups use the protocol's
  // pre-rename spelling (legacy_dialect.hpp); it may turn true after construction.
  // `resume_timeout` > 0 repeats a request answered server_resuming (spec §11.2) for that
  // long (see Open); 0 never does.
  Transport(std::string host, uint16_t port, std::shared_ptr<ssl::context> tls,
            std::chrono::milliseconds connect_timeout,
            std::shared_ptr<const std::atomic<bool>> legacy = nullptr,
            std::chrono::milliseconds resume_timeout = std::chrono::milliseconds(0),
            std::function<bool(const ResumeProgress&)> on_resuming = nullptr);
  ~Transport();
  Transport(const Transport&) = delete;
  Transport& operator=(const Transport&) = delete;

  // Sends `request` (its body, or the chunks of `chunks` with chunked encoding) and reads
  // the response's header; the body is read through the returned reader, which must be
  // done or destroyed before the next request. `timeout` (0: none) bounds the whole call.
  // A request without `chunks` that the server answers 503 server_resuming is sent again
  // after the answer's Retry-After, until the resume timeout (or `timeout`) would pass,
  // and then that answer throws as an Error with the server's detail.
  std::unique_ptr<BodyReader> Open(http::request<http::string_body> request, const ChunkSource* chunks = nullptr,
                                   std::chrono::milliseconds timeout = std::chrono::milliseconds(0));
  // The whole exchange.
  Reply Exchange(http::request<http::string_body> request, const ChunkSource* chunks = nullptr,
                 std::chrono::milliseconds timeout = std::chrono::milliseconds(0));

  // Closes the connection (the next request opens a new one).
  void Close();
  // Connected, with no response left to read: fit for another request, from anyone.
  bool Idle() const { return connected_ && active_ == nullptr; }
  // http(s)://host:port, for messages.
  std::string Origin() const;
  const std::string& host() const { return host_; }

 private:
  friend class BodyReader;

  std::unique_ptr<BodyReader> OpenOnce(http::request<http::string_body> request, const ChunkSource* chunks,
                                       std::chrono::milliseconds timeout);
  void Connect();
  void Drop();
  // Whether nothing is there to read right now: no bytes buffered unparsed, none pending in
  // TLS, none readable on the socket (a read would wait).
  bool NothingReadable();
  template <class Op>
  boost::system::error_code Run(Op&& op);
  template <class F>
  void WithStream(F&& f);
  void SetTimeout(std::chrono::milliseconds timeout);
  [[noreturn]] void Fail(std::string_view what, const boost::system::error_code& ec);

  std::string host_;
  uint16_t port_;
  std::shared_ptr<ssl::context> tls_;
  std::chrono::milliseconds connect_timeout_;
  std::chrono::milliseconds resume_timeout_;
  std::function<bool(const ResumeProgress&)> on_resuming_;
  net::io_context io_;
  std::unique_ptr<beast::tcp_stream> plain_;
  std::unique_ptr<beast::ssl_stream<beast::tcp_stream>> secure_;
  beast::flat_buffer buffer_;
  bool connected_ = false;
  uint64_t generation_ = 0;  // bumped on every drop, so a stale BodyReader can tell
  BodyReader* active_ = nullptr;
  std::chrono::steady_clock::time_point deadline_{};
  std::shared_ptr<const std::atomic<bool>> legacy_;
  bool Legacy() const { return legacy_ && legacy_->load(); }
};

std::string UrlEncode(std::string_view text);

}  // namespace chargewire::client::detail
