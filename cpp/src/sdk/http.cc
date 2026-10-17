#include "http.hpp"

#include <boost/asio/ssl.hpp>
#include <boost/url.hpp>

#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <optional>

#include "ids.hpp"
#include "tls_stream.hpp"

namespace warpsql::detail {

namespace ssl = net::ssl;
using tcp = net::ip::tcp;

std::string_view Request::Header(std::string_view name) const {
  auto it = headers.find(name);
  return it == headers.end() ? std::string_view{} : std::string_view(it->value());
}

namespace {

// An idle keep-alive connection, or a request that stops arriving, is dropped after
// this; so is a client that stops reading a response.
constexpr auto kReadTimeout = std::chrono::seconds(60);
constexpr auto kWriteTimeout = std::chrono::seconds(60);
constexpr auto kHandshakeTimeout = std::chrono::seconds(30);

// Writes all of `buffers`. Unlike net::async_write, which hands the stream at most 64 KB
// per write, this passes everything on: a TlsStream then encrypts a whole gathered
// write at once, and a plain socket takes as much as it can per writev.
template <class Stream, class Buffers>
Task<void> WriteAll(Stream& stream, const Buffers& buffers) {
  beast::buffers_suffix<Buffers> rest(buffers);
  while (net::buffer_size(rest) > 0) {
    const std::size_t written = co_await stream.async_write_some(rest, net::use_awaitable);
    rest.consume(written);
  }
}

template <class Stream>
class ChunkWriter : public BodyWriter {
 public:
  explicit ChunkWriter(Stream& stream) : stream_(stream) {}

  Task<void> Write(std::string_view bytes) override {
    if (bytes.empty()) {
      co_return;  // an empty chunk would end the body
    }
    beast::get_lowest_layer(stream_).expires_after(kWriteTimeout);
    co_await WriteAll(stream_, http::make_chunk(net::buffer(bytes.data(), bytes.size())));
  }

  Task<void> WriteGather(const std::vector<net::const_buffer>& pieces) override {
    if (net::buffer_size(pieces) == 0) {
      co_return;
    }
    beast::get_lowest_layer(stream_).expires_after(kWriteTimeout);
    co_await WriteAll(stream_, http::make_chunk(pieces));
  }

  bool ClientGone() override {
    const int fd = beast::get_lowest_layer(stream_).socket().native_handle();
#ifdef POLLRDHUP
    // Linux reports the peer's shutdown even when bytes (a TLS close_notify) are pending.
    pollfd entry{fd, POLLRDHUP, 0};
    return ::poll(&entry, 1, 0) > 0 && (entry.revents & (POLLRDHUP | POLLHUP | POLLERR)) != 0;
#else
    char byte = 0;
    const ssize_t got = ::recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
    return got == 0 || (got < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
#endif
  }

 private:
  Stream& stream_;
};

bool ParseTarget(std::string_view target, Request& request) {
  auto url = boost::urls::parse_origin_form(target);
  if (!url) {
    return false;
  }
  request.path = url->path();
  for (const auto& param : url->params()) {
    request.query[param.key] = param.has_value ? param.value : std::string();
  }
  return true;
}

template <class Body>
void ApplyHeaders(http::response<Body>& message, const Response& response, bool keep_alive) {
  for (const auto& [name, value] : response.headers) {
    message.set(name, value);
  }
  if (!response.content_type.empty()) {
    message.set(http::field::content_type, response.content_type);
  }
  message.keep_alive(keep_alive);
}

// Writes `response`; returns whether the connection can carry another request.
template <class Stream>
Task<bool> WriteResponse(Stream& stream, Response& response, unsigned version, bool keep_alive) {
  keep_alive = keep_alive && !response.close;
  beast::get_lowest_layer(stream).expires_after(kWriteTimeout);
  if (!response.stream) {
    http::response<http::string_body> message{response.status, version};
    ApplyHeaders(message, response, keep_alive);
    message.body() = std::move(response.body);
    message.prepare_payload();
    co_await http::async_write(stream, message, net::use_awaitable);
    co_return keep_alive;
  }

  http::response<http::empty_body> message{response.status, version};
  ApplyHeaders(message, response, keep_alive);
  message.chunked(true);
  http::response_serializer<http::empty_body> serializer{message};
  co_await http::async_write_header(stream, serializer, net::use_awaitable);

  ChunkWriter<Stream> writer(stream);
  bool complete = false;
  try {
    complete = co_await response.stream(writer);
  } catch (...) {
    complete = false;
  }
  if (!complete) {
    co_return false;  // no final chunk: the client sees an incomplete body
  }
  beast::get_lowest_layer(stream).expires_after(kWriteTimeout);
  co_await net::async_write(stream, http::make_chunk_last(), net::use_awaitable);
  co_return keep_alive;
}

constexpr std::size_t kBodyChunk = 64 * 1024;

// Reads a request body piece by piece from the connection, after the headers.
template <class Stream>
class ParserBodySource : public BodySource {
 public:
  ParserBodySource(Stream& stream, beast::flat_buffer& buffer,
                   http::request_parser<http::buffer_body>& parser)
      : stream_(stream), buffer_(buffer), parser_(parser), chunk_(kBodyChunk, '\0') {}

  Task<std::optional<std::string>> Next() override {
    while (!parser_.is_done()) {
      auto& body = parser_.get().body();
      body.data = chunk_.data();
      body.size = chunk_.size();
      beast::get_lowest_layer(stream_).expires_after(kReadTimeout);
      boost::system::error_code ec;
      co_await http::async_read(stream_, buffer_, parser_,
                                net::redirect_error(net::use_awaitable, ec));
      if (ec && ec != http::error::need_buffer) {
        throw boost::system::system_error(ec);
      }
      const std::size_t got = chunk_.size() - body.size;
      if (got > 0) {
        co_return std::string(chunk_.data(), got);
      }
    }
    co_return std::nullopt;
  }

  bool done() const { return parser_.is_done(); }

 private:
  Stream& stream_;
  beast::flat_buffer& buffer_;
  http::request_parser<http::buffer_body>& parser_;
  std::string chunk_;
};

template <class Stream>
Task<void> ServeRequests(Stream& stream, const HttpLimits& limits, const Handler& handler) {
  beast::flat_buffer buffer;
  for (;;) {
    http::request_parser<http::buffer_body> parser;
    parser.header_limit(static_cast<std::uint32_t>(limits.max_header_bytes));
    // Beast checks Content-Length against the body limit while reading the headers; the
    // limit depends on the endpoint, so it is applied once the headers are known.
    parser.body_limit(boost::none);
    beast::get_lowest_layer(stream).expires_after(kReadTimeout);
    boost::system::error_code ec;
    co_await http::async_read_header(stream, buffer, parser,
                                     net::redirect_error(net::use_awaitable, ec));

    Response response;
    unsigned version = 11;
    bool keep_alive = false;
    bool body_unread = false;
    if (ec == http::error::header_limit) {
      response = limits.error_response(
          http::status::request_header_fields_too_large,
          "request headers are larger than " + std::to_string(limits.max_header_bytes) + " bytes");
    } else if (ec && ec.category() == http::make_error_code(http::error::bad_target).category()) {
      if (ec == http::error::end_of_stream) {
        co_return;  // the client closed an idle connection
      }
      response = limits.error_response(http::status::bad_request, "malformed HTTP request");
    } else if (ec) {
      co_return;  // timeout or network error: nothing to answer
    } else {
      auto& message = parser.get();
      version = message.version();
      keep_alive = message.keep_alive();
      Request request;
      request.method = message.method();
      request.headers = static_cast<const http::fields&>(message.base());
      request.request_id = NewId("r");
      const bool target_ok = ParseTarget(message.target(), request);

      std::optional<Response> early;
      if (!target_ok) {
        early = limits.error_response(http::status::bad_request, "malformed request target");
      } else if (limits.streams_body(request)) {
        auto source = std::make_shared<ParserBodySource<Stream>>(stream, buffer, parser);
        request.body_source = source;
      } else if (message.has_content_length() &&
                 *parser.content_length() > limits.max_body_bytes) {
        early = limits.error_response(
            http::status::payload_too_large,
            "request body is larger than " + std::to_string(limits.max_body_bytes) + " bytes");
      } else {
        // Read the whole body, up to the limit.
        parser.body_limit(limits.max_body_bytes);
        ParserBodySource<Stream> source(stream, buffer, parser);
        try {
          while (auto piece = co_await source.Next()) {
            request.body += *piece;
          }
        } catch (const boost::system::system_error& error) {
          if (error.code() != http::error::body_limit) {
            co_return;
          }
          early = limits.error_response(
              http::status::payload_too_large,
              "request body is larger than " + std::to_string(limits.max_body_bytes) + " bytes");
        }
      }

      if (early) {
        response = std::move(*early);
      } else {
        try {
          response = co_await handler(request);
        } catch (const std::exception& e) {
          response = limits.error_response(http::status::internal_server_error, e.what());
        }
      }
      // A body the handler didn't read leaves the connection unusable for the next request.
      body_unread = !parser.is_done();
    }

    if (body_unread) {
      response.close = true;
    }
    if (!co_await WriteResponse(stream, response, version, keep_alive)) {
      co_return;
    }
  }
}

}  // namespace

class HttpServer::Impl {
 public:
  Impl(net::io_context& io, const ServerOptions& options, HttpLimits limits, Handler handler)
      : io_(io),
        options_(options),
        limits_(std::move(limits)),
        handler_(std::move(handler)),
        acceptor_(net::make_strand(io)) {
    if (options.tls) {
      tls_.emplace(ssl::context::tls_server);
      tls_->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 |
                        ssl::context::no_sslv3 | ssl::context::no_tlsv1 |
                        ssl::context::no_tlsv1_1);
      tls_->use_certificate_chain_file(options.tls->certificate_chain_file);
      tls_->use_private_key_file(options.tls->private_key_file, ssl::context::pem);
      if (!options.tls->client_ca_file.empty()) {
        tls_->load_verify_file(options.tls->client_ca_file);
        tls_->set_verify_mode(ssl::verify_peer | ssl::verify_fail_if_no_peer_cert);
      }
    }
  }

  uint16_t Bind() {
    const tcp::endpoint endpoint(net::ip::make_address(options_.host), options_.port);
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(net::socket_base::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(net::socket_base::max_listen_connections);
    return acceptor_.local_endpoint().port();
  }

  void StartAccepting() {
    net::co_spawn(acceptor_.get_executor(), Accept(), net::detached);
  }

  void Close() {
    net::post(acceptor_.get_executor(), [this] {
      boost::system::error_code ignored;
      acceptor_.close(ignored);
    });
  }

 private:
  Task<void> Accept() {
    for (;;) {
      boost::system::error_code ec;
      // Each connection gets its own strand, so its coroutine never runs on two
      // threads at once.
      tcp::socket socket = co_await acceptor_.async_accept(
          net::make_strand(io_), net::redirect_error(net::use_awaitable, ec));
      if (ec == net::error::operation_aborted || !acceptor_.is_open()) {
        co_return;
      }
      if (ec) {
        continue;
      }
      auto executor = socket.get_executor();
      net::co_spawn(executor, Serve(std::move(socket)), net::detached);
    }
  }

  Task<void> Serve(tcp::socket socket) {
    try {
      beast::tcp_stream stream(std::move(socket));
      if (!tls_) {
        co_await ServeRequests(stream, limits_, handler_);
        boost::system::error_code ignored;
        stream.socket().shutdown(tcp::socket::shutdown_both, ignored);
        co_return;
      }
      TlsStream tls_stream(std::move(stream), tls_->native_handle());
      try {
        beast::get_lowest_layer(tls_stream).expires_after(kHandshakeTimeout);
        co_await tls_stream.async_handshake(net::use_awaitable);
        co_await ServeRequests(tls_stream, limits_, handler_);
      } catch (const std::exception&) {
        // the connection failed; it still ends below
      }
      // Always: it also waits for a send still under way, which uses the stream.
      beast::get_lowest_layer(tls_stream).expires_after(kHandshakeTimeout);
      boost::system::error_code ignored;
      co_await tls_stream.async_shutdown(net::redirect_error(net::use_awaitable, ignored));
    } catch (const std::exception&) {
      // A connection that fails takes nothing else down.
    }
  }

  net::io_context& io_;
  ServerOptions options_;
  HttpLimits limits_;
  Handler handler_;
  tcp::acceptor acceptor_;
  std::optional<ssl::context> tls_;
};

HttpServer::HttpServer(net::io_context& io, const ServerOptions& options, HttpLimits limits,
                       Handler handler)
    : impl_(std::make_unique<Impl>(io, options, std::move(limits), std::move(handler))) {}

HttpServer::~HttpServer() = default;

uint16_t HttpServer::Bind() { return impl_->Bind(); }

void HttpServer::StartAccepting() { impl_->StartAccepting(); }

void HttpServer::Close() { impl_->Close(); }

}  // namespace warpsql::detail
