#include "http.hpp"

#include <boost/asio/ssl.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/url.hpp>

#include <optional>

#include "ids.hpp"

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

template <class Stream>
class ChunkWriter : public BodyWriter {
 public:
  explicit ChunkWriter(Stream& stream) : stream_(stream) {}

  Task<void> Write(std::string_view bytes) override {
    if (bytes.empty()) {
      co_return;  // an empty chunk would end the body
    }
    beast::get_lowest_layer(stream_).expires_after(kWriteTimeout);
    co_await net::async_write(stream_, http::make_chunk(net::buffer(bytes.data(), bytes.size())),
                              net::use_awaitable);
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

template <class Stream>
Task<void> ServeRequests(Stream& stream, const HttpLimits& limits, const Handler& handler) {
  beast::flat_buffer buffer;
  for (;;) {
    http::request_parser<http::string_body> parser;
    parser.header_limit(static_cast<std::uint32_t>(limits.max_header_bytes));
    parser.body_limit(limits.max_body_bytes);
    beast::get_lowest_layer(stream).expires_after(kReadTimeout);
    boost::system::error_code ec;
    co_await http::async_read(stream, buffer, parser, net::redirect_error(net::use_awaitable, ec));

    Response response;
    unsigned version = 11;
    bool keep_alive = false;
    if (ec == http::error::header_limit) {
      response = limits.error_response(
          http::status::request_header_fields_too_large,
          "request headers are larger than " + std::to_string(limits.max_header_bytes) + " bytes");
    } else if (ec == http::error::body_limit) {
      response = limits.error_response(
          http::status::payload_too_large,
          "request body is larger than " + std::to_string(limits.max_body_bytes) + " bytes");
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
      request.body = std::move(message.body());
      request.request_id = NewId("r");
      if (!ParseTarget(message.target(), request)) {
        response = limits.error_response(http::status::bad_request, "malformed request target");
      } else {
        try {
          response = co_await handler(request);
        } catch (const std::exception& e) {
          response = limits.error_response(http::status::internal_server_error, e.what());
        }
      }
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
      beast::ssl_stream<beast::tcp_stream> tls_stream(std::move(stream), *tls_);
      beast::get_lowest_layer(tls_stream).expires_after(kHandshakeTimeout);
      co_await tls_stream.async_handshake(ssl::stream_base::server, net::use_awaitable);
      co_await ServeRequests(tls_stream, limits_, handler_);
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
