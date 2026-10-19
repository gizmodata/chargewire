#include "http.hpp"

#include <boost/asio/ssl.hpp>
#include <boost/url.hpp>

#include <cctype>
#include <cstring>
#include <optional>

#include "ids.hpp"
#include "platform.hpp"
#include "tls_context.hpp"
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

  bool ClientGone() override { return PeerClosed(beast::get_lowest_layer(stream_).socket()); }

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
  if (response.head_length) {
    http::response<http::empty_body> message{response.status, version};
    ApplyHeaders(message, response, keep_alive);
    message.content_length(*response.head_length);
    http::response_serializer<http::empty_body> serializer{message};
    co_await http::async_write_header(stream, serializer, net::use_awaitable);
    co_return keep_alive;
  }
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

// A streamed request body (an upload) is handed on in pieces of up to this size: few
// enough handoffs to the engine that each costs nothing next to the bytes it carries.
// (StreamingBodySource, below.)
constexpr std::size_t kBodyChunk = 1024 * 1024;

// Reads a request body piece by piece from the connection, after the headers.
template <class Stream>
class ParserBodySource : public BodySource {
 public:
  ParserBodySource(Stream& stream, beast::flat_buffer& buffer,
                   http::request_parser<http::buffer_body>& parser, std::size_t piece_size)
      : stream_(stream), buffer_(buffer), parser_(parser), piece_size_(piece_size) {}

  // Reads straight into the piece it returns, so the bytes are copied once, from the
  // connection's buffer.
  Task<std::optional<std::string>> Next() override {
    std::string piece;
    while (!parser_.is_done()) {
      piece.resize(piece_size_);
      auto& body = parser_.get().body();
      body.data = piece.data();
      body.size = piece.size();
      beast::get_lowest_layer(stream_).expires_after(kReadTimeout);
      boost::system::error_code ec;
      co_await http::async_read(stream_, buffer_, parser_,
                                net::redirect_error(net::use_awaitable, ec));
      if (ec && ec != http::error::need_buffer) {
        throw boost::system::system_error(ec);
      }
      const std::size_t got = piece.size() - body.size;
      if (got > 0) {
        piece.resize(got);
        co_return piece;
      }
    }
    co_return std::nullopt;
  }

  bool done() const { return parser_.is_done(); }

 private:
  Stream& stream_;
  beast::flat_buffer& buffer_;
  http::request_parser<http::buffer_body>& parser_;
  const std::size_t piece_size_;
};

// A streamed request body (an upload), read straight from the connection in large
// reads instead of through Beast's parser, which reads at most 64 KB at a time and
// hands the body over in pieces of whatever each read returned. It decodes the body's
// framing itself: Content-Length, or chunked transfer coding (RFC 9112 §7.1), with
// limits on the size lines and trailers a client can send.
template <class Stream>
class StreamingBodySource : public BodySource {
 public:
  StreamingBodySource(Stream& stream, beast::flat_buffer& buffer, std::optional<std::uint64_t> length,
                      bool chunked)
      : stream_(stream), buffer_(buffer), chunked_(chunked), remaining_(length.value_or(0)) {
    done_ = !chunked_ && remaining_ == 0;
    if (chunked_) state_ = State::kSize;
  }

  Task<std::optional<std::string>> Next() override {
    if (done_) co_return std::nullopt;
    std::string piece(kBodyChunk, '\0');
    std::size_t filled = 0;
    while (filled < piece.size() && !done_) {
      if (buffer_.size() == 0) {
        if (filled > 0 && !stream_ready_) {
          break;  // hand on what arrived rather than wait with it
        }
        beast::get_lowest_layer(stream_).expires_after(kReadTimeout);
        const std::size_t got =
            co_await stream_.async_read_some(buffer_.prepare(kRawRead), net::use_awaitable);
        buffer_.commit(got);
        stream_ready_ = got == kRawRead;  // a full read: more is probably waiting
      }
      filled += Decode(piece.data() + filled, piece.size() - filled);
    }
    piece.resize(filled);
    if (filled == 0) co_return std::nullopt;
    co_return piece;
  }

  // The whole body has been read (the connection can carry another request).
  bool complete() const { return done_; }

 private:
  enum class State { kData, kSize, kDataEnd, kTrailers };
  static constexpr std::size_t kRawRead = 1024 * 1024;
  static constexpr std::size_t kMaxLine = 4096;      // a chunk-size line, extensions included
  static constexpr std::size_t kMaxTrailers = 16 * 1024;

  [[noreturn]] static void Bad() {
    throw boost::system::system_error(http::error::bad_chunk);
  }

  // Moves body bytes from the read buffer into `out`; returns how many.
  std::size_t Decode(char* out, std::size_t room) {
    std::size_t written = 0;
    while (buffer_.size() > 0 && !done_ && written < room) {
      const char* data = static_cast<const char*>(buffer_.data().data());
      const std::size_t size = buffer_.size();
      switch (state_) {
        case State::kData: {
          const std::size_t take = static_cast<std::size_t>(
              std::min<std::uint64_t>({remaining_, size, room - written}));
          std::memcpy(out + written, data, take);
          written += take;
          remaining_ -= take;
          buffer_.consume(take);
          if (remaining_ == 0) {
            if (chunked_) {
              state_ = State::kDataEnd;
            } else {
              done_ = true;
            }
          }
          break;
        }
        case State::kSize: {  // hex digits, then extensions up to CRLF
          const char* end = static_cast<const char*>(std::memchr(data, '\n', size));
          const std::size_t line = end ? static_cast<std::size_t>(end - data) + 1 : size;
          line_ += std::string_view(data, line);
          buffer_.consume(line);
          if (line_.size() > kMaxLine) Bad();
          if (!end) break;
          std::uint64_t value = 0;
          std::size_t digits = 0;
          for (; digits < line_.size() && std::isxdigit(static_cast<unsigned char>(line_[digits])); ++digits) {
            if (digits >= 15) Bad();  // no chunk is 2^60 bytes
            const char c = line_[digits];
            value = value * 16 + static_cast<std::uint64_t>(
                std::isdigit(static_cast<unsigned char>(c)) ? c - '0' : (std::tolower(c) - 'a' + 10));
          }
          if (digits == 0 || line_.size() < 2 || line_[line_.size() - 2] != '\r') Bad();
          const char after = line_[digits];
          if (after != '\r' && after != ';' && after != ' ' && after != '\t') Bad();
          line_.clear();
          remaining_ = value;
          state_ = value == 0 ? State::kTrailers : State::kData;
          break;
        }
        case State::kDataEnd: {  // the CRLF after a chunk's data
          line_ += data[0];
          buffer_.consume(1);
          if (line_.size() == 2) {
            if (line_ != "\r\n") Bad();
            line_.clear();
            state_ = State::kSize;
          }
          break;
        }
        case State::kTrailers: {  // trailer fields (ignored), then an empty line
          const char* end = static_cast<const char*>(std::memchr(data, '\n', size));
          const std::size_t line = end ? static_cast<std::size_t>(end - data) + 1 : size;
          line_ += std::string_view(data, line);
          trailer_bytes_ += line;
          buffer_.consume(line);
          if (trailer_bytes_ > kMaxTrailers) Bad();
          if (!end) break;
          if (line_ == "\r\n") {
            done_ = true;
          } else if (line_.size() < 2 || line_[line_.size() - 2] != '\r') {
            Bad();
          }
          line_.clear();
          break;
        }
      }
    }
    return written;
  }

  Stream& stream_;
  beast::flat_buffer& buffer_;
  const bool chunked_;
  State state_ = State::kData;
  std::uint64_t remaining_;
  std::string line_;
  std::size_t trailer_bytes_ = 0;
  bool done_ = false;
  bool stream_ready_ = false;
};

// `tls` and `peer`: the TLS configuration and the connection's client certificate, or null.
// `tls_version`: the connection's (SSL_get_version), static; empty without TLS.
template <class Stream>
Task<void> ServeRequests(Stream& stream, const HttpLimits& limits, const Handler& handler,
                         TlsContext* tls = nullptr, PeerCertificate* peer = nullptr,
                         std::string_view tls_version = {}) {
  beast::flat_buffer buffer;
  int auth_failures = 0;
  std::string peer_address;
  {
    boost::system::error_code ec;
    const auto remote = beast::get_lowest_layer(stream).socket().remote_endpoint(ec);
    if (!ec) peer_address = remote.address().to_string() + ":" + std::to_string(remote.port());
  }
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
    std::shared_ptr<StreamingBodySource<Stream>> streaming;
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
      request.peer = peer_address;
      request.tls_version = tls_version;
      request.connection_auth_failures = &auth_failures;
      if (peer != nullptr) {
        request.client_certificate = peer->info();
        request.client_certificate_problem = tls->Recheck(*peer);
      }
      const bool target_ok = ParseTarget(message.target(), request);

      std::optional<Response> early;
      if (!target_ok) {
        early = limits.error_response(http::status::bad_request, "malformed request target");
      } else if (limits.streams_body(request)) {
        auto length = parser.content_length();
        streaming = std::make_shared<StreamingBodySource<Stream>>(
            stream, buffer, length ? std::optional<std::uint64_t>(*length) : std::nullopt,
            message.chunked());
        request.body_source = streaming;
      } else if (message.has_content_length() &&
                 *parser.content_length() > limits.max_body_bytes) {
        early = limits.error_response(
            http::status::payload_too_large,
            "request body is larger than " + std::to_string(limits.max_body_bytes) + " bytes");
      } else {
        // Read the whole body, up to the limit.
        parser.body_limit(limits.max_body_bytes);
        ParserBodySource<Stream> source(stream, buffer, parser, 64 * 1024);
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
      body_unread = streaming ? !streaming->complete() : !parser.is_done();
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
      tls_ = std::make_unique<TlsContext>(*options.tls);
    }
  }

  uint16_t Bind() {
    const tcp::endpoint endpoint(net::ip::make_address(options_.host), options_.port);
    acceptor_.open(endpoint.protocol());
    PrepareListener(acceptor_);
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
        co_await EndConnection(stream);
        co_return;
      }
      // The context of the moment (it changes when the CRL file does), held for the
      // connection's life.
      const TlsContext::Current context = tls_->Get();
      TlsStream tls_stream(std::move(stream), context.context->native_handle());
      // Why the handshake refused a client certificate, if it does (the verify callback).
      HandshakeRefusal refusal;
      SSL_set_ex_data(tls_stream.native_handle(), TlsContext::RefusalIndex(), &refusal);
      std::string peer_address;
      {
        boost::system::error_code ec;
        const auto remote = beast::get_lowest_layer(tls_stream).socket().remote_endpoint(ec);
        if (!ec) peer_address = remote.address().to_string() + ":" + std::to_string(remote.port());
      }
      try {
        beast::get_lowest_layer(tls_stream).expires_after(kHandshakeTimeout);
        try {
          co_await tls_stream.async_handshake(net::use_awaitable);
        } catch (const std::exception&) {
          if (!refusal.reason.empty() && limits_.handshake_refused) {
            limits_.handshake_refused(refusal.reason, refusal.thumbprint, refusal.subject, peer_address);
          }
          throw;
        }
        SSL_set_ex_data(tls_stream.native_handle(), TlsContext::RefusalIndex(), nullptr);
        auto peer = tls_->PeerOf(tls_stream.native_handle(), context.generation);
        co_await ServeRequests(tls_stream, limits_, handler_, tls_.get(), peer.get(),
                               SSL_get_version(tls_stream.native_handle()));
      } catch (const std::exception&) {
        // the connection failed; it still ends below
      }
      // Always: it also waits for a send still under way, which uses the stream.
      beast::get_lowest_layer(tls_stream).expires_after(kHandshakeTimeout);
      boost::system::error_code ignored;
      co_await tls_stream.async_shutdown(net::redirect_error(net::use_awaitable, ignored));
      co_await EndConnection(beast::get_lowest_layer(tls_stream));  // after close_notify
    } catch (const std::exception&) {
      // A connection that fails takes nothing else down.
    }
  }

  net::io_context& io_;
  ServerOptions options_;
  HttpLimits limits_;
  Handler handler_;
  tcp::acceptor acceptor_;
  std::unique_ptr<TlsContext> tls_;
};

HttpServer::HttpServer(net::io_context& io, const ServerOptions& options, HttpLimits limits,
                       Handler handler)
    : impl_(std::make_unique<Impl>(io, options, std::move(limits), std::move(handler))) {}

HttpServer::~HttpServer() = default;

uint16_t HttpServer::Bind() { return impl_->Bind(); }

void HttpServer::StartAccepting() { impl_->StartAccepting(); }

void HttpServer::Close() { impl_->Close(); }

}  // namespace warpsql::detail
