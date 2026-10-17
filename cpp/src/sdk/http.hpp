// HTTP/1.1 (optionally over TLS) on Boost.Beast, with one coroutine per connection.
// It knows nothing about Warp SQL: a Handler turns each Request into a Response.
#pragma once

#include <functional>
#include <map>
#include <optional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common.hpp"
#include "warpsql/server.hpp"

namespace warpsql::detail {

// A request body read on demand, for bodies too large to hold in memory (ingest).
class BodySource {
 public:
  virtual ~BodySource() = default;
  // The next piece of the body, or nullopt at its end. Throws if the connection fails.
  virtual Task<std::optional<std::string>> Next() = 0;
};

struct Request {
  http::verb method = http::verb::unknown;
  std::string path;                          // decoded, without the query
  std::map<std::string, std::string> query;  // decoded parameters
  http::fields headers;
  std::string body;                         // when the body is read up front
  std::shared_ptr<BodySource> body_source;  // when it streams (see HttpLimits::streams_body)
  std::string request_id;

  std::string_view Header(std::string_view name) const;
};

// Writes the body of a streamed response, one HTTP chunk per call.
class BodyWriter {
 public:
  virtual ~BodyWriter() = default;
  virtual Task<void> Write(std::string_view bytes) = 0;
};

struct Response {
  http::status status = http::status::ok;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string content_type;
  std::string body;

  // For a streamed body: runs after the headers are sent. Returning true ends the
  // body normally; returning false or throwing drops the connection without the final
  // chunk, so the client sees an incomplete body (spec §6.6).
  std::function<Task<bool>(BodyWriter&)> stream;

  // Close the connection after this response.
  bool close = false;
};

using Handler = std::function<Task<Response>(Request&)>;

// What the HTTP layer answers itself, without a handler: a header block over the
// limit (431) or a request it can't parse (400).
struct HttpLimits {
  std::size_t max_header_bytes;
  std::size_t max_body_bytes;  // for bodies read up front
  // Which requests stream their body instead (no size limit).
  std::function<bool(const Request&)> streams_body;
  std::function<Response(http::status status, std::string_view detail)> error_response;
};

class HttpServer {
 public:
  HttpServer(net::io_context& io, const ServerOptions& options, HttpLimits limits,
             Handler handler);
  ~HttpServer();

  // Binds; returns the port.
  uint16_t Bind();
  // Accepts connections until Close().
  void StartAccepting();
  // Stops accepting new connections.
  void Close();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace warpsql::detail
