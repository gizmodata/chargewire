#include "json.hpp"

#include <cctype>
#include <ctime>
#include <format>

#include "platform.hpp"

namespace warpsql::detail {

namespace {

// Is the integer literal `digits` (an optional '-', then digits) outside what a JSON
// number parses into exactly here (int64 or uint64)?
bool BeyondSixtyFourBits(std::string_view digits) {
  const bool negative = digits.starts_with('-');
  if (negative) digits.remove_prefix(1);
  const std::string_view limit = negative ? "9223372036854775808" : "18446744073709551615";
  return digits.size() > limit.size() || (digits.size() == limit.size() && digits > limit);
}

// The body with every integer literal outside the 64-bit range (which the JSON parser
// would read as a double, rounding it) written as {"\u0000integer": "<digits>"}
// (kWideIntegerKey), so a bind parameter keeps its exact value (spec §6.1).
std::string ExactWideIntegers(std::string_view body) {
  std::string out;
  out.reserve(body.size());
  bool in_string = false;
  for (std::size_t i = 0; i < body.size();) {
    const char c = body[i];
    if (in_string) {
      out += c;
      if (c == '\\' && i + 1 < body.size()) {
        out += body[i + 1];
        i += 2;
        continue;
      }
      in_string = c != '"';
      i++;
      continue;
    }
    if (c == '"') {
      in_string = true;
      out += c;
      i++;
      continue;
    }
    if (c != '-' && (c < '0' || c > '9')) {
      out += c;
      i++;
      continue;
    }
    // A number: its integer part, then (for a fraction or exponent) the rest, unchanged.
    std::size_t end = i + 1;
    while (end < body.size() && body[end] >= '0' && body[end] <= '9') end++;
    const bool integer = end == body.size() || (body[end] != '.' && body[end] != 'e' && body[end] != 'E');
    if (!integer) {
      while (end < body.size() && (std::isdigit(static_cast<unsigned char>(body[end])) || body[end] == '.' ||
                                   body[end] == 'e' || body[end] == 'E' || body[end] == '+' || body[end] == '-')) {
        end++;
      }
    }
    const std::string_view token = body.substr(i, end - i);
    if (integer && token != "-" && BeyondSixtyFourBits(token)) {
      out += "{\"\\u0000integer\":\"";
      out += token;
      out += "\"}";
    } else {
      out += token;
    }
    i = end;
  }
  return out;
}

}  // namespace

json::object ParseObject(std::string_view body) {
  if (body.empty()) {
    return {};
  }
  boost::system::error_code ec;
  json::value value = json::parse(ExactWideIntegers(body), ec);
  if (ec || !value.is_object()) {
    throw Error(ErrorCode::kBadRequest, "the request body must be a JSON object");
  }
  return std::move(value.as_object());
}

std::optional<std::string> OptionalString(const json::object& object, std::string_view key) {
  const json::value* value = object.if_contains(key);
  if (value == nullptr || value->is_null()) {
    return std::nullopt;
  }
  if (!value->is_string()) {
    throw Error(ErrorCode::kBadRequest, std::format("'{}' must be a string", key));
  }
  return std::string(value->as_string());
}

std::string RequiredString(const json::object& object, std::string_view key) {
  auto value = OptionalString(object, key);
  if (!value) {
    throw Error(ErrorCode::kBadRequest, std::format("'{}' is required", key));
  }
  return *value;
}

std::string Rfc3339(WallClock::time_point time) {
  const auto millis =
      std::chrono::floor<std::chrono::milliseconds>(time.time_since_epoch()).count() % 1000;
  const std::time_t seconds = WallClock::to_time_t(time);
  const std::tm utc = UtcTime(seconds);
  char buffer[32];
  // Format the written length, not the array: some libc++ versions format a char array
  // as all of its elements, NULs and uninitialized bytes included.
  const std::size_t length = std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &utc);
  return std::format("{}.{:03}Z", std::string_view(buffer, length), millis);
}

json::object Problem(ErrorCode code, std::string_view detail, std::string_view request_id) {
  const std::string_view name = CodeName(code);
  json::object problem{
      {"type", std::format("urn:warp-sql:error:{}", name)},
      {"title", name},
      {"status", HttpStatus(code)},
      {"code", name},
      {"detail", detail},
  };
  if (!request_id.empty()) {
    problem["request_id"] = request_id;
  }
  return problem;
}

json::object Problem(const Error& error, std::string_view request_id) {
  json::object problem = Problem(error.code(), error.what(), request_id);
  if (!error.sqlstate().empty()) {
    problem["sqlstate"] = error.sqlstate();
  }
  if (error.code() == ErrorCode::kSessionEnded) {
    // Spec §5.6: a session_ended error always says why.
    problem["reason"] = error.reason().empty() ? std::string(session_ended_reason::kKilled) : error.reason();
  }
  return problem;
}

namespace {

void AddSecurityHeaders(Response& response) {
  response.headers.emplace_back("X-Content-Type-Options", "nosniff");
  response.headers.emplace_back("Cache-Control", "no-store");
}

}  // namespace

Response JsonResponse(http::status status, const json::value& body) {
  Response response;
  response.status = status;
  response.content_type = kJson;
  response.body = json::serialize(body);
  AddSecurityHeaders(response);
  return response;
}

Response ProblemResponse(const Error& error, std::string_view request_id) {
  Response response;
  response.status = static_cast<http::status>(HttpStatus(error.code()));
  response.content_type = kProblemJson;
  response.body = json::serialize(Problem(error, request_id));
  AddSecurityHeaders(response);
  if (error.code() == ErrorCode::kInvalidToken || error.code() == ErrorCode::kSessionEnded ||
      error.code() == ErrorCode::kCertificateMismatch) {
    response.headers.emplace_back("WWW-Authenticate", "Bearer");
  }
  if (error.code() == ErrorCode::kServerDraining) {
    response.headers.emplace_back("Retry-After", "5");
  }
  return response;
}

}  // namespace warpsql::detail
