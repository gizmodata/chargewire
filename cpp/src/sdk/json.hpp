// JSON bodies and problem documents (spec §2.4, §11).
#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "common.hpp"
#include "http.hpp"
#include "warpsql/errors.hpp"

namespace warpsql::detail {

inline constexpr std::string_view kJson = "application/json";
inline constexpr std::string_view kProblemJson = "application/problem+json";
inline constexpr std::string_view kArrowStream = "application/vnd.apache.arrow.stream";

// Parses a request body that must be a JSON object; an empty body is an empty object.
json::object ParseObject(std::string_view body);

// Optional typed members; throw Error(kBadRequest) when present with the wrong type.
std::optional<std::string> OptionalString(const json::object& object, std::string_view key);
std::string RequiredString(const json::object& object, std::string_view key);

// RFC 3339 in UTC with milliseconds: "2026-10-16T21:04:05.123Z".
std::string Rfc3339(WallClock::time_point time);

// The problem document for `error`.
json::object Problem(const Error& error, std::string_view request_id);
json::object Problem(ErrorCode code, std::string_view detail, std::string_view request_id);

// Responses with the security headers every response carries (spec §9.5).
Response JsonResponse(http::status status, const json::value& body);
Response ProblemResponse(const Error& error, std::string_view request_id);

}  // namespace warpsql::detail
