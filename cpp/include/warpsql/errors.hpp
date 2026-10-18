// Protocol errors (spec §11). A backend reports a failure by throwing Error with the
// code that describes it; the SDK turns it into a problem document.
#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace warpsql {

enum class ErrorCode {
  kBadRequest,
  kNotFound,
  kMethodNotAllowed,
  kPayloadTooLarge,
  kHeadersTooLarge,
  kResultNotReady,
  kSqlError,
  kInvalidCredentials,
  kInvalidToken,
  kSessionEnded,
  kPermissionDenied,
  kReadOnly,
  kSessionNotFound,
  kStatementNotFound,
  kPreparedNotFound,
  kResultNotResumable,
  kResultExpired,
  kUnsupportedArrowType,
  kSessionElsewhere,
  kIdempotencyKeyReused,
  kTooManySessions,
  kResultQuotaExceeded,
  kRateLimited,
  kStatementTimeout,
  kStatementCancelled,
  kExecutionError,
  kNotImplemented,
  kServerDraining,
  kUnknownOption,
  kInvalidOptionValue,
  kIngestNotFound,
  kIngestIncomplete,
  kIngestPartExists,
  kIngestExpired,
};

// Why a session ended (spec §5.6), the `reason` of a session_ended error.
namespace session_ended_reason {
inline constexpr const char* kKilled = "killed";
inline constexpr const char* kCredentialsChanged = "credentials_changed";
inline constexpr const char* kPrincipalDropped = "principal_dropped";
inline constexpr const char* kTokenExpired = "token_expired";
}  // namespace session_ended_reason

// The stable machine-readable name ("permission_denied").
std::string_view CodeName(ErrorCode code);
// The HTTP status a response carrying this code uses.
int HttpStatus(ErrorCode code);

class Error : public std::runtime_error {
 public:
  Error(ErrorCode code, std::string detail, std::string sqlstate = {})
      : std::runtime_error(detail), code_(code), sqlstate_(std::move(sqlstate)) {}

  // A copy owns its message: std::runtime_error's copy shares the message buffer through a
  // reference count, and errors are handed from engine threads to I/O threads, where the
  // last owner frees it (a thread sanitizer cannot see that count, and an error that
  // outlives its thread must not depend on it).
  Error(const Error& other)
      : std::runtime_error(std::string(other.what())), code_(other.code_), sqlstate_(other.sqlstate_),
        reason_(other.reason_) {}
  Error& operator=(const Error& other) {
    if (this != &other) {
      static_cast<std::runtime_error&>(*this) = std::runtime_error(std::string(other.what()));
      code_ = other.code_;
      sqlstate_ = other.sqlstate_;
      reason_ = other.reason_;
    }
    return *this;
  }

  // A session_ended error and its reason (spec §5.6, session_ended_reason).
  static Error SessionEnded(std::string reason, std::string detail) {
    Error error(ErrorCode::kSessionEnded, std::move(detail));
    error.reason_ = std::move(reason);
    return error;
  }

  ErrorCode code() const { return code_; }
  // The SQL standard's five-character class and subclass, or empty.
  const std::string& sqlstate() const { return sqlstate_; }
  // A session_ended error's reason; a session_ended error without one reports "killed".
  const std::string& reason() const { return reason_; }

 private:
  ErrorCode code_;
  std::string sqlstate_;
  std::string reason_;
};

}  // namespace warpsql
