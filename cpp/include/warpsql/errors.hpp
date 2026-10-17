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
};

// The stable machine-readable name ("permission_denied").
std::string_view CodeName(ErrorCode code);
// The HTTP status a response carrying this code uses.
int HttpStatus(ErrorCode code);

class Error : public std::runtime_error {
 public:
  Error(ErrorCode code, std::string detail, std::string sqlstate = {})
      : std::runtime_error(detail), code_(code), sqlstate_(std::move(sqlstate)) {}

  ErrorCode code() const { return code_; }
  // The SQL standard's five-character class and subclass, or empty.
  const std::string& sqlstate() const { return sqlstate_; }

 private:
  ErrorCode code_;
  std::string sqlstate_;
};

}  // namespace warpsql
