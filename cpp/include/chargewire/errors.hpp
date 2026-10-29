// Protocol errors (spec §11). A backend reports a failure by throwing Error with the
// code that describes it; the SDK turns it into a problem document.
#pragma once

#include <stdexcept>
#include <string>
#include <string_view>

namespace chargewire {

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
  kInternalError,
  kNotImplemented,
  kServerDraining,
  kUnknownOption,
  kInvalidOptionValue,
  kIngestNotFound,
  kIngestIncomplete,
  kIngestPartExists,
  kIngestExpired,
  kPartitionNotFound,
  // Client certificates (spec §4.4).
  kCertificateRequired,
  kUnknownCertificateIdentity,
  kCertificateRevoked,
  kCertificateMismatch,
  kCertificateIdentityMismatch,
  kUnsupportedGrantType,
  // Browser sign-in (spec §4.3).
  kFlowNotFound,
  kInvalidReturnTo,
};

// Why a session ended (spec §5.6) or a token stopped standing (spec §4.2): the `reason`
// of a session_ended error, and of an invalid_token error for a token the server issued
// and verified (never for one it can't tell is its own).
namespace reason {
inline constexpr const char* kKilled = "killed";  // session_ended only
inline constexpr const char* kCredentialsChanged = "credentials_changed";
inline constexpr const char* kPrincipalDropped = "principal_dropped";
inline constexpr const char* kTokenRevoked = "token_revoked";
inline constexpr const char* kTokenExpired = "token_expired";
}  // namespace reason

// The stable machine-readable name ("permission_denied").
std::string_view CodeName(ErrorCode code);
// The problem document's human-readable title ("Permission denied"), the same for every
// error with this code (spec §11).
std::string_view CodeTitle(ErrorCode code);
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
        reason_(other.reason_), cancel_reason_(other.cancel_reason_) {}
  Error& operator=(const Error& other) {
    if (this != &other) {
      static_cast<std::runtime_error&>(*this) = std::runtime_error(std::string(other.what()));
      code_ = other.code_;
      sqlstate_ = other.sqlstate_;
      reason_ = other.reason_;
      cancel_reason_ = other.cancel_reason_;
    }
    return *this;
  }

  // A session_ended error and its reason (spec §5.6, chargewire::reason).
  static Error SessionEnded(std::string reason, std::string detail) {
    Error error(ErrorCode::kSessionEnded, std::move(detail));
    error.reason_ = std::move(reason);
    return error;
  }
  // An invalid_token error that says why the token no longer stands (spec §4.2,
  // chargewire::reason). Only for a token the server issued and verified: a token it can't
  // tell is its own (malformed, forged, tampered, unknown) gets a plain
  // Error(kInvalidToken), so a reason never tells anyone but the token's holder about
  // its principal.
  static Error InvalidToken(std::string reason, std::string detail) {
    Error error(ErrorCode::kInvalidToken, std::move(detail));
    error.reason_ = std::move(reason);
    return error;
  }

  // A cancelled statement's error (spec §6.2): statement_cancelled, or statement_timeout
  // for `cancel_reason` "timeout", carrying the cancel_reason its problem document shows.
  static Error Cancelled(std::string cancel_reason, std::string detail) {
    Error error(cancel_reason == "timeout" ? ErrorCode::kStatementTimeout : ErrorCode::kStatementCancelled,
                std::move(detail), "57014");
    error.cancel_reason_ = std::move(cancel_reason);
    return error;
  }

  ErrorCode code() const { return code_; }
  // The SQL standard's five-character class and subclass, or empty.
  const std::string& sqlstate() const { return sqlstate_; }
  // A session_ended error's reason (one without reports "killed"), or an invalid_token
  // error's (empty when the server doesn't say).
  const std::string& reason() const { return reason_; }
  // A cancelled statement's cancel_reason (spec §6.2), or empty.
  const std::string& cancel_reason() const { return cancel_reason_; }

 private:
  ErrorCode code_;
  std::string sqlstate_;
  std::string reason_;
  std::string cancel_reason_;
};

}  // namespace chargewire
