#include "warpsql/errors.hpp"

namespace warpsql {

namespace {

struct CodeInfo {
  std::string_view name;
  int status;
};

CodeInfo Info(ErrorCode code) {
  switch (code) {
    case ErrorCode::kBadRequest: return {"bad_request", 400};
    case ErrorCode::kNotFound: return {"not_found", 404};
    case ErrorCode::kMethodNotAllowed: return {"method_not_allowed", 405};
    case ErrorCode::kPayloadTooLarge: return {"payload_too_large", 413};
    case ErrorCode::kHeadersTooLarge: return {"headers_too_large", 431};
    case ErrorCode::kResultNotReady: return {"result_not_ready", 409};
    case ErrorCode::kSqlError: return {"sql_error", 400};
    case ErrorCode::kInvalidCredentials: return {"invalid_credentials", 401};
    case ErrorCode::kInvalidToken: return {"invalid_token", 401};
    case ErrorCode::kSessionEnded: return {"session_ended", 401};
    case ErrorCode::kPermissionDenied: return {"permission_denied", 403};
    case ErrorCode::kReadOnly: return {"read_only", 403};
    case ErrorCode::kSessionNotFound: return {"session_not_found", 404};
    case ErrorCode::kStatementNotFound: return {"statement_not_found", 404};
    case ErrorCode::kPreparedNotFound: return {"prepared_not_found", 404};
    case ErrorCode::kResultNotResumable: return {"result_not_resumable", 409};
    case ErrorCode::kResultExpired: return {"result_expired", 410};
    case ErrorCode::kUnsupportedArrowType: return {"unsupported_arrow_type", 415};
    case ErrorCode::kSessionElsewhere: return {"session_elsewhere", 421};
    case ErrorCode::kIdempotencyKeyReused: return {"idempotency_key_reused", 422};
    case ErrorCode::kTooManySessions: return {"too_many_sessions", 429};
    case ErrorCode::kResultQuotaExceeded: return {"result_quota_exceeded", 429};
    case ErrorCode::kRateLimited: return {"rate_limited", 429};
    case ErrorCode::kStatementTimeout: return {"statement_timeout", 500};
    case ErrorCode::kStatementCancelled: return {"statement_cancelled", 500};
    case ErrorCode::kExecutionError: return {"execution_error", 500};
    case ErrorCode::kNotImplemented: return {"not_implemented", 501};
    case ErrorCode::kServerDraining: return {"server_draining", 503};
    case ErrorCode::kUnknownOption: return {"unknown_option", 400};
    case ErrorCode::kInvalidOptionValue: return {"invalid_option_value", 400};
  }
  return {"execution_error", 500};
}

}  // namespace

std::string_view CodeName(ErrorCode code) { return Info(code).name; }

int HttpStatus(ErrorCode code) { return Info(code).status; }

}  // namespace warpsql
