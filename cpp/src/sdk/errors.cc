#include "warpsql/errors.hpp"

namespace warpsql {

namespace {

struct CodeInfo {
  std::string_view name;
  int status;
  std::string_view title;
};

CodeInfo Info(ErrorCode code) {
  switch (code) {
    case ErrorCode::kBadRequest: return {"bad_request", 400, "Bad request"};
    case ErrorCode::kNotFound: return {"not_found", 404, "No such endpoint"};
    case ErrorCode::kMethodNotAllowed: return {"method_not_allowed", 405, "Method not allowed"};
    case ErrorCode::kPayloadTooLarge: return {"payload_too_large", 413, "Request body too large"};
    case ErrorCode::kHeadersTooLarge: return {"headers_too_large", 431, "Request headers too large"};
    case ErrorCode::kResultNotReady: return {"result_not_ready", 409, "Result not ready"};
    case ErrorCode::kSqlError: return {"sql_error", 400, "SQL error"};
    case ErrorCode::kInvalidCredentials: return {"invalid_credentials", 401, "Invalid credentials"};
    case ErrorCode::kInvalidToken: return {"invalid_token", 401, "Invalid access token"};
    case ErrorCode::kSessionEnded: return {"session_ended", 401, "Session ended"};
    case ErrorCode::kPermissionDenied: return {"permission_denied", 403, "Permission denied"};
    case ErrorCode::kReadOnly: return {"read_only", 403, "Read-only"};
    case ErrorCode::kSessionNotFound: return {"session_not_found", 404, "Session not found"};
    case ErrorCode::kStatementNotFound: return {"statement_not_found", 404, "Statement not found"};
    case ErrorCode::kPreparedNotFound: return {"prepared_not_found", 404, "Prepared statement not found"};
    case ErrorCode::kResultNotResumable: return {"result_not_resumable", 409, "Result not resumable"};
    case ErrorCode::kResultExpired: return {"result_expired", 410, "Result expired"};
    case ErrorCode::kUnsupportedArrowType: return {"unsupported_arrow_type", 415, "Unsupported Arrow type"};
    case ErrorCode::kSessionElsewhere: return {"session_elsewhere", 421, "Session on another server"};
    case ErrorCode::kIdempotencyKeyReused: return {"idempotency_key_reused", 422, "Idempotency key reused"};
    case ErrorCode::kTooManySessions: return {"too_many_sessions", 429, "Too many sessions"};
    case ErrorCode::kResultQuotaExceeded: return {"result_quota_exceeded", 429, "Result quota exceeded"};
    case ErrorCode::kRateLimited: return {"rate_limited", 429, "Rate limited"};
    case ErrorCode::kStatementTimeout: return {"statement_timeout", 409, "Statement timed out"};
    case ErrorCode::kStatementCancelled: return {"statement_cancelled", 409, "Statement cancelled"};
    case ErrorCode::kExecutionError: return {"execution_error", 422, "Execution error"};
    case ErrorCode::kInternalError: return {"internal_error", 500, "Internal server error"};
    case ErrorCode::kNotImplemented: return {"not_implemented", 501, "Not implemented"};
    case ErrorCode::kServerDraining: return {"server_draining", 503, "Server shutting down"};
    case ErrorCode::kUnknownOption: return {"unknown_option", 400, "Unknown session option"};
    case ErrorCode::kInvalidOptionValue: return {"invalid_option_value", 400, "Invalid session option value"};
    case ErrorCode::kIngestNotFound: return {"ingest_not_found", 404, "Ingest not found"};
    case ErrorCode::kIngestIncomplete: return {"ingest_incomplete", 409, "Ingest incomplete"};
    case ErrorCode::kIngestPartExists: return {"ingest_part_exists", 409, "Ingest part already uploaded"};
    case ErrorCode::kIngestExpired: return {"ingest_expired", 410, "Ingest expired"};
    case ErrorCode::kPartitionNotFound: return {"partition_not_found", 404, "Partition not found"};
    case ErrorCode::kCertificateRequired: return {"certificate_required", 401, "Client certificate required"};
    case ErrorCode::kUnknownCertificateIdentity: return {"unknown_certificate_identity", 401, "Unknown certificate identity"};
    case ErrorCode::kCertificateRevoked: return {"certificate_revoked", 401, "Certificate revoked"};
    case ErrorCode::kCertificateMismatch: return {"certificate_mismatch", 401, "Certificate mismatch"};
    case ErrorCode::kCertificateIdentityMismatch: return {"certificate_identity_mismatch", 401, "Certificate identity mismatch"};
    case ErrorCode::kUnsupportedGrantType: return {"unsupported_grant_type", 400, "Unsupported grant type"};
    case ErrorCode::kFlowNotFound: return {"flow_not_found", 404, "Sign-in flow not found"};
    case ErrorCode::kInvalidReturnTo: return {"invalid_return_to", 400, "Invalid return_to"};
  }
  return {"internal_error", 500, "Internal server error"};
}

}  // namespace

std::string_view CodeName(ErrorCode code) { return Info(code).name; }

std::string_view CodeTitle(ErrorCode code) { return Info(code).title; }

int HttpStatus(ErrorCode code) { return Info(code).status; }

}  // namespace warpsql
