// What an engine implements to sit behind a Warp SQL server.
//
// The SDK owns everything the protocol requires for safety and correctness:
// authentication, binding handles to principals, retries, retention, streaming,
// cancellation plumbing and draining. A backend only runs SQL. Every method has a
// default that reports "not implemented", so an engine overrides what it supports,
// as with Arrow Flight SQL's server base class.
//
// Threading: the SDK calls at most one method of an EngineSession at a time, on an
// engine thread, except Interrupt() and Progress(), which may be called from any
// thread while another call is running.
#pragma once

#include <arrow/record_batch.h>
#include <boost/json.hpp>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "warpsql/errors.hpp"

namespace warpsql {

// The identity an access token establishes (spec §9.1). Handles belong to a principal
// by name; the other fields are what the server's authorization reads.
struct Principal {
  std::string name;
  std::string role;               // e.g. "admin", "user", "readonly"; empty if unused
  std::string auth_method;        // how the principal signed in, e.g. "password"
  boost::json::object claims;     // anything else the server carries (e.g. catalog access)
  // The client certificate a token is bound to (spec §4.4, RFC 8705 "x5t#S256": base64url
  // SHA-256 of its DER, no padding); empty: the token is not bound. A TokenService must
  // carry it from Issue() to Validate() (see TokenService::BindsCertificates); the SDK then
  // refuses every request with the token on a connection without that certificate.
  std::string certificate_thumbprint;

  bool SameIdentity(const Principal& other) const { return name == other.name; }
};

// A client certificate the TLS handshake verified against the server's client CAs (spec
// §4.4): its leaf, described. Only verified certificates are ever described.
struct ClientCertificate {
  std::string thumbprint;  // base64url SHA-256 of the DER encoding, no padding (x5t#S256)
  std::string subject;     // RFC 2253, for logs
  std::string issuer;      // RFC 2253, for logs
  std::string serial;      // hex
  // The values of each kind an identity can come from, as the certificate carries them
  // (byte for byte: the SDK refuses values with control characters when it maps one).
  std::vector<std::string> san_emails;   // rfc822Name subject alternative names
  std::vector<std::string> san_uris;     // uniformResourceIdentifier subject alternative names
  std::vector<std::string> common_names; // the subject's CN attributes
  // The certificate has more than one subjectAltName extension: no SAN value is trusted.
  bool duplicate_san_extension = false;
};

// How certificate identities compare (spec §4.4): a URI-form identity ("spiffe://...", any
// "scheme://..."), whose path is case-sensitive, matches only exactly; an e-mail address or
// a common name matches ignoring ASCII case.
bool IsUriIdentity(std::string_view identity);
bool CertificateIdentitiesMatch(std::string_view a, std::string_view b);

// What running one statement produced: rows, or the count of rows it changed.
struct StatementOutcome {
  std::shared_ptr<arrow::RecordBatchReader> rows;  // null when the statement returns no rows
  std::optional<int64_t> rows_affected;            // for statements without rows, when known
  // The statement changes data (INSERT … RETURNING, …): when its reader goes away it runs
  // to the end with its rows discarded, rather than being cancelled (spec §6.5).
  bool finish_if_abandoned = false;
};

// The rows of a bulk ingest, readable from several threads at once (spec §8.1). The
// reader EngineSession::Ingest receives is one. Each NextIndexed() call takes the next
// batch from the upload in order, under a lock, and decodes, decompresses and validates
// it outside the lock, in the calling thread: an engine that calls it from N threads
// does that work N at a time. ReadNext() is NextIndexed() without the position.
class IngestBatches : public arrow::RecordBatchReader {
 public:
  // The next batch and its position in the upload (0, 1, …), or a null batch at the end.
  virtual arrow::Status NextIndexed(std::shared_ptr<arrow::RecordBatch>* batch, int64_t* index) = 0;
  // Whether the batches' order is the upload's (one stream); false for a partitioned
  // ingest (spec §8.3), whose parts arrive in no defined order. The positions are
  // still distinct and increase for each thread that reads.
  virtual bool Ordered() const { return true; }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    int64_t index = 0;
    return NextIndexed(batch, &index);
  }
};

// Per-option failures from EngineSession::SetOptions (spec §5.3).
using OptionErrors = std::map<std::string, Error>;

// A statement prepared once and executed many times (spec §7).
class PreparedStatement {
 public:
  virtual ~PreparedStatement() = default;
  // One field per parameter, in binding order (named parameters carry their name).
  virtual std::shared_ptr<arrow::Schema> ParameterSchema() = 0;
  // The rows' schema, or null when the statement returns no rows or the schema is only
  // known at execution.
  virtual std::shared_ptr<arrow::Schema> ResultSchema() = 0;
  // Whether executing returns rows (true) or only a count (false); nullopt when it is
  // only known at execution. A non-null ResultSchema() means rows.
  virtual std::optional<bool> ReturnsRows() { return std::nullopt; }
};

// Bind parameters for one request: JSON (spec §6.1), or Arrow rows, one execution per
// row (spec §7.2).
struct Parameters {
  boost::json::value json = nullptr;
  std::shared_ptr<arrow::RecordBatchReader> rows;
};

enum class IngestMode { kCreate, kAppend, kReplace, kCreateAppend };

// Where a bulk ingest writes (spec §8.1). Empty catalog/schema: the session's current.
struct IngestTarget {
  std::string catalog;
  std::string schema;
  std::string table;
  IngestMode mode = IngestMode::kCreateAppend;
  bool temporary = false;
};

// Metadata filters (spec §10). Unset fields don't narrow the search; patterns use SQL
// LIKE syntax.
struct MetadataFilter {
  std::optional<std::string> catalog;
  std::optional<std::string> db_schema;  // exact (keys) or pattern (db-schemas, tables)
  std::optional<std::string> table;      // exact (keys) or pattern (tables)
  std::vector<std::string> table_types;
  bool include_schema = false;
  // cross-reference: the foreign-key side
  std::optional<std::string> fk_catalog;
  std::optional<std::string> fk_db_schema;
  std::optional<std::string> fk_table;
  // type-info: only this data type (Flight SQL's data_type code)
  std::optional<int32_t> data_type;
};

enum class MetadataKind {
  kCatalogs,
  kDbSchemas,
  kTables,
  kTableTypes,
  kPrimaryKeys,
  kImportedKeys,
  kExportedKeys,
  kCrossReference,
  kTypeInfo,
};

// The exact result schema every backend returns for `kind` (Arrow Flight SQL's).
std::shared_ptr<arrow::Schema> MetadataSchema(MetadataKind kind, bool include_schema = false);

class EngineSession {
 public:
  virtual ~EngineSession() = default;

  // Runs SQL. May block for as long as the statement runs; reading the returned rows
  // may block too. `parameters` is the request's bind parameters (spec §6.1): null for
  // none, an array for positional parameters, or an object for named ones.
  virtual StatementOutcome Execute(std::string_view sql, const boost::json::value& parameters);

  virtual std::unique_ptr<PreparedStatement> Prepare(std::string_view sql);
  // Executes `statement` once (JSON parameters or none) or once per Arrow parameter row;
  // rows from several executions are concatenated in order.
  virtual StatementOutcome ExecutePrepared(PreparedStatement& statement,
                                           const Parameters& parameters);

  // Writes every row of `rows` into `target`, atomically where the engine can, and
  // returns how many (spec §8.1). Each batch is validated as it is read. `rows` is an
  // IngestBatches (see there), so an engine can read it from several threads.
  virtual int64_t Ingest(const IngestTarget& target,
                         std::shared_ptr<arrow::RecordBatchReader> rows);

  // Metadata as `MetadataSchema(kind)` describes it, only what the principal may see.
  virtual std::shared_ptr<arrow::RecordBatchReader> Metadata(MetadataKind kind,
                                                             const MetadataFilter& filter);

  // Asks the running Execute(), or the read of its rows, to stop. Safe from any thread.
  virtual void Interrupt();

  // Fraction of the running statement done, in [0, 1], when the engine knows.
  virtual std::optional<double> Progress();

  // The timeout for this session's statements that set none (Warp-Statement-Timeout),
  // e.g. one the session chose with SQL. nullopt: the server's default applies; zero:
  // no timeout. Safe from any thread.
  virtual std::optional<std::chrono::milliseconds> DefaultStatementTimeout();

  // Applies session options; returns the ones that failed, by name.
  virtual OptionErrors SetOptions(const boost::json::object& options);
  // The session's current options.
  virtual boost::json::object Options();
  // True (once) after a statement that may have changed Options() itself, such as SQL
  // USE moving the current schema; the SDK then reads Options() again (spec §5.2).
  virtual bool TakeOptionsChanged() { return false; }

  virtual void Commit();
  virtual void Rollback();
};

struct ServerIdentity {
  std::string name;
  std::string version;
};

class Backend {
 public:
  virtual ~Backend() = default;

  virtual ServerIdentity Identity() = 0;

  // Opens the engine side of a new session for `principal`.
  virtual std::unique_ptr<EngineSession> OpenSession(const Principal& principal,
                                                     const boost::json::object& options) = 0;

  // Reports whether the engine can take work (spec §3.2). Throws if not. Must not hang:
  // the SDK gives up on it after the health timeout either way.
  virtual void CheckHealth() {}

  // Facts for GET …/metadata/server-info (spec §10), merged over the SDK's own.
  virtual boost::json::object ServerInfo() { return {}; }
};

// Issues and checks the bearer tokens the server hands out (spec §4.1-§4.2). The SDK's
// default keeps random tokens in memory; a server that runs several replicas, or must
// accept its tokens after a restart, supplies signed ones (e.g. JWTs).
class TokenService {
 public:
  virtual ~TokenService() = default;

  struct Issued {
    std::string token;
    std::chrono::seconds expires_in;
  };
  virtual Issued Issue(const Principal& principal) = 0;
  // The principal a token belongs to. Throws Error(kInvalidToken) when it is missing,
  // malformed, forged or expired.
  virtual Principal Validate(std::string_view token) = 0;
  // Whether Validate() returns the Principal::certificate_thumbprint Issue() was given.
  // The SDK offers the client_certificate grant (spec §4.4) only with a service that does:
  // a token that lost its binding would work without the certificate.
  virtual bool BindsCertificates() const { return false; }
  // Called once a request's token has passed every check: Validate(), the certificate
  // binding (spec §4.4) and, for a session's endpoints, the session's own binding. A
  // service that logs successful uses logs here, so a refused token never reads as used.
  virtual void Accepted(std::string_view token, const Principal& principal) {
    (void)token;
    (void)principal;
  }
};

// Turns credentials into a principal (spec §4.1).
class Authenticator {
 public:
  virtual ~Authenticator() = default;

  // grant_type "password". Throws Error(kInvalidCredentials) on any mismatch.
  virtual Principal AuthenticatePassword(std::string_view username, std::string_view password);

  // grant_type "token_exchange". Throws Error(kInvalidCredentials) if the token is not
  // one the server trusts.
  virtual Principal ExchangeToken(std::string_view subject_token);

  // grant_type "client_certificate" (spec §4.4): `identity` is what the connection's
  // verified client certificate maps to (TlsOptions::client_identity_source), already
  // checked to be one value without control characters; `certificate` is the certificate.
  // Returns the principal that identity signs in as. Throws
  // Error(kUnknownCertificateIdentity) when it names no principal who may sign in this way.
  // The SDK binds the token it issues to the certificate and sets auth_method to
  // "client_certificate". Offered only when GrantTypes() lists "client_certificate" and
  // the server verifies client certificates.
  virtual Principal AuthenticateCertificate(std::string_view identity,
                                            const ClientCertificate& certificate);

  // The grant types this authenticator supports, for discovery (spec §3.1).
  virtual boost::json::array GrantTypes() = 0;
};

}  // namespace warpsql
