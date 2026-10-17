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

  bool SameIdentity(const Principal& other) const { return name == other.name; }
};

// What running one statement produced: rows, or the count of rows it changed.
struct StatementOutcome {
  std::shared_ptr<arrow::RecordBatchReader> rows;  // null when the statement returns no rows
  std::optional<int64_t> rows_affected;            // for statements without rows, when known
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
  // returns how many (spec §8.1). The SDK has already validated each batch.
  virtual int64_t Ingest(const IngestTarget& target,
                         std::shared_ptr<arrow::RecordBatchReader> rows);

  // Metadata as `MetadataSchema(kind)` describes it, only what the principal may see.
  virtual std::shared_ptr<arrow::RecordBatchReader> Metadata(MetadataKind kind,
                                                             const MetadataFilter& filter);

  // Asks the running Execute(), or the read of its rows, to stop. Safe from any thread.
  virtual void Interrupt();

  // Fraction of the running statement done, in [0, 1], when the engine knows.
  virtual std::optional<double> Progress();

  // Applies session options; returns the ones that failed, by name.
  virtual OptionErrors SetOptions(const boost::json::object& options);
  // The session's current options.
  virtual boost::json::object Options();

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

  // The grant types this authenticator supports, for discovery (spec §3.1).
  virtual boost::json::array GrantTypes() = 0;
};

}  // namespace warpsql
