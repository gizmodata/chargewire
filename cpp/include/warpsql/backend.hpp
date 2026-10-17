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

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "warpsql/errors.hpp"

namespace warpsql {

// The identity an access token establishes (spec §9.1).
struct Principal {
  std::string name;
  bool operator==(const Principal&) const = default;
};

// What running one statement produced: rows, or the count of rows it changed.
struct StatementOutcome {
  std::shared_ptr<arrow::RecordBatchReader> rows;  // null when the statement returns no rows
  std::optional<int64_t> rows_affected;            // for statements without rows, when known
};

// Per-option failures from EngineSession::SetOptions (spec §5.3).
using OptionErrors = std::map<std::string, Error>;

class EngineSession {
 public:
  virtual ~EngineSession() = default;

  // Runs SQL. May block for as long as the statement runs; reading the returned rows
  // may block too. `parameters` is the request's bind parameters (spec §6.1): null for
  // none, an array for positional parameters, or an object for named ones.
  virtual StatementOutcome Execute(std::string_view sql, const boost::json::value& parameters);

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
};

// Turns credentials into a principal (spec §4.1). The SDK issues and checks the
// access tokens itself.
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
