// The reference backend: DuckDB 2.0 behind the Warp SQL SDK, through DuckDB's v2 C
// API only.
#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "duckdb_v2.hpp"
#include "warpsql/backend.hpp"

namespace warpduckdb {

class DuckDBBackend : public warpsql::Backend {
 public:
  struct Options {
    std::string database = ":memory:";  // a file path, or ":memory:"
    bool read_only = false;
    // DuckDB settings fixed when the instance starts, e.g. {"memory_limit", "8GB"}.
    std::vector<std::pair<std::string, std::string>> instance_options;
    // Options for attaching the database, e.g. {"STORAGE_VERSION", "v1.4.0"}.
    std::vector<std::pair<std::string, std::string>> attach_options;
    // Most rows per Arrow batch in statement results; 0: DuckDB's default (131072).
    std::size_t result_batch_rows = 0;
  };

  explicit DuckDBBackend(Options options);
  ~DuckDBBackend() override;

  warpsql::ServerIdentity Identity() override;
  std::unique_ptr<warpsql::EngineSession> OpenSession(const warpsql::Principal& principal, const std::string& session_id,
                                                      const boost::json::object& options) override;
  void CheckHealth() override;
  boost::json::object ServerInfo() override;

  duckdb_v2_instance_handle instance() const { return instance_.get(); }
  std::size_t result_batch_rows() const { return result_batch_rows_; }
  const std::string& duckdb_version() const { return version_; }

 protected:
  // Creates the engine side of session `session_id` (its Warp SQL id); servers built on
  // this backend return their own DuckDBSession subclass.
  virtual std::unique_ptr<warpsql::EngineSession> NewSession(const warpsql::Principal& principal,
                                                             const std::string& session_id);

 private:
  Environment environment_;
  Instance instance_;
  std::string version_;
  std::size_t result_batch_rows_ = 0;
  std::mutex health_mutex_;
  Connection health_connection_;
};

// The single-user authenticator the reference server uses.
// One user with a password, plus (with mutual TLS) the certificate identities allowed to
// sign in as themselves with the client_certificate grant (spec §4.4).
class StaticPasswordAuthenticator : public warpsql::Authenticator {
 public:
  StaticPasswordAuthenticator(std::string username, std::string password,
                              std::vector<std::string> certificate_identities = {})
      : username_(std::move(username)),
        password_(std::move(password)),
        certificate_identities_(std::move(certificate_identities)) {}

  warpsql::Principal AuthenticatePassword(std::string_view username,
                                          std::string_view password) override;
  // An identity on the list (ignoring ASCII case) signs in as itself.
  warpsql::Principal AuthenticateCertificate(std::string_view identity,
                                             const warpsql::ClientCertificate& certificate) override;
  boost::json::array GrantTypes() override {
    boost::json::array grants{"password"};
    if (!certificate_identities_.empty()) grants.emplace_back("client_certificate");
    return grants;
  }

 private:
  std::string username_;
  std::string password_;
  std::vector<std::string> certificate_identities_;
};

}  // namespace warpduckdb
