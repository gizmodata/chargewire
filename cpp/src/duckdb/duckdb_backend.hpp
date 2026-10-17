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
  };

  explicit DuckDBBackend(Options options);
  ~DuckDBBackend() override;

  warpsql::ServerIdentity Identity() override;
  std::unique_ptr<warpsql::EngineSession> OpenSession(const warpsql::Principal& principal,
                                                      const boost::json::object& options) override;
  void CheckHealth() override;
  boost::json::object ServerInfo() override;

  duckdb_v2_instance_handle instance() const { return instance_.get(); }
  const std::string& duckdb_version() const { return version_; }

 protected:
  // Creates the engine side of a session; servers built on this backend return their
  // own DuckDBSession subclass.
  virtual std::unique_ptr<warpsql::EngineSession> NewSession(const warpsql::Principal& principal);

 private:
  Environment environment_;
  Instance instance_;
  std::string version_;
  std::mutex health_mutex_;
  Connection health_connection_;
};

// The single-user authenticator the reference server uses.
class StaticPasswordAuthenticator : public warpsql::Authenticator {
 public:
  StaticPasswordAuthenticator(std::string username, std::string password)
      : username_(std::move(username)), password_(std::move(password)) {}

  warpsql::Principal AuthenticatePassword(std::string_view username,
                                          std::string_view password) override;
  boost::json::array GrantTypes() override { return {"password"}; }

 private:
  std::string username_;
  std::string password_;
};

}  // namespace warpduckdb
