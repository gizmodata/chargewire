// The reference backend: DuckDB 2.0 behind the Warp SQL SDK, through DuckDB's v2 C
// API only.
#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "duckdb_v2.hpp"
#include "warpsql/backend.hpp"

namespace warpduckdb {

class DuckDBBackend : public warpsql::Backend {
 public:
  struct Options {
    std::string database = ":memory:";  // a file path, or ":memory:"
    bool read_only = false;
  };

  explicit DuckDBBackend(Options options);
  ~DuckDBBackend() override;

  warpsql::ServerIdentity Identity() override;
  std::unique_ptr<warpsql::EngineSession> OpenSession(const warpsql::Principal& principal,
                                                      const boost::json::object& options) override;
  void CheckHealth() override;
  boost::json::object ServerInfo() override;

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
