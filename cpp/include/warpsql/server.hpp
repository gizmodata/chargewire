// A Warp SQL server: the protocol over HTTP/1.1 (and TLS), in front of a Backend.
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "warpsql/backend.hpp"

namespace warpsql {

struct TlsOptions {
  std::string certificate_chain_file;  // PEM
  std::string private_key_file;        // PEM
  std::string client_ca_file;          // PEM; when set, clients must present a certificate
};

struct ServerOptions {
  std::string host = "127.0.0.1";
  uint16_t port = 0;  // 0: any free port; see Server::port()
  std::optional<TlsOptions> tls;

  int http_threads = 0;    // 0: one per core
  int engine_threads = 0;  // 0: two per core

  std::size_t max_header_bytes = 16 * 1024;  // larger header blocks get 431
  // Bodies read whole: JSON, and Arrow parameters for prepared statements. Ingest
  // streams and has no limit.
  std::size_t max_json_body_bytes = 64 * 1024 * 1024;

  std::chrono::seconds token_lifetime{3600};
  std::chrono::seconds session_idle_timeout{900};
  std::chrono::seconds result_ttl{3600};
  std::chrono::hours idempotency_window{24};
  std::chrono::seconds default_wait{5};  // submit without Prefer: wait / respond-async
  std::chrono::seconds max_wait{60};     // cap on Prefer: wait
  std::chrono::seconds health_timeout{5};
  std::size_t retained_bytes_per_principal = std::size_t{1} << 30;

  std::vector<std::string> cors_allowed_origins;  // empty: no cross-origin access
};

class Server {
 public:
  Server(ServerOptions options, std::shared_ptr<Backend> backend,
         std::shared_ptr<Authenticator> authenticator);
  ~Server();
  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  // Binds and starts serving on background threads.
  void Start();
  // The port actually bound (useful with port 0).
  uint16_t port() const;

  // Stops accepting new sessions and statements, lets running statements finish for
  // up to `grace`, then cancels the rest and stops (spec §11.1). Returns at once.
  void Drain(std::chrono::seconds grace);
  // Stops now, cancelling everything.
  void Stop();
  // Blocks until the server has stopped.
  void Wait();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace warpsql
