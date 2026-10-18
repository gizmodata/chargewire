// warp-duckdb: the Warp SQL reference server, DuckDB 2.0 behind the SDK.
//
//   WARP_DUCKDB_PASSWORD=… warp-duckdb [--database PATH] [--read-only] [--host HOST]
//       [--port PORT] [--username NAME] [--tls-cert PEM --tls-key PEM [--tls-client-ca PEM]]
//       [--cors-origin ORIGIN]... [--drain-seconds N] [--token-lifetime-seconds N]
//       [--session-idle-timeout-seconds N] [--finished-statement-ttl-seconds N]
//
// The password comes from the environment, never the command line, so it doesn't show
// in process listings.

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include "duckdb_backend.hpp"
#include "warpsql/server.hpp"
#include "warpsql/stop_signals.hpp"

namespace {

struct Flags {
  warpduckdb::DuckDBBackend::Options backend;
  warpsql::ServerOptions server;
  std::string username = "warp";
  int drain_seconds = 30;
};

[[noreturn]] void Usage(const std::string& problem) {
  std::cerr << "warp-duckdb: " << problem << "\n"
            << "usage: WARP_DUCKDB_PASSWORD=... warp-duckdb [--database PATH] [--read-only]\n"
            << "       [--host HOST] [--port PORT] [--username NAME]\n"
            << "       [--tls-cert PEM --tls-key PEM [--tls-client-ca PEM]]\n"
            << "       [--cors-origin ORIGIN]... [--drain-seconds N] [--token-lifetime-seconds N]\n"
            << "       [--session-idle-timeout-seconds N] [--finished-statement-ttl-seconds N]\n";
  std::exit(2);
}

Flags ParseFlags(int argc, char** argv) {
  Flags flags;
  flags.server.port = 31337;
  warpsql::TlsOptions tls;
  for (int i = 1; i < argc; i++) {
    const std::string flag = argv[i];
    auto value = [&]() -> std::string {
      if (i + 1 >= argc) Usage(flag + " needs a value");
      return argv[++i];
    };
    if (flag == "--database") flags.backend.database = value();
    else if (flag == "--read-only") flags.backend.read_only = true;
    else if (flag == "--host") flags.server.host = value();
    else if (flag == "--port") flags.server.port = static_cast<uint16_t>(std::stoi(value()));
    else if (flag == "--username") flags.username = value();
    else if (flag == "--tls-cert") tls.certificate_chain_file = value();
    else if (flag == "--tls-key") tls.private_key_file = value();
    else if (flag == "--tls-client-ca") tls.client_ca_file = value();
    else if (flag == "--cors-origin") flags.server.cors_allowed_origins.push_back(value());
    else if (flag == "--drain-seconds") flags.drain_seconds = std::stoi(value());
    // Short lifetimes let clients test token renewal, session eviction and statement expiry.
    else if (flag == "--token-lifetime-seconds")
      flags.server.token_lifetime = std::chrono::seconds(std::stoi(value()));
    else if (flag == "--session-idle-timeout-seconds")
      flags.server.session_idle_timeout = std::chrono::seconds(std::stoi(value()));
    else if (flag == "--finished-statement-ttl-seconds")
      flags.server.finished_statement_ttl = std::chrono::seconds(std::stoi(value()));
    else Usage("unknown flag " + flag);
  }
  if (!tls.certificate_chain_file.empty() || !tls.private_key_file.empty()) {
    if (tls.certificate_chain_file.empty() || tls.private_key_file.empty()) {
      Usage("--tls-cert and --tls-key go together");
    }
    flags.server.tls = tls;
  }
  return flags;
}

}  // namespace

int main(int argc, char** argv) {
  warpsql::InitializeProcess();
  Flags flags = ParseFlags(argc, argv);
  const char* password = std::getenv("WARP_DUCKDB_PASSWORD");
  if (password == nullptr || *password == '\0') {
    Usage("set WARP_DUCKDB_PASSWORD");
  }

  // Before any thread starts, so only stop_signals.Wait() below sees the stop requests
  // (SIGINT/SIGTERM; on Windows, the console's Ctrl+C/Ctrl+Break/close events).
  warpsql::StopSignals stop_signals;

  try {
    auto backend = std::make_shared<warpduckdb::DuckDBBackend>(flags.backend);
    auto authenticator =
        std::make_shared<warpduckdb::StaticPasswordAuthenticator>(flags.username, password);
    warpsql::Server server(flags.server, backend, authenticator);
    server.Start();
    std::cout << "warp-duckdb listening on " << (flags.server.tls ? "https" : "http") << "://"
              << flags.server.host << ":" << server.port() << "/warp" << std::endl;

    // First signal: drain (finish running work, refuse new work). Second: stop now.
    // Joined before `server` goes: a second signal that arrives after a drain finished
    // must not reach a server already destroyed.
    std::atomic<bool> exiting = false;
    std::thread signals([&server, &stop_signals, &exiting, drain = flags.drain_seconds] {
      stop_signals.Wait();
      if (exiting) return;
      std::cout << "draining (up to " << drain << " s); signal again to stop now" << std::endl;
      server.Drain(std::chrono::seconds(drain));
      stop_signals.Wait();
      if (exiting) return;
      server.Stop();
    });

    server.Wait();
    exiting = true;
    stop_signals.Wake();
    signals.join();
    std::cout << "stopped" << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "warp-duckdb: " << e.what() << "\n";
    return 1;
  }
  return 0;
}
