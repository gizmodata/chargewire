// warp-duckdb: the Warp SQL reference server, DuckDB 2.0 behind the SDK.
//
//   WARP_DUCKDB_PASSWORD=… warp-duckdb [--database PATH] [--read-only] [--host HOST]
//       [--port PORT] [--username NAME] [--tls-cert PEM --tls-key PEM [--tls-client-ca PEM]]
//       [--mtls-require-client-cert=BOOL] [--mtls-crl-file PEM] [--mtls-identity-source SRC]
//       [--mtls-admin-identities A,B] [--mtls-require-identity-match[=BOOL]]
//       [--cors-origin ORIGIN]... [--drain-seconds N] [--token-lifetime-seconds N]
//       [--session-idle-timeout-seconds N] [--finished-statement-ttl-seconds N]
//
// The password comes from the environment, never the command line, so it doesn't show
// in process listings. The --mtls-* flags are named as Hardcharger's options are, so test
// harnesses pass both servers the same flags; `--flag=value` works for every flag.

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
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
  std::vector<std::string> certificate_identities;
  int drain_seconds = 30;
};

[[noreturn]] void Usage(const std::string& problem) {
  std::cerr << "warp-duckdb: " << problem << "\n"
            << "usage: WARP_DUCKDB_PASSWORD=... warp-duckdb [--database PATH] [--read-only]\n"
            << "       [--host HOST] [--port PORT] [--username NAME]\n"
            << "       [--tls-cert PEM --tls-key PEM [--tls-client-ca PEM]]\n"
            << "       [--mtls-require-client-cert=BOOL] [--mtls-crl-file PEM]\n"
            << "       [--mtls-identity-source san_email|san_uri|subject_cn]\n"
            << "       [--mtls-admin-identities A,B] [--mtls-require-identity-match[=BOOL]]\n"
            << "       [--cors-origin ORIGIN]... [--drain-seconds N] [--token-lifetime-seconds N]\n"
            << "       [--session-idle-timeout-seconds N] [--finished-statement-ttl-seconds N]\n";
  std::exit(2);
}

Flags ParseFlags(int argc, char** argv) {
  Flags flags;
  flags.server.port = 31337;
  warpsql::TlsOptions tls;
  bool mtls_option = false;  // an --mtls-* flag, which needs --tls-client-ca
  for (int i = 1; i < argc; i++) {
    std::string flag = argv[i];
    std::optional<std::string> inline_value;
    if (const auto eq = flag.find('='); eq != std::string::npos && flag.starts_with("--")) {
      inline_value = flag.substr(eq + 1);
      flag.resize(eq);
    }
    auto value = [&]() -> std::string {
      if (inline_value) return *inline_value;
      if (i + 1 >= argc) Usage(flag + " needs a value");
      return argv[++i];
    };
    // A boolean: --flag alone is true; --flag=true|false.
    auto boolean = [&]() -> bool {
      if (!inline_value || *inline_value == "true") return true;
      if (*inline_value == "false") return false;
      Usage(flag + " is true or false");
    };
    if (flag.starts_with("--mtls-")) mtls_option = true;
    if (flag == "--database") flags.backend.database = value();
    else if (flag == "--read-only") flags.backend.read_only = true;
    else if (flag == "--host") flags.server.host = value();
    else if (flag == "--port") flags.server.port = static_cast<uint16_t>(std::stoi(value()));
    else if (flag == "--username") flags.username = value();
    else if (flag == "--tls-cert") tls.certificate_chain_file = value();
    else if (flag == "--tls-key") tls.private_key_file = value();
    else if (flag == "--tls-client-ca") tls.client_ca_file = value();
    else if (flag == "--mtls-require-client-cert") tls.require_client_certificate = boolean();
    else if (flag == "--mtls-crl-file") tls.client_crl_file = value();
    else if (flag == "--mtls-identity-source") {
      try {
        tls.client_identity_source = warpsql::ParseClientIdentitySource(value());
      } catch (const std::invalid_argument& e) {
        Usage(e.what());
      }
    } else if (flag == "--mtls-admin-identities") {
      std::stringstream list(value());
      for (std::string item; std::getline(list, item, ',');) {
        if (!item.empty()) flags.certificate_identities.push_back(item);
      }
    } else if (flag == "--mtls-require-identity-match") tls.require_identity_match = boolean();
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
  if (mtls_option && (!flags.server.tls || flags.server.tls->client_ca_file.empty())) {
    Usage("the --mtls-* flags need --tls-cert, --tls-key and --tls-client-ca");
  }
  if (flags.server.tls) {
    flags.server.tls->on_crl_reload = [](bool ok, const std::string& message) {
      std::cerr << "warp-duckdb: " << (ok ? "" : "WARNING: ") << message << std::endl;
    };
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
        std::make_shared<warpduckdb::StaticPasswordAuthenticator>(flags.username, password,
                                                                  flags.certificate_identities);
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
