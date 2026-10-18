// A Warp SQL server: the protocol over HTTP/1.1 (and TLS), in front of a Backend.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "warpsql/backend.hpp"

namespace warpsql {

// Where the client_certificate grant (spec §4.4) takes a certificate's identity from.
enum class ClientIdentitySource {
  kSanEmail,   // "san_email": the rfc822Name subject alternative name
  kSanUri,     // "san_uri": the uniformResourceIdentifier SAN (e.g. a SPIFFE ID)
  kSubjectCn,  // "subject_cn": the subject's common name
};
// "san_email", "san_uri" or "subject_cn"; throws std::invalid_argument for anything else.
ClientIdentitySource ParseClientIdentitySource(std::string_view name);
std::string_view ClientIdentitySourceName(ClientIdentitySource source);

struct TlsOptions {
  std::string certificate_chain_file;  // PEM
  std::string private_key_file;        // PEM
  // PEM CAs that client certificates are verified against (mutual TLS). A certificate a
  // client presents is always verified; one that does not verify fails the handshake.
  std::string client_ca_file;
  // With client_ca_file: a connection without a client certificate is refused at the
  // handshake (true), or may connect and sign in another way (false).
  bool require_client_certificate = true;
  // PEM certificate revocation lists (X509_V_FLAG_CRL_CHECK on the leaf). The handshake
  // refuses a certificate they revoke. Every CA that issues client certificates needs its
  // CRL here: a certificate whose issuer has no CRL, or an expired one, is refused too.
  // The file is checked for changes at most every crl_check_interval (on new connections
  // and requests) and reloaded; each open connection's certificate is then verified again
  // at its next request (certificate_revoked). A file that fails to load keeps the last
  // good one in force.
  std::string client_crl_file;
  std::chrono::seconds crl_check_interval{5};
  // With CRLs, every certificate of a client's chain is checked (X509_V_FLAG_CRL_CHECK_ALL),
  // so the file needs a CRL from every CA in it, the root included; a CA without one fails
  // the handshake. True accepts a CA above the leaf that has no CRL (its own certificate
  // is then not revocation-checked); the leaf's issuer always needs one. An open
  // connection is verified again whenever a loaded CRL's nextUpdate passes.
  bool allow_missing_ca_crls = false;
  ClientIdentitySource client_identity_source = ClientIdentitySource::kSanEmail;
  // A password or token_exchange sign-in on a connection with a client certificate must
  // be for the identity the certificate maps to (certificate_identity_mismatch).
  bool require_identity_match = false;
  // Told about each CRL (re)load: whether it worked and what happened, for the log.
  std::function<void(bool ok, const std::string& message)> on_crl_reload;
};

// A refusal that may be an attack (spec §4.4): a bound token on the wrong connection, a
// sign-in for another identity than the certificate's, a revoked certificate, an unknown
// identity, a client certificate the TLS handshake refused (code
// "client_certificate_refused"). Values are printable ASCII (anything else escaped as \xNN).
struct SecurityEvent {
  std::string code;                  // the problem code the client got
  std::string detail;
  std::string user;                  // the token's principal, or the identity a sign-in named
  std::string token_id;              // the token's id (Principal::claims "token_id"), if known
  std::string presented_thumbprint;  // the connection's client certificate (x5t#S256), or empty
  std::string certificate_subject;   // RFC 2253, or empty
  std::string peer;                  // the client's address:port
  std::string request_id;
  // Above 0: a summary, standing for this many refusals from the same source (client
  // address and certificate) that were not reported one by one. Each source gets its
  // first ten events a minute in full, then one summary a minute.
  int64_t suppressed = 0;
};

struct ServerOptions {
  std::string host = "127.0.0.1";
  uint16_t port = 0;  // 0: any free port; see Server::port()
  std::optional<TlsOptions> tls;
  // A second, plaintext listener that serves only the health check (spec §3.2), for
  // probes that can't present client certificates or speak TLS. 0: none.
  uint16_t health_port = 0;

  int http_threads = 0;    // 0: one per core
  int engine_threads = 0;  // 0: two per core

  std::size_t max_header_bytes = 16 * 1024;  // larger header blocks get 431
  // Bodies read whole: JSON, and Arrow parameters for prepared statements. Ingest
  // streams and has no limit.
  std::size_t max_json_body_bytes = 64 * 1024 * 1024;

  std::chrono::seconds token_lifetime{3600};
  std::chrono::seconds session_idle_timeout{900};
  std::chrono::seconds result_ttl{3600};
  // A finished statement is forgotten this long after it ends (and once its retained
  // result has expired), or sooner when its session holds more finished statements
  // than max_finished_statements_per_session; then it is 404 (spec §6.5).
  std::chrono::seconds finished_statement_ttl{600};
  // A partitioned ingest (spec §8.3) with no request and no part upload for this long
  // is aborted (ingest_expired).
  std::chrono::seconds ingest_idle_timeout{120};
  std::size_t max_finished_statements_per_session = 1000;
  std::chrono::hours idempotency_window{24};
  std::chrono::seconds default_wait{5};  // submit without Prefer: wait / respond-async
  std::chrono::seconds max_wait{60};     // cap on Prefer: wait
  std::chrono::seconds health_timeout{5};
  // Applies when a request sets no Warp-Statement-Timeout. 0: none.
  std::chrono::milliseconds default_statement_timeout{0};
  std::size_t retained_bytes_per_principal = std::size_t{1} << 30;
  // Partitioned results (spec §6.10): the most partitions one result is split into when
  // a client asks (Warp-Result-Partitions). 1: never split.
  std::size_t max_result_partitions = 32;

  std::vector<std::string> cors_allowed_origins;  // empty: no cross-origin access

  // Told about every SecurityEvent (log it, audit it). Called on an I/O thread: keep it short.
  std::function<void(const SecurityEvent&)> on_security_event;
};

// Call first in main(), before anything uses OpenSSL. It keeps OpenSSL from freeing its
// global state at exit: a thread that finishes after that (an engine or HTTP worker
// still winding down) would otherwise crash in its thread-local RNG cleanup.
void InitializeProcess();

class Server {
 public:
  // `tokens` may be null: the SDK then issues random in-memory tokens, valid for
  // `options.token_lifetime` on this process only.
  Server(ServerOptions options, std::shared_ptr<Backend> backend,
         std::shared_ptr<Authenticator> authenticator,
         std::shared_ptr<TokenService> tokens = nullptr);
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
