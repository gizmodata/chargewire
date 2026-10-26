// A Warp SQL server: the protocol over HTTP/1.1 (and TLS), in front of a Backend.
#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
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

// A block of IP addresses, for ServerOptions::trusted_proxies.
struct IpNetwork {
  bool v6 = false;
  std::array<uint8_t, 16> bytes{};  // the network address (IPv4: the first 4 bytes)
  int prefix_length = 0;            // 0-32 (IPv4) or 0-128 (IPv6)

  // "10.0.0.0/8", "2001:db8::/32".
  std::string ToString() const;
  // Whether `address` (an IP address in text, IPv4 or IPv6, no port) lies in the block.
  // An IPv4-mapped IPv6 address (::ffff:10.1.2.3) counts as its IPv4 address.
  bool Contains(std::string_view address) const;
};
// "10.0.0.0/8", "2001:db8::/32", or a bare address ("192.0.2.7", "::1"): that one address
// (/32 or /128). An IPv4-mapped IPv6 block (::ffff:10.0.0.0/104) is the IPv4 one. Throws
// std::invalid_argument naming `text` and what is wrong with it: not an address, a prefix
// length out of range, or host bits set below the prefix ("10.1.2.3/8").
IpNetwork ParseIpNetwork(std::string_view text);

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
  // The client's address:port ("[v6]:port" for IPv6), or the address without a port that a
  // trusted proxy forwarded (ServerOptions::trusted_proxies).
  std::string peer;
  // The connection's own address:port when `peer` came from a trusted proxy's forwarding
  // header; else empty.
  std::string proxy;
  std::string request_id;
  // Above 0: a summary, standing for this many refusals from the same source (client
  // address and certificate) that were not reported one by one. Each source gets its
  // first ten events a minute in full, then one summary a minute.
  int64_t suppressed = 0;
};

// A GET or HEAD request outside the protocol's paths (anything not under /warp), which an
// application may answer before the server's 404: a web UI's files, say.
struct PlainRequest {
  std::string method;  // "GET" or "HEAD"
  std::string path;    // percent-decoded, without the query
  // The request's headers as received (Accept-Encoding, If-None-Match, ...).
  std::vector<std::pair<std::string, std::string>> headers;

  // The first value of header `name` (ASCII case-insensitive), or empty.
  std::string_view Header(std::string_view name) const;
};

struct PlainResponse {
  int status = 200;
  std::string content_type;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;  // for HEAD, the body a GET would get (only its length is sent)
};

// Returns nullopt to leave the request to the server (404). Runs on an I/O thread for
// every such request: it must be quick and must not block.
using PlainHandler = std::function<std::optional<PlainResponse>(const PlainRequest&)>;

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
  // Each ingest IPC message's wire bytes and aggregate expanded buffers. Applies
  // to ordinary/partitioned ingest and Arrow prepared-statement parameters.
  std::size_t max_ingest_message_bytes = 64 * 1024 * 1024;

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
  // Shared by every principal/session. 0 refuses nonempty retained results.
  // Counts retained Arrow buffers, not the engine or in-flight encoding buffers.
  std::size_t retained_bytes_total = std::size_t{1} << 30;
  // Partitioned results (spec §6.10): the most partitions one result is split into when
  // a client asks (Warp-Result-Partitions). 1: never split.
  std::size_t max_result_partitions = 32;

  std::vector<std::string> cors_allowed_origins;  // empty: no cross-origin access

  // Reverse proxies (load balancers, ingresses) whose forwarding headers are believed.
  // When a request's TCP peer lies in one of these blocks, the client's address is taken
  // from X-Forwarded-For, or, when a request has none, from Forwarded (RFC 7239 for=):
  // the hops are read right to left, skipping each that is itself a trusted proxy, and
  // the first that isn't is the client (all trusted: the leftmost). An unknown or
  // obfuscated hop, or one that isn't an IP address, ends the search and the TCP peer
  // stands. A forwarded address has no port ("203.0.113.9", "[2001:db8::7]"). Every
  // instance of a header counts, in order. X-Forwarded-For wins because the common
  // proxies append to it and pass a client's own Forwarded header on unchanged; behind a
  // proxy that sets only Forwarded, have it drop X-Forwarded-For.
  //
  // The address only describes the client: Request peers, SecurityEvent::peer (and
  // proxy), SessionActivity::client_address (and proxy_address) and the per-address bound
  // on browser sign-ins. Nothing about authentication or TLS changes: client
  // certificates and certificate-bound tokens stay tied to the connection. Empty: no
  // proxy is trusted and the TCP peer is always the client (a proxy's headers are
  // ignored). Trust only proxies that overwrite or append to these headers: one that
  // passes a client's header on lets that client choose its address.
  std::vector<IpNetwork> trusted_proxies;

  // Browser sign-in (spec §4.3, capability oauth_browser); null: not offered.
  std::shared_ptr<BrowserSignInProvider> browser_sign_in;
  // A flow is forgotten this long after it was created, finished or not.
  std::chrono::seconds browser_sign_in_lifetime{600};
  // Flows held at once (pending, or finished and not yet collected): in all, and per
  // client address (0: no per-address bound). Beyond either, rate_limited. Behind a proxy
  // that hides client addresses, every client shares one address, unless the proxy is in
  // trusted_proxies.
  std::size_t max_browser_sign_ins = 10000;
  std::size_t max_browser_sign_ins_per_address = 100;
  std::chrono::seconds browser_sign_in_poll_interval{2};

  // Told about every SecurityEvent (log it, audit it). Called on an I/O thread: keep it short.
  std::function<void(const SecurityEvent&)> on_security_event;

  // Answers GET and HEAD requests outside /warp on the main listener (not the health
  // port). Unset: they get 404, as any unknown path.
  PlainHandler plain_handler;
};

// One session as the server sees it, for an engine's own view of its sessions (a sessions
// table, say): Server::Sessions().
struct SessionActivity {
  std::string session_id;
  std::string username;  // the owner (Principal::name)
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point last_request_at;
  // The latest request's connection.
  std::string client_address;  // address:port, or a forwarded address (ServerOptions::trusted_proxies)
  // The connection's own address:port when client_address came from a trusted proxy's
  // forwarding header; else empty.
  std::string proxy_address;
  std::string user_agent;      // its User-Agent header, or empty
  bool tls = false;
  std::string tls_version;     // e.g. "TLSv1.3"; empty without TLS
  std::string client_certificate_thumbprint;  // its verified client certificate (x5t#S256), or empty
  // The identity that certificate maps to (TlsOptions::client_identity_source), or empty.
  std::string certificate_identity;
  // The session's token is bound to a client certificate (spec §4.4).
  bool token_certificate_bound = false;
  // Server::EndSession() ended it: it is unwinding and takes no requests.
  bool ended = false;

  // The statement the session is working on: the one running on the engine, else the
  // oldest queued one, else one whose result is being read. Empty id: none.
  std::string statement_id;
  std::string statement_state;  // "queued", "running", "succeeded", ...
  bool statement_has_rows = false;  // its outcome is rows (not an update count)
  int64_t statement_batches = 0;    // result batches it has produced so far
  std::chrono::system_clock::time_point statement_submitted_at;
  std::optional<std::chrono::system_clock::time_point> statement_started_at;
  // While a cancelled statement unwinds: why (spec §6.2's cancel_reason); else empty.
  std::string cancel_reason;
  // That statement's result partitions, once decided; 0: none (no rows, or undecided).
  int result_partitions = 0;

  int open_result_streams = 0;  // result reads under way, over every statement
  int open_ingest_parts = 0;    // bulk-ingest bodies being uploaded (spec §8.1, §8.3)
  int retained_results = 0;     // statements whose retained result is kept (spec §6.5)
  int64_t retained_rows = 0;    // the rows they hold
};

// Why Server::CancelSession() cancels a session's work: the statement document's
// cancel_reason (spec §6.2).
enum class SessionCancelReason {
  kCancelled,  // "cancelled": the engine cancelled it for someone (SQL hardcharger_cancel(), ...)
  kKilled,     // "killed": an administrator ended the session (SQL KILL SESSION, ...)
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

  // ---- For the engine, about its own sessions (safe from any thread, an engine thread
  // included, once Start() has run) ----

  // Every session, the ones EndSession() is ending included.
  std::vector<SessionActivity> Sessions() const;
  // Stops the work of session `session_id`: every statement queued or running (each ends
  // cancelled, with `reason` as its cancel_reason, and its result readers get an
  // incomplete stream, spec §6.6), every partitioned ingest still open (aborted, nothing
  // written) and every bulk-ingest body still arriving (its load rolls back). The session
  // itself goes on, ready for its next statement. Returns how many statements and ingests
  // it stopped (0: it was idle), or nullopt when there is no such session. Must not be
  // called for the session whose engine call is calling it (its own statement would be
  // cancelled under it).
  std::optional<std::size_t> CancelSession(const std::string& session_id, SessionCancelReason reason);
  // Ends session `session_id`: from now on every request naming it, by its owner, gets
  // `why` (Error::SessionEnded, spec §5.6), and nothing new starts in it. Once the work it
  // is running has stopped (cancel it first: this waits for it), its retained results are
  // freed and its engine session is destroyed, on its engine strand. A DELETE of the
  // session is then answered 204. Returns false when there is no such session (or it is
  // already ending).
  bool EndSession(const std::string& session_id, const Error& why);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace warpsql
