// The server's TLS configuration and what it knows about each connection's client
// certificate (spec §2.1, §4.4): the OpenSSL context (rebuilt when the CRL file changes),
// the verified client certificate of a connection, and its re-check on every request.
#pragma once

#include <openssl/ssl.h>

#include <boost/asio/ssl/context.hpp>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>

#include "common.hpp"
#include "chargewire/backend.hpp"
#include "chargewire/server.hpp"

namespace chargewire::detail {

// Why a handshake refused a client certificate (filled in by the verify callback).
struct HandshakeRefusal {
  std::string reason;      // OpenSSL's verify error
  std::string thumbprint;  // the leaf's x5t#S256
  std::string subject;     // RFC 2253, escaped
};

// A connection's verified client certificate. Created after the handshake; lives as long
// as the connection, and is checked again before each of its requests.
class PeerCertificate {
 public:
  PeerCertificate(X509* leaf, STACK_OF(X509)* chain, uint64_t generation);  // takes both
  ~PeerCertificate();
  PeerCertificate(const PeerCertificate&) = delete;
  PeerCertificate& operator=(const PeerCertificate&) = delete;

  const std::shared_ptr<const ClientCertificate>& info() const { return info_; }

 private:
  friend class TlsContext;
  X509* leaf_;
  STACK_OF(X509)* chain_;  // the untrusted intermediates the client sent
  std::shared_ptr<const ClientCertificate> info_;
  // Only touched by the connection's own coroutine (one strand).
  uint64_t generation_;  // the context generation it was last verified under
  // When that verification stops holding: the earliest nextUpdate of the CRLs it used.
  std::chrono::system_clock::time_point verified_until_ = std::chrono::system_clock::time_point::max();
  std::string problem_;  // set once it stops verifying; it never verifies again
};

class TlsContext {
 public:
  // Loads every file; throws std::runtime_error naming the one that failed.
  explicit TlsContext(const TlsOptions& options);

  struct Current {
    std::shared_ptr<net::ssl::context> context;
    uint64_t generation = 0;
    // The earliest nextUpdate of its CRLs (max: none): after it, every certificate is
    // verified again, and fails closed once the CRL has expired.
    std::chrono::system_clock::time_point crl_next_update = std::chrono::system_clock::time_point::max();
    std::vector<std::string> notes;  // what loading the CRLs set aside, for the log
  };
  // The SSL ex_data slot of a HandshakeRefusal the handshake fills in when it refuses a
  // client certificate.
  static int RefusalIndex();
  // The context new connections use. Looks at the files first (the server's certificate
  // chain and key, the client CA, the CRL), at most every crl_check_interval, and rebuilds
  // the context when one of them changed: a renewed certificate is served without a
  // restart. Connections made before keep the context they were made with.
  Current Get();

  // After a handshake under `generation`: the connection's client certificate, or null
  // when it presented none. Never describes a certificate that did not verify.
  std::shared_ptr<PeerCertificate> PeerOf(SSL* ssl, uint64_t generation);

  // Before each request: empty when the connection's certificate still stands, else why
  // it no longer does (revoked by a CRL loaded since, expired, ...). Once set, it stays.
  std::string Recheck(PeerCertificate& peer);

 private:
  Current Build(uint64_t generation) const;
  void MaybeReload();

  const TlsOptions options_;
  std::mutex mutex_;  // guards current_ and the file's last-seen state
  Current current_;
  Clock::time_point last_check_;
  // Each watched file as last loaded: its modification time and size.
  struct Watched {
    std::string path;
    std::filesystem::file_time_type mtime{};
    std::uintmax_t size = 0;
  };
  std::vector<Watched> watched_;
};

// The identity `source` names in `certificate` (spec §4.4): exactly one value, without
// control characters. Throws Error(kUnknownCertificateIdentity) otherwise.
std::string CertificateIdentity(const ClientCertificate& certificate, ClientIdentitySource source);

// `text` with every byte outside printable ASCII written as \xNN, for error details and
// logs: a refused value can't spoof what a reader sees.
std::string EscapeForDisplay(std::string_view text);

// CertificateIdentitiesMatch (URI identities exactly, others ignoring ASCII case).
bool SameIdentityName(std::string_view a, std::string_view b);

}  // namespace chargewire::detail
