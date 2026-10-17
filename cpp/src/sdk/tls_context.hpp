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
#include "warpsql/backend.hpp"
#include "warpsql/server.hpp"

namespace warpsql::detail {

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
  std::string problem_;  // set once it stops verifying; it never verifies again
};

class TlsContext {
 public:
  // Loads every file; throws std::runtime_error naming the one that failed.
  explicit TlsContext(const TlsOptions& options);

  struct Current {
    std::shared_ptr<net::ssl::context> context;
    uint64_t generation;
  };
  // The context new connections use. Looks at the CRL file first, at most every
  // crl_check_interval, and reloads it when it changed.
  Current Get();

  // After a handshake under `generation`: the connection's client certificate, or null
  // when it presented none. Never describes a certificate that did not verify.
  std::shared_ptr<PeerCertificate> PeerOf(SSL* ssl, uint64_t generation);

  // Before each request: empty when the connection's certificate still stands, else why
  // it no longer does (revoked by a CRL loaded since, expired, ...). Once set, it stays.
  std::string Recheck(PeerCertificate& peer);

 private:
  std::shared_ptr<net::ssl::context> Build() const;
  void MaybeReload();

  const TlsOptions options_;
  std::mutex mutex_;  // guards current_ and the file's last-seen state
  Current current_;
  Clock::time_point last_check_;
  std::filesystem::file_time_type crl_mtime_{};
  std::uintmax_t crl_size_ = 0;
};

// The identity `source` names in `certificate` (spec §4.4): exactly one value, without
// control characters. Throws Error(kUnknownCertificateIdentity) otherwise.
std::string CertificateIdentity(const ClientCertificate& certificate, ClientIdentitySource source);

// ASCII case-insensitive comparison, as identities are compared for an identity match.
bool SameIdentityName(std::string_view a, std::string_view b);

}  // namespace warpsql::detail
