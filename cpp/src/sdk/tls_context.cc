#include "tls_context.hpp"

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <cctype>
#include <array>
#include <map>
#include <utility>
#include <format>
#include <stdexcept>
#include <system_error>

#include "ids.hpp"

namespace warpsql {

ClientIdentitySource ParseClientIdentitySource(std::string_view name) {
  if (name == "san_email") return ClientIdentitySource::kSanEmail;
  if (name == "san_uri") return ClientIdentitySource::kSanUri;
  if (name == "subject_cn") return ClientIdentitySource::kSubjectCn;
  throw std::invalid_argument(
      std::format("unknown client identity source '{}': use san_email, san_uri or subject_cn", name));
}

bool IsUriIdentity(std::string_view identity) {
  const auto colon = identity.find("://");
  if (colon == std::string_view::npos || colon == 0 ||
      !std::isalpha(static_cast<unsigned char>(identity[0]))) {
    return false;
  }
  for (std::size_t i = 1; i < colon; i++) {
    const unsigned char c = static_cast<unsigned char>(identity[i]);
    if (!std::isalnum(c) && c != '+' && c != '-' && c != '.') return false;
  }
  return true;
}

bool CertificateIdentitiesMatch(std::string_view a, std::string_view b) {
  if (IsUriIdentity(a) || IsUriIdentity(b)) return a == b;
  if (a.size() != b.size()) return false;
  const auto lower = [](unsigned char c) { return c >= 'A' && c <= 'Z' ? c - 'A' + 'a' : c; };
  for (std::size_t i = 0; i < a.size(); i++) {
    if (lower(static_cast<unsigned char>(a[i])) != lower(static_cast<unsigned char>(b[i]))) return false;
  }
  return true;
}

std::string_view ClientIdentitySourceName(ClientIdentitySource source) {
  switch (source) {
    case ClientIdentitySource::kSanEmail: return "san_email";
    case ClientIdentitySource::kSanUri: return "san_uri";
    case ClientIdentitySource::kSubjectCn: return "subject_cn";
  }
  return "san_email";
}

}  // namespace warpsql

namespace warpsql::detail {

namespace ssl = net::ssl;

namespace {

// The newest OpenSSL error, for a message.
std::string OpenSslError() {
  const unsigned long code = ERR_get_error();
  ERR_clear_error();
  if (code == 0) return "unknown OpenSSL error";
  std::array<char, 256> text{};
  ERR_error_string_n(code, text.data(), text.size());
  return text.data();
}

std::string NameText(const X509_NAME* name) {
  BIO* bio = BIO_new(BIO_s_mem());
  if (bio == nullptr) return {};
  X509_NAME_print_ex(bio, name, 0, XN_FLAG_RFC2253);
  char* data = nullptr;
  const long size = BIO_get_mem_data(bio, &data);
  std::string out(data, static_cast<std::size_t>(size > 0 ? size : 0));
  BIO_free(bio);
  return out;
}

std::string Base64Url(std::string_view bytes) {
  std::string out = Base64(bytes);
  while (!out.empty() && out.back() == '=') out.pop_back();
  for (char& c : out) {
    if (c == '+') c = '-';
    else if (c == '/') c = '_';
  }
  return out;
}

std::string AsnText(const ASN1_STRING* value) {
  return std::string(reinterpret_cast<const char*>(ASN1_STRING_get0_data(value)),
                     static_cast<std::size_t>(ASN1_STRING_length(value)));
}

ClientCertificate Describe(X509* leaf) {
  ClientCertificate out;
  unsigned char* der = nullptr;
  const int der_length = i2d_X509(leaf, &der);
  if (der_length <= 0) throw std::runtime_error("cannot encode the client certificate");
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_length = 0;
  const int hashed = EVP_Digest(der, static_cast<std::size_t>(der_length), digest.data(),
                                &digest_length, EVP_sha256(), nullptr);
  OPENSSL_free(der);
  if (hashed != 1) throw std::runtime_error("SHA-256 failed");
  out.thumbprint = Base64Url(std::string_view(reinterpret_cast<const char*>(digest.data()), digest_length));
  out.subject = NameText(X509_get_subject_name(leaf));
  out.issuer = NameText(X509_get_issuer_name(leaf));
  if (BIGNUM* serial = ASN1_INTEGER_to_BN(X509_get0_serialNumber(leaf), nullptr)) {
    if (char* hex = BN_bn2hex(serial)) {
      out.serial = hex;
      OPENSSL_free(hex);
    }
    BN_free(serial);
  }

  int critical = 0;
  auto* names = static_cast<GENERAL_NAMES*>(
      X509_get_ext_d2i(leaf, NID_subject_alt_name, &critical, nullptr));
  // -2: the extension occurs more than once, which RFC 5280 forbids; trust none of it.
  out.duplicate_san_extension = critical == -2;
  if (names != nullptr) {
    for (int i = 0; i < sk_GENERAL_NAME_num(names); i++) {
      const GENERAL_NAME* name = sk_GENERAL_NAME_value(names, i);
      if (name->type == GEN_EMAIL) out.san_emails.push_back(AsnText(name->d.rfc822Name));
      if (name->type == GEN_URI) out.san_uris.push_back(AsnText(name->d.uniformResourceIdentifier));
    }
    GENERAL_NAMES_free(names);
  }

  const X509_NAME* subject = X509_get_subject_name(leaf);
  for (int at = X509_NAME_get_index_by_NID(subject, NID_commonName, -1); at >= 0;
       at = X509_NAME_get_index_by_NID(subject, NID_commonName, at)) {
    const ASN1_STRING* value = X509_NAME_ENTRY_get_data(X509_NAME_get_entry(subject, at));
    unsigned char* utf8 = nullptr;
    const int length = ASN1_STRING_to_UTF8(&utf8, value);
    if (length < 0) {
      out.common_names.push_back(std::string(1, '\0'));  // undecodable: refused when mapped
      continue;
    }
    out.common_names.emplace_back(reinterpret_cast<const char*>(utf8), static_cast<std::size_t>(length));
    OPENSSL_free(utf8);
  }
  return out;
}

// Reads every PEM item of one kind from a file.
template <class T, class Read, class Free>
std::vector<T*> ReadPemAll(const std::string& path, const char* what, Read read, Free free_one) {
  BIO* bio = BIO_new_file(path.c_str(), "r");
  if (bio == nullptr) {
    throw std::runtime_error(std::format("cannot read the {} file {}: {}", what, path, OpenSslError()));
  }
  std::vector<T*> items;
  while (T* item = read(bio, nullptr, nullptr, nullptr)) items.push_back(item);
  // The loop ends at the end of the file (PEM_R_NO_START_LINE) or at a broken entry.
  const unsigned long last = ERR_peek_last_error();
  const bool clean_end = ERR_GET_LIB(last) == ERR_LIB_PEM && ERR_GET_REASON(last) == PEM_R_NO_START_LINE;
  ERR_clear_error();
  BIO_free(bio);
  if (!clean_end && last != 0) {
    for (T* item : items) free_one(item);
    throw std::runtime_error(std::format("the {} file {} has an entry that cannot be read", what, path));
  }
  return items;
}

std::string NameDer(const X509_NAME* name) {
  unsigned char* der = nullptr;
  const int length = i2d_X509_NAME(name, &der);
  std::string out(reinterpret_cast<const char*>(der), static_cast<std::size_t>(std::max(length, 0)));
  OPENSSL_free(der);
  return out;
}

// A CRL extension holding a number (CRL Number, Delta CRL Indicator), freed with it.
struct CrlNumber {
  ASN1_INTEGER* value = nullptr;
  ~CrlNumber() { ASN1_INTEGER_free(value); }
};

// Which of two CRLs from one issuer is the newer: the higher CRL Number when both have
// one, else the later thisUpdate.
bool Newer(X509_CRL* a, X509_CRL* b) {
  CrlNumber na{static_cast<ASN1_INTEGER*>(X509_CRL_get_ext_d2i(a, NID_crl_number, nullptr, nullptr))};
  CrlNumber nb{static_cast<ASN1_INTEGER*>(X509_CRL_get_ext_d2i(b, NID_crl_number, nullptr, nullptr))};
  if (na.value != nullptr && nb.value != nullptr) return ASN1_INTEGER_cmp(na.value, nb.value) > 0;
  return ASN1_TIME_compare(X509_CRL_get0_lastUpdate(a), X509_CRL_get0_lastUpdate(b)) > 0;
}

std::string NumberText(X509_CRL* crl, int nid) {
  CrlNumber n{static_cast<ASN1_INTEGER*>(X509_CRL_get_ext_d2i(crl, nid, nullptr, nullptr))};
  if (n.value == nullptr) return "(no number)";
  return "#" + std::to_string(ASN1_INTEGER_get(n.value));
}

// Loads the CRL file into `store` (spec §4.4). For each issuer it keeps only the newest
// CRL (OpenSSL would use whichever it met first). A CRL whose issuer is one of the client
// CAs must verify with that CA's key. Delta CRLs are refused: OpenSSL applies one only when
// the certificate or the base CRL has a Freshest CRL extension, and ignores it silently
// otherwise, so a revocation in a delta could go unseen. A refused file fails the load (at
// startup the server doesn't start; on a reload the last good CRLs stay in force). Returns
// the earliest nextUpdate kept, and what was set aside, for the log.
TlsContext::Current LoadCrls(X509_STORE* store, const std::string& path, const std::string& ca_path) {
  auto crls = ReadPemAll<X509_CRL>(path, "CRL", PEM_read_bio_X509_CRL, X509_CRL_free);
  std::shared_ptr<void> free_crls(nullptr, [&crls](void*) { for (auto* c : crls) X509_CRL_free(c); });
  if (crls.empty()) {
    throw std::runtime_error(std::format("the CRL file {} holds no certificate revocation list", path));
  }
  auto cas = ReadPemAll<X509>(ca_path, "client CA", PEM_read_bio_X509, X509_free);
  std::shared_ptr<void> free_cas(nullptr, [&cas](void*) { for (auto* c : cas) X509_free(c); });

  TlsContext::Current out;
  struct Kept {
    X509_CRL* base = nullptr;
    std::string issuer;
  };
  std::map<std::string, Kept> by_issuer;
  for (X509_CRL* crl : crls) {
    const X509_NAME* issuer = X509_CRL_get_issuer(crl);
    const std::string issuer_text = EscapeForDisplay(NameText(issuer));
    // Signed by the CA it names, when that CA is one of ours (an intermediate's CRL is
    // checked against the intermediate at the handshake).
    bool known = false, verified = false;
    for (X509* ca : cas) {
      if (X509_NAME_cmp(X509_get_subject_name(ca), issuer) != 0) continue;
      known = true;
      if (EVP_PKEY* key = X509_get0_pubkey(ca); key != nullptr && X509_CRL_verify(crl, key) == 1) verified = true;
    }
    ERR_clear_error();
    if (known && !verified) {
      throw std::runtime_error(std::format("a CRL in {} names {} as its issuer but is not signed by that CA's key",
                                           path, issuer_text));
    }
    if (CrlNumber delta{static_cast<ASN1_INTEGER*>(X509_CRL_get_ext_d2i(crl, NID_delta_crl, nullptr, nullptr))};
        delta.value != nullptr) {
      throw std::runtime_error(std::format("{} holds a delta CRL from {}: delta CRLs are not supported; "
                                           "use a full CRL", path, issuer_text));
    }
    auto& kept = by_issuer[NameDer(issuer)];
    kept.issuer = issuer_text;
    if (kept.base == nullptr) {
      kept.base = crl;
    } else {
      X509_CRL* older = Newer(crl, kept.base) ? std::exchange(kept.base, crl) : crl;
      out.notes.push_back(std::format("CRL {} from {} is superseded by {} and not used",
                                      NumberText(older, NID_crl_number), issuer_text,
                                      NumberText(kept.base, NID_crl_number)));
    }
  }
  for (auto& [key, kept] : by_issuer) {
    X509_CRL* crl = kept.base;
    if (const ASN1_TIME* next = X509_CRL_get0_nextUpdate(crl)) {
      int days = 0, seconds = 0;  // from now (portable, unlike timegm)
      if (ASN1_TIME_diff(&days, &seconds, nullptr, next) == 1) {
        out.crl_next_update = std::min(out.crl_next_update, std::chrono::system_clock::now() +
                                                                 std::chrono::hours(24) * days +
                                                                 std::chrono::seconds(seconds));
      }
    }
    if (X509_STORE_add_crl(store, crl) != 1) {
      throw std::runtime_error(std::format("cannot use a CRL in {}: {}", path, OpenSslError()));
    }
  }
  return out;
}

// Identities are printable ASCII only (spec §4.4): no control, format, invisible or
// non-ASCII characters, from any source.
bool PrintableAscii(std::string_view text) {
  return std::ranges::all_of(text, [](char c) { return c >= 0x20 && c <= 0x7e; });
}

// The handshake's verification callback: records why a certificate was refused (for the
// audit), and with allow_missing_ca_crls lets a CA above the leaf go without a CRL.
template <bool kAllowMissingCaCrls>
int VerifyCallback(int ok, X509_STORE_CTX* context) {
  if (ok) return ok;
  const int error = X509_STORE_CTX_get_error(context);
  if (kAllowMissingCaCrls && error == X509_V_ERR_UNABLE_TO_GET_CRL && X509_STORE_CTX_get_error_depth(context) > 0) {
    X509_STORE_CTX_set_error(context, X509_V_OK);
    return 1;
  }
  auto* ssl = static_cast<SSL*>(X509_STORE_CTX_get_ex_data(context, SSL_get_ex_data_X509_STORE_CTX_idx()));
  auto* refusal = ssl ? static_cast<HandshakeRefusal*>(SSL_get_ex_data(ssl, TlsContext::RefusalIndex())) : nullptr;
  if (refusal != nullptr && refusal->reason.empty()) {
    refusal->reason = X509_verify_cert_error_string(error);
    if (X509* leaf = X509_STORE_CTX_get0_cert(context)) {
      try {
        const ClientCertificate described = Describe(leaf);
        refusal->thumbprint = described.thumbprint;
        refusal->subject = EscapeForDisplay(described.subject);
      } catch (const std::exception&) {
      }
    }
  }
  return ok;
}

}  // namespace

PeerCertificate::PeerCertificate(X509* leaf, STACK_OF(X509)* chain, uint64_t generation)
    : leaf_(leaf), chain_(chain), generation_(generation) {
  try {
    info_ = std::make_shared<const ClientCertificate>(Describe(leaf_));
  } catch (...) {
    X509_free(leaf_);
    sk_X509_pop_free(chain_, X509_free);
    throw;
  }
}

PeerCertificate::~PeerCertificate() {
  X509_free(leaf_);
  sk_X509_pop_free(chain_, X509_free);
}

int TlsContext::RefusalIndex() {
  static const int index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
  return index;
}

TlsContext::TlsContext(const TlsOptions& options) : options_(options) {
  current_ = Build(1);
  last_check_ = Clock::now();
  if (!options_.client_crl_file.empty()) {
    std::error_code ec;
    crl_mtime_ = std::filesystem::last_write_time(options_.client_crl_file, ec);
    crl_size_ = std::filesystem::file_size(options_.client_crl_file, ec);
    if (options_.on_crl_reload) {
      options_.on_crl_reload(true, "loaded the client CRL file " + options_.client_crl_file);
      for (const auto& note : current_.notes) options_.on_crl_reload(true, note);
    }
  }
}

TlsContext::Current TlsContext::Build(uint64_t generation) const {
  Current built;
  built.generation = generation;
  auto context = std::make_shared<ssl::context>(ssl::context::tls_server);
  context->set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 |
                       ssl::context::no_sslv3 | ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1);
  SSL_CTX* native = context->native_handle();
  // A connection's certificate is fixed at its handshake: no TLS 1.2 renegotiation, which
  // could swap it (spec §9.6).
  SSL_CTX_set_options(native, SSL_OP_NO_RENEGOTIATION);
  context->use_certificate_chain_file(options_.certificate_chain_file);
  context->use_private_key_file(options_.private_key_file, ssl::context::pem);
  built.context = context;
  if (options_.client_ca_file.empty()) {
    return built;
  }
  context->load_verify_file(options_.client_ca_file);
  // Tell clients which CAs to pick a certificate from.
  if (STACK_OF(X509_NAME)* names = SSL_load_client_CA_file(options_.client_ca_file.c_str())) {
    SSL_CTX_set_client_CA_list(native, names);
  }
  context->set_verify_mode(options_.require_client_certificate
                               ? ssl::verify_peer | ssl::verify_fail_if_no_peer_cert
                               : ssl::verify_peer);
  // Resumed sessions keep their verified certificate; OpenSSL needs a session id context
  // for that with client verification on. Each context (each CRL generation) has its own
  // ticket keys and session cache, so a session from before a CRL reload is not resumed.
  static constexpr unsigned char kSessionContext[] = "warpsql";
  SSL_CTX_set_session_id_context(native, kSessionContext, sizeof(kSessionContext) - 1);
  if (!options_.client_crl_file.empty()) {
    X509_STORE* store = SSL_CTX_get_cert_store(native);
    Current loaded = LoadCrls(store, options_.client_crl_file, options_.client_ca_file);
    built.crl_next_update = loaded.crl_next_update;
    built.notes = std::move(loaded.notes);
    // Every certificate of the chain, not only the leaf: a revoked intermediate CA revokes
    // what it issued.
    X509_STORE_set_flags(store, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
  }
  X509_STORE_set_verify_cb(SSL_CTX_get_cert_store(native),
                           options_.allow_missing_ca_crls ? VerifyCallback<true> : VerifyCallback<false>);
  return built;
}

void TlsContext::MaybeReload() {
  if (options_.client_crl_file.empty()) return;
  std::unique_lock lock(mutex_, std::try_to_lock);
  if (!lock.owns_lock()) return;  // another thread is looking
  const auto now = Clock::now();
  if (now - last_check_ < options_.crl_check_interval) return;
  last_check_ = now;
  std::error_code ec;
  const auto mtime = std::filesystem::last_write_time(options_.client_crl_file, ec);
  const auto size = ec ? 0 : std::filesystem::file_size(options_.client_crl_file, ec);
  if (ec) {
    if (options_.on_crl_reload) {
      options_.on_crl_reload(false, "cannot read the client CRL file " + options_.client_crl_file +
                                        " (" + ec.message() + "); the last one loaded stays in force");
    }
    return;
  }
  if (mtime == crl_mtime_ && size == crl_size_) return;
  try {
    current_ = Build(current_.generation + 1);
    // Only now: a reload that failed (the CRL, or the server's key or CA mid-rotation) is
    // tried again at the next look, not left until the CRL file changes again.
    crl_mtime_ = mtime;
    crl_size_ = size;
    if (options_.on_crl_reload) {
      options_.on_crl_reload(true, "reloaded the client CRL file " + options_.client_crl_file);
      for (const auto& note : current_.notes) options_.on_crl_reload(true, note);
    }
  } catch (const std::exception& e) {
    if (options_.on_crl_reload) {
      options_.on_crl_reload(false, "reloading the client CRL file " + options_.client_crl_file + " failed (" +
                                        e.what() + "); the last one loaded stays in force, and the reload "
                                        "is tried again");
    }
  }
}

TlsContext::Current TlsContext::Get() {
  MaybeReload();
  std::lock_guard lock(mutex_);
  return current_;
}

std::shared_ptr<PeerCertificate> TlsContext::PeerOf(SSL* ssl, uint64_t generation) {
  X509* leaf = SSL_get1_peer_certificate(ssl);
  if (leaf == nullptr) return nullptr;
  // With verify_peer a certificate that fails verification fails the handshake; this is
  // the belt to that pair of braces.
  if (options_.client_ca_file.empty() || SSL_get_verify_result(ssl) != X509_V_OK) {
    X509_free(leaf);
    return nullptr;
  }
  STACK_OF(X509)* sent = SSL_get_peer_cert_chain(ssl);
  STACK_OF(X509)* chain = sent ? X509_chain_up_ref(sent) : sk_X509_new_null();
  auto peer = std::make_shared<PeerCertificate>(leaf, chain, generation);
  {
    std::lock_guard lock(mutex_);
    if (current_.generation == generation) peer->verified_until_ = current_.crl_next_update;
    else peer->verified_until_ = std::chrono::system_clock::time_point::min();  // check at once
  }
  return peer;
}

std::string TlsContext::Recheck(PeerCertificate& peer) {
  if (!peer.problem_.empty()) return peer.problem_;
  if (X509_cmp_current_time(X509_get0_notAfter(peer.leaf_)) < 0) {
    peer.problem_ = "the client certificate has expired";
    return peer.problem_;
  }
  const Current current = Get();
  // Verified again when the CRLs changed, or when one it was verified with passed its
  // nextUpdate (then it fails closed: an expired CRL verifies nothing).
  if (current.generation == peer.generation_ && std::chrono::system_clock::now() < peer.verified_until_) {
    return {};
  }
  // The CRLs changed since this connection was verified: verify it again under them.
  X509_STORE* store = SSL_CTX_get_cert_store(current.context->native_handle());
  X509_STORE_CTX* check = X509_STORE_CTX_new();
  if (check == nullptr || X509_STORE_CTX_init(check, store, peer.leaf_, peer.chain_) != 1) {
    X509_STORE_CTX_free(check);
    peer.problem_ = "the client certificate could not be checked again";
    return peer.problem_;
  }
  X509_STORE_CTX_set_default(check, "ssl_client");  // as the handshake verifies a client
  const bool ok = X509_verify_cert(check) == 1;
  const int error = X509_STORE_CTX_get_error(check);
  X509_STORE_CTX_free(check);
  ERR_clear_error();
  if (!ok) {
    peer.problem_ = error == X509_V_ERR_CERT_REVOKED
                        ? std::string("the client certificate has been revoked")
                        : std::string("the client certificate no longer verifies: ") +
                              X509_verify_cert_error_string(error);
    return peer.problem_;
  }
  peer.generation_ = current.generation;
  peer.verified_until_ = current.crl_next_update;
  return {};
}

std::string CertificateIdentity(const ClientCertificate& certificate, ClientIdentitySource source) {
  const std::vector<std::string>* values = nullptr;
  std::string_view kind;
  switch (source) {
    case ClientIdentitySource::kSanEmail:
      values = &certificate.san_emails;
      kind = "e-mail subject alternative name";
      break;
    case ClientIdentitySource::kSanUri:
      values = &certificate.san_uris;
      kind = "URI subject alternative name";
      break;
    case ClientIdentitySource::kSubjectCn:
      values = &certificate.common_names;
      kind = "subject common name";
      break;
  }
  auto refuse = [](std::string detail) { return Error(ErrorCode::kUnknownCertificateIdentity, std::move(detail)); };
  if (source != ClientIdentitySource::kSubjectCn && certificate.duplicate_san_extension) {
    throw refuse("the client certificate has more than one subject alternative name extension");
  }
  if (values->empty()) {
    throw refuse(std::format("the client certificate has no {}, which this server signs in by", kind));
  }
  if (values->size() > 1) {
    throw refuse(std::format("the client certificate has {} values of its {}; it must have exactly one",
                             values->size(), kind));
  }
  const std::string& value = values->front();
  if (value.empty() || !PrintableAscii(value)) {
    throw refuse(std::format("the client certificate's {} '{}' is empty or has characters other than "
                             "printable ASCII (control characters included)",
                             kind, EscapeForDisplay(value)));
  }
  return value;
}

std::string EscapeForDisplay(std::string_view text) {
  std::string out;
  for (char c : text) {
    if (c >= 0x20 && c <= 0x7e && c != '\\') {
      out += c;
    } else {
      out += std::format("\\x{:02x}", static_cast<unsigned char>(c));
    }
  }
  return out;
}

bool SameIdentityName(std::string_view a, std::string_view b) { return CertificateIdentitiesMatch(a, b); }

}  // namespace warpsql::detail
