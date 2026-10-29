#include "auth.hpp"

#include "ids.hpp"

namespace chargewire {

// Defaults for the optional Authenticator methods: "not supported".
Principal Authenticator::AuthenticatePassword(std::string_view, std::string_view) {
  throw Error(ErrorCode::kInvalidCredentials, "password sign-in is not enabled on this server");
}

Principal Authenticator::ExchangeToken(std::string_view) {
  throw Error(ErrorCode::kInvalidCredentials, "token exchange is not enabled on this server");
}

Principal Authenticator::AuthenticateCertificate(std::string_view, const ClientCertificate&) {
  throw Error(ErrorCode::kUnknownCertificateIdentity,
              "certificate sign-in is not enabled on this server");
}

}  // namespace chargewire

namespace chargewire::detail {

TokenService::Issued TokenStore::Issue(const Principal& principal) {
  std::string token = NewSecret();
  std::lock_guard lock(mutex_);
  by_hash_[Sha256Hex(token)] = Entry{principal, Clock::now() + lifetime_};
  return {std::move(token), lifetime_};
}

Principal TokenStore::Validate(std::string_view token) {
  if (!token.empty()) {
    std::lock_guard lock(mutex_);
    auto it = by_hash_.find(Sha256Hex(token));
    if (it != by_hash_.end()) {
      if (it->second.expires > Clock::now()) return it->second.principal;
      // A token this server issued: its holder may know why it no longer works.
      throw Error::InvalidToken(reason::kTokenExpired, "the access token has expired; sign in again");
    }
  }
  throw Error(ErrorCode::kInvalidToken, "a valid access token is required");
}

void TokenStore::Sweep() {
  const auto forget = Clock::now() - kExpiredKept;
  std::lock_guard lock(mutex_);
  std::erase_if(by_hash_, [forget](const auto& item) { return item.second.expires <= forget; });
}

std::string_view BearerToken(std::string_view authorization) {
  constexpr std::string_view kScheme = "Bearer ";
  if (authorization.size() <= kScheme.size()) {
    return {};
  }
  // The scheme is case-insensitive (RFC 9110 §11.1).
  for (std::size_t i = 0; i < kScheme.size(); i++) {
    if (std::tolower(static_cast<unsigned char>(authorization[i])) !=
        std::tolower(static_cast<unsigned char>(kScheme[i]))) {
      return {};
    }
  }
  return authorization.substr(kScheme.size());
}

}  // namespace chargewire::detail
