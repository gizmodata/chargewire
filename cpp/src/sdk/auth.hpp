// Access tokens the server issues (spec §4). Tokens are opaque random strings; the
// store keeps only their SHA-256.
#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common.hpp"
#include "chargewire/backend.hpp"

namespace chargewire::detail {

// The default TokenService: random tokens, kept (as SHA-256) in memory.
class TokenStore : public TokenService {
 public:
  explicit TokenStore(std::chrono::seconds lifetime) : lifetime_(lifetime) {}

  Issued Issue(const Principal& principal) override;
  Principal Validate(std::string_view token) override;
  // The whole Principal is kept, its certificate thumbprint included.
  bool BindsCertificates() const override { return true; }

  // Drops tokens that expired more than kExpiredKept ago. Until then an expired token is
  // refused with reason token_expired (spec §4.2): only its holder can present it.
  void Sweep();
  static constexpr std::chrono::hours kExpiredKept{1};

 private:
  struct Entry {
    Principal principal;
    Clock::time_point expires;
  };

  std::chrono::seconds lifetime_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Entry> by_hash_;
};

// The token from an "Authorization: Bearer <token>" header, or empty.
std::string_view BearerToken(std::string_view authorization);

}  // namespace chargewire::detail
