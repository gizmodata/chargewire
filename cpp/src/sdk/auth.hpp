// Access tokens the server issues (spec §4). Tokens are opaque random strings; the
// store keeps only their SHA-256.
#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common.hpp"
#include "warpsql/backend.hpp"

namespace warpsql::detail {

class TokenStore {
 public:
  explicit TokenStore(std::chrono::seconds lifetime) : lifetime_(lifetime) {}

  struct Issued {
    std::string token;
    std::chrono::seconds expires_in;
  };
  Issued Issue(const Principal& principal);

  // The principal a bearer token belongs to. Throws Error(kInvalidToken).
  Principal Validate(std::string_view token) const;

  // Drops expired tokens.
  void Sweep();

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

}  // namespace warpsql::detail
