// Identifiers and secrets: random, URL-safe, unguessable (spec §2.3).
#pragma once

#include <string>
#include <string_view>

namespace chargewire::detail {

// "<prefix>_" followed by 160 random bits in base32 (lower case).
std::string NewId(std::string_view prefix);

// 256 random bits, base32, for access tokens.
std::string NewSecret();

// SHA-256 of `data`, hex encoded.
std::string Sha256Hex(std::string_view data);

// Compares in time independent of where the inputs differ.
bool ConstantTimeEquals(std::string_view a, std::string_view b);

// Standard base64 with padding.
std::string Base64(std::string_view data);

}  // namespace chargewire::detail
