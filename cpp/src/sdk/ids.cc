#include "ids.hpp"

#include "chargewire/backend.hpp"

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <array>
#include <stdexcept>

namespace chargewire::detail {

namespace {

constexpr std::string_view kBase32 = "abcdefghijklmnopqrstuvwxyz234567";

std::string RandomBase32(std::size_t bytes) {
  std::string raw(bytes, '\0');
  if (RAND_bytes(reinterpret_cast<unsigned char*>(raw.data()), static_cast<int>(bytes)) != 1) {
    throw std::runtime_error("no randomness available from OpenSSL");
  }
  std::string out;
  unsigned buffer = 0;
  int bits = 0;
  for (unsigned char byte : raw) {
    buffer = (buffer << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      out.push_back(kBase32[(buffer >> (bits - 5)) & 31]);
      bits -= 5;
    }
  }
  if (bits > 0) {
    out.push_back(kBase32[(buffer << (5 - bits)) & 31]);
  }
  return out;
}

}  // namespace

std::string NewId(std::string_view prefix) {
  return std::string(prefix) + "_" + RandomBase32(20);
}

std::string NewSecret() { return RandomBase32(32); }

std::string Sha256Hex(std::string_view data) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int length = 0;
  if (EVP_Digest(data.data(), data.size(), digest.data(), &length, EVP_sha256(), nullptr) != 1) {
    throw std::runtime_error("SHA-256 failed");
  }
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  for (unsigned int i = 0; i < length; i++) {
    out.push_back(kHex[digest[i] >> 4]);
    out.push_back(kHex[digest[i] & 15]);
  }
  return out;
}

bool ConstantTimeEquals(std::string_view a, std::string_view b) {
  // Hash both so the comparison length never depends on the secret's length.
  const std::string ha = Sha256Hex(a);
  const std::string hb = Sha256Hex(b);
  return CRYPTO_memcmp(ha.data(), hb.data(), ha.size()) == 0;
}

std::string Base64(std::string_view data) {
  std::string out(4 * ((data.size() + 2) / 3), '\0');
  const int written = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()),
                                      reinterpret_cast<const unsigned char*>(data.data()),
                                      static_cast<int>(data.size()));
  out.resize(static_cast<std::size_t>(written));
  return out;
}

}  // namespace chargewire::detail

namespace chargewire {

std::string NewId(std::string_view prefix) { return detail::NewId(prefix); }

}  // namespace chargewire
