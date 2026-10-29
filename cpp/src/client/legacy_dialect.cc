#include "legacy_dialect.hpp"

#include <cctype>
#include <utility>
#include <vector>

namespace chargewire::client::detail {

namespace {

constexpr std::string_view kLegacyRoot = "/warp";
constexpr std::string_view kLegacyProtocol = "warp-sql";
constexpr std::string_view kLegacyHeaderPrefix = "Warp-";
constexpr std::string_view kCurrentHeaderPrefix = "Chargewire-";

bool StartsWithIgnoringCase(std::string_view text, std::string_view prefix) {
  if (text.size() < prefix.size()) return false;
  for (std::size_t i = 0; i < prefix.size(); i++) {
    if (std::tolower(static_cast<unsigned char>(text[i])) != std::tolower(static_cast<unsigned char>(prefix[i]))) {
      return false;
    }
  }
  return true;
}

}  // namespace

bool IsLegacyRoot(std::string_view base_path) { return base_path == kLegacyRoot; }

std::string LegacyRoot() { return std::string(kLegacyRoot); }

bool IsLegacyProtocol(std::string_view protocol) { return protocol == kLegacyProtocol; }

void ToLegacyHeaders(http::fields& headers) {
  std::vector<std::pair<std::string, std::string>> renamed;
  for (const auto& field : headers) {
    if (StartsWithIgnoringCase(field.name_string(), kCurrentHeaderPrefix)) {
      renamed.emplace_back(std::string(field.name_string()), std::string(field.value()));
    }
  }
  for (const auto& [name, value] : renamed) {
    headers.erase(name);
    headers.set(LegacyHeaderName(name), value);
  }
}

std::string LegacyHeaderName(std::string_view name) {
  if (!StartsWithIgnoringCase(name, kCurrentHeaderPrefix)) return std::string(name);
  return std::string(kLegacyHeaderPrefix) + std::string(name.substr(kCurrentHeaderPrefix.size()));
}

}  // namespace chargewire::client::detail
