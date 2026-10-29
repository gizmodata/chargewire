#include "legacy_dialect.hpp"

#include <string>
#include <utility>
#include <vector>

#include "json.hpp"

namespace chargewire::detail {

namespace {

constexpr std::string_view kLegacyRoot = "/warp";
constexpr std::string_view kCurrentRoot = "/chargewire";
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

void ReplaceAll(std::string& text, std::string_view from, std::string_view to) {
  for (std::size_t at = text.find(from); at != std::string::npos; at = text.find(from, at + to.size())) {
    text.replace(at, from.size(), to);
  }
}

// Rewrites `from` where it starts a JSON string: its opening quote follows ':', ',', '[' or
// '{' (the server writes compact JSON). Text inside a value, such as a message quoting user
// input, is never touched.
void ReplaceJsonStringStarts(std::string& json, std::string_view from, std::string_view to) {
  for (std::size_t at = json.find(from); at != std::string::npos; at = json.find(from, at + 1)) {
    if (at == 0 || std::string_view(":,[{").find(json[at - 1]) == std::string_view::npos) continue;
    json.replace(at, from.size(), to);
  }
}

// The JSON strings a server writes that carry the protocol's spelling, each from its quote.
const std::vector<std::pair<std::string_view, std::string_view>> kJsonSpellings = {
    {R"("urn:chargewire:)", R"("urn:warp-sql:)"},
    {R"("/chargewire)", R"("/warp)"},
    {R"("protocol":"chargewire")", R"("protocol":"warp-sql")"},  // a key, after '{' or ','
};

}  // namespace

const std::string_view kLegacyHealthPath = "/warp/v1/health";

bool AdoptLegacyDialect(Request& request) {
  // The first segment, found as routing finds it: leading and doubled slashes don't count.
  const std::string_view path = request.path;
  const std::size_t start = path.find_first_not_of('/');
  if (start == std::string_view::npos) return false;
  const std::string_view rest = path.substr(start);
  const std::string_view segment = rest.substr(0, rest.find('/'));
  if (segment != kLegacyRoot.substr(1)) return false;
  request.path = std::string(path.substr(0, start)) + std::string(kCurrentRoot.substr(1)) +
                 std::string(rest.substr(segment.size()));

  std::vector<std::pair<std::string, std::string>> renamed;
  for (const auto& field : request.headers) {
    const std::string_view name = field.name_string();
    if (StartsWithIgnoringCase(name, kLegacyHeaderPrefix)) {
      renamed.emplace_back(std::string(name), std::string(field.value()));
    }
  }
  for (const auto& [name, value] : renamed) {
    request.headers.erase(name);
    request.headers.set(std::string(kCurrentHeaderPrefix) + name.substr(kLegacyHeaderPrefix.size()), value);
  }
  return true;
}

void ApplyLegacyDialect(const Request& request, Response& response) {
  for (auto& [name, value] : response.headers) {
    if (StartsWithIgnoringCase(name, kCurrentHeaderPrefix)) {
      name = std::string(kLegacyHeaderPrefix) + name.substr(kCurrentHeaderPrefix.size());
    }
    if (name == "Location" && value.starts_with(kCurrentRoot)) {
      value = std::string(kLegacyRoot) + value.substr(kCurrentRoot.size());
    }
    // CORS lists of exposed and allowed header names.
    if (StartsWithIgnoringCase(name, "Access-Control-")) {
      ReplaceAll(value, kCurrentHeaderPrefix, kLegacyHeaderPrefix);
    }
  }
  if (response.content_type != kJson && response.content_type != kProblemJson) return;
  for (const auto& [from, to] : kJsonSpellings) {
    ReplaceJsonStringStarts(response.body, from, to);
  }
  // The OpenAPI document names the protocol's headers as JSON strings too.
  if (request.path.ends_with("/openapi.json")) {
    ReplaceJsonStringStarts(response.body, R"(")" + std::string(kCurrentHeaderPrefix),
                            R"(")" + std::string(kLegacyHeaderPrefix));
  }
}

}  // namespace chargewire::detail
