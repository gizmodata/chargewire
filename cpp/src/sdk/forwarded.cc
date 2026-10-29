#include "forwarded.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <format>
#include <functional>
#include <map>
#include <stdexcept>

namespace chargewire::detail {

namespace {

std::string_view Trim(std::string_view text) {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) text.remove_prefix(1);
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) text.remove_suffix(1);
  return text;
}

bool EqualsIgnoringCase(std::string_view a, std::string_view b) {
  return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char x, char y) {
           return std::tolower(static_cast<unsigned char>(x)) == std::tolower(static_cast<unsigned char>(y));
         });
}

// A dotted quad, independent of the system parser (Windows also accepts shortened
// forms and an address followed by a port). No octal, hex or leading zeroes.
std::optional<net::ip::address_v4> ParseV4(std::string_view text) {
  net::ip::address_v4::bytes_type bytes{};
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    const auto dot = text.find('.');
    if ((i < bytes.size() - 1) != (dot != std::string_view::npos)) return std::nullopt;
    const auto part = text.substr(0, dot);
    if (part.empty() || part.size() > 3 || (part.size() > 1 && part.front() == '0')) return std::nullopt;
    unsigned value = 0;
    const auto [end, ec] = std::from_chars(part.data(), part.data() + part.size(), value);
    if (ec != std::errc() || end != part.data() + part.size() || value > 255) return std::nullopt;
    bytes[i] = static_cast<unsigned char>(value);
    if (dot != std::string_view::npos) text.remove_prefix(dot + 1);
  }
  return net::ip::address_v4(bytes);
}

// An IP address in text; nullopt for anything else. No
// normalization: an IPv4-mapped address stays IPv6.
std::optional<net::ip::address> ParseRaw(std::string_view text) {
  if (text.empty() || text.size() > 64) return std::nullopt;
  for (const char c : text) {
    // Hex digits, dots and colons only: no zone ("%eth0"), no spaces or signs.
    if (!std::isxdigit(static_cast<unsigned char>(c)) && c != '.' && c != ':') return std::nullopt;
  }
  if (text.find(':') == std::string_view::npos) {
    const auto address = ParseV4(text);
    if (!address) return std::nullopt;
    return *address;
  }
  // An embedded IPv4 suffix follows the same dotted-quad rules.
  if (text.find('.') != std::string_view::npos && !ParseV4(text.substr(text.rfind(':') + 1))) {
    return std::nullopt;
  }
  boost::system::error_code ec;
  auto address = net::ip::make_address_v6(std::string(text), ec);
  if (ec) return std::nullopt;
  return address;
}

net::ip::address Normalized(const net::ip::address& address) {
  if (address.is_v6() && address.to_v6().is_v4_mapped()) {
    return net::ip::make_address_v4(net::ip::v4_mapped, address.to_v6());
  }
  return address;
}

// The address bytes: 4 for IPv4, 16 for IPv6.
std::vector<uint8_t> BytesOf(const net::ip::address& address) {
  if (address.is_v4()) {
    const auto b = address.to_v4().to_bytes();
    return {b.begin(), b.end()};
  }
  const auto b = address.to_v6().to_bytes();
  return {b.begin(), b.end()};
}

bool PrefixMatches(const uint8_t* network, const uint8_t* address, int prefix_length) {
  const int whole = prefix_length / 8;
  if (!std::equal(network, network + whole, address)) return false;
  const int rest = prefix_length % 8;
  if (rest == 0) return true;
  const auto mask = static_cast<uint8_t>(0xFF << (8 - rest));
  return (network[whole] & mask) == (address[whole] & mask);
}

bool ParsePort(std::string_view text, bool allow_obfuscated) {
  if (allow_obfuscated && text.size() > 1 && text.front() == '_') {
    return std::all_of(text.begin() + 1, text.end(), [](char c) {
      return std::isalnum(static_cast<unsigned char>(c)) || c == '.' || c == '_' || c == '-';
    });
  }
  if (text.empty() || text.size() > 5) return false;
  unsigned value = 0;
  const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
  return ec == std::errc() && end == text.data() + text.size() && value >= 1 && value <= 65535;
}

// One hop: "203.0.113.9", "203.0.113.9:5000", "2001:db8::7", "[2001:db8::7]",
// "[2001:db8::7]:5000". Nullopt for anything else ("unknown", an obfuscated "_hidden", a
// name, garbage). A port is checked and dropped (Forwarded may obfuscate it: "_p1").
std::optional<net::ip::address> ParseHop(std::string_view hop, bool obfuscated_port_ok) {
  if (hop.starts_with('[')) {
    const auto close = hop.find(']');
    if (close == std::string_view::npos) return std::nullopt;
    const auto address = ParseRaw(hop.substr(1, close - 1));
    if (!address || !address->is_v6()) return std::nullopt;
    const std::string_view rest = hop.substr(close + 1);
    if (!rest.empty() && !(rest.starts_with(':') && ParsePort(rest.substr(1), obfuscated_port_ok))) {
      return std::nullopt;
    }
    return Normalized(*address);
  }
  const auto colons = std::count(hop.begin(), hop.end(), ':');
  if (colons == 1) {  // IPv4 and a port
    const auto colon = hop.find(':');
    const auto address = ParseRaw(hop.substr(0, colon));
    if (!address || !address->is_v4() || !ParsePort(hop.substr(colon + 1), obfuscated_port_ok)) {
      return std::nullopt;
    }
    return address;
  }
  const auto address = ParseRaw(hop);  // IPv4, or IPv6 without brackets (and so no port)
  if (!address) return std::nullopt;
  return Normalized(*address);
}

// The client among `hops` (left to right, as the proxies appended them): the rightmost
// that isn't a trusted proxy, or the leftmost when all are. Nullopt when a hop read on
// the way isn't an address.
template <class Hops>
std::optional<std::string> Walk(const Hops& hops, const std::vector<IpNetwork>& trusted,
                                const std::function<std::optional<net::ip::address>(const typename Hops::value_type&)>& parse) {
  if (hops.empty()) return std::nullopt;
  std::optional<net::ip::address> address;
  for (auto it = hops.rbegin(); it != hops.rend(); ++it) {
    address = parse(*it);
    if (!address) return std::nullopt;
    if (!InNetworks(trusted, *address)) break;
  }
  return FormatPeer(*address);
}

std::optional<std::string> FromXForwardedFor(const std::vector<std::string_view>& values,
                                             const std::vector<IpNetwork>& trusted) {
  std::vector<std::string_view> hops;
  for (const std::string_view value : values) {
    std::size_t start = 0;
    for (;;) {
      const auto comma = value.find(',', start);
      hops.push_back(Trim(value.substr(start, comma == std::string_view::npos ? std::string_view::npos : comma - start)));
      if (comma == std::string_view::npos) break;
      start = comma + 1;
    }
  }
  return Walk(hops, trusted, [](std::string_view hop) -> std::optional<net::ip::address> {
    return ParseHop(hop, false);  // an empty hop, or "unknown", is no address
  });
}

bool IsTchar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || std::string_view("!#$%&'*+-.^_`|~").find(c) != std::string_view::npos;
}

using Element = std::map<std::string, std::string>;  // parameter (lower case) -> value

// RFC 7239 §4: forwarded-element *( OWS "," OWS forwarded-element ), each element
// pair *( ";" pair ), pair token "=" ( token / quoted-string ). Appends the elements of
// `value` to `out`; false on a syntax error (or a parameter given twice in an element).
// Empty list elements (",,") are skipped. Unquoted values may also hold ':', '[' and ']'
// (a common slip: for=[2001:db8::7] without quotes).
bool ParseForwarded(std::string_view value, std::vector<Element>& out) {
  std::size_t i = 0;
  auto ows = [&] {
    while (i < value.size() && (value[i] == ' ' || value[i] == '\t')) i++;
  };
  for (;;) {
    ows();
    if (i == value.size()) return true;
    if (value[i] == ',') {
      i++;
      continue;
    }
    Element element;
    for (;;) {
      ows();
      const std::size_t name_start = i;
      while (i < value.size() && IsTchar(value[i])) i++;
      if (i == name_start || i == value.size() || value[i] != '=') return false;
      std::string name(value.substr(name_start, i - name_start));
      std::transform(name.begin(), name.end(), name.begin(),
                     [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
      i++;  // '='
      std::string parsed;
      if (i < value.size() && value[i] == '"') {
        i++;
        bool closed = false;
        while (i < value.size()) {
          const char c = value[i++];
          if (c == '"') {
            closed = true;
            break;
          }
          if (c == '\\') {
            if (i == value.size()) return false;
            parsed += value[i++];
          } else if (static_cast<unsigned char>(c) < 0x20 && c != '\t') {
            return false;
          } else {
            parsed += c;
          }
        }
        if (!closed) return false;
      } else {
        const std::size_t value_start = i;
        while (i < value.size() && (IsTchar(value[i]) || value[i] == ':' || value[i] == '[' || value[i] == ']')) i++;
        if (i == value_start) return false;
        parsed = std::string(value.substr(value_start, i - value_start));
      }
      if (!element.emplace(std::move(name), std::move(parsed)).second) return false;
      ows();
      if (i < value.size() && value[i] == ';') {
        i++;
        continue;
      }
      break;
    }
    out.push_back(std::move(element));
    if (i == value.size()) return true;
    if (value[i] != ',') return false;
    i++;
  }
}

std::optional<std::string> FromForwarded(const std::vector<std::string_view>& values,
                                         const std::vector<IpNetwork>& trusted) {
  std::vector<Element> elements;
  for (const std::string_view value : values) {
    if (!ParseForwarded(value, elements)) return std::nullopt;
  }
  return Walk(elements, trusted, [](const Element& element) -> std::optional<net::ip::address> {
    const auto found = element.find("for");
    // No for=, "unknown" or an obfuscated identifier: this hop's client is not known, so
    // nothing to its left can be vouched for either.
    if (found == element.end() || found->second.starts_with('_') || EqualsIgnoringCase(found->second, "unknown")) {
      return std::nullopt;
    }
    return ParseHop(found->second, true);
  });
}

std::vector<std::string_view> Values(const http::fields& headers, std::string_view name) {
  std::vector<std::string_view> values;
  for (auto [it, end] = headers.equal_range(name); it != end; ++it) values.push_back(it->value());
  return values;
}

}  // namespace

std::optional<net::ip::address> ParseIpAddress(std::string_view text) {
  auto address = ParseRaw(text);
  if (!address) return std::nullopt;
  return Normalized(*address);
}

bool InNetworks(const std::vector<IpNetwork>& networks, const net::ip::address& address) {
  const net::ip::address normalized = Normalized(address);
  const std::vector<uint8_t> bytes = BytesOf(normalized);
  for (const auto& network : networks) {
    if (network.v6 != normalized.is_v6()) continue;
    if (PrefixMatches(network.bytes.data(), bytes.data(), network.prefix_length)) return true;
  }
  return false;
}

std::string FormatPeer(const net::ip::address& address, std::optional<uint16_t> port) {
  std::string out = address.is_v6() ? "[" + address.to_string() + "]" : address.to_string();
  if (port) out += ":" + std::to_string(*port);
  return out;
}

std::optional<std::string> ForwardedClient(const http::fields& headers, const std::vector<IpNetwork>& trusted) {
  const auto xff = Values(headers, "X-Forwarded-For");
  if (!xff.empty()) return FromXForwardedFor(xff, trusted);
  const auto forwarded = Values(headers, "Forwarded");
  if (!forwarded.empty()) return FromForwarded(forwarded, trusted);
  return std::nullopt;
}

std::string PeerHost(std::string_view peer) {
  if (peer.starts_with('[')) {
    const auto close = peer.find(']');
    return std::string(peer.substr(1, close == std::string_view::npos ? std::string_view::npos : close - 1));
  }
  const auto colon = peer.rfind(':');
  return std::string(colon == std::string_view::npos ? peer : peer.substr(0, colon));
}

}  // namespace chargewire::detail

namespace chargewire {

namespace net = detail::net;

std::string IpNetwork::ToString() const {
  std::string address;
  if (v6) {
    net::ip::address_v6::bytes_type b;
    std::copy(bytes.begin(), bytes.end(), b.begin());
    address = net::ip::address_v6(b).to_string();
  } else {
    address = net::ip::address_v4({bytes[0], bytes[1], bytes[2], bytes[3]}).to_string();
  }
  return address + "/" + std::to_string(prefix_length);
}

bool IpNetwork::Contains(std::string_view address) const {
  const auto parsed = detail::ParseIpAddress(address);
  return parsed && detail::InNetworks({*this}, *parsed);
}

IpNetwork ParseIpNetwork(std::string_view text) {
  const std::string_view trimmed = detail::Trim(text);
  const auto slash = trimmed.find('/');
  const std::string_view address_text = trimmed.substr(0, slash);
  const auto address = detail::ParseRaw(address_text);
  if (!address) {
    throw std::invalid_argument(std::format(
        "'{}' is not an IP network: '{}' is not an IPv4 or IPv6 address (write 10.0.0.0/8, 2001:db8::/32 or "
        "a single address)",
        text, address_text));
  }
  const int bits = address->is_v4() ? 32 : 128;
  int prefix = bits;
  if (slash != std::string_view::npos) {
    const std::string_view prefix_text = trimmed.substr(slash + 1);
    const auto [end, ec] = std::from_chars(prefix_text.data(), prefix_text.data() + prefix_text.size(), prefix);
    if (prefix_text.empty() || prefix_text.size() > 3 || ec != std::errc() ||
        end != prefix_text.data() + prefix_text.size() || prefix < 0 || prefix > bits) {
      throw std::invalid_argument(std::format("'{}' is not an IP network: the prefix length must be 0-{}", text, bits));
    }
  }
  IpNetwork network;
  net::ip::address effective = *address;
  if (address->is_v6() && address->to_v6().is_v4_mapped() && prefix >= 96) {
    effective = net::ip::make_address_v4(net::ip::v4_mapped, address->to_v6());
    prefix -= 96;
  }
  network.v6 = effective.is_v6();
  network.prefix_length = prefix;
  const std::vector<uint8_t> bytes = detail::BytesOf(effective);
  std::copy(bytes.begin(), bytes.end(), network.bytes.begin());
  IpNetwork masked = network;
  for (int bit = prefix; bit < static_cast<int>(bytes.size()) * 8; bit++) {
    masked.bytes[bit / 8] &= static_cast<uint8_t>(~(0x80 >> (bit % 8)));
  }
  if (masked.bytes != network.bytes) {
    throw std::invalid_argument(std::format("'{}' is not an IP network: it has bits set below its /{} prefix (did "
                                            "you mean {}?)",
                                            text, prefix, masked.ToString()));
  }
  return network;
}

}  // namespace chargewire
