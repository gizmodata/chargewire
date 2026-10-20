// Client addresses behind trusted reverse proxies (ServerOptions::trusted_proxies):
// X-Forwarded-For and RFC 7239 Forwarded.
#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common.hpp"
#include "warpsql/server.hpp"

namespace warpsql::detail {

// An address as a socket or a forwarding header gives it, with no port or brackets;
// an IPv4-mapped IPv6 address becomes its IPv4 address. Nullopt: not an IP address (an
// IPv6 zone, "fe80::1%eth0", is refused too).
std::optional<net::ip::address> ParseIpAddress(std::string_view text);

// Whether `address` lies in any of `networks`.
bool InNetworks(const std::vector<IpNetwork>& networks, const net::ip::address& address);

// How the SDK writes a client's address: "203.0.113.9:5000", "[2001:db8::7]:5000"; without
// a port, "203.0.113.9", "[2001:db8::7]".
std::string FormatPeer(const net::ip::address& address, std::optional<uint16_t> port = std::nullopt);

// The client's address that the forwarding headers of a request from a trusted proxy
// give (see ServerOptions::trusted_proxies), formatted without a port; nullopt when they
// give none (no header, a malformed one, an unknown or obfuscated hop): the TCP peer
// then stands.
std::optional<std::string> ForwardedClient(const http::fields& headers, const std::vector<IpNetwork>& trusted);

// The address of a peer as FormatPeer writes it, without port or brackets
// ("127.0.0.1:5000" -> "127.0.0.1", "[::1]:5" -> "::1", "[::1]" -> "::1", "10.0.0.1" ->
// "10.0.0.1"). An unbracketed "::1:5" is read as address:port.
std::string PeerHost(std::string_view peer);

}  // namespace warpsql::detail
