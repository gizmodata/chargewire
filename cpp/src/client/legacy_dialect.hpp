// The protocol's pre-rename spelling, for servers released before it. A client that finds
// no server at the current root but a pre-rename one at the old root speaks the old
// spelling on that connection: its root, header prefix and protocol name. This file and its
// .cc are the only places the client writes that spelling down.
#pragma once

#include <boost/beast/http.hpp>
#include <string>
#include <string_view>

namespace chargewire::client::detail {

namespace http = boost::beast::http;

// The pre-rename root, which a caller may still pass as its base path.
bool IsLegacyRoot(std::string_view base_path);
// Where to look for a pre-rename server when the current root isn't there.
std::string LegacyRoot();
// Whether a discovery document's "protocol" names the pre-rename protocol.
bool IsLegacyProtocol(std::string_view protocol);
// Request headers in the current spelling, rewritten to the pre-rename one.
void ToLegacyHeaders(http::fields& headers);
// The pre-rename name of a current header name ("Chargewire-Statement-Id"); other names
// unchanged.
std::string LegacyHeaderName(std::string_view name);

}  // namespace chargewire::client::detail
