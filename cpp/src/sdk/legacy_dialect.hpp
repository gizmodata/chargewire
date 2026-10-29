// The protocol's pre-rename spelling, for clients released before it: its own path prefix,
// header prefix, error types and protocol name. A server answers such a request in the
// spelling it was asked in, so those clients keep working; nothing else in the SDK knows
// the old spelling. This file and its .cc are the only places it is written down.
#pragma once

#include <string_view>

#include "http.hpp"

namespace chargewire::detail {

// The pre-rename health path, which load balancers and probes configured before the rename
// still ask for (on the main port and on the health port).
extern const std::string_view kLegacyHealthPath;

// True when `request` uses the pre-rename spelling. Its path and protocol headers are then
// rewritten to the current spelling, so routing and handlers see only that.
bool AdoptLegacyDialect(Request& request);

// Writes a response to such a request back in the pre-rename spelling: its protocol
// headers, Location paths, and in JSON bodies the error types, paths and protocol name.
void ApplyLegacyDialect(const Request& request, Response& response);

}  // namespace chargewire::detail
