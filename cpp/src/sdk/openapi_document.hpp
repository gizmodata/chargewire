// The protocol's OpenAPI 3.1 description (spec §3.3), spec/openapi.yaml as JSON. The build
// converts the YAML (tools/openapi_json.cc) into a generated source file that defines these.
#pragma once

#include <string_view>

namespace warpsql {

// The document, compact JSON.
std::string_view OpenApiJson();
// A strong entity tag for it, quoted ("..."), from a hash of the JSON.
std::string_view OpenApiEtag();

}  // namespace warpsql
