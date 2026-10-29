// legacy_dialect.hpp: requests in the protocol's pre-rename spelling are routed as current
// ones, and their responses go back in the spelling they were asked in. End to end, the
// pre-rename clients' own suites run against this server (see the CHANGELOG).
#include <gtest/gtest.h>

#include <string>

#include "json.hpp"
#include "legacy_dialect.hpp"

namespace {

using chargewire::detail::AdoptLegacyDialect;
using chargewire::detail::ApplyLegacyDialect;
using chargewire::detail::Request;
using chargewire::detail::Response;

Request At(const std::string& path) {
  Request request;
  request.path = path;
  return request;
}

TEST(LegacyDialect, AdoptsTheOldRootHoweverRoutingWouldSplitIt) {
  for (const auto& [old_path, current] : std::initializer_list<std::pair<const char*, const char*>>{
           {"/warp", "/chargewire"},
           {"/warp/", "/chargewire/"},
           {"/warp/v1/sessions/s_1/statements", "/chargewire/v1/sessions/s_1/statements"},
           {"//warp//v1/health", "//chargewire//v1/health"},
       }) {
    Request request = At(old_path);
    EXPECT_TRUE(AdoptLegacyDialect(request)) << old_path;
    EXPECT_EQ(request.path, current) << old_path;
  }
}

TEST(LegacyDialect, LeavesEveryOtherPathAlone) {
  for (const char* path : {"/chargewire/v1/health", "/warpx", "/ui/warp", "/health", "/", ""}) {
    Request request = At(path);
    EXPECT_FALSE(AdoptLegacyDialect(request)) << path;
    EXPECT_EQ(request.path, path);
  }
}

TEST(LegacyDialect, RenamesOldHeadersInAnyCaseAndKeepsTheRest) {
  Request request = At("/warp/v1/sessions/s_1/statements");
  request.headers.set("Warp-Statement-Timeout", "5000");
  request.headers.set("warp-result-order", "any");
  request.headers.set("Authorization", "Bearer t");
  ASSERT_TRUE(AdoptLegacyDialect(request));
  EXPECT_EQ(request.Header("Chargewire-Statement-Timeout"), "5000");
  EXPECT_EQ(request.Header("Chargewire-Result-Order"), "any");
  EXPECT_EQ(request.Header("Warp-Statement-Timeout"), "");
  EXPECT_EQ(request.Header("Authorization"), "Bearer t");
}

TEST(LegacyDialect, CurrentRequestsKeepOldNamedHeadersUnread) {
  // A current request is answered in the current spelling only: an old header on it is
  // not translated, so it can't change what the request does.
  Request request = At("/chargewire/v1/sessions/s_1/statements");
  request.headers.set("Warp-Statement-Timeout", "5000");
  EXPECT_FALSE(AdoptLegacyDialect(request));
  EXPECT_EQ(request.Header("Chargewire-Statement-Timeout"), "");
}

TEST(LegacyDialect, WritesResponsesBackInTheOldSpelling) {
  const Request request = At("/chargewire/v1/sessions");
  Response response;
  response.content_type = chargewire::detail::kProblemJson;
  response.headers = {{"Chargewire-Request-Id", "r_1"},
                      {"Location", "/chargewire/v1/sessions/s_1"},
                      {"Access-Control-Expose-Headers", "Location, Chargewire-Statement-Id, ETag"},
                      {"ETag", "\"x\""}};
  response.body = R"({"type":"urn:chargewire:error:not_found","detail":"no table \"urn:chargewire:x\" or /chargewire/v1"})";
  ApplyLegacyDialect(request, response);
  const std::vector<std::pair<std::string, std::string>> expected = {
      {"Warp-Request-Id", "r_1"},
      {"Location", "/warp/v1/sessions/s_1"},
      {"Access-Control-Expose-Headers", "Location, Warp-Statement-Id, ETag"},
      {"ETag", "\"x\""}};
  EXPECT_EQ(response.headers, expected);
  // The error type changes; text inside a message (escaped quotes, a path mid-string) does not.
  EXPECT_EQ(response.body,
            R"({"type":"urn:warp-sql:error:not_found","detail":"no table \"urn:chargewire:x\" or /chargewire/v1"})");
}

TEST(LegacyDialect, DiscoveryAndOpenApiNameTheOldProtocol) {
  Response discovery;
  discovery.content_type = chargewire::detail::kJson;
  discovery.body = R"({"protocol":"chargewire","links":{"openapi":"/chargewire/v1/openapi.json"}})";
  ApplyLegacyDialect(At("/chargewire"), discovery);
  EXPECT_EQ(discovery.body, R"({"protocol":"warp-sql","links":{"openapi":"/warp/v1/openapi.json"}})");

  Response openapi;
  openapi.content_type = chargewire::detail::kJson;
  openapi.body = R"({"paths":{"/chargewire/v1/health":{}},"name":"Chargewire-Statement-Id"})";
  ApplyLegacyDialect(At("/chargewire/v1/openapi.json"), openapi);
  EXPECT_EQ(openapi.body, R"({"paths":{"/warp/v1/health":{}},"name":"Warp-Statement-Id"})");
}

TEST(LegacyDialect, LeavesArrowBodiesAlone) {
  Response stream;
  stream.content_type = "application/vnd.apache.arrow.stream";
  stream.body = R"("urn:chargewire:)";
  ApplyLegacyDialect(At("/chargewire/v1/sessions/s_1/statements"), stream);
  EXPECT_EQ(stream.body, R"("urn:chargewire:)");
}

}  // namespace
