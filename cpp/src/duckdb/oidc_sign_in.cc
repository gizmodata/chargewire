#include "oidc_sign_in.hpp"

#include <boost/json.hpp>
#include <boost/url.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <stdexcept>

#include "warpsql/client.hpp"

namespace warpduckdb {

namespace json = boost::json;
using warpsql::Error;
using warpsql::ErrorCode;

namespace {

bool LoopbackHttp(const std::string& url) {
  auto parsed = boost::urls::parse_uri(url);
  if (!parsed) return false;
  if (parsed->scheme() == "https") return true;
  const std::string host(parsed->host());
  return parsed->scheme() == "http" && (host == "127.0.0.1" || host == "localhost" || host == "[::1]");
}

std::string Base64UrlDecode(std::string_view text) {
  static constexpr std::string_view kAlphabet =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string out;
  unsigned buffer = 0;
  int bits = 0;
  for (const char c : text) {
    if (c == '=') break;
    const auto index = kAlphabet.find(c);
    if (index == std::string_view::npos) throw std::runtime_error("the ID token is not base64url");
    buffer = (buffer << 6) | static_cast<unsigned>(index);
    bits += 6;
    if (bits >= 8) {
      out.push_back(static_cast<char>((buffer >> (bits - 8)) & 0xFF));
      bits -= 8;
    }
  }
  return out;
}

std::string Text(const json::object& object, std::string_view key) {
  const auto* value = object.if_contains(key);
  return value != nullptr && value->is_string() ? std::string(value->as_string()) : std::string();
}

bool EqualsIgnoringCase(std::string_view a, std::string_view b) {
  return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char x, char y) {
           return std::tolower(static_cast<unsigned char>(x)) == std::tolower(static_cast<unsigned char>(y));
         });
}

}  // namespace

OidcBrowserSignIn::OidcBrowserSignIn(Options options) : options_(std::move(options)) {
  std::string issuer = options_.issuer;
  while (issuer.ends_with('/')) issuer.pop_back();
  if (!LoopbackHttp(issuer)) {
    throw std::runtime_error("--oauth-issuer must be https:// (http:// only on a loopback address)");
  }
  const auto [status, document] =
      warpsql::client::GetJson(issuer + "/.well-known/openid-configuration", warpsql::client::TlsOptions{});
  if (status != 200 || !document.is_object()) {
    throw std::runtime_error("the issuer's OpenID configuration answered HTTP " + std::to_string(status));
  }
  const auto& doc = document.as_object();
  authorization_endpoint_ = Text(doc, "authorization_endpoint");
  token_endpoint_ = Text(doc, "token_endpoint");
  if (Text(doc, "issuer") != options_.issuer && Text(doc, "issuer") != issuer) {
    throw std::runtime_error("the OpenID configuration names another issuer: " + Text(doc, "issuer"));
  }
  if (authorization_endpoint_.empty() || !LoopbackHttp(token_endpoint_)) {
    throw std::runtime_error("the OpenID configuration has no usable authorization_endpoint and token_endpoint");
  }
}

std::string OidcBrowserSignIn::AuthorizeUrl(const Authorization& authorization) {
  boost::urls::url url(authorization_endpoint_);
  auto params = url.params();
  params.append({"response_type", "code"});
  params.append({"client_id", options_.client_id});
  params.append({"redirect_uri", options_.redirect_uri});
  params.append({"scope", options_.scopes});
  params.append({"state", authorization.state});
  params.append({"code_challenge", authorization.code_challenge});
  params.append({"code_challenge_method", "S256"});
  params.append({"nonce", authorization.nonce});
  return std::string(url.buffer());
}

warpsql::Principal OidcBrowserSignIn::CompleteSignIn(const Callback& callback) {
  const auto [status, answer] = warpsql::client::PostForm(token_endpoint_, {{"grant_type", "authorization_code"},
                                                                           {"code", callback.code},
                                                                           {"redirect_uri", options_.redirect_uri},
                                                                           {"client_id", options_.client_id},
                                                                           {"client_secret", options_.client_secret},
                                                                           {"code_verifier", callback.code_verifier}},
                                                          warpsql::client::TlsOptions{});
  const json::object* body = answer.is_object() ? &answer.as_object() : nullptr;
  if (body != nullptr && body->contains("error")) {
    std::string error = Text(*body, "error");
    if (const std::string description = Text(*body, "error_description"); !description.empty()) {
      error += ": " + description;
    }
    throw std::runtime_error("the identity provider refused the code: " + error);
  }
  if (status != 200 || body == nullptr) {
    throw std::runtime_error("the identity provider's token endpoint answered HTTP " + std::to_string(status));
  }
  const std::string id_token = Text(*body, "id_token");
  const auto first = id_token.find('.');
  const auto second = id_token.find('.', first == std::string::npos ? 0 : first + 1);
  if (first == std::string::npos || second == std::string::npos) {
    throw Error(ErrorCode::kInvalidCredentials, "the identity provider's answer has no ID token");
  }
  boost::system::error_code ec;
  json::value payload_value = json::parse(Base64UrlDecode(id_token.substr(first + 1, second - first - 1)), ec);
  if (ec || !payload_value.is_object()) throw Error(ErrorCode::kInvalidCredentials, "the ID token is not JSON");
  const auto& claims = payload_value.as_object();

  if (Text(claims, "iss") != options_.issuer) throw Error(ErrorCode::kInvalidCredentials, "the ID token is for another issuer");
  bool audience = false;
  if (const auto* aud = claims.if_contains("aud")) {
    if (aud->is_string()) audience = aud->as_string() == options_.client_id;
    if (aud->is_array()) {
      for (const auto& item : aud->as_array()) audience = audience || (item.is_string() && item.as_string() == options_.client_id);
    }
  }
  if (!audience) throw Error(ErrorCode::kInvalidCredentials, "the ID token is for another audience");
  const auto* exp = claims.if_contains("exp");
  const auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
  if (exp == nullptr || !exp->is_number() || exp->to_number<double>() <= static_cast<double>(now)) {
    throw Error(ErrorCode::kInvalidCredentials, "the ID token has expired");
  }
  if (Text(claims, "nonce") != callback.nonce) {
    throw Error(ErrorCode::kInvalidCredentials, "the ID token's nonce is not this sign-in's");
  }
  std::string identity = Text(claims, "email");
  if (identity.empty()) identity = Text(claims, "sub");
  for (const auto& allowed : options_.identities) {
    if (EqualsIgnoringCase(allowed, identity)) {
      return warpsql::Principal{.name = allowed, .role = "admin", .auth_method = "oauth_browser"};
    }
  }
  throw Error(ErrorCode::kInvalidCredentials, "'" + identity + "' may not sign in to this server");
}

}  // namespace warpduckdb
