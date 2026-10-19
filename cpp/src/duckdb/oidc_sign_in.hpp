// warp-duckdb's browser sign-in (spec §4.3): an OpenID Connect provider, found by its
// issuer's discovery document, as a confidential client. For testing and development:
// the ID token comes straight from the token endpoint and is checked for issuer,
// audience, expiry and nonce, not by signature (OpenID Connect Core §3.1.3.7 allows this
// over verified TLS; an http:// issuer is accepted only on a loopback address). Who may
// sign in is a fixed list of identities (the ID token's email, else its sub), and each
// signs in as the administrator, as everyone does on warp-duckdb.
#pragma once

#include <string>
#include <vector>

#include "warpsql/backend.hpp"

namespace warpduckdb {

class OidcBrowserSignIn : public warpsql::BrowserSignInProvider {
 public:
  struct Options {
    std::string issuer;
    std::string client_id;
    std::string client_secret;
    std::string scopes = "openid email profile";
    std::string redirect_uri;  // the server's callback, as registered with the provider
    std::string provider_name;
    std::vector<std::string> identities;  // who may sign in (ASCII case-insensitive)
  };
  // Reads the issuer's discovery document; throws when it can't.
  explicit OidcBrowserSignIn(Options options);

  std::string DisplayName() override { return options_.provider_name; }
  std::string AuthorizeUrl(const Authorization& authorization) override;
  warpsql::Principal CompleteSignIn(const Callback& callback) override;

 private:
  Options options_;
  std::string authorization_endpoint_;
  std::string token_endpoint_;
};

}  // namespace warpduckdb
