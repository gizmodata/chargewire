#include "duckdb_backend.hpp"

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <array>
#include <filesystem>

#include "duckdb_session.hpp"
#include "duckdb_sql.hpp"
#include "duckdb_upload.hpp"

namespace warpduckdb {

namespace json = boost::json;
using warpsql::Error;
using warpsql::ErrorCode;

DuckDBBackend::DuckDBBackend(Options options) {
  Call([&](auto* err) { return duckdb_v2_environment_create(environment_.out(), err); });
  if (options.read_only) {
    const duckdb_v2_str names[] = {Str("access_mode")};
    const duckdb_v2_str values[] = {Str("READ_ONLY")};
    Call([&](auto* err) {
      return duckdb_v2_instance_create_with_options(environment_.get(), names, values, 1,
                                                    instance_.out(), err);
    });
  } else {
    Call([&](auto* err) { return duckdb_v2_instance_create(environment_.get(), instance_.out(), err); });
  }
  // The catalog is named as DuckDB names it: the file's stem, or "memory".
  const std::string catalog = options.database == ":memory:"
                                  ? std::string("memory")
                                  : std::filesystem::path(options.database).stem().string();
  const duckdb_v2_identifier_t name = Str(catalog);
  const duckdb_v2_str path = Str(options.database);
  Call([&](auto* err) {
    return duckdb_v2_instance_attach(instance_.get(), &name, &path, nullptr, true, err);
  });

  duckdb_v2_str version{};
  Call([&](auto* err) { return duckdb_v2_library_version(&version, err); });
  version_ = ToString(version);

  Call([&](auto* err) {
    return duckdb_v2_connection_create(instance_.get(), health_connection_.out(), err);
  });
  // Functions are registered on the instance's catalog, once.
  RegisterUploadFunction(health_connection_.get());
}

DuckDBBackend::~DuckDBBackend() {
  health_connection_.reset();
  instance_.reset();
  environment_.reset();
}

warpsql::ServerIdentity DuckDBBackend::Identity() {
  return {"warp-duckdb", "0.1.0 (DuckDB " + version_ + ")"};
}

std::unique_ptr<warpsql::EngineSession> DuckDBBackend::NewSession(const warpsql::Principal&) {
  return std::make_unique<DuckDBSession>(instance_.get());
}

std::unique_ptr<warpsql::EngineSession> DuckDBBackend::OpenSession(
    const warpsql::Principal& principal, const json::object& options) {
  auto session = NewSession(principal);
  if (!options.empty()) {
    auto errors = session->SetOptions(options);
    if (!errors.empty()) {
      throw Error(ErrorCode::kInvalidOptionValue,
                  errors.begin()->first + ": " + errors.begin()->second.what());
    }
  }
  return session;
}

void DuckDBBackend::CheckHealth() {
  std::lock_guard lock(health_mutex_);
  QueryText(health_connection_.get(), "SELECT 1");
}

json::object DuckDBBackend::ServerInfo() {
  return {{"engine", "duckdb"}, {"engine_version", version_}};
}

warpsql::Principal StaticPasswordAuthenticator::AuthenticatePassword(std::string_view username,
                                                                     std::string_view password) {
  // Compare digests, so neither the time taken nor the comparison length depends on
  // where the inputs differ.
  auto digest = [](std::string_view text) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> out{};
    unsigned int length = 0;
    EVP_Digest(text.data(), text.size(), out.data(), &length, EVP_sha256(), nullptr);
    return out;
  };
  const auto given_user = digest(username);
  const auto want_user = digest(username_);
  const auto given_password = digest(password);
  const auto want_password = digest(password_);
  const bool user_ok = CRYPTO_memcmp(given_user.data(), want_user.data(), 32) == 0;
  const bool password_ok = CRYPTO_memcmp(given_password.data(), want_password.data(), 32) == 0;
  if (!user_ok || !password_ok) {
    throw Error(ErrorCode::kInvalidCredentials, "the user name or password is not correct");
  }
  return warpsql::Principal{std::string(username), "admin", "password", {}};
}

}  // namespace warpduckdb
