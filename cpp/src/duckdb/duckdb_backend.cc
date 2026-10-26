#include "duckdb_backend.hpp"

#include <algorithm>
#include <cctype>

#include <openssl/crypto.h>
#include <openssl/evp.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <thread>

#include "duckdb_describe.hpp"
#include "duckdb_session.hpp"
#include "duckdb_sql.hpp"
#include "duckdb_upload.hpp"

namespace warpduckdb {

namespace json = boost::json;

// How long a health check may run before it is interrupted.
constexpr auto kHealthCheckLimit = std::chrono::seconds(10);
using warpsql::Error;
using warpsql::ErrorCode;

DuckDBBackend::DuckDBBackend(Options options)
    : result_batch_rows_(options.result_batch_rows), read_only_(options.read_only),
      health_query_(options.health_query) {
  Call([&](auto* err) { return duckdb_v2_environment_create(environment_.out(), err); });
  std::vector<std::pair<std::string, std::string>> settings = options.instance_options;
  if (options.read_only) {
    settings.emplace_back("access_mode", "READ_ONLY");
  }
  std::vector<duckdb_v2_str> names;
  std::vector<duckdb_v2_str> values;
  for (const auto& [name, value] : settings) {
    names.push_back(Str(name));
    values.push_back(Str(value));
  }
  Call([&](auto* err) {
    return duckdb_v2_instance_create_with_options(environment_.get(), names.data(), values.data(),
                                                  names.size(), instance_.out(), err);
  });

  // The catalog is named as DuckDB names it: the file's stem, or "memory".
  const std::string catalog = options.database == ":memory:"
                                  ? std::string("memory")
                                  : std::filesystem::path(options.database).stem().string();
  using AttachOptions = Owned<duckdb_v2_attach_options_handle, duckdb_v2_attach_options_destroy>;
  AttachOptions attach;
  if (!options.attach_options.empty()) {
    Call([&](auto* err) { return duckdb_v2_attach_options_create(instance_.get(), attach.out(), err); });
    for (const auto& [key, value] : options.attach_options) {
      const duckdb_v2_identifier_t key_str = Str(key);
      const duckdb_v2_str value_str = Str(value);
      Call([&](auto* err) { return duckdb_v2_attach_options_set(attach.get(), &key_str, &value_str, err); });
    }
  }
  const duckdb_v2_identifier_t name = Str(catalog);
  const duckdb_v2_str path = Str(options.database);
  Call([&](auto* err) {
    return duckdb_v2_instance_attach(instance_.get(), &name, &path, attach.get(), true, err);
  });

  duckdb_v2_str version{};
  Call([&](auto* err) { return duckdb_v2_library_version(&version, err); });
  version_ = ToString(version);

  Call([&](auto* err) {
    return duckdb_v2_connection_create(instance_.get(), health_connection_.out(), err);
  });
  // Functions are registered on the instance's catalog, once.
  RegisterUploadFunction(health_connection_.get());
  RegisterResultSchemaFunction(health_connection_.get());
}

DuckDBBackend::~DuckDBBackend() {
  health_connection_.reset();
  instance_.reset();
  environment_.reset();
}

warpsql::ServerIdentity DuckDBBackend::Identity() {
  return {"warp-duckdb", "1.0.2 (DuckDB " + version_ + ")"};
}

std::unique_ptr<warpsql::EngineSession> DuckDBBackend::NewSession(const warpsql::Principal&, const std::string&) {
  return std::make_unique<DuckDBSession>(instance_.get(), result_batch_rows_);
}

std::unique_ptr<warpsql::EngineSession> DuckDBBackend::OpenSession(
    const warpsql::Principal& principal, const std::string& session_id, const json::object& options) {
  auto session = NewSession(principal, session_id);
  if (!options.empty()) {
    auto errors = session->SetOptions(options);
    if (!errors.empty()) {
      // The first failure, with its own code (unknown_option or invalid_option_value).
      const auto& [name, error] = *errors.begin();
      throw Error(error.code(), name + ": " + error.what());
    }
  }
  return session;
}

void DuckDBBackend::CheckHealth() {
  // One check at a time; a probe that comes while one hangs fails at once rather than
  // queue an engine thread behind it.
  std::unique_lock lock(health_mutex_, std::try_to_lock);
  if (!lock.owns_lock()) {
    throw Error(ErrorCode::kInternalError, "the previous health check has not finished");
  }
  // A check that hangs is interrupted, so it never holds its engine thread for long (the
  // SDK has answered "check_failed" by then, spec §3.2).
  std::mutex mutex;
  std::condition_variable finished;
  bool done = false;
  std::thread watchdog([&] {
    std::unique_lock wait(mutex);
    if (!finished.wait_for(wait, kHealthCheckLimit, [&] { return done; })) {
      duckdb_v2_connection_interrupt(health_connection_.get(), nullptr);
    }
  });
  struct Join {
    std::mutex& mutex;
    std::condition_variable& finished;
    bool& done;
    std::thread& watchdog;
    ~Join() {
      {
        std::lock_guard guard(mutex);
        done = true;
      }
      finished.notify_all();
      watchdog.join();
    }
  } join{mutex, finished, done, watchdog};
  RunSql(health_connection_.get(), health_query_);
}

warpsql::BackendCapabilities DuckDBBackend::Capabilities() {
  return {.prepared_statements = true, .transactions = true, .ingest = true};
}

json::object DuckDBBackend::ServerInfo() {
  // server_read_only: the database file is opened read-only (spec §10); duckdb_version: the
  // engine's own key (spec §2.7).
  return {{"server_read_only", read_only_}, {"duckdb_version", version_}};
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
  return warpsql::Principal{std::string(username), "admin", "password", {}, {}, {}};
}

warpsql::Principal StaticPasswordAuthenticator::AuthenticateCertificate(
    std::string_view identity, const warpsql::ClientCertificate&) {
  for (const auto& allowed : certificate_identities_) {
    if (warpsql::CertificateIdentitiesMatch(allowed, identity)) {
      return warpsql::Principal{allowed, "admin", "client_certificate", {}, {}, {}};
    }
  }
  // A URI identity (a SPIFFE ID) differing from an allowed one only in case is another
  // identity: URI paths are case-sensitive.
  for (const auto& allowed : certificate_identities_) {
    if (warpsql::IsUriIdentity(identity) && allowed.size() == identity.size() &&
        std::equal(allowed.begin(), allowed.end(), identity.begin(), [](char a, char b) {
          return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b));
        })) {
      throw Error(ErrorCode::kUnknownCertificateIdentity,
                  "the client certificate's URI identity '" + std::string(identity) + "' differs from '" + allowed +
                      "' only in case; URI identities must match exactly");
    }
  }
  throw Error(ErrorCode::kUnknownCertificateIdentity,
              "the client certificate's identity '" + std::string(identity) +
                  "' may not sign in on this server");
}

}  // namespace warpduckdb
