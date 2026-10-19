// Tests of the client library: unit tests, and live tests against a real warp-duckdb
// (started here, on a free port; WARPSQL_TEST_SERVER_BINARY names another binary). A
// server that doesn't start fails the tests; nothing is skipped.
#include <gtest/gtest.h>

#include <algorithm>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <boost/asio/ip/tcp.hpp>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "warpsql/client.hpp"

#ifdef _WIN32
#include <process.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
#endif

namespace wc = warpsql::client;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::string Base64(const std::string& bytes) {
  static constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string out;
  size_t i = 0;
  for (; i + 2 < bytes.size(); i += 3) {
    const uint32_t n = (uint8_t(bytes[i]) << 16) | (uint8_t(bytes[i + 1]) << 8) | uint8_t(bytes[i + 2]);
    for (int s = 18; s >= 0; s -= 6) out += kAlphabet[(n >> s) & 63];
  }
  if (i + 1 == bytes.size()) {
    const uint32_t n = uint8_t(bytes[i]) << 16;
    out += kAlphabet[(n >> 18) & 63];
    out += kAlphabet[(n >> 12) & 63];
    out += "==";
  } else if (i + 2 == bytes.size()) {
    const uint32_t n = (uint8_t(bytes[i]) << 16) | (uint8_t(bytes[i + 1]) << 8);
    out += kAlphabet[(n >> 18) & 63];
    out += kAlphabet[(n >> 12) & 63];
    out += kAlphabet[(n >> 6) & 63];
    out += '=';
  }
  return out;
}

std::string SchemaStream(const std::shared_ptr<arrow::Schema>& schema) {
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
  EXPECT_TRUE(writer->Close().ok());
  return sink->Finish().ValueOrDie()->ToString();
}

int ProcessId() {
#ifdef _WIN32
  return _getpid();
#else
  return ::getpid();
#endif
}

uint16_t FreePort() {
  boost::asio::io_context io;
  boost::asio::ip::tcp::acceptor acceptor(io, {boost::asio::ip::make_address("127.0.0.1"), 0});
  return acceptor.local_endpoint().port();
}

// A certificate signed by `issuer_cert`/`issuer_key` (self-signed when they are null),
// with the extensions TLS libraries expect, written to `cert_path` and `key_path`.
struct Issued {
  X509* cert;
  EVP_PKEY* key;
};

Issued Issue(const std::string& common_name, const Issued* issuer, const std::vector<std::string>& extensions,
             const fs::path& cert_path, const fs::path& key_path) {
  EVP_PKEY* key = EVP_RSA_gen(2048);
  X509* cert = X509_new();
  X509_set_version(cert, 2);
  static long serial = 1;
  ASN1_INTEGER_set(X509_get_serialNumber(cert), serial++ + (std::rand() & 0xFFFF) * 1000);
  X509_gmtime_adj(X509_getm_notBefore(cert), -60);
  X509_gmtime_adj(X509_getm_notAfter(cert), 2 * 24 * 3600);
  X509_set_pubkey(cert, key);
  X509_NAME* name = X509_get_subject_name(cert);
  X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char*>(common_name.c_str()),
                             -1, -1, 0);
  X509* issuer_cert = issuer != nullptr ? issuer->cert : cert;
  EVP_PKEY* issuer_key = issuer != nullptr ? issuer->key : key;
  X509_set_issuer_name(cert, X509_get_subject_name(issuer_cert));
  X509V3_CTX ctx;
  X509V3_set_ctx_nodb(&ctx);
  X509V3_set_ctx(&ctx, issuer_cert, cert, nullptr, nullptr, 0);
  for (const auto& extension : extensions) {
    const auto eq = extension.find('=');
    X509_EXTENSION* ext = X509V3_EXT_conf(nullptr, &ctx, extension.substr(0, eq).c_str(), extension.substr(eq + 1).c_str());
    EXPECT_NE(ext, nullptr) << extension;
    if (ext != nullptr) {
      X509_add_ext(cert, ext, -1);
      X509_EXTENSION_free(ext);
    }
  }
  EXPECT_GT(X509_sign(cert, issuer_key, EVP_sha256()), 0);
  FILE* out = std::fopen(cert_path.string().c_str(), "w");
  PEM_write_X509(out, cert);
  std::fclose(out);
  out = std::fopen(key_path.string().c_str(), "w");
  PEM_write_PrivateKey(out, key, nullptr, nullptr, 0, nullptr, nullptr);
  std::fclose(out);
  return {cert, key};
}

// A CA, a server certificate it signs (for `names`, a subjectAltName list), a client
// certificate it signs, and an unrelated CA, made with OpenSSL's library (the openssl
// command differs between machines: LibreSSL on macOS).
// A CRL of `ca`'s, valid for a day, revoking the certificate with `serial`.
void WriteCrl(const Issued& ca, const ASN1_INTEGER* serial, const fs::path& path) {
  X509_CRL* crl = X509_CRL_new();
  X509_CRL_set_version(crl, 1);
  X509_CRL_set_issuer_name(crl, X509_get_subject_name(ca.cert));
  ASN1_TIME* now = ASN1_TIME_adj(nullptr, std::time(nullptr), 0, -60);
  ASN1_TIME* next = ASN1_TIME_adj(nullptr, std::time(nullptr), 1, 0);
  X509_CRL_set1_lastUpdate(crl, now);
  X509_CRL_set1_nextUpdate(crl, next);
  X509_REVOKED* revoked = X509_REVOKED_new();
  X509_REVOKED_set_serialNumber(revoked, const_cast<ASN1_INTEGER*>(serial));
  X509_REVOKED_set_revocationDate(revoked, now);
  X509_CRL_add0_revoked(crl, revoked);
  X509_CRL_sort(crl);
  EXPECT_GT(X509_CRL_sign(crl, ca.key, EVP_sha256()), 0);
  FILE* out = std::fopen(path.string().c_str(), "w");
  PEM_write_X509_CRL(out, crl);
  std::fclose(out);
  ASN1_TIME_free(now);
  ASN1_TIME_free(next);
  X509_CRL_free(crl);
}

struct Certificates {
  fs::path dir;
  fs::path ca, server_cert, server_key, client_cert, client_key, other_ca;
  // A second client certificate from the same CA, for another identity.
  fs::path other_client_cert, other_client_key;
  // The CA's CRL, revoking other_client_cert only.
  fs::path crl;

  static Certificates Make(const std::string& names) {
    Certificates c;
    c.dir = fs::temp_directory_path() / ("warpsql-client-certs-" + std::to_string(ProcessId()) + "-" +
                                         std::to_string(std::rand()));
    fs::create_directories(c.dir);
    c.ca = c.dir / "ca.pem";
    c.server_cert = c.dir / "server.pem";
    c.server_key = c.dir / "server.key";
    c.client_cert = c.dir / "client.pem";
    c.client_key = c.dir / "client.key";
    c.other_ca = c.dir / "other-ca.pem";
    c.other_client_cert = c.dir / "other-client.pem";
    c.other_client_key = c.dir / "other-client.key";
    c.crl = c.dir / "ca.crl";
    const std::vector<std::string> ca_extensions{"basicConstraints=critical,CA:TRUE",
                                                 "keyUsage=critical,keyCertSign,cRLSign",
                                                 "subjectKeyIdentifier=hash"};
    Issued ca = Issue("test-ca", nullptr, ca_extensions, c.ca, c.dir / "ca.key");
    Issued other = Issue("other-ca", nullptr, ca_extensions, c.other_ca, c.dir / "other-ca.key");
    Issued server = Issue("server", &ca,
                          {"basicConstraints=CA:FALSE", "keyUsage=critical,digitalSignature,keyEncipherment",
                           "extendedKeyUsage=serverAuth", "subjectAltName=" + names,
                           "authorityKeyIdentifier=keyid"},
                          c.server_cert, c.server_key);
    Issued client = Issue("client", &ca,
                          {"basicConstraints=CA:FALSE", "keyUsage=critical,digitalSignature,keyEncipherment",
                           "extendedKeyUsage=clientAuth", "subjectAltName=email:client@example.com",
                           "authorityKeyIdentifier=keyid"},
                          c.client_cert, c.client_key);
    Issued other_client = Issue("other-client", &ca,
                                {"basicConstraints=CA:FALSE", "keyUsage=critical,digitalSignature,keyEncipherment",
                                 "extendedKeyUsage=clientAuth", "subjectAltName=email:other@example.com",
                                 "authorityKeyIdentifier=keyid"},
                                c.other_client_cert, c.other_client_key);
    WriteCrl(ca, X509_get_serialNumber(other_client.cert), c.crl);
    for (Issued* issued : {&ca, &other, &server, &client, &other_client}) {
      X509_free(issued->cert);
      EVP_PKEY_free(issued->key);
    }
    return c;
  }
};

// A warp-duckdb process for the tests.
class Server {
 public:
  explicit Server(std::vector<std::string> extra = {}, bool tls = false) : tls_(tls) {
    const char* binary = std::getenv("WARPSQL_TEST_SERVER_BINARY");
    binary_ = binary != nullptr && *binary != '\0' ? binary : WARPSQL_TEST_DEFAULT_SERVER;
    port_ = FreePort();
    std::vector<std::string> args{binary_, "--host", "127.0.0.1", "--port", std::to_string(port_)};
    args.insert(args.end(), extra.begin(), extra.end());
    log_ = fs::temp_directory_path() / ("warpsql-client-test-" + std::to_string(port_) + ".log");
#ifdef _WIN32
    // The command line, each argument quoted (none here holds a quote or ends in '\\').
    std::wstring command;
    for (const auto& a : args) command += L"\"" + std::wstring(a.begin(), a.end()) + L"\" ";
    // This process's environment without WARP_DUCKDB_*, plus the password.
    std::wstring block = L"WARP_DUCKDB_PASSWORD=" + std::wstring(password_.begin(), password_.end());
    block.push_back(L'\0');
    if (wchar_t* env = GetEnvironmentStringsW()) {
      for (const wchar_t* e = env; *e != L'\0'; e += wcslen(e) + 1) {
        if (std::wstring_view(e).starts_with(L"WARP_DUCKDB_")) continue;
        block.append(e);
        block.push_back(L'\0');
      }
      FreeEnvironmentStringsW(env);
    }
    block.push_back(L'\0');
    SECURITY_ATTRIBUTES inherit{sizeof(inherit), nullptr, TRUE};
    HANDLE log = CreateFileW(log_.wstring().c_str(), GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, &inherit,
                             CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
    startup.hStdOutput = log;
    startup.hStdError = log;
    PROCESS_INFORMATION info{};
    if (CreateProcessW(nullptr, command.data(), nullptr, nullptr, TRUE, CREATE_UNICODE_ENVIRONMENT, block.data(),
                       nullptr, &startup, &info)) {
      CloseHandle(info.hThread);
      process_ = info.hProcess;
    }
    if (log != INVALID_HANDLE_VALUE) CloseHandle(log);
#else
    std::vector<char*> argv;
    for (auto& a : args) argv.push_back(a.data());
    argv.push_back(nullptr);
    std::vector<std::string> env_strings{"WARP_DUCKDB_PASSWORD=" + password_};
    for (char** e = environ; *e != nullptr; e++) {
      if (std::string_view(*e).starts_with("WARP_DUCKDB_")) continue;
      env_strings.emplace_back(*e);
    }
    std::vector<char*> envp;
    for (auto& e : env_strings) envp.push_back(e.data());
    envp.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, 1, log_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_adddup2(&actions, 1, 2);
    if (posix_spawn(&pid_, binary_.c_str(), &actions, nullptr, argv.data(), envp.data()) != 0) pid_ = -1;
    posix_spawn_file_actions_destroy(&actions);
#endif
  }

  ~Server() {
#ifdef _WIN32
    if (process_ != nullptr) {
      TerminateProcess(process_, 1);
      WaitForSingleObject(process_, INFINITE);
      CloseHandle(process_);
    }
#else
    if (pid_ > 0) {
      kill(pid_, SIGKILL);
      waitpid(pid_, nullptr, 0);
    }
#endif
  }

  bool Started() const {
#ifdef _WIN32
    return process_ != nullptr;
#else
    return pid_ > 0;
#endif
  }

  // Whether the process started and hasn't exited.
  bool Running() {
#ifdef _WIN32
    return process_ != nullptr && WaitForSingleObject(process_, 0) == WAIT_TIMEOUT;
#else
    if (pid_ <= 0) return false;
    int status = 0;
    if (waitpid(pid_, &status, WNOHANG) == pid_) {
      pid_ = -1;
      return false;
    }
    return true;
#endif
  }

  // Waits until the server answers GET /warp.
  ::testing::AssertionResult WaitReady(const wc::TlsOptions& tls = {}) {
    if (!Started()) return ::testing::AssertionFailure() << "cannot start " << binary_;
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (!Running()) {
        return ::testing::AssertionFailure() << "server exited:\n" << Log();
      }
      try {
        wc::Client client(Options(tls));
        client.Discover();
        return ::testing::AssertionSuccess();
      } catch (const wc::Error&) {
        std::this_thread::sleep_for(50ms);
      }
    }
    return ::testing::AssertionFailure() << "server did not come up:\n" << Log();
  }

  wc::ConnectOptions Options(const wc::TlsOptions& tls = {}) const {
    wc::ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = port_;
    options.tls = tls;
    if (tls_ && !options.tls.enabled) {
      options.tls.enabled = true;
      options.tls.skip_verify = true;
    }
    return options;
  }

  std::unique_ptr<wc::Client> SignedIn() const {
    auto client = std::make_unique<wc::Client>(Options());
    client->SignInWithPassword(username_, password_);
    return client;
  }

  std::string Log() const {
    std::ifstream in(log_);
    return std::string(std::istreambuf_iterator<char>(in), {});
  }

  const std::string& username() const { return username_; }
  const std::string& password() const { return password_; }
  uint16_t port() const { return port_; }

 private:
  std::string binary_;
  uint16_t port_ = 0;
#ifdef _WIN32
  HANDLE process_ = nullptr;
#else
  pid_t pid_ = -1;
#endif
  bool tls_;
  fs::path log_;
  std::string username_ = "warp";
  std::string password_ = "p:ss@word;" + std::to_string(ProcessId());
};

int64_t Scalar(wc::Session& session, const std::string& sql) {
  auto result = session.Execute(sql);
  EXPECT_TRUE(result.HasRows()) << sql;
  auto table = result.reader->ToTable();
  return std::stoll(table->column(0)->GetScalar(0).ValueOrDie()->ToString());
}

class Live : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    server_ = new Server();
    ASSERT_TRUE(server_->WaitReady());
  }
  static void TearDownTestSuite() {
    delete server_;
    server_ = nullptr;
  }
  void SetUp() override {
    ASSERT_NE(server_, nullptr);
    client_ = server_->SignedIn();
    session_ = client_->CreateSession();
  }
  static Server* server_;
  std::unique_ptr<wc::Client> client_;
  std::unique_ptr<wc::Session> session_;
};

Server* Live::server_ = nullptr;

}  // namespace

// ---- Unit tests ----

TEST(ClientUnit, DecodesAnEmbeddedSchema) {
  auto schema = arrow::schema({arrow::field("a", arrow::int32()), arrow::field("b", arrow::utf8(), false)});
  auto decoded = wc::DecodeSchema(Base64(SchemaStream(schema)));
  ASSERT_NE(decoded, nullptr);
  EXPECT_TRUE(decoded->Equals(*schema)) << decoded->ToString();
  EXPECT_EQ(wc::DecodeSchema(""), nullptr);
}

TEST(ClientUnit, RefusesASchemaThatIsNotBase64) {
  try {
    wc::DecodeSchema("not base64!");
    FAIL() << "accepted";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "client_protocol_error");
    EXPECT_NE(std::string(e.what()).find("not base64"), std::string::npos) << e.what();
  }
}

TEST(ClientUnit, ReadsAProblemDocument) {
  auto problem = boost::json::parse(R"({"type":"urn:warp-sql:error:session_ended","status":401,
    "code":"session_ended","detail":"killed by an administrator","reason":"killed",
    "sqlstate":"57P01","request_id":"r_1"})").as_object();
  auto error = wc::Error::FromProblem(0, problem);
  EXPECT_EQ(error.status(), 401);
  EXPECT_EQ(error.code(), "session_ended");
  EXPECT_EQ(error.reason(), "killed");
  EXPECT_EQ(error.sqlstate(), "57P01");
  EXPECT_EQ(error.request_id(), "r_1");
  EXPECT_STREQ(error.what(), "killed by an administrator");
  EXPECT_FALSE(error.IsTransport());
  EXPECT_FALSE(error.IsCancelled());
  // The HTTP status wins over the document's; a missing code is the client's own, named by
  // the status (spec §11: client-made codes begin with client_).
  auto bare = wc::Error::FromProblem(503, boost::json::object{{"status", 500}});
  EXPECT_EQ(bare.status(), 503);
  EXPECT_EQ(bare.code(), "client_http_503");
  // A cancelled statement's problem document carries its cancel_reason (spec §6.1).
  auto cancelled = wc::Error::FromProblem(409, boost::json::parse(R"({"status":409,"code":"statement_cancelled",
    "detail":"cancelled","cancel_reason":"client"})").as_object());
  EXPECT_EQ(cancelled.cancel_reason(), "client");
  EXPECT_TRUE(cancelled.IsCancelled());
}

TEST(ClientUnit, BaseUrls) {
  wc::ConnectOptions options;
  options.host = "db.example.com";
  options.port = 443;
  options.tls.enabled = true;
  EXPECT_EQ(wc::BaseUrl(options), "https://db.example.com:443/warp");
  options.host = "::1";
  options.tls.enabled = false;
  options.port = 31337;
  EXPECT_EQ(wc::BaseUrl(options), "http://[::1]:31337/warp");
}

TEST(ClientUnit, TotalRowsNeedsEveryPartitionCounted) {
  wc::StatementDocument doc;
  EXPECT_FALSE(doc.TotalRows().has_value());
  doc.partitions.push_back({0, "", 2, 10});
  doc.partitions.push_back({1, "", 1, 5});
  EXPECT_EQ(doc.TotalRows(), 15);
  doc.partitions.push_back({2, "", std::nullopt, std::nullopt});
  EXPECT_FALSE(doc.TotalRows().has_value());
}

TEST(ClientUnit, MissingTlsFilesAreReportedBeforeConnecting) {
  wc::ConnectOptions options;
  options.tls.enabled = true;
  options.tls.root_certs_file = "/nonexistent/ca.pem";
  try {
    wc::Client client(options);
    FAIL() << "accepted";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "client_tls_failed");
    EXPECT_NE(std::string(e.what()).find("/nonexistent/ca.pem"), std::string::npos) << e.what();
  }
  options.tls.root_certs_file.clear();
  options.tls.cert_file = "/nonexistent/client.pem";
  try {
    wc::Client client(options);
    FAIL() << "accepted a certificate without its key";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "client_tls_failed");
    EXPECT_NE(std::string(e.what()).find("both"), std::string::npos) << e.what();
  }
}

TEST(ClientUnit, ConnectionRefusedIsATransportError) {
  wc::ConnectOptions options;
  options.host = "127.0.0.1";
  options.port = FreePort();  // nothing listens there
  wc::Client client(options);
  try {
    client.Discover();
    FAIL() << "connected";
  } catch (const wc::Error& e) {
    EXPECT_TRUE(e.IsTransport());
    EXPECT_EQ(e.code(), "client_connection_failed");
    EXPECT_NE(std::string(e.what()).find("could not connect to http://127.0.0.1:"), std::string::npos) << e.what();
  }
}

// ---- Live tests ----

TEST_F(Live, DiscoversTheServer) {
  wc::Client anonymous(server_->Options());
  auto discovery = anonymous.Discover();
  EXPECT_FALSE(discovery.server_name.empty());
  EXPECT_TRUE(discovery.Has("prepared_statements"));
  EXPECT_TRUE(discovery.Has("ingest"));
  EXPECT_FALSE(discovery.Has("no_such_capability"));
  EXPECT_EQ(anonymous.Health().at("status").as_string(), "serving");
}

TEST_F(Live, WrongPasswordIsRefusedAndTheRightOneSignsIn) {
  wc::Client client(server_->Options());
  try {
    client.SignInWithPassword(server_->username(), "wrong");
    FAIL() << "signed in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 401);
    EXPECT_EQ(e.code(), "invalid_credentials");
  }
  EXPECT_FALSE(client.SignedIn());
  client.SignInWithPassword(server_->username(), server_->password());
  EXPECT_TRUE(client.SignedIn());
}

TEST_F(Live, ASessionNeedsAToken) {
  wc::Client anonymous(server_->Options());
  try {
    anonymous.CreateSession();
    FAIL() << "made a session without signing in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 401);
    EXPECT_EQ(e.code(), "invalid_token");
  }
  EXPECT_NO_THROW(client_->CreateSession());
}

TEST_F(Live, StreamsRowsInBatches) {
  auto result = session_->Execute("SELECT i, 'row ' || i AS label FROM range(300000) t(i)");
  ASSERT_TRUE(result.HasRows());
  EXPECT_FALSE(result.statement_id.empty());
  EXPECT_EQ(result.reader->schema()->field(0)->name(), "i");
  int64_t rows = 0;
  int batches = 0;
  while (auto batch = result.reader->Next()) {
    rows += batch->num_rows();
    batches++;
  }
  EXPECT_EQ(rows, 300000);
  EXPECT_GT(batches, 1);
  EXPECT_EQ(result.reader->Next(), nullptr);  // stays at the end
  // The connection is reused for the next statement.
  EXPECT_EQ(Scalar(*session_, "SELECT 7"), 7);
}

TEST_F(Live, AStatementWithoutRowsAnswersADocument) {
  auto created = session_->Execute("CREATE TABLE IF NOT EXISTS docs (x INTEGER)");
  ASSERT_FALSE(created.HasRows());
  EXPECT_EQ(created.document->result_kind, "update");
  auto inserted = session_->Execute("INSERT INTO docs VALUES (1), (2), (3)");
  ASSERT_TRUE(inserted.document.has_value());
  EXPECT_EQ(inserted.document->rows_affected, 3);
  EXPECT_EQ(Scalar(*session_, "SELECT count(*) FROM docs"), 3);
}

TEST_F(Live, SqlErrorsCarryTheirCodeAndSqlstate) {
  try {
    session_->Execute("SELECT * FROM no_such_table_xyz");
    FAIL() << "ran";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 400);
    EXPECT_EQ(e.code(), "sql_error");
    EXPECT_EQ(e.sqlstate(), "42P01");
    EXPECT_NE(std::string(e.what()).find("no_such_table_xyz"), std::string::npos) << e.what();
  }
  EXPECT_EQ(Scalar(*session_, "SELECT 1"), 1);  // the session goes on
}

TEST_F(Live, AFailureInTheMiddleOfAStreamIsTheStatementsError) {
  // Batches stream before the error: the body ends without the end-of-stream marker
  // (spec §6.6) and the reader reports the statement document's error.
  session_->Execute("SET threads = 1");  // in order, so the first batches come before the error
  const std::string failing =
      "SELECT CASE WHEN i < 3000000 THEN i ELSE error('boom at ' || i) END AS v FROM range(4000000) t(i)";
  // Split (the default): the error ends the reader too, never a clean end.
  try {
    auto split = session_->Execute(failing);
    while (auto batch = split.reader->Next()) {
    }
    FAIL() << "the split result ended normally";
  } catch (const wc::Error& e) {
    EXPECT_NE(std::string(e.what()).find("boom at"), std::string::npos) << e.code() << ": " << e.what();
  }
  wc::StatementOptions one_stream;
  one_stream.result_partitions = 1;
  auto result = session_->Execute(failing, one_stream);
  ASSERT_TRUE(result.HasRows());
  int64_t rows = 0;
  try {
    while (auto batch = result.reader->Next()) rows += batch->num_rows();
    FAIL() << "the stream ended normally after " << rows << " rows";
  } catch (const wc::Error& e) {
    EXPECT_NE(std::string(e.what()).find("boom at"), std::string::npos) << e.code() << ": " << e.what();
    EXPECT_FALSE(e.IsTransport()) << e.what();
    EXPECT_GT(rows, 0) << "the error came before any batch";
  }
  EXPECT_EQ(Scalar(*session_, "SELECT 2"), 2);
  session_->Execute("RESET threads");
}

TEST_F(Live, BindsParametersByPositionAndByName) {
  wc::StatementOptions positional;
  positional.parameters = boost::json::array{40, 2};
  auto sum = session_->Execute("SELECT ?::INTEGER + ?::INTEGER AS s", positional).reader->ToTable();
  EXPECT_EQ(sum->column(0)->GetScalar(0).ValueOrDie()->ToString(), "42");
  wc::StatementOptions named;
  named.parameters = boost::json::object{{"word", "it's"}};
  auto text = session_->Execute("SELECT $word AS w", named).reader->ToTable();
  EXPECT_EQ(text->column(0)->GetScalar(0).ValueOrDie()->ToString(), "it's");
}

TEST_F(Live, TheStatusDocumentPath) {
  wc::StatementOptions options;
  options.retain_result = true;
  auto doc = session_->Submit("SELECT i FROM range(1000) t(i)", 0, options);
  EXPECT_FALSE(doc.statement_id.empty());
  doc = session_->Wait(doc.statement_id);
  ASSERT_EQ(doc.state, "succeeded");
  EXPECT_EQ(doc.result_kind, "rows");
  ASSERT_NE(doc.schema, nullptr);
  EXPECT_EQ(doc.schema->field(0)->name(), "i");
  EXPECT_EQ(doc.TotalRows(), 1000);
  EXPECT_EQ(session_->FetchResult(doc.statement_id)->ToTable()->num_rows(), 1000);
  // A retained result reads again.
  EXPECT_EQ(session_->FetchResult(doc.statement_id)->ToTable()->num_rows(), 1000);
  session_->DeleteStatement(doc.statement_id);
  try {
    session_->Status(doc.statement_id);
    FAIL() << "a deleted statement is still there";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 404);
  }
}

TEST_F(Live, PreparedStatementsWithJsonAndArrowParameters) {
  session_->Execute("CREATE OR REPLACE TABLE prepared_t (id INTEGER, name VARCHAR)");
  auto insert = session_->Prepare("INSERT INTO prepared_t VALUES (?, ?)");
  ASSERT_NE(insert->parameter_schema(), nullptr);
  EXPECT_EQ(insert->parameter_schema()->num_fields(), 2);
  wc::StatementOptions one;
  one.parameters = boost::json::array{1, "one"};
  insert->Execute(one);
  // Each row of an Arrow body is one execution.
  arrow::Int32Builder ids;
  arrow::StringBuilder names;
  ASSERT_TRUE(ids.AppendValues({2, 3, 4}).ok());
  ASSERT_TRUE(names.AppendValues(std::vector<std::string>{"two", "three", "four"}).ok());
  auto batch = arrow::RecordBatch::Make(insert->parameter_schema(), 3,
                                        {ids.Finish().ValueOrDie(), names.Finish().ValueOrDie()});
  auto reader = arrow::RecordBatchReader::Make({batch}).ValueOrDie();
  insert->Execute(*reader);
  insert->Close();
  EXPECT_EQ(Scalar(*session_, "SELECT count(*) FROM prepared_t"), 4);
  auto query = session_->Prepare("SELECT name FROM prepared_t WHERE id = ?");
  EXPECT_EQ(query->result_kind(), "rows");
  wc::StatementOptions three;
  three.parameters = boost::json::array{3};
  auto table = query->Execute(three).reader->ToTable();
  EXPECT_EQ(table->column(0)->GetScalar(0).ValueOrDie()->ToString(), "three");
}

TEST_F(Live, IngestCreatesAppendsAndReplaces) {
  auto make = [](std::vector<int64_t> values) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.AppendValues(values).ok());
    auto schema = arrow::schema({arrow::field("v", arrow::int64())});
    auto batch = arrow::RecordBatch::Make(schema, static_cast<int64_t>(values.size()), {builder.Finish().ValueOrDie()});
    return arrow::RecordBatchReader::Make({batch, batch}).ValueOrDie();
  };
  session_->Execute("DROP TABLE IF EXISTS ingested");
  wc::IngestOptions create;
  create.mode = "create";
  EXPECT_EQ(session_->Ingest("ingested", *make({1, 2, 3}), create), 6);
  try {
    session_->Ingest("ingested", *make({9}), create);
    FAIL() << "created a table that exists";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.sqlstate(), "42P07") << e.code() << ": " << e.what();
  }
  wc::IngestOptions append;
  append.mode = "append";
  EXPECT_EQ(session_->Ingest("ingested", *make({4}), append), 2);
  EXPECT_EQ(Scalar(*session_, "SELECT count(*) FROM ingested"), 8);
  wc::IngestOptions replace;
  replace.mode = "replace";
  EXPECT_EQ(session_->Ingest("ingested", *make({5}), replace), 2);
  EXPECT_EQ(Scalar(*session_, "SELECT sum(v) FROM ingested"), 10);
  wc::IngestOptions temporary;
  temporary.mode = "replace";
  temporary.temporary = true;
  EXPECT_EQ(session_->Ingest("ingested_temp", *make({7}), temporary), 2);
  EXPECT_EQ(Scalar(*session_, "SELECT sum(v) FROM ingested_temp"), 14);
  // Temporary: another session doesn't see it.
  auto other = client_->CreateSession();
  EXPECT_THROW(other->Execute("SELECT * FROM ingested_temp"), wc::Error);
}

TEST_F(Live, ALargeIngestGoesInPartsAndCommitsOnce) {
  // 64 batches over 4 parts (spec §8.3): every row once, nothing visible before the commit.
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  auto schema = arrow::schema({arrow::field("v", arrow::int64()), arrow::field("s", arrow::utf8())});
  int64_t next = 0;
  for (int b = 0; b < 64; b++) {
    arrow::Int64Builder values;
    arrow::StringBuilder labels;
    for (int i = 0; i < 10000; i++, next++) {
      EXPECT_TRUE(values.Append(next).ok());
      EXPECT_TRUE((next % 9 == 0 ? labels.AppendNull() : labels.Append("v" + std::to_string(next))).ok());
    }
    batches.push_back(arrow::RecordBatch::Make(schema, 10000, {values.Finish().ValueOrDie(), labels.Finish().ValueOrDie()}));
  }
  session_->Execute("DROP TABLE IF EXISTS ingested_parts");
  wc::IngestOptions options;
  options.mode = "create";
  options.parts = 4;
  EXPECT_EQ(session_->Ingest("ingested_parts", *arrow::RecordBatchReader::Make(batches).ValueOrDie(), options), 640000);
  EXPECT_EQ(Scalar(*session_, "SELECT count(DISTINCT v) FROM ingested_parts"), 640000);
  EXPECT_EQ(Scalar(*session_, "SELECT sum(v) FROM ingested_parts"), 640000LL * 639999 / 2);
  EXPECT_EQ(Scalar(*session_, "SELECT count(s) FROM ingested_parts"), 640000 - 71112);
  // A failing partitioned ingest leaves nothing: create into a table that exists.
  try {
    session_->Ingest("ingested_parts", *arrow::RecordBatchReader::Make(batches).ValueOrDie(), options);
    FAIL() << "created a table that exists";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.sqlstate(), "42P07") << e.code() << ": " << e.what();
  }
  EXPECT_EQ(Scalar(*session_, "SELECT count(*) FROM ingested_parts"), 640000);
  // parts = 1 keeps the upload's order.
  options.parts = 1;
  options.mode = "replace";
  EXPECT_EQ(session_->Ingest("ingested_parts", *arrow::RecordBatchReader::Make(batches).ValueOrDie(), options), 640000);
  auto first = session_->Execute("SELECT v FROM ingested_parts LIMIT 3").reader->ToTable();
  EXPECT_EQ(first->column(0)->ToString(), arrow::ChunkedArray(arrow::ArrayVector{
      std::static_pointer_cast<arrow::Int64Array>(batches[0]->column(0))->Slice(0, 3)}).ToString());
}

TEST_F(Live, CancelRunningFromAnotherThreadCancelsTheStatement) {
  // A query that runs for minutes unless cancelled.
  const std::string slow = "SELECT count(*) FROM range(100000000000) a(i) WHERE i % 7 = 3";
  std::atomic<bool> stop{false};
  std::thread canceller([&] {
    std::this_thread::sleep_for(500ms);
    while (!stop.load()) {
      session_->CancelRunning();
      std::this_thread::sleep_for(200ms);
    }
  });
  const auto start = std::chrono::steady_clock::now();
  try {
    auto result = session_->Execute(slow);
    if (result.HasRows()) result.reader->ToTable();
    ADD_FAILURE() << "the query finished";
  } catch (const wc::Error& e) {
    EXPECT_TRUE(e.IsCancelled()) << e.code() << ": " << e.what();
  }
  stop.store(true);
  canceller.join();
  EXPECT_LT(std::chrono::steady_clock::now() - start, 30s);
  // The session goes on, and a statement that isn't cancelled finishes.
  EXPECT_EQ(Scalar(*session_, "SELECT count(*) FROM range(1000)"), 1000);
  session_->CancelRunning();  // nothing runs: a no-op
  EXPECT_EQ(Scalar(*session_, "SELECT 3"), 3);
}

TEST_F(Live, AnAbandonedResultLeavesTheSessionUsable) {
  auto result = session_->Execute("SELECT i FROM range(5000000) t(i)");
  ASSERT_TRUE(result.HasRows());
  bool more = false;
  auto first = result.reader->ToTable(10, &more);
  EXPECT_EQ(first->num_rows(), 10);
  EXPECT_TRUE(more);
  EXPECT_EQ(Scalar(*session_, "SELECT 5"), 5);
  // An exact fit leaves nothing over.
  auto exact = session_->Execute("SELECT i FROM range(10) t(i)").reader->ToTable(10, &more);
  EXPECT_EQ(exact->num_rows(), 10);
  EXPECT_FALSE(more);
}

// ---- Split results (spec §6.10) ----

namespace {

std::vector<int64_t> Int64s(wc::ResultReader& reader) {
  std::vector<int64_t> out;
  while (auto batch = reader.Next()) {
    auto values = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    for (int64_t i = 0; i < values->length(); i++) out.push_back(values->Value(i));
  }
  return out;
}

wc::StatementOptions Split(int partitions, bool any_order = false) {
  wc::StatementOptions options;
  options.result_partitions = partitions;
  options.any_order = any_order;
  return options;
}

}  // namespace

TEST_F(Live, ASplitResultKeepsItsOrder) {
  auto result = session_->Execute("SELECT i FROM range(1000000) t(i) ORDER BY i DESC", Split(4));
  ASSERT_TRUE(result.HasRows());
  auto values = Int64s(*result.reader);
  ASSERT_EQ(values.size(), 1000000u);
  for (int64_t i = 0; i < 1000000; i++) ASSERT_EQ(values[i], 999999 - i) << "at " << i;
  EXPECT_EQ(Scalar(*session_, "SELECT 3"), 3);  // the session goes on
}

TEST_F(Live, ASplitResultInAnyOrderHasEveryRowOnce) {
  auto result = session_->Execute("SELECT i FROM range(2000000) t(i)", Split(6, true));
  auto values = Int64s(*result.reader);
  std::sort(values.begin(), values.end());
  ASSERT_EQ(values.size(), 2000000u);
  for (int64_t i = 0; i < 2000000; i++) ASSERT_EQ(values[i], i);
}

TEST_F(Live, ASplitResultOnTheStatusDocumentPath) {
  wc::StatementOptions options = Split(3);
  options.retain_result = true;
  auto doc = session_->Submit("SELECT i FROM range(1000000) t(i) ORDER BY i", 0, options);
  doc = session_->Wait(doc.statement_id);
  ASSERT_GT(doc.partitions.size(), 1u);
  EXPECT_EQ(doc.partition_order, "interleaved");
  EXPECT_EQ(doc.TotalRows(), 1000000);
  int64_t rows = 0;
  for (const auto& partition : doc.partitions) {
    rows += session_->FetchResult(doc.statement_id, partition.index)->ToTable()->num_rows();
  }
  EXPECT_EQ(rows, 1000000);
}

TEST_F(Live, AFailureInOnePartitionFailsTheSplitResult) {
  auto result = session_->Execute(
      "SELECT CASE WHEN i < 30000000 THEN i ELSE error('boom in a split') END AS v FROM range(100000000) t(i)",
      Split(4));
  ASSERT_TRUE(result.HasRows());
  int64_t rows = 0;
  try {
    while (auto batch = result.reader->Next()) rows += batch->num_rows();
    FAIL() << "the split result ended normally after " << rows << " rows";
  } catch (const wc::Error& e) {
    EXPECT_NE(std::string(e.what()).find("boom in a split"), std::string::npos) << e.code() << ": " << e.what();
  }
  EXPECT_EQ(Scalar(*session_, "SELECT 4"), 4);
}

TEST_F(Live, ClosingASplitResultEarlyLeavesTheSessionUsable) {
  auto result = session_->Execute("SELECT i FROM range(3000000000) t(i)", Split(4, true));
  ASSERT_TRUE(result.HasRows());
  ASSERT_NE(result.reader->Next(), nullptr);
  result.reader->Close();
  EXPECT_EQ(result.reader->Next(), nullptr);
  EXPECT_EQ(Scalar(*session_, "SELECT 6"), 6);
}

TEST_F(Live, Metadata) {
  session_->Execute("CREATE OR REPLACE TABLE meta_t (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)");
  EXPECT_GT(session_->Catalogs()->num_rows(), 0);
  auto schemas = session_->DbSchemas("", "ma%");
  EXPECT_GT(schemas->num_rows(), 0);
  EXPECT_EQ(schemas->schema()->field(1)->name(), "db_schema_name");
  wc::TablesRequest request;
  request.table_name_filter_pattern = "meta_t";
  request.include_schema = true;
  auto tables = session_->Tables(request);
  ASSERT_EQ(tables->num_rows(), 1);
  ASSERT_EQ(tables->num_columns(), 5);
  EXPECT_EQ(tables->schema()->field(4)->name(), "table_schema");
  EXPECT_GT(session_->TableTypes()->num_rows(), 0);
  auto keys = session_->PrimaryKeys("", "", "meta_t");
  ASSERT_EQ(keys->num_rows(), 1);
  auto info = session_->ServerInfo();
  EXPECT_TRUE(info.contains("server_name"));
  EXPECT_EQ(std::string(info.at("server_arrow_version").as_string()).substr(0, 3), "26.");
}

TEST_F(Live, SessionOptionsAndTransactions) {
  auto doc = session_->Get();
  EXPECT_TRUE(doc.contains("options"));
  auto report = session_->SetOptions({{"schema", "no_such_schema_xyz"}});
  ASSERT_TRUE(report.contains("errors")) << boost::json::serialize(report);
  EXPECT_TRUE(report.at("errors").as_object().contains("schema"));
  session_->Execute("CREATE OR REPLACE TABLE tx_t (x INTEGER)");
  session_->SetOptions({{"autocommit", false}});
  session_->Execute("INSERT INTO tx_t VALUES (1)");
  session_->Rollback();
  EXPECT_EQ(Scalar(*session_, "SELECT count(*) FROM tx_t"), 0);
  session_->Execute("INSERT INTO tx_t VALUES (2)");
  session_->Commit();
  session_->SetOptions({{"autocommit", true}});
  EXPECT_EQ(Scalar(*session_, "SELECT count(*) FROM tx_t"), 1);
}

TEST_F(Live, AClosedSessionIsGone) {
  auto session = client_->CreateSession();
  const std::string id = session->id();
  session->Close();
  try {
    session->Execute("SELECT 1");
    FAIL() << "ran on a closed session";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "client_session_closed");
  }
}

// The token's lifetime is shorter than the client's refresh margin, so a password
// sign-in renews it before each request; the same token set by hand isn't renewed, and
// once it has expired the server refuses it.
TEST(LiveTokens, APasswordSignInRenewsItsToken) {
  Server server({"--token-lifetime-seconds", "2"});
  ASSERT_TRUE(server.WaitReady());
  auto renewing = server.SignedIn();
  auto session = renewing->CreateSession();
  wc::Client manual(server.Options());
  manual.SetAccessToken(renewing->access_token());
  EXPECT_NO_THROW(manual.CreateSession());  // still valid
  std::this_thread::sleep_for(3s);
  EXPECT_EQ(Scalar(*session, "SELECT 11"), 11);
  EXPECT_NO_THROW(renewing->CreateSession());
  try {
    manual.CreateSession();
    FAIL() << "an expired token made a session";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 401);
    EXPECT_EQ(e.code(), "invalid_token");
  }
}

// ---- TLS ----

class LiveTls : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { certs_ = new Certificates(Certificates::Make("DNS:localhost,IP:127.0.0.1")); }
  static void TearDownTestSuite() {
    std::error_code ec;
    fs::remove_all(certs_->dir, ec);
    delete certs_;
  }
  static Certificates* certs_;
};

Certificates* LiveTls::certs_ = nullptr;

TEST_F(LiveTls, VerifiesTheServerAgainstTheGivenRoots) {
  Server server({"--tls-cert", certs_->server_cert.string(), "--tls-key", certs_->server_key.string()}, true);
  ASSERT_TRUE(server.WaitReady());
  wc::TlsOptions trusted{.enabled = true, .root_certs_file = certs_->ca.string()};
  wc::Client good(server.Options(trusted));
  good.SignInWithPassword(server.username(), server.password());
  EXPECT_EQ(Scalar(*good.CreateSession(), "SELECT 1"), 1);
  // A CA that didn't sign it.
  wc::TlsOptions wrong{.enabled = true, .root_certs_file = certs_->other_ca.string()};
  wc::Client bad(server.Options(wrong));
  try {
    bad.Discover();
    FAIL() << "trusted a certificate its roots didn't sign";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "client_tls_failed");
    EXPECT_NE(std::string(e.what()).find("certificate verify failed"), std::string::npos) << e.what();
  }
  // Plain HTTP to a TLS server is not a Warp SQL answer.
  wc::ConnectOptions plain = server.Options();
  plain.tls = {};
  EXPECT_THROW(wc::Client(plain).Discover(), wc::Error);
  // skip_verify accepts it.
  wc::TlsOptions skip{.enabled = true, .skip_verify = true};
  EXPECT_NO_THROW(wc::Client(server.Options(skip)).Discover());
}

TEST_F(LiveTls, ChecksTheHostName) {
  Certificates other = Certificates::Make("DNS:db.example.com");
  Server server({"--tls-cert", other.server_cert.string(), "--tls-key", other.server_key.string()}, true);
  ASSERT_TRUE(server.WaitReady());
  wc::TlsOptions trusted{.enabled = true, .root_certs_file = other.ca.string()};
  try {
    wc::Client(server.Options(trusted)).Discover();
    FAIL() << "accepted a certificate for another host";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "client_tls_failed");
  }
  wc::TlsOptions skip{.enabled = true, .skip_verify = true};
  EXPECT_NO_THROW(wc::Client(server.Options(skip)).Discover());
  std::error_code ec;
  fs::remove_all(other.dir, ec);
}

TEST_F(LiveTls, MutualTls) {
  Server server({"--tls-cert", certs_->server_cert.string(), "--tls-key", certs_->server_key.string(),
                 "--tls-client-ca", certs_->ca.string()},
                true);
  wc::TlsOptions with_cert{.enabled = true,
                           .root_certs_file = certs_->ca.string(),
                           .cert_file = certs_->client_cert.string(),
                           .key_file = certs_->client_key.string()};
  ASSERT_TRUE(server.WaitReady(with_cert));
  wc::Client client(server.Options(with_cert));
  client.SignInWithPassword(server.username(), server.password());
  EXPECT_EQ(Scalar(*client.CreateSession(), "SELECT 1"), 1);
  // Without a client certificate the handshake (or, with TLS 1.3, the first read) fails,
  // as the refusal it is (spec §4.4's code), not as a failure to connect.
  wc::TlsOptions without{.enabled = true, .root_certs_file = certs_->ca.string()};
  try {
    wc::Client(server.Options(without)).Discover();
    FAIL() << "a client without a certificate got in";
  } catch (const wc::Error& e) {
    EXPECT_TRUE(e.IsTransport()) << e.code() << ": " << e.what();
    EXPECT_EQ(e.code(), "certificate_required") << e.what();
    EXPECT_EQ(std::string(e.what()), "the server at https://127.0.0.1:" + std::to_string(server.Options().port) +
                                         " requires a client certificate, and none was sent");
  }
  // Skipping verification of the server still presents the client's certificate (with
  // Arrow Flight it didn't, which is why GizmoSQL 1.x refused the combination).
  wc::TlsOptions skip_with_cert = with_cert;
  skip_with_cert.root_certs_file.clear();
  skip_with_cert.skip_verify = true;
  wc::Client skipping(server.Options(skip_with_cert));
  skipping.SignInWithPassword(server.username(), server.password());
  EXPECT_EQ(Scalar(*skipping.CreateSession(), "SELECT 2"), 2);
}

TEST_F(LiveTls, ARevokedClientCertificateIsReportedAsRevoked) {
  Server server({"--tls-cert", certs_->server_cert.string(), "--tls-key", certs_->server_key.string(),
                 "--tls-client-ca", certs_->ca.string(), "--mtls-crl-file", certs_->crl.string()},
                true);
  wc::TlsOptions good{.enabled = true,
                      .root_certs_file = certs_->ca.string(),
                      .cert_file = certs_->client_cert.string(),
                      .key_file = certs_->client_key.string()};
  ASSERT_TRUE(server.WaitReady(good));
  EXPECT_NO_THROW(wc::Client(server.Options(good)).Discover());  // the twin: not revoked
  wc::TlsOptions revoked = good;
  revoked.cert_file = certs_->other_client_cert.string();
  revoked.key_file = certs_->other_client_key.string();
  const auto options = server.Options(revoked);
  try {
    wc::Client(options).Discover();
    FAIL() << "a revoked certificate got in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 0);
    EXPECT_EQ(e.code(), "certificate_revoked") << e.what();
    EXPECT_EQ(std::string(e.what()), "the server at https://" + options.host + ":" + std::to_string(options.port) +
                                         " refused the client certificate: certificate revoked");
  }
}

TEST_F(LiveTls, SignsInWithTheClientCertificate) {
  Server server({"--tls-cert", certs_->server_cert.string(), "--tls-key", certs_->server_key.string(),
                 "--tls-client-ca", certs_->ca.string(), "--mtls-certificate-sign-in",
                 "--mtls-admin-identities", "client@example.com",
                 "--token-lifetime-seconds", "62"},
                true);
  wc::TlsOptions with_cert{.enabled = true,
                           .root_certs_file = certs_->ca.string(),
                           .cert_file = certs_->client_cert.string(),
                           .key_file = certs_->client_key.string()};
  ASSERT_TRUE(server.WaitReady(with_cert));
  wc::Client client(server.Options(with_cert));
  const wc::Discovery discovery = client.Discover();
  EXPECT_TRUE(discovery.Has("mtls_identity"));
  EXPECT_NE(std::ranges::find(discovery.grant_types, "client_certificate"), discovery.grant_types.end());
  client.SignInWithClientCertificate();
  auto session = client.CreateSession();
  EXPECT_EQ(Scalar(*session, "SELECT 7"), 7);
  // A split result: every partition is read on a connection of its own, each presenting
  // the certificate the token is bound to.
  wc::StatementOptions split;
  split.result_partitions = 4;
  auto result = session->Execute("SELECT i FROM range(1000000) t(i)", split);
  ASSERT_TRUE(result.HasRows());
  EXPECT_EQ(result.reader->ToTable()->num_rows(), 1000000);

  // The token is bound: over another certificate it is refused.
  wc::TlsOptions other_cert = with_cert;
  other_cert.cert_file = certs_->other_client_cert.string();
  other_cert.key_file = certs_->other_client_key.string();
  wc::Client thief(server.Options(other_cert));
  thief.SetAccessToken(client.access_token());
  try {
    thief.CreateSession();
    FAIL() << "a bound token worked over another certificate";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 401);
    EXPECT_EQ(e.code(), "certificate_mismatch");
  }
  // That certificate's own identity is not allowed to sign in.
  try {
    thief.SignInWithClientCertificate();
    FAIL() << "an unknown identity signed in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 401);
    EXPECT_EQ(e.code(), "unknown_certificate_identity");
  }
  // The token (62 s, renewed in its last minute) is renewed by signing in again with the
  // certificate: the renewed token differs.
  const std::string first = client.access_token();
  std::this_thread::sleep_for(3s);
  EXPECT_NE(client.access_token(), first);
  EXPECT_EQ(Scalar(*client.CreateSession(), "SELECT 8"), 8);
}

