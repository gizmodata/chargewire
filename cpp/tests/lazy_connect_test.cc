// Lazy connect to a stopped server (spec §3.1 `state`, §11.2). A connection pool opens
// connections on a timer, and each signs in; a sign-in at a stand-in for a stopped server
// starts the server. A client that sees "state": "stopped" in discovery therefore connects
// without signing in or opening a session, and does both at the first request that does work.
// A stub server here plays the stand-in.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>

#include "chargewire/client.hpp"
#include "stub_server.hpp"

namespace wc = chargewire::client;
using namespace std::chrono_literals;
using namespace chargewire_test_stub;

namespace {

const std::string kRoot = "/chargewire";
const std::string kSignIn = "/chargewire/v1/auth/token";
const std::string kNewSession = "/chargewire/v1/sessions";
const std::string kSession = "/chargewire/v1/sessions/s1";

std::string OneBatch() {
  arrow::Int64Builder builder;
  EXPECT_TRUE(builder.Append(42).ok());
  auto column = builder.Finish().ValueOrDie();
  auto schema = arrow::schema({arrow::field("answer", arrow::int64())});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
  EXPECT_TRUE(writer->WriteRecordBatch(*arrow::RecordBatch::Make(schema, 1, {column})).ok());
  EXPECT_TRUE(writer->Close().ok());
  return sink->Finish().ValueOrDie()->ToString();
}

Answer DiscoveryDocument(bool stopped) {
  boost::json::object document{
      {"protocol", "chargewire"},
      {"protocol_versions", boost::json::array{"1"}},
      {"server", {{"name", "stub"}, {"version", "0"}}},
      {"capabilities", boost::json::array{"prepared_statements", "transactions"}},
      {"auth", {{"grant_types", boost::json::array{"password"}}}}};
  if (stopped) document["state"] = "stopped";
  return {200, "application/json", boost::json::serialize(document), {}};
}

// A stand-in for a stopped server (`stopped`) or a server that serves: the first `resuming`
// sign-ins are answered server_resuming; a password other than "pw" is refused.
struct Stand {
  explicit Stand(bool stopped, int resuming = 0)
      : rows(OneBatch()), stub([this, stopped, resuming](const Seen& request, int nth) -> Answer {
          const std::string path = request.target.substr(0, request.target.find('?'));
          if (request.method == "GET" && path == kRoot) return DiscoveryDocument(stopped);
          if (request.method == "POST" && path == kSignIn) {
            if (nth < resuming) return Resuming("1");
            if (request.body.find(R"("password":"pw")") == std::string::npos) {
              return {401, "application/problem+json",
                      R"({"status":401,"code":"invalid_credentials","detail":"wrong user name or password"})", {}};
            }
            return Token();
          }
          if (request.method == "POST" && path == kNewSession) return Created();
          if (request.method == "POST" && path == kSession + "/statements") {
            return {200, "application/vnd.apache.arrow.stream", rows, {{"Chargewire-Statement-Id", "st1"}}};
          }
          if (request.method == "DELETE") return {204, "application/json", "", {}};
          return {404, "application/problem+json", R"({"status":404,"code":"not_found","detail":"no"})", {}};
        }) {}

  std::unique_ptr<wc::Client> Connect(const std::string& password = "pw", bool lazily = true) {
    auto options = stub.Options();
    options.connect_lazily_when_stopped = lazily;
    auto client = std::make_unique<wc::Client>(options);
    client->SignInWithPassword("joe", password);
    return client;
  }
  // Every request so far, as "METHOD /path".
  std::vector<std::string> Sent() {
    std::vector<std::string> sent;
    for (const auto& s : stub.Requests()) sent.push_back(s.method + " " + s.target.substr(0, s.target.find('?')));
    return sent;
  }

  std::string rows;
  StubServer stub;
};

int64_t Answer42(wc::Session& session) {
  auto result = session.Execute("SELECT 42 AS answer");
  EXPECT_TRUE(result.reader);
  auto table = result.reader->ToTable();
  return std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0))->Value(0);
}

TEST(LazyConnect, ConnectingToAStoppedServerSendsOnlyDiscovery) {
  Stand stand(/*stopped=*/true);
  auto client = stand.Connect();
  auto session = client->CreateSession({{"catalog", "lake"}});
  EXPECT_EQ(stand.Sent(), std::vector<std::string>{"GET " + kRoot});
  EXPECT_EQ(session->id(), "");
}

TEST(LazyConnect, CheckingAndClosingTheConnectionSendNothing) {
  Stand stand(/*stopped=*/true);
  auto client = stand.Connect();
  auto session = client->CreateSession();
  const auto sent = stand.Sent();
  EXPECT_EQ(client->current_access_token(), "");
  session->CancelRunning();
  session->CancelStatement("st1");
  session->DeleteStatement("st1");
  session->Close();
  session.reset();
  client.reset();
  EXPECT_EQ(stand.Sent(), sent);
}

// The first statement signs in (waiting out server_resuming: that sign-in starts the server),
// opens the session with its options and runs; capabilities come from the one discovery.
TEST(LazyConnect, TheFirstStatementSignsInOpensTheSessionAndRuns) {
  Stand stand(/*stopped=*/true, /*resuming=*/1);
  auto client = stand.Connect();
  auto session = client->CreateSession({{"catalog", "lake"}});
  ASSERT_EQ(stand.Sent(), std::vector<std::string>{"GET " + kRoot});  // nothing yet
  EXPECT_EQ(Answer42(*session), 42);
  EXPECT_EQ(session->id(), "s1");
  EXPECT_EQ(stand.Sent(), (std::vector<std::string>{"GET " + kRoot, "POST " + kSignIn, "POST " + kSignIn,
                                                    "POST " + kNewSession, "POST " + kSession + "/statements"}));
  for (const auto& s : stand.stub.Requests()) {
    if (s.target == kNewSession) EXPECT_NE(s.body.find(R"("catalog":"lake")"), std::string::npos) << s.body;
  }
  EXPECT_EQ(client->current_access_token(), "t0k");
}

TEST(LazyConnect, AWrongPasswordIsReportedByTheFirstStatement) {
  Stand stand(/*stopped=*/true);
  auto client = stand.Connect("wrong");
  auto session = client->CreateSession();
  try {
    session->Execute("SELECT 42");
    FAIL() << "ran with a wrong password";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 401);
    EXPECT_EQ(e.code(), "invalid_credentials");
  }
  EXPECT_EQ(stand.stub.Count("POST", kNewSession), 0);
}

// The positive twins: a server that serves, and lazy connect not asked for.
TEST(LazyConnect, AServerThatServesIsSignedInToAtConnectAsBefore) {
  Stand stand(/*stopped=*/false);
  auto client = stand.Connect();
  auto session = client->CreateSession();
  EXPECT_EQ(stand.Sent(), (std::vector<std::string>{"GET " + kRoot, "POST " + kSignIn, "POST " + kNewSession}));
  EXPECT_EQ(session->id(), "s1");
  EXPECT_EQ(Answer42(*session), 42);
}

TEST(LazyConnect, WithoutTheOptionAStoppedServerIsSignedInToAtConnect) {
  Stand stand(/*stopped=*/true);
  auto client = stand.Connect("pw", /*lazily=*/false);
  auto session = client->CreateSession();
  // (The GET is the client's dialect check, sent before any request that names a path.)
  EXPECT_EQ(stand.Sent(), (std::vector<std::string>{"GET " + kRoot, "POST " + kSignIn, "POST " + kNewSession}));
}

TEST(LazyConnect, DiscoveryReportsTheState) {
  Stand stopped(/*stopped=*/true);
  EXPECT_EQ(wc::Client(stopped.stub.Options()).Discover().state, "stopped");
  Stand serving(/*stopped=*/false);
  EXPECT_EQ(wc::Client(serving.stub.Options()).Discover().state, "");
}

}  // namespace
