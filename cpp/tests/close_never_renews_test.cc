// Requests that end or stop something never sign in. The client renews a password sign-in
// shortly before its token expires, on the next request; a close or a cancel that did so would
// sign in to a server that may have been stopped since, and a stopped server behind a resume
// holder (spec §11.2) starts again on a sign-in. A stub server here hands out tokens that pass
// the client's renew point (60 s before expiry) a moment after they are issued, and the tests
// count the sign-ins, check which token each close carried, and time closes on a server that
// is resuming.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
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

// Tokens last 61 s: after kPastRenewPoint, any request that renews would sign in.
constexpr auto kPastRenewPoint = 1500ms;

const std::string kToken = "/chargewire/v1/auth/token";
const std::string kSession = "/chargewire/v1/sessions/s1";
const std::string kArrow = "application/vnd.apache.arrow.stream";

Answer Problem(int status, const std::string& code, const std::string& reason = "") {
  boost::json::object problem{{"status", status}, {"code", code}, {"detail", "from the stub"}};
  if (!reason.empty()) problem["reason"] = reason;
  return {status, "application/problem+json", boost::json::serialize(problem), {}};
}

// An Arrow IPC stream of `batches` batches of 1000 BIGINTs.
std::string Stream(int batches) {
  arrow::Int64Builder builder;
  for (int i = 0; i < 1000; i++) EXPECT_TRUE(builder.Append(i).ok());
  auto column = builder.Finish().ValueOrDie();
  auto schema = arrow::schema({arrow::field("n", arrow::int64())});
  auto batch = arrow::RecordBatch::Make(schema, column->length(), {column});
  auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
  for (int i = 0; i < batches; i++) EXPECT_TRUE(writer->WriteRecordBatch(*batch).ok());
  EXPECT_TRUE(writer->Close().ok());
  return sink->Finish().ValueOrDie()->ToString();
}

// Serves discovery, sign-ins (token t<n>, 61 s), session s1 and prepared statement p1; every
// DELETE is 204, every cancel 202. `override` answers first when it returns a status.
Answer Serve(const Seen& request, int nth, const std::function<Answer(const Seen&, int)>& override = nullptr) {
  if (override) {
    Answer answer = override(request, nth);
    if (answer.status != 0) return answer;
  }
  const std::string path = request.target.substr(0, request.target.find('?'));
  if (request.method == "GET" && path == "/chargewire") {
    return {200, "application/json",
            R"({"protocol":"chargewire","protocol_versions":["1"],"server":{"name":"stub","version":"0"},)"
            R"("capabilities":["ingest","partitioned_ingest","prepared_statements","result_partitions"],)"
            R"("auth":{"grant_types":["password"]}})",
            {}};
  }
  if (request.method == "POST" && path == kToken) {
    return {200, "application/json", R"({"access_token":"t)" + std::to_string(nth) + R"(","expires_in":61})", {}};
  }
  if (request.method == "POST" && path == "/chargewire/v1/sessions") return Created();
  if (request.method == "POST" && path == kSession + "/prepared") {
    return {201, "application/json", R"({"prepared_id":"p1","result_kind":"update"})", {}};
  }
  if (request.method == "DELETE") return {204, "application/json", "", {}};
  if (request.method == "POST" && path.size() > 7 && path.substr(path.size() - 7) == "/cancel") {
    return {202, "application/json", "{}", {}};
  }
  return Problem(404, "not_found");
}

Answer None() { return {0, "", "", {}}; }

// A stub server and a client signed in to it, its token already past the renew point.
struct Fixture {
  explicit Fixture(std::function<Answer(const Seen&, int)> override = nullptr,
                   std::chrono::seconds resume_timeout = std::chrono::seconds(600))
      : stub([override](const Seen& request, int nth) { return Serve(request, nth, override); }) {
    auto options = stub.Options();
    options.resume_timeout = resume_timeout;
    client = std::make_unique<wc::Client>(options);
    client->SignInWithPassword("joe", "pw");
    session = client->CreateSession();
  }
  int SignIns() { return stub.Count("POST", kToken); }
  // The Authorization header of the last request to `method` `path`.
  std::string LastAuthorization(const std::string& method, const std::string& path) {
    std::string found;
    for (const auto& seen : stub.Requests()) {
      if (seen.method == method && seen.target.substr(0, seen.target.find('?')) == path) found = seen.authorization;
    }
    return found;
  }
  StubServer stub;
  std::unique_ptr<wc::Client> client;
  std::unique_ptr<wc::Session> session;
};

TEST(CloseNeverRenews, ARequestThatDoesWorkStillRenews) {
  Fixture f;
  std::this_thread::sleep_for(kPastRenewPoint);
  const int before = f.SignIns();
  f.session->Prepare("SELECT 1");
  EXPECT_EQ(f.SignIns(), before + 1);
  EXPECT_EQ(f.LastAuthorization("POST", kSession + "/prepared"), "Bearer t1");
}

TEST(CloseNeverRenews, TheCurrentTokenIsReadWithoutSigningIn) {
  Fixture f;
  std::this_thread::sleep_for(kPastRenewPoint);
  const int before = f.SignIns();
  EXPECT_EQ(f.client->current_access_token(), "t0");
  EXPECT_EQ(f.SignIns(), before);
  EXPECT_EQ(f.client->access_token(), "t1");  // the renewing read, as before
  EXPECT_EQ(f.SignIns(), before + 1);
  EXPECT_EQ(f.client->current_access_token(), "t1");
  wc::Client unsigned_in(f.stub.Options());
  EXPECT_EQ(unsigned_in.current_access_token(), "");
}

TEST(CloseNeverRenews, ClosingASessionSendsNoSignIn) {
  Fixture f;
  std::this_thread::sleep_for(kPastRenewPoint);
  const int before = f.SignIns();
  f.session->Close();
  EXPECT_EQ(f.SignIns(), before);
  EXPECT_EQ(f.stub.Count("DELETE", kSession), 1);
  EXPECT_EQ(f.LastAuthorization("DELETE", kSession), "Bearer t0");
}

TEST(CloseNeverRenews, DestroyingASessionAndAPreparedStatementSendsNoSignIn) {
  Fixture f;
  auto prepared = f.session->Prepare("SELECT 1");
  std::this_thread::sleep_for(kPastRenewPoint);
  const int before = f.SignIns();
  prepared.reset();
  f.session.reset();
  EXPECT_EQ(f.SignIns(), before);
  EXPECT_EQ(f.stub.Count("DELETE", kSession + "/prepared/p1"), 1);
  EXPECT_EQ(f.stub.Count("DELETE", kSession), 1);
}

TEST(CloseNeverRenews, ClosingAPreparedStatementSendsNoSignInAndTakes401And404AsClosed) {
  for (int status : {204, 401, 404}) {
    SCOPED_TRACE(status);
    Fixture f([status](const Seen& request, int) {
      if (request.method != "DELETE" || status == 204) return None();
      return status == 401 ? Problem(401, "invalid_token", "token_revoked") : Problem(404, "prepared_not_found");
    });
    auto prepared = f.session->Prepare("SELECT 1");
    std::this_thread::sleep_for(kPastRenewPoint);
    const int before = f.SignIns();
    EXPECT_NO_THROW(prepared->Close());
    EXPECT_EQ(f.SignIns(), before);
    EXPECT_EQ(f.LastAuthorization("DELETE", kSession + "/prepared/p1"), "Bearer t0");
  }
}

TEST(CloseNeverRenews, DeletingAStatementSendsNoSignInAndTakes401And404AsDone) {
  for (int status : {204, 401, 404}) {
    SCOPED_TRACE(status);
    Fixture f([status](const Seen& request, int) {
      if (request.method != "DELETE" || status == 204) return None();
      return status == 401 ? Problem(401, "invalid_token", "token_expired") : Problem(404, "statement_not_found");
    });
    std::this_thread::sleep_for(kPastRenewPoint);
    const int before = f.SignIns();
    EXPECT_NO_THROW(f.session->DeleteStatement("st1"));
    EXPECT_EQ(f.SignIns(), before);
  }
}

TEST(CloseNeverRenews, CancellingAStatementSendsNoSignIn) {
  Fixture f;
  std::this_thread::sleep_for(kPastRenewPoint);
  const int before = f.SignIns();
  f.session->CancelStatement("st1");
  EXPECT_EQ(f.SignIns(), before);
  EXPECT_EQ(f.LastAuthorization("POST", kSession + "/statements/st1/cancel"), "Bearer t0");
}

// The statement's own request renews (it does work); the cancel sent while it runs, once the
// token is past the renew point again, does not.
TEST(CloseNeverRenews, CancelRunningSendsNoSignIn) {
  const std::string rows = Stream(1);
  Fixture f([&](const Seen& request, int) {
    if (request.method == "POST" && request.target == kSession + "/statements") {
      std::this_thread::sleep_for(3s);
      return Answer{200, kArrow, rows, {{"Chargewire-Statement-Id", "st1"}}};
    }
    return None();
  });
  std::this_thread::sleep_for(kPastRenewPoint);
  std::thread runner([&] {
    auto result = f.session->Execute("SELECT n FROM slow");
    if (result.reader) result.reader->Close();
  });
  std::this_thread::sleep_for(kPastRenewPoint);  // the statement's fresh token is past it again
  const int before = f.SignIns();
  f.session->CancelRunning();
  EXPECT_EQ(f.SignIns(), before);
  EXPECT_EQ(f.stub.Count("POST", kSession + "/cancel"), 1);
  runner.join();
}

TEST(CloseNeverRenews, ClosingASplitResultEarlySendsNoSignIn) {
  const std::string rows = Stream(200);
  Fixture f([&](const Seen& request, int) {
    const std::string path = request.target.substr(0, request.target.find('?'));
    if (request.method == "POST" && path == kSession + "/statements") {
      return Answer{200, kArrow, rows,
                    {{"Chargewire-Statement-Id", "st1"}, {"Chargewire-Result-Partitions", "2"},
                     {"Chargewire-Result-Order", "interleaved"}}};
    }
    if (request.method == "GET" && path == kSession + "/statements/st1/result") return Answer{200, kArrow, rows, {}};
    return None();
  });
  auto result = f.session->Execute("SELECT n FROM big");
  ASSERT_TRUE(result.reader);
  ASSERT_TRUE(result.reader->Next());
  std::this_thread::sleep_for(kPastRenewPoint);
  const int before = f.SignIns();
  result.reader->Close();
  EXPECT_EQ(f.stub.Count("POST", kSession + "/statements/st1/cancel"), 1);
  EXPECT_EQ(f.SignIns(), before);
}

// A partitioned ingest whose part fails is aborted (DELETE): without a sign-in, and within
// seconds although the server answers that DELETE server_resuming.
TEST(CloseNeverRenews, AnIngestsAbortSendsNoSignInAndIsBounded) {
  Fixture f(
      [](const Seen& request, int) {
        const std::string path = request.target.substr(0, request.target.find('?'));
        if (request.method == "POST" && path == kSession + "/ingests") {
          return Answer{201, "application/json", R"({"ingest_id":"i1"})", {}};
        }
        if (request.method == "POST" && path.rfind(kSession + "/ingests/i1/parts/", 0) == 0) {
          std::this_thread::sleep_for(kPastRenewPoint);  // the abort comes past the renew point
          return Problem(500, "internal_error");
        }
        if (request.method == "DELETE" && path == kSession + "/ingests/i1") return Resuming("1");
        return None();
      },
      std::chrono::seconds(30));
  arrow::Int64Builder builder;
  ASSERT_TRUE(builder.Append(1).ok());
  auto column = builder.Finish().ValueOrDie();
  auto schema = arrow::schema({arrow::field("n", arrow::int64())});
  auto batch = arrow::RecordBatch::Make(schema, 1, {column});
  auto reader = arrow::RecordBatchReader::Make({batch, batch, batch}, schema).ValueOrDie();
  wc::IngestOptions options;
  options.parts = 2;
  const int before = f.SignIns();
  const auto started = std::chrono::steady_clock::now();
  EXPECT_THROW(f.session->Ingest("t", *reader, options), wc::Error);
  EXPECT_LT(std::chrono::steady_clock::now() - started, 7s);
  EXPECT_GE(f.stub.Count("DELETE", kSession + "/ingests/i1"), 1);
  EXPECT_EQ(f.SignIns(), before);
}

// Closes give up after 3 s on a server that is resuming, not after ResumeTimeout.
TEST(CloseNeverRenews, ClosesOnAResumingServerAreBounded) {
  std::atomic<bool> resuming = false;
  Fixture f(
      [&](const Seen&, int) {
        if (resuming) return Resuming("1");
        return None();
      },
      std::chrono::seconds(30));
  auto prepared = f.session->Prepare("SELECT 1");
  resuming = true;
  for (const auto& [name, close] : std::vector<std::pair<std::string, std::function<void()>>>{
           {"prepared statement", [&] { prepared->Close(); }},
           {"statement", [&] { f.session->DeleteStatement("st1"); }},
           {"session", [&] { f.session->Close(); }}}) {
    SCOPED_TRACE(name);
    const auto started = std::chrono::steady_clock::now();
    try {
      close();
    } catch (const wc::Error& e) {
      EXPECT_EQ(e.code(), "server_resuming");
    }
    EXPECT_LT(std::chrono::steady_clock::now() - started, 4s);
  }
}

// Planted to prove .github/workflows/cpp.yml fails on a failing test; removed in the next commit.
TEST(CiGateProof, FailsOnPurpose) { FAIL() << "planted failure"; }

}  // namespace
