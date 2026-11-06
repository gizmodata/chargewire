// The client against a server that is resuming (spec §11.2): a stub server, here in the test
// process, answers 503 server_resuming as a stand-in would, and the tests check what the
// client sends again, how long it waits and when it gives up.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

#include "chargewire/client.hpp"
#include "transport.hpp"

namespace wc = chargewire::client;
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = net::ip::tcp;
using namespace std::chrono_literals;

namespace {

struct Seen {
  std::string method;
  std::string target;
  std::string body;
};

struct Answer {
  int status = 200;
  std::string content_type = "application/json";
  std::string body;
  std::map<std::string, std::string> headers;
};

// One problem document of server_resuming, as a stand-in sends it; `retry_after` empty: no
// Retry-After header.
Answer Resuming(const std::string& retry_after = "1") {
  Answer answer;
  answer.status = 503;
  answer.content_type = "application/problem+json";
  const std::string seconds = retry_after.empty() ? "3" : retry_after;
  answer.body = boost::json::serialize(boost::json::object{
      {"type", "urn:chargewire:error:server_resuming"},
      {"title", "Server resuming"},
      {"status", 503},
      {"code", "server_resuming"},
      {"detail", "the server is resuming; retry after " + seconds + " s"},
      {"request_id", "r_resume"}});
  if (!retry_after.empty()) answer.headers["Retry-After"] = retry_after;
  return answer;
}

Answer Discovery() {
  return {200, "application/json",
          R"({"protocol":"chargewire","protocol_versions":["1"],"server":{"name":"stub","version":"0"},)"
          R"("capabilities":["ingest"],"auth":{"grant_types":["password"]}})",
          {}};
}

Answer Token() { return {200, "application/json", R"({"access_token":"t0k","token_type":"Bearer","expires_in":3600})", {}}; }

Answer Created() {
  return {201, "application/json", R"({"session_id":"s1","idle_timeout_s":900,"options":{},"transaction":"none"})", {}};
}

// A Chargewire stand-in on 127.0.0.1: every request goes to `handler` (by method and path,
// with how many such requests came before), and is recorded. Connections are kept alive
// as the client asks.
class StubServer {
 public:
  using Handler = std::function<Answer(const Seen& request, int nth)>;

  explicit StubServer(Handler handler)
      : handler_(std::move(handler)), acceptor_(io_, {net::ip::make_address("127.0.0.1"), 0}) {
    port_ = acceptor_.local_endpoint().port();
    thread_ = std::thread([this] { Run(); });
  }

  ~StubServer() {
    stopping_ = true;
    {
      std::lock_guard lock(mutex_);
      for (auto* socket : open_) {
        boost::system::error_code ignored;
        socket->shutdown(tcp::socket::shutdown_both, ignored);
      }
    }
    try {  // wake the accept
      net::io_context io;
      tcp::socket poke(io);
      poke.connect({net::ip::make_address("127.0.0.1"), port_});
    } catch (...) {
    }
    thread_.join();
    for (auto& connection : connections_) connection.join();
  }

  uint16_t port() const { return port_; }

  wc::ConnectOptions Options() const {
    wc::ConnectOptions options;
    options.host = "127.0.0.1";
    options.port = port_;
    return options;
  }

  std::vector<Seen> Requests() {
    std::lock_guard lock(mutex_);
    return seen_;
  }
  // How many requests came with this method and path (query ignored).
  int Count(const std::string& method, const std::string& path) {
    std::lock_guard lock(mutex_);
    int n = 0;
    for (const auto& s : seen_) {
      if (s.method == method && s.target.substr(0, s.target.find('?')) == path) n++;
    }
    return n;
  }

 private:
  // Each connection on a thread of its own: a client keeps several open (its sign-in's,
  // each session's).
  void Run() {
    while (!stopping_) {
      auto socket = std::make_shared<tcp::socket>(io_);
      boost::system::error_code ec;
      acceptor_.accept(*socket, ec);
      if (ec || stopping_) break;
      std::lock_guard lock(mutex_);
      open_.push_back(socket.get());
      connections_.emplace_back([this, socket] {
        Serve(*socket);
        std::lock_guard lock(mutex_);
        std::erase(open_, socket.get());
      });
    }
  }

  void Serve(tcp::socket& socket) {
    beast::flat_buffer buffer;
    while (!stopping_) {
      http::request<http::string_body> request;
      boost::system::error_code ec;
      http::read(socket, buffer, request, ec);
      if (ec) return;
      Seen seen{std::string(request.method_string()), std::string(request.target()), request.body()};
      int nth = 0;
      {
        std::lock_guard lock(mutex_);
        for (const auto& s : seen_) {
          if (s.method == seen.method && s.target.substr(0, s.target.find('?')) ==
                                             seen.target.substr(0, seen.target.find('?'))) {
            nth++;
          }
        }
        seen_.push_back(seen);
      }
      Answer answer = handler_(seen, nth);
      http::response<http::string_body> response(static_cast<http::status>(answer.status), 11);
      response.set(http::field::content_type, answer.content_type);
      for (const auto& [name, value] : answer.headers) response.set(name, value);
      response.body() = answer.body;
      response.keep_alive(request.keep_alive());
      response.prepare_payload();
      http::write(socket, response, ec);
      if (ec || !request.keep_alive()) return;
    }
  }

  Handler handler_;
  net::io_context io_;
  tcp::acceptor acceptor_;
  uint16_t port_ = 0;
  std::atomic<bool> stopping_ = false;
  std::mutex mutex_;
  std::vector<tcp::socket*> open_;
  std::vector<Seen> seen_;
  std::thread thread_;
  std::vector<std::thread> connections_;
};

// Records the client's waits between attempts instead of sleeping them; restores sleeping
// when it goes.
class RecordedWaits {
 public:
  RecordedWaits() {
    wc::detail::SetResumeSleepForTesting([this](std::chrono::milliseconds wait) {
      std::lock_guard lock(mutex_);
      waits_.push_back(wait);
    });
  }
  ~RecordedWaits() { wc::detail::SetResumeSleepForTesting(nullptr); }
  std::vector<std::chrono::milliseconds> waits() {
    std::lock_guard lock(mutex_);
    return waits_;
  }

 private:
  std::mutex mutex_;
  std::vector<std::chrono::milliseconds> waits_;
};

bool Is(const Seen& s, const char* method, const char* path) {
  return s.method == method && s.target.substr(0, s.target.find('?')) == path;
}

const char* kToken = "/chargewire/v1/auth/token";
const char* kSessions = "/chargewire/v1/sessions";

}  // namespace

TEST(Resuming, TheSignInAndTheSessionAreSentAgainUntilTheServerAnswers) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    if (Is(s, "POST", kToken)) return nth < 3 ? Resuming("2") : Token();
    if (Is(s, "POST", kSessions)) return nth < 2 ? Resuming("2") : Created();
    if (Is(s, "DELETE", "/chargewire/v1/sessions/s1")) return Answer{204, "application/json", "", {}};
    return Answer{404, "application/problem+json", R"({"code":"not_found","status":404})", {}};
  });
  wc::Client client(stub.Options());
  client.SignInWithPassword("joe", "s3:cret");
  EXPECT_EQ(client.access_token(), "t0k");
  auto session = client.CreateSession();
  EXPECT_EQ(session->id(), "s1");
  session->Close();

  EXPECT_EQ(stub.Count("POST", kToken), 4);
  EXPECT_EQ(stub.Count("POST", kSessions), 3);
  // The same request each time: the sign-in's credential, the session's options.
  std::vector<std::string> sign_ins, creates;
  for (const auto& s : stub.Requests()) {
    if (Is(s, "POST", kToken)) sign_ins.push_back(s.body);
    if (Is(s, "POST", kSessions)) creates.push_back(s.body);
  }
  for (const auto& body : sign_ins) EXPECT_EQ(body, sign_ins.front());
  EXPECT_NE(sign_ins.front().find("\"password\":\"s3:cret\""), std::string::npos) << sign_ins.front();
  for (const auto& body : creates) EXPECT_EQ(body, creates.front());
  EXPECT_EQ(waits.waits(), std::vector<std::chrono::milliseconds>(5, 2000ms));
}

TEST(Resuming, WaitsRetryAfterThreeSecondsWithoutOneAtLeastOneAndAtMostSixty) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    switch (nth) {
      case 0: return Resuming("");      // no Retry-After: 3 s
      case 1: return Resuming("7");
      case 2: return Resuming("120");   // more than 60: 60
      case 3: {
        Answer answer = Resuming("1");
        answer.headers["Retry-After"] = "Wed, 21 Oct 2026 07:28:00 GMT";  // not an integer: 3 s
        return answer;
      }
      case 4: return Resuming("0");     // less than 1: 1
      default: return Token();
    }
  });
  wc::Client client(stub.Options());
  client.SignInWithPassword("joe", "pw");
  EXPECT_EQ(waits.waits(), (std::vector<std::chrono::milliseconds>{3000ms, 7000ms, 60000ms, 3000ms, 1000ms}));
  EXPECT_EQ(wc::detail::ResumeWait(" 12 "), 12000ms);
  EXPECT_EQ(wc::detail::ResumeWait("-1"), 3000ms);
  EXPECT_EQ(wc::detail::ResumeWait("99999999999999"), 3000ms);
}

TEST(Resuming, TheResumeTimeoutSurfacesTheServersError) {
  StubServer stub([](const Seen& s, int) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    return Resuming("1");
  });
  auto options = stub.Options();
  options.resume_timeout = 2s;
  wc::Client client(options);
  const auto started = std::chrono::steady_clock::now();
  try {
    client.SignInWithPassword("joe", "pw");
    FAIL() << "signed in";
  } catch (const wc::Error& e) {
    const auto took = std::chrono::steady_clock::now() - started;
    EXPECT_EQ(e.status(), 503);
    EXPECT_EQ(e.code(), "server_resuming");
    EXPECT_EQ(e.request_id(), "r_resume");
    EXPECT_NE(std::string(e.what()).find("the server is resuming; retry after 1 s"), std::string::npos) << e.what();
    EXPECT_NE(std::string(e.what()).find("still resuming after 1 s of retries"), std::string::npos) << e.what();
    // Attempts at 0 s and 1 s; a third would start at 2 s, not before the deadline.
    EXPECT_EQ(stub.Count("POST", kToken), 2);
    EXPECT_GE(took, 900ms);
    EXPECT_LT(took, 1900ms);
  }
}

TEST(Resuming, ZeroTurnsTheRetryOff) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    return nth < 3 ? Resuming("1") : Token();  // a client that did retry would sign in
  });
  auto options = stub.Options();
  options.resume_timeout = 0s;
  wc::Client client(options);
  try {
    client.SignInWithPassword("joe", "pw");
    FAIL() << "signed in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "server_resuming");
    EXPECT_STREQ(e.what(), "the server is resuming; retry after 1 s");
  }
  EXPECT_EQ(stub.Count("POST", kToken), 1);
}

TEST(Resuming, DiscoveryAndHealthAreNotSentAgain) {
  RecordedWaits waits;
  // Answered after the fifth time, so a client that did send them again would be seen to.
  StubServer stub([](const Seen& s, int nth) {
    if (nth < 5) return Resuming("1");
    return Is(s, "GET", "/cw/v1/health") ? Answer{200, "application/json", R"({"status":"serving"})", {}} : Discovery();
  });
  auto options = stub.Options();
  options.base_path = "/cw";  // another root: no dialect probe, the stub sees only what is asked
  wc::Client client(options);
  try {
    client.Discover();
    FAIL() << "discovered";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "server_resuming");
  }
  // Health hands a 503's document back as it came.
  EXPECT_EQ(client.Health().at("code").as_string(), "server_resuming");
  EXPECT_EQ(stub.Count("GET", "/cw"), 1);
  EXPECT_EQ(stub.Count("GET", "/cw/v1/health"), 1);
  EXPECT_TRUE(waits.waits().empty());
}

TEST(Resuming, OtherServiceUnavailableAnswersAreThrownAsBefore) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    if (nth == 0) {
      return Answer{503, "application/problem+json",
                    R"({"type":"urn:chargewire:error:server_draining","title":"Server shutting down","status":503,)"
                    R"("code":"server_draining","detail":"the server is shutting down"})",
                    {{"Retry-After", "5"}}};
    }
    return Answer{503, "text/html", "<html>no healthy upstream</html>", {}};  // a proxy's own page
  });
  wc::Client client(stub.Options());
  try {
    client.SignInWithPassword("joe", "pw");
    FAIL() << "signed in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "server_draining");
    EXPECT_STREQ(e.what(), "the server is shutting down");
  }
  try {
    client.SignInWithPassword("joe", "pw");
    FAIL() << "signed in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "client_http_503");
    EXPECT_STREQ(e.what(), "<html>no healthy upstream</html>");
  }
  EXPECT_EQ(stub.Count("POST", kToken), 2);
  EXPECT_TRUE(waits.waits().empty());
}

TEST(Resuming, AStreamedBodyIsNotSentAgain) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    if (Is(s, "POST", kToken)) return Token();
    if (Is(s, "POST", kSessions)) return Created();
    if (Is(s, "DELETE", "/chargewire/v1/sessions/s1")) return Answer{204, "application/json", "", {}};
    // Answered after the third time, so a client that did send it again would be seen to.
    return nth < 3 ? Resuming("1") : Answer{200, "application/json", R"({"rows_ingested":0})", {}};
  });
  wc::Client client(stub.Options());
  client.SignInWithPassword("joe", "pw");
  auto session = client.CreateSession();
  auto schema = arrow::schema({arrow::field("i", arrow::int64())});
  arrow::Int64Builder builder;
  ASSERT_TRUE(builder.Append(1).ok());
  auto batch = arrow::RecordBatch::Make(schema, 1, {builder.Finish().ValueOrDie()});
  auto reader = arrow::RecordBatchReader::Make({batch}, schema).ValueOrDie();
  try {
    session->Ingest("t", *reader);
    FAIL() << "ingested";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.code(), "server_resuming");
  }
  EXPECT_EQ(stub.Count("POST", "/chargewire/v1/sessions/s1/ingest"), 1);
  EXPECT_TRUE(waits.waits().empty());
}

TEST(Resuming, ARequestWithItsOwnTimeLimitStopsAtIt) {
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    if (Is(s, "POST", kToken)) return Token();
    if (Is(s, "POST", kSessions)) return Created();
    // A client that waited out the 10 s would be answered the second time.
    return nth < 1 ? Resuming("10") : Answer{204, "application/json", "", {}};
  });
  wc::Client client(stub.Options());
  client.SignInWithPassword("joe", "pw");
  auto session = client.CreateSession();
  // Closing a session is bounded at 3 s (a client that exits must not hang on a server that
  // is gone): a 10 s wait would end past that, so it gives up at once.
  const auto started = std::chrono::steady_clock::now();
  session->Close();
  EXPECT_LT(std::chrono::steady_clock::now() - started, 1500ms);
  EXPECT_EQ(stub.Count("DELETE", "/chargewire/v1/sessions/s1"), 1);
}

// A stand-in holds a sign-in up to 25 s while the server comes up (spec §11.2); the client
// waits for its answer although its connect timeout is far shorter. Real time, no stub sleep.
TEST(Resuming, ASignInHeldTwentyFiveSecondsIsAnswered) {
  StubServer stub([](const Seen& s, int) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    if (Is(s, "POST", kToken)) {
      std::this_thread::sleep_for(25s);
      return Token();
    }
    return Answer{404, "application/problem+json", R"({"code":"not_found","status":404})", {}};
  });
  auto options = stub.Options();
  options.connect_timeout = 2s;
  wc::Client client(options);
  const auto started = std::chrono::steady_clock::now();
  client.SignInWithPassword("joe", "pw");
  EXPECT_GE(std::chrono::steady_clock::now() - started, 25s);
  EXPECT_EQ(client.access_token(), "t0k");
}

// on_resuming hears each wait as it starts and about once a second after (a countdown an
// application can show), with the attempt, the time left, the time spent and the server's
// detail; the request goes on as without it.
TEST(Resuming, TheProgressCallbackHearsEachWaitSecondBySecond) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    return nth < 2 ? Resuming(nth == 0 ? "3" : "1") : Token();
  });
  auto options = stub.Options();
  std::vector<wc::ResumeProgress> heard;
  options.on_resuming = [&](const wc::ResumeProgress& progress) {
    heard.push_back(progress);
    return true;
  };
  wc::Client client(options);
  client.SignInWithPassword("joe", "pw");
  EXPECT_EQ(client.access_token(), "t0k");
  ASSERT_EQ(heard.size(), 4u);  // 3 s as 3, 2, 1; then 1 s as 1
  const std::vector<std::pair<int, std::chrono::milliseconds>> expected = {
      {1, 3000ms}, {1, 2000ms}, {1, 1000ms}, {2, 1000ms}};
  for (size_t i = 0; i < heard.size(); ++i) {
    EXPECT_EQ(heard[i].attempt, expected[i].first) << i;
    EXPECT_EQ(heard[i].retry_in, expected[i].second) << i;
    EXPECT_EQ(heard[i].resume_timeout, 600000ms) << i;
    EXPECT_GE(heard[i].elapsed.count(), 0) << i;
    // The detail of the answer being waited out.
    const std::string retry = expected[i].first == 1 ? "retry after 3 s" : "retry after 1 s";
    EXPECT_NE(heard[i].detail.find(retry), std::string::npos) << i << ": " << heard[i].detail;
  }
  // The same 4 s in all, in 1 s steps.
  EXPECT_EQ(waits.waits(), std::vector<std::chrono::milliseconds>(4, 1000ms));
}

// Returning false stops at once (an application's Ctrl-C) with the server's error, saying
// so, and the request isn't sent again.
TEST(Resuming, TheProgressCallbackCanStopTheWait) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    return Resuming("30");
  });
  auto options = stub.Options();
  int calls = 0;
  options.on_resuming = [&](const wc::ResumeProgress&) { return ++calls < 3; };
  wc::Client client(options);
  try {
    client.SignInWithPassword("joe", "pw");
    FAIL() << "signed in";
  } catch (const wc::Error& e) {
    EXPECT_EQ(e.status(), 503);
    EXPECT_EQ(e.code(), "server_resuming");
    EXPECT_NE(std::string(e.what()).find("the server is resuming"), std::string::npos) << e.what();
  }
  EXPECT_EQ(calls, 3);
  EXPECT_EQ(waits.waits(), std::vector<std::chrono::milliseconds>(2, 1000ms)) << "stopped within the first wait";
  EXPECT_EQ(stub.Count("POST", kToken), 1);
}

// A callback that throws is ignored: the client keeps waiting and signs in.
TEST(Resuming, AThrowingProgressCallbackChangesNothing) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    return nth < 1 ? Resuming("2") : Token();
  });
  auto options = stub.Options();
  options.on_resuming = [](const wc::ResumeProgress&) -> bool { throw std::runtime_error("display broke"); };
  wc::Client client(options);
  client.SignInWithPassword("joe", "pw");
  EXPECT_EQ(client.access_token(), "t0k");
  EXPECT_EQ(waits.waits(), std::vector<std::chrono::milliseconds>(2, 1000ms));
}

TEST(Resuming, ARecycledSessionConnectionStillResumesAfterDiscoveryAndHealth) {
  RecordedWaits waits;
  StubServer stub([](const Seen& s, int nth) {
    if (Is(s, "GET", "/chargewire")) return Discovery();
    if (Is(s, "GET", "/chargewire/v1/health")) return Answer{200, "application/json", "{}", {}};
    if (Is(s, "POST", kToken)) return Token();
    if (Is(s, "POST", kSessions)) return nth == 1 ? Resuming("1") : Created();
    if (Is(s, "DELETE", "/chargewire/v1/sessions/s1")) return Answer{204, "application/json", "", {}};
    return Answer{404, "application/problem+json", R"({"code":"not_found","status":404})", {}};
  });
  wc::Client client(stub.Options());
  client.SignInWithPassword("joe", "pw");
  client.CreateSession()->Close();
  client.Discover();
  client.Health();
  const auto opened = client.connections_opened();
  auto session = client.CreateSession();
  EXPECT_EQ(session->id(), "s1");
  EXPECT_EQ(client.connections_opened(), opened);
  EXPECT_EQ(stub.Count("POST", kSessions), 3);
  EXPECT_EQ(waits.waits(), std::vector<std::chrono::milliseconds>{1000ms});
  session->Close();
}
