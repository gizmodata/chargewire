// Tests only: holds a server's engine thread in the calls the SDK makes after a statement's
// work is done, and reads the statement over every path while it is held (spec §6.2: an
// `update` result comes only with `succeeded`; §6.8: a cancellation after the work is done
// changes nothing).
//
// Why hold the engine there. A statement without rows used to get its result
// (Statement::SetUpdate()) and, a few calls later on the engine thread, its end (Finish()); in
// between the SDK asks the engine for the session's options and transaction. A request that
// read the statement in that window got `"state": "running"` with the final update result
// (the streamed submit's own answer did, on a busy machine), and a cancel or timeout that
// landed there ended a statement as `cancelled` whose change had been made and reported. The
// window is a few microseconds wide, so nothing is left to chance here: the engine's
// TakeOptionsChanged() and Options(), which the SDK calls right after the work, wait until the
// test has read what it came for. (Not Transaction(): the SDK calls it under the lock that a
// status request's progress read and a cancel's interrupt take, so holding it there would
// hold the requests the test sends.)
//
// Used with an engine that only plays statements (update_outcome_test.cc) and with DuckDB
// (duckdb_update_outcome_test.cc).
#pragma once

#include <gtest/gtest.h>

#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

#include "chargewire/backend.hpp"

namespace update_outcome {

namespace json = boost::json;
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;

using Headers = std::map<std::string, std::string>;

inline constexpr char kArrow[] = "application/vnd.apache.arrow.stream";
// How long a step that must happen may take before the test gives up on it. Nothing waits
// this long when things work: every wait ends on the event it waits for.
inline constexpr std::chrono::seconds kPatience{60};

// The engine thread's stops after a statement's work. The engine side calls WorkStarted(),
// WorkDone(), Hold() and Cancelled(); the test arms it and lets each stop go.
class Gate {
 public:
  // ---- Test side ----
  void Arm() {
    std::lock_guard lock(mutex_);
    armed_ = true;
  }
  // Lets go of a held call and holds no more.
  void Disarm() {
    {
      std::lock_guard lock(mutex_);
      armed_ = false;
    }
    changed_.notify_all();
  }
  // True once the engine thread is held after a statement's work (and not yet let go).
  bool WaitHeld(std::chrono::milliseconds limit) {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, limit, [&] { return HeldNow(); });
  }
  void Release() {
    {
      std::lock_guard lock(mutex_);
      if (HeldNow()) released_ = holds_;
    }
    changed_.notify_all();
  }
  bool WaitCancelled(const std::string& reason, std::chrono::milliseconds limit) {
    std::unique_lock lock(mutex_);
    return changed_.wait_for(lock, limit, [&] {
      for (const auto& seen : cancels_after_work_) {
        if (seen == reason) return true;
      }
      return false;
    });
  }
  int holds() const {
    std::lock_guard lock(mutex_);
    return holds_;
  }
  // Cancel() calls that reached the engine before the running statement's work was done:
  // those may stop the work, so a test about cancels that come too late wants none.
  int cancels_before_work_done() const {
    std::lock_guard lock(mutex_);
    return cancels_before_work_;
  }
  bool gave_up() const {
    std::lock_guard lock(mutex_);
    return gave_up_;
  }

  // ---- Engine side ----
  void WorkStarted() {
    std::lock_guard lock(mutex_);
    work_done_ = false;
    working_ = true;
  }
  // Execute() or ExecutePrepared() is about to return a statement's outcome.
  void WorkDone() {
    std::lock_guard lock(mutex_);
    work_done_ = true;
    working_ = false;
  }
  // The work failed, or returned rows: nothing to hold for.
  void NothingToHold() {
    std::lock_guard lock(mutex_);
    working_ = false;
  }
  // TakeOptionsChanged() and Options() call this first: after a statement's work, while
  // armed, it waits for the test.
  void Hold() {
    std::unique_lock lock(mutex_);
    if (!armed_ || !work_done_) return;
    const int mine = ++holds_;
    held_ = true;
    changed_.notify_all();
    if (!changed_.wait_for(lock, kPatience, [&] { return released_ >= mine || !armed_; })) {
      gave_up_ = true;  // the test never came: it fails on this
    }
    held_ = false;
    changed_.notify_all();
  }
  void Cancelled(std::string_view reason) {
    {
      std::lock_guard lock(mutex_);
      if (working_) {
        ++cancels_before_work_;
      } else {
        cancels_after_work_.emplace_back(reason);
      }
    }
    changed_.notify_all();
  }

 private:
  // Held and not let go yet (a call that was let go may not have woken up).
  bool HeldNow() const { return held_ && released_ < holds_; }  // mutex_ held

  mutable std::mutex mutex_;
  std::condition_variable changed_;
  bool armed_ = false;
  bool working_ = false;
  bool work_done_ = false;
  bool held_ = false;
  bool gave_up_ = false;
  int holds_ = 0;
  int released_ = 0;
  int cancels_before_work_ = 0;
  std::vector<std::string> cancels_after_work_;
};

class AnyPassword : public chargewire::Authenticator {
 public:
  chargewire::Principal AuthenticatePassword(std::string_view username, std::string_view) override {
    return chargewire::Principal{.name = std::string(username), .role = "admin"};
  }
  json::array GrantTypes() override { return {"password"}; }
};

struct Answer {
  int status = 0;
  Headers headers;  // lower-case names
  std::string body;
  // The body as a JSON object; an empty object when it isn't one.
  json::object Json() const {
    boost::system::error_code ec;
    json::value value = json::parse(body, ec);
    return !ec && value.is_object() ? value.as_object() : json::object{};
  }
  bool IsJson() const {
    const auto it = headers.find("content-type");
    return it != headers.end() && it->second.starts_with("application/json");
  }
};

// A connection to the server, kept open from one request to the next.
class Connection {
 public:
  explicit Connection(uint16_t port) : port_(port) {}
  Answer Exchange(http::verb method, const std::string& target, const Headers& headers = {},
                  const std::string& body = "") {
    if (!stream_) {
      stream_.emplace(io_);
      stream_->connect(net::ip::tcp::endpoint(net::ip::make_address("127.0.0.1"), port_));
    }
    http::request<http::string_body> request{method, target, 11};
    request.set(http::field::host, "127.0.0.1");
    for (const auto& [name, value] : headers) request.set(name, value);
    if (!body.empty() || method == http::verb::post) {
      if (!headers.contains("Content-Type")) request.set(http::field::content_type, "application/json");
      request.body() = body;
    }
    request.prepare_payload();
    Answer answer;
    try {
      http::write(*stream_, request);
      http::response_parser<http::string_body> parser;
      parser.body_limit(64 * 1024 * 1024);
      http::read(*stream_, buffer_, parser);
      const auto& message = parser.get();
      answer.status = static_cast<int>(message.result_int());
      for (const auto& field : message) {
        std::string name(field.name_string());
        for (auto& c : name) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        answer.headers[name] = std::string(field.value());
      }
      answer.body = message.body();
      if (!message.keep_alive()) Close();
    } catch (...) {
      Close();
      throw;
    }
    return answer;
  }

 private:
  void Close() {
    beast::error_code ec;
    stream_->socket().shutdown(net::ip::tcp::socket::shutdown_both, ec);
    stream_.reset();
    buffer_.clear();
  }

  uint16_t port_;
  net::io_context io_;
  std::optional<beast::tcp_stream> stream_;
  beast::flat_buffer buffer_;
};

// One request on a connection of its own: what a request that waits on another thread uses.
inline Answer Send(uint16_t port, http::verb method, const std::string& target, const Headers& headers = {},
                   const std::string& body = "") {
  Connection connection(port);
  return connection.Exchange(method, target, headers, body);
}

// A signed-in client with one session.
struct Wire {
  uint16_t port = 0;
  std::string token;
  std::string session;  // "/chargewire/v1/sessions/s_..."
  // The test thread's connection: Post() and Get() are for that thread only. (A new
  // connection for each of a test's hundreds of requests uses up the machine's local ports
  // when the tests run in a loop.)
  std::shared_ptr<Connection> connection;

  static Wire Open(uint16_t port) {
    Wire wire;
    wire.port = port;
    wire.connection = std::make_shared<Connection>(port);
    const Answer signed_in = wire.connection->Exchange(
        http::verb::post, "/chargewire/v1/auth/token", {},
        R"({"grant_type": "password", "username": "alice", "password": "x"})");
    EXPECT_EQ(signed_in.status, 200) << signed_in.body;
    wire.token = std::string(signed_in.Json().at("access_token").as_string());
    const Answer created = wire.Post("/chargewire/v1/sessions", R"({"options": {}})");
    EXPECT_EQ(created.status, 201) << created.body;
    wire.session = "/chargewire/v1/sessions/" + std::string(created.Json().at("session_id").as_string());
    return wire;
  }
  Headers With(Headers headers) const {
    headers["Authorization"] = "Bearer " + token;
    return headers;
  }
  Answer Post(const std::string& target, const std::string& body, Headers headers = {}) const {
    return connection->Exchange(http::verb::post, target, With(std::move(headers)), body);
  }
  Answer Get(const std::string& target, Headers headers = {}) const {
    return connection->Exchange(http::verb::get, target, With(std::move(headers)));
  }
  // The same on a connection of their own, for requests another thread waits on.
  Answer PostAlone(const std::string& target, const std::string& body, Headers headers = {}) const {
    return Send(port, http::verb::post, target, With(std::move(headers)), body);
  }
  Answer GetAlone(const std::string& target, Headers headers = {}) const {
    return Send(port, http::verb::get, target, With(std::move(headers)));
  }
  json::object Status(const std::string& statement_id, Headers headers = {}) const {
    const Answer answer = Get(session + "/statements/" + statement_id, std::move(headers));
    EXPECT_EQ(answer.status, 200) << answer.body;
    return answer.Json();
  }
  json::object Session() const {
    const Answer answer = Get(session);
    EXPECT_EQ(answer.status, 200) << answer.body;
    return answer.Json();
  }
  // Runs SQL on the status-document path to its end; the finished document.
  json::object Run(const std::string& sql) const {
    const Answer answer = Post(session + "/statements", json::serialize(json::object{{"sql", sql}}),
                               {{"Prefer", "wait=30"}});
    EXPECT_EQ(answer.status, 201) << sql << ": " << answer.body;
    const json::object doc = answer.Json();
    EXPECT_EQ(StateOf(doc), "succeeded") << sql << ": " << answer.body;
    return doc;
  }
  static std::string StateOf(const json::object& doc) {
    const json::value* state = doc.if_contains("state");
    return state != nullptr && state->is_string() ? std::string(state->as_string()) : "(no state)";
  }
};

inline bool Terminal(const std::string& state) {
  return state == "succeeded" || state == "failed" || state == "cancelled";
}

// The guarantee (spec §6.2): a document with an `update` result says `succeeded`. Returns
// false, and fails the test, for one that doesn't.
inline bool UpdateOnlyWhenSucceeded(const json::object& doc, const std::string& where) {
  const json::value* result = doc.if_contains("result");
  const bool update = result != nullptr && result->is_object() && result->as_object().contains("kind") &&
                      result->as_object().at("kind") == json::value("update");
  if (update && Wire::StateOf(doc) != "succeeded") {
    ADD_FAILURE() << where << ": a statement that is \"" << Wire::StateOf(doc)
                  << "\" carries an update result: " << json::serialize(doc);
    return false;
  }
  return true;
}

// A statement without rows, and what it must answer.
struct Kind {
  std::string name;
  std::string sql;     // "{n}" becomes a number that differs from run to run
  json::value result;  // the statement document's `result`
  // What the session document says once the statement's answer has come (spec §5.2);
  // empty: not looked at.
  std::string transaction;
};

// How the statement is started.
enum class Path { kStreamedSubmit, kStreamedPrepared, kStatusSubmit, kRespondAsync };

inline const char* PathName(Path path) {
  switch (path) {
    case Path::kStreamedSubmit: return "streamed submit";
    case Path::kStreamedPrepared: return "streamed prepared execute";
    case Path::kStatusSubmit: return "status-document submit";
    case Path::kRespondAsync: return "respond-async submit";
  }
  return "?";
}

inline std::string Numbered(std::string sql) {
  static std::atomic<int> next{0};
  const std::string n = std::to_string(++next);
  for (auto at = sql.find("{n}"); at != std::string::npos; at = sql.find("{n}")) sql.replace(at, 3, n);
  return sql;
}

// Stops holding the engine thread when a forced run ends, however it ends. Declared after
// the Started it guards, so it goes first: a request still in flight is waited for (its
// future) only once the engine thread it waits on is free.
class LetGoAtExit {
 public:
  explicit LetGoAtExit(Gate& gate) : gate_(gate) {}
  ~LetGoAtExit() { gate_.Disarm(); }
  LetGoAtExit(const LetGoAtExit&) = delete;
  LetGoAtExit& operator=(const LetGoAtExit&) = delete;

 private:
  Gate& gate_;
};

// A started statement: its request is in flight on another thread (or answered already).
struct Started {
  std::string target;
  std::string body;
  Headers headers;  // with the Idempotency-Key a replay repeats
  std::future<Answer> answer;
};

inline Started Start(const Wire& wire, const Kind& kind, Path path, Headers extra = {}) {
  static std::atomic<int> next_key{0};
  Started started;
  const std::string sql = Numbered(kind.sql);
  started.headers = std::move(extra);
  started.headers["Idempotency-Key"] = "update-outcome-" + std::to_string(++next_key);
  if (path == Path::kStreamedPrepared) {
    const Answer prepared = wire.Post(wire.session + "/prepared", json::serialize(json::object{{"sql", sql}}));
    EXPECT_EQ(prepared.status, 201) << prepared.body;
    started.target = wire.session + "/prepared/" + std::string(prepared.Json().at("prepared_id").as_string()) + "/execute";
    started.body = R"({"parameters": []})";
  } else {
    started.target = wire.session + "/statements";
    started.body = json::serialize(json::object{{"sql", sql}});
  }
  switch (path) {
    case Path::kStreamedSubmit:
    case Path::kStreamedPrepared: started.headers["Accept"] = kArrow; break;
    case Path::kStatusSubmit: started.headers["Prefer"] = "wait=30"; break;
    case Path::kRespondAsync: started.headers["Prefer"] = "respond-async"; break;
  }
  started.answer = std::async(std::launch::async, [wire, target = started.target, body = started.body,
                                                   headers = started.headers] { return wire.PostAlone(target, body, headers); });
  return started;
}

// The statement of `started`, by replaying its request (spec §6.7): the document of the
// statement the key created, in whatever state it is.
inline json::object Replay(const Wire& wire, const Started& started) {
  const Answer replay = wire.Post(started.target, started.body, started.headers);
  EXPECT_EQ(replay.status, 200) << replay.body;
  EXPECT_TRUE(replay.IsJson()) << replay.body;
  return replay.Json();
}

// What a forced run saw.
struct Seen {
  int holds = 0;           // times the engine thread was held after the statement's work
  int documents = 0;       // documents read while it was held
  json::object answer;     // the submit's own answer (a statement document)
  int answer_status = 0;
  json::object last;       // the statement once it had ended
  std::string statement_id;
};

// Lets the held engine thread go, stop by stop, until the statement has ended; at each
// stop `while_held` runs (the statement's id in hand) and the statement is read over the
// replay, status and long-poll paths. Returns what it saw.
template <class WhileHeld>
Seen FollowToTheEnd(const Wire& wire, Gate& gate, Started& started, WhileHeld&& while_held) {
  Seen seen;
  const auto deadline = std::chrono::steady_clock::now() + kPatience;
  std::future<json::object> long_poll;
  const LetGoAtExit let_go(gate);  // before the long-poll is waited for, if this ends early
  bool answered = false;
  for (;;) {
    if (gate.WaitHeld(std::chrono::milliseconds(5))) {
      seen.holds += 1;
      const std::string where = "held after the work, stop " + std::to_string(seen.holds);
      const json::object replayed = Replay(wire, started);
      UpdateOnlyWhenSucceeded(replayed, where + ", idempotent replay");
      seen.documents += 1;
      if (const json::value* id = replayed.if_contains("statement_id"); id != nullptr && id->is_string()) {
        seen.statement_id = std::string(id->as_string());
      }
      if (!seen.statement_id.empty()) {
        UpdateOnlyWhenSucceeded(wire.Status(seen.statement_id), where + ", status");
        UpdateOnlyWhenSucceeded(wire.Status(seen.statement_id, {{"Prefer", "wait=0"}}), where + ", status with wait=0");
        seen.documents += 2;
        if (!long_poll.valid()) {
          // A long-poll that is waiting while the statement ends (spec §6.3).
          long_poll = std::async(std::launch::async, [wire, id = seen.statement_id] {
            const Answer answer = wire.GetAlone(wire.session + "/statements/" + id, {{"Prefer", "wait=30"}});
            EXPECT_EQ(answer.status, 200) << answer.body;
            return answer.Json();
          });
        }
        while_held(seen.statement_id);
      }
      gate.Release();
      continue;
    }
    if (!answered && started.answer.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
      const Answer answer = started.answer.get();
      answered = true;
      seen.answer_status = answer.status;
      seen.answer = answer.Json();
      if (const auto it = answer.headers.find("chargewire-statement-id"); it != answer.headers.end()) {
        seen.statement_id = it->second;
      }
    }
    if (answered && !seen.statement_id.empty()) {
      seen.last = wire.Status(seen.statement_id);
      if (Terminal(Wire::StateOf(seen.last))) break;
    }
    if (std::chrono::steady_clock::now() > deadline) {
      ADD_FAILURE() << "the statement did not end";
      break;
    }
  }
  while (gate.WaitHeld(std::chrono::milliseconds(0))) gate.Release();  // nothing stays held
  if (long_poll.valid()) {
    const json::object polled = long_poll.get();
    UpdateOnlyWhenSucceeded(polled, "the long-poll that waited for the end");
    seen.documents += 1;
  }
  EXPECT_FALSE(gate.gave_up()) << "the engine thread was held and nobody let it go";
  return seen;
}

// One statement without rows, started over `path` with the engine thread held after its
// work: no document read on the way carries an update result before the statement has
// succeeded, and the submit's answer is the finished statement.
inline void ExpectUpdateOnlyWithSucceeded(const Wire& wire, Gate& gate, const Kind& kind, Path path) {
  SCOPED_TRACE(kind.name + ", " + PathName(path));
  gate.Arm();
  Started started = Start(wire, kind, path);
  const LetGoAtExit let_go(gate);
  const Seen seen = FollowToTheEnd(wire, gate, started, [](const std::string&) {});
  gate.Disarm();

  // The forcing step happened: the engine was stopped after the work, and documents were
  // read while it was.
  ASSERT_GE(seen.holds, 1) << "the engine thread was never held after the statement's work";
  ASSERT_GE(seen.documents, 3);

  const bool streamed = path == Path::kStreamedSubmit || path == Path::kStreamedPrepared;
  EXPECT_EQ(seen.answer_status, streamed ? 200 : 201) << json::serialize(seen.answer);
  UpdateOnlyWhenSucceeded(seen.answer, "the submit's answer");
  if (path != Path::kRespondAsync) {
    // The answer to a submit that waited is the statement's final document.
    EXPECT_EQ(Wire::StateOf(seen.answer), "succeeded") << json::serialize(seen.answer);
    ASSERT_TRUE(seen.answer.contains("result")) << json::serialize(seen.answer);
    EXPECT_EQ(seen.answer.at("result"), kind.result) << json::serialize(seen.answer);
    EXPECT_TRUE(seen.answer.contains("finished_at")) << json::serialize(seen.answer);
  }
  EXPECT_EQ(Wire::StateOf(seen.last), "succeeded") << json::serialize(seen.last);
  ASSERT_TRUE(seen.last.contains("result")) << json::serialize(seen.last);
  EXPECT_EQ(seen.last.at("result"), kind.result) << json::serialize(seen.last);
  if (!kind.transaction.empty()) {
    // What the early answer was for (spec §5.2): the session shows what the statement did.
    EXPECT_EQ(wire.Session().at("transaction"), json::value(kind.transaction));
  }
}

// How a cancellation reaches a statement.
enum class CancelBy { kStatementCancel, kSessionCancel, kTimeout };

inline const char* CancelName(CancelBy by) {
  switch (by) {
    case CancelBy::kStatementCancel: return "statement cancel";
    case CancelBy::kSessionCancel: return "session cancel";
    case CancelBy::kTimeout: return "timeout";
  }
  return "?";
}

// A cancellation that arrives once the engine has done the statement's work (the engine
// thread is held right after it): the statement's change is made, so it ends `succeeded`
// with its update result, and its streamed answer says so.
inline void ExpectALateCancelChangesNothing(const Wire& wire, Gate& gate, const Kind& kind, CancelBy by) {
  SCOPED_TRACE(kind.name + ", " + CancelName(by));
  gate.Arm();
  Headers extra;
  // Long enough for the work to be done first on the slowest machine; the engine thread is
  // then held until it has run out.
  if (by == CancelBy::kTimeout) extra["Chargewire-Statement-Timeout"] = "1500";
  Started started = Start(wire, kind, Path::kStreamedSubmit, std::move(extra));
  const LetGoAtExit let_go(gate);
  bool cancelled = false;
  const Seen seen = FollowToTheEnd(wire, gate, started, [&](const std::string& statement_id) {
    if (std::exchange(cancelled, true)) return;
    const std::string reason = by == CancelBy::kTimeout ? "timeout" : "client";
    if (by == CancelBy::kStatementCancel) {
      const Answer answer = wire.Post(wire.session + "/statements/" + statement_id + "/cancel", "");
      EXPECT_EQ(answer.status, 202) << answer.body;
      UpdateOnlyWhenSucceeded(answer.Json(), "the cancel's answer");
    } else if (by == CancelBy::kSessionCancel) {
      const Answer answer = wire.Post(wire.session + "/cancel", "");
      EXPECT_EQ(answer.status, 202) << answer.body;
      EXPECT_EQ(json::serialize(answer.Json().at("cancelled")), "[\"" + statement_id + "\"]");
    }
    // The forcing step: the cancellation reached the engine while the statement, its work
    // done, had not ended yet.
    EXPECT_TRUE(gate.WaitCancelled(reason, kPatience)) << "the engine was never told of the " << reason << " cancel";
    EXPECT_EQ(Wire::StateOf(wire.Status(statement_id)), "running");
  });
  gate.Disarm();

  ASSERT_GE(seen.holds, 1) << "the engine thread was never held after the statement's work";
  ASSERT_TRUE(cancelled) << "no cancellation was sent while the engine thread was held";
  ASSERT_EQ(gate.cancels_before_work_done(), 0) << "a cancellation came before the work was done";

  EXPECT_EQ(seen.answer_status, 200) << json::serialize(seen.answer);
  EXPECT_EQ(Wire::StateOf(seen.answer), "succeeded") << json::serialize(seen.answer);
  EXPECT_EQ(Wire::StateOf(seen.last), "succeeded") << json::serialize(seen.last);
  ASSERT_TRUE(seen.last.contains("result")) << json::serialize(seen.last);
  EXPECT_EQ(seen.last.at("result"), kind.result) << json::serialize(seen.last);
  EXPECT_FALSE(seen.last.contains("error")) << json::serialize(seen.last);
  EXPECT_FALSE(seen.last.contains("cancel_reason")) << json::serialize(seen.last);
}

}  // namespace update_outcome
