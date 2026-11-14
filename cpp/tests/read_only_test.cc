// The read_only session option (spec §5.8) as the SDK owns it, against a backend that only
// records what it is asked: the option never reaches the engine as an option, a backend
// without read-only sessions never gets it at all (unknown_option), the session document's
// read_only is the SDK's own, there is no way back, and bulk ingest is refused before the
// engine is called.
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "chargewire/backend.hpp"
#include "chargewire/server.hpp"
#include "update_outcome.hpp"

namespace {

namespace json = boost::json;
namespace http = boost::beast::http;
using chargewire::Error;
using chargewire::ErrorCode;
using update_outcome::Answer;
using update_outcome::Wire;

struct Seen {
  std::mutex mutex;
  int set_read_only = 0;        // EngineSession::SetReadOnly() calls
  int ingests = 0;              // EngineSession::Ingest() calls
  bool option_passed = false;   // `read_only` reached OpenSession() or SetOptions()
  json::object last_options;    // what SetOptions() last got
  std::atomic<bool> refuse{false};  // SetReadOnly() throws, as with a transaction open
};

class RecordingSession : public chargewire::EngineSession {
 public:
  explicit RecordingSession(std::shared_ptr<Seen> seen) : seen_(std::move(seen)) {}
  void SetReadOnly() override {
    std::lock_guard lock(seen_->mutex);
    seen_->set_read_only++;
    if (seen_->refuse) {
      throw Error(ErrorCode::kInvalidOptionValue, "a transaction with work in it is open");
    }
  }
  chargewire::OptionErrors SetOptions(const json::object& options) override {
    std::lock_guard lock(seen_->mutex);
    if (options.contains("read_only")) seen_->option_passed = true;
    seen_->last_options = options;
    chargewire::OptionErrors errors;
    for (const auto& option : options) {
      if (option.key() == "tag") {
        tag_ = option.value();
      } else {
        errors.emplace(std::string(option.key()), Error(ErrorCode::kUnknownOption, "unknown session option"));
      }
    }
    return errors;
  }
  // An engine that reports the option wrongly: the session document must not follow it.
  json::object Options() override { return {{"tag", tag_}, {"read_only", "the engine's own idea"}}; }
  int64_t Ingest(const chargewire::IngestTarget&, std::shared_ptr<arrow::RecordBatchReader>) override {
    std::lock_guard lock(seen_->mutex);
    return seen_->ingests++;
  }

 private:
  std::shared_ptr<Seen> seen_;
  json::value tag_ = nullptr;
};

class RecordingBackend : public chargewire::Backend {
 public:
  RecordingBackend(std::shared_ptr<Seen> seen, bool read_only_sessions)
      : seen_(std::move(seen)), read_only_sessions_(read_only_sessions) {}
  chargewire::ServerIdentity Identity() override { return {"read-only-test", "0"}; }
  chargewire::BackendCapabilities Capabilities() override {
    return {.ingest = true, .read_only_sessions = read_only_sessions_};
  }
  std::unique_ptr<chargewire::EngineSession> OpenSession(const chargewire::Principal&, const std::string&,
                                                         const json::object& options) override {
    if (options.contains("read_only")) {
      std::lock_guard lock(seen_->mutex);
      seen_->option_passed = true;
    }
    return std::make_unique<RecordingSession>(seen_);
  }

 private:
  std::shared_ptr<Seen> seen_;
  bool read_only_sessions_;
};

class ReadOnlyOption : public ::testing::Test {
 protected:
  void Start(bool read_only_sessions) {
    chargewire::ServerOptions options;
    options.host = "127.0.0.1";
    server_ = std::make_unique<chargewire::Server>(options, std::make_shared<RecordingBackend>(seen_, read_only_sessions),
                                                   std::make_shared<update_outcome::AnyPassword>());
    server_->Start();
    wire_ = Wire::Open(server_->port());
  }
  void TearDown() override {
    if (server_) server_->Stop();
  }
  Answer Create(const std::string& options) { return wire_.Post("/chargewire/v1/sessions", R"({"options": )" + options + "}"); }
  Answer Patch(const std::string& session, const std::string& options) {
    return wire_.connection->Exchange(http::verb::patch, session, wire_.With({}), R"({"options": )" + options + "}");
  }
  static std::string SessionOf(const Answer& created) {
    return "/chargewire/v1/sessions/" + std::string(created.Json().at("session_id").as_string());
  }
  bool Lists(const std::string& capability) {
    const json::object discovery = wire_.connection->Exchange(http::verb::get, "/chargewire").Json();
    for (const auto& item : discovery.at("capabilities").as_array()) {
      if (item.as_string() == capability) return true;
    }
    return false;
  }
  int SetReadOnlyCalls() {
    std::lock_guard lock(seen_->mutex);
    return seen_->set_read_only;
  }
  bool OptionPassed() {
    std::lock_guard lock(seen_->mutex);
    return seen_->option_passed;
  }

  std::shared_ptr<Seen> seen_ = std::make_shared<Seen>();
  std::unique_ptr<chargewire::Server> server_;
  Wire wire_;
};

std::string CodeOf(const Answer& answer) {
  const json::object body = answer.Json();
  const json::value* code = body.if_contains("code");
  return code && code->is_string() ? std::string(code->as_string()) : std::string();
}

std::string OptionError(const Answer& patched, const std::string& option) {
  const json::object errors = patched.Json().at("errors").as_object();
  const json::value* error = errors.if_contains(option);
  return error ? std::string(error->as_object().at("code").as_string()) : std::string();
}

// An adapter that cannot enforce read-only never gets to accept the option: the SDK
// answers unknown_option itself, so a client is never told read-only by a server that isn't.
TEST_F(ReadOnlyOption, ABackendWithoutReadOnlySessionsNeverSeesTheOption) {
  Start(false);
  EXPECT_FALSE(Lists("read_only_sessions"));
  const Answer created = Create(R"({"read_only": true})");
  EXPECT_EQ(created.status, 400) << created.body;
  EXPECT_EQ(CodeOf(created), "unknown_option");
  EXPECT_NE(created.body.find("read_only"), std::string::npos) << created.body;
  const Answer falsely = Create(R"({"read_only": false})");
  EXPECT_EQ(CodeOf(falsely), "unknown_option") << falsely.body;

  const Answer patched = Patch(wire_.session, R"({"read_only": true, "tag": "kept"})");
  EXPECT_EQ(patched.status, 200) << patched.body;
  EXPECT_EQ(OptionError(patched, "read_only"), "unknown_option");
  EXPECT_EQ(patched.Json().at("options").as_object().at("tag").as_string(), "kept");  // the others still apply
  EXPECT_FALSE(patched.Json().at("options").as_object().contains("read_only")) << patched.body;
  EXPECT_FALSE(wire_.Session().at("options").as_object().contains("read_only"));
  EXPECT_EQ(SetReadOnlyCalls(), 0);
  EXPECT_FALSE(OptionPassed());
}

TEST_F(ReadOnlyOption, ASessionIsCreatedReadOnlyAndTheDocumentIsTheSdksOwn) {
  Start(true);
  EXPECT_TRUE(Lists("read_only_sessions"));
  // Every session lists the option, whatever the engine's Options() says about it.
  EXPECT_EQ(wire_.Session().at("options").as_object().at("read_only"), json::value(false));
  const Answer created = Create(R"({"read_only": true})");
  ASSERT_EQ(created.status, 201) << created.body;
  EXPECT_EQ(created.Json().at("options").as_object().at("read_only"), json::value(true));
  EXPECT_EQ(SetReadOnlyCalls(), 1);
  const Answer read = wire_.Get(SessionOf(created));
  EXPECT_EQ(read.Json().at("options").as_object().at("read_only"), json::value(true));
  EXPECT_FALSE(OptionPassed());
  const Answer writable = Create(R"({"read_only": false})");
  ASSERT_EQ(writable.status, 201) << writable.body;
  EXPECT_EQ(writable.Json().at("options").as_object().at("read_only"), json::value(false));
  EXPECT_EQ(SetReadOnlyCalls(), 1);
}

TEST_F(ReadOnlyOption, ThereIsNoWayBack) {
  Start(true);
  const Answer on = Patch(wire_.session, R"({"read_only": true})");
  EXPECT_EQ(on.Json().at("errors").as_object().size(), 0u) << on.body;
  EXPECT_EQ(on.Json().at("options").as_object().at("read_only"), json::value(true));
  EXPECT_EQ(SetReadOnlyCalls(), 1);

  const Answer off = Patch(wire_.session, R"({"read_only": false, "tag": "applied"})");
  EXPECT_EQ(off.status, 200) << off.body;
  EXPECT_EQ(OptionError(off, "read_only"), "read_only");
  EXPECT_EQ(off.Json().at("options").as_object().at("read_only"), json::value(true));
  EXPECT_EQ(off.Json().at("options").as_object().at("tag").as_string(), "applied");
  EXPECT_EQ(wire_.Session().at("options").as_object().at("read_only"), json::value(true));

  const Answer again = Patch(wire_.session, R"({"read_only": true})");
  EXPECT_EQ(again.Json().at("errors").as_object().size(), 0u) << again.body;
  EXPECT_EQ(SetReadOnlyCalls(), 1);  // the engine is told once
  EXPECT_FALSE(OptionPassed());
}

TEST_F(ReadOnlyOption, AnEngineThatCannotSwitchNowLeavesTheSessionReadWrite) {
  Start(true);
  seen_->refuse = true;
  const Answer refused = Patch(wire_.session, R"({"read_only": true})");
  EXPECT_EQ(refused.status, 200) << refused.body;
  EXPECT_EQ(OptionError(refused, "read_only"), "invalid_option_value");
  EXPECT_EQ(refused.Json().at("options").as_object().at("read_only"), json::value(false));
  // At creation the same failure fails the request, naming the option, and no session is made.
  const Answer created = Create(R"({"read_only": true})");
  EXPECT_EQ(created.status, 400) << created.body;
  EXPECT_EQ(CodeOf(created), "invalid_option_value");
  EXPECT_NE(created.body.find("read_only"), std::string::npos) << created.body;
  seen_->refuse = false;
  const Answer accepted = Patch(wire_.session, R"({"read_only": true})");
  EXPECT_EQ(accepted.Json().at("options").as_object().at("read_only"), json::value(true)) << accepted.body;
}

TEST_F(ReadOnlyOption, AValueThatIsNotABooleanIsInvalid) {
  Start(true);
  const Answer created = Create(R"({"read_only": "true"})");
  EXPECT_EQ(created.status, 400) << created.body;
  EXPECT_EQ(CodeOf(created), "invalid_option_value");
  const Answer patched = Patch(wire_.session, R"({"read_only": 1})");
  EXPECT_EQ(OptionError(patched, "read_only"), "invalid_option_value") << patched.body;
  EXPECT_EQ(SetReadOnlyCalls(), 0);
}

TEST_F(ReadOnlyOption, BulkIngestIsRefusedBeforeTheEngineIsCalled) {
  Start(true);
  const Answer created = Create(R"({"read_only": true})");
  ASSERT_EQ(created.status, 201) << created.body;
  const std::string session = SessionOf(created);
  const Answer one_request = wire_.Post(session + "/ingest?table=t", "not even an Arrow stream",
                                        {{"Content-Type", "application/vnd.apache.arrow.stream"}});
  EXPECT_EQ(one_request.status, 403) << one_request.body;
  EXPECT_EQ(CodeOf(one_request), "read_only");
  EXPECT_EQ(one_request.Json().at("sqlstate").as_string(), "25006");
  const Answer partitioned = wire_.Post(session + "/ingests", R"({"table": "t"})");
  EXPECT_EQ(partitioned.status, 403) << partitioned.body;
  EXPECT_EQ(CodeOf(partitioned), "read_only");
  std::lock_guard lock(seen_->mutex);
  EXPECT_EQ(seen_->ingests, 0);
}

}  // namespace
