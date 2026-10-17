#include "api.hpp"

#include <algorithm>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <format>

#include <arrow/array.h>
#include <arrow/table.h>
#include <arrow/util/config.h>  // ARROW_VERSION_STRING

#include "ids.hpp"
#include "ipc.hpp"
#include "json.hpp"
#include "metadata.hpp"
#include "upload.hpp"

namespace warpsql::detail {

namespace {

// How often a result stream waiting for its next batch checks that its client is still
// there, so an abandoned query is cancelled rather than left running.
constexpr auto kClientCheck = std::chrono::seconds(1);

// The result stream's pipeline (Api::StreamBatches): batches being encoded at once
// (compressing is slow, so more of them then), the bytes they may hold, and how much
// encoded data is gathered into one write.
constexpr std::size_t kEncodeWindow = 2;
constexpr std::size_t kEncodeWindowZstd = 8;
constexpr std::size_t kEncodeBytes = 64 * 1024 * 1024;
constexpr std::size_t kGatherBytes = 1024 * 1024;

// WARPSQL_STREAM_STATS=1 logs, for every result stream and statement, how long each
// stage of the pipeline took (stderr), to see which one limits a stream.
bool StreamStatsEnabled() {
  static const bool enabled = [] {
    const char* value = std::getenv("WARPSQL_STREAM_STATS");
    return value != nullptr && std::string_view(value) == "1";
  }();
  return enabled;
}

double Seconds(Clock::duration d) { return std::chrono::duration<double>(d).count(); }

int64_t BatchBytes(const arrow::RecordBatch& batch) {
  int64_t bytes = 0;
  for (const auto& column : batch.columns()) {
    for (const auto& buffer : column->data()->buffers) {
      if (buffer) bytes += buffer->size();
    }
  }
  return bytes;
}

std::vector<std::string_view> Segments(std::string_view path) {
  std::vector<std::string_view> out;
  while (!path.empty()) {
    const auto start = path.find_first_not_of('/');
    if (start == std::string_view::npos) {
      break;
    }
    path.remove_prefix(start);
    const auto end = path.find('/');
    out.push_back(path.substr(0, end));
    path = end == std::string_view::npos ? std::string_view{} : path.substr(end);
  }
  return out;
}

int64_t ParseInt(std::string_view text, std::string_view what) {
  int64_t value = 0;
  const auto* end = text.data() + text.size();
  auto [ptr, ec] = std::from_chars(text.data(), end, value);
  if (ec != std::errc{} || ptr != end) {
    throw Error(ErrorCode::kBadRequest, std::format("{} must be an integer", what));
  }
  return value;
}

std::string_view Trim(std::string_view text) {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) text.remove_prefix(1);
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) text.remove_suffix(1);
  return text;
}

// RFC 7240 Prefer: "wait=N" and "respond-async", comma-separated.
struct Preferences {
  std::optional<std::chrono::seconds> wait;
  bool respond_async = false;
};

Preferences ParsePrefer(std::string_view header) {
  Preferences prefs;
  while (!header.empty()) {
    const auto comma = header.find(',');
    std::string_view item = Trim(header.substr(0, comma));
    header = comma == std::string_view::npos ? std::string_view{} : header.substr(comma + 1);
    if (item == "respond-async") {
      prefs.respond_async = true;
    } else if (item.starts_with("wait=")) {
      prefs.wait = std::chrono::seconds(std::max<int64_t>(0, ParseInt(item.substr(5), "wait")));
    }
  }
  return prefs;
}

bool AcceptsArrow(std::string_view accept) {
  return accept.find(kArrowStream) != std::string_view::npos;
}

Response NoContent() {
  Response response;
  response.status = http::status::no_content;
  response.headers.emplace_back("X-Content-Type-Options", "nosniff");
  response.headers.emplace_back("Cache-Control", "no-store");
  return response;
}

json::object OptionErrorsDocument(const OptionErrors& errors) {
  json::object out;
  for (const auto& [name, error] : errors) {
    out[name] = json::object{{"code", CodeName(error.code())}, {"detail", error.what()}};
  }
  return out;
}

}  // namespace

bool RetainedQuota::Change(const std::string& principal, int64_t bytes) {
  std::lock_guard lock(mutex_);
  int64_t& used = used_[principal];
  if (bytes > 0 && used + bytes > limit_) {
    return false;
  }
  used = std::max<int64_t>(0, used + bytes);
  return true;
}

template <typename Fn>
Task<std::invoke_result_t<Fn>> Api::OnEngine(Session& session, Fn work) {
  using Result = std::invoke_result_t<Fn>;
  if constexpr (std::is_void_v<Result>) {
    co_await net::co_spawn(
        session.strand(), [work = std::move(work)]() mutable -> Task<void> {
          work();
          co_return;
        },
        net::use_awaitable);
  } else {
    co_return co_await net::co_spawn(
        session.strand(), [work = std::move(work)]() mutable -> Task<Result> { co_return work(); },
        net::use_awaitable);
  }
}

Task<Response> Api::Handle(Request& request) {
  Response response;
  try {
    response = co_await Route(request);
  } catch (const Error& error) {
    response = ProblemResponse(error, request.request_id);
  } catch (const std::exception&) {
    response = ProblemResponse(Error(ErrorCode::kExecutionError, "internal server error"),
                               request.request_id);
  }
  response.headers.emplace_back("Warp-Request-Id", request.request_id);

  // CORS: only for origins on the allow-list (spec §9.5).
  const std::string_view origin = request.Header("Origin");
  const auto& allowed = state_.options.cors_allowed_origins;
  if (!origin.empty() && std::ranges::find(allowed, origin) != allowed.end()) {
    response.headers.emplace_back("Access-Control-Allow-Origin", std::string(origin));
    response.headers.emplace_back("Vary", "Origin");
    response.headers.emplace_back(
        "Access-Control-Expose-Headers",
        "Location, Warp-Statement-Id, Warp-IPC-Compression, Warp-Request-Id, Retry-After");
    if (request.method == http::verb::options) {
      response.headers.emplace_back("Access-Control-Allow-Methods",
                                    "GET, POST, PATCH, DELETE, OPTIONS");
      response.headers.emplace_back(
          "Access-Control-Allow-Headers",
          "Authorization, Content-Type, Accept, Prefer, Idempotency-Key, "
          "Warp-Statement-Timeout, Warp-Retain-Result, Warp-Query-Tag, Warp-IPC-Compression");
      response.headers.emplace_back("Access-Control-Max-Age", "600");
    }
  }
  co_return response;
}

Task<Response> Api::Route(Request& request) {
  if (request.method == http::verb::options) {
    co_return NoContent();  // CORS preflight; Handle() adds the headers
  }
  const auto segments = Segments(request.path);
  const auto method = request.method;
  auto wrong_method = [] {
    return Error(ErrorCode::kMethodNotAllowed, "this endpoint does not take that method");
  };

  if (segments.size() == 1 && segments[0] == "warp") {
    if (method != http::verb::get) throw wrong_method();
    co_return co_await Discovery(request);
  }
  if (segments.size() < 3 || segments[0] != "warp" || segments[1] != "v1") {
    throw Error(ErrorCode::kNotFound, "no such endpoint");
  }
  const std::vector<std::string_view> rest(segments.begin() + 2, segments.end());

  if (rest.size() == 1 && rest[0] == "health") {
    if (method != http::verb::get) throw wrong_method();
    co_return co_await Health(request);
  }
  if (rest.size() == 2 && rest[0] == "auth" && rest[1] == "token") {
    if (method != http::verb::post) throw wrong_method();
    co_return co_await IssueToken(request);
  }
  if (rest[0] != "sessions") {
    throw Error(ErrorCode::kNotFound, "no such endpoint");
  }
  if (rest.size() == 1) {
    if (method != http::verb::post) throw wrong_method();
    co_return co_await CreateSession(request);
  }

  const std::string session_id(rest[1]);
  if (rest.size() == 2) {
    switch (method) {
      case http::verb::get: co_return co_await GetSession(request, session_id);
      case http::verb::patch: co_return co_await UpdateSession(request, session_id);
      case http::verb::delete_: co_return co_await DeleteSession(request, session_id);
      default: throw wrong_method();
    }
  }
  if (rest.size() == 3 && (rest[2] == "commit" || rest[2] == "rollback")) {
    if (method != http::verb::post) throw wrong_method();
    co_return co_await EndTransaction(request, session_id, rest[2] == "commit");
  }
  if (rest[2] == "prepared") {
    if (rest.size() == 3) {
      if (method != http::verb::post) throw wrong_method();
      co_return co_await PrepareStatement(request, session_id);
    }
    const std::string prepared_id(rest[3]);
    if (rest.size() == 4) {
      if (method != http::verb::delete_) throw wrong_method();
      co_return co_await ClosePrepared(request, session_id, prepared_id);
    }
    if (rest.size() == 5 && rest[4] == "execute") {
      if (method != http::verb::post) throw wrong_method();
      co_return co_await ExecutePrepared(request, session_id, prepared_id);
    }
    throw Error(ErrorCode::kNotFound, "no such endpoint");
  }
  if (rest.size() == 3 && rest[2] == "cancel") {
    if (method != http::verb::post) throw wrong_method();
    co_return co_await CancelSession(request, session_id);
  }
  if (rest.size() == 3 && rest[2] == "ingest") {
    if (method != http::verb::post) throw wrong_method();
    co_return co_await Ingest(request, session_id);
  }
  if (rest.size() == 4 && rest[2] == "metadata") {
    if (method != http::verb::get) throw wrong_method();
    co_return co_await Metadata(request, session_id, rest[3]);
  }
  if (rest[2] != "statements") {
    throw Error(ErrorCode::kNotFound, "no such endpoint");
  }
  if (rest.size() == 3) {
    if (method != http::verb::post) throw wrong_method();
    co_return co_await SubmitStatement(request, session_id);
  }

  const std::string statement_id(rest[3]);
  if (rest.size() == 4) {
    switch (method) {
      case http::verb::get: co_return co_await GetStatement(request, session_id, statement_id);
      case http::verb::delete_:
        co_return co_await DeleteStatement(request, session_id, statement_id);
      default: throw wrong_method();
    }
  }
  if (rest.size() == 5 && rest[4] == "result") {
    if (method != http::verb::get) throw wrong_method();
    co_return co_await GetResult(request, session_id, statement_id);
  }
  if (rest.size() == 5 && rest[4] == "cancel") {
    if (method != http::verb::post) throw wrong_method();
    co_return co_await CancelStatement(request, session_id, statement_id);
  }
  throw Error(ErrorCode::kNotFound, "no such endpoint");
}

// ---- Discovery, health, authentication ----

Task<Response> Api::Discovery(Request&) {
  const ServerIdentity identity = state_.backend->Identity();
  json::object doc{
      {"protocol", "warp-sql"},
      {"protocol_versions", json::array{"1"}},
      {"draft", "0.1.0"},
      {"server", {{"name", identity.name}, {"version", identity.version}}},
      {"capabilities",
       json::array{"prepared_statements", "transactions", "ingest", "result_retention",
                   "long_poll", "ipc_compression_zstd"}},
      {"auth", {{"grant_types", state_.authenticator->GrantTypes()}}},
  };
  co_return JsonResponse(http::status::ok, doc);
}

Task<Response> Api::Health(Request&) {
  auto not_serving = [](std::string_view reason) {
    return JsonResponse(http::status::service_unavailable,
                        json::object{{"status", "not_serving"}, {"reason", reason}});
  };
  if (state_.draining) {
    co_return not_serving("draining");
  }

  // The check runs on the engine pool and may hang; the answer must not (spec §3.2).
  struct Outcome {
    std::mutex mutex;
    bool done = false;
    bool ok = false;
    Signal signal;
  };
  auto outcome = std::make_shared<Outcome>();
  net::post(state_.engine_pool, [backend = state_.backend, outcome] {
    bool ok = true;
    try {
      backend->CheckHealth();
    } catch (...) {
      ok = false;
    }
    {
      std::lock_guard lock(outcome->mutex);
      outcome->done = true;
      outcome->ok = ok;
    }
    outcome->signal.Notify();
  });

  const auto deadline = Clock::now() + state_.options.health_timeout;
  uint64_t seen = 0;
  for (;;) {
    {
      std::lock_guard lock(outcome->mutex);
      if (outcome->done) {
        if (!outcome->ok) co_return not_serving("check_failed");
        co_return JsonResponse(http::status::ok, json::object{{"status", "serving"}});
      }
    }
    if (Clock::now() >= deadline) {
      co_return not_serving("check_failed");
    }
    seen = co_await outcome->signal.WaitChange(seen, deadline);
  }
}

Task<Response> Api::IssueToken(Request& request) {
  const json::object body = ParseObject(request.body);
  const std::string grant_type = RequiredString(body, "grant_type");
  auto authenticator = state_.authenticator;
  Principal principal;
  if (grant_type == "password") {
    const std::string username = RequiredString(body, "username");
    const std::string password = RequiredString(body, "password");
    principal = co_await net::co_spawn(
        state_.engine_pool,
        [=]() -> Task<Principal> {
          co_return authenticator->AuthenticatePassword(username, password);
        },
        net::use_awaitable);
  } else if (grant_type == "token_exchange") {
    const std::string subject_token = RequiredString(body, "subject_token");
    principal = co_await net::co_spawn(
        state_.engine_pool,
        [=]() -> Task<Principal> { co_return authenticator->ExchangeToken(subject_token); },
        net::use_awaitable);
  } else {
    throw Error(ErrorCode::kBadRequest, std::format("unsupported grant_type '{}'", grant_type));
  }
  const auto issued = state_.tokens->Issue(principal);
  co_return JsonResponse(http::status::ok,
                         json::object{{"access_token", issued.token},
                                      {"token_type", "Bearer"},
                                      {"expires_in", issued.expires_in.count()}});
}

Principal Api::Authenticate(const Request& request) const {
  return state_.tokens->Validate(BearerToken(request.Header("Authorization")));
}

std::shared_ptr<Session> Api::OwnSession(const Request& request, const std::string& session_id) {
  const Principal principal = Authenticate(request);
  auto session = state_.sessions.Find(session_id, principal);
  session->Touch();
  return session;
}

void Api::RejectIfDraining() const {
  if (state_.draining) {
    throw Error(ErrorCode::kServerDraining, "the server is shutting down");
  }
}

// ---- Sessions ----

json::object Api::SessionDocument(Session& session) const {
  return json::object{
      {"session_id", session.id()},
      {"options", session.options()},
      {"idle_timeout_s", state_.options.session_idle_timeout.count()},
  };
}

Task<Response> Api::CreateSession(Request& request) {
  const Principal principal = Authenticate(request);
  RejectIfDraining();
  const json::object body = ParseObject(request.body);
  json::object options;
  if (const json::value* given = body.if_contains("options"); given && !given->is_null()) {
    if (!given->is_object()) {
      throw Error(ErrorCode::kBadRequest, "'options' must be an object");
    }
    options = given->as_object();
  }

  auto session = std::make_shared<Session>(NewId("s"), principal,
                                           net::make_strand(state_.engine_pool.get_executor()));
  auto backend = state_.backend;
  auto opened = co_await OnEngine(*session, [backend, principal, options] {
    auto engine = backend->OpenSession(principal, options);
    json::object current = engine->Options();
    return std::make_pair(std::move(engine), std::move(current));
  });
  session->SetEngine(std::move(opened.first));
  session->set_options(std::move(opened.second));
  state_.sessions.Add(session);

  Response response = JsonResponse(http::status::created, SessionDocument(*session));
  response.headers.emplace_back("Location", "/warp/v1/sessions/" + session->id());
  co_return response;
}

Task<Response> Api::GetSession(Request& request, const std::string& session_id) {
  auto session = OwnSession(request, session_id);
  co_return JsonResponse(http::status::ok, SessionDocument(*session));
}

Task<Response> Api::UpdateSession(Request& request, const std::string& session_id) {
  auto session = OwnSession(request, session_id);
  const json::object body = ParseObject(request.body);
  const json::value* given = body.if_contains("options");
  if (given == nullptr || !given->is_object()) {
    throw Error(ErrorCode::kBadRequest, "'options' must be an object");
  }
  json::object options = given->as_object();
  auto result = co_await OnEngine(*session, [session, options] {
    OptionErrors errors = session->engine().SetOptions(options);
    json::object current = session->engine().Options();
    return std::make_pair(std::move(errors), std::move(current));
  });
  session->set_options(result.second);
  co_return JsonResponse(http::status::ok,
                         json::object{{"options", std::move(result.second)},
                                      {"errors", OptionErrorsDocument(result.first)}});
}

Task<Response> Api::DeleteSession(Request& request, const std::string& session_id) {
  auto session = OwnSession(request, session_id);
  state_.sessions.Remove(session_id);
  session->Close(CancelReason::kSessionClosed);
  net::post(session->strand(), [session] { session->DestroyEngine(); });
  co_return NoContent();
}

Task<Response> Api::EndTransaction(Request& request, const std::string& session_id,
                                   bool commit) {
  auto session = OwnSession(request, session_id);
  co_await OnEngine(*session, [session, commit] {
    if (commit) {
      session->engine().Commit();
    } else {
      session->engine().Rollback();
    }
  });
  co_return NoContent();
}

// ---- Statements ----

json::object Api::StatementDocument(Session& session, Statement& statement) const {
  std::optional<double> progress;
  if (statement.state() == StatementState::kRunning) {
    progress = session.Progress();
  }
  return statement.Document(progress);
}

void Api::Cancel(const std::shared_ptr<Session>& session, const std::shared_ptr<Statement>& statement,
                 CancelReason reason) {
  if (statement->RequestCancel(reason)) {
    session->InterruptIfRunning(statement.get());
    // A running statement may not have reached the engine yet (the backend is still
    // checking or queueing it), and an engine forgets an interrupt that comes before
    // its query starts (DuckDB clears it then): keep interrupting until it ends.
    KeepInterrupting(session, statement);
  }
}

void Api::KeepInterrupting(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement) {
  auto timer = std::make_shared<net::steady_timer>(state_.io, std::chrono::milliseconds(100));
  timer->async_wait([this, timer, session = std::move(session),
                     statement = std::move(statement)](boost::system::error_code ec) mutable {
    if (ec || statement->IsTerminal()) {
      return;
    }
    session->InterruptIfRunning(statement.get());
    KeepInterrupting(std::move(session), std::move(statement));
  });
}

void Api::Run(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement,
              StatementWork work) {
  net::post(session->strand(), [session, statement, work = std::move(work)] {
    // Marked before it may start, cleared once it is over: interrupts meant for this
    // statement reach the engine only while it runs.
    struct Running {
      Session& session;
      Running(Session& s, const Statement* st) : session(s) { session.SetRunning(st); }
      ~Running() { session.SetRunning(nullptr); }
    } running{*session, statement.get()};
    if (!statement->BeginRunning()) {
      return;  // cancelled while queued
    }
    try {
      const auto started = Clock::now();
      StatementOutcome outcome = work(session->engine());
      if (outcome.rows) {
        statement->SetRows(outcome.rows->schema(), outcome.finish_if_abandoned);
        Clock::duration producing{}, handing_over{};
        int64_t produced = 0, produced_bytes = 0;
        struct Report {
          Clock::time_point started;
          Clock::duration& producing;
          Clock::duration& handing_over;
          int64_t& produced;
          int64_t& produced_bytes;
          ~Report() {
            if (!StreamStatsEnabled()) return;
            std::fprintf(stderr,
                         "warpsql statement: %lld batches, %.1f MB in %.3f s: engine producing "
                         "%.3f s (%.0f MB/s), waiting for readers %.3f s\n",
                         static_cast<long long>(produced), produced_bytes / 1e6,
                         Seconds(Clock::now() - started), Seconds(producing),
                         Seconds(producing) > 0 ? produced_bytes / 1e6 / Seconds(producing) : 0.0,
                         Seconds(handing_over));
          }
        } report{started, producing, handing_over, produced, produced_bytes};
        // A read deleted before it started producing stops before its first batch.
        for (; !statement->CancelRequested();) {
          const auto next_started = Clock::now();
          auto next = outcome.rows->Next();
          producing += Clock::now() - next_started;
          if (!next.ok()) {
            throw Error(ErrorCode::kExecutionError, next.status().message());
          }
          std::shared_ptr<arrow::RecordBatch> batch = std::move(next).ValueUnsafe();
          if (batch == nullptr) {
            break;
          }
          if (batch->num_rows() == 0) {
            continue;
          }
          produced += 1;
          produced_bytes += BatchBytes(*batch);
          const auto add_started = Clock::now();
          const bool added = statement->AddBatch(std::move(batch));
          handing_over += Clock::now() - add_started;
          if (!added) {
            break;  // cancelled, or over the retention quota
          }
        }
      } else {
        statement->SetUpdate(outcome.rows_affected);
      }
      outcome.rows.reset();  // the engine runs one result at a time
      if (session->engine().TakeOptionsChanged()) {
        try {
          session->set_options(session->engine().Options());
        } catch (const std::exception&) {
          // the document keeps the options it had; the statement itself succeeded
        }
      }
      statement->Finish();
    } catch (const Error& error) {
      statement->Fail(error);
    } catch (const std::exception& error) {
      statement->Fail(Error(ErrorCode::kExecutionError, error.what()));
    }
  });
}

void Api::ScheduleTimeout(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement,
                          std::chrono::milliseconds timeout) {
  auto timer = std::make_shared<net::steady_timer>(state_.io, timeout);
  timer->async_wait([this, timer, session, statement](boost::system::error_code ec) {
    if (!ec) {
      Cancel(session, statement, CancelReason::kTimeout);
    }
  });
}

std::optional<IdempotencyStore::Entry> Api::ReserveIdempotencyKey(const Request& request,
                                                                   const Session& session,
                                                                   const std::string& statement_id,
                                                                   const std::string& material) {
  const auto key = request.Header("Idempotency-Key");
  if (key.empty()) {
    return std::nullopt;
  }
  if (key.size() > 255) {
    throw Error(ErrorCode::kBadRequest, "Idempotency-Key is longer than 255 characters");
  }
  const std::string hash = Sha256Hex(material);
  auto existing = state_.idempotency.Reserve(
      session.owner().name, std::string(key),
      {hash, session.id(), statement_id, Clock::now() + state_.options.idempotency_window,
       std::make_shared<std::string>()});
  if (existing && existing->request_hash != hash) {
    throw Error(ErrorCode::kIdempotencyKeyReused,
                "this Idempotency-Key was used for a different request");
  }
  return existing;
}

Task<Response> Api::Launch(Request& request, std::shared_ptr<Session> session, StatementWork work,
                           const std::string& idempotency_material) {
  RejectIfDraining();
  const Preferences prefs = ParsePrefer(request.Header("Prefer"));
  const bool streamed = AcceptsArrow(request.Header("Accept")) && !prefs.respond_async;
  bool retain = !streamed;
  if (const auto header = request.Header("Warp-Retain-Result"); !header.empty()) {
    if (header != "true" && header != "false") {
      throw Error(ErrorCode::kBadRequest, "Warp-Retain-Result must be true or false");
    }
    retain = header == "true";
  }
  std::optional<std::chrono::milliseconds> timeout;
  if (state_.options.default_statement_timeout.count() > 0) {
    timeout = state_.options.default_statement_timeout;
  }
  // A timeout the session chose beats the server's; a request's own beats both.
  if (auto session_default = session->DefaultStatementTimeout()) {
    timeout = session_default->count() > 0 ? session_default : std::nullopt;
  }
  if (const auto header = request.Header("Warp-Statement-Timeout"); !header.empty()) {
    const int64_t ms = ParseInt(header, "Warp-Statement-Timeout");
    if (ms <= 0) {
      throw Error(ErrorCode::kBadRequest, "Warp-Statement-Timeout must be positive");
    }
    timeout = std::chrono::milliseconds(ms);
  }

  const std::string statement_id = NewId("q");
  // Idempotent submission (spec §6.7): a retry gets the original statement.
  if (auto existing =
          ReserveIdempotencyKey(request, *session, statement_id, idempotency_material)) {
    auto original = state_.sessions.Find(existing->session_id, session->owner())
                        ->FindStatement(existing->statement_id);
    co_return JsonResponse(http::status::ok, StatementDocument(*session, *original));
  }

  Statement::Config config;
  config.retain = retain;
  config.retention = state_.options.result_ttl;
  config.reserve_bytes = [this, owner = session->owner().name](int64_t bytes) {
    return state_.quota.Change(owner, bytes);
  };
  auto statement = std::make_shared<Statement>(statement_id, config);
  session->AddStatement(statement);
  Run(session, statement, std::move(work));
  if (timeout) {
    ScheduleTimeout(session, statement, *timeout);
  }

  // Wait for the outcome (streamed) or for the end (status document), within limits.
  const auto wait = prefs.respond_async
                        ? std::chrono::seconds(0)
                        : std::min(prefs.wait.value_or(streamed ? state_.options.max_wait
                                                                : state_.options.default_wait),
                                   state_.options.max_wait);
  const auto deadline = Clock::now() + wait;
  uint64_t seen = statement->changed().version();
  while (Clock::now() < deadline &&
         !(streamed ? statement->HasOutcome() : statement->IsTerminal())) {
    seen = co_await statement->changed().WaitChange(seen, deadline);
  }

  // A statement that failed before producing anything is an error response (§6.1).
  if (statement->state() == StatementState::kFailed && !statement->HasRows()) {
    throw *statement->error();
  }
  if (streamed && statement->HasRows() && statement->state() != StatementState::kFailed &&
      statement->state() != StatementState::kCancelled) {
    statement->ClaimRead(0);
    co_return ResultStream(session, statement, 0, request.Header("Warp-IPC-Compression"));
  }
  Response response = JsonResponse(streamed ? http::status::ok : http::status::created,
                                   StatementDocument(*session, *statement));
  response.headers.emplace_back(
      "Location", std::format("/warp/v1/sessions/{}/statements/{}", session->id(), statement_id));
  response.headers.emplace_back("Warp-Statement-Id", statement_id);
  co_return response;
}

namespace {

// Bind parameters from a JSON body: an array binds positionally (? or $1), an object by
// name ($name) (spec §6.1).
json::value JsonParameters(const json::object& body) {
  const json::value* given = body.if_contains("parameters");
  if (given == nullptr || given->is_null()) {
    return nullptr;
  }
  if (!given->is_array() && !given->is_object()) {
    throw Error(ErrorCode::kBadRequest, "'parameters' must be an array or an object");
  }
  return *given;
}

}  // namespace

Task<Response> Api::SubmitStatement(Request& request, const std::string& session_id) {
  auto session = OwnSession(request, session_id);
  const json::object body = ParseObject(request.body);
  std::string sql = RequiredString(body, "sql");
  json::value parameters = JsonParameters(body);
  StatementWork work = [sql = std::move(sql), parameters = std::move(parameters)](
                           EngineSession& engine) { return engine.Execute(sql, parameters); };
  co_return co_await Launch(request, session, std::move(work), request.path + "\n" + request.body);
}

// ---- Prepared statements (spec §7) ----

Task<Response> Api::PrepareStatement(Request& request, const std::string& session_id) {
  auto session = OwnSession(request, session_id);
  RejectIfDraining();
  const json::object body = ParseObject(request.body);
  std::string sql = RequiredString(body, "sql");
  struct Prepared {
    std::shared_ptr<PreparedStatement> statement;
    json::value parameter_schema;
    json::value result_schema;
    json::value result_kind;
  };
  auto prepared = co_await OnEngine(*session, [session, sql = std::move(sql)] {
    Prepared out;
    out.statement = session->engine().Prepare(sql);
    out.parameter_schema = SchemaBase64(out.statement->ParameterSchema());
    std::optional<bool> rows = out.statement->ReturnsRows();
    if (auto result = out.statement->ResultSchema()) {
      out.result_schema = SchemaBase64(result);
      rows = true;
    }
    out.result_kind = rows ? json::value(*rows ? "rows" : "update") : json::value(nullptr);
    return out;
  });
  const std::string id = session->AddPrepared(prepared.statement);
  Response response = JsonResponse(http::status::created,
                                   json::object{{"prepared_id", id},
                                                {"parameter_schema", prepared.parameter_schema},
                                                {"result_schema", prepared.result_schema},
                                                {"result_kind", prepared.result_kind}});
  response.headers.emplace_back(
      "Location", std::format("/warp/v1/sessions/{}/prepared/{}", session_id, id));
  co_return response;
}

Task<Response> Api::ExecutePrepared(Request& request, const std::string& session_id,
                                    const std::string& prepared_id) {
  auto session = OwnSession(request, session_id);
  auto prepared = session->FindPrepared(prepared_id);
  Parameters parameters;
  std::shared_ptr<ValidatedReader> rows;
  if (request.Header("Content-Type").starts_with(kArrowStream)) {
    rows = OpenValidatedIpc(request.body);  // one execution per row (spec §7.2)
    parameters.rows = rows;
  } else {
    parameters.json = JsonParameters(ParseObject(request.body));
  }
  StatementWork work = [prepared, parameters, rows](EngineSession& engine) {
    try {
      return engine.ExecutePrepared(*prepared, parameters);
    } catch (const Error&) {
      if (rows && rows->failure()) {
        throw Error(ErrorCode::kUnsupportedArrowType, *rows->failure());
      }
      throw;
    }
  };
  co_return co_await Launch(request, session, std::move(work),
                            request.path + "\n" + Sha256Hex(request.body));
}

Task<Response> Api::ClosePrepared(Request& request, const std::string& session_id,
                                  const std::string& prepared_id) {
  auto session = OwnSession(request, session_id);
  auto prepared = session->RemovePrepared(prepared_id);
  // Engine objects are destroyed on the session's strand.
  net::post(session->strand(), [prepared = std::move(prepared)]() mutable { prepared.reset(); });
  co_return NoContent();
}

// ---- Bulk ingest (spec §8) ----

Task<Response> Api::Ingest(Request& request, const std::string& session_id) {
  auto session = OwnSession(request, session_id);
  RejectIfDraining();
  if (!request.body_source) {
    throw Error(ErrorCode::kBadRequest, "ingest needs an Arrow IPC stream body");
  }

  IngestTarget target;
  auto query = [&](const char* name) -> std::optional<std::string> {
    auto it = request.query.find(name);
    return it == request.query.end() ? std::nullopt : std::optional(it->second);
  };
  target.table = query("table").value_or("");
  if (target.table.empty()) {
    throw Error(ErrorCode::kBadRequest, "the 'table' query parameter is required");
  }
  target.catalog = query("catalog").value_or("");
  target.schema = query("schema").value_or("");
  const std::string mode = query("mode").value_or("create_append");
  if (mode == "create") target.mode = IngestMode::kCreate;
  else if (mode == "append") target.mode = IngestMode::kAppend;
  else if (mode == "replace") target.mode = IngestMode::kReplace;
  else if (mode == "create_append") target.mode = IngestMode::kCreateAppend;
  else throw Error(ErrorCode::kBadRequest, "mode must be create, append, replace or create_append");
  const std::string temporary = query("temporary").value_or("false");
  if (temporary != "true" && temporary != "false") {
    throw Error(ErrorCode::kBadRequest, "temporary must be true or false");
  }
  target.temporary = temporary == "true";

  // A retried upload returns the first one's answer. The key covers the target, not the
  // body: hashing the body would mean reading all of it before answering a retry.
  std::string material = request.path;
  for (const auto& [name, value] : request.query) {
    material += "\n" + name + "=" + value;
  }
  if (auto existing = ReserveIdempotencyKey(request, *session, "", material)) {
    if (existing->response && !existing->response->empty()) {
      Response response = JsonResponse(http::status::ok, json::parse(*existing->response));
      response.close = true;  // the retried body is not read
      co_return response;
    }
    throw Error(ErrorCode::kIdempotencyKeyReused, "an upload with this key is still running");
  }
  const std::string key(request.Header("Idempotency-Key"));

  auto queue = std::make_shared<ByteQueue>(8);
  auto completion = std::make_shared<Completion<int64_t>>();
  net::post(session->strand(), [session, queue, completion, target] {
    std::shared_ptr<ValidatedReader> rows;
    try {
      rows = OpenValidatedIpc(QueueInput(queue));
      completion->Succeed(session->engine().Ingest(target, rows));
    } catch (const Error& error) {
      if (rows && rows->failure()) {
        completion->Fail(std::make_exception_ptr(
            Error(ErrorCode::kUnsupportedArrowType, *rows->failure())));
      } else {
        completion->Fail(std::current_exception());
      }
    } catch (...) {
      completion->Fail(std::current_exception());
    }
    queue->StopReading();
  });

  // Feed the body to the engine as it arrives; the queue's bound is the backpressure.
  try {
    while (auto piece = co_await request.body_source->Next()) {
      if (!co_await queue->Push(std::move(*piece))) {
        break;  // the engine stopped reading: its error is the answer
      }
    }
    queue->Close();
  } catch (...) {
    queue->Abort();
  }
  const int64_t rows = co_await completion->Wait();

  json::object answer{{"rows_ingested", rows}};
  if (!key.empty()) {
    if (auto entry = state_.idempotency.Reserve(session->owner().name, key, {})) {
      if (entry->response) *entry->response = json::serialize(answer);
    }
  }
  co_return JsonResponse(http::status::ok, answer);
}

// ---- Metadata (spec §10) ----

Task<Response> Api::Metadata(Request& request, const std::string& session_id,
                             std::string_view kind_name) {
  auto session = OwnSession(request, session_id);
  if (kind_name == "server-info") {
    const ServerIdentity identity = state_.backend->Identity();
    json::object info{
        {"server_name", identity.name},
        {"server_version", identity.version},
        {"protocol", "warp-sql"},
        {"protocol_version", "1"},
        {"sql_identifier_quote_char", "\""},
        {"sql_transactions_supported", true},
        {"server_arrow_version", ARROW_VERSION_STRING},
    };
    for (const auto& item : state_.backend->ServerInfo()) {
      info[item.key()] = item.value();
    }
    co_return JsonResponse(http::status::ok, info);
  }

  static const std::map<std::string_view, MetadataKind> kKinds{
      {"catalogs", MetadataKind::kCatalogs},
      {"db-schemas", MetadataKind::kDbSchemas},
      {"tables", MetadataKind::kTables},
      {"table-types", MetadataKind::kTableTypes},
      {"primary-keys", MetadataKind::kPrimaryKeys},
      {"imported-keys", MetadataKind::kImportedKeys},
      {"exported-keys", MetadataKind::kExportedKeys},
      {"cross-reference", MetadataKind::kCrossReference},
      {"type-info", MetadataKind::kTypeInfo},
  };
  auto found = kKinds.find(kind_name);
  if (found == kKinds.end()) {
    throw Error(ErrorCode::kNotFound, "no such metadata");
  }
  const MetadataKind kind = found->second;

  auto query = [&](const char* name) -> std::optional<std::string> {
    auto it = request.query.find(name);
    return it == request.query.end() ? std::nullopt : std::optional(it->second);
  };
  MetadataFilter filter;
  if (kind == MetadataKind::kCrossReference) {
    filter.catalog = query("pk_catalog");
    filter.db_schema = query("pk_db_schema");
    filter.table = query("pk_table");
    filter.fk_catalog = query("fk_catalog");
    filter.fk_db_schema = query("fk_db_schema");
    filter.fk_table = query("fk_table");
  } else {
    filter.catalog = query("catalog");
    filter.db_schema = query("db_schema");
    if (!filter.db_schema) filter.db_schema = query("db_schema_filter_pattern");
    filter.table = query("table");
    if (!filter.table) filter.table = query("table_name_filter_pattern");
  }
  if (auto types = query("table_types")) {
    std::string_view rest = *types;
    while (!rest.empty()) {
      const auto comma = rest.find(',');
      filter.table_types.emplace_back(Trim(rest.substr(0, comma)));
      rest = comma == std::string_view::npos ? std::string_view{} : rest.substr(comma + 1);
    }
  }
  filter.include_schema = query("include_schema").value_or("false") == "true";
  if (auto data_type = query("data_type")) {
    filter.data_type = static_cast<int32_t>(ParseInt(*data_type, "data_type"));
  }
  // Primary and imported keys without a table cover every table the principal sees, so
  // a client lists all constraints in one request (spec §10).
  if (kind == MetadataKind::kExportedKeys && !filter.table) {
    throw Error(ErrorCode::kBadRequest, "the 'table' query parameter is required");
  }

  auto table = co_await OnEngine(*session, [session, kind, filter] {
    auto rows = session->engine().Metadata(kind, filter);
    return ConformMetadata(kind, filter.include_schema, *rows);
  });

  IpcStreamEncoder encoder(table->schema(),
                           ParseIpcCompression(request.Header("Warp-IPC-Compression")));
  std::string body;
  arrow::TableBatchReader batches(*table);
  for (;;) {
    std::shared_ptr<arrow::RecordBatch> batch;
    if (!batches.ReadNext(&batch).ok() || batch == nullptr) {
      break;
    }
    body += encoder.Write(*batch);
  }
  body += encoder.Finish();
  Response response;
  response.content_type = kArrowStream;
  response.body = std::move(body);
  response.headers.emplace_back("X-Content-Type-Options", "nosniff");
  response.headers.emplace_back("Cache-Control", "no-store");
  co_return response;
}

Task<Response> Api::GetStatement(Request& request, const std::string& session_id,
                                 const std::string& statement_id) {
  auto session = OwnSession(request, session_id);
  auto statement = session->FindStatement(statement_id);
  const Preferences prefs = ParsePrefer(request.Header("Prefer"));
  if (prefs.wait) {
    // Long-poll: hold until the state changes (spec §6.3). A finished statement's
    // state can't change, so it answers at once.
    const auto initial = statement->state();
    const auto deadline = Clock::now() + std::min(*prefs.wait, state_.options.max_wait);
    uint64_t seen = statement->changed().version();
    while (!statement->IsTerminal() && statement->state() == initial && Clock::now() < deadline) {
      seen = co_await statement->changed().WaitChange(seen, deadline);
    }
  }
  co_return JsonResponse(http::status::ok, StatementDocument(*session, *statement));
}

Task<Response> Api::GetResult(Request& request, const std::string& session_id,
                              const std::string& statement_id) {
  auto session = OwnSession(request, session_id);
  auto statement = session->FindStatement(statement_id);
  int64_t from_batch = 0;
  if (auto it = request.query.find("from_batch"); it != request.query.end()) {
    from_batch = ParseInt(it->second, "from_batch");
  }
  if (auto it = request.query.find("partition"); it != request.query.end()) {
    if (ParseInt(it->second, "partition") != 0) {
      throw Error(ErrorCode::kNotFound, "this result has one partition, 0");
    }
  }

  const auto deadline = Clock::now() + state_.options.max_wait;
  uint64_t seen = statement->changed().version();
  while (!statement->HasOutcome() && Clock::now() < deadline) {
    seen = co_await statement->changed().WaitChange(seen, deadline);
  }
  if (!statement->HasOutcome()) {
    throw Error(ErrorCode::kResultNotReady, "the statement has not produced a result yet");
  }
  if (const auto state = statement->state();
      state == StatementState::kFailed || state == StatementState::kCancelled) {
    throw *statement->error();
  }
  if (!statement->HasRows()) {
    throw Error(ErrorCode::kBadRequest, "this statement returns no rows");
  }
  statement->ClaimRead(from_batch);
  co_return ResultStream(session, statement, from_batch, request.Header("Warp-IPC-Compression"));
}

Response Api::ResultStream(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement,
                           int64_t from_batch, std::string_view compression_header) {
  const IpcCompression compression = ParseIpcCompression(compression_header);
  Response response;
  response.status = http::status::ok;
  response.content_type = kArrowStream;
  response.headers.emplace_back("Warp-Statement-Id", statement->id());
  response.headers.emplace_back("Warp-IPC-Compression", std::string(IpcCompressionName(compression)));
  response.headers.emplace_back("X-Content-Type-Options", "nosniff");
  response.headers.emplace_back("Cache-Control", "no-store");

  response.stream = [this, session, statement, from_batch,
                     compression](BodyWriter& out) -> Task<bool> {
    // However the read ends, release it; an unretained result that wasn't read to the
    // end can never be read again, so its statement is cancelled (spec §6.5).
    struct ReadGuard {
      Api* api;
      std::shared_ptr<Session> session;
      std::shared_ptr<Statement> statement;
      bool complete = false;
      ~ReadGuard() {
        statement->EndRead();
        if (!complete && !statement->retained() && !statement->AbandonRead()) {
          api->Cancel(session, statement, CancelReason::kClient);
        }
      }
    } guard{this, session, statement};

    IpcStreamEncoder encoder(statement->schema(), compression);
    co_await out.Write(encoder.Begin());
    if (!CanEncodeIndependently(*statement->schema())) {
      // Dictionaries depend on the stream's state: one batch after another.
      int64_t index = from_batch;
      for (;;) {
        const uint64_t seen = statement->changed().version();
        auto [step, batch] = statement->Read(index);
        switch (step) {
          case ReadStep::kBatch:
            co_await out.Write(encoder.Write(*batch));
            ++index;
            continue;
          case ReadStep::kEnd:
            co_await out.Write(encoder.Finish());
            guard.complete = true;
            co_return true;
          case ReadStep::kFailed:
            co_return false;  // no end-of-stream marker: the client sees an incomplete stream
          case ReadStep::kWait:
            co_await statement->changed().WaitChange(seen, Clock::now() + kClientCheck);
            if (out.ClientGone()) {
              co_return false;  // the read is incomplete: an unretained statement is cancelled
            }
            continue;
        }
      }
    }
    const bool complete = co_await StreamBatches(*statement, from_batch, compression, out);
    guard.complete = complete;
    co_return complete;
  };
  return response;
}

// The result stream's pipeline. The statement's engine thread produces batches; encoding
// (and zstd compression) runs on the encode pool, several batches at once; this
// coroutine, on the connection's strand, writes the encoded batches in order, gathered
// into large writes (with TLS, each is encrypted in one go). Each stage runs while the
// others do, within bounds: the statement buffers a few batches, at most kEncodeWindow
// (kEncodeBytes) are being encoded, and one write is in flight.
Task<bool> Api::StreamBatches(Statement& statement, int64_t from_batch, IpcCompression compression,
                              BodyWriter& out) {
  struct Job {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::size_t bytes = 0;
    Completion<EncodedBatch> encoded;
    Clock::duration took{};
  };
  struct Stats {
    Clock::time_point started = Clock::now();
    Clock::duration wait_rows{}, wait_encode{}, write{}, encode{};
    int64_t batches = 0, writes = 0;
    std::size_t raw = 0, sent = 0;
    ~Stats() {
      if (!StreamStatsEnabled()) return;
      const double wall = Seconds(Clock::now() - started);
      std::fprintf(stderr,
                   "warpsql stream: %lld batches, %.1f MB raw, %.1f MB sent, %.3f s "
                   "(%.0f MB/s sent): waiting for rows %.3f s, waiting for encoding %.3f s, "
                   "writing %.3f s in %lld writes (%.0f MB/s while writing), encoding %.3f s of CPU "
                   "(%.0f MB/s per thread)\n",
                   static_cast<long long>(batches), raw / 1e6, sent / 1e6, wall,
                   wall > 0 ? sent / 1e6 / wall : 0.0, Seconds(wait_rows), Seconds(wait_encode),
                   Seconds(write), static_cast<long long>(writes),
                   Seconds(write) > 0 ? sent / 1e6 / Seconds(write) : 0.0, Seconds(encode),
                   Seconds(encode) > 0 ? raw / 1e6 / Seconds(encode) : 0.0);
    }
  } stats;
  const std::size_t window = compression == IpcCompression::kZstd ? kEncodeWindowZstd : kEncodeWindow;
  std::deque<std::shared_ptr<Job>> encoding;
  std::size_t encoding_bytes = 0;
  std::vector<EncodedBatch> gathered;  // encoded, not yet written
  std::vector<net::const_buffer> pieces;
  std::size_t gathered_bytes = 0;
  auto flush = [&]() -> Task<void> {
    if (!pieces.empty()) {
      const auto started = Clock::now();
      co_await out.WriteGather(pieces);
      stats.write += Clock::now() - started;
      stats.writes += 1;
      stats.sent += gathered_bytes;
    }
    pieces.clear();
    gathered.clear();
    gathered_bytes = 0;
  };

  int64_t index = from_batch;
  bool produced_all = false;
  for (;;) {
    const uint64_t seen = statement.changed().version();
    while (!produced_all && encoding.size() < window &&
           (encoding.empty() || encoding_bytes < kEncodeBytes)) {
      auto [step, batch] = statement.Read(index);
      if (step == ReadStep::kFailed) {
        co_return false;  // no end-of-stream marker: the client sees an incomplete stream
      }
      if (step == ReadStep::kEnd) {
        produced_all = true;
        break;
      }
      if (step == ReadStep::kWait) {
        break;
      }
      ++index;
      auto job = std::make_shared<Job>();
      job->bytes = static_cast<std::size_t>(BatchBytes(*batch));
      job->batch = std::move(batch);
      encoding_bytes += job->bytes;
      net::post(state_.encode_pool, [job, compression] {
        try {
          const auto started = Clock::now();
          EncodedBatch encoded = EncodeBatch(*job->batch, compression);
          job->took = Clock::now() - started;
          job->batch.reset();  // the encoded batch holds the buffers it sends
          job->encoded.Succeed(std::move(encoded));
        } catch (...) {
          job->encoded.Fail(std::current_exception());
        }
      });
      encoding.push_back(std::move(job));
    }

    if (encoding.empty()) {
      if (produced_all) {
        pieces.push_back(net::buffer(EndOfStreamMarker()));
        co_await flush();
        co_return true;
      }
      // Nothing to encode until the statement produces more: send what is ready, then wait.
      co_await flush();
      const auto started = Clock::now();
      co_await statement.changed().WaitChange(seen, Clock::now() + kClientCheck);
      stats.wait_rows += Clock::now() - started;
      if (out.ClientGone()) {
        co_return false;  // the read is incomplete: an unretained statement is cancelled
      }
      continue;
    }
    auto job = encoding.front();
    if (!job->encoded.done() && !pieces.empty()) {
      co_await flush();  // write what is ready while the next batch is encoded
      continue;
    }
    const auto wait_started = Clock::now();
    EncodedBatch encoded = co_await job->encoded.Wait();  // an encoding failure ends the stream incomplete
    stats.wait_encode += Clock::now() - wait_started;
    stats.encode += job->took;
    stats.batches += 1;
    stats.raw += job->bytes;
    encoding.pop_front();
    encoding_bytes -= job->bytes;
    gathered_bytes += encoded.size();
    pieces.insert(pieces.end(), encoded.pieces().begin(), encoded.pieces().end());
    gathered.push_back(std::move(encoded));
    if (gathered_bytes >= kGatherBytes) {
      co_await flush();
    }
  }
}

Task<Response> Api::CancelStatement(Request& request, const std::string& session_id,
                                    const std::string& statement_id) {
  auto session = OwnSession(request, session_id);
  auto statement = session->FindStatement(statement_id);
  Cancel(session, statement, CancelReason::kClient);
  co_return JsonResponse(http::status::accepted, StatementDocument(*session, *statement));
}

Task<Response> Api::CancelSession(Request& request, const std::string& session_id) {
  auto session = OwnSession(request, session_id);
  json::array cancelled;
  for (const auto& statement : session->Statements()) {
    if (!statement->IsTerminal()) {
      Cancel(session, statement, CancelReason::kClient);
      cancelled.push_back(json::value(statement->id()));
    }
  }
  co_return JsonResponse(http::status::accepted, json::object{{"cancelled", std::move(cancelled)}});
}

Task<Response> Api::DeleteStatement(Request& request, const std::string& session_id,
                                    const std::string& statement_id) {
  auto session = OwnSession(request, session_id);
  auto statement = session->FindStatement(statement_id);
  if (!statement->AbandonRead()) {
    Cancel(session, statement, CancelReason::kClient);  // a write runs on (spec §6.8)
  }
  statement->Release();
  session->RemoveStatement(statement_id);
  co_return NoContent();
}

}  // namespace warpsql::detail
