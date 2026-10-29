#include "chargewire/server.hpp"

#include <openssl/crypto.h>

#include <thread>

#include "api.hpp"
#include "http.hpp"
#include "json.hpp"
#include "legacy_dialect.hpp"

namespace chargewire {

using namespace detail;

namespace {

// How often idle sessions, expired results, tokens and idempotency keys are swept.
constexpr auto kSweepInterval = std::chrono::seconds(5);

int ThreadCount(int configured, int per_core) {
  if (configured > 0) {
    return configured;
  }
  return std::max(2, static_cast<int>(std::thread::hardware_concurrency()) * per_core);
}

ErrorCode CodeForStatus(http::status status) {
  switch (status) {
    case http::status::request_header_fields_too_large: return ErrorCode::kHeadersTooLarge;
    case http::status::payload_too_large: return ErrorCode::kPayloadTooLarge;
    case http::status::bad_request: return ErrorCode::kBadRequest;
    default: return ErrorCode::kInternalError;
  }
}

}  // namespace

class Server::Impl {
 public:
  Impl(ServerOptions options, std::shared_ptr<Backend> backend,
       std::shared_ptr<Authenticator> authenticator, std::shared_ptr<TokenService> tokens)
      : engine_pool_(static_cast<std::size_t>(ThreadCount(options.engine_threads, 2))),
        encode_pool_(static_cast<std::size_t>(ThreadCount(0, 1))),
        state_{options,
               std::move(backend),
               std::move(authenticator),
               io_,
               engine_pool_,
               encode_pool_,
               tokens,
               tokens ? nullptr : std::make_shared<TokenStore>(options.token_lifetime),
               {},
               {},
               RetainedQuota(options.retained_bytes_per_principal)},
        api_(state_),
        limits_{state_.options.max_header_bytes, state_.options.max_json_body_bytes,
                // Bulk ingest streams its body; everything else is read up front.
                [](const Request& request) {
                  // and so do a partitioned ingest's parts (spec §8.3)
                  return request.method == http::verb::post &&
                         (request.path.ends_with("/ingest") ||
                          (request.path.find("/ingests/") != std::string::npos &&
                           request.path.find("/parts/") != std::string::npos));
                },
                [](http::status status, std::string_view detail) {
                  Response response =
                      ProblemResponse(Error(CodeForStatus(status), std::string(detail)), "");
                  response.status = status;
                  response.close = true;
                  return response;
                },
                [this](const std::string& reason, const std::string& thumbprint, const std::string& subject,
                       const std::string& peer) { api_.ReportHandshakeRefusal(reason, thumbprint, subject, peer); }},
        http_(io_, state_.options, limits_, [this](Request& request) { return api_.Handle(request); }),
        sweep_timer_(io_) {
    if (!state_.tokens) {
      state_.tokens = state_.in_memory_tokens;
    }
    if (state_.options.browser_sign_in) {
      state_.browser_flows = std::make_unique<BrowserFlows>(
          BrowserFlows::Limits{.lifetime = state_.options.browser_sign_in_lifetime,
                               .max_flows = state_.options.max_browser_sign_ins,
                               .max_per_address = state_.options.max_browser_sign_ins_per_address});
    }
  }

  // Stops (or waits for the stop already under way, e.g. a drain's), then joins the drain's
  // thread, so nothing of the server runs while its members are torn down.
  ~Impl() {
    Stop();
    if (drain_thread_.joinable()) {
      drain_thread_.join();
    }
  }

  void Start() {
    port_ = http_.Bind();
    http_.StartAccepting();
    if (state_.options.health_port != 0) {
      ServerOptions health_options = state_.options;
      health_options.port = state_.options.health_port;
      health_options.tls.reset();
      health_http_ = std::make_unique<HttpServer>(
          io_, health_options, limits_,
          [this](Request& request) -> Task<Response> {
            if (request.method == http::verb::get &&
                (request.path == "/chargewire/v1/health" || request.path == "/health" ||
                 request.path == kLegacyHealthPath)) {
              if (request.path == "/health") request.path = "/chargewire/v1/health";
              co_return co_await api_.Handle(request);
            }
            co_return ProblemResponse(Error(ErrorCode::kNotFound, "this port serves /health only"),
                                      request.request_id);
          });
      health_http_->Bind();
      health_http_->StartAccepting();
    }
    ScheduleSweep();
    const int threads = ThreadCount(state_.options.http_threads, 1);
    for (int i = 0; i < threads; i++) {
      threads_.emplace_back([this] { io_.run(); });
    }
  }

  uint16_t port() const { return port_; }

  void Drain(std::chrono::seconds grace) {
    state_.draining = true;
    net::co_spawn(io_, FinishDraining(grace), net::detached);
  }

  // One stop, whoever asks first: Stop(), the destructor or a finished drain. A caller that
  // isn't first waits until that stop has finished.
  void Stop() {
    if (stopping_.exchange(true)) {
      Wait();
      return;
    }
    state_.draining = true;
    http_.Close();
    if (health_http_) {
      health_http_->Close();
    }
    for (auto& session : state_.sessions.All()) {
      state_.sessions.Remove(session->id());
      session->Close(CancelReason::kServerShutdown);
    }
    io_.stop();
    for (auto& thread : threads_) {
      if (thread.joinable() && thread.get_id() != std::this_thread::get_id()) {
        thread.join();
      }
    }
    engine_pool_.join();
    encode_pool_.join();
    {
      std::lock_guard lock(stopped_mutex_);
      stopped_ = true;
    }
    stopped_cv_.notify_all();
  }

  // Returns once a stop has finished (not merely begun).
  void Wait() {
    std::unique_lock lock(stopped_mutex_);
    stopped_cv_.wait(lock, [this] { return stopped_; });
  }

  std::vector<SessionActivity> Sessions() const {
    std::vector<SessionActivity> out;
    for (const auto& session : state_.sessions.All()) {
      out.push_back(api_.Describe(*session));
    }
    return out;
  }

  std::optional<std::size_t> CancelSession(const std::string& id, SessionCancelReason reason) {
    auto session = state_.sessions.Get(id);
    if (!session) return std::nullopt;
    return api_.CancelWork(session, reason == SessionCancelReason::kKilled ? CancelReason::kKilled
                                                                         : CancelReason::kCancelled);
  }

  bool EndSession(const std::string& id, const Error& why) {
    auto session = state_.sessions.Get(id);
    if (!session || !session->End(why)) return false;
    // On the session's strand, so after the engine call it is running (which a cancel
    // stops): then nothing runs in it, its results are freed and its engine goes.
    net::post(session->strand(), [this, session, why] {
      session->Close(CancelReason::kKilled);
      state_.sessions.Retire(session->id(), session->owner(), why,
                             Clock::now() + std::max<Clock::duration>(state_.options.session_idle_timeout,
                                                                      std::chrono::minutes(15)));
      session->DestroyEngine();
    });
    return true;
  }

 private:
  // Lets running statements finish within `grace`, then stops (spec §11.1).
  Task<void> FinishDraining(std::chrono::seconds grace) {
    const auto deadline = Clock::now() + grace;
    net::steady_timer timer(io_);
    while (Clock::now() < deadline && AnyRunning()) {
      timer.expires_after(std::chrono::milliseconds(100));
      co_await timer.async_wait(net::use_awaitable);
    }
    // Stop() joins the io threads, so it can't run on one of them: it runs on a thread the
    // destructor joins (never detached, so it can't outlive the server). The io threads
    // are joined before the destructor reads drain_thread_, which orders this write first.
    if (!drain_spawned_.exchange(true)) {  // a second Drain() finds the first one's
      drain_thread_ = std::thread([this] { Stop(); });
    }
  }

  bool AnyRunning() const {
    for (const auto& session : state_.sessions.All()) {
      for (const auto& statement : session->Statements()) {
        if (!statement->IsTerminal()) {
          return true;
        }
      }
    }
    return false;
  }

  void ScheduleSweep() {
    sweep_timer_.expires_after(kSweepInterval);
    sweep_timer_.async_wait([this](boost::system::error_code ec) {
      if (ec) {
        return;
      }
      Sweep();
      ScheduleSweep();
    });
  }

  void Sweep() {
    if (state_.in_memory_tokens) {
      state_.in_memory_tokens->Sweep();
    }
    state_.idempotency.Sweep();
    state_.sessions.SweepEnded(Clock::now());
    api_.FlushSecurityEvents();
    const auto now = WallClock::now();
    const auto idle_cutoff = Clock::now() - state_.options.session_idle_timeout;
    const auto ingest_cutoff = Clock::now() - state_.options.ingest_idle_timeout;
    for (auto& session : state_.sessions.All()) {
      for (const auto& statement : session->Statements()) {
        statement->ExpireIfDue(now);
      }
      for (const auto& ingest : session->Ingests()) {
        if (ingest->Expired(ingest_cutoff)) {
          // Kept for a while, so its requests get ingest_expired; nothing of it was written.
          ingest->Abort(Error(ErrorCode::kIngestExpired, "the ingest had no request for too long and was aborted"));
        } else if (ingest->Forgettable(ingest_cutoff)) {
          session->RemoveIngest(ingest->id());  // finished, and unasked about for as long
        }
      }
      session->ForgetFinished(now, state_.options.finished_statement_ttl,
                              state_.options.max_finished_statements_per_session);
      if (session->CloseIfIdle(idle_cutoff, CancelReason::kSessionClosed)) {
        state_.sessions.Remove(session->id());
        net::post(session->strand(), [session] { session->DestroyEngine(); });
      }
    }
  }

  // The engine pool outlives the io_context: destroying the io_context destroys the
  // coroutines of requests still in flight, and their sessions' strands belong to the
  // engine pool, so tearing the pool down first crashed a server stopped mid-request.
  net::thread_pool engine_pool_;
  net::thread_pool encode_pool_;
  net::io_context io_;
  ServerState state_;
  Api api_;
  HttpLimits limits_;
  HttpServer http_;
  std::unique_ptr<HttpServer> health_http_;
  net::steady_timer sweep_timer_;
  std::vector<std::thread> threads_;
  uint16_t port_ = 0;

  std::thread drain_thread_;
  std::atomic<bool> drain_spawned_ = false;
  std::atomic<bool> stopping_ = false;
  bool stopped_ = false;  // guarded by stopped_mutex_
  std::mutex stopped_mutex_;
  std::condition_variable stopped_cv_;
};

Server::Server(ServerOptions options, std::shared_ptr<Backend> backend,
               std::shared_ptr<Authenticator> authenticator,
               std::shared_ptr<TokenService> tokens)
    : impl_(std::make_unique<Impl>(std::move(options), std::move(backend),
                                   std::move(authenticator), std::move(tokens))) {}

Server::~Server() = default;

void Server::Start() { impl_->Start(); }

uint16_t Server::port() const { return impl_->port(); }

void Server::Drain(std::chrono::seconds grace) { impl_->Drain(grace); }

void Server::Stop() { impl_->Stop(); }

void Server::Wait() { impl_->Wait(); }

std::vector<SessionActivity> Server::Sessions() const { return impl_->Sessions(); }

std::optional<std::size_t> Server::CancelSession(const std::string& session_id, SessionCancelReason reason) {
  return impl_->CancelSession(session_id, reason);
}

bool Server::EndSession(const std::string& session_id, const Error& why) { return impl_->EndSession(session_id, why); }

void InitializeProcess() { OPENSSL_init_crypto(OPENSSL_INIT_NO_ATEXIT, nullptr); }

}  // namespace chargewire
