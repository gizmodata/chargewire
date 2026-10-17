#include "warpsql/server.hpp"

#include <thread>

#include "api.hpp"
#include "http.hpp"
#include "json.hpp"

namespace warpsql {

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
    default: return ErrorCode::kExecutionError;
  }
}

}  // namespace

class Server::Impl {
 public:
  Impl(ServerOptions options, std::shared_ptr<Backend> backend,
       std::shared_ptr<Authenticator> authenticator, std::shared_ptr<TokenService> tokens)
      : engine_pool_(static_cast<std::size_t>(ThreadCount(options.engine_threads, 2))),
        state_{options,
               std::move(backend),
               std::move(authenticator),
               io_,
               engine_pool_,
               tokens,
               tokens ? nullptr : std::make_shared<TokenStore>(options.token_lifetime),
               {},
               {},
               RetainedQuota(options.retained_bytes_per_principal)},
        api_(state_),
        http_(io_, state_.options,
              HttpLimits{state_.options.max_header_bytes, state_.options.max_json_body_bytes,
                         // Bulk ingest streams its body; everything else is read up front.
                         [](const Request& request) {
                           return request.method == http::verb::post &&
                                  request.path.ends_with("/ingest");
                         },
                         [](http::status status, std::string_view detail) {
                           Response response = ProblemResponse(
                               Error(CodeForStatus(status), std::string(detail)), "");
                           response.status = status;
                           response.close = true;
                           return response;
                         }},
              [this](Request& request) { return api_.Handle(request); }),
        sweep_timer_(io_) {
    if (!state_.tokens) {
      state_.tokens = state_.in_memory_tokens;
    }
  }

  ~Impl() { Stop(); }

  void Start() {
    port_ = http_.Bind();
    http_.StartAccepting();
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

  void Stop() {
    if (stopped_.exchange(true)) {
      return;
    }
    state_.draining = true;
    http_.Close();
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
    stopped_cv_.notify_all();
  }

  void Wait() {
    std::unique_lock lock(stopped_mutex_);
    stopped_cv_.wait(lock, [this] { return stopped_.load(); });
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
    // Stop() joins the io threads, so it can't run on one of them.
    std::thread([this] { Stop(); }).detach();
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
    const auto now = WallClock::now();
    const auto idle_cutoff = Clock::now() - state_.options.session_idle_timeout;
    for (auto& session : state_.sessions.All()) {
      for (const auto& statement : session->Statements()) {
        statement->ExpireIfDue(now);
      }
      if (session->IdleSince(idle_cutoff)) {
        state_.sessions.Remove(session->id());
        session->Close(CancelReason::kSessionClosed);
        net::post(session->strand(), [session] { session->DestroyEngine(); });
      }
    }
  }

  net::io_context io_;
  net::thread_pool engine_pool_;
  ServerState state_;
  Api api_;
  HttpServer http_;
  net::steady_timer sweep_timer_;
  std::vector<std::thread> threads_;
  uint16_t port_ = 0;

  std::atomic<bool> stopped_ = false;
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

}  // namespace warpsql
