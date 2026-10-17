// The Warp SQL endpoints (spec §3–§11): request in, response out.
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>

#include "auth.hpp"
#include "common.hpp"
#include "http.hpp"
#include "sessions.hpp"
#include "statements.hpp"
#include "warpsql/server.hpp"

namespace warpsql::detail {

// Bytes of retained results each principal holds (spec §6.5).
class RetainedQuota {
 public:
  explicit RetainedQuota(std::size_t limit) : limit_(static_cast<int64_t>(limit)) {}
  // Positive: reserve, false if it doesn't fit. Negative: release.
  bool Change(const std::string& principal, int64_t bytes);

 private:
  int64_t limit_;
  std::mutex mutex_;
  std::map<std::string, int64_t> used_;
};

// Everything the endpoints share.
struct ServerState {
  ServerOptions options;
  std::shared_ptr<Backend> backend;
  std::shared_ptr<Authenticator> authenticator;
  net::io_context& io;
  net::thread_pool& engine_pool;

  std::shared_ptr<TokenService> tokens;
  std::shared_ptr<TokenStore> in_memory_tokens;  // when the server brought no TokenService
  SessionRegistry sessions;
  IdempotencyStore idempotency;
  RetainedQuota quota;
  std::atomic<bool> draining = false;
};

class Api {
 public:
  explicit Api(ServerState& state) : state_(state) {}

  Task<Response> Handle(Request& request);

 private:
  Task<Response> Route(Request& request);

  Task<Response> Discovery(Request& request);
  Task<Response> Health(Request& request);
  Task<Response> IssueToken(Request& request);

  Task<Response> CreateSession(Request& request);
  Task<Response> GetSession(Request& request, const std::string& session_id);
  Task<Response> UpdateSession(Request& request, const std::string& session_id);
  Task<Response> DeleteSession(Request& request, const std::string& session_id);
  Task<Response> EndTransaction(Request& request, const std::string& session_id, bool commit);

  using StatementWork = std::function<StatementOutcome(EngineSession&)>;

  Task<Response> SubmitStatement(Request& request, const std::string& session_id);
  Task<Response> PrepareStatement(Request& request, const std::string& session_id);
  Task<Response> ExecutePrepared(Request& request, const std::string& session_id,
                                 const std::string& prepared_id);
  Task<Response> ClosePrepared(Request& request, const std::string& session_id,
                               const std::string& prepared_id);
  Task<Response> Ingest(Request& request, const std::string& session_id);
  Task<Response> Metadata(Request& request, const std::string& session_id,
                          std::string_view kind_name);
  // Starts a statement and answers per spec §6.1 (streamed or status document).
  Task<Response> Launch(Request& request, std::shared_ptr<Session> session, StatementWork work,
                        const std::string& idempotency_material);
  std::optional<IdempotencyStore::Entry> ReserveIdempotencyKey(const Request& request,
                                                               const Session& session,
                                                               const std::string& statement_id,
                                                               const std::string& material);
  Task<Response> GetStatement(Request& request, const std::string& session_id,
                              const std::string& statement_id);
  Task<Response> GetResult(Request& request, const std::string& session_id,
                           const std::string& statement_id);
  Task<Response> CancelStatement(Request& request, const std::string& session_id,
                                 const std::string& statement_id);
  Task<Response> CancelSession(Request& request, const std::string& session_id);
  Task<Response> DeleteStatement(Request& request, const std::string& session_id,
                                 const std::string& statement_id);

  Principal Authenticate(const Request& request) const;
  std::shared_ptr<Session> OwnSession(const Request& request, const std::string& session_id);
  void RejectIfDraining() const;
  json::object SessionDocument(Session& session) const;
  json::object StatementDocument(Session& session, Statement& statement) const;
  Response ResultStream(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement,
                        int64_t from_batch, std::string_view compression_header);
  void Cancel(Session& session, Statement& statement, CancelReason reason);
  void Run(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement,
           StatementWork work);
  void ScheduleTimeout(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement,
                       std::chrono::milliseconds timeout);

  // Runs `work` on the session's engine strand and returns its result.
  template <typename Fn>
  Task<std::invoke_result_t<Fn>> OnEngine(Session& session, Fn work);

  ServerState& state_;
};

}  // namespace warpsql::detail
