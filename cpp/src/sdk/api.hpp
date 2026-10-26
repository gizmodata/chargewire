// The Warp SQL endpoints (spec §3–§11): request in, response out.
#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <mutex>

#include "auth.hpp"
#include "browser_sign_in.hpp"
#include "common.hpp"
#include "http.hpp"
#include "ipc.hpp"
#include "security_events.hpp"
#include "sessions.hpp"
#include "statements.hpp"
#include "warpsql/server.hpp"

namespace warpsql::detail {

// Bytes of retained results each principal holds and the shared server ceiling.
class RetainedQuota {
 public:
  explicit RetainedQuota(std::size_t limit, std::size_t total_limit = std::size_t{1} << 30);
  // Positive: reserve, false if it doesn't fit. Negative: release.
  bool Change(const std::string& principal, int64_t bytes);

 private:
  int64_t limit_;
  int64_t total_limit_;
  int64_t total_ = 0;
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
  // Encodes (and compresses) result batches for the streams that send them.
  net::thread_pool& encode_pool;

  std::shared_ptr<TokenService> tokens;
  std::shared_ptr<TokenStore> in_memory_tokens;  // when the server brought no TokenService
  SessionRegistry sessions;
  IdempotencyStore idempotency;
  RetainedQuota quota;
  std::atomic<bool> draining = false;
  SecurityEventLimiter security_events{};
  // Browser sign-in's flows (spec §4.3), when options.browser_sign_in is set.
  std::unique_ptr<BrowserFlows> browser_flows = nullptr;
};

class Api {
 public:
  explicit Api(ServerState& state) : state_(state) {}

  Task<Response> Handle(Request& request);

 private:
  Task<Response> Route(Request& request);

  Task<Response> Discovery(Request& request);
  Task<Response> Health(Request& request);
  // The OpenAPI document (spec §3.3).
  Response OpenApi(const Request& request);
  Task<Response> IssueToken(Request& request);
  // Browser sign-in (spec §4.3).
  Task<Response> CreateBrowserFlow(Request& request);
  Task<Response> PollBrowserFlow(Request& request, const std::string& flow_id);
  Task<Response> BrowserCallback(Request& request);

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
  // Partitioned ingest (spec §8.3).
  Task<Response> OpenIngest(Request& request, const std::string& session_id);
  Task<Response> GetIngest(Request& request, const std::string& session_id, const std::string& ingest_id);
  Task<Response> AbortIngest(Request& request, const std::string& session_id, const std::string& ingest_id);
  Task<Response> CommitIngest(Request& request, const std::string& session_id, const std::string& ingest_id);
  Task<Response> IngestPart(Request& request, const std::string& session_id, const std::string& ingest_id,
                            std::string_view part);
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

  // The request's principal from its token, checked against the connection's certificate.
  // `announce`: tell the token service it was accepted (OwnSession does, after the
  // session's own check).
  Principal Authenticate(const Request& request, bool announce = true) const;
  // Reports a refusal that may be an attack (ServerOptions::on_security_event).
  void ReportSecurityEvent(const Request& request, const Error& error) const;

 public:
  // A client certificate the TLS handshake refused (rate-limited like the rest).
  void ReportHandshakeRefusal(const std::string& reason, const std::string& thumbprint,
                              const std::string& subject, const std::string& peer) const;
  // Reports the summaries of windows that ended with refusals held back.
  void FlushSecurityEvents() const;
  // Stops a session's work (Server::CancelSession): its statements queued or running, its
  // open partitioned ingests and its ingest bodies still arriving. Returns how many.
  std::size_t CancelWork(const std::shared_ptr<Session>& session, CancelReason reason);
  // Server::Sessions()'s description of `session`.
  SessionActivity Describe(Session& session) const;

 private:
  void Emit(const SecurityEvent& event) const;
  // Whether the client_certificate grant is offered (spec §4.4).
  bool OffersCertificateSignIn() const;
  std::shared_ptr<Session> OwnSession(const Request& request, const std::string& session_id);
  static Session::Client ClientOf(const Request& request);
  void RejectIfDraining() const;
  // Throws not_implemented unless the backend has `feature` (spec §3.1).
  void RequireCapability(bool BackendCapabilities::*feature, std::string_view name) const;
  json::object SessionDocument(Session& session) const;
  json::object StatementDocument(Session& session, Statement& statement) const;
  Response ResultStream(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement,
                        std::size_t partition, int64_t from_batch,
                        std::string_view compression_header);
  Task<bool> StreamBatches(Statement& statement, std::size_t partition, int64_t from_batch,
                           IpcCompression compression, BodyWriter& out);
  void Cancel(const std::shared_ptr<Session>& session, const std::shared_ptr<Statement>& statement,
              CancelReason reason);
  // Interrupts the engine again every 100 ms until `statement` ends.
  void KeepInterrupting(std::shared_ptr<Session> session, std::shared_ptr<Statement> statement);
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
