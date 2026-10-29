// Sessions (spec §5): each owns one engine session, its statements, and a strand on
// the engine pool so the engine sees one call at a time.
#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common.hpp"
#include "ingests.hpp"
#include "statements.hpp"
#include "chargewire/backend.hpp"

namespace chargewire::detail {

using EngineStrand = net::strand<net::thread_pool::executor_type>;

class Session {
 public:
  Session(std::string id, Principal owner, EngineStrand strand);

  const std::string& id() const { return id_; }
  const Principal& owner() const { return owner_; }
  EngineStrand& strand() { return strand_; }

  // The engine session. Only touch it on strand(); see Interrupt() and Progress().
  EngineSession& engine();
  void SetEngine(std::unique_ptr<EngineSession> engine);
  // Destroys the engine session. Call on strand(), after Close().
  void DestroyEngine();

  // Safe from any thread while the engine is busy. `reason`: the cancel_reason it is for
  // (EngineSession::Cancel).
  void Interrupt(CancelReason reason);
  // Interrupts the engine only while `statement` is the one running on it, so a late
  // interrupt never reaches the session's next statement. Safe from any thread.
  void InterruptIfRunning(const Statement* statement, CancelReason reason);
  // The statement the engine is running (nullptr when idle). Set on strand().
  void SetRunning(const Statement* statement);
  // Whether `statement` is the one running on the engine. Safe from any thread.
  bool IsRunning(const Statement* statement) const;
  std::optional<double> Progress();
  std::optional<std::chrono::milliseconds> DefaultStatementTimeout();
  // Throws session_ended when the engine ended the session (EngineSession::CheckEnded).
  // Safe from any thread.
  void CheckEnded();

  json::object options() const;
  void set_options(json::object options);

  // The session's transaction as the engine last reported it (spec §5.2).
  TransactionState transaction() const;
  // Asks the engine for its transaction again. Call on strand(), after work ran in it
  // (or failed); an engine that can't answer keeps the last value.
  void RefreshTransaction();

  // Prepared statements (spec §7). Engine objects: destroy them on strand().
  // Keeps `prepared` under `id` (NewId("p"), chosen before the engine prepared it).
  void AddPrepared(const std::string& id, std::shared_ptr<PreparedStatement> prepared);
  // Throws prepared_not_found.
  std::shared_ptr<PreparedStatement> FindPrepared(const std::string& id) const;
  std::shared_ptr<PreparedStatement> RemovePrepared(const std::string& id);

  // Partitioned ingests (spec §8.3).
  void AddIngest(std::shared_ptr<IngestOperation> ingest);
  // Throws ingest_not_found.
  std::shared_ptr<IngestOperation> FindIngest(const std::string& id) const;
  void RemoveIngest(const std::string& id);
  std::vector<std::shared_ptr<IngestOperation>> Ingests() const;

  // A one-request bulk ingest's body still arriving (spec §8.1). Close() aborts it, so an
  // engine thread waiting for its bytes fails instead of waiting for I/O that a stopping
  // server no longer does (Server::Stop() joins the engine threads after stopping the I/O
  // ones). On a closed session it is aborted at once.
  void AddUpload(const std::shared_ptr<ByteQueue>& upload);
  void RemoveUpload(const ByteQueue* upload);
  // Aborts every one-request bulk ingest body still arriving (its load rolls back);
  // returns how many.
  std::size_t AbortUploads();
  std::size_t UploadCount() const;

  void AddStatement(std::shared_ptr<Statement> statement);
  // Throws statement_not_found.
  std::shared_ptr<Statement> FindStatement(const std::string& id) const;
  void RemoveStatement(const std::string& id);
  std::vector<std::shared_ptr<Statement>> Statements() const;
  // Forgets finished statements: those Forgettable(now, keep), then the oldest finished
  // ones beyond `max_finished` (spec §6.5).
  void ForgetFinished(WallClock::time_point now, std::chrono::seconds keep,
                      std::size_t max_finished);

  // Where a request came from, for Server::Sessions().
  struct Client {
    std::string_view address;
    std::string_view proxy;  // the connection's address when `address` was forwarded; else empty
    std::string_view user_agent;
    std::string_view tls_version;  // empty without TLS
    std::shared_ptr<const ClientCertificate> certificate;
  };
  // Records a request on the session. Throws session_not_found once it is closed (or the
  // session's own error once it was ended): a request that found the session just before
  // an idle sweep (or a KILL) closed it must not go on to run in it.
  void Touch(const Client& client = {});
  // The latest request's client, and when it came.
  struct LastRequest {
    std::string address;
    std::string proxy;
    std::string user_agent;
    std::string tls_version;
    std::shared_ptr<const ClientCertificate> certificate;
    WallClock::time_point at;
  };
  LastRequest last_request() const;
  WallClock::time_point created_at() const { return created_at_; }

  // Ends the session: cancels its statements and frees their results. Also finishes ending
  // a session End() ended.
  void Close(CancelReason reason);
  // Ends the session for `why` (Server::EndSession): it takes nothing new, and every
  // request naming it gets `why` (Touch). Nothing it runs is cancelled; Close() then tears
  // it down. Returns false when it was already closed or ended.
  bool End(const Error& why);
  // The error End() ended it with.
  std::optional<Error> ended() const;
  // Closes the session if it has been idle since `cutoff`: no request since then, nothing
  // running, and no retained result kept. The check and the close are one step under the
  // session's lock, as Touch() is, so a request either touches it first (and it stays) or
  // finds it closed (session_not_found); it never gets a session closed under it, its
  // result stream cut mid-body. Returns whether it closed it.
  bool CloseIfIdle(Clock::time_point cutoff, CancelReason reason);
  bool closed() const;

 private:
  const std::string id_;
  const Principal owner_;
  EngineStrand strand_;
  const WallClock::time_point created_at_ = WallClock::now();

  mutable std::mutex engine_mutex_;  // guards engine_ lifetime against Interrupt/Progress
  std::unique_ptr<EngineSession> engine_;
  const Statement* running_ = nullptr;  // guarded by engine_mutex_

  mutable std::mutex mutex_;
  json::object options_;
  TransactionState transaction_ = TransactionState::kNone;
  std::map<std::string, std::shared_ptr<Statement>> statements_;
  std::map<std::string, std::shared_ptr<PreparedStatement>> prepared_;
  std::map<std::string, std::shared_ptr<IngestOperation>> ingests_;
  std::vector<std::shared_ptr<ByteQueue>> uploads_;
  Clock::time_point last_activity_;
  LastRequest last_request_;
  bool closed_ = false;      // takes nothing new
  bool torn_down_ = false;   // its statements were cancelled and their results freed
  std::optional<Error> ended_;  // End()'s error

  bool IdleLocked(Clock::time_point cutoff) const;  // mutex_ held
  void Teardown(CancelReason reason);              // after closed_ and torn_down_ were set
  void ThrowIfClosedLocked() const;                // mutex_ held
};

class SessionRegistry {
 public:
  void Add(std::shared_ptr<Session> session);
  // The caller's session. Unknown ids and other principals' sessions both get
  // session_not_found (spec §9.2); a session that was ended, for its owner, the error it
  // was ended with, as long as it is remembered (Retire).
  std::shared_ptr<Session> Find(const std::string& id, const Principal& principal) const;
  // Any principal's session, or null.
  std::shared_ptr<Session> Get(const std::string& id) const;
  std::shared_ptr<Session> Remove(const std::string& id);
  // Removes an ended session, remembering until `until` that its owner's requests get
  // `why`.
  void Retire(const std::string& id, const Principal& owner, const Error& why, Clock::time_point until);
  // Forgets an ended session: a DELETE of it. Returns whether it was remembered.
  bool ForgetEnded(const std::string& id, const Principal& principal);
  // Forgets ended sessions remembered until before `now`.
  void SweepEnded(Clock::time_point now);
  std::vector<std::shared_ptr<Session>> All() const;
  std::size_t size() const;

 private:
  struct Ended {
    Principal owner;
    Error why;
    Clock::time_point until;
  };
  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Session>> sessions_;
  std::map<std::string, Ended> ended_;
};

}  // namespace chargewire::detail
