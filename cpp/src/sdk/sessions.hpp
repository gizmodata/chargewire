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
#include "warpsql/backend.hpp"

namespace warpsql::detail {

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

  // Safe from any thread while the engine is busy.
  void Interrupt();
  // Interrupts the engine only while `statement` is the one running on it, so a late
  // interrupt never reaches the session's next statement. Safe from any thread.
  void InterruptIfRunning(const Statement* statement);
  // The statement the engine is running (nullptr when idle). Set on strand().
  void SetRunning(const Statement* statement);
  std::optional<double> Progress();
  std::optional<std::chrono::milliseconds> DefaultStatementTimeout();
  // Throws session_ended when the engine ended the session (EngineSession::CheckEnded).
  // Safe from any thread.
  void CheckEnded();

  json::object options() const;
  void set_options(json::object options);

  // Prepared statements (spec §7). Engine objects: destroy them on strand().
  std::string AddPrepared(std::shared_ptr<PreparedStatement> prepared);
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

  void AddStatement(std::shared_ptr<Statement> statement);
  // Throws statement_not_found.
  std::shared_ptr<Statement> FindStatement(const std::string& id) const;
  void RemoveStatement(const std::string& id);
  std::vector<std::shared_ptr<Statement>> Statements() const;
  // Forgets finished statements: those Forgettable(now, keep), then the oldest finished
  // ones beyond `max_finished` (spec §6.5).
  void ForgetFinished(WallClock::time_point now, std::chrono::seconds keep,
                      std::size_t max_finished);

  // Records a request on the session. Throws session_not_found once it is closed: a
  // request that found the session just before an idle sweep (or a KILL) closed it must
  // not go on to run in it.
  void Touch();

  // Ends the session: cancels its statements and frees their results.
  void Close(CancelReason reason);
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

  mutable std::mutex engine_mutex_;  // guards engine_ lifetime against Interrupt/Progress
  std::unique_ptr<EngineSession> engine_;
  const Statement* running_ = nullptr;  // guarded by engine_mutex_

  mutable std::mutex mutex_;
  json::object options_;
  std::map<std::string, std::shared_ptr<Statement>> statements_;
  std::map<std::string, std::shared_ptr<PreparedStatement>> prepared_;
  std::map<std::string, std::shared_ptr<IngestOperation>> ingests_;
  std::vector<std::shared_ptr<ByteQueue>> uploads_;
  Clock::time_point last_activity_;
  bool closed_ = false;

  bool IdleLocked(Clock::time_point cutoff) const;  // mutex_ held
  void Teardown(CancelReason reason);              // after closed_ was set
};

class SessionRegistry {
 public:
  void Add(std::shared_ptr<Session> session);
  // The caller's session. Unknown ids and other principals' sessions both get
  // session_not_found (spec §9.2).
  std::shared_ptr<Session> Find(const std::string& id, const Principal& principal) const;
  std::shared_ptr<Session> Remove(const std::string& id);
  std::vector<std::shared_ptr<Session>> All() const;
  std::size_t size() const;

 private:
  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Session>> sessions_;
};

}  // namespace warpsql::detail
