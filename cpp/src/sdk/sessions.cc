#include "sessions.hpp"

#include <algorithm>

#include "ids.hpp"

namespace warpsql {

// Defaults for the optional EngineSession methods: "not implemented" (spec §11).
StatementOutcome EngineSession::Execute(std::string_view, const boost::json::value&) {
  throw Error(ErrorCode::kNotImplemented, "this server does not run SQL");
}

std::unique_ptr<PreparedStatement> EngineSession::Prepare(std::string_view) {
  throw Error(ErrorCode::kNotImplemented, "this server does not support prepared statements");
}

StatementOutcome EngineSession::ExecutePrepared(PreparedStatement&, const Parameters&) {
  throw Error(ErrorCode::kNotImplemented, "this server does not support prepared statements");
}

int64_t EngineSession::Ingest(const IngestTarget&, std::shared_ptr<arrow::RecordBatchReader>) {
  throw Error(ErrorCode::kNotImplemented, "this server does not support bulk ingest");
}

std::shared_ptr<arrow::RecordBatchReader> EngineSession::Metadata(MetadataKind,
                                                                  const MetadataFilter&) {
  throw Error(ErrorCode::kNotImplemented, "this server does not serve this metadata");
}

void EngineSession::Interrupt() {}

std::optional<double> EngineSession::Progress() { return std::nullopt; }

std::optional<std::chrono::milliseconds> EngineSession::DefaultStatementTimeout() {
  return std::nullopt;
}

OptionErrors EngineSession::SetOptions(const boost::json::object& options) {
  OptionErrors errors;
  for (const auto& option : options) {
    errors.emplace(std::string(option.key()),
                   Error(ErrorCode::kUnknownOption, "this server has no session options"));
  }
  return errors;
}

boost::json::object EngineSession::Options() { return {}; }

std::string_view TransactionStateName(TransactionState state) {
  switch (state) {
    case TransactionState::kOpen:
      return "open";
    case TransactionState::kAborted:
      return "aborted";
    case TransactionState::kNone:
      break;
  }
  return "none";
}

void EngineSession::Commit() {
  throw Error(ErrorCode::kNotImplemented, "this server does not support transactions");
}

void EngineSession::Rollback() {
  throw Error(ErrorCode::kNotImplemented, "this server does not support transactions");
}

}  // namespace warpsql

namespace warpsql::detail {

Session::Session(std::string id, Principal owner, EngineStrand strand)
    : id_(std::move(id)),
      owner_(std::move(owner)),
      strand_(std::move(strand)),
      last_activity_(Clock::now()) {}

EngineSession& Session::engine() {
  std::lock_guard lock(engine_mutex_);
  if (!engine_) {
    throw Error(ErrorCode::kSessionNotFound, "the session has ended");
  }
  return *engine_;
}

void Session::SetEngine(std::unique_ptr<EngineSession> engine) {
  std::lock_guard lock(engine_mutex_);
  engine_ = std::move(engine);
}

void Session::DestroyEngine() {
  std::unique_ptr<EngineSession> engine;
  std::map<std::string, std::shared_ptr<PreparedStatement>> prepared;
  {
    std::lock_guard lock(mutex_);
    prepared.swap(prepared_);
  }
  prepared.clear();  // prepared statements go before the engine session they belong to
  {
    std::lock_guard lock(engine_mutex_);
    engine = std::move(engine_);
  }
  // Destroyed outside the lock: closing an engine session can take a while.
}

void Session::Interrupt(CancelReason reason) {
  std::lock_guard lock(engine_mutex_);
  if (engine_) {
    engine_->Cancel(CancelReasonName(reason));
  }
}

void Session::InterruptIfRunning(const Statement* statement, CancelReason reason) {
  std::lock_guard lock(engine_mutex_);
  if (engine_ && running_ == statement) {
    engine_->Cancel(CancelReasonName(reason));
  }
}

void Session::SetRunning(const Statement* statement) {
  std::lock_guard lock(engine_mutex_);
  running_ = statement;
}

bool Session::IsRunning(const Statement* statement) const {
  std::lock_guard lock(engine_mutex_);
  return running_ != nullptr && running_ == statement;
}

std::optional<double> Session::Progress() {
  std::lock_guard lock(engine_mutex_);
  return engine_ ? engine_->Progress() : std::nullopt;
}

std::optional<std::chrono::milliseconds> Session::DefaultStatementTimeout() {
  std::lock_guard lock(engine_mutex_);
  return engine_ ? engine_->DefaultStatementTimeout() : std::nullopt;
}

void Session::CheckEnded() {
  std::lock_guard lock(engine_mutex_);
  if (engine_) engine_->CheckEnded();
}

json::object Session::options() const {
  std::lock_guard lock(mutex_);
  return options_;
}

void Session::set_options(json::object options) {
  std::lock_guard lock(mutex_);
  options_ = std::move(options);
}

TransactionState Session::transaction() const {
  std::lock_guard lock(mutex_);
  return transaction_;
}

void Session::RefreshTransaction() {
  TransactionState now;
  {
    std::lock_guard lock(engine_mutex_);
    if (!engine_) return;
    try {
      now = engine_->Transaction();
    } catch (const std::exception&) {
      return;  // the engine is going away, or can't tell: keep what it said last
    }
  }
  std::lock_guard lock(mutex_);
  transaction_ = now;
}

void Session::ThrowIfClosedLocked() const {
  if (ended_) throw *ended_;
  if (closed_) {
    throw Error(ErrorCode::kSessionNotFound, "no such session");  // closed since the request found it
  }
}

void Session::AddIngest(std::shared_ptr<IngestOperation> ingest) {
  std::lock_guard lock(mutex_);
  ThrowIfClosedLocked();
  ingests_[ingest->id()] = std::move(ingest);
}

std::shared_ptr<IngestOperation> Session::FindIngest(const std::string& id) const {
  std::lock_guard lock(mutex_);
  auto it = ingests_.find(id);
  if (it == ingests_.end()) {
    throw Error(ErrorCode::kIngestNotFound, "no such ingest in this session");
  }
  return it->second;
}

void Session::RemoveIngest(const std::string& id) {
  std::lock_guard lock(mutex_);
  ingests_.erase(id);
}

void Session::AddUpload(const std::shared_ptr<ByteQueue>& upload) {
  {
    std::lock_guard lock(mutex_);
    if (!closed_) {
      uploads_.push_back(upload);
      return;
    }
  }
  upload->Abort();
}

void Session::RemoveUpload(const ByteQueue* upload) {
  std::lock_guard lock(mutex_);
  std::erase_if(uploads_, [upload](const auto& held) { return held.get() == upload; });
}

std::size_t Session::UploadCount() const {
  std::lock_guard lock(mutex_);
  return uploads_.size();
}

std::size_t Session::AbortUploads() {
  std::vector<std::shared_ptr<ByteQueue>> uploads;
  {
    std::lock_guard lock(mutex_);
    uploads = uploads_;  // each removes itself when its request ends
  }
  for (const auto& upload : uploads) upload->Abort();
  return uploads.size();
}

std::vector<std::shared_ptr<IngestOperation>> Session::Ingests() const {
  std::lock_guard lock(mutex_);
  std::vector<std::shared_ptr<IngestOperation>> out;
  for (const auto& [id, ingest] : ingests_) out.push_back(ingest);
  return out;
}

void Session::AddPrepared(const std::string& id, std::shared_ptr<PreparedStatement> prepared) {
  std::lock_guard lock(mutex_);
  prepared_[id] = std::move(prepared);
}

std::shared_ptr<PreparedStatement> Session::FindPrepared(const std::string& id) const {
  std::lock_guard lock(mutex_);
  auto it = prepared_.find(id);
  if (it == prepared_.end()) {
    throw Error(ErrorCode::kPreparedNotFound, "no such prepared statement in this session");
  }
  return it->second;
}

std::shared_ptr<PreparedStatement> Session::RemovePrepared(const std::string& id) {
  std::lock_guard lock(mutex_);
  auto it = prepared_.find(id);
  if (it == prepared_.end()) {
    throw Error(ErrorCode::kPreparedNotFound, "no such prepared statement in this session");
  }
  auto prepared = std::move(it->second);
  prepared_.erase(it);
  return prepared;
}

void Session::AddStatement(std::shared_ptr<Statement> statement) {
  std::lock_guard lock(mutex_);
  ThrowIfClosedLocked();
  statements_[statement->id()] = std::move(statement);
}

std::shared_ptr<Statement> Session::FindStatement(const std::string& id) const {
  std::lock_guard lock(mutex_);
  auto it = statements_.find(id);
  if (it == statements_.end()) {
    throw Error(ErrorCode::kStatementNotFound, "no such statement in this session");
  }
  return it->second;
}

void Session::RemoveStatement(const std::string& id) {
  std::lock_guard lock(mutex_);
  statements_.erase(id);
}

std::vector<std::shared_ptr<Statement>> Session::Statements() const {
  std::lock_guard lock(mutex_);
  std::vector<std::shared_ptr<Statement>> out;
  for (const auto& [id, statement] : statements_) {
    out.push_back(statement);
  }
  return out;
}

void Session::ForgetFinished(WallClock::time_point now, std::chrono::seconds keep,
                             std::size_t max_finished) {
  std::vector<std::shared_ptr<Statement>> forgotten;  // released outside the lock
  std::lock_guard lock(mutex_);
  std::vector<std::pair<WallClock::time_point, std::string>> finished;
  for (auto it = statements_.begin(); it != statements_.end();) {
    if (it->second->Forgettable(now, keep)) {
      forgotten.push_back(std::move(it->second));
      it = statements_.erase(it);
      continue;
    }
    if (auto at = it->second->finished_at(); at && it->second->Forgettable(now, std::chrono::seconds(0))) {
      finished.emplace_back(*at, it->first);
    }
    ++it;
  }
  if (finished.size() > max_finished) {
    std::sort(finished.begin(), finished.end());
    for (std::size_t i = 0; i < finished.size() - max_finished; ++i) {
      auto it = statements_.find(finished[i].second);
      forgotten.push_back(std::move(it->second));
      statements_.erase(it);
    }
  }
}

void Session::Touch(const Client& client) {
  const auto now = WallClock::now();
  std::lock_guard lock(mutex_);
  ThrowIfClosedLocked();
  last_activity_ = Clock::now();
  // Assigned only when they change: a session's requests mostly come over one connection.
  LastRequest& last = last_request_;
  if (last.address != client.address) last.address.assign(client.address);
  if (last.user_agent != client.user_agent) last.user_agent.assign(client.user_agent);
  if (last.tls_version != client.tls_version) last.tls_version.assign(client.tls_version);
  if (last.certificate != client.certificate) last.certificate = client.certificate;
  last.at = now;
}

Session::LastRequest Session::last_request() const {
  std::lock_guard lock(mutex_);
  return last_request_;
}

bool Session::IdleLocked(Clock::time_point cutoff) const {
  if (last_activity_ > cutoff) {
    return false;
  }
  for (const auto& [id, ingest] : ingests_) {
    if (!ingest->Finished()) return false;  // an ingest in progress (it expires on its own)
  }
  for (const auto& [id, statement] : statements_) {
    // A statement still running keeps the session alive, unless it is only waiting
    // for someone to read an unretained result. So does a retained result until it
    // expires (spec §5.7), and a read of a result in progress, however slow: it is a
    // request that hasn't ended, and evicting the session would cut its stream (a
    // finished unretained result still has up to unretained_buffer_bytes to send).
    if ((!statement->IsTerminal() && !statement->WaitingForReader()) ||
        statement->HoldsRetainedResult() || statement->ReadInProgress()) {
      return false;
    }
  }
  return true;
}

void Session::Close(CancelReason reason) {
  {
    std::lock_guard lock(mutex_);
    if (torn_down_) {
      return;
    }
    closed_ = true;
    torn_down_ = true;
  }
  Teardown(reason);
}

bool Session::End(const Error& why) {
  std::lock_guard lock(mutex_);
  if (closed_) {
    return false;
  }
  closed_ = true;
  ended_ = why;
  return true;
}

std::optional<Error> Session::ended() const {
  std::lock_guard lock(mutex_);
  return ended_;
}

bool Session::CloseIfIdle(Clock::time_point cutoff, CancelReason reason) {
  {
    std::lock_guard lock(mutex_);
    if (closed_ || !IdleLocked(cutoff)) {
      return false;
    }
    closed_ = true;
    torn_down_ = true;
  }
  Teardown(reason);
  return true;
}

void Session::Teardown(CancelReason reason) {
  // closed_ is set, so nothing is added from here on (AddStatement, AddIngest and
  // AddUpload refuse a closed session): these are all there will be.
  std::vector<std::shared_ptr<Statement>> statements;
  {
    std::lock_guard lock(mutex_);
    for (const auto& [id, statement] : statements_) {
      statements.push_back(statement);
    }
  }
  std::vector<std::shared_ptr<IngestOperation>> ingests;
  std::vector<std::shared_ptr<ByteQueue>> uploads;
  {
    std::lock_guard lock(mutex_);
    for (const auto& [id, ingest] : ingests_) ingests.push_back(ingest);
    ingests_.clear();
    uploads.swap(uploads_);
  }
  for (const auto& ingest : ingests) {
    ingest->Abort(Error(ErrorCode::kStatementCancelled, "the session ended, so its ingest was aborted"));
  }
  for (const auto& upload : uploads) {
    upload->Abort();  // its engine thread's read fails, and the load rolls back
  }
  bool interrupt = false;
  for (const auto& statement : statements) {
    interrupt = statement->RequestCancel(reason) || interrupt;
    statement->Release();
  }
  if (interrupt) {
    Interrupt(reason);
  }
}

bool Session::closed() const {
  std::lock_guard lock(mutex_);
  return closed_;
}

void SessionRegistry::Add(std::shared_ptr<Session> session) {
  std::lock_guard lock(mutex_);
  sessions_[session->id()] = std::move(session);
}

std::shared_ptr<Session> SessionRegistry::Find(const std::string& id,
                                               const Principal& principal) const {
  std::lock_guard lock(mutex_);
  auto it = sessions_.find(id);
  if (it == sessions_.end()) {
    // An ended session's owner learns why (spec §5.6), as long as it is remembered.
    if (auto ended = ended_.find(id); ended != ended_.end() && ended->second.owner.SameIdentity(principal)) {
      throw ended->second.why;
    }
    throw Error(ErrorCode::kSessionNotFound, "no such session");
  }
  if (!it->second->owner().SameIdentity(principal)) {
    throw Error(ErrorCode::kSessionNotFound, "no such session");
  }
  if (auto why = it->second->ended()) {
    throw *why;
  }
  if (it->second->closed()) {
    throw Error(ErrorCode::kSessionNotFound, "no such session");
  }
  // A session keeps the binding of the token that opened it (spec §4.4): every request on
  // it carries a token bound the same way, the same certificate or none. So whatever runs
  // in the session (a scoped token it mints, say) acts under the binding of the request.
  const std::string& bound = it->second->owner().certificate_thumbprint;
  const std::string& presented = principal.certificate_thumbprint;
  if (bound.empty() != presented.empty() || (!bound.empty() && !ConstantTimeEquals(bound, presented))) {
    throw Error(ErrorCode::kCertificateMismatch,
                bound.empty() ? "this session was opened with a token not bound to a client certificate; "
                                "a certificate-bound token opens sessions of its own"
                              : "this session was opened with a token bound to a client certificate; use "
                                "it with a token from that certificate's sign-in");
  }
  return it->second;
}

std::shared_ptr<Session> SessionRegistry::Get(const std::string& id) const {
  std::lock_guard lock(mutex_);
  auto it = sessions_.find(id);
  return it == sessions_.end() ? nullptr : it->second;
}

void SessionRegistry::Retire(const std::string& id, const Principal& owner, const Error& why,
                             Clock::time_point until) {
  std::lock_guard lock(mutex_);
  sessions_.erase(id);
  ended_.insert_or_assign(id, Ended{owner, why, until});
}

bool SessionRegistry::ForgetEnded(const std::string& id, const Principal& principal) {
  std::lock_guard lock(mutex_);
  auto it = ended_.find(id);
  if (it == ended_.end() || !it->second.owner.SameIdentity(principal)) return false;
  ended_.erase(it);
  return true;
}

void SessionRegistry::SweepEnded(Clock::time_point now) {
  std::lock_guard lock(mutex_);
  std::erase_if(ended_, [now](const auto& item) { return item.second.until <= now; });
}

std::shared_ptr<Session> SessionRegistry::Remove(const std::string& id) {
  std::lock_guard lock(mutex_);
  auto it = sessions_.find(id);
  if (it == sessions_.end()) {
    return nullptr;
  }
  auto session = std::move(it->second);
  sessions_.erase(it);
  return session;
}

std::vector<std::shared_ptr<Session>> SessionRegistry::All() const {
  std::lock_guard lock(mutex_);
  std::vector<std::shared_ptr<Session>> out;
  for (const auto& [id, session] : sessions_) {
    out.push_back(session);
  }
  return out;
}

std::size_t SessionRegistry::size() const {
  std::lock_guard lock(mutex_);
  return sessions_.size();
}

}  // namespace warpsql::detail
