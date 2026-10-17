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

OptionErrors EngineSession::SetOptions(const boost::json::object& options) {
  OptionErrors errors;
  for (const auto& option : options) {
    errors.emplace(std::string(option.key()),
                   Error(ErrorCode::kUnknownOption, "this server has no session options"));
  }
  return errors;
}

boost::json::object EngineSession::Options() { return {}; }

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

void Session::Interrupt() {
  std::lock_guard lock(engine_mutex_);
  if (engine_) {
    engine_->Interrupt();
  }
}

std::optional<double> Session::Progress() {
  std::lock_guard lock(engine_mutex_);
  return engine_ ? engine_->Progress() : std::nullopt;
}

json::object Session::options() const {
  std::lock_guard lock(mutex_);
  return options_;
}

void Session::set_options(json::object options) {
  std::lock_guard lock(mutex_);
  options_ = std::move(options);
}

std::string Session::AddPrepared(std::shared_ptr<PreparedStatement> prepared) {
  std::string id = NewId("p");
  std::lock_guard lock(mutex_);
  prepared_[id] = std::move(prepared);
  return id;
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

void Session::Touch() {
  std::lock_guard lock(mutex_);
  last_activity_ = Clock::now();
}

bool Session::IdleSince(Clock::time_point cutoff) const {
  std::lock_guard lock(mutex_);
  if (last_activity_ > cutoff) {
    return false;
  }
  for (const auto& [id, statement] : statements_) {
    // A statement still running keeps the session alive, unless it is only waiting
    // for someone to read an unretained result.
    if ((!statement->IsTerminal() && !statement->WaitingForReader()) ||
        statement->HoldsRetainedResult()) {
      return false;
    }
  }
  return true;
}

void Session::Close(CancelReason reason) {
  std::vector<std::shared_ptr<Statement>> statements;
  {
    std::lock_guard lock(mutex_);
    if (closed_) {
      return;
    }
    closed_ = true;
    for (const auto& [id, statement] : statements_) {
      statements.push_back(statement);
    }
  }
  bool interrupt = false;
  for (const auto& statement : statements) {
    interrupt = statement->RequestCancel(reason) || interrupt;
    statement->Release();
  }
  if (interrupt) {
    Interrupt();
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
  if (it == sessions_.end() || !it->second->owner().SameIdentity(principal) ||
      it->second->closed()) {
    throw Error(ErrorCode::kSessionNotFound, "no such session");
  }
  return it->second;
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
