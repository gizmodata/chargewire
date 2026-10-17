#include "statements.hpp"

#include <arrow/array.h>

#include "ipc.hpp"
#include "json.hpp"

namespace warpsql::detail {

namespace {

std::string_view ReasonName(CancelReason reason) {
  switch (reason) {
    case CancelReason::kNone: return "";
    case CancelReason::kClient: return "client";
    case CancelReason::kTimeout: return "timeout";
    case CancelReason::kSessionClosed: return "session_closed";
    case CancelReason::kServerShutdown: return "server_shutdown";
  }
  return "";
}

Error CancelError(CancelReason reason) {
  if (reason == CancelReason::kTimeout) {
    return Error(ErrorCode::kStatementTimeout, "the statement ran longer than its timeout");
  }
  return Error(ErrorCode::kStatementCancelled,
               "the statement was cancelled (" + std::string(ReasonName(reason)) + ")");
}

int64_t BatchBytes(const arrow::RecordBatch& batch) {
  int64_t bytes = 0;
  for (const auto& column : batch.columns()) {
    for (const auto& buffer : column->data()->buffers) {
      if (buffer) {
        bytes += buffer->size();
      }
    }
  }
  return bytes;
}

}  // namespace

std::string_view StateName(StatementState state) {
  switch (state) {
    case StatementState::kQueued: return "queued";
    case StatementState::kRunning: return "running";
    case StatementState::kSucceeded: return "succeeded";
    case StatementState::kFailed: return "failed";
    case StatementState::kCancelled: return "cancelled";
  }
  return "failed";
}

Statement::Statement(std::string id, Config config)
    : id_(std::move(id)),
      config_(std::move(config)),
      submitted_at_(WallClock::now()) {}

Statement::~Statement() { Release(); }

bool Statement::BeginRunning() {
  {
    std::lock_guard lock(mutex_);
    if (state_ != StatementState::kQueued) {
      return false;  // cancelled while queued
    }
    state_ = StatementState::kRunning;
    started_at_ = WallClock::now();
  }
  changed_.Notify();
  return true;
}

void Statement::SetRows(std::shared_ptr<arrow::Schema> schema, bool finish_if_abandoned) {
  std::string encoded = SchemaBase64(schema);
  {
    std::lock_guard lock(mutex_);
    kind_ = Kind::kRows;
    finish_if_abandoned_ = finish_if_abandoned;
    schema_ = std::move(schema);
    schema_base64_ = std::move(encoded);
  }
  changed_.Notify();
}

void Statement::SetUpdate(std::optional<int64_t> rows_affected) {
  {
    std::lock_guard lock(mutex_);
    kind_ = Kind::kUpdate;
    rows_affected_ = rows_affected;
  }
  changed_.Notify();
}

bool Statement::AddBatch(std::shared_ptr<arrow::RecordBatch> batch) {
  {
    std::unique_lock lock(mutex_);
    if (abandoned_ && cancel_reason_ == CancelReason::kNone && state_ == StatementState::kRunning) {
      row_count_ += batch->num_rows();
      batch_count_ += 1;
      return true;  // nobody will read it; the statement still runs to the end
    }
    if (config_.retain) {
      const int64_t bytes = BatchBytes(*batch);
      if (config_.reserve_bytes && !config_.reserve_bytes(bytes)) {
        Terminate(StatementState::kFailed,
                  Error(ErrorCode::kResultQuotaExceeded,
                        "the result is larger than the space left for retained results"));
        lock.unlock();
        changed_.Notify();
        return false;
      }
      reserved_bytes_ += bytes;
    } else {
      // Backpressure: wait for the reader, unless the statement ends meanwhile.
      waiting_for_reader_ = true;
      room_.wait(lock, [this] {
        return batches_.size() < config_.unretained_buffer_batches || abandoned_ ||
               cancel_reason_ != CancelReason::kNone || IsTerminalLocked();
      });
      waiting_for_reader_ = false;
    }
    if (cancel_reason_ != CancelReason::kNone || state_ != StatementState::kRunning) {
      return false;
    }
    if (abandoned_) {
      row_count_ += batch->num_rows();
      batch_count_ += 1;
      return true;  // nobody will read it; the statement still runs to the end
    }
    row_count_ += batch->num_rows();
    batch_count_ += 1;
    batches_.push_back(std::move(batch));
  }
  changed_.Notify();
  return true;
}

void Statement::Finish() {
  {
    std::lock_guard lock(mutex_);
    if (IsTerminalLocked()) {
      return;  // already failed (e.g. over quota) or cancelled
    }
    if (cancel_reason_ != CancelReason::kNone) {
      Terminate(StatementState::kCancelled, CancelError(cancel_reason_));
    } else {
      produced_all_ = true;
      Terminate(StatementState::kSucceeded, std::nullopt);
    }
  }
  changed_.Notify();
}

void Statement::Fail(const Error& error) {
  {
    std::lock_guard lock(mutex_);
    if (IsTerminalLocked()) {
      return;
    }
    // An engine error after a cancel request is the engine stopping as asked.
    if (cancel_reason_ != CancelReason::kNone) {
      Terminate(StatementState::kCancelled, CancelError(cancel_reason_));
    } else {
      Terminate(StatementState::kFailed, error);
    }
  }
  changed_.Notify();
}

bool Statement::RequestCancel(CancelReason reason) {
  bool running = false;
  {
    std::lock_guard lock(mutex_);
    if (IsTerminalLocked()) {
      return false;
    }
    if (cancel_reason_ == CancelReason::kNone) {
      cancel_reason_ = reason;
    }
    if (state_ == StatementState::kQueued) {
      Terminate(StatementState::kCancelled, CancelError(cancel_reason_));
    } else {
      running = true;
    }
  }
  room_.notify_all();
  changed_.Notify();
  return running;
}

bool Statement::AbandonRead() {
  {
    std::lock_guard lock(mutex_);
    if (!finish_if_abandoned_) {
      return false;
    }
    abandoned_ = true;
    DropBatches();
  }
  room_.notify_all();
  return true;
}

void Statement::Terminate(StatementState state, std::optional<Error> error) {
  state_ = state;
  error_ = std::move(error);
  finished_at_ = WallClock::now();
  if (state == StatementState::kSucceeded && config_.retain) {
    expires_at_ = *finished_at_ + config_.retention;
  }
  if (state != StatementState::kSucceeded) {
    DropBatches();
  }
  room_.notify_all();
}

bool Statement::IsTerminalLocked() const {
  return state_ == StatementState::kSucceeded || state_ == StatementState::kFailed ||
         state_ == StatementState::kCancelled;
}

void Statement::DropBatches() {
  batches_.clear();
  if (reserved_bytes_ != 0 && config_.reserve_bytes) {
    config_.reserve_bytes(-reserved_bytes_);
  }
  reserved_bytes_ = 0;
}

StatementState Statement::state() const {
  std::lock_guard lock(mutex_);
  return state_;
}

bool Statement::IsTerminal() const {
  std::lock_guard lock(mutex_);
  return IsTerminalLocked();
}

bool Statement::HasOutcome() const {
  std::lock_guard lock(mutex_);
  return kind_ != Kind::kUnknown || IsTerminalLocked();
}

bool Statement::HasRows() const {
  std::lock_guard lock(mutex_);
  return kind_ == Kind::kRows;
}

std::optional<Error> Statement::error() const {
  std::lock_guard lock(mutex_);
  return error_;
}

std::shared_ptr<arrow::Schema> Statement::schema() const {
  std::lock_guard lock(mutex_);
  return schema_;
}

json::object Statement::Document(std::optional<double> progress) const {
  std::lock_guard lock(mutex_);
  json::object doc{
      {"statement_id", id_},
      {"state", StateName(state_)},
      {"submitted_at", Rfc3339(submitted_at_)},
  };
  if (started_at_) {
    doc["started_at"] = Rfc3339(*started_at_);
  }
  if (finished_at_) {
    doc["finished_at"] = Rfc3339(*finished_at_);
  }
  if (state_ == StatementState::kRunning) {
    json::object progress_doc{{"rows_produced", row_count_}};
    if (progress) {
      progress_doc["fraction"] = *progress;
    }
    doc["progress"] = std::move(progress_doc);
  }
  if (error_) {
    doc["error"] = Problem(*error_, "");
  }
  if (state_ == StatementState::kCancelled) {
    doc["cancel_reason"] = ReasonName(cancel_reason_);
  }

  json::value result = nullptr;
  if (kind_ == Kind::kUpdate) {
    json::object update{{"kind", "update"}};
    if (rows_affected_) {
      update["rows_affected"] = *rows_affected_;
    }
    result = std::move(update);
  } else if (kind_ == Kind::kRows && state_ != StatementState::kFailed &&
             state_ != StatementState::kCancelled) {
    json::object partition{{"index", 0}, {"location", nullptr}};
    partition["batches"] = produced_all_ ? json::value(batch_count_) : json::value(nullptr);
    partition["rows"] = produced_all_ ? json::value(row_count_) : json::value(nullptr);
    json::object rows{
        {"kind", "rows"},
        {"schema", schema_base64_},
        {"partitions", json::array{std::move(partition)}},
        {"retained", config_.retain},
    };
    if (expires_at_) {
      rows["expires_at"] = Rfc3339(*expires_at_);
    }
    result = std::move(rows);
  }
  doc["result"] = std::move(result);
  return doc;
}

void Statement::ClaimRead(int64_t from_batch) {
  std::lock_guard lock(mutex_);
  if (expired_) {
    throw Error(ErrorCode::kResultExpired, "this statement's result is no longer kept");
  }
  if (from_batch < 0) {
    throw Error(ErrorCode::kBadRequest, "from_batch must not be negative");
  }
  if (config_.retain) {
    reader_active_ = true;
    return;
  }
  if (read_claimed_ || from_batch > 0) {
    throw Error(ErrorCode::kResultNotResumable,
                "this result is not retained: it can be read once, from batch 0");
  }
  read_claimed_ = true;
  reader_active_ = true;
}

std::pair<ReadStep, std::shared_ptr<arrow::RecordBatch>> Statement::Read(int64_t index) {
  std::shared_ptr<arrow::RecordBatch> batch;
  {
    std::lock_guard lock(mutex_);
    if (state_ == StatementState::kFailed || state_ == StatementState::kCancelled || expired_) {
      return {ReadStep::kFailed, nullptr};
    }
    const int64_t available_end = first_index_ + static_cast<int64_t>(batches_.size());
    if (index < available_end && index >= first_index_) {
      if (config_.retain) {
        return {ReadStep::kBatch, batches_[static_cast<std::size_t>(index - first_index_)]};
      }
      batch = std::move(batches_.front());
      batches_.pop_front();
      first_index_ += 1;
    } else if (produced_all_ && index >= batch_count_) {
      return {ReadStep::kEnd, nullptr};
    } else {
      return {ReadStep::kWait, nullptr};
    }
  }
  room_.notify_all();  // an unretained producer may be waiting for this room
  return {ReadStep::kBatch, std::move(batch)};
}

void Statement::EndRead() {
  std::lock_guard lock(mutex_);
  reader_active_ = false;
}

bool Statement::ExpireIfDue(WallClock::time_point now) {
  {
    std::lock_guard lock(mutex_);
    if (!expires_at_ || expired_ || now < *expires_at_) {
      return false;
    }
    expired_ = true;
    DropBatches();
  }
  changed_.Notify();
  return true;
}

void Statement::Release() {
  std::lock_guard lock(mutex_);
  DropBatches();
  expired_ = true;
}

bool Statement::WaitingForReader() const {
  std::lock_guard lock(mutex_);
  return waiting_for_reader_ && !reader_active_;
}

bool Statement::Forgettable(WallClock::time_point now, std::chrono::seconds keep) const {
  std::lock_guard lock(mutex_);
  if (!IsTerminalLocked() || reader_active_ || !finished_at_ || now < *finished_at_ + keep) {
    return false;
  }
  return !expires_at_ || expired_ || now >= *expires_at_;
}

std::optional<WallClock::time_point> Statement::finished_at() const {
  std::lock_guard lock(mutex_);
  return finished_at_;
}

bool Statement::HoldsRetainedResult() const {
  std::lock_guard lock(mutex_);
  return config_.retain && !expired_ && !batches_.empty();
}

std::optional<IdempotencyStore::Entry> IdempotencyStore::Reserve(const std::string& principal,
                                                                  const std::string& key,
                                                                  Entry entry) {
  std::lock_guard lock(mutex_);
  auto [it, inserted] = entries_.try_emplace({principal, key}, entry);
  if (!inserted && it->second.expires <= Clock::now()) {
    it->second = std::move(entry);  // an expired key is free again
    return std::nullopt;
  }
  if (inserted) {
    return std::nullopt;
  }
  return it->second;
}

void IdempotencyStore::Sweep() {
  const auto now = Clock::now();
  std::lock_guard lock(mutex_);
  std::erase_if(entries_, [now](const auto& item) { return item.second.expires <= now; });
}

}  // namespace warpsql::detail
