#include "statements.hpp"

#include <arrow/array.h>

#include <format>

#include "ipc.hpp"
#include "json.hpp"

namespace warpsql::detail {

namespace {

Error CancelError(CancelReason reason) {
  if (reason == CancelReason::kTimeout) {
    return Error(ErrorCode::kStatementTimeout, "the statement ran longer than its timeout");
  }
  return Error(ErrorCode::kStatementCancelled,
               "the statement was cancelled (" + std::string(CancelReasonName(reason)) + ")");
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

std::string_view CancelReasonName(CancelReason reason) {
  switch (reason) {
    case CancelReason::kNone: return "";
    case CancelReason::kClient: return "client";
    case CancelReason::kTimeout: return "timeout";
    case CancelReason::kSessionClosed: return "session_closed";
    case CancelReason::kServerShutdown: return "server_shutdown";
    case CancelReason::kKilled: return "killed";
    case CancelReason::kCancelled: return "cancelled";
  }
  return "";
}

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
      submitted_at_(WallClock::now()) {
  split_decided_ = config_.partitions <= 1;
}

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
    if (abandon_pending_) {  // deleted before anyone knew whether it writes
      if (finish_if_abandoned) {
        abandoned_ = true;
      } else if (cancel_reason_ == CancelReason::kNone) {
        cancel_reason_ = CancelReason::kClient;
      }
    }
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

std::size_t Statement::Route() const {
  const std::size_t count = partitions_.size();
  if (count == 1) return 0;
  if (config_.any_order && !config_.retain) {
    // Any order: the partition whose reader is furthest ahead (fewest batches waiting).
    std::size_t best = 0;
    for (std::size_t i = 1; i < count; i++) {
      if (partitions_[i].batches.size() < partitions_[best].batches.size()) best = i;
    }
    return best;
  }
  // Interleaved (spec §6.10): batch j of the result is batch j / k of partition j mod k.
  // A retained result in any order is dealt the same way, so its partitions are fixed.
  return static_cast<std::size_t>(batch_count_ % static_cast<int64_t>(count));
}

bool Statement::HasRoom() const {
  const Partition& partition = partitions_[Route()];
  const auto limit = static_cast<int64_t>(config_.unretained_buffer_bytes / partitions_.size());
  return partition.batches.size() < 2 || partition.buffered_bytes < limit;
}

void Statement::DecideSplit(bool total) {
  if (split_decided_) return;
  const std::size_t waiting = pending_.size();
  const std::size_t most = std::max<std::size_t>(config_.partitions, 1);
  std::size_t k = 1;
  if (waiting >= most) {
    k = most;  // a large result: as many partitions as the client reads at once
  } else if (total) {
    k = std::clamp<std::size_t>((waiting + 1) / 2, 1, most);  // the whole result: two batches each
  } else {
    k = std::clamp<std::size_t>(waiting, 1, most);  // a slow one: one per batch so far
  }
  partitions_.resize(k);
  split_decided_ = true;
  for (auto& batch : pending_) {
    Partition& partition = partitions_[Route()];
    batch_count_ += 1;
    partition.row_count += batch->num_rows();
    partition.batch_count += 1;
    if (!config_.retain) partition.buffered_bytes += BatchBytes(*batch);
    partition.batches.push_back(std::move(batch));
  }
  pending_.clear();
}

std::optional<Clock::time_point> Statement::FirstBatchAt() const {
  std::lock_guard lock(mutex_);
  return first_pending_at_;
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
    }
    if (!split_decided_) {
      // Held back until the result's size shows (see PartitionCount). At most as many
      // batches wait as the result may get partitions, then the split is decided; that is
      // no more than the two per partition a split result buffers anyway.
      if (cancel_reason_ != CancelReason::kNone || state_ != StatementState::kRunning) {
        return false;
      }
      row_count_ += batch->num_rows();
      if (!first_pending_at_) first_pending_at_ = Clock::now();
      pending_.push_back(std::move(batch));
      if (pending_.size() >= std::max<std::size_t>(config_.partitions, 1)) DecideSplit(false);
      lock.unlock();
      changed_.Notify();
      return true;
    }
    if (!config_.retain) {
      // Backpressure: wait for the partition's reader, unless the statement ends meanwhile.
      waiting_for_reader_ = true;
      room_.wait(lock, [this] {
        return HasRoom() || abandoned_ || cancel_reason_ != CancelReason::kNone || IsTerminalLocked();
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
    Partition& partition = partitions_[Route()];
    row_count_ += batch->num_rows();
    batch_count_ += 1;
    partition.row_count += batch->num_rows();
    partition.batch_count += 1;
    if (!config_.retain) partition.buffered_bytes += BatchBytes(*batch);
    partition.batches.push_back(std::move(batch));
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
      DecideSplit(true);  // the whole result is known: split by its size
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
    if (IsTerminalLocked()) {
      return true;  // nothing left to stop
    }
    if (kind_ == Kind::kUnknown) {
      abandon_pending_ = true;  // SetRows decides, once the engine says whether it writes
      return true;
    }
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
  split_decided_ = true;
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
  pending_.clear();
  for (auto& partition : partitions_) {
    partition.batches.clear();
    partition.buffered_bytes = 0;
  }
  if (reserved_bytes_ != 0 && config_.reserve_bytes) {
    config_.reserve_bytes(-reserved_bytes_);
  }
  reserved_bytes_ = 0;
}

StatementState Statement::state() const {
  std::lock_guard lock(mutex_);
  return state_;
}

bool Statement::CancelRequested() const {
  std::lock_guard lock(mutex_);
  return cancel_reason_ != CancelReason::kNone;
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
    doc["cancel_reason"] = CancelReasonName(cancel_reason_);
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
    // Until the split is decided, the document lists partition 0 only (spec §6.10).
    json::array partitions;
    for (std::size_t i = 0; i < partitions_.size(); i++) {
      const Partition& part = partitions_[i];
      json::object partition{{"index", i}, {"location", nullptr}};
      partition["batches"] = produced_all_ ? json::value(part.batch_count) : json::value(nullptr);
      partition["rows"] = produced_all_ ? json::value(part.row_count) : json::value(nullptr);
      partitions.push_back(std::move(partition));
    }
    json::object rows{
        {"kind", "rows"},
        {"schema", schema_base64_},
        {"partitions", std::move(partitions)},
        {"retained", config_.retain},
    };
    if (partitions_.size() > 1) {
      rows["partition_order"] = config_.any_order ? "any" : "interleaved";
    }
    if (expires_at_) {
      rows["expires_at"] = Rfc3339(*expires_at_);
    }
    result = std::move(rows);
  }
  doc["result"] = std::move(result);
  return doc;
}

std::optional<std::size_t> Statement::PartitionCount() const {
  std::lock_guard lock(mutex_);
  if (!split_decided_) return std::nullopt;
  return partitions_.size();
}

void Statement::SettleSplit() {
  {
    std::lock_guard lock(mutex_);
    if (split_decided_) return;
    DecideSplit(false);
  }
  room_.notify_all();
  changed_.Notify();
}

std::string_view Statement::PartitionOrder() const {
  return config_.any_order ? "any" : "interleaved";
}

void Statement::ClaimRead(std::size_t partition, int64_t from_batch) {
  std::lock_guard lock(mutex_);
  if (expired_) {
    throw Error(ErrorCode::kResultExpired, "this statement's result is no longer kept");
  }
  if (from_batch < 0) {
    throw Error(ErrorCode::kBadRequest, "from_batch must not be negative");
  }
  if (partition >= partitions_.size()) {
    throw Error(ErrorCode::kNotFound,
                std::format("this result has {} partition{}", partitions_.size(),
                            partitions_.size() == 1 ? ", 0" : "s"));
  }
  Partition& part = partitions_[partition];
  if (config_.retain) {
    part.reader_active = true;
    return;
  }
  if (part.read_claimed || from_batch > 0) {
    throw Error(ErrorCode::kResultNotResumable,
                "this result is not retained: each partition can be read once, from batch 0");
  }
  part.read_claimed = true;
  part.reader_active = true;
}

std::pair<ReadStep, std::shared_ptr<arrow::RecordBatch>> Statement::Read(std::size_t partition,
                                                                         int64_t index) {
  std::shared_ptr<arrow::RecordBatch> batch;
  {
    std::lock_guard lock(mutex_);
    if (state_ == StatementState::kFailed || state_ == StatementState::kCancelled || expired_ ||
        partition >= partitions_.size()) {
      return {ReadStep::kFailed, nullptr};
    }
    Partition& part = partitions_[partition];
    const int64_t available_end = part.first_index + static_cast<int64_t>(part.batches.size());
    if (index < available_end && index >= part.first_index) {
      if (config_.retain) {
        return {ReadStep::kBatch, part.batches[static_cast<std::size_t>(index - part.first_index)]};
      }
      batch = std::move(part.batches.front());
      part.batches.pop_front();
      part.first_index += 1;
      part.buffered_bytes -= BatchBytes(*batch);
    } else if (produced_all_ && index >= part.batch_count) {
      return {ReadStep::kEnd, nullptr};
    } else {
      return {ReadStep::kWait, nullptr};
    }
  }
  room_.notify_all();  // an unretained producer may be waiting for this room
  return {ReadStep::kBatch, std::move(batch)};
}

void Statement::EndRead(std::size_t partition) {
  std::lock_guard lock(mutex_);
  if (partition < partitions_.size()) partitions_[partition].reader_active = false;
}

bool Statement::AnyReaderActive() const {
  for (const auto& partition : partitions_) {
    if (partition.reader_active) return true;
  }
  return false;
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
  return waiting_for_reader_ && !AnyReaderActive();
}

bool Statement::Forgettable(WallClock::time_point now, std::chrono::seconds keep) const {
  std::lock_guard lock(mutex_);
  if (!IsTerminalLocked() || AnyReaderActive() || !finished_at_ || now < *finished_at_ + keep) {
    return false;
  }
  return !expires_at_ || expired_ || now >= *expires_at_;
}

std::optional<WallClock::time_point> Statement::finished_at() const {
  std::lock_guard lock(mutex_);
  return finished_at_;
}

Statement::Snapshot Statement::Describe() const {
  std::lock_guard lock(mutex_);
  Snapshot out;
  out.state = state_;
  out.cancel_reason = cancel_reason_;
  out.has_rows = kind_ == Kind::kRows;
  out.submitted_at = submitted_at_;
  out.started_at = started_at_;
  if (kind_ == Kind::kRows && split_decided_) out.partitions = partitions_.size();
  for (const auto& partition : partitions_) {
    if (partition.reader_active) out.active_readers++;
  }
  if (config_.retain && !expired_) {
    for (const auto& partition : partitions_) {
      if (!partition.batches.empty()) out.holds_retained_result = true;
    }
  }
  out.rows = row_count_;
  out.batches = batch_count_;
  return out;
}

CancelReason Statement::cancel_reason() const {
  std::lock_guard lock(mutex_);
  return cancel_reason_;
}

bool Statement::HoldsRetainedResult() const {
  std::lock_guard lock(mutex_);
  if (!config_.retain || expired_) return false;
  for (const auto& partition : partitions_) {
    if (!partition.batches.empty()) return true;
  }
  return false;
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
