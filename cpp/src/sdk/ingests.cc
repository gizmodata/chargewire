#include "ingests.hpp"

#include <format>

#include <arrow/type.h>

#include "json.hpp"

namespace warpsql::detail {

namespace {

std::string_view StateName(IngestOperation::State state) {
  switch (state) {
    case IngestOperation::State::kOpen: return "open";
    case IngestOperation::State::kCommitting: return "committing";
    case IngestOperation::State::kCommitted: return "committed";
    case IngestOperation::State::kFailed: return "failed";
    case IngestOperation::State::kAborted: return "aborted";
  }
  return "failed";
}

}  // namespace

// The engine's view of the ingest: every part's batches, read by however many engine
// threads call NextIndexed at once, each taking a part no other thread is reading.
class IngestOperation::Reader : public IngestBatches {
 public:
  explicit Reader(std::shared_ptr<IngestOperation> op) : op_(std::move(op)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return op_->schema_; }
  bool Ordered() const override { return false; }  // parts arrive in no defined order (§8.3)

  // Each call reads from the part with the fewest threads in it. A part's reader is
  // itself safe from several threads (it reads one message at a time and decodes outside
  // its lock), so a part that arrives fast is decoded by several threads at once.
  arrow::Status NextIndexed(std::shared_ptr<arrow::RecordBatch>* batch, int64_t* index) override {
    *batch = nullptr;
    IngestOperation& op = *op_;
    std::unique_lock lock(op.mutex_);
    for (;;) {
      if (op.failure_) return arrow::Status::IOError(op.failure_->what());
      Part* pick = nullptr;
      int number = 0;
      bool any_reading = false;
      for (auto& [n, part] : op.parts_) {
        any_reading = any_reading || part.readers > 0;
        if (part.ended || part.opening) continue;
        if (pick == nullptr || part.readers < pick->readers) {
          pick = &part;
          number = n;
        }
      }
      if (pick == nullptr) {
        if (op.sealed_ && !any_reading) {
          return arrow::Status::OK();  // every part read: the end of the upload
        }
        op.work_.wait(lock);
        continue;
      }
      if (!pick->reader) {
        // Opening reads the part's schema message: one thread does it, the others wait.
        pick->opening = true;
        auto queue = pick->queue;
        lock.unlock();
        std::optional<Error> failed;
        std::shared_ptr<ValidatedReader> opened;
        try {
          opened = OpenValidatedIpc(QueueInput(queue));
          if (!opened->schema()->Equals(*op.schema_)) {
            throw Error(ErrorCode::kBadRequest,
                        std::format("part {} has another schema than the ingest's first part", number));
          }
        } catch (const Error& error) {
          failed = error;
        }
        lock.lock();
        Part& part = op.parts_[number];
        part.opening = false;
        part.reader = opened;
        if (failed) op.FailLocked(*failed);
        op.work_.notify_all();
        continue;
      }
      pick->readers += 1;
      auto reader = pick->reader;
      lock.unlock();

      std::optional<Error> failed;
      std::shared_ptr<arrow::RecordBatch> next;
      int64_t ignored = 0;
      if (auto status = reader->NextIndexed(&next, &ignored); !status.ok()) {
        failed = reader->failure() ? Error(ErrorCode::kUnsupportedArrowType, *reader->failure())
                                   : Error(ErrorCode::kExecutionError,
                                           std::format("part {}: {}", number, status.message()));
      }

      lock.lock();
      Part& part = op.parts_[number];
      part.readers -= 1;
      if (part.ended && part.readers == 0) op.changed_.Notify();  // the part's rows are final
      if (failed) {
        op.FailLocked(*failed);
        continue;
      }
      if (!next) {
        part.ended = true;
        if (part.readers == 0) op.changed_.Notify();
        op.work_.notify_all();
        continue;
      }
      part.rows += next->num_rows();
      *batch = std::move(next);
      *index = op.next_index_++;
      return arrow::Status::OK();
    }
  }

 private:
  std::shared_ptr<IngestOperation> op_;
};

IngestOperation::IngestOperation(std::string id, IngestTarget target)
    : id_(std::move(id)), target_(std::move(target)), last_activity_(Clock::now()) {}

void IngestOperation::Touch() { last_activity_ = Clock::now(); }

std::shared_ptr<ByteQueue> IngestOperation::AddPart(int part, bool* first) {
  std::lock_guard lock(mutex_);
  Touch();
  if (failure_) throw *failure_;
  if (state_ != State::kOpen) {
    throw Error(ErrorCode::kBadRequest, "this ingest is being committed and takes no more parts");
  }
  if (parts_.contains(part)) {
    throw Error(ErrorCode::kIngestPartExists, std::format("part {} of this ingest was already sent", part));
  }
  auto queue = std::make_shared<ByteQueue>(4);
  parts_[part].queue = queue;
  *first = !started_;
  started_ = true;
  active_uploads_ += 1;
  work_.notify_all();
  return queue;
}

void IngestOperation::PartUploadEnded() {
  std::lock_guard lock(mutex_);
  active_uploads_ -= 1;
  Touch();
}

std::optional<int64_t> IngestOperation::PartRows(int part) {
  std::lock_guard lock(mutex_);
  if (failure_) throw *failure_;
  auto it = parts_.find(part);
  if (it == parts_.end() || !it->second.ended || it->second.readers > 0) return std::nullopt;
  return it->second.rows;
}

void IngestOperation::Seal(int parts) {
  {
    std::lock_guard lock(mutex_);
    Touch();
    if (failure_) throw *failure_;
    if (state_ != State::kOpen) {
      return;  // already committing: the caller waits for the outcome
    }
    if (parts < 1) {
      throw Error(ErrorCode::kBadRequest, "an ingest is committed with at least one part");
    }
    std::string missing;
    for (int i = 0; i < parts; i++) {
      auto it = parts_.find(i);
      if (it == parts_.end() || !it->second.ended || it->second.readers > 0) {
        missing += (missing.empty() ? "" : ", ") + std::to_string(i);
      }
    }
    for (const auto& [n, part] : parts_) {
      if (n >= parts) missing += (missing.empty() ? "" : ", ") + std::to_string(n) + " (beyond the count)";
    }
    if (!missing.empty()) {
      throw Error(ErrorCode::kIngestIncomplete,
                  "these parts have not finished: " + missing + "; the ingest stays open");
    }
    sealed_ = parts;
    state_ = State::kCommitting;
    work_.notify_all();
  }
  changed_.Notify();
}

std::optional<int64_t> IngestOperation::Committed() {
  std::lock_guard lock(mutex_);
  if (failure_) throw *failure_;
  return rows_ingested_;
}

void IngestOperation::FailLocked(const Error& error) {
  if (!failure_) failure_ = error;
  if (state_ == State::kOpen || state_ == State::kCommitting) state_ = State::kFailed;
  // Readers blocked on a part's network stop; uploads still sending are refused.
  for (auto& [n, part] : parts_) {
    if (part.queue) {
      part.queue->Abort();        // a reader waiting for the part's bytes fails
      part.queue->StopReading();  // an upload still sending stops
    }
  }
  work_.notify_all();
  changed_.Notify();
}

void IngestOperation::Fail(const Error& error) {
  std::lock_guard lock(mutex_);
  FailLocked(error);
}

void IngestOperation::Abort(const Error& why) {
  std::lock_guard lock(mutex_);
  if (state_ == State::kCommitted) return;
  FailLocked(why);
  state_ = State::kAborted;
}

bool IngestOperation::Finished() const {
  std::lock_guard lock(mutex_);
  return state_ == State::kCommitted || state_ == State::kFailed || state_ == State::kAborted;
}

bool IngestOperation::Expired(Clock::time_point cutoff) const {
  std::lock_guard lock(mutex_);
  return state_ == State::kOpen && active_uploads_ == 0 && last_activity_ < cutoff;
}

json::object IngestOperation::Document() const {
  std::lock_guard lock(mutex_);
  json::array parts;
  for (const auto& [n, part] : parts_) {
    if (part.ended) parts.push_back(json::object{{"part", n}, {"rows_received", part.rows}});
  }
  json::object doc{{"ingest_id", id_}, {"state", StateName(state_)}, {"parts", std::move(parts)}};
  if (rows_ingested_) doc["rows_ingested"] = *rows_ingested_;
  if (failure_) doc["error"] = Problem(*failure_, "");
  return doc;
}

std::shared_ptr<IngestBatches> IngestOperation::Batches(int part, std::shared_ptr<ValidatedReader> first_part) {
  std::lock_guard lock(mutex_);
  schema_ = first_part->schema();
  parts_[part].reader = std::move(first_part);
  return std::make_shared<Reader>(shared_from_this());
}

void IngestOperation::EngineDone(std::optional<int64_t> rows, std::exception_ptr error) {
  {
    std::lock_guard lock(mutex_);
    if (error) {
      try {
        std::rethrow_exception(error);
      } catch (const Error& e) {
        FailLocked(e);
      } catch (const std::exception& e) {
        FailLocked(Error(ErrorCode::kExecutionError, e.what()));
      }
    } else {
      // The engine committed. An abort that came after it had read every part (while
      // it committed) changed nothing: the rows are written, so that is the outcome.
      failure_.reset();
      rows_ingested_ = rows;
      state_ = State::kCommitted;
    }
    for (auto& [n, part] : parts_) {
      if (part.queue) part.queue->StopReading();
    }
  }
  changed_.Notify();
}

}  // namespace warpsql::detail
