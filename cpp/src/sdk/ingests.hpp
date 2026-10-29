// Partitioned ingest (spec §8.3): one ingest, opened once, fed by several part uploads
// at once, each over its own connection, and committed atomically. The engine sees one
// IngestBatches reader over every part: its threads take the next batch from whichever
// part has one, so the parts are decoded and inserted in parallel, in one statement (one
// transaction). Memory stays bounded: each part's body goes through a bounded queue.
#pragma once

#include <arrow/record_batch.h>

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "common.hpp"
#include "signal.hpp"
#include "upload.hpp"
#include "chargewire/backend.hpp"
#include "chargewire/errors.hpp"

namespace chargewire::detail {

class IngestOperation : public std::enable_shared_from_this<IngestOperation> {
 public:
  enum class State { kOpen, kCommitting, kCommitted, kFailed, kAborted };

  IngestOperation(std::string id, IngestTarget target, std::optional<boost::json::object> query_tag = std::nullopt,
                  std::size_t max_message_bytes = kDefaultMaxIngestMessageBytes);

  const std::string& id() const { return id_; }
  const IngestTarget& target() const { return target_; }
  // The opening request's Chargewire-Query-Tag (spec §2.5), if it had one.
  const std::optional<boost::json::object>& query_tag() const { return query_tag_; }
  Signal& changed() { return changed_; }

  // ---- Part uploads (HTTP) ----
  // Registers part `part` and returns its queue. Throws the ingest's error, or
  // ingest_part_exists, or bad_request when the ingest no longer takes parts. Sets
  // *first when this is the first part (the caller then starts the engine's statement).
  std::shared_ptr<ByteQueue> AddPart(int part, bool* first);
  // The part's upload ended (its body was read, or the connection failed).
  void PartUploadEnded();
  // Whether the engine has read part `part` to its end; throws the ingest's error.
  // nullopt while it hasn't.
  std::optional<int64_t> PartRows(int part);

  // ---- Control ----
  // Asks to commit parts 0..n-1. Throws ingest_incomplete (some part isn't done; the
  // ingest stays open), bad_request, or the ingest's error.
  void Seal(int parts);
  // The result once the engine committed: rows ingested; nullopt while it hasn't.
  // Throws the ingest's error.
  std::optional<int64_t> Committed();
  // Aborts: nothing is written. Safe from any thread.
  void Abort(const Error& why);
  // Fails the ingest with `error` (the first failure wins).
  void Fail(const Error& error);
  bool Expired(Clock::time_point cutoff) const;
  // Finished, and nothing has asked about it since `cutoff`: the session may forget it
  // (until then a retried commit or a GET still gets its outcome).
  bool Forgettable(Clock::time_point cutoff) const;
  // Committed, failed or aborted.
  bool Finished() const;
  // Part uploads under way.
  int ActiveUploads() const;
  json::object Document() const;

  // ---- Engine side ----
  // Starts with the first part's schema (read by the caller on an engine thread) and
  // returns the reader the engine's Ingest() reads every part through.
  std::shared_ptr<IngestBatches> Batches(int part, std::shared_ptr<ValidatedReader> first_part);
  // The engine's statement ended with `rows` (committed) or threw.
  void EngineDone(std::optional<int64_t> rows, std::exception_ptr error);

 private:
  class Reader;
  struct Part {
    std::shared_ptr<ByteQueue> queue;
    std::shared_ptr<ValidatedReader> reader;  // opened by the engine thread that first reads it
    int readers = 0;       // engine threads reading it now
    bool opening = false;  // an engine thread is reading its schema message
    bool ended = false;    // read to its end
    int64_t rows = 0;
  };

  // mutex_ held:
  void FailLocked(const Error& error);
  void Touch();

  const std::string id_;
  const IngestTarget target_;
  const std::optional<boost::json::object> query_tag_;
  const std::size_t max_message_bytes_;
  mutable std::mutex mutex_;
  std::condition_variable work_;  // engine threads: a part to read, a seal, a failure
  Signal changed_;                // HTTP coroutines: a part ended, committed, failed
  State state_ = State::kOpen;
  std::map<int, Part> parts_;
  bool started_ = false;
  std::optional<int> sealed_;
  std::optional<Error> failure_;
  std::optional<int64_t> rows_ingested_;
  std::shared_ptr<arrow::Schema> schema_;
  int active_uploads_ = 0;
  Clock::time_point last_activity_;
  std::atomic<int64_t> next_index_ = 0;
};

}  // namespace chargewire::detail
