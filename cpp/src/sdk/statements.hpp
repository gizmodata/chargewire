// One statement's life (spec §6): state machine, result batches, retention, and the
// signal HTTP requests wait on. The engine thread produces; HTTP coroutines read.
#pragma once

#include <arrow/record_batch.h>

#include <condition_variable>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include "common.hpp"
#include "signal.hpp"
#include "warpsql/errors.hpp"

namespace warpsql::detail {

enum class StatementState { kQueued, kRunning, kSucceeded, kFailed, kCancelled };

enum class CancelReason { kNone, kClient, kTimeout, kSessionClosed, kServerShutdown };

std::string_view StateName(StatementState state);

// What a reader finds at a batch position.
enum class ReadStep { kBatch, kWait, kEnd, kFailed };

class Statement {
 public:
  struct Config {
    bool retain = false;
    // An unretained result's producer runs ahead of its reader by this many bytes (split
    // over the partitions), and by at least two batches per partition.
    std::size_t unretained_buffer_bytes = 32 * 1024 * 1024;
    // Partitioned results (spec §6.10): the most partitions the client reads at once
    // (and the server allows), and whether it lets go of the rows' order.
    std::size_t partitions = 1;
    bool any_order = false;
    std::chrono::seconds retention = std::chrono::seconds(3600);
    // Reserves (positive) or releases (negative) retained bytes against the owner's
    // quota; returns false when a reservation doesn't fit.
    std::function<bool(int64_t)> reserve_bytes;
  };

  Statement(std::string id, Config config);
  ~Statement();

  const std::string& id() const { return id_; }
  bool retained() const { return config_.retain; }
  Signal& changed() { return changed_; }

  // ---- Engine side ----
  // Moves queued -> running. False if the statement was cancelled while queued.
  bool BeginRunning();
  void SetRows(std::shared_ptr<arrow::Schema> schema, bool finish_if_abandoned = false);
  void SetUpdate(std::optional<int64_t> rows_affected);
  // Adds a result batch. For an unretained result, waits for the reader to make room.
  // False means stop producing: the statement was cancelled or exceeded its quota.
  bool AddBatch(std::shared_ptr<arrow::RecordBatch> batch);
  void Finish();
  void Fail(const Error& error);

  // ---- Control ----
  // Requests cancellation. Returns true when the statement is running, so the caller
  // must interrupt the engine.
  bool RequestCancel(CancelReason reason);
  // Nobody will read the result (its unretained reader went away, or the client
  // deleted the statement). A statement that changes data then runs on with its
  // batches discarded (returns true); any other must be cancelled by the caller
  // (returns false).
  bool AbandonRead();

  // ---- Readers (HTTP) ----
  json::object Document(std::optional<double> progress) const;
  StatementState state() const;
  bool IsTerminal() const;
  bool CancelRequested() const;
  // True once the statement has produced its outcome (rows or update) or ended.
  bool HasOutcome() const;
  bool HasRows() const;
  // The failure, for a failed or cancelled statement.
  std::optional<Error> error() const;
  std::shared_ptr<arrow::Schema> schema() const;

  // How many partitions the result has, once that is decided: at once for a result the
  // client asked to have in one partition, otherwise when the statement produces its
  // second batch (a split result) or ends with fewer (one partition).
  std::optional<std::size_t> PartitionCount() const;
  // Decides on one partition now if the split isn't decided yet: a result whose second
  // batch is slow to come isn't held back for it.
  void SettleSplit();
  // "interleaved" or "any" for a result in several partitions (spec §6.10).
  std::string_view PartitionOrder() const;

  // Starts reading partition `partition` at `from_batch`. Throws not_found (no such
  // partition), result_not_resumable (an unretained partition read before, or from a
  // later batch) or result_expired. Call once the partition count is decided.
  void ClaimRead(std::size_t partition, int64_t from_batch);
  // The batch at `index` of `partition`. Reading an unretained result consumes it.
  std::pair<ReadStep, std::shared_ptr<arrow::RecordBatch>> Read(std::size_t partition, int64_t index);
  // Ends a read. For an unretained result that didn't reach the end, the caller
  // cancels the statement (spec §6.5).
  void EndRead(std::size_t partition);

  // Drops a retained result whose time is up. Returns true if it did.
  bool ExpireIfDue(WallClock::time_point now);
  // Frees the result now (statement deleted or session closed).
  void Release();
  // True while a retained result is being kept.
  bool HoldsRetainedResult() const;
  // An unretained result nobody is reading, with its producer waiting for a reader.
  bool WaitingForReader() const;
  // True when the statement has finished at least `keep` ago, nobody is reading it,
  // and any retained result has expired: the session may forget it.
  bool Forgettable(WallClock::time_point now, std::chrono::seconds keep) const;
  // When the statement finished, if it has.
  std::optional<WallClock::time_point> finished_at() const;

 private:
  void Terminate(StatementState state, std::optional<Error> error);  // mutex_ held
  void DropBatches();                                                // mutex_ held
  bool IsTerminalLocked() const;                                     // mutex_ held
  // The partition the next batch goes to, and whether it has room for it.
  std::size_t Route() const;                                         // mutex_ held
  bool HasRoom() const;                                              // mutex_ held
  bool AnyReaderActive() const;                                      // mutex_ held

  // One partition's batches (spec §6.10); a result the client didn't ask to split has
  // just partition 0.
  struct Partition {
    std::deque<std::shared_ptr<arrow::RecordBatch>> batches;
    int64_t buffered_bytes = 0;  // of an unretained partition's batches, not yet read
    int64_t first_index = 0;  // index of batches.front(); grows as an unretained read consumes
    int64_t batch_count = 0;
    int64_t row_count = 0;
    bool read_claimed = false;  // unretained: the one read has started
    bool reader_active = false;
  };

  const std::string id_;
  const Config config_;
  const WallClock::time_point submitted_at_;

  mutable std::mutex mutex_;
  std::condition_variable room_;  // an unretained reader consumed a batch
  Signal changed_;

  StatementState state_ = StatementState::kQueued;
  CancelReason cancel_reason_ = CancelReason::kNone;
  std::optional<Error> error_;
  std::optional<WallClock::time_point> started_at_;
  std::optional<WallClock::time_point> finished_at_;

  enum class Kind { kUnknown, kRows, kUpdate } kind_ = Kind::kUnknown;
  std::optional<int64_t> rows_affected_;
  std::shared_ptr<arrow::Schema> schema_;
  std::string schema_base64_;

  std::vector<Partition> partitions_{1};
  bool split_decided_ = false;  // partitions_ has its final size
  int64_t batch_count_ = 0;  // over all partitions
  int64_t row_count_ = 0;
  int64_t reserved_bytes_ = 0;
  bool produced_all_ = false;

  bool finish_if_abandoned_ = false;
  bool abandoned_ = false;  // unretained, reader gone: batches are counted, not kept
  bool abandon_pending_ = false;  // abandoned before the outcome was known
  bool waiting_for_reader_ = false;
  bool expired_ = false;
  std::optional<WallClock::time_point> expires_at_;
};

// Remembers which statement an Idempotency-Key created (spec §6.7).
class IdempotencyStore {
 public:
  struct Entry {
    std::string request_hash;
    std::string session_id;
    std::string statement_id;
    Clock::time_point expires;
    std::shared_ptr<std::string> response;  // an ingest's answer, once it has one
  };

  // Records `entry` unless the key already has one; returns the existing entry if so.
  std::optional<Entry> Reserve(const std::string& principal, const std::string& key, Entry entry);
  void Sweep();

 private:
  std::mutex mutex_;
  std::map<std::pair<std::string, std::string>, Entry> entries_;
};

}  // namespace warpsql::detail
