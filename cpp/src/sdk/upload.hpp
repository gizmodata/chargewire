// Moving an HTTP request body into the engine as Arrow (spec §7.2, §8.1): the HTTP
// coroutine pushes chunks into a bounded queue; an engine thread reads them as an Arrow
// IPC stream. Every batch is fully validated before the engine sees it.
#pragma once

#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>

#include <condition_variable>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "common.hpp"
#include "signal.hpp"

namespace warpsql::detail {

class ByteQueue {
 public:
  explicit ByteQueue(std::size_t max_chunks) : max_chunks_(max_chunks) {}

  // Coroutine side. Waits for room; false if the consumer has stopped.
  Task<bool> Push(std::string chunk);
  void Close();  // end of input
  void Abort();  // the input failed; the reader sees an error

  // Engine side: blocks until bytes, end of input, or abort.
  arrow::Result<int64_t> Read(int64_t nbytes, void* out);
  // The consumer is done (finished or failed); further pushes are dropped.
  void StopReading();

 private:
  const std::size_t max_chunks_;
  std::mutex mutex_;
  std::condition_variable data_;
  Signal room_;
  std::deque<std::string> chunks_;
  std::size_t offset_ = 0;  // into chunks_.front()
  bool closed_ = false;
  bool aborted_ = false;
  bool stopped_ = false;
};

// An Arrow input stream over a ByteQueue.
std::shared_ptr<arrow::io::InputStream> QueueInput(std::shared_ptr<ByteQueue> queue);

// Reads batches through ValidateFull(). A malformed batch fails the read with an Arrow
// status (it runs inside the engine, so it can't throw) and is remembered in failure(),
// which the SDK reports as unsupported_arrow_type.
class ValidatedReader : public arrow::RecordBatchReader {
 public:
  explicit ValidatedReader(std::shared_ptr<arrow::RecordBatchReader> source);
  std::shared_ptr<arrow::Schema> schema() const override;
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override;
  std::optional<std::string> failure() const;

 private:
  std::shared_ptr<arrow::RecordBatchReader> source_;
  mutable std::mutex mutex_;
  std::optional<std::string> failure_;
};

// An IPC stream from `input`, validated as it is read.
std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::shared_ptr<arrow::io::InputStream> input);
// The same over bytes held in memory (small bodies: prepared-statement parameters).
std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::string bytes);

// The result of engine work started with net::post, awaited by a coroutine.
template <typename T>
class Completion {
 public:
  void Succeed(T value) {
    {
      std::lock_guard lock(mutex_);
      value_ = std::move(value);
    }
    signal_.Notify();
  }
  void Fail(std::exception_ptr error) {
    {
      std::lock_guard lock(mutex_);
      error_ = std::move(error);
    }
    signal_.Notify();
  }
  bool done() {
    std::lock_guard lock(mutex_);
    return value_.has_value() || error_ != nullptr;
  }
  Task<T> Wait() {
    uint64_t seen = signal_.version();
    for (;;) {
      {
        std::lock_guard lock(mutex_);
        if (error_) std::rethrow_exception(error_);
        if (value_) co_return std::move(*value_);
      }
      seen = co_await signal_.WaitChange(seen, Clock::now() + std::chrono::hours(24));
    }
  }

 private:
  std::mutex mutex_;
  std::optional<T> value_;
  std::exception_ptr error_;
  Signal signal_;
};

}  // namespace warpsql::detail
