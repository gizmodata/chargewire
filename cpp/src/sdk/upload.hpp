// Moving an HTTP request body into the engine as Arrow (spec §7.2, §8.1): the HTTP
// coroutine pushes chunks into a bounded queue; an engine thread reads them as an Arrow
// IPC stream. Every batch is fully validated before the engine sees it.
#pragma once

#include <arrow/io/interfaces.h>
#include <arrow/ipc/dictionary.h>
#include <arrow/ipc/message.h>
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
#include "warpsql/backend.hpp"

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

// An upload's batches, each fully validated (ValidateFull()) before the engine sees it.
// A malformed batch fails the read with an Arrow status (it runs inside the engine, so it
// can't throw) and is remembered in failure(), which the SDK reports as
// unsupported_arrow_type. Safe to read from several threads (IngestBatches): reading
// the next message is serialized, decoding and validating it is not. A schema with
// dictionaries is read one batch at a time, as dictionaries are stream state.
class ValidatedReader : public warpsql::IngestBatches {
 public:
  // Reads the schema message; throws bad_request if the input isn't an IPC stream.
  explicit ValidatedReader(std::shared_ptr<arrow::io::InputStream> input);
  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
  arrow::Status NextIndexed(std::shared_ptr<arrow::RecordBatch>* batch, int64_t* index) override;
  std::optional<std::string> failure() const;

 private:
  arrow::Status Fail(const arrow::Status& status);

  std::shared_ptr<arrow::io::InputStream> input_;
  std::shared_ptr<arrow::Schema> schema_;
  std::mutex read_mutex_;  // the input, the messages and the dictionaries
  std::unique_ptr<arrow::ipc::MessageReader> messages_;
  std::unique_ptr<arrow::ipc::DictionaryMemo> dictionaries_;
  bool has_dictionaries_ = false;
  std::shared_ptr<arrow::RecordBatchReader> serial_;  // a schema with dictionaries
  bool ended_ = false;
  int64_t next_index_ = 0;
  mutable std::mutex mutex_;
  std::optional<std::string> failure_;
};

// An IPC stream from `input`, validated as it is read.
std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::shared_ptr<arrow::io::InputStream> input);
// The same over bytes held in memory (small bodies: prepared-statement parameters).
std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::string bytes);

// The error `error` holds, as an Error of the caller's own: an Error is copied, anything
// else is the internal error the API answers for it. Call it on the thread handling it.
inline Error OwnedError(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const Error& e) {
    return e;
  } catch (...) {
    return Error(ErrorCode::kExecutionError, "internal server error");
  }
}

// The result of engine work started with net::post, awaited by a coroutine. A failure
// crosses from the engine thread to the coroutine's I/O thread by value: Fail() copies the
// error there, on the thread that caught it, and Wait() throws a fresh copy of its own.
// Never the caught exception itself: its object is freed by whichever thread lets go of it
// last, possibly the engine's while the I/O thread still reads it for the response.
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
  // `error` is the one being handled on the calling thread (std::current_exception()).
  void Fail(std::exception_ptr error) { Fail(OwnedError(error)); }
  void Fail(const Error& error) {
    {
      std::lock_guard lock(mutex_);
      error_.emplace(error);  // a copy owning its message (Error's copy constructor)
    }
    signal_.Notify();
  }
  bool done() {
    std::lock_guard lock(mutex_);
    return value_.has_value() || error_.has_value();
  }
  Task<T> Wait() {
    uint64_t seen = signal_.version();
    for (;;) {
      {
        std::lock_guard lock(mutex_);
        if (error_) throw Error(*error_);  // this thread's own copy
        if (value_) co_return std::move(*value_);
      }
      seen = co_await signal_.WaitChange(seen, Clock::now() + std::chrono::hours(24));
    }
  }

 private:
  std::mutex mutex_;
  std::optional<T> value_;
  std::optional<Error> error_;
  Signal signal_;
};

}  // namespace warpsql::detail
