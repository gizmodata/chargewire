#include "upload.hpp"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>

#include <cstring>

#include "warpsql/errors.hpp"

namespace warpsql::detail {

Task<bool> ByteQueue::Push(std::string chunk) {
  for (;;) {
    uint64_t seen = room_.version();
    {
      std::lock_guard lock(mutex_);
      if (stopped_) {
        co_return false;
      }
      if (chunks_.size() < max_chunks_) {
        chunks_.push_back(std::move(chunk));
        data_.notify_all();
        co_return true;
      }
    }
    co_await room_.WaitChange(seen, Clock::now() + std::chrono::seconds(30));
  }
}

void ByteQueue::Close() {
  std::lock_guard lock(mutex_);
  closed_ = true;
  data_.notify_all();
}

void ByteQueue::Abort() {
  std::lock_guard lock(mutex_);
  aborted_ = true;
  data_.notify_all();
}

void ByteQueue::StopReading() {
  {
    std::lock_guard lock(mutex_);
    stopped_ = true;
    chunks_.clear();
  }
  room_.Notify();
}

arrow::Result<int64_t> ByteQueue::Read(int64_t nbytes, void* out) {
  auto* dest = static_cast<char*>(out);
  int64_t copied = 0;
  std::unique_lock lock(mutex_);
  while (copied < nbytes) {
    data_.wait(lock, [this] { return !chunks_.empty() || closed_ || aborted_; });
    if (aborted_) {
      return arrow::Status::IOError("the upload was interrupted");
    }
    if (chunks_.empty()) {
      break;  // closed: end of input
    }
    const std::string& front = chunks_.front();
    const auto take = std::min<int64_t>(nbytes - copied,
                                        static_cast<int64_t>(front.size() - offset_));
    std::memcpy(dest + copied, front.data() + offset_, static_cast<std::size_t>(take));
    copied += take;
    offset_ += static_cast<std::size_t>(take);
    if (offset_ == front.size()) {
      chunks_.pop_front();
      offset_ = 0;
      lock.unlock();
      room_.Notify();
      lock.lock();
    }
  }
  return copied;
}

namespace {

class QueueInputStream : public arrow::io::InputStream {
 public:
  explicit QueueInputStream(std::shared_ptr<ByteQueue> queue) : queue_(std::move(queue)) {}
  ~QueueInputStream() override { queue_->StopReading(); }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t got, queue_->Read(nbytes, out));
    position_ += got;
    return got;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t got, Read(nbytes, buffer->mutable_data()));
    ARROW_RETURN_NOT_OK(buffer->Resize(got, /*shrink_to_fit=*/false));
    return std::shared_ptr<arrow::Buffer>(std::move(buffer));
  }
  arrow::Status Close() override {
    closed_ = true;
    queue_->StopReading();
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }
  arrow::Result<int64_t> Tell() const override { return position_; }

 private:
  std::shared_ptr<ByteQueue> queue_;
  int64_t position_ = 0;
  bool closed_ = false;
};

}  // namespace

std::shared_ptr<arrow::io::InputStream> QueueInput(std::shared_ptr<ByteQueue> queue) {
  return std::make_shared<QueueInputStream>(std::move(queue));
}

ValidatedReader::ValidatedReader(std::shared_ptr<arrow::RecordBatchReader> source)
    : source_(std::move(source)) {}

std::shared_ptr<arrow::Schema> ValidatedReader::schema() const { return source_->schema(); }

arrow::Status ValidatedReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) {
  // This runs behind the Arrow C stream interface, inside the engine: report failures
  // as a status, never an exception.
  ARROW_RETURN_NOT_OK(source_->ReadNext(batch));
  if (*batch != nullptr) {
    auto status = (*batch)->ValidateFull();
    if (!status.ok()) {
      std::lock_guard lock(mutex_);
      failure_ = "the uploaded Arrow data is not valid: " + status.message();
      *batch = nullptr;
      return arrow::Status::Invalid(*failure_);
    }
  }
  return arrow::Status::OK();
}

std::optional<std::string> ValidatedReader::failure() const {
  std::lock_guard lock(mutex_);
  return failure_;
}

std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::shared_ptr<arrow::io::InputStream> input) {
  auto reader = arrow::ipc::RecordBatchStreamReader::Open(std::move(input));
  if (!reader.ok()) {
    throw Error(ErrorCode::kBadRequest,
                "the body is not an Arrow IPC stream: " + reader.status().message());
  }
  return std::make_shared<ValidatedReader>(std::move(reader).ValueUnsafe());
}

std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::string bytes) {
  return OpenValidatedIpc(
      std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(bytes))));
}

}  // namespace warpsql::detail
