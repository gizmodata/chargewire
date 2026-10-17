#include "upload.hpp"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/message.h>

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

namespace {

bool HasDictionary(const arrow::DataType& type) {
  if (type.id() == arrow::Type::DICTIONARY) return true;
  for (const auto& child : type.fields()) {
    if (HasDictionary(*child->type())) return true;
  }
  return false;
}

// Gives back the schema message the reader already read, then the rest.
class ReplaySchema : public arrow::ipc::MessageReader {
 public:
  ReplaySchema(std::unique_ptr<arrow::ipc::Message> schema, std::unique_ptr<arrow::ipc::MessageReader> rest)
      : schema_(std::move(schema)), rest_(std::move(rest)) {}
  arrow::Result<std::unique_ptr<arrow::ipc::Message>> ReadNextMessage() override {
    if (schema_) return std::move(schema_);
    return rest_->ReadNextMessage();
  }

 private:
  std::unique_ptr<arrow::ipc::Message> schema_;
  std::unique_ptr<arrow::ipc::MessageReader> rest_;
};

arrow::ipc::IpcReadOptions ReadOptions() {
  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.use_threads = false;  // batches are decoded in parallel instead
  return options;
}

}  // namespace

ValidatedReader::ValidatedReader(std::shared_ptr<arrow::io::InputStream> input)
    : input_(std::move(input)),
      messages_(arrow::ipc::MessageReader::Open(input_.get())),
      dictionaries_(std::make_unique<arrow::ipc::DictionaryMemo>()) {
  auto not_ipc = [](const std::string& why) {
    return Error(ErrorCode::kBadRequest, "the body is not an Arrow IPC stream: " + why);
  };
  auto message = messages_->ReadNextMessage();
  if (!message.ok()) throw not_ipc(message.status().message());
  if (*message == nullptr) throw not_ipc("it is empty");
  if ((*message)->type() != arrow::ipc::MessageType::SCHEMA) {
    throw not_ipc("it does not start with a schema message");
  }
  auto schema = arrow::ipc::ReadSchema(**message, dictionaries_.get());
  if (!schema.ok()) throw not_ipc(schema.status().message());
  schema_ = std::move(schema).ValueUnsafe();
  for (const auto& field : schema_->fields()) {
    has_dictionaries_ = has_dictionaries_ || HasDictionary(*field->type());
  }
  if (has_dictionaries_) {
    // Dictionaries are stream state: Arrow's stream reader reads it, one batch at a time.
    auto reader = arrow::ipc::RecordBatchStreamReader::Open(
        std::make_unique<ReplaySchema>(std::move(message).ValueUnsafe(), std::move(messages_)), ReadOptions());
    if (!reader.ok()) throw not_ipc(reader.status().message());
    serial_ = std::move(reader).ValueUnsafe();
  }
}

arrow::Status ValidatedReader::Fail(const arrow::Status& status) {
  std::lock_guard lock(mutex_);
  failure_ = "the uploaded Arrow data is not valid: " + status.message();
  return arrow::Status::Invalid(*failure_);
}

arrow::Status ValidatedReader::NextIndexed(std::shared_ptr<arrow::RecordBatch>* batch,
                                           int64_t* index) {
  // This runs behind the Arrow C stream interface, inside the engine: report failures
  // as a status, never an exception.
  *batch = nullptr;
  std::unique_lock read(read_mutex_);
  if (serial_) {
    std::shared_ptr<arrow::RecordBatch> decoded;
    ARROW_RETURN_NOT_OK(serial_->ReadNext(&decoded));
    if (decoded == nullptr) return arrow::Status::OK();
    *index = next_index_++;
    if (auto status = decoded->ValidateFull(); !status.ok()) return Fail(status);
    *batch = std::move(decoded);
    return arrow::Status::OK();
  }
  if (ended_) return arrow::Status::OK();
  ARROW_ASSIGN_OR_RAISE(auto message, messages_->ReadNextMessage());
  if (message == nullptr) {
    ended_ = true;
    return arrow::Status::OK();
  }
  if (message->type() != arrow::ipc::MessageType::RECORD_BATCH) {
    return arrow::Status::Invalid("an unexpected IPC message in the upload's stream");
  }
  *index = next_index_++;
  read.unlock();  // decoding needs nothing the next reader changes
  auto decoded = arrow::ipc::ReadRecordBatch(*message, schema_, nullptr, ReadOptions());
  ARROW_RETURN_NOT_OK(decoded.status());
  if (auto status = (*decoded)->ValidateFull(); !status.ok()) return Fail(status);
  *batch = std::move(decoded).ValueUnsafe();
  return arrow::Status::OK();
}

std::optional<std::string> ValidatedReader::failure() const {
  std::lock_guard lock(mutex_);
  return failure_;
}

std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::shared_ptr<arrow::io::InputStream> input) {
  return std::make_shared<ValidatedReader>(std::move(input));
}

std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::string bytes) {
  return OpenValidatedIpc(
      std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(bytes))));
}

}  // namespace warpsql::detail
