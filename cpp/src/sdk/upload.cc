#include "upload.hpp"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/message.h>

#include <cstring>
#include <atomic>
#include <bit>
#include <limits>
#include <stdexcept>
#include <string_view>

#include "chargewire/errors.hpp"

namespace chargewire::detail {

struct IngestMessageLimit {
  explicit IngestMessageLimit(std::size_t bytes) : bytes(static_cast<int64_t>(bytes)) {
    if (bytes == 0 || bytes > static_cast<std::size_t>(std::numeric_limits<int64_t>::max()))
      throw std::invalid_argument("max_ingest_message_bytes must be a positive signed 64-bit size");
  }
  arrow::Status Exceeded() {
    exceeded = true;
    return arrow::Status::CapacityError("Arrow IPC message exceeds max_ingest_message_bytes (", bytes, " bytes)");
  }
  const int64_t bytes;
  int64_t read = 0;  // one input reader, serialized by ValidatedReader::read_mutex_
  std::atomic<bool> exceeded = false;
};

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

// Arrow asks the input for a declared metadata/body size before validating that
// body. Refuse the request BEFORE QueueInputStream can allocate it. A MessageReader
// resets the budget once per message, so total upload length remains unlimited.
class LimitedInput : public arrow::io::InputStream {
 public:
  LimitedInput(std::shared_ptr<arrow::io::InputStream> input, std::shared_ptr<IngestMessageLimit> limit)
      : input_(std::move(input)), limit_(std::move(limit)) {}
  arrow::Status Check(int64_t bytes) {
    if (bytes < 0) return arrow::Status::Invalid("negative IPC read size");
    if (bytes > limit_->bytes - limit_->read) return limit_->Exceeded();
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> Read(int64_t bytes, void* out) override {
    ARROW_RETURN_NOT_OK(Check(bytes));
    ARROW_ASSIGN_OR_RAISE(auto got, input_->Read(bytes, out));
    limit_->read += got;
    return got;
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t bytes) override {
    ARROW_RETURN_NOT_OK(Check(bytes));
    ARROW_ASSIGN_OR_RAISE(auto buffer, input_->Read(bytes));
    limit_->read += buffer->size();
    return buffer;
  }
  arrow::Status Close() override { return input_->Close(); }
  bool closed() const override { return input_->closed(); }
  arrow::Result<int64_t> Tell() const override { return input_->Tell(); }
 private:
  std::shared_ptr<arrow::io::InputStream> input_;
  std::shared_ptr<IngestMessageLimit> limit_;
};

// Read only the standard FlatBuffer offsets needed to inspect compression sizes.
// Arrow has verified the metadata already; bounds are checked here independently.
class IpcMetadata {
 public:
  explicit IpcMetadata(const arrow::Buffer& bytes)
      : bytes_(reinterpret_cast<const char*>(bytes.data()), static_cast<std::size_t>(bytes.size())) {}
  template <typename T> T At(std::size_t at) const {
    static_assert(std::endian::native == std::endian::little);
    if (at > bytes_.size() || sizeof(T) > bytes_.size() - at)
      throw std::invalid_argument("truncated IPC compression metadata");
    T value;
    std::memcpy(&value, bytes_.data() + at, sizeof(T));
    return value;
  }
  std::size_t Indirect(std::size_t at) const {
    const auto distance = At<uint32_t>(at);
    if (at > bytes_.size() || distance > bytes_.size() - at)
      throw std::invalid_argument("invalid IPC metadata offset");
    return at + distance;
  }
  std::size_t Field(std::size_t table, std::size_t index) const {
    const int64_t vtable = static_cast<int64_t>(table) - At<int32_t>(table);
    if (vtable < 0 || static_cast<uint64_t>(vtable) > bytes_.size())
      throw std::invalid_argument("invalid IPC metadata table");
    const auto start = static_cast<std::size_t>(vtable);
    const auto size = At<uint16_t>(start);
    const auto entry = 4 + 2 * index;
    if (entry + 2 > size) return 0;
    const auto offset = At<uint16_t>(start + entry);
    if (offset > bytes_.size() - table) throw std::invalid_argument("invalid IPC metadata field");
    return offset == 0 ? 0 : table + offset;
  }
 private:
  std::string_view bytes_;
};

arrow::Status CheckExpanded(const arrow::ipc::Message& message, IngestMessageLimit& limit) {
  if (message.type() != arrow::ipc::MessageType::RECORD_BATCH &&
      message.type() != arrow::ipc::MessageType::DICTIONARY_BATCH) return arrow::Status::OK();
  try {
    IpcMetadata metadata(*message.metadata());
    const auto header_at = metadata.Field(metadata.Indirect(0), 2);
    if (!header_at) return arrow::Status::Invalid("IPC message has no header");
    auto batch = metadata.Indirect(header_at);
    if (message.type() == arrow::ipc::MessageType::DICTIONARY_BATCH) {
      const auto data = metadata.Field(batch, 1);
      if (!data) return arrow::Status::Invalid("IPC dictionary has no data");
      batch = metadata.Indirect(data);
    }
    if (!metadata.Field(batch, 3)) return arrow::Status::OK(); // no compression
    const auto buffers_at = metadata.Field(batch, 2);
    if (!buffers_at) return arrow::Status::Invalid("compressed IPC batch has no buffers");
    const auto buffers = metadata.Indirect(buffers_at);
    const auto count = metadata.At<uint32_t>(buffers);
    int64_t expanded = 0;
    const auto body = message.body();
    const int64_t body_size = body ? body->size() : 0;
    for (uint32_t i = 0; i < count; ++i) {
      const auto at = buffers + 4 + static_cast<std::size_t>(i) * 16;
      const auto offset = metadata.At<int64_t>(at);
      const auto length = metadata.At<int64_t>(at + 8);
      if (length == 0) continue;
      if (offset < 0 || length < 8 || offset > body_size || length > body_size - offset)
        return arrow::Status::Invalid("invalid compressed IPC buffer range");
      int64_t size;
      std::memcpy(&size, body->data() + offset, sizeof(size));
      if (size == -1) size = length - 8; // stored uncompressed
      if (size < 0) return arrow::Status::Invalid("negative IPC decompressed buffer size");
      if (size > limit.bytes - expanded) return limit.Exceeded();
      expanded += size;
    }
  } catch (const std::exception& error) {
    return arrow::Status::Invalid(error.what());
  }
  return arrow::Status::OK();
}

class LimitedMessages : public arrow::ipc::MessageReader {
 public:
  LimitedMessages(std::unique_ptr<arrow::ipc::MessageReader> input, std::shared_ptr<IngestMessageLimit> limit)
      : input_(std::move(input)), limit_(std::move(limit)) {}
  arrow::Result<std::unique_ptr<arrow::ipc::Message>> ReadNextMessage() override {
    limit_->read = 0;
    ARROW_ASSIGN_OR_RAISE(auto message, input_->ReadNextMessage());
    if (message) ARROW_RETURN_NOT_OK(CheckExpanded(*message, *limit_));
    return message;
  }
 private:
  std::unique_ptr<arrow::ipc::MessageReader> input_;
  std::shared_ptr<IngestMessageLimit> limit_;
};

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

ValidatedReader::ValidatedReader(std::shared_ptr<arrow::io::InputStream> input, std::size_t max_message_bytes)
    : limit_(std::make_shared<IngestMessageLimit>(max_message_bytes)),
      input_(std::make_shared<LimitedInput>(std::move(input), limit_)),
      messages_(std::make_unique<LimitedMessages>(arrow::ipc::MessageReader::Open(input_.get()), limit_)),
      dictionaries_(std::make_unique<arrow::ipc::DictionaryMemo>()) {
  auto not_ipc = [](const std::string& why) {
    return Error(ErrorCode::kBadRequest, "the body is not an Arrow IPC stream: " + why);
  };
  auto message = messages_->ReadNextMessage();
  if (!message.ok()) {
    if (limit_exceeded()) throw limit_error();
    throw not_ipc(message.status().message());
  }
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

bool ValidatedReader::limit_exceeded() const { return limit_->exceeded.load(); }
Error ValidatedReader::limit_error() const {
  return Error(ErrorCode::kPayloadTooLarge, "Arrow IPC message exceeds max_ingest_message_bytes (" +
               std::to_string(limit_->bytes) + " bytes, including expanded buffers)");
}

std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::shared_ptr<arrow::io::InputStream> input, std::size_t max_message_bytes) {
  return std::make_shared<ValidatedReader>(std::move(input), max_message_bytes);
}

std::shared_ptr<ValidatedReader> OpenValidatedIpc(std::string bytes, std::size_t max_message_bytes) {
  return OpenValidatedIpc(
      std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(bytes))), max_message_bytes);
}

}  // namespace chargewire::detail
