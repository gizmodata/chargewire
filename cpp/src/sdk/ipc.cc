#include "ipc.hpp"

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>

#include <stdexcept>

#include "ids.hpp"

namespace warpsql::detail {

namespace {

// An output stream that collects what the IPC writer emits, so each piece can be sent
// as its own HTTP chunk.
class CaptureStream : public arrow::io::OutputStream {
 public:
  std::string Take() { return std::exchange(bytes_, {}); }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    bytes_.append(static_cast<const char*>(data), static_cast<std::size_t>(nbytes));
    position_ += nbytes;
    return arrow::Status::OK();
  }
  arrow::Result<int64_t> Tell() const override { return position_; }
  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }

 private:
  std::string bytes_;
  int64_t position_ = 0;
  bool closed_ = false;
};

void Check(const arrow::Status& status) {
  if (!status.ok()) {
    throw std::runtime_error("Arrow IPC: " + status.ToString());
  }
}

// Forwards everything but the schema message, which the encoder sends itself, up front.
class WithoutSchema : public arrow::ipc::internal::IpcPayloadWriter {
 public:
  explicit WithoutSchema(std::unique_ptr<arrow::ipc::internal::IpcPayloadWriter> inner)
      : inner_(std::move(inner)) {}
  arrow::Status Start() override { return inner_->Start(); }
  arrow::Status WritePayload(const arrow::ipc::IpcPayload& payload) override {
    if (payload.type == arrow::ipc::MessageType::SCHEMA) {
      return arrow::Status::OK();
    }
    return inner_->WritePayload(payload);
  }
  arrow::Status Close() override { return inner_->Close(); }

 private:
  std::unique_ptr<arrow::ipc::internal::IpcPayloadWriter> inner_;
};

}  // namespace

IpcCompression ParseIpcCompression(std::string_view header) {
  return header == "zstd" ? IpcCompression::kZstd : IpcCompression::kNone;
}

std::string_view IpcCompressionName(IpcCompression compression) {
  return compression == IpcCompression::kZstd ? "zstd" : "none";
}

class IpcStreamEncoder::Impl {
 public:
  Impl(std::shared_ptr<arrow::Schema> schema, IpcCompression compression)
      : schema_(std::move(schema)), sink_(std::make_shared<CaptureStream>()) {
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    if (compression == IpcCompression::kZstd) {
      auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD);
      Check(codec.status());
      options.codec = std::move(codec).ValueUnsafe();
    }
    auto schema_message = arrow::ipc::SerializeSchema(*schema_);
    Check(schema_message.status());
    schema_message_ = (*schema_message)->ToString();
    auto payloads = arrow::ipc::internal::MakePayloadStreamWriter(sink_.get(), options);
    Check(payloads.status());
    auto writer = arrow::ipc::internal::OpenRecordBatchWriter(
        std::make_unique<WithoutSchema>(std::move(payloads).ValueUnsafe()), schema_, options);
    Check(writer.status());
    writer_ = std::move(writer).ValueUnsafe();
  }

  std::string Begin() { return std::exchange(schema_message_, {}); }

  std::string Write(const arrow::RecordBatch& batch) {
    Check(writer_->WriteRecordBatch(batch));
    return Begin() + sink_->Take();
  }

  std::string Finish() {
    Check(writer_->Close());
    return Begin() + sink_->Take();
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<CaptureStream> sink_;
  std::string schema_message_;  // until sent
  std::unique_ptr<arrow::ipc::RecordBatchWriter> writer_;
};

IpcStreamEncoder::IpcStreamEncoder(std::shared_ptr<arrow::Schema> schema,
                                   IpcCompression compression)
    : impl_(std::make_unique<Impl>(std::move(schema), compression)) {}

IpcStreamEncoder::~IpcStreamEncoder() = default;

std::string IpcStreamEncoder::Begin() { return impl_->Begin(); }

std::string IpcStreamEncoder::Write(const arrow::RecordBatch& batch) {
  return impl_->Write(batch);
}

std::string IpcStreamEncoder::Finish() { return impl_->Finish(); }

std::string SchemaBase64(const std::shared_ptr<arrow::Schema>& schema) {
  auto sink = std::make_shared<CaptureStream>();
  auto writer = arrow::ipc::MakeStreamWriter(sink, schema);
  Check(writer.status());
  Check((*writer)->Close());
  return Base64(sink->Take());
}

}  // namespace warpsql::detail
