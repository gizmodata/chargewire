#include "ipc.hpp"

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>

#include <cstring>
#include <stdexcept>
#include <vector>

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

// Collects what WriteIpcPayload emits: small writes are copied into owned pieces, large
// buffers are kept by reference.
class GatherStream : public arrow::io::OutputStream {
 public:
  explicit GatherStream(std::vector<std::unique_ptr<std::string>>& owned,
                        std::vector<std::shared_ptr<arrow::Buffer>>& referenced,
                        std::vector<boost::asio::const_buffer>& pieces)
      : owned_(owned), referenced_(referenced), pieces_(pieces) {}

  arrow::Status Write(const void* data, int64_t nbytes) override {
    if (nbytes <= 0) return arrow::Status::OK();
    if (!open_) {
      owned_.push_back(std::make_unique<std::string>());
      open_ = owned_.back().get();
      pieces_.emplace_back();  // filled in when the piece is closed
    }
    open_->append(static_cast<const char*>(data), static_cast<std::size_t>(nbytes));
    pieces_.back() = boost::asio::buffer(*open_);
    position_ += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override {
    if (data->size() < kReferenceAtLeast) {
      return Write(data->data(), data->size());
    }
    open_ = nullptr;
    referenced_.push_back(data);
    pieces_.push_back(boost::asio::buffer(data->data(), static_cast<std::size_t>(data->size())));
    position_ += data->size();
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override { return position_; }
  arrow::Status Close() override {
    closed_ = true;
    return arrow::Status::OK();
  }
  bool closed() const override { return closed_; }

 private:
  // Smaller buffers are cheaper to copy than to send as a piece of their own.
  static constexpr int64_t kReferenceAtLeast = 4096;

  std::vector<std::unique_ptr<std::string>>& owned_;
  std::vector<std::shared_ptr<arrow::Buffer>>& referenced_;
  std::vector<boost::asio::const_buffer>& pieces_;
  std::string* open_ = nullptr;
  int64_t position_ = 0;
  bool closed_ = false;
};

// A buffer that compression would shrink by less than this is sent as it is, so the
// reader doesn't spend time decompressing it (random doubles, already-compact data).
constexpr double kMinSpaceSavings = 0.1;

// Compresses only the buffers worth it. A large buffer is judged by a sample first: one
// whose sample shrinks by less than `min_sample_savings` is not compressed at all (the
// codec answers that it didn't shrink, so Arrow's min_space_savings sends it as it is),
// which spares the CPU a whole compression would cost.
//
// lz4_frame is the codec of fast links, where CPU, not bandwidth, limits a result: it
// compresses a buffer only when the sample loses more than half its bytes, which keeps
// the cheap wins (fixed-width columns with small values, repetitive strings) and skips
// buffers that cost as much to compress as sending them does (free text, keys spread over
// their range, dates). zstd is for scarce bandwidth and compresses whatever shrinks.
class SelectiveCodec : public arrow::util::Codec {
 public:
  SelectiveCodec(std::unique_ptr<arrow::util::Codec> inner, double min_sample_savings)
      : inner_(std::move(inner)), min_sample_savings_(min_sample_savings) {}

  arrow::Result<int64_t> Compress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len,
                                  uint8_t* output_buffer) override {
    if (input_len >= kSampleFrom && !Worthwhile(input_len, input)) {
      return input_len;  // as large as the input: sent uncompressed
    }
    return inner_->Compress(input_len, input, output_buffer_len, output_buffer);
  }
  arrow::Result<int64_t> Decompress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len,
                                    uint8_t* output_buffer) override {
    return inner_->Decompress(input_len, input, output_buffer_len, output_buffer);
  }
  int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input) override {
    return inner_->MaxCompressedLen(input_len, input);
  }
  arrow::Result<std::shared_ptr<arrow::util::Compressor>> MakeCompressor() override {
    return inner_->MakeCompressor();
  }
  arrow::Result<std::shared_ptr<arrow::util::Decompressor>> MakeDecompressor() override {
    return inner_->MakeDecompressor();
  }
  arrow::Compression::type compression_type() const override { return inner_->compression_type(); }
  int minimum_compression_level() const override { return inner_->minimum_compression_level(); }
  int maximum_compression_level() const override { return inner_->maximum_compression_level(); }
  int default_compression_level() const override { return inner_->default_compression_level(); }
  int compression_level() const override { return inner_->compression_level(); }

 private:
  // Buffers smaller than this are compressed without a sample.
  static constexpr int64_t kSampleFrom = 256 * 1024;
  // The sample: this much from the start, the middle and the end of the buffer.
  static constexpr int64_t kSampleSlice = 16 * 1024;

  bool Worthwhile(int64_t input_len, const uint8_t* input) {
    thread_local std::vector<uint8_t> sample;
    thread_local std::vector<uint8_t> packed;
    sample.resize(3 * kSampleSlice);
    for (int i = 0; i < 3; i++) {
      const int64_t at = i == 0 ? 0 : i == 1 ? (input_len - kSampleSlice) / 2 : input_len - kSampleSlice;
      std::memcpy(sample.data() + i * kSampleSlice, input + at, kSampleSlice);
    }
    const int64_t bound = inner_->MaxCompressedLen(static_cast<int64_t>(sample.size()), sample.data());
    packed.resize(static_cast<std::size_t>(bound));
    auto size = inner_->Compress(static_cast<int64_t>(sample.size()), sample.data(), bound, packed.data());
    if (!size.ok()) return true;  // let the real compression report it
    return 1.0 - static_cast<double>(*size) / static_cast<double>(sample.size()) >= min_sample_savings_;
  }

  std::unique_ptr<arrow::util::Codec> inner_;
  double min_sample_savings_;
};

// What a sample must lose for its buffer to be compressed (see SelectiveCodec).
constexpr double kLz4MinSampleSavings = 0.55;
constexpr double kZstdMinSampleSavings = kMinSpaceSavings;

arrow::ipc::IpcWriteOptions WriteOptions(IpcCompression compression) {
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  if (compression == IpcCompression::kNone) return options;
  const bool zstd = compression == IpcCompression::kZstd;
  auto codec = arrow::util::Codec::Create(zstd ? arrow::Compression::ZSTD : arrow::Compression::LZ4_FRAME);
  Check(codec.status());
  options.codec = std::make_shared<SelectiveCodec>(std::move(codec).ValueUnsafe(),
                                                   zstd ? kZstdMinSampleSavings : kLz4MinSampleSavings);
  options.min_space_savings = kMinSpaceSavings;
  return options;
}

bool HasDictionary(const arrow::DataType& type) {
  if (type.id() == arrow::Type::DICTIONARY) return true;
  for (const auto& child : type.fields()) {
    if (HasDictionary(*child->type())) return true;
  }
  return false;
}

}  // namespace

bool CanEncodeIndependently(const arrow::Schema& schema) {
  for (const auto& field : schema.fields()) {
    if (HasDictionary(*field->type())) return false;
  }
  return true;
}

EncodedBatch EncodeBatch(const arrow::RecordBatch& batch, IpcCompression compression) {
  auto options = WriteOptions(compression);
  options.use_threads = false;  // batches are encoded in parallel instead
  arrow::ipc::IpcPayload payload;
  Check(arrow::ipc::GetRecordBatchPayload(batch, options, &payload));
  EncodedBatch out;
  GatherStream sink(out.owned_, out.referenced_, out.pieces_);
  int32_t metadata_length = 0;
  Check(arrow::ipc::WriteIpcPayload(payload, options, &sink, &metadata_length));
  for (const auto& piece : out.pieces_) out.size_ += piece.size();
  return out;
}

std::string_view EndOfStreamMarker() {
  static constexpr char kMarker[8] = {'\xff', '\xff', '\xff', '\xff', 0, 0, 0, 0};
  return {kMarker, sizeof(kMarker)};
}

IpcCompression ParseIpcCompression(std::string_view header) {
  while (!header.empty()) {
    const auto comma = header.find(',');
    std::string_view name = header.substr(0, comma);
    header = comma == std::string_view::npos ? std::string_view() : header.substr(comma + 1);
    while (!name.empty() && (name.front() == ' ' || name.front() == '\t')) name.remove_prefix(1);
    while (!name.empty() && (name.back() == ' ' || name.back() == '\t')) name.remove_suffix(1);
    if (name == "lz4_frame") return IpcCompression::kLz4Frame;
    if (name == "zstd") return IpcCompression::kZstd;
    if (name == "none") return IpcCompression::kNone;
    // A codec this server doesn't know: try the client's next choice.
  }
  return IpcCompression::kNone;
}

std::string_view IpcCompressionName(IpcCompression compression) {
  switch (compression) {
    case IpcCompression::kZstd: return "zstd";
    case IpcCompression::kLz4Frame: return "lz4_frame";
    case IpcCompression::kNone: break;
  }
  return "none";
}

class IpcStreamEncoder::Impl {
 public:
  Impl(std::shared_ptr<arrow::Schema> schema, IpcCompression compression)
      : schema_(std::move(schema)), sink_(std::make_shared<CaptureStream>()) {
    auto options = WriteOptions(compression);
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
