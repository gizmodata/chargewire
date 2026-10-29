#include "ipc.hpp"

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>

#include <bit>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "ids.hpp"
#include "vendor/lz4frame.h"  // LZ4 1.10.0's; Arrow's bundled LZ4 provides the functions

namespace chargewire::detail {

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

// What a buffer's sample must lose for the buffer to be compressed at all (Packer).
//
// lz4_frame is the codec of fast links, where CPU, not bandwidth, limits a result: it
// compresses a buffer only when the sample loses more than half its bytes, which keeps
// the cheap wins (fixed-width columns with small values, repetitive strings) and skips
// buffers that cost as much to compress as sending them does (free text, keys spread over
// their range, dates). zstd is for scarce bandwidth and compresses whatever shrinks.
constexpr double kLz4MinSampleSavings = 0.55;
constexpr double kZstdMinSampleSavings = kMinSpaceSavings;

// Compresses Arrow body buffers, one at a time, with the codec spec §8.2 names. LZ4 frames
// are written with independent 4 MiB blocks (Arrow's own codec links 64 KiB blocks, which
// compresses about a quarter slower); zstd goes through Arrow's codec.
class Packer {
 public:
  explicit Packer(IpcCompression compression)
      : compression_(compression),
        min_sample_savings_(compression == IpcCompression::kZstd ? kZstdMinSampleSavings : kLz4MinSampleSavings) {
    if (compression == IpcCompression::kZstd) {
      auto codec = arrow::util::Codec::Create(arrow::Compression::ZSTD);
      Check(codec.status());
      zstd_ = std::move(codec).ValueUnsafe();
    }
    std::memset(&prefs_, 0, sizeof(prefs_));
    prefs_.frameInfo.blockSizeID = LZ4F_max4MB;
    prefs_.frameInfo.blockMode = LZ4F_blockIndependent;
  }

  arrow::Compression::type type() const {
    return compression_ == IpcCompression::kZstd ? arrow::Compression::ZSTD : arrow::Compression::LZ4_FRAME;
  }

  int64_t Bound(int64_t size) {
    if (zstd_) return zstd_->MaxCompressedLen(size, nullptr);
    return static_cast<int64_t>(LZ4F_compressFrameBound(static_cast<std::size_t>(size), &prefs_));
  }

  // Compresses `size` bytes into `out` (Bound(size) of room); returns the compressed size.
  int64_t CompressInto(const uint8_t* in, int64_t size, uint8_t* out, int64_t room) {
    if (zstd_) {
      auto n = zstd_->Compress(size, in, room, out);
      Check(n.status());
      return *n;
    }
    const std::size_t n = LZ4F_compressFrame(out, static_cast<std::size_t>(room), in,
                                             static_cast<std::size_t>(size), &prefs_);
    if (LZ4F_isError(n)) throw std::runtime_error(std::string("LZ4: ") + LZ4F_getErrorName(n));
    return static_cast<int64_t>(n);
  }

  // Whether a buffer is worth compressing: small ones always are; a large one when a
  // sample of it (the start, the middle, the end) shrinks enough.
  bool Worthwhile(const uint8_t* in, int64_t size) {
    if (size < kSampleFrom) return true;
    thread_local std::vector<uint8_t> sample;
    thread_local std::vector<uint8_t> packed;
    sample.resize(3 * kSampleSlice);
    for (int i = 0; i < 3; i++) {
      const int64_t at = i == 0 ? 0 : i == 1 ? (size - kSampleSlice) / 2 : size - kSampleSlice;
      std::memcpy(sample.data() + i * kSampleSlice, in + at, kSampleSlice);
    }
    const auto sample_size = static_cast<int64_t>(sample.size());
    packed.resize(static_cast<std::size_t>(Bound(sample_size)));
    const int64_t n = CompressInto(sample.data(), sample_size, packed.data(), static_cast<int64_t>(packed.size()));
    return 1.0 - static_cast<double>(n) / static_cast<double>(sample_size) >= min_sample_savings_;
  }

  // The buffer as spec §8.2 sends it compressed: its uncompressed length (little-endian
  // int64), then the compressed bytes. Null when it isn't worth compressing: the caller
  // sends it as it is.
  std::shared_ptr<arrow::Buffer> Pack(const arrow::Buffer& buffer) {
    const int64_t size = buffer.size();
    if (!Worthwhile(buffer.data(), size)) return nullptr;
    const int64_t room = Bound(size);
    auto out = arrow::AllocateResizableBuffer(room + 8);
    Check(out.status());
    std::unique_ptr<arrow::ResizableBuffer> packed = std::move(out).ValueUnsafe();
    const int64_t n = CompressInto(buffer.data(), size, packed->mutable_data() + 8, room);
    if (1.0 - static_cast<double>(n) / static_cast<double>(size) < kMinSpaceSavings) return nullptr;
    std::memcpy(packed->mutable_data(), &size, sizeof(size));
    Check(packed->Resize(n + 8, /*shrink_to_fit=*/true));
    return packed;
  }

 private:
  // Buffers smaller than this are compressed without a sample.
  static constexpr int64_t kSampleFrom = 256 * 1024;
  // The sample: this much from the start, the middle and the end of the buffer.
  static constexpr int64_t kSampleSlice = 16 * 1024;

  IpcCompression compression_;
  double min_sample_savings_;
  std::unique_ptr<arrow::util::Codec> zstd_;
  LZ4F_preferences_t prefs_;
};

// The Packer as an Arrow codec, for the streams Arrow's own writer encodes (dictionary
// results, metadata): a buffer not worth compressing is answered as not shrinking, so
// Arrow's min_space_savings sends it as it is.
class PackerCodec : public arrow::util::Codec {
 public:
  explicit PackerCodec(IpcCompression compression) : packer_(compression) {}

  arrow::Result<int64_t> Compress(int64_t input_len, const uint8_t* input, int64_t output_buffer_len,
                                  uint8_t* output_buffer) override {
    if (!packer_.Worthwhile(input, input_len)) return input_len;  // as large as the input: sent uncompressed
    try {
      return packer_.CompressInto(input, input_len, output_buffer, output_buffer_len);
    } catch (const std::exception& e) {
      return arrow::Status::IOError(e.what());
    }
  }
  arrow::Result<int64_t> Decompress(int64_t, const uint8_t*, int64_t, uint8_t*) override {
    return arrow::Status::NotImplemented("PackerCodec only compresses");
  }
  int64_t MaxCompressedLen(int64_t input_len, const uint8_t*) override { return packer_.Bound(input_len); }
  arrow::Result<std::shared_ptr<arrow::util::Compressor>> MakeCompressor() override {
    return arrow::Status::NotImplemented("PackerCodec has no streaming compressor");
  }
  arrow::Result<std::shared_ptr<arrow::util::Decompressor>> MakeDecompressor() override {
    return arrow::Status::NotImplemented("PackerCodec has no streaming decompressor");
  }
  arrow::Compression::type compression_type() const override { return packer_.type(); }
  int minimum_compression_level() const override { return 1; }
  int maximum_compression_level() const override { return 1; }
  int default_compression_level() const override { return 1; }

 private:
  Packer packer_;
};

arrow::ipc::IpcWriteOptions WriteOptions(IpcCompression compression) {
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  if (compression == IpcCompression::kNone) return options;
  options.codec = std::make_shared<PackerCodec>(compression);
  options.min_space_savings = kMinSpaceSavings;
  return options;
}

// A codec that pretends every buffer compresses to one byte, so Arrow writes record batch
// metadata of the compressed shape (the BodyCompression field, one entry per buffer)
// without compressing or copying anything. EncodeBatch then fills in the real lengths.
class ShapeCodec : public arrow::util::Codec {
 public:
  explicit ShapeCodec(arrow::Compression::type type) : type_(type) {}
  arrow::Result<int64_t> Compress(int64_t, const uint8_t*, int64_t, uint8_t* output_buffer) override {
    output_buffer[0] = 0;
    return 1;
  }
  arrow::Result<int64_t> Decompress(int64_t, const uint8_t*, int64_t, uint8_t*) override {
    return arrow::Status::NotImplemented("ShapeCodec");
  }
  int64_t MaxCompressedLen(int64_t, const uint8_t*) override { return 1; }
  arrow::Result<std::shared_ptr<arrow::util::Compressor>> MakeCompressor() override {
    return arrow::Status::NotImplemented("ShapeCodec");
  }
  arrow::Result<std::shared_ptr<arrow::util::Decompressor>> MakeDecompressor() override {
    return arrow::Status::NotImplemented("ShapeCodec");
  }
  arrow::Compression::type compression_type() const override { return type_; }
  int minimum_compression_level() const override { return 1; }
  int maximum_compression_level() const override { return 1; }
  int default_compression_level() const override { return 1; }

 private:
  arrow::Compression::type type_;
};

// Flatbuffer reading, just enough to find the fields EncodeBatch rewrites in a record
// batch Message (Message.fbs): Message.bodyLength and RecordBatch.buffers.
uint32_t U32(const std::string& b, std::size_t at) {
  if (at + 4 > b.size()) throw std::runtime_error("Arrow IPC: truncated metadata");
  uint32_t v;
  std::memcpy(&v, b.data() + at, 4);
  return v;
}

// Where field `index` of the table at `table` is stored, or 0 when it is absent.
std::size_t FieldAt(const std::string& b, std::size_t table, int index) {
  const std::size_t vtable = table - static_cast<std::size_t>(static_cast<int32_t>(U32(b, table)));
  uint16_t vtable_size, offset = 0;
  std::memcpy(&vtable_size, b.data() + vtable, 2);
  const std::size_t entry = 4 + 2 * static_cast<std::size_t>(index);
  if (entry + 2 <= vtable_size) std::memcpy(&offset, b.data() + vtable + entry, 2);
  return offset == 0 ? 0 : table + offset;
}

// Writes the body layout into record batch metadata of the compressed shape.
void SetBodyLayout(std::string& meta, const std::vector<std::pair<int64_t, int64_t>>& buffers, int64_t body_length) {
  static_assert(std::endian::native == std::endian::little, "Arrow IPC metadata is little-endian");
  const std::size_t root = U32(meta, 0);
  const std::size_t header = FieldAt(meta, root, 2);
  const std::size_t body_length_at = FieldAt(meta, root, 3);
  if (header == 0) throw std::runtime_error("Arrow IPC: a record batch message without its header");
  const std::size_t batch = header + U32(meta, header);
  const std::size_t buffers_at = FieldAt(meta, batch, 2);
  if (buffers_at == 0) throw std::runtime_error("Arrow IPC: a record batch without buffers");
  const std::size_t vector = buffers_at + U32(meta, buffers_at);
  if (U32(meta, vector) != buffers.size() || vector + 4 + 16 * buffers.size() > meta.size()) {
    throw std::runtime_error("Arrow IPC: the record batch's buffers don't match its payload");
  }
  for (std::size_t i = 0; i < buffers.size(); i++) {
    std::memcpy(meta.data() + vector + 4 + 16 * i, &buffers[i].first, 8);
    std::memcpy(meta.data() + vector + 12 + 16 * i, &buffers[i].second, 8);
  }
  if (body_length_at != 0) {
    std::memcpy(meta.data() + body_length_at, &body_length, 8);
  } else if (body_length != 0) {
    throw std::runtime_error("Arrow IPC: no bodyLength field to set");
  }
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
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.use_threads = false;  // batches are encoded in parallel instead
  arrow::ipc::IpcPayload payload;  // its body references the batch's buffers
  Check(arrow::ipc::GetRecordBatchPayload(batch, options, &payload));
  if (compression != IpcCompression::kNone) {
    // Compress the buffers worth it; send the others as they are, by reference, behind
    // the -1 length that says so (spec §8.2), rather than through Arrow's writer, which
    // copies every buffer it doesn't compress.
    static const auto kSentAsIs = [] {
      constexpr int64_t kUncompressed = -1;
      return arrow::Buffer::FromString(std::string(reinterpret_cast<const char*>(&kUncompressed), 8));
    }();
    Packer packer(compression);
    std::vector<std::shared_ptr<arrow::Buffer>> body;
    std::vector<std::pair<int64_t, int64_t>> layout;
    int64_t offset = 0;
    for (const auto& buffer : payload.body_buffers) {
      const int64_t size = buffer ? buffer->size() : 0;
      int64_t length = 0;
      if (size > 0) {
        if (auto packed = packer.Pack(*buffer)) {
          length = packed->size();
          body.push_back(std::move(packed));
        } else {
          length = 8 + size;
          body.push_back(kSentAsIs);
          body.push_back(buffer);
        }
      }
      layout.emplace_back(offset, length);
      offset += (length + 7) & ~int64_t{7};
    }
    auto shape_options = options;
    shape_options.codec = std::make_shared<ShapeCodec>(packer.type());
    arrow::ipc::IpcPayload shape;
    Check(arrow::ipc::GetRecordBatchPayload(batch, shape_options, &shape));
    if (shape.body_buffers.size() != layout.size()) {
      throw std::runtime_error("Arrow IPC: compressed and plain payloads have different buffers");
    }
    std::string meta = shape.metadata->ToString();
    SetBodyLayout(meta, layout, offset);
    payload.metadata = arrow::Buffer::FromString(std::move(meta));
    payload.body_buffers = std::move(body);
    payload.body_length = offset;
  }
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

}  // namespace chargewire::detail
