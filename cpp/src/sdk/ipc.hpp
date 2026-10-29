// Arrow IPC stream encoding for response bodies (spec §2.2, §8.2).
#pragma once

#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <boost/asio/buffer.hpp>

#include <memory>
#include <string>
#include <string_view>

namespace chargewire::detail {

// Arrow IPC body compression (spec §8.2).
enum class IpcCompression { kNone, kZstd, kLz4Frame };

// From the Chargewire-IPC-Compression request header: the codecs the client accepts, in its
// order of preference ("lz4_frame, zstd"). The first one this server supports wins;
// `none`, an empty header, or only unknown names: no compression.
IpcCompression ParseIpcCompression(std::string_view header);
std::string_view IpcCompressionName(IpcCompression compression);

// Encodes one IPC stream incrementally: each call returns the bytes to send next.
class IpcStreamEncoder {
 public:
  IpcStreamEncoder(std::shared_ptr<arrow::Schema> schema, IpcCompression compression);
  ~IpcStreamEncoder();

  // The schema message, so a reader learns the columns before the first batch exists
  // (a long aggregate's stream isn't silent until it ends). Write and Finish include it
  // when Begin wasn't called.
  std::string Begin();
  // One record batch (and any dictionary batches it needs).
  std::string Write(const arrow::RecordBatch& batch);
  // The end-of-stream marker.
  std::string Finish();

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// One record batch as its IPC message, encoded on its own: the message's framing and
// metadata, then the batch's buffers by reference (no copy) or, compressed, the new
// buffers. Batches of a schema without dictionaries are independent of each other, so
// several can be encoded (and compressed) on different threads at once; written in
// order after the schema message, they make the same stream IpcStreamEncoder writes.
class EncodedBatch {
 public:
  EncodedBatch() = default;
  EncodedBatch(EncodedBatch&&) = default;
  EncodedBatch& operator=(EncodedBatch&&) = default;

  // The bytes to send, in order. Valid while this object lives (moving it keeps them).
  const std::vector<boost::asio::const_buffer>& pieces() const { return pieces_; }
  std::size_t size() const { return size_; }

 private:
  friend EncodedBatch EncodeBatch(const arrow::RecordBatch& batch, IpcCompression compression);
  std::vector<std::unique_ptr<std::string>> owned_;
  std::vector<std::shared_ptr<arrow::Buffer>> referenced_;
  std::vector<boost::asio::const_buffer> pieces_;
  std::size_t size_ = 0;
};

// Whether EncodeBatch can encode batches of `schema` (no dictionary-encoded field at any
// depth: dictionaries need the stream's state).
bool CanEncodeIndependently(const arrow::Schema& schema);
EncodedBatch EncodeBatch(const arrow::RecordBatch& batch, IpcCompression compression);
// The end-of-stream marker.
std::string_view EndOfStreamMarker();

// A schema as spec §2.2 embeds it in JSON: an IPC stream holding only the schema
// message and the end-of-stream marker, base64-encoded.
std::string SchemaBase64(const std::shared_ptr<arrow::Schema>& schema);

}  // namespace chargewire::detail
