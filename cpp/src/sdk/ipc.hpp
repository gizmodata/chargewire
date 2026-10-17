// Arrow IPC stream encoding for response bodies (spec §2.2, §8.2).
#pragma once

#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <memory>
#include <string>
#include <string_view>

namespace warpsql::detail {

enum class IpcCompression { kNone, kZstd };

// From the Warp-IPC-Compression request header: "zstd" if the client accepts it.
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

// A schema as spec §2.2 embeds it in JSON: an IPC stream holding only the schema
// message and the end-of-stream marker, base64-encoded.
std::string SchemaBase64(const std::shared_ptr<arrow::Schema>& schema);

}  // namespace warpsql::detail
