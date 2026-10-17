#include "ipc.hpp"

#include <nanoarrow/nanoarrow_ipc.hpp>

#include <string>

#include "duckdb_v2.hpp"  // for Error

namespace warp {

namespace {

void Check(ArrowErrorCode code, const ArrowError& error, std::string_view what) {
  if (code != NANOARROW_OK) {
    throw Error(std::string(what) + ": " + error.message);
  }
}

ArrowIpcCompressionType ToNanoarrow(Compression compression) {
  switch (compression) {
    case Compression::kNone:
      return NANOARROW_IPC_COMPRESSION_TYPE_NONE;
    case Compression::kZstd:
      return NANOARROW_IPC_COMPRESSION_TYPE_ZSTD;
    case Compression::kLz4:
      return NANOARROW_IPC_COMPRESSION_TYPE_LZ4_FRAME;
  }
  return NANOARROW_IPC_COMPRESSION_TYPE_NONE;
}

}  // namespace

Compression ParseCompression(std::string_view name) {
  if (name == "none") return Compression::kNone;
  if (name == "zstd") return Compression::kZstd;
  if (name == "lz4") return Compression::kLz4;
  throw Error("unknown compression '" + std::string(name) + "' (none, zstd, lz4)");
}

void WriteIpcStream(ArrowArrayStream* rows, std::FILE* out, Compression compression) {
  ArrowError error{};
  nanoarrow::ipc::UniqueOutputStream output;
  Check(ArrowIpcOutputStreamInitFile(output.get(), out, /*close_on_release=*/0), error,
        "open output");
  nanoarrow::ipc::UniqueWriter writer;
  Check(ArrowIpcWriterInit(writer.get(), output.get()), error, "start IPC writer");
  // Before the schema, so the schema message declares compressed bodies.
  Check(ArrowIpcWriterSetCompression(writer.get(), ToNanoarrow(compression),
                                     NANOARROW_IPC_COMPRESSION_LEVEL_DEFAULT, &error),
        error, "set compression");
  Check(ArrowIpcWriterWriteArrayStream(writer.get(), rows, &error), error, "write IPC stream");
}

nanoarrow::UniqueArrayStream ReadIpcStream(std::FILE* in) {
  nanoarrow::ipc::UniqueInputStream input;
  ArrowError error{};
  Check(ArrowIpcInputStreamInitFile(input.get(), in, /*close_on_release=*/0), error,
        "open input");
  nanoarrow::UniqueArrayStream rows;
  Check(ArrowIpcArrayStreamReaderInit(rows.get(), input.get(), nullptr), error,
        "start IPC reader");
  return rows;
}

}  // namespace warp
