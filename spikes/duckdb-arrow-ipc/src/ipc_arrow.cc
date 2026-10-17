// The same IPC interface as ipc.cc, implemented with Arrow C++ instead of
// nanoarrow, for comparison. Arrow C++ is built with IPC only (no Flight).
#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/util/compression.h>
#include <unistd.h>

#include <string>

#include "duckdb_v2.hpp"  // for Error
#include "ipc.hpp"

namespace warp {

namespace {

void Check(const arrow::Status& status, std::string_view what) {
  if (!status.ok()) {
    throw Error(std::string(what) + ": " + status.ToString());
  }
}

template <typename T>
T Value(arrow::Result<T> result, std::string_view what) {
  Check(result.status(), what);
  return std::move(result).ValueUnsafe();
}

// Passes batches through after checking them completely (offsets, UTF-8, decimal
// precision, …), so malformed uploads never reach the engine.
class ValidatingReader : public arrow::RecordBatchReader {
 public:
  explicit ValidatingReader(std::shared_ptr<arrow::RecordBatchReader> source)
      : source_(std::move(source)) {}

  std::shared_ptr<arrow::Schema> schema() const override { return source_->schema(); }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    ARROW_RETURN_NOT_OK(source_->ReadNext(batch));
    if (*batch != nullptr) {
      ARROW_RETURN_NOT_OK((*batch)->ValidateFull());
    }
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<arrow::RecordBatchReader> source_;
};

}  // namespace

Compression ParseCompression(std::string_view name) {
  if (name == "none") return Compression::kNone;
  if (name == "zstd") return Compression::kZstd;
  if (name == "lz4") return Compression::kLz4;
  throw Error("unknown compression '" + std::string(name) + "' (none, zstd, lz4)");
}

void WriteIpcStream(ArrowArrayStream* rows, std::FILE* out, Compression compression) {
  auto reader = Value(arrow::ImportRecordBatchReader(rows), "import result stream");
  std::fflush(out);
  auto sink = Value(arrow::io::FileOutputStream::Open(dup(fileno(out))), "open output");

  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  if (compression != Compression::kNone) {
    const auto type = compression == Compression::kZstd ? arrow::Compression::ZSTD
                                                        : arrow::Compression::LZ4_FRAME;
    options.codec = Value(arrow::util::Codec::Create(type), "create codec");
  }
  auto writer =
      Value(arrow::ipc::MakeStreamWriter(sink, reader->schema(), options), "start IPC writer");
  for (;;) {
    auto batch = Value(reader->Next(), "read result");
    if (batch == nullptr) {
      break;
    }
    Check(writer->WriteRecordBatch(*batch), "write IPC stream");
  }
  Check(writer->Close(), "finish IPC stream");
  Check(sink->Close(), "close output");
}

nanoarrow::UniqueArrayStream ReadIpcStream(std::FILE* in) {
  auto file = Value(arrow::io::ReadableFile::Open(dup(fileno(in))), "open input");
  auto reader = Value(arrow::ipc::RecordBatchStreamReader::Open(file), "start IPC reader");
  nanoarrow::UniqueArrayStream rows;
  Check(arrow::ExportRecordBatchReader(std::make_shared<ValidatingReader>(reader), rows.get()),
        "export stream");
  return rows;
}

}  // namespace warp
