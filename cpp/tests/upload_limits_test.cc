#include <gtest/gtest.h>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>

#include <cstring>

#include "upload.hpp"

namespace chargewire::detail {
namespace {

template <typename T> T Value(arrow::Result<T> value) {
  if (!value.ok()) throw std::runtime_error(value.status().ToString());
  return std::move(value).ValueUnsafe();
}

std::shared_ptr<arrow::RecordBatch> Zeros(int rows, int columns = 1) {
  arrow::Int64Builder builder;
  for (int i = 0; i < rows; ++i) EXPECT_TRUE(builder.Append(0).ok());
  auto array = Value(builder.Finish());
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (int i = 0; i < columns; ++i) {
    fields.push_back(arrow::field("v" + std::to_string(i), arrow::int64()));
    arrays.push_back(array);
  }
  return arrow::RecordBatch::Make(arrow::schema(fields), rows, arrays);
}

std::string Stream(const std::shared_ptr<arrow::RecordBatch>& batch, bool compressed = false, int count = 1) {
  auto sink = Value(arrow::io::BufferOutputStream::Create());
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.use_threads = false;
  if (compressed) options.codec = Value(arrow::util::Codec::Create(arrow::Compression::LZ4_FRAME));
  auto writer = Value(arrow::ipc::MakeStreamWriter(sink, batch->schema(), options));
  for (int i = 0; i < count; ++i) EXPECT_TRUE(writer->WriteRecordBatch(*batch).ok());
  EXPECT_TRUE(writer->Close().ok());
  return Value(sink->Finish())->ToString();
}

class ReadProbe : public arrow::io::InputStream {
 public:
  explicit ReadProbe(std::string bytes) : source_(arrow::Buffer::FromString(std::move(bytes))) {}
  arrow::Result<int64_t> Read(int64_t size, void* out) override {
    largest = std::max(largest, size);
    if (size > 4096) return arrow::Status::IOError("oversized request reached input");
    return source_.Read(size, out);
  }
  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t size) override {
    largest = std::max(largest, size);
    if (size > 4096) return arrow::Status::IOError("oversized request reached allocating input");
    return source_.Read(size);
  }
  arrow::Status Close() override { return source_.Close(); }
  bool closed() const override { return source_.closed(); }
  arrow::Result<int64_t> Tell() const override { return source_.Tell(); }
  int64_t largest = 0;
 private:
  arrow::io::BufferReader source_;
};

TEST(UploadLimits, RejectsDeclaredMetadataBeforeRequestingAnAllocation) {
  int32_t prefix[] = {-1, 0x7fffffff};
  auto input = std::make_shared<ReadProbe>(std::string(reinterpret_cast<char*>(prefix), sizeof(prefix)));
  try {
    OpenValidatedIpc(input, 4096);
    FAIL() << "oversized metadata was accepted";
  } catch (const Error& error) {
    EXPECT_EQ(error.code(), ErrorCode::kPayloadTooLarge);
  }
  EXPECT_LE(input->largest, 4096);
}

TEST(UploadLimits, RejectsDeclaredBodyBeforeRequestingAnAllocation) {
  auto bytes = Stream(Zeros(1));
  auto u32 = [&](std::size_t at) { uint32_t value; std::memcpy(&value, bytes.data() + at, 4); return value; };
  const std::size_t frame = 8 + u32(4); // after the schema (which has no body)
  const std::size_t metadata = frame + 8;
  const std::size_t table = metadata + u32(metadata);
  const auto vtable = table - static_cast<std::size_t>(static_cast<int32_t>(u32(table)));
  uint16_t offset;
  std::memcpy(&offset, bytes.data() + vtable + 4 + 2 * 3, 2); // Message.bodyLength
  ASSERT_NE(offset, 0);
  const int64_t huge = int64_t{1} << 40;
  std::memcpy(bytes.data() + table + offset, &huge, 8);
  auto input = std::make_shared<ReadProbe>(std::move(bytes));
  auto reader = OpenValidatedIpc(input, 4096);
  std::shared_ptr<arrow::RecordBatch> batch;
  int64_t index;
  EXPECT_FALSE(reader->NextIndexed(&batch, &index).ok());
  EXPECT_TRUE(reader->limit_exceeded());
  EXPECT_EQ(reader->limit_error().code(), ErrorCode::kPayloadTooLarge);
  EXPECT_LE(input->largest, 4096);
}

TEST(UploadLimits, CapsTheSumOfExpandedBuffersBeforeDecompression) {
  // Each of the two value buffers is individually below 4096; their sum exceeds it.
  auto bytes = Stream(Zeros(300, 2), true);
  ASSERT_LT(bytes.size(), 4096);
  auto reader = OpenValidatedIpc(std::move(bytes), 4096);
  std::shared_ptr<arrow::RecordBatch> batch;
  int64_t index;
  EXPECT_FALSE(reader->NextIndexed(&batch, &index).ok());
  EXPECT_TRUE(reader->limit_exceeded());
}

TEST(UploadLimits, RejectsForgedExpandedLengthBeforeInvokingTheCodec) {
  auto bytes = Stream(Zeros(64), true);
  auto u32 = [&](std::size_t at) { uint32_t value; std::memcpy(&value, bytes.data() + at, 4); return value; };
  const std::size_t frame = 8 + u32(4);
  const std::size_t body = frame + 8 + u32(frame + 4);
  int64_t expanded;
  std::memcpy(&expanded, bytes.data() + body, 8);
  ASSERT_EQ(expanded, 64 * 8); // first nonempty buffer is the compressed int64 values
  const int64_t huge = int64_t{1} << 40;
  std::memcpy(bytes.data() + body, &huge, 8);
  auto reader = OpenValidatedIpc(std::move(bytes), 4096);
  std::shared_ptr<arrow::RecordBatch> batch;
  int64_t index;
  EXPECT_FALSE(reader->NextIndexed(&batch, &index).ok());
  EXPECT_TRUE(reader->limit_exceeded()); // codec errors would not set the quota flag
}

TEST(UploadLimits, AcceptsExpandedBuffersExactlyAtTheLimit) {
  auto reader = OpenValidatedIpc(Stream(Zeros(256, 2), true), 4096);
  std::shared_ptr<arrow::RecordBatch> batch;
  int64_t index;
  ASSERT_TRUE(reader->NextIndexed(&batch, &index).ok());
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(batch->num_rows(), 256);
  EXPECT_FALSE(reader->limit_exceeded());
}

TEST(UploadLimits, DictionaryMessagesReceiveTheSameExpandedSizeCheck) {
  arrow::StringBuilder strings;
  ASSERT_TRUE(strings.Append(std::string(10000, 'x')).ok());
  auto dictionary = Value(strings.Finish());
  arrow::Int8Builder indices;
  ASSERT_TRUE(indices.Append(0).ok());
  auto array = Value(arrow::DictionaryArray::FromArrays(Value(indices.Finish()), dictionary));
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("v", array->type())}), 1, {array});
  auto bytes = Stream(batch, true);
  ASSERT_LT(bytes.size(), 4096);
  auto reader = OpenValidatedIpc(std::move(bytes), 4096);
  int64_t index;
  EXPECT_FALSE(reader->NextIndexed(&batch, &index).ok());
  EXPECT_TRUE(reader->limit_exceeded());
}

TEST(UploadLimits, TheLimitIsPerMessageNotPerUpload) {
  auto bytes = Stream(Zeros(64), false, 100);
  ASSERT_GT(bytes.size(), 4096);
  auto reader = OpenValidatedIpc(std::move(bytes), 4096);
  int count = 0;
  for (;;) {
    std::shared_ptr<arrow::RecordBatch> batch;
    int64_t index;
    ASSERT_TRUE(reader->NextIndexed(&batch, &index).ok());
    if (!batch) break;
    ++count;
  }
  EXPECT_EQ(count, 100);
}

}  // namespace
}  // namespace chargewire::detail
