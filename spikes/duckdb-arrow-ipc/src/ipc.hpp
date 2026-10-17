// Arrow IPC streams with nanoarrow: the only Arrow library a Warp SQL server needs.
#pragma once

#include <nanoarrow/nanoarrow.hpp>

#include <cstdio>
#include <string_view>

namespace warp {

enum class Compression { kNone, kZstd, kLz4 };

// "none", "zstd" or "lz4"; throws Error otherwise.
Compression ParseCompression(std::string_view name);

// Writes all of `rows` (schema, batches, end-of-stream marker) to `out`.
void WriteIpcStream(ArrowArrayStream* rows, std::FILE* out, Compression compression);

// Reads an IPC stream from `in`, which must stay open while the stream is read.
// Compressed bodies (ZSTD, LZ4) are decompressed transparently.
nanoarrow::UniqueArrayStream ReadIpcStream(std::FILE* in);

}  // namespace warp
