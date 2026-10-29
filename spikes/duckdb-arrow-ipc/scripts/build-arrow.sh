#!/usr/bin/env bash
# Builds Arrow C++ with only what a Chargewire server would use from it: the IPC
# format and its ZSTD/LZ4 codecs. No Flight (so no gRPC/protobuf/abseil), no
# compute, no Parquet, no filesystems. For comparing against nanoarrow.
set -euo pipefail

ARROW_TAG=apache-arrow-26.0.0

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPS="$HERE/.deps"
SRC="$DEPS/arrow"
BUILD="$DEPS/arrow-build"
PREFIX="$DEPS/arrow-install"

mkdir -p "$DEPS"
if [ ! -d "$SRC/.git" ]; then
  git clone --quiet --depth 1 --branch "$ARROW_TAG" https://github.com/apache/arrow.git "$SRC"
fi

cmake -S "$SRC/cpp" -B "$BUILD" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" \
  -DARROW_DEPENDENCY_SOURCE=BUNDLED \
  -DARROW_BUILD_STATIC=ON \
  -DARROW_BUILD_SHARED=OFF \
  -DARROW_IPC=ON \
  -DARROW_WITH_ZSTD=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_COMPUTE=OFF \
  -DARROW_FLIGHT=OFF \
  -DARROW_PARQUET=OFF \
  -DARROW_CSV=OFF \
  -DARROW_JSON=OFF \
  -DARROW_FILESYSTEM=OFF \
  -DARROW_DATASET=OFF \
  -DARROW_ACERO=OFF \
  -DARROW_JEMALLOC=OFF \
  -DARROW_MIMALLOC=OFF \
  -DARROW_BUILD_UTILITIES=OFF \
  -DARROW_BUILD_TESTS=OFF
cmake --build "$BUILD"
cmake --install "$BUILD"

echo "Arrow C++ installed in $PREFIX"
