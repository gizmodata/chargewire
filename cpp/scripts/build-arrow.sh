#!/usr/bin/env bash
# Builds Arrow C++ with only what a Chargewire server would use from it: the IPC
# format and its ZSTD/LZ4 codecs. No Flight (so no gRPC/protobuf/abseil), no
# compute, no Parquet, no filesystems. For comparing against nanoarrow.
set -euo pipefail

ARROW_TAG=apache-arrow-26.0.1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# CHARGEWIRE_DEPS_DIR lets another project build these into its own directory.
DEPS="${CHARGEWIRE_DEPS_DIR:-$HERE/.deps}"
SRC="$DEPS/arrow"
BUILD="$DEPS/arrow-build"
PREFIX="$DEPS/arrow-install"

# CHARGEWIRE_ARROW_SIMD_LEVEL: Arrow's ARROW_SIMD_LEVEL (DEFAULT, NONE, SSE4_2, AVX2, ...).
# Unset keeps Arrow's default, which on x86 compiles everything for SSE4.2 (x86-64-v2); NONE
# builds for the compiler's baseline. Code Arrow picks at run time by CPU is unaffected.
SIMD_ARGS=()
if [ -n "${CHARGEWIRE_ARROW_SIMD_LEVEL:-}" ]; then
  SIMD_ARGS+=(-DARROW_SIMD_LEVEL="$CHARGEWIRE_ARROW_SIMD_LEVEL")
fi

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
  -DARROW_BUILD_TESTS=OFF \
  ${SIMD_ARGS[@]+"${SIMD_ARGS[@]}"}
cmake --build "$BUILD"
cmake --install "$BUILD"

echo "Arrow C++ installed in $PREFIX"
