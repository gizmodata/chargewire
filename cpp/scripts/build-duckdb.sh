#!/usr/bin/env bash
# Builds libduckdb from DuckDB 2.0.1's release commit: the v2.0.1 tag on DuckDB's release
# branch (v2.0-cyanoptera), which DuckDB's own v2.0.1 release build was made from, so the
# extensions DuckDB publishes for v2.0.1 load into it.
set -euo pipefail

DUCKDB_COMMIT=8d78114e90a53823803660bf821f6073ac87da8c
# The version DuckDB's own builds of this commit report (PRAGMA version). Extensions are
# fetched by this string (extensions.duckdb.org/v2.0.1/<platform>/), so INSTALL finds the
# published ones; a source build without it would say "v2.0.1-devN" and look for them
# under the commit's hash, where DuckDB publishes none.
DUCKDB_VERSION_NAME=v2.0.1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# CHARGEWIRE_DEPS_DIR lets another project build these into its own directory.
DEPS="${CHARGEWIRE_DEPS_DIR:-$HERE/.deps}"
SRC="$DEPS/duckdb"
BUILD="$DEPS/duckdb-build"
PREFIX="$DEPS/duckdb-install"

# CHARGEWIRE_DUCKDB_EXTENSION_CONFIGS: extension config files (CMake files of
# duckdb_extension_load() calls, ';'-separated) whose extensions are built into libduckdb,
# on top of the ones DuckDB always builds in (core_functions, parquet, json, icu). Empty
# (the default, and the reference server's choice): no others; the rest load at run time.
EXTENSION_ARGS=(-DBUILD_EXTENSIONS="")
if [ -n "${CHARGEWIRE_DUCKDB_EXTENSION_CONFIGS:-}" ]; then
  EXTENSION_ARGS+=(-DDUCKDB_EXTENSION_CONFIGS="$CHARGEWIRE_DUCKDB_EXTENSION_CONFIGS")
fi

# CHARGEWIRE_DUCKDB_OPTIMIZATION_PROFILE: DuckDB's DUCKDB_OPTIMIZATION_PROFILE (CLI, EXTENSION
# or NONE). Unset keeps DuckDB's default, CLI, which on Linux x86_64 with GCC or Clang compiles
# for x86-64-v3 (AVX2); NONE builds for the compiler's baseline, for CPUs without AVX.
PROFILE_ARGS=()
if [ -n "${CHARGEWIRE_DUCKDB_OPTIMIZATION_PROFILE:-}" ]; then
  PROFILE_ARGS+=(-DDUCKDB_OPTIMIZATION_PROFILE="$CHARGEWIRE_DUCKDB_OPTIMIZATION_PROFILE")
fi

mkdir -p "$DEPS"
if [ ! -e "$SRC/.git" ]; then
  git clone --quiet --filter=blob:none https://github.com/duckdb/duckdb.git "$SRC"
fi
git -C "$SRC" fetch --quiet --tags origin
git -C "$SRC" checkout --quiet "$DUCKDB_COMMIT"
cmake -DDUCKDB_SOURCE_DIR="$SRC" -P "$HERE/scripts/patch-duckdb-binding-commit.cmake"

cmake -S "$SRC" -B "$BUILD" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" \
  -DOVERRIDE_GIT_DESCRIBE="$DUCKDB_VERSION_NAME" \
  -DDUCKDB_CAPABILITIES="httplib;loadable_extensions" \
  -DBUILD_SHELL=OFF \
  -DBUILD_UNITTESTS=OFF \
  -DENABLE_SANITIZER=OFF \
  -DENABLE_UBSAN=OFF \
  "${EXTENSION_ARGS[@]}" \
  ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"}
cmake --build "$BUILD"
cmake --install "$BUILD"
# The v2 C API header isn't installed by DuckDB's install rules (2.0.1).
cp "$SRC/src/include/duckdb_v2.h" "$PREFIX/include/"
# DuckDB's internal headers, for the one file that needs them (src/duckdb/duckdb_read_only_guard.cc,
# the guard of read-only sessions): duckdb.hpp and duckdb/, and the three vendored libraries
# their headers include, which DuckDB's install leaves out. Everything else here uses
# duckdb_v2.h only.
rm -rf "$PREFIX/include/duckdb" "$PREFIX/include/duckdb_third_party"
cp -R "$SRC/src/include/duckdb" "$SRC/src/include/duckdb.hpp" "$PREFIX/include/"
mkdir -p "$PREFIX/include/duckdb_third_party"
cp -R "$SRC/third_party/fmt/include/fmt" "$SRC/third_party/re2/re2" \
  "$SRC/third_party/fast_float/fast_float" "$PREFIX/include/duckdb_third_party/"

echo "libduckdb installed in $PREFIX"
