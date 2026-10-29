#!/usr/bin/env bash
# Builds libduckdb from DuckDB 2.0.0's release commit: the tip of DuckDB's release branch
# (v2.0-cyanoptera) that DuckDB's own v2.0.0 release build (its Main workflow, run 49137)
# was made from, so the extensions DuckDB publishes for v2.0.0 load into it.
set -euo pipefail

DUCKDB_COMMIT=d54cbbe3df17e8e5ce68312f483b4155836112aa
# The version DuckDB's own builds of this commit report (PRAGMA version). Extensions are
# fetched by this string (extensions.duckdb.org/v2.0.0/<platform>/), so INSTALL finds the
# published ones; a source build without it would say "v2.0.0-devN" and look for them
# under the commit's hash, where DuckDB publishes none.
DUCKDB_VERSION_NAME=v2.0.0

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

mkdir -p "$DEPS"
if [ ! -d "$SRC/.git" ]; then
  git clone --quiet --filter=blob:none https://github.com/duckdb/duckdb.git "$SRC"
fi
git -C "$SRC" fetch --quiet --tags origin
git -C "$SRC" checkout --quiet "$DUCKDB_COMMIT"

cmake -S "$SRC" -B "$BUILD" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" \
  -DOVERRIDE_GIT_DESCRIBE="$DUCKDB_VERSION_NAME" \
  -DBUILD_SHELL=OFF \
  -DBUILD_UNITTESTS=OFF \
  -DENABLE_SANITIZER=OFF \
  -DENABLE_UBSAN=OFF \
  "${EXTENSION_ARGS[@]}"
cmake --build "$BUILD"
cmake --install "$BUILD"
# The v2 C API header isn't installed by DuckDB's install rules (2.0.0). Copy
# just that header, so nothing here can include DuckDB's internal headers.
cp "$SRC/src/include/duckdb_v2.h" "$PREFIX/include/"

echo "libduckdb installed in $PREFIX"
