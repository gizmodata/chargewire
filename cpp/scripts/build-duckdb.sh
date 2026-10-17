#!/usr/bin/env bash
# Builds libduckdb from the exact commit the pinned DuckDB Python wheel was built
# from, so the C tool and the Python reference run the same engine.
set -euo pipefail

DUCKDB_COMMIT=ffe0b8b7a222114f15577ea05434d14357906416
# The version DuckDB's own builds of this commit report. Extensions are fetched by this
# string, so INSTALL finds the published ones (a source build would say "-devN").
DUCKDB_VERSION_NAME=v2.0.0-alpha48624

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# WARPSQL_DEPS_DIR lets another project build these into its own directory.
DEPS="${WARPSQL_DEPS_DIR:-$HERE/.deps}"
SRC="$DEPS/duckdb"
BUILD="$DEPS/duckdb-build"
PREFIX="$DEPS/duckdb-install"

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
  -DBUILD_EXTENSIONS=""
cmake --build "$BUILD"
cmake --install "$BUILD"
# The v2 C API header isn't installed by DuckDB's install rules yet (alpha). Copy
# just that header, so nothing here can include DuckDB's internal headers.
cp "$SRC/src/include/duckdb_v2.h" "$PREFIX/include/"

echo "libduckdb installed in $PREFIX"
