#!/usr/bin/env bash
# Puts DuckDB's internal headers of the pinned commit into an install prefix that has only
# DuckDB's libraries and C headers (DuckDB's own release build, as CI uses it):
# include/duckdb.hpp, include/duckdb/ and include/duckdb_third_party/{fmt,re2,fast_float}.
# src/duckdb/duckdb_read_only_guard.cc compiles against them; scripts/build-duckdb.sh, which
# builds DuckDB from source, installs the same without this script.
#
# The headers come from DuckDB's repository at exactly the commit scripts/build-duckdb.sh
# pins (a sparse checkout of that commit: git verifies what it fetched by its hash).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEPS="${CHARGEWIRE_DEPS_DIR:-$HERE/.deps}"
PREFIX="$DEPS/duckdb-install"
SRC="$DEPS/duckdb-headers-src"

DUCKDB_COMMIT="$(sed -n 's/^DUCKDB_COMMIT=//p' "$HERE/scripts/build-duckdb.sh")"
if [ -z "$DUCKDB_COMMIT" ]; then
  echo "scripts/build-duckdb.sh pins no DUCKDB_COMMIT" >&2
  exit 1
fi

rm -rf "$SRC"
mkdir -p "$SRC" "$PREFIX/include"
git -C "$SRC" init --quiet
git -C "$SRC" remote add origin https://github.com/duckdb/duckdb.git
git -C "$SRC" sparse-checkout set --no-cone /src/include/ /third_party/fmt/include/fmt/ \
  /third_party/re2/re2/ /third_party/fast_float/fast_float/
git -C "$SRC" fetch --quiet --depth 1 --filter=blob:none origin "$DUCKDB_COMMIT"
git -C "$SRC" checkout --quiet FETCH_HEAD
if [ "$(git -C "$SRC" rev-parse HEAD)" != "$DUCKDB_COMMIT" ]; then
  echo "fetched $(git -C "$SRC" rev-parse HEAD), not the pinned $DUCKDB_COMMIT" >&2
  exit 1
fi

rm -rf "$PREFIX/include/duckdb" "$PREFIX/include/duckdb_third_party"
cp -R "$SRC/src/include/duckdb" "$SRC/src/include/duckdb.hpp" "$PREFIX/include/"
mkdir -p "$PREFIX/include/duckdb_third_party"
cp -R "$SRC/third_party/fmt/include/fmt" "$SRC/third_party/re2/re2" \
  "$SRC/third_party/fast_float/fast_float" "$PREFIX/include/duckdb_third_party/"
rm -rf "$SRC"

echo "DuckDB's internal headers ($DUCKDB_COMMIT) installed in $PREFIX/include"
