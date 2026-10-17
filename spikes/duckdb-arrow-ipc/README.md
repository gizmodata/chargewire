# Spike: DuckDB 2.0 + Arrow IPC, without gRPC

**Question:** can a Warp SQL server move every DuckDB type in and out as Arrow IPC
through DuckDB's public C API only, and which Arrow library should do the IPC:
nanoarrow or Arrow C++ (built with IPC only, no Flight)?

**Answer (2026-10-16): yes, and Arrow C++.** The same 1,247-case harness ran against
both:

| | Arrow C++ 26.0.0, IPC only | nanoarrow (main) |
|---|---|---|
| Harness | **1,221 pass**, 20 fail, 6 expected | 1,144 pass, 97 fail, 6 expected |
| Failures in the Arrow library | **none** | view layouts, run-end encoding (reader and writer) |
| Compressed IPC writing | released (26.0.0) | unreleased (main only, apache/arrow-nanoarrow#935) |
| Validates uploads | `ValidateFull()` refuses malformed batches (e.g. decimals beyond their precision) | not in this spike |
| Build (18-core Mac) | 29 s, 213 steps, from a clean clone | seconds |
| Tool size, stripped | 9.0 MB | 0.7 MB |
| Extra dependencies | zstd, lz4 (bundled) | flatcc (vendored), zstd, lz4 |

Arrow C++ without Flight pulls in none of what made the 1.x build hard: no gRPC,
protobuf, abseil, c-ares or re2. GizmoSQL 2.0 uses Arrow C++ with IPC only;
nanoarrow stays in the spike for comparison.

The 20 Arrow C++ failures are all outside Arrow:

1. **DuckDB's INTERVAL holds microseconds.** An uploaded Arrow `month_day_nano`
   interval or nanosecond `duration` loses its sub-microsecond part (4,000,000,123 ns
   is stored as 4,000,000,000), silently, in DuckDB's own Arrow ingest too. The server
   must refuse values it would truncate. (6 cases)
2. **Arrow JS 21.2.0 can't read list views, and its `get()` throws on DuckDB's infinite
   timestamps**, which DuckDB writes as the int64 extremes. The server doesn't send
   list views by default, and the JS client reads timestamps as raw int64s. (6 cases)
3. **DuckDB's default Arrow setting changes types**: UUID and JSON come back as
   VARCHAR, BIT as BLOB, TIMETZ as TIME (the offset is lost), and HUGEINT/UHUGEINT as
   `decimal128(38, 0)`, which can't hold their 39-digit values. That last one is
   invalid Arrow and a DuckDB bug: taken back into DuckDB it is stored unchecked, and
   reading the table then fails, `v::VARCHAR` with a DuckDB INTERNAL error. With Arrow
   C++ the server's validation now refuses that upload instead. With
   `arrow_lossless_conversion = true` only ENUM changes (it comes back VARCHAR, values
   intact). (8 cases)

The 6 expected failures are types DuckDB itself can't ingest (`decimal256`, dense
union); both builds refuse them with DuckDB's error and never store them as text.

## What it built

`warp-spike`, a small C++20 tool that does what a Warp SQL server does with data:

| Command | Path |
|---|---|
| `export` | SQL → DuckDB v2 C API (`duckdb_v2_statement_execute_arrow`, `…_to_arrow_c_stream`) → IPC writer |
| `ingest` | IPC reader (validating) → `warp_upload()` table function (DuckDB's v2 Arrow importer) → `CREATE TABLE … AS` / `INSERT INTO … SELECT * FROM warp_upload()` |
| `copy` | IPC reader → IPC writer |

The IPC layer is one file per library (`src/ipc_arrow.cc`, `src/ipc.cc`), behind
`src/ipc.hpp`. The DuckDB side (`src/database.cc`) includes **no DuckDB internal
header**, only `duckdb_v2.h`, DuckDB 2.0's new C API, where every function used is
marked `stable: v2.0.0`. Ingest is one SQL statement, so it is atomic, and creating a
table needs no type-to-SQL code. The v2 Arrow importer needs a client context, which
only a callback has, so ingest has to be a table function; DuckDB's own v2 tests say
so.

## The checks

`check/` runs in about 40 seconds:

| Check | What it compares |
|---|---|
| `test_export` | 45 DuckDB types × 3 Arrow settings × 3 compressions: ours vs DuckDB's own Arrow export, schema metadata included |
| `test_copy` | 51 Arrow types × input compression × output compression: pyarrow → our reader → our writer → pyarrow |
| `test_ingest` | 51 Arrow types × 3 compressions: ours vs DuckDB's own Arrow ingest, plus stored values against the upload; ingest modes; a 1,000,003-row upload in 7,777-row batches |
| `test_roundtrip` | 45 DuckDB types × 2 settings: DuckDB → IPC → DuckDB, type and values |
| `test_js` | 45 DuckDB types × 3 settings: our IPC → Arrow JS 21.2.0 → back, schema and values |

The three Arrow settings are DuckDB's `default`, `lossless`
(`arrow_lossless_conversion`) and `views` (lossless plus Arrow format 1.5 and list
views).

### What this means for the server

- **IPC with Arrow C++**, built with IPC only. Every incoming batch goes through
  `ValidateFull()` before DuckDB sees it.
- **Default Arrow setting: `arrow_lossless_conversion = true`, without list views.**
  It keeps UUID, JSON, TIMETZ offsets and HUGEINT exactly, and Arrow JS reads it.
- **Refuse uploads DuckDB would truncate** (sub-microsecond intervals and durations)
  with `unsupported_arrow_type` and a reason.
- **Compression stays opt-in** (spec §8.2): Arrow JS decodes compressed IPC only with a
  codec the application registers (`compressionRegistry`).
- **The JS client reads timestamps as raw int64s**, so infinite timestamps don't throw.

## Running it

```bash
./scripts/build-duckdb.sh                  # libduckdb at the pinned DuckDB 2.0 commit
./scripts/build-arrow.sh                   # Arrow C++ 26.0.0, IPC only
cmake -S . -B build -G Ninja && cmake --build build
uv venv .venv && uv pip install --python .venv/bin/python \
    pyarrow==26.0.0 duckdb==2.0.0.dev2610161340 pytest
(cd js && npm install)
cd check && ../.venv/bin/python -m pytest -q
```

For the nanoarrow comparison, build with `-DWARP_IPC_LIBRARY=nanoarrow` into another
directory and point the harness at it with `WARP_SPIKE_TOOL=<dir>/warp-spike`.

Pinned versions, all the newest available on 2026-10-16:

| Dependency | Version |
|---|---|
| DuckDB | `ffe0b8b7a2` (`v2.0.0-alpha48624`), the commit the `2.0.0.dev2610161340` wheel was built from |
| Arrow C++ | 26.0.0 (2026-10-09) |
| nanoarrow (comparison only) | `main` at `e5f3d0a55d55`; compressed IPC writing is not in a release yet (latest: 0.9.0) |
| pyarrow | 26.0.0 |
| Arrow JS | 21.2.0 |
| zstd / lz4 | 1.5.7 / 1.10.0 |

DuckDB's install step does not ship `duckdb_v2.h` yet; `build-duckdb.sh` copies just
that header, so nothing here can include DuckDB's internals.
