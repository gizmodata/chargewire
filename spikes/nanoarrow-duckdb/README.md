# Spike: DuckDB 2.0 + nanoarrow, no Arrow C++

**Question:** can a Warp SQL server move every DuckDB type in and out as Arrow IPC with
nanoarrow alone, through DuckDB's public C API only?

**Answer (2026-10-16): yes, with three gaps to close before 2.0.** Every type DuckDB
produces round-trips value for value through nanoarrow's IPC writer and reader with
no compression, ZSTD or LZ4, for both of DuckDB's main Arrow settings. The gaps:

1. **nanoarrow IPC has no Arrow "view" layouts or run-end encoding yet**: string view,
   binary view, list view and run-end-encoded arrays fail in both the reader and the
   writer ("… not yet supported in IPC reader", "Expected a valid enum ArrowType value
   but found 40/44"). DuckDB only emits views when asked (`arrow_output_version = '1.5'`,
   `arrow_output_list_view`), but clients such as Polars send them. Fix upstream in
   nanoarrow, which already has these types in its core library; until then the
   server refuses them with `unsupported_arrow_type`.
2. **DuckDB's INTERVAL holds microseconds.** An uploaded Arrow `month_day_nano` interval
   or nanosecond `duration` loses its sub-microsecond part (4,000,000,123 ns is stored as
   4,000,000,000), silently, in DuckDB's own Arrow ingest too. The server must refuse
   values it would truncate, or the protocol must say it truncates.
3. **DuckDB's default Arrow export of HUGEINT is invalid Arrow.** It is written as
   `decimal128(38, 0)`, but HUGEINT values reach 39 digits; pyarrow's full validation
   rejects the batch. Taking it back into DuckDB stores the out-of-range value without
   a check, and reading the table then fails, `v::VARCHAR` with a DuckDB INTERNAL
   error. This is a DuckDB bug, reproducible without nanoarrow; report it upstream.
   `arrow_lossless_conversion = true` avoids it (HUGEINT then travels as an opaque
   extension type).

## What it built

`warp-spike`, a small C++20 tool that does what a Warp SQL server does with data:

| Command | Path |
|---|---|
| `export` | SQL → DuckDB v2 C API (`duckdb_v2_statement_execute_arrow`, `…_to_arrow_c_stream`) → nanoarrow IPC writer |
| `ingest` | nanoarrow IPC reader → `warp_upload()` table function (DuckDB's v2 Arrow importer) → `CREATE TABLE … AS` / `INSERT INTO … SELECT * FROM warp_upload()` |
| `copy` | nanoarrow IPC reader → nanoarrow IPC writer |

The code (`src/`) includes **no DuckDB internal header**: only `duckdb_v2.h`, DuckDB 2.0's
new C API, where every function used is marked `stable: v2.0.0`. Ingest is one SQL
statement, so it is atomic, and creating a table needs no type-to-SQL code. The v2
Arrow importer needs a client context, which only a callback has, so ingest has to be
a table function; DuckDB's own v2 tests say so.

## Results

`check/` runs 1,247 cases in about 40 seconds: 1,144 pass, 6 are types DuckDB itself
refuses (decimal256, dense union; ours refuses them too, with DuckDB's error), and the
97 failures are all one of the gaps above or the two below.

| Check | What it compares | Result |
|---|---|---|
| `test_export` | 45 DuckDB types × 3 settings × 3 compressions, ours vs DuckDB's own Arrow export, schema metadata included | All pass with `default` and `lossless`. `views` fails on 8 types (gap 1) |
| `test_copy` | 51 Arrow types × input compression × output compression, pyarrow → nanoarrow → pyarrow | 47 of 51 types pass every combination; the 4 view/REE types fail (gap 1) |
| `test_ingest` | 51 Arrow types × 3 compressions, ours vs DuckDB's own Arrow ingest, plus values against the upload | All match DuckDB's own ingest. Value losses: `month_day_nano`, `duration_ns` (gap 2); gap 1 types fail |
| `test_roundtrip` | 45 DuckDB types, DuckDB → IPC → DuckDB, type and values | `lossless`: only ENUM changes (comes back VARCHAR, values intact). `default` also turns UUID, JSON → VARCHAR, BIT → BLOB, TIMETZ → TIME (offset lost), HUGEINT/UHUGEINT → DECIMAL(38,0) (gap 3) |
| `test_js` | 45 DuckDB types × 3 settings, nanoarrow IPC → Arrow JS 21.2.0 → back | All decode and re-encode identically except infinite timestamps: DuckDB writes ±infinity as the int64 extremes, and Arrow JS's `get()` throws on them |

Many-batch cases (a 1,000,000-row result in 10,000-row batches; a 1,000,003-row
ZSTD upload in 7,777-row batches) arrive complete and in order.

### What this means for the server

- **Default Arrow setting: `arrow_lossless_conversion = true`, without views.** It
  keeps UUID, JSON, TIMETZ offsets and HUGEINT exactly, and nanoarrow handles all of it.
- **Validate uploads before DuckDB sees them:** refuse view and REE layouts (until
  nanoarrow has them), sub-microsecond intervals and durations, and decimals beyond
  their declared precision, each with `unsupported_arrow_type` and a reason.
- **Compression stays opt-in** (spec §8.2): Arrow JS decodes compressed IPC only with a
  codec the application registers (`compressionRegistry`).
- **The JS client reads timestamps as raw int64s**, so infinite timestamps don't throw.

## Running it

```bash
./scripts/build-duckdb.sh                  # libduckdb at the pinned DuckDB 2.0 commit
cmake -S . -B build -G Ninja && cmake --build build
uv venv .venv && uv pip install --python .venv/bin/python \
    pyarrow==26.0.0 duckdb==2.0.0.dev2610161340 pytest
(cd js && npm install)
cd check && ../.venv/bin/python -m pytest -q
```

Pinned versions, all the newest available on 2026-10-16:

| Dependency | Version |
|---|---|
| DuckDB | `ffe0b8b7a2` (`v2.0.0-alpha48624`), the commit the `2.0.0.dev2610161340` wheel was built from |
| nanoarrow | `main` at `e5f3d0a55d55`: compressed IPC writing (apache/arrow-nanoarrow#935) is not in a release yet (latest: 0.9.0) |
| pyarrow | 26.0.0 |
| Arrow JS | 21.2.0 |
| zstd / lz4 | 1.5.7 / 1.10.0 |

DuckDB's install step does not ship `duckdb_v2.h` yet; `build-duckdb.sh` copies just
that header, so nothing here can include DuckDB's internals.
