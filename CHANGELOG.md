# Changelog

All notable changes to the Warp SQL protocol and this repository are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the protocol
spec's version is the repository's version.

## [Unreleased]

### Added

- Draft 0.1.0 of the protocol spec (`spec/warp-sql.md`): discovery and health,
  token authentication (password, token exchange, browser sign-in), sessions,
  transactions, statements with a one-round-trip streamed path and an async
  submit / poll / fetch path, resumable retained results, partitioned results,
  idempotent submission, cancellation and timeouts, prepared statements with Arrow
  parameter batches, bulk ingest, compression, metadata with Flight SQL's result
  schemas, a problem-document error model, graceful shutdown and versioning rules.
- Server SDK design (`docs/server-sdk.md`): the library owns every safety
  requirement; engines override "not implemented" backend methods.
- GizmoSQL 1.x parity plan (`docs/gizmosql-parity.md`): every Flight SQL operation
  and every 1.x integration test file mapped to Warp SQL.
- Arrow IPC spike (`spikes/duckdb-arrow-ipc`): DuckDB 2.0's v2 C API with no DuckDB
  internals, and the IPC done by Arrow C++ (IPC only, no Flight) or nanoarrow, with a
  1,247-case type harness (export, copy, ingest, DuckDB round trip, Arrow JS). Arrow C++
  passes 1,221 cases and fails none of its own; nanoarrow passes 1,144 (no view layouts
  or run-end encoding). The DuckDB and Arrow JS issues found are listed in its README.

- C++ server SDK (`cpp/`, `libwarpsql`): HTTP/1.1 and TLS on Boost.Beast with C++20
  coroutines; discovery, health, password sign-in, sessions and options, transactions,
  statements on the streamed and status-document paths, positional and named bind
  parameters, long-poll, retained and resumable results, unretained single reads,
  incomplete-stream signalling, ZSTD results, cancellation, timeouts, idempotent
  submission, CORS allow-list, header limits and graceful drain.
- Reference server `warp-duckdb`: DuckDB 2.0 through its v2 C API only.
- Prepared statements (§7) with JSON or Arrow bind parameters (one execution per row),
  streaming bulk ingest (§8: the body flows to the engine through a bounded queue; every
  batch is checked with `ValidateFull()`; sub-microsecond intervals and durations are
  refused rather than truncated), and metadata (§10) with Flight SQL's exact schemas.
- Conformance suite (`conformance/`): 46 black-box tests; fails, never skips, without a
  server.

### Changed

- Spec: bind parameters spelled out (§6.1: values never spliced into SQL; positional or
  named; how `null` is typed); response headers (§2.6); `result_not_ready`; more error
  codes (`not_found`, `method_not_allowed`, `payload_too_large`, `headers_too_large`,
  `statement_cancelled`, option errors).

- Server SDK design: GizmoSQL 2.0 targets DuckDB 2.0's v2 C API only (the RBAC plan
  inspector is the one module allowed DuckDB internals), runs as a standalone server,
  and keeps statement execution off HTTP request threads. IPC is done by Arrow C++ built
  with IPC only, not nanoarrow. ADBC client: `gizmodata/warp-sql-adbc` (Go).
