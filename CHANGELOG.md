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

### Changed

- Server SDK design: GizmoSQL 2.0 targets DuckDB 2.0's v2 C API only (the RBAC plan
  inspector is the one module allowed DuckDB internals), runs as a standalone server,
  and keeps statement execution off HTTP request threads. IPC is done by Arrow C++ built
  with IPC only, not nanoarrow. ADBC client: `gizmodata/warp-sql-adbc` (Go).
