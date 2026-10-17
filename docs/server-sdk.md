# Server SDK design (draft)

This is the plan for the reference C++ server library in this repo. It is not part
of the protocol: any server that follows `spec/warp-sql.md` is a Warp SQL server. The
SDK exists so that putting an engine behind the protocol means writing the engine
part only.

## The pattern

Arrow Flight SQL's C++ server has a base class, `FlightSqlServerBase`, whose virtual
methods (`GetFlightInfoStatement`, `DoGetTables`, `BeginTransaction`, …) return
"not implemented" until a server overrides them. GizmoSQL 1.x has a DuckDB server
and a SQLite server built exactly that way. The Warp SQL SDK keeps that pattern and
fixes one thing about it.

**What changes:** in Flight SQL every server writes its own authentication and
handle checks. In the Warp SQL SDK the library owns everything the spec requires for
safety, and the overridable methods only do engine work:

| The library does (not overridable) | The backend does (virtual, defaults to `NotImplemented`) |
|---|---|
| HTTP, TLS / mTLS, CORS, security headers | Run a statement and return an Arrow stream |
| Token exchange and validation (§4), with a pluggable authenticator | Prepare, describe parameters, execute with a batch of parameters |
| Session registry, idle eviction, `session_not_found` / `session_ended` (§5) | Session options, commit, rollback |
| Binding every handle to its principal (§9.2) | Bulk ingest from an Arrow stream |
| Idempotency keys (§6.7) | Metadata queries (§10) |
| Statement state machine, long-poll, progress plumbing | Cancel a running statement; report progress |
| Result retention, resumption from a batch, expiry, quotas (§6.5) | |
| Arrow IPC encoding, buffer compression, incomplete-stream signalling (§6.6) | |
| Graceful drain, health endpoint (§3.2, §11.1) | |

A public entry point such as fetching a result is not virtual. It checks the
principal, the session and the handle, and only then calls the backend. A backend
author cannot forget the ownership check, because there is nothing to forget.

## Sketch

```cpp
namespace warpsql {

// What the library hands the backend: who is asking, in which session.
struct CallContext {
  const Principal& principal;      // fixed for the session's life
  SessionHandle& session;          // backend-owned state lives here
  CancellationToken cancel;        // set by cancel, timeout, disconnect, shutdown
};

// Results cross the boundary as the Arrow C stream interface, which DuckDB and
// most Arrow-capable engines produce natively. The library serializes them.
using ResultStream = ArrowArrayStream;

class Backend {
 public:
  virtual ~Backend() = default;

  // Sessions
  virtual Result<std::unique_ptr<SessionState>> OpenSession(const Principal&, const Options&);
  virtual Result<OptionResults> SetSessionOptions(CallContext&, const Options&);
  virtual Status Commit(CallContext&);
  virtual Status Rollback(CallContext&);

  // Statements
  virtual Result<StatementOutcome> Execute(CallContext&, const Sql&, const JsonParams&);
  virtual Result<PreparedHandle> Prepare(CallContext&, const Sql&);
  virtual Result<StatementOutcome> ExecutePrepared(CallContext&, PreparedHandle&,
                                                   ArrowArrayStream* parameter_rows);
  virtual Result<int64_t> Ingest(CallContext&, const IngestTarget&, ArrowArrayStream* rows);
  virtual Result<Progress> GetProgress(CallContext&, StatementHandle&);

  // Metadata (schemas as Flight SQL defines them)
  virtual Result<ResultStream> GetCatalogs(CallContext&);
  virtual Result<ResultStream> GetDbSchemas(CallContext&, const DbSchemaFilter&);
  virtual Result<ResultStream> GetTables(CallContext&, const TableFilter&);
  // … table types, keys, cross reference, type info, server info
};

// `StatementOutcome` is either rows (a ResultStream, optionally partitioned) or an
// update count. Every method above returns Status::NotImplemented() by default,
// and the library answers 501 `not_implemented` for it.

}  // namespace warpsql
```

## Dependencies

The point of Warp SQL is a small build. The SDK needs only:

- **nanoarrow** with its IPC library for reading and writing Arrow IPC streams. Not
  Arrow C++. Writing compressed IPC (ZSTD, LZ4) landed on nanoarrow's main branch on
  2026-09-09 (apache/arrow-nanoarrow#935) and is not in a release yet (0.9.0 can only
  read compressed IPC), so the SDK pins a main commit until the next release.
- **An HTTP/1.1 server library** (to be chosen).
- **OpenSSL** for TLS.
- **zstd and lz4** for compression.

No gRPC, no protobuf, no abseil, no c-ares, no re2.

### The nanoarrow spike (done 2026-10-16)

[`spikes/nanoarrow-duckdb`](../spikes/nanoarrow-duckdb/README.md) proved nanoarrow on
DuckDB 2.0: every DuckDB type round-trips value for value, with and without
compression, read back by pyarrow and by Arrow JS. Three gaps to close: nanoarrow's IPC
has no view layouts or run-end encoding yet (contribute upstream; refuse until then),
DuckDB's INTERVAL truncates sub-microsecond values, and DuckDB's default Arrow export
of HUGEINT is invalid (a DuckDB bug to report). The server defaults to
`arrow_lossless_conversion = true` without views.

## GizmoSQL 2.0 on this SDK (decided 2026-10-16)

- **DuckDB 2.0 only**, through its **v2 C API** (`duckdb_v2.h`). It covers everything
  the server needs: statements executed as Arrow with stepping, interrupt and progress
  (`duckdb_v2_statement_execute_arrow`, `…_arrow_result_step`,
  `…_connection_interrupt`, `…_connection_progress_get`), parameter schemas
  (`…_statement_bind`), settings, table descriptions, log sinks, and custom table
  functions (bulk ingest is one, `warp_upload()`, fed by the v2 Arrow importer).
- **One exception: the RBAC plan inspector.** The v2 API has no planner or binder hooks,
  and RBAC must see every table and column a bound plan touches. That module alone uses
  DuckDB's C++ internals, built against the exact pinned DuckDB; nothing else includes
  a DuckDB header beyond `duckdb_v2.h`. If DuckDB adds a plan-level "objects accessed"
  hook to the C API, the exception goes away.
- **A standalone server**, not a DuckDB extension. GizmoSQL embeds DuckDB, and the C API
  boundary keeps DuckDB upgrades cheap.
- **Asynchronous inside too.** Statements run on an engine worker pool that no HTTP
  request owns. The HTTP layer only submits, waits on state changes and copies batches
  out, so a slow reader or a long-poll never holds a query thread.

## Clients

- **ADBC: [`gizmodata/warp-sql-adbc`](https://github.com/gizmodata/warp-sql-adbc)**, a
  generic Go driver for any Warp SQL server, with the same shape as `gizmosql-adbc`
  (Go driver + cgo C exports + Python bindings). One driver serves every ADBC language.
- **JavaScript**: separate. It must read timestamps as raw int64 values (DuckDB writes
  infinite timestamps as the int64 extremes, which Arrow JS's `get()` refuses), and it
  registers a ZSTD codec with Arrow JS's `compressionRegistry` before asking for
  compressed results.

## Open questions

- **Where results are retained.** Memory with a cap, then local disk; shared object
  storage for clusters so a handle survives a node.
- **HTTP library** and the threading model for many concurrent long-polls and result
  streams.
