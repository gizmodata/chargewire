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

The point of Warp SQL is a small build. The SDK should need only:

- **nanoarrow** with its IPC library for reading and writing Arrow IPC streams.
  nanoarrow 0.9.0 has an IPC encoder and writer (`src/nanoarrow/ipc/encoder.c`,
  `writer.c`) and an optional ZSTD codec (`NANOARROW_IPC_WITH_ZSTD`). Not Arrow C++.
- **An HTTP/1.1 server library** (to be chosen; cpp-httplib is already in GizmoSQL
  1.x for its OAuth endpoint).
- **OpenSSL** for TLS.
- **zstd** (optional) for compression.

No gRPC, no protobuf, no abseil, no c-ares, no re2.

### The first spike

Before the SDK depends on nanoarrow, prove it on the real engine:

1. Take a DuckDB result as an `ArrowArrayStream` and write it with nanoarrow's IPC
   writer.
2. Read it back with pyarrow and with Arrow JS, and compare **values**, not only
   column types, for every type DuckDB can return: all integer widths, decimals of
   each width, every temporal type and interval kind, strings and string views,
   binary, UUID, lists, structs, maps, unions, dictionaries, GeoArrow extension
   types, and nulls in each.
3. Repeat with ZSTD buffer compression on.
4. Do the reverse for ingest: write each type with pyarrow, read it with nanoarrow,
   ingest it into DuckDB, and compare.

Any type that fails is either fixed upstream or refused explicitly
(`unsupported_arrow_type`); none falls back to text.

## Open questions

- **Client library.** One ADBC driver gives every ADBC language (C/C++, Python, Go,
  Java, R, C#, Rust) access through the driver manager. Should it be C++ on
  nanoarrow plus libcurl, or Go? The JS client is separate either way.
- **Where results are retained.** Memory with a cap, then local disk; shared object
  storage for clusters so a handle survives a node.
- **Threading model** for long-polls and many concurrent result streams.
