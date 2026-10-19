# Server SDK design

This describes the reference C++ server library in this repo, `libwarpsql`. It is not
part of the protocol: any server that follows `spec/warp-sql.md` is a Warp SQL server.
The SDK exists so that putting an engine behind the protocol means writing the engine
part only. The API is in [`cpp/include/warpsql/`](../cpp/include/warpsql/); this page is
the map.

## The pattern

Arrow Flight SQL's C++ server has a base class, `FlightSqlServerBase`, whose virtual
methods (`GetFlightInfoStatement`, `DoGetTables`, `BeginTransaction`, …) return
"not implemented" until a server overrides them. The Warp SQL SDK keeps that pattern and
fixes one thing about it.

**What changes:** in Flight SQL every server writes its own authentication and
handle checks. In the Warp SQL SDK the library owns everything the spec requires for
safety, and the overridable methods only do engine work:

| The library does (not overridable) | The backend does (virtual, defaults to `not_implemented`) |
|---|---|
| HTTP/1.1 (Boost.Beast), TLS / mTLS, CORS, security headers | Run a statement and return an Arrow stream |
| Token exchange and validation (§4), with a pluggable authenticator and token service | Prepare, describe parameters, execute with a batch of parameters |
| Client certificates (§4.4): verification, CRLs and their reload, mapping a certificate to one identity, binding tokens to it and checking every request's connection | Map an identity to a principal (`Authenticator::AuthenticateCertificate`) |
| Session registry, idle eviction, `session_not_found` / `session_ended` (§5) | Session options, commit, rollback, the session's transaction state |
| Binding every handle to its principal (§9.2) | Bulk ingest from an Arrow stream |
| Idempotency keys (§6.7) | Metadata queries (§10), only what the principal may see |
| Statement state machine, long-poll, progress plumbing, timeouts | Interrupt a running statement; report progress |
| Result retention, resumption from a batch, expiry, quotas (§6.5); split results (§6.10) | Record a request's query tag (§2.5), if it keeps a history |
| Arrow IPC encoding, buffer compression, incomplete-stream signalling (§6.6) | Say which optional features it has (§3.1) |
| Graceful drain, health endpoint (§3.2, §11.1), problem documents (§11) | Check its own health; add `server-info` facts |
| The OpenAPI document (§3.3), compiled in from `spec/openapi.yaml` | |

A public entry point such as fetching a result is not virtual. It checks the
principal, the session and the handle, and only then calls the backend. A backend
author cannot forget the ownership check, because there is nothing to forget.

## The interfaces

[`backend.hpp`](../cpp/include/warpsql/backend.hpp):

- **`Backend`**: `Identity()` (discovery's `server`), `Capabilities()` (which of
  `prepared_statements`, `transactions` and `ingest` the engine has: the SDK lists only
  those in discovery and answers `not_implemented` for the rest), `OpenSession()`,
  `CheckHealth()` and `ServerInfo()`.
- **`EngineSession`**, one per session, called on that session's engine strand:
  `Execute()`, `Prepare()` and `ExecutePrepared()` return a `StatementOutcome` (rows as
  an `arrow::RecordBatchReader`, or a count); `Ingest()`, `Metadata()`, `SetOptions()` and
  `Options()`, `Commit()`, `Rollback()` and `Transaction()`; `Interrupt()` / `Cancel()`,
  `Progress()` and `CheckEnded()`, which may be called from any thread; `SetQueryTag()`
  around each statement and ingest, and `SetStatementId()` around each statement,
  prepared execution and prepare, with the id the client holds for it (`q_…`, `p_…`), so
  an engine that records or logs statements keys them the way clients see them.
  `warpsql::NewId(prefix)` makes ids of the same shape for an engine's own kinds.
- **`Authenticator`** turns credentials into a `Principal`; **`TokenService`** issues and
  checks tokens (the default keeps random tokens in memory; a server with replicas supplies
  signed ones).

A backend reports a failure by throwing `warpsql::Error` with the code that describes it
([`errors.hpp`](../cpp/include/warpsql/errors.hpp)); the SDK turns it into a problem
document with that code's status and title. Anything else that escapes a backend is
answered as `internal_error`.

[`server.hpp`](../cpp/include/warpsql/server.hpp) holds `ServerOptions` (listeners, TLS,
limits, lifetimes) and `Server`, plus what an engine may ask about its own sessions
(`Sessions()`, `CancelSession()`, `EndSession()`), for SQL such as a sessions table or
a command that ends a session.

`warp-duckdb` ([`cpp/src/duckdb/`](../cpp/src/duckdb/)) is a complete backend in about
3,000 lines, and the one the conformance suite and the drivers test against.

## Dependencies

The point of Warp SQL is a small build. The SDK needs only:

- **Arrow C++, built with IPC only**: `ARROW_IPC` with ZSTD and LZ4, and Flight,
  compute, Parquet, filesystems and the bundled allocators off. Without Flight it needs
  none of gRPC, protobuf, abseil, c-ares or re2; it builds in about 30 seconds and adds
  about 9 MB to the binary. Every incoming batch goes through `ValidateFull()`.
  [`spikes/duckdb-arrow-ipc`](../spikes/duckdb-arrow-ipc/README.md) compares it with
  nanoarrow, value by value for every DuckDB type.
- **Boost** (Beast, JSON, URL), fetched by CMake.
- **OpenSSL 3** for TLS.
