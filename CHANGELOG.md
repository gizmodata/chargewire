# Changelog

All notable changes to the Warp SQL protocol and this repository are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the protocol
spec's version is the repository's version.

## [Unreleased]

### Added

- Mutual TLS that carries identity (spec §4.4, §9.6; capability `mtls_identity`): the
  `client_certificate` grant signs in with the connection's verified client certificate and
  no password. The identity is one configured part of the certificate (SAN e-mail by
  default, SAN URI such as a SPIFFE ID, or subject CN); none or several values, or control
  characters, are refused (`unknown_certificate_identity`). Its tokens are always bound to
  the certificate (RFC 8705 `x5t#S256`): every request with one, partition reads and
  keep-alive connections included, must come over a connection presenting that
  certificate (`certificate_mismatch`). New codes `client_certificate_required`,
  `certificate_revoked`, `certificate_identity_mismatch`.
  A session opened with a bound token keeps the binding: any later request on it needs a
  token bound to the same certificate (`certificate_mismatch`), even one of the same
  principal from a password sign-in; sessions of unbound tokens are unchanged.
- Certificate sign-in is opt-in: the `client_certificate` grant is offered only where the
  server enables it, and answered with `400` `unsupported_grant_type` elsewhere (client CAs
  alone only admit connections). warp-duckdb: `--mtls-certificate-sign-in`, with
  `--mtls-admin-identities`. CRLs are checked for every certificate of the chain
  (`X509_V_FLAG_CRL_CHECK_ALL`): a revoked intermediate revokes what it issued, and a CA
  without a CRL fails closed unless `TlsOptions::allow_missing_ca_crls`
  (`--mtls-crl-allow-missing-ca-crls`) lets a CA above the leaf go without. Open connections
  are verified again when a CRL passes its nextUpdate. Identities are printable ASCII from
  every source, and refused values are escaped in details and logs. A session's binding is
  exact: an unbound session takes only unbound tokens too. `ServerOptions::on_security_event`
  reports every certificate refusal (user, token id, thumbprint, peer address);
  `TokenService::Accepted` is told about a token only after every check passed.
- Second review: of several CRLs from one CA the newest is used (CRL Number, else
  thisUpdate), the others reported as superseded; a CRL naming a client CA must be signed by
  it; delta CRLs are refused (OpenSSL would ignore one silently without a Freshest CRL
  extension). A refused CRL file stops startup, and on a reload keeps the last good CRLs.
  SecurityEvents are rate-limited per source (address and certificate: ten a minute in full,
  then one summary with `SecurityEvent::suppressed`, in a bounded LRU), a connection is closed
  after five failed authentications, and a client certificate the handshake refuses is a
  SecurityEvent too (`client_certificate_refused`, with its thumbprint and subject).
- Third review: handshake refusals count against the client's address only (an IPv6
  address by its /64), never the unverified certificate it chose; every client-supplied
  field of a SecurityEvent (subject, detail, user, ...) is cut at 512 bytes, in the record
  and in what the limiter keeps. A CRL reload that failed (the CRL, or the server's key or CA
  mid-rotation) is tried again at the next look. A bound credential (a scoped token sent as
  a password) is exchanged only over its own certificate. URI identities match exactly,
  case included (`CertificateIdentitiesMatch`); e-mail and CN identities ignore ASCII case.
  `warpsql-sdk-tests`: the limiter's unit tests.
- SDK: `TlsOptions::require_client_certificate` (default true), `client_crl_file` (CRLs
  checked at the handshake, reloaded when the file changes, checked every
  `crl_check_interval`, default 5 s; open connections are re-checked at their next request),
  `client_identity_source`, `require_identity_match` (default off: a password or
  token_exchange sign-in over a certificate must be for its identity, decided before the
  password is checked), `on_crl_reload`. `Authenticator::AuthenticateCertificate`,
  `Principal::certificate_thumbprint`, `TokenService::BindsCertificates` (the grant is
  offered only with a token service that keeps the binding), `ClientCertificate`.
- warp-duckdb: `--mtls-require-client-cert`, `--mtls-crl-file`, `--mtls-identity-source`,
  `--mtls-admin-identities`, `--mtls-require-identity-match` (Hardcharger's names), and
  `--flag=value` for every flag.
- `libwarpsql_client`: `Client::SignInWithClientCertificate()`, renewed before expiry like
  the password grant.
- Conformance: `test_mtls.py` (41 tests, marker `mtls`), with a PKI made by
  `cryptography`; `WARP_SQL_TEST_SERVER_MTLS_FLAG` names the server's client-CA flag.

- The C++ client library (`libwarpsql_client`, `cpp/include/warpsql/client.hpp`):
  discovery and health, sign-in (password, renewed before its token expires; token
  exchange), sessions and their options, transactions, statements on the streamed path
  (rows read as they arrive, an incomplete stream reported with the statement's own
  error, spec §6.6) and on the status-document path (submit, long-poll, fetch, re-read a
  retained result), bind parameters, prepared statements (JSON or Arrow parameter
  batches), cancellation from another thread (the running statement, or the session's
  when its id isn't known yet), bulk ingest as one chunked Arrow stream, the metadata
  endpoints, TLS with the system's or given roots and host-name checks, and mutual TLS.
  Tested by `cpp/tests/client_test.cc`: unit tests and live tests against warp-duckdb.
- The server-info document carries `server_arrow_version` (Flight SQL's
  `FLIGHT_SQL_SERVER_ARROW_VERSION`).
- Partitioned results on one server (spec §6.10, capability `result_partitions`): a
  client sends `Warp-Result-Partitions: n` (and `Warp-Result-Order: any`) and reads the
  partitions at once over separate connections; `interleaved` partitions rebuild the
  result's order round robin. Without the header a result has one partition, as before.
  `ServerOptions::max_result_partitions` (default 32). Conformance: `test_partitions.py`.
- `libwarpsql_client` reads split results: `StatementOptions::result_partitions` and
  `any_order` (sent to servers with `result_partitions`); every partition is read at
  once on its own connection and thread and merged into one `ResultReader` (round robin
  for interleaved results), and a failed partition fails the result with the statement's
  error. `StatementDocument::partition_order`.
- `WARPSQL_STREAM_STATS=1`: the SDK logs each result stream's and statement's stage times.
- Partitioned ingest (spec §8.3, capability `partitioned_ingest`): one ingest opened
  with `POST …/ingests`, fed by up to 256 parts uploaded at once over separate
  connections, and committed atomically with `POST …/commit`; all or nothing (a failed
  or broken part fails the whole ingest), rows in no defined order, every part bound to
  the session and its principal, retried parts answered from their Idempotency-Key,
  and an idle ingest aborted after `ServerOptions::ingest_idle_timeout` (120 s). New
  error codes `ingest_not_found`, `ingest_incomplete`, `ingest_part_exists`,
  `ingest_expired`. The SDK reads every part through one `IngestBatches` reader, so the
  engine inserts from all parts in parallel in one transaction, with each part's body in
  a bounded queue. Conformance: `test_partitioned_ingest.py`.
- `libwarpsql_client` sends a large upload as a partitioned ingest:
  `IngestOptions::parts` (default 8) on a server with `partitioned_ingest`, when the
  upload has two batches or more; `parts = 1` keeps one ordered stream. A failed part
  aborts the ingest (`DELETE`), so nothing is written.
- `session_ended` problems always carry `reason` (spec §5.6): `Error::SessionEnded()`
  and the `session_ended_reason` constants (`killed`, `credentials_changed`,
  `principal_dropped`, `token_expired`). Conformance tests for idle eviction and token
  expiry.

### Changed

- TLS servers refuse TLS 1.2 renegotiation (a connection's certificate is fixed at its
  handshake), send the client CA list in the certificate request, and set a session id
  context. Before, every resumed TLS session failed its handshake when client certificates
  were verified (OpenSSL needs a session id context then), so clients turned session reuse
  off. A resumed session keeps the certificate of its first handshake (a bound token checks
  that one, and another certificate cannot be swapped in by resuming), and sessions don't
  resume across a CRL reload: each CRL generation has its own session cache and ticket keys.
  Conformance: two resumption tests, and the revocation test resumes a pre-revocation session.
- Spec §9.4 spells out that metadata leaves out the schemas, tables, views, columns and
  keys a principal holds no privilege on, inside catalogs it may see, and that SQL's own
  catalog listings should follow the same rules.
- DuckDB backend: `DuckDBSession::TablesWithSchemas()` is protected, so a server that
  leaves tables out of the tables metadata describes only the ones it keeps.

- `libwarpsql_client` asks for split results by default:
  `StatementOptions::result_partitions` is 8 (was 1). The server decides: a result of
  fewer than two batches still comes as one stream, and a server without
  `result_partitions` ignores the request. `result_partitions = 1` keeps one stream.
- TLS: the SDK's own TLS stream (`TlsStream`) encrypts whole gathered writes (up to
  1 MiB) at once and sends them while it encrypts the next, instead of Asio's
  record-at-a-time `ssl::stream`. A TLS result stream went from about a third of
  plaintext to the same speed on one AWS connection (the flow's 9.5 Gbit/s).
- Result streams are a pipeline: batches are encoded (and zstd-compressed) on an encode
  pool, several at once, and written in order in about 1 MiB writes; an unretained
  result's buffer is bounded by bytes (32 MiB) rather than batch count.
- Uploads are read straight from the connection in 1 MiB pieces (Content-Length or
  chunked framing decoded by the SDK), not through Beast's 64 KB parser reads.
- The DuckDB backend ingests in parallel: `warp_upload()` is a parallel table function
  over the SDK's thread-safe `IngestBatches` reader, with batch indexes, so DuckDB's
  batch insert runs on many threads and keeps the upload's order, in one transaction.
- The SDK and `warp-duckdb` build on Windows x64 and arm64 (MSVC). `warpsql::StopSignals`
  (`include/warpsql/stop_signals.hpp`) gives a server's main() its stop requests:
  SIGINT/SIGTERM on POSIX, the console's Ctrl+C, Ctrl+Break, close and shutdown events on
  Windows. Platform code is in `src/sdk/platform.cc` (client-gone detection with WSAPoll
  on Windows; no SO_REUSEADDR there, where it lets a second server bind a listening
  port). On Windows DuckDB is linked statically, with the extensions its shared library
  links. The conformance harness stops a server it started with Ctrl+Break on Windows.

### Fixed

- A statement that failed before its answer started, after it had buffered rows, was
  answered `200` with a `failed` statement document (the answer a statement without rows
  gets) instead of its error response. A streamed request that asks for partitions waits
  up to 5 s for the split decision, so with partitions (the drivers' default) any query
  failing in that time got it. It is now the error response (`500 execution_error`, …),
  as spec §6.1 now says outright; §6.2 adds that a `failed` or `cancelled` document is an
  error whatever the status code. Conformance:
  `test_a_failure_before_the_answer_is_an_error_response`, and the mid-stream test accepts
  only an incomplete stream or the error response, with the statement's own error.

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

### Fixed

- A JSON bind parameter that is an integer outside the 64-bit range is bound exactly
  (DuckDB backend: HUGEINT, else UHUGEINT; wider is refused with `bad_request`). It was
  read as a double and bound rounded (10^38 - 1 as 1e38). Spec §6.1 now requires exact or
  refused; the SDK hands such integers to the engine as `{"\0integer": "<digits>"}`
  (`warpsql::kWideIntegerKey`). Conformance: `test_integer_parameters_keep_their_exact_value`.
- `GET /warp/v1/sessions/{id}` of a session the server ended answers `401`
  `session_ended` with its `reason` (spec §5.2, §5.6), as every other request does; it
  answered `200`, so a client using it as a liveness probe saw a dead session as alive.
  Engines report it with the new `EngineSession::CheckEnded()` (safe from any thread).
- DuckDB backend: the `arrow_lossless_conversion` session option ran a plain `SET`,
  which for that setting changes the instance's default: one session switching it
  changed every other session's HUGEINT, UUID and JSON results (their options still said
  `false`). It is now `SET SESSION`.
