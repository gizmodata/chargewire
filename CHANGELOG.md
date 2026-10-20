# Changelog

All notable changes to the Warp SQL protocol and this repository are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html): the protocol
spec's version is the repository's version.

## [Unreleased]

### Added

- **C++ SDK: `ServerOptions::trusted_proxies`**, the reverse proxies whose forwarding headers
  name the client (`IpNetwork` blocks, from `ParseIpNetwork("10.0.0.0/8")`, `"fd00::/8"` or a
  bare address; it throws `std::invalid_argument` naming a bad one). When a request's TCP peer
  is in one, its client address is the rightmost `X-Forwarded-For` hop that is not itself a
  trusted proxy (all trusted: the leftmost), or, without `X-Forwarded-For`, the same walk
  over RFC 7239 `Forwarded: for=` (quoted values, `"[v6]:port"`). `X-Forwarded-For` wins
  because the common proxies append to it and pass a client's `Forwarded` on. An unknown,
  obfuscated or malformed hop leaves the TCP peer. Resolved per request (a proxy's
  connection carries many clients). The address feeds `SecurityEvent::peer`,
  `SessionActivity::client_address` and the per-address bound on browser sign-ins, never
  authentication or TLS; the connection's own address is in the new
  `SecurityEvent::proxy` and `SessionActivity::proxy_address` when it differs. Default:
  empty, no proxy trusted. Tests: `cpp/tests/forwarded_test.cc`.

### Changed

- **C++ SDK: an IPv6 peer is written `[2001:db8::7]:5000`** in `SecurityEvent::peer` and
  `SessionActivity::client_address` (it was `2001:db8::7:5000`, ambiguous with an address
  without a port). A forwarded address has no port: `203.0.113.9`, `[2001:db8::7]`.

### Fixed

- **A slow read no longer loses its session** (§5.7). The SDK's idle sweep evicted a
  session whose statement had finished while the client was still reading its unretained
  result (up to `unretained_buffer_bytes` of it goes out after the statement finishes), and
  the stream ended without its end-of-stream marker. A read in progress now keeps its
  session alive, retained or not, and §5.7 says a request in progress counts until its
  response ends.

## [1.0.0] - 2026-10-19

The first stable version of the protocol. From here on version 1 changes only by addition
(spec §12). These shape fixes landed before the tag, so they are not additive changes over
the drafts; nobody depended on the drafts, so none has a fallback.

### Changed

- **Statuses say whose problem it is** (§11). `execution_error` (a statement that failed
  while running: a division by zero, a constraint, a conversion) is `422`, not `500`; the
  new `internal_error` (`500`) is for the server's own faults (DuckDB's internal and fatal
  errors, an exception that isn't a protocol error, a broken invariant). A statement's own
  outcome is never `5xx` and never `408`. `statement_cancelled` and `statement_timeout` are
  `409` (they were "—" in the spec and `500` in the SDK).
- **A statement cancelled by the time its submit is answered is an error response** (§6.1),
  as a failed one already was: `409` with the problem document's new `cancel_reason`
  member, and `Warp-Statement-Id` naming the statement (now on a failed statement's error
  response too). Before, the SDK answered `200`/`201` with a `cancelled` document. An
  engine that cancels a statement itself (`statement_cancelled` from the engine) leaves it
  `cancelled` with `cancel_reason: cancelled`, not `failed`.
- **Codes**: a forgotten statement is `statement_not_found` (§6.5 said `not_found`); a
  partition the result doesn't have is the new `partition_not_found`; `not_found` means no
  such endpoint only. `client_certificate_required` is `certificate_required`, the name of
  the TLS alert it stands for. Codes a client makes up for its own failures begin with
  `client_`, which servers never send; the C++ client's are `client_protocol_error`,
  `client_connection_failed`, `client_tls_failed`, `client_session_closed`,
  `client_invalid_argument`, and `client_http_<status>` for an error answer that carries no
  problem document.
- **Open enumerations** (§12): clients accept unknown values of every enumerated field (an
  unknown statement or ingest `state` is not terminal, an unknown `reason` or
  `cancel_reason` is generic), and a request field, header or value that asks for new
  behaviour comes with a capability.
- **Capabilities** (§3.1): a table of them, with what each gates. Result retention is core
  (a server may still decline to retain, and says so in `retained`), so `result_retention`
  is gone; `partitioned_results` is gone, and partitions on other servers are the new
  `result_locations`: a partition's `location` is `null` unless the client sent
  `Warp-Result-Locations: remote`. `mtls_identity` is in the example. Discovery has no
  `draft` field.
- **Names for extensions** (§2.7): the protocol's capabilities, session options,
  `server-info` keys, codes and reasons are unprefixed; an implementation's own take its
  prefix. warp-duckdb's session option is `duckdb_arrow_lossless_conversion` (was
  `arrow_lossless_conversion`) and its `server-info` key `duckdb_version` (was `engine` and
  `engine_version`).
- **`server-info`** (§10): the naming rule is exact (drop a leading `FLIGHT_SQL_`, then lower
  case), values take their `SqlInfo` type's JSON type, and the version 1 keys are a table
  (`server_name`, `server_version`, `server_arrow_version`, `server_read_only`,
  `sql_identifier_quote_char`, `sql_transactions_supported`). `protocol` and
  `protocol_version` are gone (discovery has them). warp-duckdb adds `server_read_only`.
- Partitioned ingest's commit body is `{"part_count": n}` (was `{"parts": n}`, which the
  ingest document uses for its array of parts).
- `session_ended`'s MUST (§5.6) is for the token-related reasons only: after `killed` a
  client may open a new session with the token it has.
- After `invalid_token` with `token_revoked`, `credentials_changed` or `principal_dropped`,
  a client doesn't sign in again on its own (§4.2); it may once without a reason, after
  `token_expired`, or with a reason it doesn't know.
- `problem.title` is a human-readable summary of the code ("Permission denied"); the SDK
  sent the code.
- The spec names no implementation: its examples are generic.

### Added

- **An OpenAPI 3.1 description of the protocol** (`spec/openapi.yaml`): every endpoint of
  version 1 with its parameters, headers, request and response schemas, the problem
  document, and Arrow bodies as `application/vnd.apache.arrow.stream` binary; HTTP bearer
  authentication (`bearerAuth`) everywhere except discovery, health, sign-in and the document
  itself. The specification stays the definition. `spec/scripts/lint-openapi.sh` lints it
  (Redocly CLI 2.57.0's recommended rules, with no warnings, and openapi-spec-validator).
- **Servers can serve it** (spec §3.3, optional capability `openapi`): `GET
  /warp/v1/openapi.json` answers the document as JSON without authentication, cacheable
  (`Cache-Control` with `max-age`, `ETag`, `304` on `If-None-Match`); discovery lists the
  capability and the path in its new `links` member (`links.openapi`). Appendix B lists every
  endpoint of version 1, the list the OpenAPI document must match.
- **The SDK serves it**, so `warp-duckdb` and every server on `libwarpsql` do: the build
  converts `spec/openapi.yaml` to JSON with a small host tool (`cpp/tools/openapi_json.cc`, a
  strict YAML subset that refuses anything it would not read as PyYAML does) and compiles it
  in, so nothing reads YAML at run time. `GET /warp/v1/openapi.json` answers it with
  `Cache-Control: public, max-age=3600` and an `ETag`; discovery lists `openapi` and
  `links.openapi`. Unit tests in `cpp/tests/openapi_test.cc`.
- **The conformance suite holds every response to the OpenAPI document**
  (`conformance/tests/openapi_check.py`): status, content type, required headers and the
  JSON body's schema, on every request of every test, so a server and the document can't
  drift apart unnoticed (responses of all 38 operations are checked in a run against
  `warp-duckdb`). `test_openapi.py` checks the document against Appendix B and the served
  copy against the published one, and shows the check rejects real responses broken on
  purpose.
- Browser sign-in in the protocol (spec §4.3, capability `oauth_browser`), complete: a
  client creates a flow (`POST /warp/v1/auth/oauth/flows`, optional `return_to`) and gets
  `flow_id`, `poll_secret`, `authorize_url`, `poll_interval_s` and `expires_at`; the identity
  provider sends the browser to the server's callback (`GET /warp/v1/auth/oauth/callback`, the
  redirect URI operators register); the client collects the server's own access token once by
  polling `POST /warp/v1/auth/oauth/flows/{flow_id}` with `{"poll_secret"}` in the body
  (`pending`, `complete`, `failed` with `error.code` `access_denied`, `invalid_credentials` or
  `identity_provider_error`; then `404 flow_not_found`, which a wrong secret also gets). The
  poll secret is never in a URL. Clients in a browser pass a `return_to` path on the server's
  own origin and are sent back to it with the flow id in the URL fragment
  (`#warp_oauth_flow=…`); anything else (absolute URLs, `//host`, backslashes, fragments,
  whitespace, non-ASCII) is `400 invalid_return_to`. The server uses `state`, PKCE (S256) and
  an OpenID Connect `nonce` with the provider; flows are bounded (`429 rate_limited` with
  `Retry-After`), expire and are single-use; every answer is `Cache-Control: no-store`, and the
  callback page runs no script (CSP `default-src 'none'`), sends no Referer and can't be framed.
  Discovery describes it in `auth.oauth_browser` (`{"provider": "<name>"}`), and §9.7 lists the
  security rules, phishing by a forwarded authorize URL included. New error codes
  `flow_not_found` (404) and `invalid_return_to` (400). It replaces a separate sign-in port.
- SDK: `BrowserSignInProvider` (`ServerOptions::browser_sign_in`): a server supplies the
  identity provider's authorize URL and the code exchange; the SDK keeps the flows (ids,
  poll secrets as digests compared in constant time, state, PKCE verifiers, nonces, expiry,
  bounds in all and per client address: `max_browser_sign_ins`,
  `max_browser_sign_ins_per_address`, `browser_sign_in_lifetime`), serves the endpoints and
  the callback, applies identity match (§4.4) when the token is collected, and issues the
  token (`auth_method` `oauth_browser`, never certificate-bound). `StatePrefix()` and
  `PublicOrigin()` let a proxy on another origin route the provider's redirects to the server.
- Client library: `Client::StartBrowserSignIn`, `PollBrowserSignIn`, `SignInWithBrowser` and
  `BrowserSignInProviderName`; `PostForm` beside `GetJson`.
- `warp-duckdb`: browser sign-in with an OpenID Connect provider (`--oauth-issuer`,
  `--oauth-client-id`, `--oauth-identities`, `--oauth-scopes`, `--oauth-base-url`,
  `--oauth-redirect-uri`, `--oauth-provider-name`, `--oauth-max-flows`; the client secret from
  `WARP_DUCKDB_OAUTH_CLIENT_SECRET`).
- Conformance: `test_oauth_browser.py` (marker `oauth`; 34 tests) against a mock OpenID
  Connect provider (`idp.py`) that checks PKCE, redirect URIs and single-use codes: discovery,
  a whole flow and its token, the authorize URL (state, PKCE, nonce, no secret), the poll
  secret required (wrong, missing, in the URL or a header), single-use flows and callbacks,
  unknown states, callback headers and escaping, refusals at the provider, unauthorized users,
  a foreign nonce, `return_to` redirects (fragment only, on failure too) and 17 rejected
  `return_to` values, the bound on pending flows, and `not_implemented` without the
  capability. The server flags are a template (`WARP_SQL_TEST_SERVER_OAUTH_ARGS`). With the
  secret check removed the secret test fails, and with `return_to` unchecked 17 tests fail.
- Spec: `Warp-Query-Tag` defined (§2.5): an ASCII JSON object of at most 4,096 bytes that a
  server may record with the statement or ingest. The SDK refuses a malformed one (`400`)
  and hands it to `EngineSession::SetQueryTag()` around each statement, prepared execution
  and ingest (for a partitioned ingest, the opening request's).
- Spec: the statement document's fields as a table (`finished_at`, and a `rows` result while
  the statement runs), the ingest document's (`rows_ingested`), `PATCH` of a session (`200`,
  every option and `errors`), a bad option failing `POST /sessions`, `Retry-After` as
  delta-seconds only, split-result headers that MUST both be present for more than one
  partition, and `Warp-` header names kept for the protocol.
- Spec §6.5: expiry applies to reads that start after it; a server MUST deliver a read
  already under way when its result expires in full, end-of-stream marker included
  (deleting the statement or ending its session may still cut it, §6.6). Conformance:
  `test_a_read_under_way_when_its_result_expires_is_delivered_in_full` (warp-duckdb's new
  `--result-retention-seconds`; it fails on a server without the SDK fix above).
- Spec: Appendix A is now "Possible future additions" (upload resumption, result
  statistics, push notification, forwarded client certificates, per-partition tokens), each
  additive behind a capability.
- SDK: `Backend::Capabilities()` (`prepared_statements`, `transactions`, `ingest`): discovery
  lists only what the backend has, `sql_transactions_supported` follows it, and the
  endpoints of a missing feature answer `not_implemented`. The default is none.
- SDK: `CodeTitle()`, `ErrorCode::kInternalError`, `kPartitionNotFound`,
  `Error::Cancelled()` and `Error::cancel_reason()`.
- warp-duckdb: `--health-check-query SQL`; a health check that hangs is interrupted after
  10 s, and a probe that comes while one runs fails at once instead of queueing.
- C++ client: `User-Agent: warpsql-client-cpp/1.0.0` (the version from CMake), and
  `cancel_reason` read from problem documents.
- Conformance (266 tests with browser sign-in's): another principal's session, statement, retained result,
  prepared statement and ingest are `404 session_not_found` (and its ids `*_not_found` in
  one's own session), with the owner's twin; a forwarded client-certificate header is no
  certificate; unknown request fields are ignored; ids are long random strings; query tags;
  one signal drains (new work `503 server_draining` with `Retry-After`, running work
  finishes, what remains at the end of the grace period is cut); health answers in bounded
  time while its query hangs; metadata follows authorization and a prepared statement is
  authorized at each execution (on a server with privileges, `WARP_SQL_TEST_AUTHZ_*`);
  `PATCH` and create of a session; the statement document while running and finished;
  cancelled-at-submit answers; `Cache-Control: no-store` on every result response; status-only
  checks now assert codes; every problem's `title` is for people.
  `conformance/README.md` lists the MUSTs it doesn't test yet.
- **SDK: `EngineSession::SetStatementId()`** tells the engine the id the client holds for
  what it is about to run: the statement's `q_…` around `Execute()` and
  `ExecutePrepared()`, the prepared statement's `p_…` around `Prepare()` (the SDK now
  chooses that id before preparing). An engine that records statements (a query history,
  instrumentation, logs) keys them by the ids clients see in `Warp-Statement-Id`, errors
  and prepare answers. `warpsql::NewId(prefix)` makes ids in the SDK's shape (§2.3) for an
  engine's own kinds.
- **The protocol's site** (`site/`, Astro Starlight; future address `warpsql.gizmodata.com`,
  a private GitHub Pages preview until launch): the specification, generated from
  `spec/warp-sql.md` with every "§" linked; eleven flows (sign-in by password, token exchange
  and browser, sessions and transactions, the streamed and submit/long-poll/fetch paths,
  retention and resumption, partitioned results, prepared statements, single and partitioned
  ingest, cancel and close, errors mid-stream, graceful shutdown), each with sequence diagrams
  rendered at build time to light and dark SVG and `curl` commands that
  `site/scripts/verify_examples.py` runs against the reference server (60 examples); the
  OpenAPI reference in Swagger UI 5.33.3, bundled, with "Try it out" against a server of the
  reader's choosing; the endpoint list; the conformance page, generated from the suite; the
  implementations; and versioning. `.github/workflows/site.yml` checks the generated pages
  and diagrams, lints the OpenAPI document, builds (failing on broken internal links), checks
  external links with lychee, and deploys to GitHub Pages on pushes to `main`.

### Added (before the 1.0.0 shape fixes)

- The SDK's view of its sessions, for an engine's own SQL (Hardcharger's
  `hardcharger_active_connections()`, `hardcharger_cancel()`, `KILL SESSION`):
  `Server::Sessions()` describes each session as the transport sees it (the latest
  request's address, User-Agent, TLS version and client certificate, the identity that
  certificate maps to, whether the token is bound, the statement it works on and its state,
  submit and start times, partitions, open result streams, ingest bodies arriving, retained
  results and their rows, and why a cancelled statement is unwinding).
  `Server::CancelSession(id, reason)` stops a session's work and keeps the session: its
  statements end cancelled with `cancel_reason` `cancelled` or `killed` (spec §6.2, which
  gains `cancelled`), its result readers get an incomplete stream (§6.6), its open
  partitioned ingests and its ingest bodies still arriving are aborted (nothing written).
  `Server::EndSession(id, error)` ends one: every request naming it, by its owner, gets that
  error at once (`session_ended`, §5.6), nothing new starts in it, and once its running work
  has stopped (it cancels nothing itself) its results are freed and its engine session
  destroyed, on its strand; the registry remembers why for its owner (for at least 15
  minutes, or the idle timeout), and a DELETE of it answers 204.
  `EngineSession::Cancel(reason)` (default: `Interrupt()`) is how the SDK interrupts an
  engine for a cancelled statement, with the cancel_reason, so an engine can record why its
  work stopped.

- The session's transaction (spec §5.2): the session document carries `transaction`,
  `none`, `open` or `aborted`, so a client shows whether a transaction is open, and
  whether a failed or cancelled statement has left it needing a rollback, without
  tracking `BEGIN` and `COMMIT` itself (which it can't do for SQL it doesn't parse, nor for
  errors an engine forgives). `aborted` is what the engine does: DuckDB keeps a
  transaction usable after a syntax error, so the reference server reports `open` there.
  The SDK's `EngineSession::Transaction()` (default `kNone`) is read on the engine thread
  after every statement, ingest, option change, commit and rollback, before the statement
  ends, so a client that reads the session once its result is done sees what it did. The
  DuckDB backend follows `BEGIN`/`START` and `COMMIT`/`END`/`ROLLBACK`/`ABORT` by
  statement type and first keyword, and asks DuckDB whether an open transaction is
  aborted. Conformance: ten tests in `test_sessions.py` (each spelling of begin and end, a
  multi-statement request, an abort until `ROLLBACK`, what the engine reports after a
  syntax, catalog and conversion error and after a cancel, failures outside a
  transaction, and `autocommit` off across commits).
- LZ4_FRAME Arrow buffer compression (spec §8.2), capability `ipc_compression_lz4_frame`.
  `Warp-IPC-Compression` is now a list of codecs in the client's order of preference
  (`lz4_frame, zstd`): the server uses the first it supports, skipping names it doesn't
  know; `none`, or no codec it knows, means uncompressed. Each partition's request carries
  its own header. Inside a compressed stream a server may leave a buffer uncompressed
  (Arrow's length -1), and clients must read it. Conformance: both codecs listed;
  lz4_frame results (LZ4 frames present, zstd's absent, equal rows); the preference rules;
  incompressible and null columns under both codecs; each partition gets the codec its
  own request asked for; lz4 uploads, and a partitioned ingest whose parts use lz4, zstd
  and none. The five about results fail on a server without the change.
- Spec §6.10: a server SHOULD size a split result by the result's size, and may hold the
  first batches back, for a bounded time, to see it.

- Why a token stopped working (spec §4.2): when a server refuses a token it issued and
  verified (an expired one, or one it ended because the principal's credentials changed,
  the principal was dropped or disabled, or the token was revoked), the `invalid_token`
  problem document carries a `reason`: `token_expired`, `token_revoked`,
  `credentials_changed` or `principal_dropped`. A token the server can't verify as its own
  gets none, so a reason never tells anyone but the token's holder about its principal.
  The SDK's `Error::InvalidToken(reason, detail)` sends it, and its default token store
  answers `token_expired` for its own tokens up to an hour after they expire (before, an
  expired token was as unknown as a forged one). `warpsql::session_ended_reason` is
  `warpsql::reason`, with `kTokenRevoked`. Conformance:
  `test_an_expired_token_is_refused_until_the_client_signs_in_again` now requires
  `invalid_token` with `reason` `token_expired`, also when creating a session, and no
  reason for the same token altered (it failed on both servers before);
  `test_requests_without_a_valid_token_are_refused` asserts no reason. The drivers read
  `reason` from every problem document already, so they report it unchanged.

- Spec §5.6: `session_ended` has a `token_revoked` reason (the session's token was
  revoked; `token_expired` is now only for a token that expired), and says how the reason
  reaches a client: the session's own token is usually ended with the session, so its next
  request gets `invalid_token` with the same reason first, and `session_ended` reaches a
  client that signed in again. It used to read as if `session_ended` came first.

- Client library: a TLS alert refusing the client certificate is reported as the refusal
  it stands for (spec §4.4), `certificate_revoked` or `client_certificate_required`
  (status 0), and any other TLS failure after the handshake as `tls_failed`; with TLS 1.3
  these alerts arrive on the first read, and were `connection_failed` ("could not
  connect"). The spec's §4.4 says clients should do this. Tests:
  `LiveTls.ARevokedClientCertificateIsReportedAsRevoked`, `LiveTls.MutualTls`.

- CORS with credentials (spec §9.5): the SDK answers an allowed origin, preflights
  included, with `Access-Control-Allow-Credentials: true` beside the origin itself (never
  `*`), so a page there can send requests with `credentials: "include"` and a browser
  presents the user's client certificate for the `client_certificate` grant. Other origins
  still get no CORS headers. Conformance: `test_http.py`'s `own_server` tests (an allowed
  origin's answers and preflight, another origin's), with `WARP_SQL_TEST_SERVER_CORS_FLAG`
  naming a server's own flag (Hardcharger: `--cors-origins`).

- SDK: `ServerOptions::plain_handler` lets an application answer GET and HEAD requests
  outside `/warp` on the main listener (Hardcharger serves its web UI at `/ui` with it), before
  the server's own 404, with the request's headers (`PlainRequest::Header`). A HEAD answer carries the GET's headers and Content-Length and no
  body. Paths are decided after decoding (`/%77arp`, `//warp` are the protocol's and never
  reach it), other methods never do, the health-only port never consults it, and CORS
  headers are added as to every answer. `warpsql-sdk-tests` covers it, over mutual TLS too.
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

### Changed (before the 1.0.0 shape fixes)

- SDK: compressed results compress only the buffers worth it. A buffer of 256 KiB or more
  is judged by compressing 48 KiB of it (start, middle, end); lz4_frame wants that sample
  to lose 55% (fast links, where CPU limits), zstd 10%. Buffers not compressed are sent by
  reference behind their -1 length instead of being copied by Arrow's writer, and LZ4
  frames use independent 4 MiB blocks (LZ4F from Arrow's bundled LZ4; `lz4frame.h` is
  vendored). On two c8gn.8xlarge over a 100 Gbit/s LAN, SF10 lineitem in 32 partitions:
  1.00 s uncompressed, 0.99 s with plain lz4_frame, 0.85 s now.
- SDK: a split result holds its first batches back until its size shows: when as many
  batches wait as the client reads partitions at once, it gets that many partitions; when
  it ends first, one per two batches; when a batch has waited 100 ms, one per batch
  waiting. Before, every result of two batches or more got all the partitions asked for.
  Conformance: the empty-partitions test no longer assumes a server makes more
  partitions than batches.
- SDK: accepted connections set TCP_NODELAY. Answers go out in more than one write, and
  Nagle's algorithm held the later ones for the client's delayed ACK: every small request
  (discovery, a status poll, `SELECT 1`) took 40 ms on Linux, over a LAN as on loopback.
  It also made the drivers' `auto` compression see a 40 ms round trip on a LAN and pick
  zstd. Conformance: `test_small_requests_answer_without_waiting_for_acks` (20 discovery
  requests on one connection, median under 20 ms).
- C++ client: `StatementOptions::ipc_compression` (the header's value) replaces
  `accept_zstd`, and every partition's request sends it (partitions 1 and up of a split
  result came back uncompressed before).
- SDK: `Backend::OpenSession(principal, session_id, options)` is given the session's id
  (spec §2.3), the one its client holds, and the DuckDB backend passes it on to
  `NewSession(principal, session_id)`. An engine that shows or takes session ids (a
  sessions table, a command that ends a session, logs) uses that id, so there is one id per
  session and a client finds its own session without translating; Hardcharger used to
  make up a second one.

- With `autocommit` false, a SQL `COMMIT` or `ROLLBACK` now starts the next transaction,
  as the commit and rollback calls do (spec §5.5). The reference server used to leave
  manual-commit mode silently after one.

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

- Spec: bind parameters spelled out (§6.1: values never spliced into SQL; positional or
  named; how `null` is typed); response headers (§2.6); `result_not_ready`; more error
  codes (`not_found`, `method_not_allowed`, `payload_too_large`, `headers_too_large`,
  `statement_cancelled`, option errors).

- Server SDK design: Hardcharger targets DuckDB 2.0's v2 C API only (the RBAC plan
  inspector is the one module allowed DuckDB internals), runs as a standalone server,
  and keeps statement execution off HTTP request threads. IPC is done by Arrow C++ built
  with IPC only, not nanoarrow. ADBC client: `gizmodata/warp-sql-adbc` (Go).

### Fixed

- The SDK builds with clang against libstdc++ 14: `cpp/tools/openapi_json.cc` (which turns
  `spec/openapi.yaml` into the served JSON at build time) kept a mapping's entries as
  `std::vector<std::pair<std::string, Node>>` inside `Node` itself, and libstdc++ 14 refuses a
  `std::pair` of an incomplete type with a static assertion (GCC and libc++ accepted it). Keys
  and values are now two vectors; the generated document is byte for byte the same.
- A retained result is expired from its `expires_at` on (spec §6.5): the SDK refused new reads
  only after its next sweep, up to 5 seconds later, and served them in the meantime. Found
  by the site's examples; `test_a_result_is_expired_from_its_expires_at_on` reads two results
  0.2 s after they expire.
- An idempotent replay of a submit or execute (spec §6.7) carries `Warp-Statement-Id`, as
  every statement response does (§2.6); the SDK left it out. Found by the OpenAPI check.
- Spec §6.10 names the partition read in full (`GET …/statements/{statement_id}/result`),
  as §6.4 does; it wrote `…/result`, where `…` stood for something else than everywhere else.
- SDK: a retained result that expires while it is being read no longer cuts that read.
  The sweep (every 5 s) dropped the result's batches at once, so a read in progress found
  them gone at its next batch and the server ended the body without the end-of-stream
  marker (spec §6.6's signal for a failed statement) for a statement that had succeeded;
  the client saw "incomplete chunked read", and the statement document then said
  `succeeded`. Hardcharger's Windows CI hit it with a 2 s retention polled every 0.5 s.
  Expiry now applies to reads that start afterwards (`410 result_expired`, §6.5); a read
  that has started runs to the end, and the batches (and their quota) are freed when the
  last such read ends. A partition's reads are counted, not flagged, so one of two reads
  of a retained partition ending no longer frees the batches under the other (which would
  then have waited forever), and a response whose stream never starts (the client went
  before the headers were written) still ends its read. Deleting the statement or ending
  its session still ends a read in progress incomplete (§6.6, §6.8). SDK tests:
  `RetainedResult.*` drive the expiry between a read's batches.

- Bulk ingest (spec §8.1): an Arrow type the reference engine can't keep is refused with
  `415 unsupported_arrow_type` before any table is touched; `decimal256` (any precision:
  DuckDB decimals are 128-bit, at most 38 digits) and dense unions, at the top level or
  nested in a list, struct or dictionary, were a `400 sql_error` from inside DuckDB
  ("Unsupported Internal Arrow Type"). The DuckDB backend now lets through only the types
  DuckDB's Arrow scan reads and refuses any other, a type Arrow adds later included.
  Conformance: `test_types_the_engine_cannot_keep_are_refused_with_415` (six columns) and
  `test_a_refused_type_appends_nothing`.

- DuckDB backend: preparing a query no longer runs it. A prepare learned a SELECT's
  result schema by executing it with NULL parameters and dropping the result unread, on
  the claim that this ran nothing; but DuckDB's threads could already have picked the plan
  up, and the drop then waited in `Executor::CancelTasks()` for the whole query (a prepare
  of a long cross join stalled for minutes, about once in 150-1000 prepares with several
  sessions busy). The schema now comes from `duckdb_v2_statement_bind` (nothing runs)
  converted by DuckDB's own Arrow exporter (`duckdb_v2_arrow_exporter_create`), the
  conversion every result uses, under the session's settings (`arrow_lossless_conversion`,
  `produce_arrow_string_view`, `arrow_output_list_view`, `arrow_large_buffer_size`,
  `arrow_output_version`, `TimeZone`). The exporter needs a client context, which the v2
  C API hands out only in callbacks, so the conversion runs in the bind callback of a new
  table function, `warp_result_schema()`, bound and never executed (called directly it is
  refused, as `warp_upload()` is). Conformance: `test_prepare_describe.py`: 4000 prepares
  of hours-long cross joins in 16 sessions at once, each under a second (failed 5 of 5 runs
  before), and the described schema equal, metadata included, to the executed one for
  every DuckDB type (`test_all_types()` and more) under seven setting combinations.
- DuckDB backend: a prepared `INSERT`, `UPDATE`, `DELETE` or `MERGE` with `RETURNING`
  describes its rows' schema (it answered `result_kind: rows` with no `result_schema`),
  from the same bind and Arrow exporter, never by executing the write. A `RETURNING` of
  one BIGINT column named `Count` binds exactly like a write's changed-rows count, so the
  statement's tokens decide it (a top-level `RETURNING` keyword; one in a string, a
  quoted name, a comment or a CTE doesn't count). Conformance: schema parity with the
  executed `RETURNING` under three setting combinations, the table unchanged by the
  prepares (in an explicit transaction too), and the negative twin, writes without
  `RETURNING` described as updates.
- DuckDB backend: the session option `arrow_lossless_conversion` reported the value last
  set through the option, not the one the session's results use: after SQL `SET SESSION
  arrow_lossless_conversion = true`, or an administrator's plain `SET` (which for this
  setting is DuckDB's `SET GLOBAL`) in the same session, it still said `false`. It is now
  read from the session's own Arrow export whenever the session document's options are
  read again (after the session's SET/USE/ATTACH statements and option changes; another
  session's `SET GLOBAL` shows at the next of those).

- A server stopped during a one-request bulk ingest (`POST …/ingest`) whose body was still
  arriving never finished stopping: `Server::Stop()` joined the engine threads after the
  I/O threads, and the engine thread loading the upload waited for bytes only the stopped
  I/O side could deliver. Closing a session (`Stop()` closes every one) now aborts its
  uploads, as it already aborted partitioned ingests' parts; the load fails and rolls back.
- A statement that failed before its answer started, after it had buffered rows, was
  answered `200` with a `failed` statement document (the answer a statement without rows
  gets) instead of its error response. A streamed request that asks for partitions waits
  up to 5 s for the split decision, so with partitions (the drivers' default) any query
  failing in that time got it. It is now the error response (`500 execution_error`, …),
  as spec §6.1 now says outright; §6.2 adds that a `failed` or `cancelled` document is an
  error whatever the status code. Conformance:
  `test_a_failure_before_the_answer_is_an_error_response`, and the mid-stream test accepts
  only an incomplete stream or the error response, with the statement's own error.
- Conformance: `test_an_idle_session_is_not_found_and_never_recreated` probed every
  1.5 s, and each probe is a request that keeps the session alive, so it found the session
  evicted only if the server's sweep (every 5 s in the SDK) fell in the 0.5 s between a
  probe and the timeout; it failed about 1 run in 40 (macOS, TSan). The silences between
  probes now double (2, 4, 8, 16 s), so one outlasts the timeout plus the sweep interval.

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
- The answer to an upload or a part sent again with its `Idempotency-Key` (§8.1, §8.3) is
  no longer lost on Linux and macOS. The server answers it from the first upload without
  reading the body, and then closed the connection with the body unread, which resets it:
  a client still sending got a broken pipe or a reset instead of the answer, and had to
  send it again, with the same risk each time (warp-sql-adbc's
  `TestIntegrationIngestPartAnswerLost` saw a part sent three times; an ingest whose
  retries were all lost would fail). The lingering close that Windows already had (stop
  sending, then read and drop what the client still sends, for up to 2 s) now applies on
  every platform, which also lets clients read a 413, a 431 or an ingest error sent before
  the body was read. No row was ever written twice: the retries were answered from the
  first upload. Conformance: `test_the_answer_to_a_part_sent_again_arrives`,
  `test_the_answer_to_an_upload_sent_again_arrives` (both failed before the fix).
- A request that reached a session as the idle sweep (§5.7) closed it no longer runs in
  the closed session. The sweep checked for idleness and closed the session in two steps,
  so a request could find the session and touch it in between; its statement then ran in
  a session being torn down, and a streamed result was cut mid-body (a Windows
  conformance run: `peer closed connection without sending complete message body`, on a
  `SELECT 1` submitted just as the session went idle). The check and the close are now one
  step under the session's lock, ordered against the request's touch: a request either
  keeps the session alive or gets `404 session_not_found`. A session closed for any reason
  (idle, `KILL SESSION`, `DELETE`) also refuses new statements and ingests from requests
  that found it just before. SDK test: `SessionLifetime.*` in `warpsql-sdk-tests`.

### Documentation

- `cpp/README.md` says `warp-duckdb` is for testing and development only: whoever signs in
  is the administrator with all of DuckDB's file, network and extension access, and it has
  no users, access control or file guards. Production use is Hardcharger.

[Unreleased]: https://github.com/gizmodata/warp-sql/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/gizmodata/warp-sql/releases/tag/v1.0.0
