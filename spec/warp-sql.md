# Warp SQL Protocol, version 1

**Status:** Draft 0.1.0 (2026-10-16). Nothing here is frozen; every section can change
until 1.0.0 is tagged.

Warp SQL is an open protocol for running SQL against a database server over plain
HTTP, with results and bulk data carried in the
[Apache Arrow IPC stream format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
It does for HTTP what Arrow Flight SQL does for gRPC: a small set of operations that
any engine can implement and any HTTP client can call.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be read as described
in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

---

## 1. Goals and non-goals

**Goals**

- **Any HTTP client can talk to it.** A browser `fetch`, `curl`, or any language's
  standard HTTP library is enough. No gRPC, no protobuf, no code generation.
- **Arrow all the way.** Query results, query parameters and bulk-ingest uploads
  are Arrow IPC streams (`application/vnd.apache.arrow.stream`). Control messages
  are small JSON documents.
- **Long queries survive real networks.** A query can be submitted, left running,
  polled and fetched later, and a dropped result download can resume where it
  stopped. No request needs to stay open longer than a proxy allows.
- **Short queries stay short.** One request can run a query and stream its result.
- **Safe by construction.** Every handle a server gives out (session, statement,
  prepared statement) is checked against the caller's identity on every use.
- **Engine-neutral.** Nothing in the protocol depends on one database engine.

**Non-goals**

- **Administration.** Anything that can be done in SQL (users, grants, killing a
  session, settings) is done in SQL through this protocol, not with extra
  endpoints.
- **Defining SQL.** The SQL dialect is the engine's.
- **Replacing ADBC, JDBC or ODBC.** Those are client APIs; Warp SQL is a wire
  protocol their drivers can speak.

## 2. Conventions

### 2.1 Transport

- Servers MUST support HTTP/1.1 and MAY support HTTP/2 and HTTP/3. Every operation
  MUST work over HTTP/1.1 through an ordinary reverse proxy. Nothing in the protocol
  relies on HTTP/2 streams, trailers or server push.
- Servers MUST offer TLS, and SHOULD refuse plain HTTP except on a loopback address.
  Servers MAY require client certificates (mutual TLS) in addition to the
  authentication in §4.
- All endpoints live under a base path. This document writes it as `/warp`; a
  server MAY mount the protocol under another prefix and MUST say so in its
  documentation. Version 1 operations are under `/warp/v1`.

### 2.2 Media types

| Content | Media type |
|---|---|
| Control messages | `application/json` (UTF-8) |
| Errors | `application/problem+json` ([RFC 9457](https://www.rfc-editor.org/rfc/rfc9457)) |
| Results, parameters, uploads | `application/vnd.apache.arrow.stream` |

An Arrow body is always one complete IPC *stream*: a schema message, zero or more
record batches (with any dictionary batches they need), and the end-of-stream marker.
A stream that ends without the end-of-stream marker is **incomplete** (§6.6).

Arrow schemas embedded in JSON (for example a prepared statement's parameter schema)
are an IPC stream containing only the schema message and the end-of-stream marker,
base64-encoded (standard alphabet, with padding).

### 2.3 Identifiers

Session, statement and prepared-statement ids are opaque strings chosen by the
server, at most 128 characters from `[A-Za-z0-9._~-]`. Servers MUST generate them
with at least 128 bits of randomness. Ids are not credentials: servers MUST still
check the caller on every use (§9.2).

### 2.4 JSON

Field names are `snake_case`. Both sides MUST ignore JSON fields they do not
recognise, so later minor versions can add fields. Timestamps are RFC 3339 strings
in UTC (`2026-10-16T21:04:05.123Z`). Durations are integers with the unit in the
name (`timeout_ms`, `idle_timeout_s`).

### 2.5 Request headers defined by this protocol

| Header | Used on | Meaning |
|---|---|---|
| `Authorization` | all except §3 | `Bearer <access token>` (§4) |
| `Idempotency-Key` | submit, execute, ingest | Makes a retried request safe (§6.7) |
| `Prefer` | submit, execute, status | `wait=<seconds>` and `respond-async` ([RFC 7240](https://www.rfc-editor.org/rfc/rfc7240)) |
| `Warp-Statement-Timeout` | submit, execute | Milliseconds before the server cancels the statement |
| `Warp-Retain-Result` | submit, execute | `true` or `false`: keep the result for re-reading (§6.5) |
| `Warp-Query-Tag` | submit, execute, ingest | A JSON object recorded with the statement for observability |
| `Warp-IPC-Compression` | any request that returns Arrow | `zstd` or `none`: Arrow buffer compression the client accepts (§8) |

Servers MUST accept request headers totalling at least 16 KiB (identity tokens can
be large) and MUST answer a larger header block with `431 Request Header Fields Too
Large` (code `headers_too_large`). The limit SHOULD be configurable.

### 2.6 Response headers defined by this protocol

| Header | On | Meaning |
|---|---|---|
| `Warp-Request-Id` | every response | The `request_id` of this request (§11), also in the server's logs |
| `Warp-Statement-Id` | statement responses | The statement the response belongs to |
| `Warp-IPC-Compression` | Arrow responses | The Arrow buffer compression used: `zstd` or `none` (§8.2) |

## 3. Discovery and health

These endpoints need no authentication.

### 3.1 `GET /warp`

```json
{
  "protocol": "warp-sql",
  "protocol_versions": ["1"],
  "draft": "0.1.0",
  "server": {"name": "GizmoSQL", "version": "2.0.0"},
  "capabilities": ["prepared_statements", "transactions", "ingest", "result_retention",
                   "long_poll", "ipc_compression_zstd", "oauth_browser"],
  "auth": {"grant_types": ["password", "token_exchange"]}
}
```

`capabilities` lists the optional features (§10) the server supports. A client MUST
NOT use an optional feature the server does not list.

### 3.2 `GET /warp/v1/health`

`200` with `{"status": "serving"}` when the server accepts new work, otherwise `503`
with `{"status": "not_serving", "reason": "draining" | "starting" | "check_failed"}`.
A server MAY run a configurable health query to decide; if that query hangs, the
endpoint MUST still answer (`503`, `check_failed`) within a bounded time.

## 4. Authentication

### 4.1 `POST /warp/v1/auth/token`

Exchanges a credential for an access token. The credential travels in a JSON body,
never in a URL.

```http
POST /warp/v1/auth/token
Content-Type: application/json

{"grant_type": "password", "username": "joe", "password": "s3:cret"}
```

```json
{"access_token": "eyJhbGciOi…", "token_type": "Bearer", "expires_in": 3600}
```

Grant types:

- `password`: `username` and `password`. Because they are separate JSON fields, a
  password may contain any character, `:` included.
- `token_exchange`: `subject_token`, a token from an identity provider the server
  trusts (for example an OpenID Connect ID token). The server validates it and
  issues its own access token.
- `oauth_browser` (optional capability): see §4.3.

The access token's format is the server's choice; clients MUST treat it as opaque.
A token MAY be valid only on the server instance that issued it or across a cluster;
that is server policy.

Failures return `401` with code `invalid_credentials`. The error MUST NOT say which
part of the credential was wrong.

### 4.2 Using the token

Every other request carries `Authorization: Bearer <access token>`. A missing,
malformed, expired or forged token gets `401` with code `invalid_token` and a
`WWW-Authenticate: Bearer` header. Servers MUST NOT read credentials from cookies,
which keeps the protocol immune to cross-site request forgery by construction.

### 4.3 Browser sign-in (optional capability `oauth_browser`)

For clients that cannot handle a redirect themselves (command-line tools, desktop
drivers):

1. `POST /warp/v1/auth/oauth/flows` returns `{"flow_id", "authorize_url",
   "poll_interval_s", "expires_at"}`.
2. The client opens `authorize_url` in a browser; the user signs in with the
   identity provider.
3. The client polls `GET /warp/v1/auth/oauth/flows/{flow_id}`, which returns
   `{"state": "pending"}`, then `{"state": "complete", "access_token", "token_type",
   "expires_in"}` once, or `{"state": "failed", "error"}`.

A completed flow's token is returned exactly once; later polls get `404`.

## 5. Sessions

A session is the server-side state of one client connection: current catalog and
schema, settings, temporary objects, an open transaction. Every statement runs in a
session.

### 5.1 `POST /warp/v1/sessions`

```json
{"options": {"catalog": "lake", "schema": "analytics", "autocommit": true}}
```

`201` with:

```json
{
  "session_id": "s_8f3k…",
  "options": {"catalog": "lake", "schema": "analytics", "autocommit": true},
  "idle_timeout_s": 900
}
```

The session belongs to the principal the access token identifies (§9.1), for its
whole life.

### 5.2 `GET /warp/v1/sessions/{session_id}`

Returns the session document above, with the session's current options: a change
made through SQL (`USE other_schema`) shows here too. Clients MAY use it as a cheap
liveness probe.

### 5.3 `PATCH /warp/v1/sessions/{session_id}`

Sets options. Body `{"options": {…}}`. The response reports each option:

```json
{"options": {"catalog": "lake"},
 "errors": {"schema": {"code": "invalid_option_value", "detail": "schema 'x' does not exist"}}}
```

Standard options:

| Option | Type | Meaning |
|---|---|---|
| `catalog` | string | Current catalog |
| `schema` | string | Current schema |
| `autocommit` | boolean | `false` starts a transaction that §5.5 ends |
| `session_tag` | object | JSON recorded with the session for observability |

A server MAY support more options; names it does not support get
`unknown_option`.

### 5.4 `DELETE /warp/v1/sessions/{session_id}`

Ends the session, cancels its running statements, and frees its retained results.
`204`.

### 5.5 Transactions

With `autocommit` false, `POST /warp/v1/sessions/{session_id}/commit` and
`POST /warp/v1/sessions/{session_id}/rollback` end the current transaction (`204`)
and the next statement starts a new one. SQL transaction statements (`BEGIN`,
`COMMIT`, `ROLLBACK`) also work, as the engine allows. Capability: `transactions`.

### 5.6 Sessions that no longer exist

A request naming a session the server does not have gets `404` with code
`session_not_found`, whether the session was closed, evicted for idleness, never
existed, or belongs to someone else. A server that knows the session lives on
another instance of the same cluster SHOULD answer `421 Misdirected Request` with
code `session_elsewhere`, so a router can retry the right instance.

**A server MUST NOT create a session implicitly for an unknown id.** The client's
state (settings, temporary tables, an open transaction) is gone, and only the
client can decide whether to start over. Drivers SHOULD create a new session, replay
the options they set, and report the loss of temporary state.

A session the server ended for a reason the client must know about gets `401` with
code `session_ended` and a `reason`:

| `reason` | When |
|---|---|
| `killed` | An administrator ended it (e.g. SQL `KILL SESSION`) |
| `credentials_changed` | The principal's password or credentials changed |
| `principal_dropped` | The principal no longer exists |
| `token_expired` | The access token used for the request has expired |

After `session_ended` the client MUST get a new access token before creating a new
session.

### 5.7 Idle sessions

A session with no request and no running statement for `idle_timeout_s` MAY be
evicted. A running statement, or a retained result not yet expired, keeps a session
alive.

## 6. Statements

### 6.1 Submit: `POST /warp/v1/sessions/{session_id}/statements`

```http
POST /warp/v1/sessions/s_8f3k…/statements
Authorization: Bearer eyJ…
Content-Type: application/json
Accept: application/json
Prefer: wait=5
Idempotency-Key: 7c1e0a52-…

{"sql": "SELECT region, sum(amount) FROM sales WHERE year = ? GROUP BY region",
 "parameters": [2026]}
```

`parameters` (optional) holds **bind parameters**, which are values, never SQL text:
servers MUST bind them through the engine's parameter mechanism and MUST NOT splice them
into the statement. It is either an array, binding positionally (`?`, or `$1`, `$2`, … in
engines that number them), or an object, binding by name (`$name` in engines that support
named parameters):

```json
{"sql": "SELECT * FROM sales WHERE region = ? AND year = ?", "parameters": ["EU", 2026]}
{"sql": "SELECT * FROM sales WHERE region = $region", "parameters": {"region": "EU"}}
```

Values are JSON scalars: numbers, strings, booleans and `null`, each typed as the engine
infers for that parameter. A `null` takes the type the engine infers for its parameter;
where the statement doesn't fix one (`? + 1`), the client casts in SQL
(`CAST(? AS INTEGER)`), as in other SQL databases. For exact types (decimals, timestamps,
binary, nested values) use a prepared statement with Arrow parameters (§7).

How the server answers depends on `Accept` and `Prefer`:

- **Streamed (one round trip).** With `Accept: application/vnd.apache.arrow.stream`
  and no `respond-async`, a query's result streams in the response body: `200`,
  `Content-Type: application/vnd.apache.arrow.stream`, and `Warp-Statement-Id`
  naming the statement. A statement that returns no rows (DML, DDL) answers
  `200` with a JSON statement document (§6.2) instead; clients MUST check
  `Content-Type`.
- **Status document.** Otherwise the server answers `201` with the statement
  document (§6.2) and `Location: /warp/v1/sessions/{session_id}/statements/{statement_id}`.
  With `Prefer: wait=N` the server MAY hold the response up to N seconds for the
  statement to finish; with `Prefer: respond-async` it SHOULD answer as soon as the
  statement is accepted. Without either, the wait is the server's choice.

An error found before execution starts (a syntax error, a permission denial) is an
error response (§11), not a statement document.

### 6.2 The statement document

```json
{
  "statement_id": "q_19dd…",
  "state": "running",
  "submitted_at": "2026-10-16T21:04:05.123Z",
  "started_at": "2026-10-16T21:04:05.130Z",
  "progress": {"fraction": 0.42, "rows_produced": 1048576},
  "result": null
}
```

`state` is one of `queued`, `running`, `succeeded`, `failed`, `cancelled`. The last
three are terminal. `progress` is optional and every field in it is optional;
`fraction` is between 0 and 1.

A failed or cancelled statement carries `error`, a problem document (§11), and a
cancelled one also carries `cancel_reason`: `client`, `timeout`, `session_closed`,
`killed`, `privilege_revoked` or `server_shutdown`. A cancelled statement's error code
is `statement_timeout` when its timeout ran out and `statement_cancelled` otherwise.

A succeeded statement carries `result`:

```json
"result": {
  "kind": "rows",
  "schema": "<base64 Arrow schema>",
  "partitions": [
    {"index": 0, "location": null, "batches": 12, "rows": 98304}
  ],
  "retained": true,
  "expires_at": "2026-10-16T22:04:05Z"
}
```

or, for a statement that returns no rows:

```json
"result": {"kind": "update", "rows_affected": 42}
```

`rows_affected` is present when the engine counts the rows a statement changed (INSERT,
UPDATE, DELETE) and absent otherwise (DDL, SET, …); clients report it as unknown.

`partitions` lists the pieces of the result. Most results have one partition with
`location: null`, meaning "this server". A server MAY split a result into several
partitions, and a partition's `location` MAY be the base URL of another server that
holds it (capability `partitioned_results`); the client fetches each partition from
its location with the same access token and may do so in parallel. `location` is an
absolute URL of the same form as a server's base (`https://worker-3.example:31337/warp`),
and the partition is read at
`{location}/v1/sessions/{session_id}/statements/{statement_id}/result?partition={index}`
with the coordinator's session and statement ids (whether it carries its own token
instead is §A.4). The order of rows
across partitions is undefined unless the query's `ORDER BY` fixes it, in which case
the server MUST return a single partition. `batches` and `rows` are known only once
the partition is complete and MAY be `null` before that.

### 6.3 Status: `GET /warp/v1/sessions/{session_id}/statements/{statement_id}`

Returns the statement document. With `Prefer: wait=N` (capability `long_poll`) the
server MAY hold the response until the state changes or N seconds pass, whichever is
first. A statement that has finished (`succeeded`, `failed`, `cancelled`) can't change,
so the server MUST answer at once.

### 6.4 Results: `GET …/statements/{statement_id}/result`

Query parameters: `partition` (default 0) and `from_batch` (default 0).

Returns `200` with an Arrow stream of that partition's record batches, starting at
batch number `from_batch` (counting from 0). The stream always begins with the
schema message, which servers SHOULD send as soon as the schema is known rather than
with the first batch: an aggregate produces its first batch only at the end, and a
response that stays silent until then is cut by proxies' idle timeouts. A client MAY request the result while the statement is still
running; the server streams batches as they are produced. If the statement has not
produced its result's schema within the server's wait limit, the answer is `409` with
code `result_not_ready`, and the client retries. A failed or cancelled statement's
result request gets the statement's own error; a statement that returns no rows gets
`400`, `bad_request`.

### 6.5 Retention and resumption

A **retained** result can be read again, and from any batch, until it expires; an
unretained result can be read once, from batch 0, while the statement's session
lives.

- `Warp-Retain-Result: true|false` on submit chooses; without it the server decides
  (servers SHOULD retain for `respond-async` submissions and SHOULD NOT for
  streamed submissions). The statement document says what the server chose
  (`retained`) and until when (`expires_at`).
- Re-reading an unretained result, or `from_batch` greater than 0 on one, gets `409`
  with code `result_not_resumable`.
- An expired result gets `410 Gone` with code `result_expired`.
- Retained results are a resource the server MAY limit per principal; a statement
  that would exceed the limit fails with `429` and code `result_quota_exceeded`.
- If the client disconnects while reading an **unretained** result, the server
  SHOULD cancel the statement (`cancel_reason: client`), including while the stream
  is waiting for the statement's next batch. A retained result's statement keeps
  running, and so does one that changes data (`INSERT … RETURNING`, …): the server
  MUST NOT undo a write because its reader went away; it runs to the end with its
  rows discarded.
- A finished statement is not kept forever. A server MAY forget one once at least 5
  minutes have passed since it finished and its retained result, if any, has expired,
  or sooner when the session holds more than 1,000 finished statements (oldest first);
  requests for a forgotten statement get `404` (`not_found`). Clients that are done
  with a statement SHOULD delete it (§6.8).

### 6.6 Errors during a result stream

Once a `200` response with an Arrow body has started, the status code cannot change.
If the statement fails mid-stream, the server ends the body **without** the Arrow
end-of-stream marker (in HTTP/1.1, by closing the connection or sending the final
chunk early). A client that reads an incomplete stream MUST treat the result as
failed and SHOULD fetch the statement document for the error. A complete stream
always ends with the end-of-stream marker.

### 6.7 Idempotent submission

A client SHOULD send `Idempotency-Key` (a unique string, at most 255 characters) on
every request that creates a statement or writes data. If the server has already
accepted a request with the same key from the same principal within its
idempotency window (at least 24 hours, RECOMMENDED), it MUST NOT run it again: it
answers with the existing statement (the statement document with `200`, or for
ingest the original response). A reused key with a different request body gets
`422` with code `idempotency_key_reused`. This makes it safe to retry a request
whose response was lost, including DML.

### 6.8 Cancel and close

- `POST …/statements/{statement_id}/cancel`: `202`. Cancelling a queued statement
  removes it from the queue; it never starts. Cancelling a finished statement is a
  no-op (`202`).
- `DELETE …/statements/{statement_id}`: the client is done with the statement. Its
  result is freed and its id forgotten (`204`). A query still running is cancelled; a
  statement that changes data runs to the end with its rows discarded, as when its
  reader goes away (§6.5). To stop a write, cancel it.
- `POST /warp/v1/sessions/{session_id}/cancel`: cancels every statement of the session
  that is queued or running when the request arrives; `202` with
  `{"cancelled": ["q_…", …]}` (possibly empty). This is for a client that has no
  statement id yet, because its submit's answer hasn't arrived (`Prefer: wait`, or a
  streamed submit still waiting for its result). A submit that reaches the server
  after the cancel isn't affected.

### 6.9 Timeouts

`Warp-Statement-Timeout: <ms>` limits how long the statement may run. When it
expires the server cancels the statement (`cancel_reason: timeout`, error code
`statement_timeout`). A server MAY enforce a maximum of its own.

## 7. Prepared statements

Capability: `prepared_statements`.

### 7.1 `POST /warp/v1/sessions/{session_id}/prepared`

Body `{"sql": "INSERT INTO t VALUES (?, ?)"}`. `201` with:

```json
{
  "prepared_id": "p_77a…",
  "parameter_schema": "<base64 Arrow schema>",
  "result_schema": "<base64 Arrow schema, or null>",
  "result_kind": "rows"
}
```

`result_kind` says what executing returns: `"rows"` (with `result_schema`, the rows'
schema, when the server can tell it before running), `"update"` (a count, no rows;
`result_schema` is null), or `null` when the server can only tell at execution.
Preparing MUST NOT run the statement.

### 7.2 `POST …/prepared/{prepared_id}/execute`

The body is either JSON `{"parameters": [...]}` (as in §6.1) or an Arrow stream
(`Content-Type: application/vnd.apache.arrow.stream`) whose schema matches
`parameter_schema`. **Each row of the Arrow body is one execution**, so a client can
run a batch of inserts in one request. A query with more than one parameter row
returns the concatenation of each execution's rows, in order. A query run for an Arrow
body with no rows returns no rows (its schema, no batches), not a count.

The response is exactly as for §6.1, with the same headers (`Accept`, `Prefer`,
`Idempotency-Key`, …). Each execution creates a statement.

Servers MUST authorize each execution as the principal executes it, not once at
prepare time: a privilege revoked between prepare and execute applies.

### 7.3 `DELETE …/prepared/{prepared_id}`

`204`.

## 8. Bulk ingest

Capability: `ingest`.

### 8.1 `POST /warp/v1/sessions/{session_id}/ingest`

Query parameters:

| Parameter | Required | Meaning |
|---|---|---|
| `table` | yes | Target table name |
| `catalog`, `schema` | no | Default: the session's current catalog and schema |
| `mode` | no | `create` (fail if it exists), `append` (fail if it does not), `replace`, or `create_append` (default) |
| `temporary` | no | `true` to create a temporary table |

The body is an Arrow stream; it MAY be sent with chunked transfer encoding and any
size. The response is `200` with `{"rows_ingested": 1000000}` once every row is
written. An ingest is one transaction when the engine supports it: on failure, no
rows from the request remain.

The server MUST map each Arrow type in the upload to a column type that preserves
its values, and MUST refuse with `415` and code `unsupported_arrow_type` a type it
cannot preserve. It MUST NOT fall back to storing values as text.

`Idempotency-Key` (§6.7) makes a retried upload safe.

*Deployment note:* reverse proxies commonly cap request bodies (often at 1 MB) and
buffer them. A deployment that offers ingest must lift the body-size limit for this
path and SHOULD disable request buffering, or uploads fail with `413` after the whole
body has been sent.

### 8.2 Compression

Two independent layers, both optional:

- **HTTP content coding.** Standard `Accept-Encoding` / `Content-Encoding`
  (`gzip`, `zstd`, …). Browsers decode it transparently.
- **Arrow buffer compression.** The Arrow IPC format's own per-buffer compression.
  A client sends `Warp-IPC-Compression: zstd` to accept it; the server says what it
  used in the response's `Warp-IPC-Compression` header. Servers with capability
  `ipc_compression_zstd` MUST be able to produce and read it. Uploads MAY use it.

Uncompressed Arrow MUST always be accepted.

## 9. Security

### 9.1 Principals

A **principal** is the identity an access token establishes: a user known to the
server. The server decides what each principal may do (for example with SQL roles and
grants); the protocol only requires that a session's principal is fixed when the
session is created and that every operation runs as that principal.

### 9.2 Handles are bound to their principal

Every session belongs to the principal that created it, and every statement,
prepared statement and retained result belongs to its session. On every request the
server MUST check that the token's principal owns the session named in the URL. A
request for another principal's handle gets the same `404` as a handle that does not
exist, so ids cannot be probed. Possessing an id grants nothing.

Administrative actions on other principals' work (ending their session, cancelling
their query) are done in SQL, under the server's own authorization rules, not
through these endpoints.

### 9.3 Results are governed data

A retained result was produced under its principal's privileges. It MUST only be
readable by that principal. If the server stores retained results outside its own
memory (local disk, object storage), it MUST protect them as it protects the source
data, and MUST delete them when they expire or their session ends.

### 9.4 Metadata follows authorization

The metadata endpoints (§10) MUST return only the objects the principal may see,
applying the same rules as SQL.

### 9.5 Browsers

- Servers MUST NOT send `Access-Control-Allow-Origin: *` together with
  `Access-Control-Allow-Credentials`. Cross-origin access SHOULD be an explicit
  allow-list of origins, off by default.
- Every response MUST carry `X-Content-Type-Options: nosniff`. Responses with query
  results MUST carry `Cache-Control: no-store`.
- Because credentials are bearer tokens in a header (never cookies), the protocol is
  not exposed to cross-site request forgery.

### 9.6 Error messages

Errors SHOULD help the user fix the problem without revealing what they may not see:
a denied query names the privilege missing on objects the user named, not the
existence of objects they cannot see.

## 10. Metadata

These endpoints serve driver metadata calls (ADBC `GetObjects`, JDBC
`DatabaseMetaData`). They are `GET` requests under
`/warp/v1/sessions/{session_id}/metadata/` that return Arrow streams. **Their result
schemas are exactly those of the matching Arrow Flight SQL commands**, so existing
driver code that consumes Flight SQL metadata works unchanged. Filter patterns use
SQL `LIKE` syntax (`%`, `_`, `\` escape), as in Flight SQL.

| Endpoint | Query parameters | Result schema (as Flight SQL) |
|---|---|---|
| `catalogs` | none | `CommandGetCatalogs`: `catalog_name: utf8 not null` |
| `db-schemas` | `catalog`, `db_schema_filter_pattern` | `CommandGetDbSchemas`: `catalog_name: utf8, db_schema_name: utf8 not null` |
| `tables` | `catalog`, `db_schema_filter_pattern`, `table_name_filter_pattern`, `table_types` (comma-separated), `include_schema` | `CommandGetTables`: `catalog_name: utf8, db_schema_name: utf8, table_name: utf8 not null, table_type: utf8 not null` [, `table_schema: binary not null`] |
| `table-types` | none | `CommandGetTableTypes`: `table_type: utf8 not null` |
| `primary-keys` | `catalog`, `db_schema`, `table` (optional: without it, every table) | `CommandGetPrimaryKeys`: `catalog_name: utf8, db_schema_name: utf8, table_name: utf8 not null, column_name: utf8 not null, key_name: utf8, key_sequence: int32 not null` |
| `imported-keys` | `catalog`, `db_schema`, `table` (optional, as for `primary-keys`) | `CommandGetImportedKeys` (13 columns, `pk_catalog_name` … `delete_rule`) |
| `exported-keys` | `catalog`, `db_schema`, `table` | `CommandGetExportedKeys` (same columns) |
| `cross-reference` | `pk_catalog`, `pk_db_schema`, `pk_table`, `fk_catalog`, `fk_db_schema`, `fk_table` | `CommandGetCrossReference` (same columns) |
| `type-info` | `data_type` | `CommandGetXdbcTypeInfo` |

`table_schema` (when `include_schema=true`) is the table's Arrow schema serialized as
an IPC message, with the same per-field metadata keys Flight SQL defines
(`ARROW:FLIGHT:SQL:TYPE_NAME`, `…:PRECISION`, `…:SCALE`, `…:REMARKS`, …). A field is
nullable unless the column can't hold NULL (`NOT NULL`, primary key), and decimal
columns carry `…:PRECISION` and `…:SCALE`. Ordering follows the Flight SQL definitions.

`GET /warp/v1/sessions/{session_id}/metadata/server-info` returns JSON rather than
Flight SQL's `SqlInfo` union: one object whose keys are the Flight SQL `SqlInfo`
names in lower case (`flight_sql_server_name` becomes `server_name`,
`sql_identifier_quote_char`, `sql_transactions_supported`, …) with JSON values. A
server includes the keys it knows. *(Draft note: the full key list is §A.2's job.)*

## 11. Errors

Every error response is a problem document:

```json
{
  "type": "urn:warp-sql:error:permission_denied",
  "title": "Permission denied",
  "status": 403,
  "code": "permission_denied",
  "detail": "SELECT on table lake.raw.events requires a grant you do not have",
  "sqlstate": "42501",
  "request_id": "r_5d2…"
}
```

`code` is the stable, machine-readable part; `detail` is for people and may change.
`sqlstate` (optional) is the SQL standard's five-character class and subclass. Where
the standard has one, servers SHOULD send it, in particular `42P07` (the table already
exists) and `42P01` (the table doesn't exist), which bulk ingest's `create` and `append`
modes meet (§8.1) and which clients map to "already exists" and "not found".
`request_id` SHOULD also appear in the server's logs.

| Code | Status | Meaning |
|---|---|---|
| `bad_request` | 400 | Malformed request |
| `unknown_option`, `invalid_option_value` | 400 | A session option the server doesn't have, or a bad value (§5.3) |
| `sql_error` | 400 | The engine rejected the SQL (syntax, binding, missing object) |
| `invalid_credentials` | 401 | §4.1 |
| `invalid_token` | 401 | §4.2 |
| `session_ended` | 401 | §5.6 |
| `permission_denied` | 403 | The principal lacks a privilege |
| `read_only` | 403 | The server or session is read-only |
| `session_not_found`, `statement_not_found`, `prepared_not_found` | 404 | No such handle, or not the caller's |
| `not_found` | 404 | No such endpoint |
| `method_not_allowed` | 405 | The endpoint exists but not for this method |
| `result_not_ready` | 409 | The statement has no result schema yet (§6.4) |
| `result_not_resumable` | 409 | §6.5 |
| `result_expired` | 410 | §6.5 |
| `payload_too_large` | 413 | A JSON request body over the server's limit |
| `unsupported_arrow_type` | 415 | §8.1 |
| `session_elsewhere` | 421 | §5.6 |
| `idempotency_key_reused` | 422 | §6.7 |
| `headers_too_large` | 431 | §2.5 |
| `too_many_sessions`, `result_quota_exceeded`, `rate_limited` | 429 | A limit was reached (with `Retry-After` when retrying can help) |
| `statement_timeout`, `statement_cancelled` | — | Errors inside a statement document (§6.2, §6.9) |
| `execution_error` | 500 | The statement failed while running |
| `server_draining` | 503 | The server is shutting down; with `Retry-After` |
| `not_implemented` | 501 | The server does not implement this operation |

### 11.1 Graceful shutdown

A server that is shutting down MUST stop accepting new sessions and statements
(`503`, `server_draining`, with `Retry-After`), report `not_serving` on health, let
running statements finish within its grace period, and then cancel what remains
(`cancel_reason: server_shutdown`).

## 12. Versioning

The major version is in the path (`/warp/v1`). Within a major version, changes are
additive only: new endpoints, new optional fields, new capabilities, new error codes.
Clients MUST ignore unknown fields and treat an unknown error `code` by its HTTP
status. A change that would break a v1 client gets a new major version, and a server
MAY serve several majors at once.

---

## Appendix A. Open items before 1.0

1. **Upload resumption.** An interrupted multi-gigabyte ingest restarts from zero.
   Consider an ingest session (create, append chunks with offsets, commit).
2. **Server-info keys.** Fix the full list of `server-info` keys and their types.
3. **Result statistics.** Whether the statement document should carry per-column
   statistics or a query profile handle.
4. **Partition routing.** Whether a partition `location` needs a per-partition
   short-lived token instead of the client's own access token.
5. **Push notification.** Whether long-poll (§6.3) is enough or a server-sent-events
   stream of state changes is worth adding.
6. **Conformance suite.** Every MUST in this document needs a test in the
   conformance suite (`conformance/`); most have one, and the rest are listed there.
