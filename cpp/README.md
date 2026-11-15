# Chargewire server SDK (C++) and reference server

- **`libchargewire`** (`include/chargewire/`, `src/sdk/`): the protocol, over HTTP/1.1 and TLS
  on Boost.Beast with C++20 coroutines. An engine implements `chargewire::Backend` (see
  `include/chargewire/backend.hpp`); the SDK owns authentication, handles bound to their
  principal, retries, retention, streaming, cancellation and draining.
- **`libchargewire_client`** (`include/chargewire/client.hpp`, `src/client/`): the client side,
  synchronous, on Boost.Beast: sign-in, sessions, the streamed and status-document
  statement paths, prepared statements, ingest, metadata, cancellation from another
  thread, TLS and mutual TLS. It needs Arrow, Boost and OpenSSL, not DuckDB. Hardcharger's
  command-line client is built on it.
- **`chargewire-duckdb`** (`src/duckdb/`): the reference server, DuckDB 2.0 through its v2 C API
  (one file, the guard of read-only sessions, uses DuckDB's internal headers). It is what the
  conformance suite and the drivers test against.

> [!WARNING]
> **`chargewire-duckdb` is for testing and development, not production.** It has one password,
> and whoever signs in is the administrator, with all of DuckDB's power: reading and
> writing local files, attaching databases, loading extensions, reaching the network.
> `--read-only` protects only the database file (a session's `read_only` option, below, is what
> makes one session change nothing). It has no users, access control, file
> guards, audit log or metrics. It listens on `127.0.0.1` unless told otherwise; keep it
> there. To serve data to people, run [Hardcharger](https://github.com/gizmodata/hardcharger),
> which is built on this SDK and adds all of that.

## Build

```bash
./scripts/build-duckdb.sh     # DuckDB 2.0 at the pinned commit (about 10 minutes)
./scripts/build-arrow.sh      # Arrow C++ 26.0.1, IPC only (about 30 seconds)
OPENSSL_ROOT_DIR=$(brew --prefix openssl@3) cmake -S . -B build -G Ninja
cmake --build build
```

Boost 1.92 (Beast, JSON, URL) is fetched by CMake. OpenSSL 3 comes from the system.

The DuckDB build script applies a transaction-recovery patch to the pinned engine.
Use that build for the reference server: DuckDB's unpatched release library does not
include the fix. Projects embedding the adapter can provide their own DuckDB build,
but must apply the same patch to obtain this recovery behavior.

DuckDB builds in its core extensions (core_functions, parquet, json, icu); the rest load
at run time. A server that wants more built in lists them in a CMake file of
`duckdb_extension_load()` calls and names it in `CHARGEWIRE_DUCKDB_EXTENSION_CONFIGS`
(several: `;`-separated) when it runs `build-duckdb.sh`.

Windows (x64 and arm64) builds with MSVC from a Developer shell's Git Bash. DuckDB always
builds with the static C runtime (/MT) there, so everything linked with it must too:
build Arrow with `CMAKE_TOOLCHAIN_FILE` naming a file that sets
`CMAKE_MSVC_RUNTIME_LIBRARY` to `MultiThreaded` (this project sets it for itself), and
configure with `-DOPENSSL_ROOT_DIR=<an OpenSSL 3 built with /MT>
-DOPENSSL_USE_STATIC_LIBS=TRUE -DOPENSSL_MSVC_STATIC_RT=TRUE`. DuckDB is linked
statically there. Hardcharger's `.github/workflows/windows.yml` builds and tests it this
way.

## Run

```bash
CHARGEWIRE_DUCKDB_PASSWORD='…' ./build/chargewire-duckdb --database my.duckdb --port 31337
```

The password is read from the environment so it never shows in process listings. TLS:
`--tls-cert` and `--tls-key` (PEM), plus `--tls-client-ca` to verify client certificates
(mutual TLS). With a client CA:

| Flag | Default | Meaning |
|---|---|---|
| `--mtls-require-client-cert=BOOL` | `true` | Refuse connections without a client certificate at the handshake; `false` lets them connect and sign in with the password |
| `--mtls-crl-file PEM` | none | Certificate revocation lists, checked at the handshake for every certificate of the chain; the file is looked at every 5 s and reloaded when it changes, and open connections are re-checked then and when a CRL passes its nextUpdate (spec §4.4). Of several CRLs from one CA the newest is used; a CRL that names a client CA but isn't signed by it, or any delta CRL, fails the load (startup refuses, a reload keeps the last good CRLs) |
| `--mtls-identity-source` | `san_email` | Where the `client_certificate` grant takes the identity: `san_email`, `san_uri` (SPIFFE IDs) or `subject_cn` |
| `--mtls-certificate-sign-in` | off | Offer the `client_certificate` grant; off, client certificates only admit connections. Needs `--mtls-admin-identities` |
| `--mtls-admin-identities A,B` | none | Identities that may sign in with their certificate (as the administrator). A URI identity (`spiffe://…`) matches exactly, case included; an e-mail address or CN ignoring ASCII case |
| `--mtls-crl-allow-missing-ca-crls` | off | Every certificate of a chain is revocation-checked, so the CRL file needs every CA's CRL (the root's too); this accepts a CA above the leaf without one |
| `--mtls-require-identity-match` | off | A password sign-in over a client certificate must be for its identity |

Browser sign-in (spec §4.3) with an OpenID Connect provider, for testing clients: everyone
listed in `--oauth-identities` signs in as the administrator. The ID token is checked for
issuer, audience, expiry and nonce, not by signature (it comes straight from the token
endpoint; an `http://` issuer is accepted only on a loopback address).

| Flag | Default | Meaning |
|---|---|---|
| `--oauth-issuer URL` | none | The provider, found by its `/.well-known/openid-configuration` |
| `--oauth-client-id ID` | none | The server's client id (the secret comes from `CHARGEWIRE_DUCKDB_OAUTH_CLIENT_SECRET`) |
| `--oauth-identities A,B` | none | Who may sign in (the ID token's `email`, else `sub`; ASCII case ignored) |
| `--oauth-scopes S` | `openid email profile` | |
| `--oauth-base-url URL` | `http(s)://HOST:PORT` | The server's public URL; the redirect URI to register is `<it>/chargewire/v1/auth/oauth/callback` |
| `--oauth-redirect-uri URL` | from the base URL | The redirect URI, given whole |
| `--oauth-provider-name NAME` | none | The provider's name in discovery ("Sign in with NAME") |
| `--oauth-max-flows N` | 10000 | Sign-ins held at once |

The `--mtls-*` flags have Hardcharger's option names, so test harnesses start either server
the same way. `--flag=value` works for every flag. The first SIGTERM drains (running work finishes, new work is refused); a
second one stops at once. On Windows, Ctrl+C and Ctrl+Break do the same (a parent stops a
child it started with `CREATE_NEW_PROCESS_GROUP` by sending it Ctrl+Break); closing the
console stops it at once.

## Test

The client library's tests (unit, and live against `chargewire-duckdb`, which they start), the
SDK's own, and the DuckDB backend's (each of the last two starts a server in the test process):

```bash
cmake -S . -B build -G Ninja -DCHARGEWIRE_BUILD_TESTS=ON && cmake --build build
./build/chargewire-client-tests
./build/chargewire-sdk-tests
./build/chargewire-duckdb-tests
```

The conformance suite:

```bash
cd ../conformance
uv venv .venv && uv pip install --python .venv/bin/python -e .
CHARGEWIRE_TEST_SERVER_BINARY=../cpp/build/chargewire-duckdb .venv/bin/python -m pytest -q
```

### Read-only sessions

The `read_only` session option (spec §5.8) makes a session that changes nothing and cannot be
made read-write again. The SDK owns the option: it parses it, keeps the one-way rule, puts
`read_only` into every session document itself and refuses bulk ingest. The engine gets one
call, `EngineSession::SetReadOnly()`, and from then on refuses with `ErrorCode::kReadOnly`
whatever would change something. A backend lists `BackendCapabilities::read_only_sessions` only
if it enforces that; without it the SDK answers `unknown_option` for `read_only` and never
passes the option on, so no server built on the SDK reports a session read-only that is not.

`chargewire-duckdb`'s adapter (`src/duckdb/`, which a server can build on) is built on the
spec's rule that what is checked is what runs (`src/duckdb/duckdb_read_only.hpp`). DuckDB plans
a statement once and hands the plan it is about to execute to the session's guard, which
refuses it or lets that same plan run; nothing is planned twice, and nothing of a statement
runs before its plan has passed. A read-only session is held by three things:

1. Every transaction of the session is a DuckDB `READ ONLY` transaction that the adapter
   starts; the client's `BEGIN`, `COMMIT` and `ROLLBACK` are run by the adapter and never
   reach DuckDB, so no statement opens a transaction that writes.
2. Only listed kinds of statement are handed to DuckDB at all (its parser names the kind,
   before anything is bound): queries, `EXPLAIN`, transaction statements, `SET`/`RESET`/`USE`
   and `PRAGMA`. Everything else is refused: writes, temporary objects, `COPY`, `EXPORT`,
   `ATTACH`, `INSTALL`, `LOAD`, `CALL`, `CHECKPOINT`, SQL `PREPARE` and `EXECUTE`, and a
   `PIVOT` that takes its values from the data (DuckDB runs a query for the values before it
   plans the pivot; `PIVOT … ON x IN (…)` with the values written out is a query).
3. The guard: a plan runs only when DuckDB's planner says it modifies no catalog (the
   temporary one included, so a sequence is never advanced) and every operator, table
   function, setting and pragma in it is listed. The plan is the one DuckDB made of
   everything the statement reaches: views, macros, functions that take SQL text. A setting
   is applied to the session only, however the statement spells it; `SET GLOBAL` is refused.

All of these are allow-lists (`ReadOnlyPolicy`): a kind of statement, an operator or a table
function that is not listed is refused, also one that a later DuckDB or an extension adds. The
error says which name was refused. A server that embeds the adapter adds the table functions it
knows to read (`DuckDBSession::set_read_only_policy`, before the session becomes read-only).
`chargewire-duckdb` itself has no setting to add names.

Before DuckDB binds a statement, the adapter screens the parser's tree
(`ScreenBeforeBinding`): binding itself evaluates the arguments of table functions, the value
of a setting, the parameters of a pragma, the file name and the options of a `COPY` and the
version or time a table is read `AT`, before any plan exists to refuse, and what a function
did there is done (a sequence advanced or set stays so). The screen is an allow-list too:

- It walks the whole statement itself and decides for every kind of statement, query, table
  reference and expression by name; a kind it has no rule for is refused. What an `EXPLAIN`
  holds is walked as the statement it is: a query, a `COPY … TO`, a `CALL`, a setting. Every
  other kind is refused under an `EXPLAIN` as without one: the ones that change a catalog
  (`INSERT`, `UPDATE`, `DELETE`, `MERGE`, also as a CTE's body), `PREPARE` and `EXECUTE`, and
  the rest. Nothing of them is bound.
- What binding would evaluate holds values only: constants, parameters, columns, casts,
  operators, lists and structs.
- A table function is bound only when it is listed (`table_functions`, or
  `explained_table_functions`: the ones that change something when they run and nothing when
  they are bound, such as `checkpoint()`, which a plain `EXPLAIN` may show and no plan may
  run) or is a table macro of a user's that the adapter has read the definition of. The bind
  step of any other never runs. That keeps out the functions that bind or run SQL a statement
  does not hold (`query()`, `query_table()`, `json_execute_serialized_sql()`, the pass-through
  functions of attached systems), which are refused by name besides; a function that does
  not exist is answered `read_only` like one that is not listed.
- A `PRAGMA` is bound only when it is listed: DuckDB replaces some pragmas by the text of a
  statement, which it then binds.
- `json_serialize_plan()`, a scalar function that plans SQL it is given as text, is refused
  by name.
- The definitions of the views and macros a statement names, and of those these name, are
  screened the same way, whoever defined them. The adapter's own queries call DuckDB's
  functions by their full names (`system.main.duckdb_views()`).

What the screen does not reach is a definition that DuckDB binds and the statement does not
lead the adapter to. A read-only session can still advance a sequence through: a view or
macro that the session's listing of definitions does not show (hidden from the user by a
server's access control, granted after the adapter last read the definitions, or created later
in a catalog without a version, such as a DuckLake); a view created `WITH (DEFER_BINDING)`,
which `duckdb_columns()` binds when it runs; a view with a dot in its name, quoted inside a
string argument; a macro's argument that its definition passes to a table function; and a
macro of a user's named like one of DuckDB's functions, which DuckDB's own text binds in its
place. Each needs a view or macro that someone allowed to create one has made.

A prepared statement (spec §7) is planned, checked and run anew at each execution in a
read-only session, so it resolves names as the session stands then, and DuckDB keeps no plan
for it. A read-only session's transactions are ended by a rollback, never a commit. Becoming
read-only is refused while the session's transaction holds work, by what the adapter noted
and by what DuckDB itself holds (a transaction that has changed something), so no request
sent between the work and the switch makes the adapter lose sight of it. The cost
of all of this is the screen, the guard's walk over the plan and the transaction around each
statement: about 0.05 ms a statement on a laptop, for trivial and for wide queries alike (the
first statement after a view or macro changed anywhere that names a function or a relation
reads the definitions again, about 5 ms).

The guard is the one file that uses DuckDB's internal headers (`src/duckdb/duckdb_read_only_guard.cc`):
the v2 C API shows nothing of a bound statement and cannot run one that was inspected.
`scripts/build-duckdb.sh` installs those headers alongside the patched library.
`scripts/duckdb-headers.sh` can fetch headers alone for the pinned commit; it does not
patch or replace an existing library.

> [!IMPORTANT]
> **Read-only is not a sandbox.** It promises that a session changes nothing through SQL the
> server can classify: statements, and the operators and table functions of their plans. It
> does not see what a *scalar* function does, except that DuckDB's planner says which catalogs
> a statement modifies (so `nextval` is refused). DuckDB's `write_log()`, and a scalar
> function of an extension that an administrator installed and that writes or sends something
> on its own, are not refused. DuckDB also runs the bind step of a table function before the
> guard sees a plan: only listed functions get that far (and the one DuckDB puts in place of
> a file's name, and a `COPY`'s format under an `EXPLAIN`), and those of DuckDB and of the
> extensions the tests cover change nothing there. Read-only says nothing about what a session may read or which hosts a query may
> contact (that is access control), and an extension that DuckDB loads on demand for a query
> is loaded for a read-only session too.

### Ingest message limits

`ServerOptions::max_ingest_message_bytes` defaults to 64 MiB. Each Arrow IPC message
must fit on the wire, including framing and metadata, and its aggregate expanded
buffers must fit before decompression. The limit covers ingest, partitioned ingest
and Arrow prepared parameters, including dictionary messages. Oversized input is
rejected with HTTP 413 `payload_too_large` before the declared allocation. Split
larger uploads into smaller record batches; the limit is per message, not per upload.

### Query progress and TLS renewal

Set `StatementOptions::on_progress` to observe status while an execution runs and its Arrow
results stream. Callbacks receive the statement document; exceptions are ignored. The watcher
uses a separate bounded transport and stops when the reader closes. Explicit result retention
still takes precedence.

The server reloads replaced certificate, key, client CA and CRL files for new TLS connections.
Existing connections retain their context. An invalid replacement does not replace the last
working context. The C++ client reuses idle connections and reads results in full-size buffers.
