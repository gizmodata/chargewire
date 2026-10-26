# Warp SQL server SDK (C++) and reference server

- **`libwarpsql`** (`include/warpsql/`, `src/sdk/`): the protocol, over HTTP/1.1 and TLS
  on Boost.Beast with C++20 coroutines. An engine implements `warpsql::Backend` (see
  `include/warpsql/backend.hpp`); the SDK owns authentication, handles bound to their
  principal, retries, retention, streaming, cancellation and draining.
- **`libwarpsql_client`** (`include/warpsql/client.hpp`, `src/client/`): the client side,
  synchronous, on Boost.Beast: sign-in, sessions, the streamed and status-document
  statement paths, prepared statements, ingest, metadata, cancellation from another
  thread, TLS and mutual TLS. It needs Arrow, Boost and OpenSSL, not DuckDB. Hardcharger's
  command-line client is built on it.
- **`warp-duckdb`** (`src/duckdb/`): the reference server, DuckDB 2.0 through its v2 C API
  only. It is what the conformance suite and the drivers test against.

> [!WARNING]
> **`warp-duckdb` is for testing and development, not production.** It has one password,
> and whoever signs in is the administrator, with all of DuckDB's power: reading and
> writing local files, attaching databases, loading extensions, reaching the network.
> `--read-only` protects only the database file. It has no users, access control, file
> guards, audit log or metrics. It listens on `127.0.0.1` unless told otherwise; keep it
> there. To serve data to people, run [Hardcharger](https://github.com/gizmodata/hardcharger),
> which is built on this SDK and adds all of that.

## Build

```bash
./scripts/build-duckdb.sh     # DuckDB 2.0 at the pinned commit (about 10 minutes)
./scripts/build-arrow.sh      # Arrow C++ 26.0.0, IPC only (about 30 seconds)
OPENSSL_ROOT_DIR=$(brew --prefix openssl@3) cmake -S . -B build -G Ninja
cmake --build build
```

Boost 1.92 (Beast, JSON, URL) is fetched by CMake. OpenSSL 3 comes from the system.

DuckDB builds in its core extensions (core_functions, parquet, json, icu); the rest load
at run time. A server that wants more built in lists them in a CMake file of
`duckdb_extension_load()` calls and names it in `WARPSQL_DUCKDB_EXTENSION_CONFIGS`
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
WARP_DUCKDB_PASSWORD='…' ./build/warp-duckdb --database my.duckdb --port 31337
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
| `--oauth-client-id ID` | none | The server's client id (the secret comes from `WARP_DUCKDB_OAUTH_CLIENT_SECRET`) |
| `--oauth-identities A,B` | none | Who may sign in (the ID token's `email`, else `sub`; ASCII case ignored) |
| `--oauth-scopes S` | `openid email profile` | |
| `--oauth-base-url URL` | `http(s)://HOST:PORT` | The server's public URL; the redirect URI to register is `<it>/warp/v1/auth/oauth/callback` |
| `--oauth-redirect-uri URL` | from the base URL | The redirect URI, given whole |
| `--oauth-provider-name NAME` | none | The provider's name in discovery ("Sign in with NAME") |
| `--oauth-max-flows N` | 10000 | Sign-ins held at once |

The `--mtls-*` flags have Hardcharger's option names, so test harnesses start either server
the same way. `--flag=value` works for every flag. The first SIGTERM drains (running work finishes, new work is refused); a
second one stops at once. On Windows, Ctrl+C and Ctrl+Break do the same (a parent stops a
child it started with `CREATE_NEW_PROCESS_GROUP` by sending it Ctrl+Break); closing the
console stops it at once.

## Test

The client library's tests (unit, and live against `warp-duckdb`, which they start):

```bash
cmake -S . -B build -G Ninja -DWARPSQL_BUILD_TESTS=ON && cmake --build build
./build/warpsql-client-tests
```

### Upload and retained-result limits

`ServerOptions::max_ingest_message_bytes` defaults to 64 MiB. It caps each IPC message's
wire size (framing, metadata and body) before allocating its declared body, and the sum
of expanded buffers before decompression. All Arrow input routes, including dictionaries
and prepared parameters, use it. Oversized messages return HTTP 413 `payload_too_large`.
Total upload size may exceed the limit; send smaller record batches. A zero limit is
invalid.

`retained_bytes_per_principal` and `retained_bytes_total` default to 1 GiB each. A retained
result must reserve against both; exhaustion fails with `result_quota_exceeded`. Zero
disallows nonempty retained results. Cached Arrow buffers, including nested children and
dictionaries, count; shared buffers in different batches may be counted conservatively
more than once. Cache disposal returns reservations; expiry keeps them until an existing
reader finishes. These budgets do not bound engine memory or temporary encoder/socket
references, and do not add a global budget for unretained streams. Backpressure still
uses blocking production/input waits; this limits patch does not change their scheduling.

The conformance suite:

```bash
cd ../conformance
uv venv .venv && uv pip install --python .venv/bin/python -e .
WARP_SQL_TEST_SERVER_BINARY=../cpp/build/warp-duckdb .venv/bin/python -m pytest -q
```
