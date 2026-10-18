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
| `--mtls-crl-file PEM` | none | Certificate revocation lists, checked at the handshake for every certificate of the chain; the file is looked at every 5 s and reloaded when it changes, and open connections are re-checked then and when a CRL passes its nextUpdate (spec §4.4) |
| `--mtls-identity-source` | `san_email` | Where the `client_certificate` grant takes the identity: `san_email`, `san_uri` (SPIFFE IDs) or `subject_cn` |
| `--mtls-certificate-sign-in` | off | Offer the `client_certificate` grant; off, client certificates only admit connections. Needs `--mtls-admin-identities` |
| `--mtls-admin-identities A,B` | none | Identities that may sign in with their certificate (as the administrator) |
| `--mtls-crl-allow-missing-ca-crls` | off | Every certificate of a chain is revocation-checked, so the CRL file needs every CA's CRL (the root's too); this accepts a CA above the leaf without one |
| `--mtls-require-identity-match` | off | A password sign-in over a client certificate must be for its identity |

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

The conformance suite:

```bash
cd ../conformance
uv venv .venv && uv pip install --python .venv/bin/python -e .
WARP_SQL_TEST_SERVER_BINARY=../cpp/build/warp-duckdb .venv/bin/python -m pytest -q
```
