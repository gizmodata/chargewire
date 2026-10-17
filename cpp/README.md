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

## Run

```bash
WARP_DUCKDB_PASSWORD='…' ./build/warp-duckdb --database my.duckdb --port 31337
```

The password is read from the environment so it never shows in process listings. TLS:
`--tls-cert` and `--tls-key` (PEM), plus `--tls-client-ca` to require client
certificates. The first SIGTERM drains (running work finishes, new work is refused); a
second one stops at once.

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
