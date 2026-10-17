# Warp SQL server SDK (C++) and reference server

- **`libwarpsql`** (`include/warpsql/`, `src/sdk/`): the protocol, over HTTP/1.1 and TLS
  on Boost.Beast with C++20 coroutines. An engine implements `warpsql::Backend` (see
  `include/warpsql/backend.hpp`); the SDK owns authentication, handles bound to their
  principal, retries, retention, streaming, cancellation and draining.
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

```bash
cd ../conformance
uv venv .venv && uv pip install --python .venv/bin/python -e .
WARP_SQL_TEST_SERVER_BINARY=../cpp/build/warp-duckdb .venv/bin/python -m pytest -q
```
