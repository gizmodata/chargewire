# Warp SQL conformance suite

Black-box tests of a Warp SQL server: they speak the protocol over HTTP with
[`warp.py`](tests/warp.py), a few plain calls with nothing hidden, so a failure points at
the protocol rather than at a driver. The suite fails, never skips, when it has no server
to talk to.

```bash
uv venv .venv && uv pip install --python .venv/bin/python -e .
WARP_SQL_TEST_SERVER_BINARY=../cpp/build/warp-duckdb .venv/bin/python -m pytest -q
```

## Pointing it at a server

| Variable | Default | Meaning |
|---|---|---|
| `WARP_SQL_TEST_SERVER_BINARY` | | A server binary to start (taking `--host`, `--port`) |
| `WARP_SQL_TEST_SERVER_PASSWORD_ENV` | `WARP_DUCKDB_PASSWORD` | The variable it reads its administrator's password from |
| `WARP_SQL_TEST_SERVER_USERNAME` | `warp` | That administrator's name |
| `WARP_SQL_TEST_SERVER_LOG` | a temporary file | Where the started server's output goes |
| `WARP_SQL_TEST_URL`, `WARP_SQL_TEST_USERNAME`, `WARP_SQL_TEST_PASSWORD` | | A running server instead (then deselect `-m "not mtls and not own_server"`) |

Tests marked `mtls` and `own_server` start servers of their own, with flags that differ
between servers; the defaults are `warp-duckdb`'s:

| Variable | Default | Hardcharger's |
|---|---|---|
| `WARP_SQL_TEST_SERVER_MTLS_FLAG` | `--tls-client-ca` | `--mtls-ca-cert` |
| `WARP_SQL_TEST_SERVER_CORS_FLAG` | `--cors-origin` | `--cors-origins` |
| `WARP_SQL_TEST_SERVER_IDLE_FLAG` | `--session-idle-timeout-seconds` | `--session-idle-timeout` |
| `WARP_SQL_TEST_SERVER_TOKEN_LIFETIME_FLAG` | `--token-lifetime-seconds` | |
| `WARP_SQL_TEST_SERVER_DRAIN_FLAG` | `--drain-seconds` | `--shutdown-grace-period-seconds` |
| `WARP_SQL_TEST_SERVER_HEALTH_QUERY_FLAG` | `--health-check-query` | `--health-check-query` |

[`test_authorization.py`](tests/test_authorization.py) needs a server with SQL privileges
and the `WARP_SQL_TEST_AUTHZ_*` variables its docstring describes; on a server without
privileges (`warp-duckdb`) it skips, saying so.

## MUSTs without a test

Every MUST of the spec that a server can be tested for has a test, except these:

| Spec | MUST | Why not yet |
|---|---|---|
| §2.1 | Every operation works over HTTP/1.1 through an ordinary reverse proxy | Needs a proxy in the harness |
| §2.3 | Ids carry at least 128 bits of randomness | A black box can't measure entropy: `test_ids_are_long_random_strings` refuses ids too short or regular to hold it, no more |
| §2.6 | `Retry-After` is delta-seconds on `429` | Tested on `503` (draining); no portable way to make a server rate-limit |
| §4.4 | An identity shown in error details or logs is escaped | Refusing such identities is tested; their escaping in a server's logs is not visible to the suite |
| §4.4 | Tokens a principal derives from a bound token keep the binding | The protocol has no way to derive one (an engine's scoped tokens are its own SQL); Hardcharger tests its own (`test_mtls_identity.py`) |
| §9.3 | Retained results stored outside memory are protected like the source data and deleted on expiry or session end | Storage is outside the protocol |
| §9.5 | `X-Content-Type-Options: nosniff` on every response | Checked on a sample of responses, not all |
| §9.6 | No TLS 1.2 renegotiation to change the certificate | Python's `ssl` can't start a renegotiation |

These MUSTs bind clients, not servers, so a server suite can't test them: §3.1 (use only
listed features), §4.1 (tokens are opaque), §5.6 (a new token after `session_ended` for the
token reasons), §6.1 (check `Content-Type`), §6.2 (a `failed` or `cancelled` document is an
error), §6.6 (an incomplete stream is a failed result), §6.10 (read every partition to its
end, all at once, and never guess a missing `Warp-Result-Order`), §8.2 (read buffers left
uncompressed), §8.3 (no reliance on row order after a partitioned ingest) and §12 (ignore
what is unknown, accept unknown enumeration values). The drivers' own suites test them.

§2.1's "say so in its documentation" (a base path other than `/warp`) and §4.4's "end TLS
at the server" are about documentation and deployment.
