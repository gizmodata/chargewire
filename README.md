# Warp SQL

**An open protocol for SQL over HTTP, powered by Apache Arrow.**

Warp SQL lets any HTTP client run SQL against a database server and get Arrow data
back: a browser `fetch`, `curl`, or any language's standard HTTP library. No gRPC,
no protobuf, no generated code.

- **Arrow on the wire.** Results, query parameters and bulk uploads are Arrow IPC
  streams (`application/vnd.apache.arrow.stream`), so every Arrow library can read
  them directly. Control messages are small JSON documents.
- **Long queries survive real networks.** Submit a query, get a handle, poll it,
  and fetch the result later. A dropped download resumes from the batch where it
  stopped. No request has to outlive a proxy timeout.
- **Short queries stay one round trip.** One `POST` can run a query and stream its
  result.
- **Safe by construction.** Every handle is bound to the identity that created it
  and checked on every use; credentials are bearer tokens, never cookies.
- **Engine-neutral and open.** Apache-2.0 licensed. Any engine can implement it.

```bash
# Get a token, open a session, run a query, read the result with pyarrow.
TOKEN=$(curl -s https://db.example.com/warp/v1/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"grant_type":"password","username":"joe","password":"…"}' | jq -r .access_token)

SESSION=$(curl -s https://db.example.com/warp/v1/sessions \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{}' | jq -r .session_id)

curl -s https://db.example.com/warp/v1/sessions/$SESSION/statements \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.apache.arrow.stream' \
  -d '{"sql":"SELECT 42 AS answer"}' > result.arrows

python -c "import pyarrow.ipc as ipc; print(ipc.open_stream(source='result.arrows').read_all())"
```

*(Illustrative: no server implements this draft yet, so this example has not been run.)*

## Status

**Draft 0.1.0.** The protocol is being designed in the open; expect changes until
1.0.0. GizmoSQL 2.0 is the first server, and its acceptance bar is that it passes
every GizmoSQL 1.x test over Warp SQL alone ([parity plan](docs/gizmosql-parity.md)).

## Contents

| Path | What |
|---|---|
| [`spec/warp-sql.md`](spec/warp-sql.md) | The protocol specification |
| [`docs/server-sdk.md`](docs/server-sdk.md) | Design of the reference C++ server library (engines override "bare" methods, as with Flight SQL's server base class) |
| [`docs/gizmosql-parity.md`](docs/gizmosql-parity.md) | How GizmoSQL 1.x's Flight SQL surface and tests map onto Warp SQL |
| [`CHANGELOG.md`](CHANGELOG.md) | Changes by version |

## Roadmap

1. Spec draft (this).
2. ~~Spike: DuckDB results through nanoarrow's IPC writer, checked value by value for
   every type.~~ Done ([results](spikes/nanoarrow-duckdb/README.md)).
3. Reference server SDK (C++) and a conformance test suite.
4. Clients: [`warp-sql-adbc`](https://github.com/gizmodata/warp-sql-adbc) (generic Go
   ADBC driver) and a JavaScript client.
5. GizmoSQL 2.0 on the SDK, passing the 1.x test suite.
6. Spec 1.0.0.

## Relationship to Apache Arrow

Warp SQL is an independent project; it is not part of, or endorsed by, the Apache
Software Foundation. It uses the Arrow IPC format and borrows Arrow Flight SQL's
metadata schemas so existing driver code carries over. The Arrow community's
[HTTP data transport work](https://github.com/apache/arrow-experiments/tree/main/http)
is the reference for how Arrow streams should travel over HTTP.

Apache Arrow, Arrow, Apache, the Apache logo, and the Apache Arrow project logo are
either registered trademarks or trademarks of The Apache Software Foundation in the
United States and other countries.

## License

[Apache License 2.0](LICENSE).
