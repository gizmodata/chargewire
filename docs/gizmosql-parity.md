# GizmoSQL 1.x parity

**The bar:** GizmoSQL 2.0, serving only Warp SQL, passes every test GizmoSQL 1.x
passes over Arrow Flight SQL. This page maps 1.x's protocol surface onto Warp SQL, so
a gap in the spec shows up here before it shows up in a port.

Checked against `gizmodata/gizmosql` at v1.42.0 (`a3f79b5e`).

## How the tests get ported

The 1.x integration tests (`tests/integration/*.cpp`, 61 files) call the Flight SQL
C++ client directly: `Execute` (≈170 calls), `AuthenticateBasicToken` (≈150),
`DoGet` (≈130), then `Prepare`, `ExecuteIngest`, `ExecuteUpdate`, `GetTables`,
`GetDbSchemas`, `GetCatalogs`, transactions, session options and
`CancelFlightInfo`.

Plan:

1. Put a small client interface in the test fixture (connect, execute, fetch,
   prepare, ingest, metadata, sessions, cancel) and move every test onto it, with a
   Flight SQL implementation. 1.x must stay green through this step; that proves the
   port didn't change what the tests check.
2. Add a Warp SQL implementation of the same interface.
3. Run the whole suite against GizmoSQL 2.0 through it.

Tests whose subject is the transport itself are rewritten against the Warp SQL
equivalent (third table below), keeping the behaviour they check.

## Server operations

Every Flight SQL method the 1.x DuckDB server overrides (`src/duckdb/duckdb_server.h`):

| Flight SQL (1.x) | Warp SQL |
|---|---|
| Handshake / `AuthenticateBasicToken` | `POST /warp/v1/auth/token` (§4.1) |
| `GetFlightInfoStatement` + `DoGetStatement` | `POST …/statements`, `GET …/result` (§6) |
| `DoPutCommandStatementUpdate` | `POST …/statements` returning `kind: update` (§6.2) |
| `CreatePreparedStatement` / `ClosePreparedStatement` | `POST …/prepared`, `DELETE …/prepared/{id}` (§7) |
| `DoPutPreparedStatementQuery` (bind parameters) | Body of `POST …/prepared/{id}/execute` (§7.2) |
| `GetFlightInfoPreparedStatement` + `DoGetPreparedStatement` | `POST …/prepared/{id}/execute` (§7.2) |
| `DoPutPreparedStatementUpdate` | Same, returning `kind: update` |
| `DoPutCommandStatementIngest` | `POST …/ingest` (§8) |
| `GetFlightInfoCatalogs` / `DoGetCatalogs` | `GET …/metadata/catalogs` |
| `GetFlightInfoSchemas` / `DoGetDbSchemas` | `GET …/metadata/db-schemas` |
| `GetFlightInfoTables` / `DoGetTables` | `GET …/metadata/tables` |
| `GetFlightInfoTableTypes` / `DoGetTableTypes` | `GET …/metadata/table-types` |
| `GetFlightInfoPrimaryKeys` / `DoGetPrimaryKeys` | `GET …/metadata/primary-keys` |
| `GetFlightInfoImportedKeys` / `DoGetImportedKeys` | `GET …/metadata/imported-keys` |
| `GetFlightInfoExportedKeys` / `DoGetExportedKeys` | `GET …/metadata/exported-keys` |
| `GetFlightInfoCrossReference` / `DoGetCrossReference` | `GET …/metadata/cross-reference` |
| `GetFlightInfoXdbcTypeInfo` / `DoGetXdbcTypeInfo` | `GET …/metadata/type-info` |
| `GetSqlInfo` (base class, ~136 registered values) | `GET …/metadata/server-info` (§10) |
| `BeginTransaction` / `EndTransaction` | `autocommit: false`, `POST …/commit` / `…/rollback` (§5.5) |
| `CancelFlightInfo` / `CancelQuery` | `POST …/statements/{id}/cancel` (§6.8) |
| `SetSessionOptions` / `GetSessionOptions` | `PATCH` / `GET /warp/v1/sessions/{id}` (§5.2–5.3) |
| `CloseSession` | `DELETE /warp/v1/sessions/{id}` (§5.4) |
| `SetQueryTimeout` | `Warp-Statement-Timeout` (§6.9) |
| gRPC health service | `GET /warp/v1/health` (§3.2) |
| OAuth endpoints (`/oauth/initiate`, polling) | `/warp/v1/auth/oauth/flows` (§4.3) |
| Kill session (SQL `KILL SESSION`) | Unchanged: SQL. The victim's next request gets `session_ended`, `reason: killed` (§5.6) |
| Query and session tags | `Warp-Query-Tag` header; `session_tag` option |

## Test files

**SQL-level: port onto the client interface, assertions unchanged.** These check
engine behaviour; only the calls that send SQL and read results change.

`active_session_count`, `admin_command_guard`, `admission_controller`,
`admission_queue_telemetry`, `allow_unsigned_extensions`, `auth_log_level`,
`catalog_access`, `catalog_logging`, `catalog_permissions_enterprise`,
`catalog_wildcards`, `ducklake`, `ducklake_catalog_fanout`, `eager_execution`,
`enterprise_gating`, `geoarrow`, `init_sql_splitting`, `inline_license_key`,
`instrumentation`, `instrumentation_ducklake`, `internal_query_log_level`,
`kill_session`, `log_level_filtering`, `max_sessions`, `memory_limit`, `metrics`,
`query_profile_capture`, `read_only_mode`, `sensitive_path_guard`,
`session_idle`, `set_query_log_level`, `sqlite_backend`, `statement_queue`,
`storage_version`, `system_catalog`, `system_managed_catalog_visibility`,
`tpch_benchmark`, `variant`, and the RBAC suites once they merge.

**Protocol operations: port onto the matching Warp SQL operation.**

| Test | Warp SQL |
|---|---|
| `authentication` (30 cases: wrong/blank/forged credentials, tampered tokens, injection, very long input) | §4; every case keeps its expected rejection, now `invalid_credentials` / `invalid_token` |
| `credential_parsing` (passwords containing `:`) | §4.1: credentials are JSON fields, so there is nothing to split |
| `token_authorized_emails`, `oauth_server` | §4.1 `token_exchange`, §4.3 |
| `cross_instance_tokens` | §4.1: token scope is server policy; same cases |
| `prepared_statement_params` | §7 (Arrow and JSON parameters) |
| `result_streaming` | §6.4: many batches, in order, complete |
| `bulk_ingest` (+ `test_bulk_ingest.py`) | §8.1, including the type-preservation rule |
| `table_column_metadata` | §10 `tables` with `include_schema=true` |
| `session_options_probe` | §5.2 |
| `reject_unknown_sessions` | §5.6, **one deliberate change**, below |
| `instance_session_query_tags` | `Warp-Query-Tag`, `session_tag` |
| `test_autocommit_false.py` | §5.5 |

**Transport: rewrite for HTTP, keep the behaviour.**

| Test | Flight SQL behaviour | Warp SQL equivalent |
|---|---|---|
| `health_check` | gRPC health reports NOT_SERVING on a hung/failing check, and shutdown doesn't hang | §3.2: `503 check_failed` within a bounded time |
| `max_metadata_size` | Oversized gRPC metadata rejected unless the limit is raised | §2.5: `431`, configurable limit (≥ 16 KiB) |
| `client_disconnect_cancel` | A cancelled `DoGet` or an expired client deadline interrupts the query | §6.5: disconnecting from an unretained result cancels it; §6.9 timeout |
| `graceful_shutdown`, `graceful_shutdown_settings` | Drain in-flight work, refuse new work, second signal forces stop | §11.1: `503 server_draining` + `Retry-After` |
| `system_ca_certs` | Client trusts the OS CA store | Same, in the Warp SQL client |
| `server_cli`, `client_cli`, `client_config`, `interactive_client`, `test_client_shell.sh` | Flags, URIs and the `gizmosql_client` shell | Same tests after the client moves to Warp SQL; URI scheme and port flags change |

**Python and driver suites.** `test_geoarrow.py`, `test_v1_22_features.py` and
`test_v1_22_1_features.py` move to the Warp SQL Python ADBC driver.
`tests/drivers/` (ADBC, JDBC and ODBC compatibility) runs once each driver has a
Warp SQL version; until then those drivers are the 1.x product.

## Deliberate changes in behaviour

- **No implicit session creation.** 1.x can recreate a session for an unknown or
  closed id (`ClosedSessionIsRecreatedByDefault`,
  `OtherInstancesSessionIsCreatedByDefault`). Warp SQL always answers
  `session_not_found` (§5.6), because the client's settings, temporary tables and
  transaction are gone and only the client can decide to start over. The driver
  recreates the session and replays its options; those two tests become driver
  tests.

## Not yet covered by the spec

Nothing found so far. Add a row here for any 1.x behaviour a port finds that the
spec does not cover, and fix the spec before the port works around it.
