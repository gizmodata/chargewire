"""Spec §5.8 on a DuckDB server (`chargewire-duckdb`, Hardcharger): the ways DuckDB lets a
session change something that are not in the portable tests, each refused with `403
read_only` in a read-only session, and what a reader uses, each still working there.

These are regression cases. DuckDB's own READ ONLY transaction does not stop every one of
them (some functions run SQL text, or send it to an attached database), which is why a
server needs more than that transaction; each case here was a way through one layer.

Marked `duckdb`: they need a DuckDB server that reads this machine's files (deselect with
`-m "not duckdb"`). The ones marked `duckdb_extensions` load an extension and fail when it
cannot be loaded (deselect with `-m "not duckdb_extensions"`)."""

import functools
import http.server
import json
import os
import sqlite3
import threading
import uuid

import pytest

from test_discovery_auth import assert_problem
from test_read_only import (capabilities, document, execute_prepared, name, no_read_only_sessions, patch, prepare,
                            refused, rows, succeeded)

pytestmark = pytest.mark.duckdb


@pytest.fixture
def duck(chargewire):
    """A read-write session on a DuckDB server with read-only sessions."""
    sid = chargewire.new_session()
    info = chargewire.http.get(url=f"{chargewire.session_url(sid)}/metadata/server-info", headers=chargewire.headers())
    if "duckdb_version" not in info.json():
        pytest.skip(reason="not a DuckDB server")
    if "read_only_sessions" not in capabilities(chargewire):
        no_read_only_sessions()
    yield sid
    chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


@pytest.fixture
def ro(chargewire, duck):
    sid = chargewire.new_session(options={"read_only": True})
    yield sid
    chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


@pytest.fixture
def table(chargewire, duck):
    created = name("ro_duck")
    succeeded(chargewire, duck, f"CREATE TABLE {created} AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'b')) t(id, v)")
    yield created
    assert rows(chargewire, duck, f"SELECT count(*) AS n FROM {created}") == [{"n": 3}], "a write got through"


def one(chargewire, session_id: str, sql: str):
    return list(rows(chargewire, session_id, sql)[0].values())[0]


# ---- Statements the kind of which is refused ----


def kinds(tmp: str) -> list[tuple[str, str]]:
    return [
        ("COPY TO a file", f"COPY {{t}} TO '{tmp}/copy.csv'"),
        ("COPY a query TO a file", f"COPY (SELECT 1) TO '{tmp}/copy.parquet'"),
        ("COPY FROM a file", f"COPY {{t}} FROM '{tmp}/in.csv'"),
        ("EXPORT DATABASE", f"EXPORT DATABASE '{tmp}/export'"),
        ("IMPORT DATABASE", f"IMPORT DATABASE '{tmp}/export'"),
        ("COPY FROM DATABASE", "COPY FROM DATABASE {c} TO {c}"),
        ("ATTACH a file", f"ATTACH '{tmp}/attached.duckdb' AS ro_attached"),
        ("ATTACH in memory", "ATTACH ':memory:' AS ro_attached"),
        ("DETACH", "DETACH {c}"),
        ("INSTALL", "INSTALL tpch"),
        ("LOAD", "LOAD tpch"),
        ("CHECKPOINT", "CHECKPOINT"),
        ("FORCE CHECKPOINT", "FORCE CHECKPOINT"),
        ("CALL", "CALL checkpoint()"),
        ("VACUUM", "VACUUM"),
        ("ANALYZE", "ANALYZE"),
        ("CREATE SECRET", "CREATE SECRET ro_secret (TYPE s3, KEY_ID 'not-a-key', SECRET 'not-a-secret')"),
        ("CREATE PERSISTENT SECRET", "CREATE PERSISTENT SECRET ro_secret (TYPE s3, KEY_ID 'not-a-key', SECRET 'not-a-secret')"),
        ("DROP SECRET", "DROP SECRET ro_secret"),
        ("CREATE SEQUENCE", "CREATE SEQUENCE ro_sequence"),
        ("CREATE MACRO", "CREATE MACRO ro_macro(a) AS a + 1"),
        ("CREATE TYPE", "CREATE TYPE ro_type AS ENUM ('a')"),
        ("CREATE INDEX", "CREATE INDEX ro_index ON {t} (id)"),
        ("COMMENT ON", "COMMENT ON TABLE {t} IS 'x'"),
        ("TRUNCATE", "TRUNCATE {t}"),
        ("MERGE", "MERGE INTO {t} USING (SELECT 1 AS id) s ON {t}.id = s.id WHEN MATCHED THEN DELETE"),
        ("CREATE TEMP VIEW", "CREATE TEMP VIEW ro_temp_view AS SELECT 1"),
        ("CREATE TEMP MACRO", "CREATE TEMP MACRO ro_temp_macro(a) AS a + 1"),
        ("CREATE TEMP SEQUENCE", "CREATE TEMP SEQUENCE ro_temp_sequence"),
        ("SQL PREPARE", "PREPARE ro_prepared AS SELECT 1"),
        ("SQL PREPARE of a file write", f"PREPARE ro_prepared AS COPY {{t}} TO '{tmp}/prepared.csv'"),
        ("SQL EXECUTE", "EXECUTE ro_prepared"),
        ("SQL DEALLOCATE", "DEALLOCATE ro_prepared"),
        ("EXPLAIN ANALYZE of a file write", f"EXPLAIN ANALYZE COPY {{t}} TO '{tmp}/explained.csv'"),
        ("EXPLAIN of a file write", f"EXPLAIN COPY {{t}} TO '{tmp}/explained.csv'"),
        ("EXPLAIN ANALYZE of a temporary table", "EXPLAIN ANALYZE CREATE TEMP TABLE ro_explained AS SELECT 1 AS i"),
        ("UPDATE EXTENSIONS", "UPDATE EXTENSIONS"),
    ]


@pytest.mark.parametrize("what", [k[0] for k in kinds("")])
def test_a_statement_of_a_refused_kind(chargewire, duck, ro, table, tmp_path, what):
    sql = dict(kinds(str(tmp_path)))[what]
    catalog = document(chargewire, duck)["options"]["catalog"]
    succeeded(chargewire, duck, f"COPY (SELECT 9 AS id, 'copied' AS v) TO '{tmp_path}/in.csv'")
    before = sorted(os.listdir(tmp_path))
    databases = rows(chargewire, duck, "SELECT database_name FROM duckdb_databases() ORDER BY 1")
    refused(chargewire, ro, sql.format(t=table, c=catalog))
    assert sorted(os.listdir(tmp_path)) == before, "a file was written"
    assert rows(chargewire, duck, "SELECT database_name FROM duckdb_databases() ORDER BY 1") == databases
    assert one(chargewire, duck, "SELECT count(*) FROM duckdb_secrets() WHERE name = 'ro_secret'") == 0


def test_the_twins_of_the_refused_kinds_work_in_a_read_write_session(chargewire, duck, table, tmp_path):
    # So the refusals above are of statements that work: a sample of each family, cleaned up.
    for sql in (f"COPY {table} TO '{tmp_path}/copy.csv'", f"EXPLAIN ANALYZE COPY {table} TO '{tmp_path}/explained.csv'",
                "ATTACH ':memory:' AS ro_twin", "DETACH ro_twin", "CHECKPOINT", "CALL checkpoint()", "ANALYZE",
                "CREATE TEMP VIEW ro_twin_view AS SELECT 1", "DROP VIEW ro_twin_view",
                "PREPARE ro_twin_prepared AS SELECT 1", "EXECUTE ro_twin_prepared", "DEALLOCATE ro_twin_prepared",
                "CREATE SEQUENCE ro_twin_sequence", "SELECT nextval('ro_twin_sequence')", "DROP SEQUENCE ro_twin_sequence"):
        response = chargewire.submit(session_id=duck, sql=sql)
        assert response.status_code in (200, 201), (sql, response.text)
    assert sorted(os.listdir(tmp_path)) == ["copy.csv", "explained.csv"]


# ---- Writes inside statements whose kind is allowed ----


def test_a_sequence_is_not_advanced(chargewire, duck, ro):
    sequence = name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {sequence}")
    succeeded(chargewire, duck, f"CREATE VIEW {sequence}_view AS SELECT nextval('{sequence}') AS n")
    succeeded(chargewire, duck, f"CREATE MACRO {sequence}_macro() AS nextval('{sequence}')")
    for sql in (f"SELECT nextval('{sequence}')", f"SELECT * FROM {sequence}_view", f"SELECT {sequence}_macro()",
                f"SELECT * FROM query('SELECT nextval(''{sequence}'')')",
                f"SET VARIABLE ro_v = (SELECT nextval('{sequence}'))"):
        refused(chargewire, ro, sql)
    assert one(chargewire, duck, f"SELECT nextval('{sequence}')") == 1  # the twin, and the proof nothing advanced it


# Functions that run SQL text of their own, or do the server's housekeeping, called the ways a
# statement can reach a function: directly, through query(), qualified, and through a macro
# a writer made earlier.
def through_functions(table: str, macro: str) -> list[str]:
    write = f"WITH written AS (INSERT INTO {table} VALUES (9, ''z'') RETURNING id) SELECT count(*) FROM written"
    serialized = f"json_serialize_sql('{write}')"
    return [
        f"SELECT * FROM json_execute_serialized_sql({serialized})",
        f"SELECT * FROM system.main.json_execute_serialized_sql({serialized})",
        f"SELECT * FROM {macro}_run('{write}')",
        f"SELECT (SELECT count(*) FROM json_execute_serialized_sql({serialized}))",
        f"WITH c AS (SELECT * FROM json_execute_serialized_sql({serialized})) SELECT * FROM c",
        f"EXPLAIN ANALYZE SELECT * FROM json_execute_serialized_sql({serialized})",
        f"SET VARIABLE ro_v = (SELECT count(*) FROM json_execute_serialized_sql({serialized}))",
        f"PRAGMA json_execute_serialized_sql('{{}}')",
        "SELECT * FROM checkpoint()",
        "SELECT * FROM force_checkpoint()",
        "SELECT * FROM system.main.checkpoint()",
        "SELECT * FROM query('SELECT * FROM checkpoint()')",
        f"SELECT * FROM {macro}_checkpoint()",
        "SELECT * FROM enable_logging()",
        "SELECT * FROM disable_logging()",
        "SELECT * FROM truncate_duckdb_logs()",
        "SELECT * FROM enable_profiling()",
    ]


@pytest.mark.parametrize("index", range(len(through_functions("t", "m"))))
def test_a_function_that_changes_something_is_refused_however_it_is_called(chargewire, duck, ro, table, index):
    macro = name("ro_macro")
    succeeded(chargewire, duck, f"CREATE MACRO {macro}_run(q) AS TABLE SELECT * FROM json_execute_serialized_sql(json_serialize_sql(q))")
    succeeded(chargewire, duck, f"CREATE MACRO {macro}_checkpoint() AS TABLE SELECT * FROM checkpoint()")
    logging = one(chargewire, duck, "SELECT current_setting('enable_logging')")
    refused(chargewire, ro, through_functions(table, macro)[index])
    assert one(chargewire, duck, "SELECT current_setting('enable_logging')") == logging
    # (the table fixture checks that no row was written)


def test_a_prepared_statement_through_such_a_function_is_refused_at_its_execution(chargewire, duck, ro, table):
    write = f"WITH written AS (INSERT INTO {table} VALUES (9, ''z'') RETURNING id) SELECT count(*) FROM written"
    prepared = prepare(chargewire, ro, f"SELECT * FROM json_execute_serialized_sql(json_serialize_sql('{write}')) WHERE ? = 1")
    if prepared.status_code == 201:
        prepared = execute_prepared(chargewire, ro, prepared.json()["prepared_id"], [1])
    assert_problem(response=prepared, status=403, code="read_only")


def test_a_temporary_table_from_before_is_not_written(chargewire, duck):
    temp = name("ro_temp")
    succeeded(chargewire, duck, f"CREATE TEMP TABLE {temp} AS SELECT 1 AS i")
    assert patch(chargewire, duck, {"read_only": True})["errors"] == {}
    for sql in (f"INSERT INTO {temp} VALUES (2)", f"UPDATE {temp} SET i = 3", f"DELETE FROM {temp}", f"DROP TABLE {temp}"):
        refused(chargewire, duck, sql)
    assert rows(chargewire, duck, f"SELECT i FROM {temp}") == [{"i": 1}]  # still readable, unchanged


# ---- Settings ----


@pytest.mark.parametrize("sql", [
    "SET GLOBAL default_order = 'desc'", "SET GLOBAL TimeZone = 'Asia/Tokyo'", "RESET GLOBAL default_order",
    "SET memory_limit = '1GB'", "SET threads = 1", "PRAGMA threads = 1", "SET temp_directory = '/tmp/x'",
    "SET enable_profiling = 'json'", "SET profiling_output = '/tmp/x.json'", "SET log_query_path = '/tmp/x.log'",
    "PRAGMA enable_profiling", "SET enable_logging = true", "SET explain_output = 'all'", "SET enable_optimizer = false",
    "SET autoinstall_known_extensions = false", "SET custom_extension_repository = 'http://127.0.0.1:1/x'",
    "SET extension_directory = '/tmp/x'", "SET home_directory = '/tmp/x'", "SET lock_configuration = true",
    "SET enable_external_access = false", "SET TimeZone = (SELECT 'UTC')", "SET search_path = lower('MAIN')",
    "SET SESSION log_query_path = '/tmp/x.log'", "RESET explain_output", "SET \"TimeZone\" TO 'UTC'; SET GLOBAL threads = 1",
])
def test_a_setting_that_reaches_beyond_the_session_is_refused(chargewire, duck, ro, sql):
    before = one(chargewire, duck, "SELECT md5(string_agg(name || '=' || value, '|' ORDER BY name ASC)) FROM duckdb_settings()")
    refused(chargewire, ro, sql)
    assert one(chargewire, duck, "SELECT md5(string_agg(name || '=' || value, '|' ORDER BY name ASC)) FROM duckdb_settings()") == before


def test_a_readers_settings_work_and_stay_in_the_session(chargewire, duck, ro, table):
    # DuckDB's plain SET changes default_order for every session; in a read-only session
    # it is applied to the session only.
    zone = one(chargewire, duck, "SELECT current_setting('TimeZone')")
    order = one(chargewire, duck, "SELECT current_setting('default_order')")
    other = "Asia/Tokyo" if zone != "Asia/Tokyo" else "UTC"
    for sql in (f"SET TimeZone = '{other}'", "SET default_order = 'desc'", "SET SESSION default_null_order = 'nulls_first'",
                "SET search_path = 'main'", "SET integer_division TO true", "PRAGMA default_collation = 'nocase'"):
        succeeded(chargewire, ro, sql)
    assert one(chargewire, ro, "SELECT current_setting('TimeZone')") == other
    assert rows(chargewire, ro, f"SELECT id FROM {table} ORDER BY id") == [{"id": 3}, {"id": 2}, {"id": 1}]
    assert one(chargewire, duck, "SELECT current_setting('TimeZone')") == zone
    assert one(chargewire, duck, "SELECT current_setting('default_order')") == order
    assert rows(chargewire, duck, f"SELECT id FROM {table} ORDER BY id") == [{"id": 1}, {"id": 2}, {"id": 3}]
    succeeded(chargewire, ro, "RESET TimeZone")
    succeeded(chargewire, ro, "RESET default_order")
    assert one(chargewire, ro, "SELECT current_setting('TimeZone')") == zone
    # Variables are the session's own; a value may be a query that reads.
    succeeded(chargewire, ro, "SET VARIABLE ro_answer = 42")
    succeeded(chargewire, ro, f"SET VARIABLE ro_most = (SELECT max(id) FROM {table})")
    assert rows(chargewire, ro, "SELECT getvariable('ro_answer') AS a, getvariable('ro_most') AS m") == [{"a": 42, "m": 3}]
    succeeded(chargewire, ro, "RESET VARIABLE ro_answer")
    # USE moves the session, and the session document follows it.
    schema = name("ro_schema")
    succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
    succeeded(chargewire, ro, f"USE {schema}")
    assert document(chargewire, ro)["options"]["schema"] == schema


# ---- What a reader uses still works ----


def reads(table: str, tmp: str) -> list[str]:
    return [
        # shapes of query: every operator DuckDB plans them with must be one a reader may run
        f"SELECT * FROM {table}", f"SELECT v, count(*), sum(id), avg(id), list(id) FROM {table} GROUP BY v HAVING count(*) > 0 ORDER BY v",
        f"SELECT DISTINCT v FROM {table}", f"SELECT count(*), max(id) FILTER (id > 1), arg_max(v, id) FROM {table}",
        f"SELECT * FROM {table} a JOIN {table} b USING (id)", f"SELECT * FROM {table} a LEFT JOIN {table} b ON a.id < b.id",
        f"SELECT * FROM {table} a FULL JOIN {table} b ON a.id = b.id AND a.v < b.v", f"SELECT * FROM {table} a, {table} b",
        f"SELECT * FROM {table} a SEMI JOIN {table} b ON a.id = b.id", f"SELECT * FROM {table} a ANTI JOIN {table} b ON a.id = b.id + 1",
        f"SELECT * FROM {table} a ASOF JOIN {table} b ON a.id >= b.id", f"SELECT * FROM {table} a POSITIONAL JOIN {table} b",
        "SELECT count(*) FROM range(60000) a(i) JOIN range(60000) b(j) ON a.i < b.j AND a.i > b.j - 3",
        "SELECT count(*) FROM range(200000) a(i) JOIN range(200000) b(j) ON a.i = b.j",
        "SELECT i % 7 AS k, count(*) FROM range(300000) t(i) GROUP BY ALL ORDER BY ALL",
        f"SELECT id, row_number() OVER (ORDER BY id), sum(id) OVER (PARTITION BY v ORDER BY id) FROM {table}",
        f"SELECT * FROM {table} QUALIFY row_number() OVER (PARTITION BY v ORDER BY id) = 1",
        f"SELECT * FROM {table} UNION ALL SELECT * FROM {table}", f"SELECT * FROM {table} UNION SELECT * FROM {table}",
        f"SELECT * FROM {table} EXCEPT SELECT * FROM {table}", f"SELECT * FROM {table} INTERSECT SELECT * FROM {table}",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT sum(n) FROM r",
        f"WITH c AS MATERIALIZED (SELECT * FROM {table}) SELECT * FROM c, c c2", f"WITH c AS (SELECT * FROM {table}) SELECT * FROM c",
        f"SELECT * FROM {table} WHERE id IN (SELECT id FROM {table})", f"SELECT * FROM {table} t WHERE EXISTS (SELECT 1 FROM {table} b WHERE b.id = t.id)",
        f"SELECT (SELECT max(id) FROM {table} b WHERE b.id <= t.id) FROM {table} t", f"SELECT * FROM {table} t, LATERAL (SELECT t.id + 1 AS next) l",
        f"SELECT * FROM {table} ORDER BY id LIMIT 1 OFFSET 1", f"SELECT * FROM {table} LIMIT 50%", f"SELECT * FROM {table} ORDER BY v DESC, id",
        f"SELECT * FROM {table} USING SAMPLE 50%", f"SELECT * FROM {table} TABLESAMPLE reservoir(2)", f"SELECT * FROM {table} WHERE id = 1 AND v LIKE 'a%'",
        "SELECT 1", "VALUES (1, 'a'), (2, 'b')", f"FROM {table}", f"TABLE {table}", "SELECT * FROM (VALUES (1), (2)) v(x)",
        "SELECT * FROM range(10)", "SELECT * FROM generate_series(1, 3)", "SELECT unnest([1, 2, 3])", "SELECT * FROM unnest([1, 2]) WITH ORDINALITY",
        "SELECT unnest([{'a': 1}], recursive := true)", "SELECT * FROM repeat('x', 3)", "SELECT * FROM json_each('[1, 2]')",
        f"PIVOT {table} ON v USING count(*)", f"PIVOT {table} ON v IN ('a', 'b') USING count(*)", "UNPIVOT (SELECT 1 AS a, 2 AS b) ON a, b",
        f"SELECT * FROM query_table('{table}')", "SELECT * FROM query('SELECT 1')", "SELECT random() >= 0, now() IS NOT NULL, uuid() IS NOT NULL",
        # describing and showing
        f"DESCRIBE {table}", f"DESCRIBE SELECT * FROM {table}", "SHOW TABLES", "SHOW ALL TABLES", "SHOW DATABASES", f"SHOW {table}",
        f"SUMMARIZE {table}", f"SUMMARIZE SELECT * FROM {table}", f"EXPLAIN SELECT * FROM {table}", f"EXPLAIN ANALYZE SELECT count(*) FROM {table}",
        f"EXPLAIN (FORMAT JSON) SELECT * FROM {table}", f"EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM {table}", "SELECT current_setting('TimeZone')",
        # the catalog
        "SELECT * FROM information_schema.tables", "SELECT * FROM information_schema.columns", "SELECT * FROM information_schema.schemata",
        "SELECT * FROM duckdb_tables()", "SELECT * FROM duckdb_columns()", "SELECT * FROM duckdb_views()", "SELECT * FROM duckdb_schemas()",
        "SELECT * FROM duckdb_databases()", "SELECT * FROM duckdb_functions() LIMIT 5", "SELECT * FROM duckdb_types() LIMIT 5",
        "SELECT * FROM duckdb_settings()", "SELECT * FROM duckdb_extensions()", "SELECT * FROM duckdb_constraints()", "SELECT * FROM duckdb_indexes()",
        "SELECT * FROM duckdb_sequences()", "SELECT * FROM duckdb_keywords() LIMIT 5", "SELECT * FROM pg_catalog.pg_tables", "SELECT * FROM sqlite_master",
        f"SELECT * FROM pragma_table_info('{table}')", "SELECT * FROM pragma_database_size()", "SELECT * FROM pragma_version()",
        f"PRAGMA table_info('{table}')", "PRAGMA show_tables", "PRAGMA database_list", "PRAGMA version", "PRAGMA database_size",
        # files
        f"SELECT * FROM read_csv('{tmp}/data.csv')", f"SELECT * FROM '{tmp}/data.csv'", f"SELECT * FROM read_csv_auto('{tmp}/*.csv')",
        f"SELECT * FROM sniff_csv('{tmp}/data.csv')", f"SELECT * FROM read_parquet('{tmp}/data.parquet')", f"SELECT * FROM '{tmp}/data.parquet'",
        f"SELECT * FROM parquet_scan('{tmp}/*.parquet')", f"SELECT * FROM read_parquet(['{tmp}/data.parquet', '{tmp}/data.parquet'], filename = true)",
        f"SELECT * FROM parquet_metadata('{tmp}/data.parquet')", f"SELECT * FROM parquet_schema('{tmp}/data.parquet')",
        f"SELECT * FROM parquet_file_metadata('{tmp}/data.parquet')", f"SELECT * FROM parquet_kv_metadata('{tmp}/data.parquet')",
        f"SELECT * FROM read_json('{tmp}/data.json')", f"SELECT * FROM read_json_auto('{tmp}/data.json')", f"SELECT * FROM '{tmp}/data.json'",
        f"SELECT * FROM read_ndjson('{tmp}/data.json')", f"SELECT * FROM read_json_objects('{tmp}/data.json')",
        f"SELECT * FROM glob('{tmp}/*')", f"SELECT * FROM read_text('{tmp}/data.csv')", f"SELECT size FROM read_blob('{tmp}/data.csv')",
        f"SELECT * FROM read_parquet('{tmp}/data.parquet') p JOIN {table} t ON p.id = t.id",
        # a second DuckDB database, attached by someone who may
        "SELECT * FROM ro_reads_other.main.o", "SELECT * FROM information_schema.tables WHERE table_catalog = 'ro_reads_other'",
    ]


@pytest.fixture
def files(chargewire, duck, table, tmp_path):
    for sql in (f"COPY {table} TO '{tmp_path}/data.csv'", f"COPY {table} TO '{tmp_path}/data.parquet'",
                f"COPY {table} TO '{tmp_path}/data.json'", "ATTACH IF NOT EXISTS ':memory:' AS ro_reads_other",
                "CREATE TABLE IF NOT EXISTS ro_reads_other.main.o AS SELECT 1 AS i"):
        succeeded(chargewire, duck, sql)
    return str(tmp_path)


def test_what_a_reader_uses_still_works(chargewire, duck, ro, table, files):
    # One session for all of them, as a reader has: a refusal names the statement. Each is
    # also run in the read-write session, so a statement that fails for another reason
    # shows as that, not as a read-only refusal.
    failures = []
    for sql in reads(table, files):
        twin = chargewire.submit(session_id=duck, sql=sql, arrow=True)
        assert twin.status_code == 200, (sql, twin.text)
        response = chargewire.submit(session_id=ro, sql=sql, arrow=True)
        if response.status_code != 200:
            failures.append(f"{sql}\n    -> {response.status_code} {response.text}")
    assert not failures, "refused or failed in a read-only session:\n" + "\n".join(failures)


def test_a_reader_sees_one_snapshot_inside_its_transaction(chargewire, duck, ro, table):
    succeeded(chargewire, ro, "BEGIN")
    assert one(chargewire, ro, f"SELECT count(*) FROM {table}") == 3
    other = name("ro_snapshot")
    succeeded(chargewire, duck, f"CREATE TABLE {other} AS SELECT 1 AS i")
    response = chargewire.submit(session_id=ro, sql=f"SELECT * FROM {other}")
    assert response.status_code == 400, response.text  # not there in the reader's snapshot
    succeeded(chargewire, ro, "ROLLBACK")
    assert one(chargewire, ro, f"SELECT count(*) FROM {other}") == 1


def test_a_read_only_server_answers_read_only_too(chargewire, duck):
    # §11: read_only is the code of a read-only server as of a read-only session. An
    # explicit READ ONLY transaction in a read-write session gets it as well.
    other = name()
    succeeded(chargewire, duck, f"CREATE TABLE {other} (i INTEGER)")
    succeeded(chargewire, duck, "BEGIN TRANSACTION READ ONLY")
    refused(chargewire, duck, f"INSERT INTO {other} VALUES (1)")
    succeeded(chargewire, duck, "ROLLBACK")
    succeeded(chargewire, duck, f"INSERT INTO {other} VALUES (1)")


# ---- Extensions: other systems' catalogs, lakes, object storage, spatial files ----


def load(chargewire, session_id: str, extension: str) -> None:
    response = chargewire.submit(session_id=session_id, sql=f"LOAD {extension}")
    assert response.status_code in (200, 201), f"the {extension} extension cannot be loaded: {response.text}"


@pytest.mark.duckdb_extensions
def test_an_attached_sqlite_database_is_read_and_not_written(chargewire, duck, ro, tmp_path):
    path = tmp_path / "lite.db"
    connection = sqlite3.connect(database=path)
    connection.execute("CREATE TABLE st (id INTEGER, v TEXT)")
    connection.execute("INSERT INTO st VALUES (1, 'a')")
    connection.commit()
    connection.close()
    catalog = name("ro_lite")
    load(chargewire, duck, "sqlite_scanner")
    succeeded(chargewire, duck, f"ATTACH '{path}' AS {catalog} (TYPE sqlite)")
    try:
        assert rows(chargewire, ro, f"SELECT * FROM {catalog}.st") == [{"id": 1, "v": "a"}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {catalog}.st s JOIN {catalog}.st t USING (id)") == [{"n": 1}]
        assert rows(chargewire, ro, f"SELECT * FROM sqlite_scan('{path}', 'st')") == [{"id": 1, "v": "a"}]
        for sql in (f"INSERT INTO {catalog}.st VALUES (2, 'b')", f"DELETE FROM {catalog}.st", f"CREATE TABLE {catalog}.n (i INTEGER)",
                    f"SELECT * FROM sqlite_query('{catalog}', 'INSERT INTO st VALUES (3, ''q'') RETURNING *')",
                    f"SELECT * FROM sqlite_query('{catalog}', 'SELECT count(*) FROM st')"):
            refused(chargewire, ro, sql)
    finally:
        succeeded(chargewire, duck, f"DETACH {catalog}")
    connection = sqlite3.connect(database=path)
    assert connection.execute("SELECT count(*) FROM st").fetchone()[0] == 1
    assert [r[0] for r in connection.execute("SELECT name FROM sqlite_master")] == ["st"]
    connection.close()


@pytest.mark.duckdb_extensions
def test_a_ducklake_is_read_with_time_travel_and_not_changed(chargewire, duck, ro, tmp_path):
    lake = name("ro_lake")
    load(chargewire, duck, "ducklake")
    succeeded(chargewire, duck, f"ATTACH 'ducklake:{tmp_path}/lake.ducklake' AS {lake} (DATA_PATH '{tmp_path}/lake_data')")
    try:
        succeeded(chargewire, duck, f"CREATE TABLE {lake}.lt AS SELECT 1 AS id, 'a' AS v")
        first = one(chargewire, duck, f"SELECT max(snapshot_id) FROM {lake}.snapshots()")
        succeeded(chargewire, duck, f"INSERT INTO {lake}.lt VALUES (2, 'b')")
        state = lambda: (one(chargewire, duck, f"SELECT count(*) FROM {lake}.lt"),
                         one(chargewire, duck, f"SELECT count(*) FROM {lake}.snapshots()"),
                         sorted(f for _, _, fs in os.walk(tmp_path / "lake_data") for f in fs))
        before = state()
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {lake}.lt") == [{"n": 2}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {lake}.lt AT (VERSION => {first})") == [{"n": 1}]
        assert one(chargewire, ro, f"SELECT count(*) FROM {lake}.snapshots()") == before[1]
        assert one(chargewire, ro, f"SELECT count(*) FROM ducklake_snapshots('{lake}')") == before[1]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {lake}.lt l JOIN {lake}.lt r USING (id)") == [{"n": 2}]
        for sql in (f"INSERT INTO {lake}.lt VALUES (3, 'c')", f"DELETE FROM {lake}.lt", f"CREATE TABLE {lake}.n (i INTEGER)",
                    f"CALL ducklake_expire_snapshots('{lake}', older_than => now())",
                    f"SELECT * FROM ducklake_expire_snapshots('{lake}', older_than => now())",
                    f"SELECT * FROM ducklake_flush_inlined_data('{lake}')",
                    f"SELECT * FROM ducklake_cleanup_old_files('{lake}', cleanup_all => true)",
                    f"SELECT * FROM {lake}.set_option('target_file_size', '10MB')"):
            refused(chargewire, ro, sql)
        # A function that plans to nothing when it has nothing to do runs as nothing; with
        # work to do, its plan has operators that write, and it is refused. Either way:
        chargewire.submit(session_id=ro, sql=f"SELECT * FROM ducklake_merge_adjacent_files('{lake}')")
        assert state() == before
    finally:
        succeeded(chargewire, duck, f"DETACH {lake}")


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@pytest.mark.duckdb_extensions
def test_files_over_http_are_read(chargewire, duck, ro, table, files):
    # httpfs: the same functions that read local files read object storage and web servers.
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHandler, directory=files))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        load(chargewire, duck, "httpfs")
        assert one(chargewire, ro, f"SELECT count(*) FROM read_parquet('{base}/data.parquet')") == 3
        assert one(chargewire, ro, f"SELECT count(*) FROM '{base}/data.csv'") == 3
        assert one(chargewire, ro, f"SELECT count(*) FROM parquet_metadata('{base}/data.parquet')") >= 1
        refused(chargewire, ro, f"COPY {table} TO '{base}/upload.parquet'")
    finally:
        server.shutdown()
        thread.join()


@pytest.mark.duckdb_extensions
def test_spatial_files_are_read(chargewire, duck, ro, tmp_path):
    feature = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"name": "origin"}, "geometry": {"type": "Point", "coordinates": [0, 0]}}]}
    (tmp_path / "points.geojson").write_text(json.dumps(feature))
    load(chargewire, duck, "spatial")
    assert rows(chargewire, ro, f"SELECT name, ST_AsText(geom) AS wkt FROM ST_Read('{tmp_path}/points.geojson')") == \
        [{"name": "origin", "wkt": "POINT (0 0)"}]
    assert one(chargewire, ro, "SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4))") == 5.0
    refused(chargewire, ro, f"COPY (SELECT ST_Point(0, 0) AS geom) TO '{tmp_path}/out.geojson' WITH (FORMAT gdal, DRIVER 'GeoJSON')")
    assert sorted(os.listdir(tmp_path)) == ["points.geojson"]


@pytest.mark.duckdb_extensions
def test_an_attached_postgres_database_is_read_and_not_written(chargewire, duck, ro):
    # CHARGEWIRE_TEST_POSTGRES: a libpq connection string of a PostgreSQL the server can
    # reach and may write (a throwaway one); without it this test skips, saying so.
    dsn = os.environ.get("CHARGEWIRE_TEST_POSTGRES")
    if not dsn:
        pytest.skip(reason="set CHARGEWIRE_TEST_POSTGRES to a throwaway PostgreSQL's connection string")
    catalog, remote = name("ro_pg"), name("ro_pt")
    load(chargewire, duck, "postgres_scanner")
    succeeded(chargewire, duck, f"ATTACH '{dsn}' AS {catalog} (TYPE postgres)")
    try:
        succeeded(chargewire, duck, f"CREATE TABLE {catalog}.{remote} AS SELECT 1 AS id, 'a' AS v")
        assert rows(chargewire, ro, f"SELECT * FROM {catalog}.{remote}") == [{"id": 1, "v": "a"}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {catalog}.{remote} WHERE id = 1") == [{"n": 1}]
        for sql in (f"INSERT INTO {catalog}.{remote} VALUES (2, 'b')", f"DELETE FROM {catalog}.{remote}",
                    f"CALL postgres_execute('{catalog}', 'INSERT INTO {remote} VALUES (3, ''e'')')",
                    f"SELECT * FROM postgres_query('{catalog}', 'INSERT INTO {remote} VALUES (4, ''q'') RETURNING *')",
                    f"SELECT * FROM postgres_query('{catalog}', 'SELECT count(*) FROM {remote}')"):
            refused(chargewire, ro, sql)
        assert one(chargewire, duck, f"SELECT count(*) FROM {catalog}.{remote}") == 1
        succeeded(chargewire, duck, f"DROP TABLE {catalog}.{remote}")
    finally:
        succeeded(chargewire, duck, f"DETACH {catalog}")
