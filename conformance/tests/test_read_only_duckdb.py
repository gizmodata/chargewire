"""Spec §5.8 on a DuckDB server (`chargewire-duckdb`, Hardcharger): the ways DuckDB lets a
session change something that are not in the portable tests, each refused with `403
read_only` in a read-only session, and what a reader uses, each still working there.

These are regression cases: each was a way through one layer of an earlier server, or a
read that an earlier server refused. A refusal is only worth something when nothing was
done before it was given, so every refusal here is inside a snapshot of what a second
session sees (`unchanged`, test_read_only.py): rows, the catalog, sequences, attached
databases, files, every setting, logging, secrets, extensions, and the read-only session's
own temporary objects, settings and variables.

Marked `duckdb`: they need a DuckDB server that reads this machine's files (deselect with
`-m "not duckdb"`). The ones marked `duckdb_extensions` install an extension from the
network into the server's scratch home directory (conftest.py) and load it, and fail when
that cannot be done (deselect with `-m "not duckdb_extensions"`)."""

import contextlib
import functools
import http.server
import json
import os
import sqlite3
import threading
import time
import uuid

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from conftest import start_server, stop_server
from test_discovery_auth import assert_problem
from test_read_only import observer, unchanged  # noqa: F401  (fixtures)
from test_read_only import (Unchanged, capabilities, document, execute_prepared, name, no_read_only_sessions, patch,
                            prepare, refused, rows, succeeded)
from chargewire import Chargewire

pytestmark = pytest.mark.duckdb

EXPRESSION_DEPTH_CAPPED = os.environ.get("CHARGEWIRE_TEST_DUCKDB_EXPRESSION_DEPTH_CAPPED") == "1"
AUTOCOMPLETE_DISABLED = os.environ.get("CHARGEWIRE_TEST_DUCKDB_AUTOCOMPLETE_DISABLED") == "1"
AUTOCOMPLETE_READS = [
    "SELECT count(*) FROM sql_tokenize('SELECT 1')",
    "SELECT count(*) FROM sql_auto_complete('SEL')",
]


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


def one(chargewire, session_id: str, sql: str, parameters=None):
    return list(rows(chargewire, session_id, sql, parameters)[0].values())[0]


def answer_of(chargewire, session_id: str, sql: str) -> tuple:
    """(200, rows) of a query that ran, or (status, code) of one that did not."""
    response = chargewire.submit(session_id=session_id, sql=sql, arrow=True)
    if response.status_code == 200:
        from chargewire import read_table
        return 200, read_table(content=response.content).to_pylist()
    return response.status_code, response.json()["code"]


def kept_out_when_prepared(chargewire, session_id: str, sql: str) -> None:
    """A statement that a read-only session may not run, prepared: refused then, or accepted
    (DuckDB binds nothing of the definition it names to describe it) and refused when it is
    executed, as every execution of a prepared statement is checked."""
    response = prepare(chargewire, session_id, sql)
    if response.status_code == 201:
        response = execute_prepared(chargewire, session_id, response.json()["prepared_id"])
    assert_problem(response=response, status=403, code="read_only")


def logging_is_on(chargewire, session_id: str) -> bool:
    return bool(one(chargewire, session_id, "SELECT current_setting('enable_logging')::BOOLEAN"))


@pytest.fixture
def logging_stays_off(chargewire, duck):
    """Several tests use `enable_logging()` as the function that must not run: it changes a
    setting of the whole server, which every session sees. If one ever gets through, the
    server is put back (so the tests after it are not judged by it) and the test fails."""
    assert not logging_is_on(chargewire, duck), "the server's logging was on before the test"
    yield
    if logging_is_on(chargewire, duck):
        succeeded(chargewire, duck, "CALL disable_logging()")
        succeeded(chargewire, duck, "CALL truncate_duckdb_logs()")
        pytest.fail(reason="a read-only session switched the server's logging on")


def test_the_server_keeps_its_extensions_and_secrets_in_a_home_directory_of_its_own(chargewire, server, duck):
    # These tests send INSTALL and CREATE PERSISTENT SECRET to be refused. A build that lets
    # one through must leave it in the scratch home directory the suite gave the server
    # (conftest.py), never under the home of whoever runs the suite, where every later run
    # of every build would find it.
    if server.process is None:
        pytest.skip(reason="the suite did not start this server (CHARGEWIRE_TEST_URL): its home directory is its own business")
    assert server.home is not None and server.home.is_dir()
    assert os.path.realpath(server.home) != os.path.realpath(os.path.expanduser("~"))
    assert one(chargewire, duck, "SELECT current_setting('secret_directory')") == os.path.join(str(server.home), ".duckdb", "stored_secrets")
    succeeded(chargewire, duck, "CREATE OR REPLACE PERSISTENT SECRET ro_home_probe (TYPE http, BEARER_TOKEN 'not-a-token')")
    try:
        assert os.path.exists(os.path.join(server.home, ".duckdb", "stored_secrets", "ro_home_probe.duckdb_secret"))
    finally:
        succeeded(chargewire, duck, "DROP PERSISTENT SECRET ro_home_probe")
    assert not os.path.exists(os.path.join(server.home, ".duckdb", "stored_secrets", "ro_home_probe.duckdb_secret"))


# ---- Statements the kind of which is refused ----


def kinds(tmp: str) -> list[tuple[str, str]]:
    return [
        ("COPY TO a file", f"COPY {{t}} TO '{tmp}/copy.csv'"),
        ("COPY a query TO a file", f"COPY (SELECT 1) TO '{tmp}/copy.parquet'"),
        ("COPY FROM a file", f"COPY {{t}} FROM '{tmp}/in.csv'"),
        ("EXPORT DATABASE", f"EXPORT DATABASE '{tmp}/export'"),
        ("COPY FROM DATABASE", "COPY FROM DATABASE {c} TO ro_reads_other"),
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
        ("ALTER TABLE RENAME", "ALTER TABLE {t} RENAME TO ro_renamed"),
        ("CREATE OR REPLACE TABLE", "CREATE OR REPLACE TABLE {t} AS SELECT 0 AS id, 'replaced' AS v"),
        ("CREATE TEMP VIEW", "CREATE TEMP VIEW ro_temp_view AS SELECT 1"),
        ("CREATE TEMP MACRO", "CREATE TEMP MACRO ro_temp_macro(a) AS a + 1"),
        ("CREATE TEMP SEQUENCE", "CREATE TEMP SEQUENCE ro_temp_sequence"),
        ("CREATE TEMP TABLE AS", "CREATE TEMP TABLE ro_temp_table AS SELECT 1 AS i"),
        ("SQL PREPARE", "PREPARE ro_prepared AS SELECT 1"),
        ("SQL PREPARE of a file write", f"PREPARE ro_prepared AS COPY {{t}} TO '{tmp}/prepared.csv'"),
        ("SQL EXECUTE", "EXECUTE ro_prepared"),
        ("SQL DEALLOCATE", "DEALLOCATE ro_prepared"),
        ("EXPLAIN ANALYZE of a file write", f"EXPLAIN ANALYZE COPY {{t}} TO '{tmp}/explained.csv'"),
        ("EXPLAIN ANALYZE of a temporary table", "EXPLAIN ANALYZE CREATE TEMP TABLE ro_explained AS SELECT 1 AS i"),
        ("UPDATE EXTENSIONS", "UPDATE EXTENSIONS"),
    ]


@pytest.mark.parametrize("what", [k[0] for k in kinds("")])
def test_a_statement_of_a_refused_kind(chargewire, duck, ro, table, tmp_path, unchanged, what):
    sql = dict(kinds(str(tmp_path)))[what]
    catalog = document(chargewire, duck)["options"]["catalog"]
    succeeded(chargewire, duck, f"COPY (SELECT 9 AS id, 'copied' AS v) TO '{tmp_path}/in.csv'")
    succeeded(chargewire, duck, "ATTACH IF NOT EXISTS ':memory:' AS ro_reads_other")
    with unchanged(subject=ro, tables=[table]):
        refused(chargewire, ro, sql.format(t=table, c=catalog))


@pytest.mark.parametrize("parameters,placeholder", [([None], "?"), ({"p": None}, "$p")])
def test_null_parameter_inference_is_screened_before_binding(chargewire, duck, ro, unchanged,
                                                           parameters, placeholder):
    sequence = name("null_parameter_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {sequence}")
    with unchanged(subject=ro):
        for expression in (f"nextval('{sequence}')", f"setval('{sequence}', 50)"):
            response = chargewire.submit(session_id=ro,
                                         sql=f"SELECT * FROM range({expression}) WHERE {placeholder} IS NULL",
                                         parameters=parameters)
            assert_problem(response=response, status=403, code="read_only")
        assert rows(chargewire=chargewire, session_id=ro,
                    sql=f"SELECT {placeholder}::INTEGER IS NULL AS is_null", parameters=parameters) == [{"is_null": True}]
    assert one(chargewire=chargewire, session_id=duck, sql=f"SELECT nextval('{sequence}')") == 1


def test_import_database_changes_nothing(chargewire, duck, ro, table, tmp_path, unchanged):
    # IMPORT DATABASE is DuckDB's pragma that reads a directory's schema.sql and load.sql and
    # runs what they say. Whatever a read-only session is answered, none of it is run.
    export = tmp_path / "export"
    export.mkdir()
    (export / "schema.sql").write_text(f"CREATE TABLE ro_imported (i INTEGER);\nINSERT INTO {table} VALUES (9, 'imported');\n")
    (export / "load.sql").write_text(f"INSERT INTO {table} VALUES (9, 'loaded');\n")
    with unchanged(subject=ro, tables=[table]):
        for directory in (export, tmp_path / "not_there"):
            response = chargewire.submit(session_id=ro, sql=f"IMPORT DATABASE '{directory}'")
            assert response.status_code >= 400, response.text


def test_the_twins_of_the_refused_kinds_work_in_a_read_write_session(chargewire, duck, table, tmp_path):
    # So the refusals above are of statements that work: a sample of each family, cleaned up.
    for sql in (f"COPY {table} TO '{tmp_path}/copy.csv'", f"EXPLAIN ANALYZE COPY {table} TO '{tmp_path}/explained.csv'",
                "ATTACH ':memory:' AS ro_twin", "DETACH ro_twin", "CHECKPOINT", "CALL checkpoint()", "ANALYZE",
                "CREATE TEMP VIEW ro_twin_view AS SELECT 1", "DROP VIEW ro_twin_view",
                "PREPARE ro_twin_prepared AS SELECT 1", "EXECUTE ro_twin_prepared", "DEALLOCATE ro_twin_prepared",
                "CREATE SEQUENCE ro_twin_sequence", "SELECT nextval('ro_twin_sequence')", "SELECT setval('ro_twin_sequence', 9)",
                "DROP SEQUENCE ro_twin_sequence", "EXPLAIN ANALYZE CREATE TEMP TABLE ro_twin_explained AS SELECT 1 AS i",
                "DROP TABLE ro_twin_explained", "CREATE TEMP TABLE ro_twin_temp AS SELECT 1 AS i", "DROP TABLE ro_twin_temp"):
        response = chargewire.submit(session_id=duck, sql=sql)
        assert response.status_code in (200, 201), (sql, response.text)
    assert sorted(os.listdir(tmp_path)) == ["copy.csv", "explained.csv"]


@pytest.mark.parametrize("sql", [
    "INSERT INTO ro_no_such_table VALUES (1)", "UPDATE ro_no_such_table SET i = 1", "DELETE FROM ro_no_such_table",
    "DROP TABLE ro_no_such_table", "ALTER TABLE ro_no_such_table ADD COLUMN j INTEGER", "CREATE VIEW ro_no_such_view AS SELECT * FROM ro_no_such_table",
    "COPY ro_no_such_table TO '{tmp}/missing.csv'", "EXECUTE ro_no_such_prepared", "CALL ro_no_such_function()",
    "PIVOT ro_no_such_table ON a USING sum(b)", "EXPLAIN PIVOT ro_no_such_table ON a USING sum(b)",
])
def test_a_refused_kind_is_refused_before_anything_of_the_statement_is_looked_up(chargewire, duck, ro, tmp_path, unchanged, sql):
    # The kind is the parser's word, and the refusal comes before DuckDB binds the statement:
    # binding is where names are resolved and the arguments of table functions are evaluated.
    # So a write to a table that is not there is refused as a write, where a read-write
    # session is told that the table is missing.
    sql = sql.format(tmp=tmp_path)
    with unchanged(subject=ro):
        refused(chargewire, ro, sql)
    twin = chargewire.submit(session_id=duck, sql=sql)
    assert_problem(response=twin, status=400, code="sql_error")


# ---- A PIVOT that takes its values from the data ----
#
# DuckDB runs such a PIVOT as two statements: a query for the values (it makes a temporary
# enum type of them) and then the pivot. An earlier server checked only the second: any table
# function in the first ran unchecked, twice, also when the answer was 403. Such a PIVOT is
# refused by its kind now, with the values written out it is a plain query.

DATA_PIVOTS = [
    "PIVOT {t} ON v USING count(*)",
    "PIVOT {t} ON v USING count(*) GROUP BY id",
    "PIVOT_WIDER {t} ON v USING count(*)",
    "FROM (PIVOT {t} ON v USING count(*))",
    "SELECT * FROM (PIVOT {t} ON v USING count(*)) p",
    "WITH c AS (PIVOT {t} ON v USING count(*)) SELECT * FROM c",
    "PIVOT {t} ON v IN (SELECT v FROM {t}) USING count(*)",
    "EXPLAIN PIVOT {t} ON v USING count(*)",
    "EXPLAIN ANALYZE PIVOT {t} ON v USING count(*)",
    "DESCRIBE PIVOT {t} ON v USING count(*)",
    "SUMMARIZE PIVOT {t} ON v USING count(*)",
    "SET VARIABLE ro_pivoted = (SELECT count(*) FROM (PIVOT {t} ON v USING count(*)))",
]


@pytest.mark.parametrize("sql", DATA_PIVOTS)
def test_a_pivot_that_takes_its_values_from_the_data_is_refused(chargewire, duck, ro, table, unchanged, sql):
    with unchanged(subject=ro, tables=[table]):  # the session's temporary objects too: no enum type is left
        problem = refused(chargewire, ro, sql.format(t=table))
        assert "PIVOT" in problem["detail"] and "IN (" in problem["detail"], problem  # it says what to write instead
        prepared = prepare(chargewire, ro, sql.format(t=table))
        assert_problem(response=prepared, status=403, code="read_only")
    succeeded(chargewire, duck, sql.format(t=table))  # the twin


# The same with a table function in the part DuckDB runs first, in its list of values or in
# its source. {f} is the function: in the twin one that reads, so the statement is one that
# works, and in the read-only session each of the ones that must not run.
PIVOTS_OVER_A_FUNCTION = [
    "PIVOT {t} ON v IN (SELECT 'k' FROM (SELECT count(*) FROM {f})) USING count(*)",
    "PIVOT {t} ON v IN (SELECT v FROM {t}, (SELECT count(*) FROM {f})) USING count(*)",
    "PIVOT (SELECT id, v FROM {t}, (SELECT count(*) AS c FROM {f}) e) ON v USING count(*)",
    "DESCRIBE PIVOT {t} ON v IN (SELECT v FROM {t}, (SELECT count(*) FROM {f})) USING count(*)",
    "EXPLAIN PIVOT {t} ON v IN (SELECT 'k' FROM (SELECT count(*) FROM {f})) USING count(*)",
    "SET VARIABLE ro_pivoted = (SELECT count(*) FROM (PIVOT {t} ON v IN (SELECT v FROM {t}, (SELECT count(*) FROM {f})) USING count(*)))",
]
FUNCTIONS_THAT_CHANGE_THE_SERVER = ["enable_logging(level = 'debug')", "disable_logging()", "truncate_duckdb_logs()",
                                    "checkpoint()", "force_checkpoint()", "enable_profiling()"]


@pytest.mark.parametrize("sql", PIVOTS_OVER_A_FUNCTION)
def test_a_function_inside_a_pivot_does_not_run(chargewire, duck, ro, table, unchanged, logging_stays_off, sql):
    succeeded(chargewire, duck, sql.format(t=table, f="range(1)"))  # the twin: the statement is one that works
    with unchanged(subject=ro, tables=[table]):
        for function in FUNCTIONS_THAT_CHANGE_THE_SERVER + ["duckdb_secrets()", "range(1)"]:
            refused(chargewire, ro, sql.format(t=table, f=function))
            assert not logging_is_on(chargewire, duck), function


def test_a_pivot_with_its_values_written_out_works(chargewire, duck, ro, table):
    expected = [{"id": 1, "a": 1, "b": 0}, {"id": 2, "a": 0, "b": 1}, {"id": 3, "a": 0, "b": 1}]
    for sql in (f"PIVOT {table} ON v IN ('a', 'b') USING count(*) ORDER BY id",
                f"SELECT * FROM {table} PIVOT (count(*) FOR v IN ('a', 'b')) ORDER BY id",
                f"FROM (PIVOT {table} ON v IN ('a', 'b') USING count(*)) ORDER BY id"):
        assert rows(chargewire, ro, sql) == expected == rows(chargewire, duck, sql), sql
    for sql in (f"EXPLAIN PIVOT {table} ON v IN ('a', 'b') USING count(*)", f"EXPLAIN ANALYZE PIVOT {table} ON v IN ('a', 'b') USING count(*)",
                f"DESCRIBE PIVOT {table} ON v IN ('a', 'b') USING count(*)", "UNPIVOT (SELECT 1 AS a, 2 AS b) ON a, b",
                "UNPIVOT (SELECT 1 AS a, 2 AS b) ON COLUMNS(*)"):
        assert answer_of(chargewire, ro, sql)[0] == 200 == answer_of(chargewire, duck, sql)[0], sql


# ---- Writes inside statements whose kind is allowed ----


def test_a_sequence_is_not_advanced_or_set(chargewire, duck, ro, table, unchanged):
    sequence = name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {sequence}")
    succeeded(chargewire, duck, f"CREATE VIEW {sequence}_view AS SELECT nextval('{sequence}') AS n")
    succeeded(chargewire, duck, f"CREATE MACRO {sequence}_macro() AS nextval('{sequence}')")
    with unchanged(subject=ro, tables=[table]):  # the snapshot has every sequence's last value
        for sql in (f"SELECT nextval('{sequence}')", f"SELECT * FROM {sequence}_view", f"SELECT {sequence}_macro()",
                    f"SELECT * FROM query('SELECT nextval(''{sequence}'')')",
                    f"SET VARIABLE ro_v = (SELECT nextval('{sequence}'))",
                    f"SELECT setval('{sequence}', 1000)", f"SELECT setval('{sequence}', 1000, false)",
                    f"SELECT list(nextval('{sequence}')) FROM range(3)", f"SELECT id FROM {table} WHERE nextval('{sequence}') > 0",
                    f"SELECT id FROM {table} ORDER BY nextval('{sequence}')",
                    f"EXPLAIN ANALYZE SELECT nextval('{sequence}')", f"EXPLAIN SELECT nextval('{sequence}')",
                    f"SELECT * FROM query('SELECT setval(''{sequence}'', 7)')"):
            refused(chargewire, ro, sql)
    assert one(chargewire, duck, f"SELECT nextval('{sequence}')") == 1  # the twin, and the proof nothing advanced it
    assert one(chargewire, duck, f"SELECT setval('{sequence}', 1000)") == 1000
    assert one(chargewire, duck, f"SELECT nextval('{sequence}')") == 1001


# ---- What DuckDB evaluates while it binds a statement ----
#
# The arguments of a table function, the value of a setting, the parameters of a pragma and
# a time-travel clause are evaluated while DuckDB binds the statement, before any plan exists
# that could be refused: an earlier server answered 403 to `SELECT * FROM range(nextval('s'))`
# and the sequence had moved. Before DuckDB binds anything of a read-only session's statement,
# the server screens it: such an expression holds plain values only. The same for the
# definition of every view and macro the statement reaches, since DuckDB binds those with it.


@pytest.fixture
def made(chargewire, duck):
    """`made("VIEW", name, "CREATE VIEW ...")`: something a read-write session makes for the
    test and drops after it. A view whose definition does not pass the screen changes what
    every read-only session of the server is answered while it exists (and a macro named
    like a function does), so none is left for the tests that follow."""
    kept = []

    def make(kind: str, qualified_name: str, sql: str) -> None:
        succeeded(chargewire, duck, sql)
        if (kind, qualified_name) not in kept:
            kept.append((kind, qualified_name))
    yield make
    for kind, qualified_name in reversed(kept):
        succeeded(chargewire, duck, f"DROP {kind} IF EXISTS {qualified_name}")


def placements(seq: str, t: str, pv: str, tmp: str) -> list[str]:
    f = f"range(nextval('{seq}'))"
    deep = f"(SELECT * FROM {f}) d0"
    for i in range(1, 40):
        deep = f"(SELECT * FROM {deep}) d{i}"
    return [
        f"SELECT * FROM {f}", f"FROM {f}", f"SELECT * FROM range(setval('{seq}', 50))", f"SELECT * FROM range(setval('{seq}', 50, false))",
        f"SELECT * FROM generate_series(1, nextval('{seq}'))", f"SELECT * FROM repeat('x', nextval('{seq}'))", f"SELECT * FROM unnest([nextval('{seq}')])",
        f"SELECT * FROM {t} JOIN {f} r ON true", f"SELECT * FROM {f} r JOIN {t} ON true", f"SELECT * FROM (SELECT * FROM {f}) s",
        f"WITH c AS (SELECT * FROM {f}) SELECT * FROM c", f"WITH c AS MATERIALIZED (SELECT * FROM {f}) SELECT 1",
        f"SELECT (SELECT count(*) FROM {f})", f"SELECT * FROM {t} WHERE id IN (SELECT * FROM {f})", f"SELECT * FROM {t} WHERE EXISTS (SELECT 1 FROM {f})",
        f"SELECT id FROM {t} UNION ALL SELECT * FROM {f}", f"SELECT * FROM {f} UNION ALL SELECT id FROM {t}",
        f"SELECT * FROM {t}, LATERAL (SELECT * FROM {f})", f"SELECT * FROM {t} t, LATERAL range(nextval('{seq}') + t.id)",
        f"PIVOT (SELECT a, b FROM {pv}, {f}) ON a IN ('x', 'y') USING sum(b)", f"SELECT * FROM (VALUES ((SELECT count(*) FROM {f}))) v(n)",
        f"VALUES ((SELECT count(*) FROM {f}))", f"SELECT * FROM {t} ORDER BY (SELECT count(*) FROM {f})", f"SELECT * FROM {t} LIMIT (SELECT count(*) FROM {f})",
        f"SELECT * FROM {t} QUALIFY row_number() OVER () IN (SELECT * FROM {f})",
        f"EXPLAIN SELECT * FROM {f}", f"EXPLAIN ANALYZE SELECT * FROM {f}", f"DESCRIBE SELECT * FROM {f}", f"SUMMARIZE SELECT * FROM {f}",
        f"SET VARIABLE ro_x = (SELECT count(*) FROM {f})", f"SET progress_bar_time = nextval('{seq}')", f"SET SESSION progress_bar_time = setval('{seq}', 9)",
        f"PRAGMA table_info(nextval('{seq}'))", f"SELECT * FROM read_csv('{tmp}/no_such_' || nextval('{seq}') || '.csv')",
        f"SELECT * FROM duckdb_table_sample('{t}' || nextval('{seq}'))", f"SELECT * FROM json_each(json_array(nextval('{seq}')))",
        # what is not a plain value is refused whatever it is: no list of the functions that change something is kept
        "SELECT * FROM range(CASE WHEN random() < 0.5 THEN 1 ELSE 2 END)", "SELECT * FROM range(greatest(1, 2))", "SELECT * FROM range(length('abc'))",
        f"SELECT count(*) FROM {deep}",  # forty derived tables down
        # functions that take SQL text or a table's name: what DuckDB then binds is not in the statement
        "SELECT * FROM query('SELECT 1')", f"SELECT * FROM query_table('{t}')", f"SELECT * FROM query_table({t})",
        f"SELECT * FROM query('SELECT * FROM range(nextval(''{seq}''))')", f"SELECT (SELECT count(*) FROM query('SELECT * FROM range(nextval(''{seq}''))'))",
        # (not while binding, but refused by the plan all the same)
        f"SELECT * FROM range((SELECT nextval('{seq}')))", f"SELECT * FROM {t} LIMIT nextval('{seq}')", f"SELECT unnest([nextval('{seq}')])",
    ]


def test_what_duckdb_evaluates_while_it_binds_holds_plain_values_only(chargewire, duck, ro, table, tmp_path, unchanged):
    seq, pv = name("ro_seq"), name("ro_pv")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    succeeded(chargewire, duck, f"CREATE TABLE {pv} AS SELECT * FROM (VALUES ('x', 1), ('y', 2)) v(a, b)")
    with unchanged(subject=ro, tables=[table]):  # the snapshot has every sequence's last value
        for sql in placements(seq, table, pv, str(tmp_path)):
            refused(chargewire, ro, sql)
        # Preparing a statement describes it, and describing binds it.
        for sql in (f"SELECT * FROM range(nextval('{seq}'))", f"SELECT * FROM range(setval('{seq}', 50)) WHERE ? = 1",
                    f"SELECT (SELECT count(*) FROM range(nextval('{seq}')))", f"SELECT * FROM query('SELECT * FROM range(nextval(''{seq}''))')"):
            assert_problem(response=prepare(chargewire, ro, sql), status=403, code="read_only")
    # The twins, and the proof that nothing advanced the sequence.
    assert one(chargewire, duck, f"SELECT nextval('{seq}')") == 1
    for sql in (f"SELECT * FROM range(nextval('{seq}'))", "SELECT * FROM query('SELECT 1')", f"SELECT * FROM query_table('{table}')",
                "SELECT * FROM range(greatest(1, 2))", f"SELECT * FROM range(setval('{seq}', 50))"):
        assert answer_of(chargewire, duck, sql)[0] == 200, sql
    assert one(chargewire, duck, f"SELECT nextval('{seq}')") == 51


# What DuckDB binds under another statement, and the SQL it binds that a statement does not
# hold. An earlier server screened a query and what an EXPLAIN held when that was a query; it
# decided for everything else by the outer statement, and answered `403 read_only` to each of
# these after the sequence had moved (to a value of the reader's choice, with setval):
#   - what an EXPLAIN holds when it is not a query (CALL, INSERT, UPDATE, DELETE, MERGE, COPY,
#     PREPARE, EXECUTE): DuckDB binds it all the same;
#   - the file name and the options of a COPY, as an EXPLAIN's statement or a CTE's body;
#   - what a PIVOT is pivoted on;
#   - a serialized statement (json_execute_serialized_sql, as a table function and as a
#     pragma) and SQL text given to a scalar function that plans it (json_serialize_plan).
# The server now walks every kind of statement itself and refuses a kind it has no rule for,
# binds a table function or a pragma only when it is listed, and every one of these is refused
# before DuckDB binds anything of it.
# (What DuckDB binds through a view or macro, whoever wrote it: the tests after these.)


def bound_under_another_statement(seq: str, t: str, pv: str, tmp: str, n: str) -> list[str]:
    f, g = f"range(nextval('{seq}'))", f"range(setval('{seq}', 4242))"
    return [
        # what an EXPLAIN holds
        f"EXPLAIN CALL {f}", f"EXPLAIN CALL {g}", f"EXPLAIN (FORMAT JSON) CALL {f}", f"EXPLAIN ANALYZE CALL {f}",
        f"EXPLAIN INSERT INTO {t} SELECT range, 'x' FROM {f}", f"EXPLAIN INSERT INTO {t} VALUES ((SELECT count(*) FROM {g}), 'x')",
        f"EXPLAIN UPDATE {t} SET v = 'q' FROM {f} r WHERE {t}.id = r.range", f"EXPLAIN DELETE FROM {t} USING {f} r WHERE {t}.id = r.range",
        f"EXPLAIN MERGE INTO {t} USING {f} s ON {t}.id = s.range WHEN MATCHED THEN DELETE",
        f"EXPLAIN MERGE INTO {t} USING (SELECT 1 AS id) s ON {t}.id = s.id WHEN MATCHED THEN UPDATE SET v = (SELECT count(*)::VARCHAR FROM {f})",
        f"EXPLAIN COPY (SELECT * FROM {f}) TO '{tmp}/never.csv'", f"EXPLAIN COPY {t} TO '{tmp}/never.csv' (DELIMITER (nextval('{seq}')::VARCHAR))",
        f"EXPLAIN COPY {t} TO ('{tmp}/never' || nextval('{seq}') || '.csv')", f"EXPLAIN COPY {t} FROM ('{tmp}/never' || setval('{seq}', 7) || '.csv')",
        f"EXPLAIN COPY {n}_bad_view TO '{tmp}/never.csv'",
        f"EXPLAIN PREPARE ro_never AS SELECT * FROM {f}", f"EXPLAIN EXECUTE ro_prepared_before(nextval('{seq}'))",
        f"EXPLAIN CREATE TABLE ro_never AS SELECT * FROM {f}", f"EXPLAIN CREATE VIEW ro_never AS SELECT * FROM {f}",
        f"EXPLAIN SET VARIABLE ro_x = (SELECT count(*) FROM {f})", f"EXPLAIN SET progress_bar_time = nextval('{seq}')",
        # a CTE that is not a query
        f"WITH c AS (COPY (SELECT 1 AS i) TO '{tmp}/never.csv' (DELIMITER (nextval('{seq}')::VARCHAR))) SELECT 1",
        f"WITH c AS (COPY (SELECT 1 AS i) TO ('{tmp}/never' || nextval('{seq}') || '.csv')) SELECT 1",
        f"WITH c AS (COPY (SELECT * FROM {f}) TO '{tmp}/never.csv') SELECT 1", f"WITH c AS (COPY {n}_bad_view TO '{tmp}/never.csv') SELECT 1",
        f"WITH c AS (INSERT INTO {t} SELECT range, 'x' FROM {f} RETURNING *) SELECT * FROM c",
        f"WITH c AS (UPDATE {t} SET v = 'q' FROM {f} r WHERE {t}.id = r.range RETURNING *) SELECT * FROM c",
        f"WITH c AS (DELETE FROM {t} USING {f} r WHERE {t}.id = r.range RETURNING *) SELECT * FROM c",
        # what a PIVOT is pivoted on
        f"SELECT * FROM {pv} PIVOT (sum(b) FOR (SELECT a || count(*) FROM {f}) IN ('x1', 'y1'))",
        # SQL that is not in the statement
        f"SELECT json_serialize_plan('SELECT * FROM range(nextval(''{seq}''))')", f"SELECT json_serialize_plan(v) FROM {t}",
        f"SELECT (SELECT json_serialize_plan('SELECT * FROM range(setval(''{seq}'', 9))'))",
        # functions and pragmas that are on no list are not bound at all: their bind step is theirs to write
        "PRAGMA tpch(1)", f"PRAGMA import_database('{tmp}')", "PRAGMA copy_database('memory', 'memory')", "PRAGMA ro_no_such_pragma",
        "SELECT * FROM test_vector_types(NULL::INTEGER)", "SELECT * FROM tpch_queries()",
        "SELECT * FROM postgres_query('ro_no_such_database', 'SELECT 1')", "EXPLAIN SELECT * FROM sqlite_query('x', 'SELECT 1')",
        "EXPLAIN PRAGMA table_info('x')", "EXPLAIN LOAD tpch", "EXPLAIN VACUUM", "EXPLAIN BEGIN", f"EXPLAIN DROP TABLE {t}",
        f"EXPLAIN ALTER TABLE {t} ALTER COLUMN id SET DEFAULT nextval('{seq}')",
    ]


@pytest.fixture
def bound_under(chargewire, duck, table, made, tmp_path):
    """The sequence, the views and the macros those statements name; `made` drops them."""
    n, seq, pv = name("ro_under"), name("ro_seq"), name("ro_pv")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    succeeded(chargewire, duck, f"CREATE TABLE {pv} AS SELECT * FROM (VALUES ('x', 1), ('y', 2)) v(a, b)")
    made("MACRO", f"{n}_plain", f"CREATE MACRO {n}_plain(a) AS a + 1")
    made("MACRO TABLE", f"{n}_rows", f"CREATE MACRO {n}_rows(k) AS TABLE SELECT * FROM range(k)")
    yield n, seq, pv
    succeeded(chargewire, duck, f"DROP TABLE {pv}")


def sequence_state(chargewire, session_id: str, seq: str):
    return one(chargewire, session_id, f"SELECT last_value FROM system.main.duckdb_sequences() WHERE sequence_name = '{seq}'")


def test_what_duckdb_binds_under_another_statement_is_refused_before_it_is_bound(chargewire, duck, ro, table, bound_under, made,
                                                                                tmp_path, unchanged):
    n, seq, pv = bound_under
    made("VIEW", f"{n}_bad_view", f"CREATE VIEW {n}_bad_view AS SELECT * FROM range(nextval('{seq}'))")  # (creating it binds it once)
    before = sequence_state(chargewire, duck, seq)
    with unchanged(subject=ro, tables=[table]):  # the snapshot has every sequence's last value, the files and the loaded extensions
        for sql in bound_under_another_statement(seq, table, pv, str(tmp_path), n):
            refused(chargewire, ro, sql)
            assert sequence_state(chargewire, duck, seq) == before, sql
            # Preparing a statement describes it, and describing binds it: never executed, same refusal.
            assert_problem(response=prepare(chargewire, ro, sql), status=403, code="read_only")
            assert sequence_state(chargewire, duck, seq) == before, "prepare " + sql
        # A table function that does not exist is DuckDB's to say (the server refuses a function
        # that is on none of its lists when DuckDB comes to bind it, wherever the call stands).
        assert_problem(response=chargewire.submit(session_id=ro, sql="SELECT * FROM ro_no_such_function()"), status=400, code="sql_error")
    assert sorted(os.listdir(tmp_path)) == []
    assert one(chargewire, duck, f"SELECT nextval('{seq}')") == (before or 0) + 1  # what a writer gets next: nothing moved it


def test_the_statements_next_to_those_still_work(chargewire, duck, ro, table, bound_under, tmp_path, unchanged, logging_stays_off):
    # The twins. In the read-only session: what binds nothing of that kind is answered as before.
    n, seq, pv = bound_under
    with unchanged(subject=ro, tables=[table]):
        for sql in ("EXPLAIN CALL range(3)", "EXPLAIN (FORMAT JSON) CALL range(3)", "EXPLAIN CALL checkpoint()", "EXPLAIN CHECKPOINT",
                    f"EXPLAIN SELECT * FROM {table}", f"EXPLAIN ANALYZE SELECT count(*) FROM {table}", "EXPLAIN SELECT * FROM checkpoint()",
                    f"EXPLAIN COPY {table} TO '{tmp_path}/explained.csv'", f"EXPLAIN COPY {table} TO '{tmp_path}/explained.csv' (DELIMITER '|', HEADER)",
                    f"EXPLAIN COPY (SELECT * FROM {table} WHERE id > 1) TO '{tmp_path}/explained.parquet' (FORMAT parquet, COMPRESSION zstd)",
                    f"EXPLAIN COPY {table} TO '{tmp_path}/explained' (FORMAT parquet, PARTITION_BY (v))",
                    f"EXPLAIN WITH c AS (COPY (SELECT 1 AS i) TO '{tmp_path}/explained.csv') SELECT * FROM c",
                    "EXPLAIN SET progress_bar_time = 100", "EXPLAIN RESET progress_bar_time", "EXPLAIN SET VARIABLE ro_x = 1",
                    f"SELECT * FROM {pv} PIVOT (sum(b) FOR a IN ('x', 'y'))", f"SELECT * FROM {pv} PIVOT (sum(b) FOR (a || '1') IN ('x1', 'y1'))",
                    f"SELECT json_serialize_sql('SELECT * FROM range(nextval(''{seq}''))')",
                    f"SELECT * FROM histogram({table}, id)", "SELECT count(*) FROM duckdb_logs_parsed('QueryLog')",
                    "PRAGMA version", f"PRAGMA table_info('{table}')", "PRAGMA database_list",
                    f"SELECT {n}_plain(length(v)) FROM {table}", f"SELECT {n}_plain(nullif(id, 0)) FROM {table}"):
            assert answer_of(chargewire, ro, sql)[0] == 200, sql
        # A table macro of a user's is on no list and is called all the same: its definition is followed.
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {n}_rows(3)") == [{"n": 3}]
        assert answer_of(chargewire, ro, f"EXPLAIN SELECT * FROM {n}_rows(3)")[0] == 200
        # What changes a catalog is refused under an EXPLAIN as it is without one, plain values or not.
        for sql in (f"EXPLAIN INSERT INTO {table} VALUES (4, 'c')", f"EXPLAIN UPDATE {table} SET v = 'z'", f"EXPLAIN DELETE FROM {table}",
                    f"EXPLAIN COPY {table} FROM '{tmp_path}/in.csv'"):
            refused(chargewire, ro, sql)
    assert sorted(os.listdir(tmp_path)) == []
    assert sequence_state(chargewire, duck, seq) is None
    # In a read-write session each of the refused statements is one DuckDB answers, and binding
    # them does what the refusals above are for: the sequence moves, though none of them is executed.
    for sql in (f"EXPLAIN INSERT INTO {table} VALUES (4, 'c')", f"EXPLAIN UPDATE {table} SET v = 'z'", f"EXPLAIN DELETE FROM {table}",
                f"EXPLAIN MERGE INTO {table} USING (SELECT 1 AS id) s ON {table}.id = s.id WHEN MATCHED THEN DELETE",
                "EXPLAIN PREPARE ro_never AS SELECT 1", f"SELECT * FROM {table} JOIN (SELECT 1) ON true"):
        assert answer_of(chargewire, duck, sql)[0] == 200, sql
    moved = 0
    for sql in (f"EXPLAIN COPY {table} TO '{tmp_path}/never.csv' (DELIMITER (nextval('{seq}')::VARCHAR))",  # (first: a delimiter of one character)
                f"EXPLAIN CALL range(nextval('{seq}'))", f"EXPLAIN INSERT INTO {table} SELECT range, 'x' FROM range(nextval('{seq}'))",
                f"EXPLAIN COPY (SELECT * FROM range(nextval('{seq}'))) TO '{tmp_path}/never.csv'",
                f"SELECT length(json_serialize_plan('SELECT * FROM range(nextval(''{seq}''))')) > 0",
                f"SELECT * FROM {pv} PIVOT (sum(b) FOR (SELECT a || 1 + 0 * count(*) FROM range(nextval('{seq}'))) IN ('x1', 'y1'))"):
        # (whatever DuckDB then answers: some of them it fails itself, after it has bound them)
        assert chargewire.submit(session_id=duck, sql=sql).status_code != 403, sql
        now = sequence_state(chargewire, duck, seq)
        assert now is not None and now > moved, sql
        moved = now
    assert one(chargewire, duck, f"SELECT count(*) FROM {table}") == 3 and sorted(os.listdir(tmp_path)) == []


def serialized_statement(chargewire, session_id: str, sql: str) -> str:
    return one(chargewire, session_id, "SELECT json_serialize_sql(?)", [sql])


@pytest.mark.parametrize("function", ["nextval('{seq}')", "setval('{seq}', 4242)"])
def test_a_serialized_statement_is_not_bound(chargewire, duck, ro, table, unchanged, function):
    # json_execute_serialized_sql binds the statement it is given on a connection of its own,
    # which no read-only session's guard sees: as a table function with the JSON as a literal
    # or as a bind parameter, and as a pragma, which DuckDB replaces by the statement's text.
    seq = name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    serialized = serialized_statement(chargewire, duck, f"SELECT * FROM range({function.format(seq=seq)})")
    literal = "'" + serialized.replace("'", "''") + "'"
    with unchanged(subject=ro, tables=[table]):
        for sql, parameters in ((f"SELECT * FROM json_execute_serialized_sql({literal})", None), ("SELECT * FROM json_execute_serialized_sql(?)", [serialized]),
                                (f"SELECT * FROM system.main.json_execute_serialized_sql({literal})", None),
                                (f"EXPLAIN SELECT * FROM json_execute_serialized_sql({literal})", None),
                                (f"SELECT (SELECT count(*) FROM json_execute_serialized_sql({literal}))", None),
                                (f"PRAGMA json_execute_serialized_sql({literal})", None), (f"CALL json_execute_serialized_sql({literal})", None),
                                (f"EXPLAIN CALL json_execute_serialized_sql({literal})", None)):
            refused(chargewire, ro, sql, parameters)
            assert sequence_state(chargewire, duck, seq) is None, sql
            assert_problem(response=prepare(chargewire, ro, sql), status=403, code="read_only")
            assert sequence_state(chargewire, duck, seq) is None, "prepare " + sql
        # The twin: the text of a statement is a value like any other.
        assert one(chargewire, ro, f"SELECT length({literal}) > 0") is True
        assert one(chargewire, ro, f"SELECT json_deserialize_sql({literal}::JSON) LIKE 'SELECT%'") is True
    # In a read-write session the function runs a statement it is given, and binding this one
    # moves the sequence there (DuckDB then fails the function's own transaction: the answer is
    # its error, not a refusal): the proof that it is this the refusals keep out.
    plain = "'" + serialized_statement(chargewire, duck, "SELECT 42 AS answer").replace("'", "''") + "'"
    assert answer_of(chargewire, duck, f"SELECT * FROM json_execute_serialized_sql({plain})") == (200, [{"answer": 42}])
    assert answer_of(chargewire, duck, f"SELECT * FROM json_execute_serialized_sql({literal})")[1] != "read_only"
    assert sequence_state(chargewire, duck, seq) is not None


def test_a_statement_prepared_in_sql_before_the_switch_is_not_reached_through_explain(chargewire, duck, unchanged):
    # EXECUTE evaluates its arguments while it binds, and binds a statement the server has
    # not screened: refused under an EXPLAIN as it is without one.
    seq = name("ro_seq")
    sid = chargewire.new_session()
    try:
        succeeded(chargewire, sid, f"CREATE SEQUENCE {seq}")
        succeeded(chargewire, sid, "PREPARE ro_prepared_before AS SELECT $1 AS x")
        assert answer_of(chargewire, sid, "EXPLAIN EXECUTE ro_prepared_before(1)")[0] == 200  # the twin, while it is read-write
        assert patch(chargewire, sid, {"read_only": True})["errors"] == {}
        with unchanged(subject=sid):
            for sql in (f"EXPLAIN EXECUTE ro_prepared_before(nextval('{seq}'))", f"EXPLAIN EXECUTE ro_prepared_before(setval('{seq}', 9))",
                        "EXPLAIN EXECUTE ro_prepared_before(1)", f"EXECUTE ro_prepared_before(nextval('{seq}'))", "EXPLAIN DEALLOCATE ro_prepared_before"):
                refused(chargewire, sid, sql)
                assert_problem(response=prepare(chargewire, sid, sql), status=403, code="read_only")
        assert sequence_state(chargewire, sid, seq) is None
    finally:
        chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


# What DuckDB binds that no statement shows: the definitions of views and macros, whoever
# wrote them and whether or not the session's own listing shows them, and views that a
# catalog function binds only when it runs. An earlier server read the definitions it could
# list before it bound a statement and refused what it found there; DuckDB bound more than
# that list held, and a read-only session moved a sequence through:
#   - a view created WITH (DEFER_BINDING), bound by duckdb_columns() when it runs
#     (information_schema.columns, SHOW ALL TABLES, DESCRIBE, the server's table listing);
#   - a view with a dot in its name, quoted inside a string argument;
#   - a view or macro made in a DuckLake after the session's first statement;
#   - a macro's argument that its definition passes to a table function;
#   - a view named as a column (histogram(view, column)).
# Now nothing is listed in advance. DuckDB itself refuses a sequence's use in a read-only
# session's transaction where it happens, and asks the server before the bind step of any
# table function, so each of these changes nothing however the definition is reached; and
# no view or macro of anyone's changes what another statement is answered.


@pytest.fixture
def bound_later(chargewire, duck, table, made):
    """Views and macros whose definitions would advance or set the sequence, or call a table
    function a read-only session may not: each reached in a way no listing foresees."""
    n, seq = name("ro_later"), name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    made("VIEW", f'"{n}.bad"', f"CREATE VIEW \"{n}.bad\" AS SELECT * FROM range(nextval('{seq}'))")  # (creating it binds it once)
    made("VIEW", f'"{n}.good"', f'CREATE VIEW "{n}.good" AS SELECT id FROM {table} WHERE id > 1')
    made("VIEW", f"{n}_bad", f"CREATE VIEW {n}_bad AS SELECT * FROM range(setval('{seq}', 7))")
    made("VIEW", f"{n}_runs_sql", f"CREATE VIEW {n}_runs_sql AS SELECT * FROM query('SELECT 42 AS x')")
    made("MACRO", f"{n}_series", f"CREATE MACRO {n}_series(k) AS (SELECT count(*) FROM range(k))")
    made("MACRO", f"{n}_twice", f"CREATE MACRO {n}_twice(k) AS {n}_series(k) + 1")
    made("MACRO", f"{n}_rows", f"CREATE MACRO {n}_rows(p) AS (SELECT count(*) FROM read_csv(p))")
    return n, seq


def test_a_view_bound_only_when_a_catalog_function_runs_changes_nothing(chargewire, duck, ro, table, bound_later, made):
    # (Not inside the usual snapshot: taking it lists the catalog in a read-write session,
    # which binds the deferred views itself, once and for every session. The sequence is read
    # through DuckDB's function for sequences, which binds no view.)
    n, seq = bound_later
    made("VIEW", f"{n}_deferred", f"CREATE VIEW {n}_deferred WITH (DEFER_BINDING) AS SELECT * FROM range(nextval('{seq}'))")
    made("VIEW", f"{n}_deferred_set", f"CREATE VIEW {n}_deferred_set WITH (DEFER_BINDING) AS SELECT * FROM range(setval('{seq}', 4242))")
    before = sequence_state(chargewire, duck, seq)
    # Each of these answers, as it does when no such view exists: DuckDB leaves a view it
    # cannot bind for the reader without columns, and nothing has moved.
    for sql in ("SELECT count(*) FROM information_schema.columns", "SHOW ALL TABLES", "DESCRIBE", "SELECT count(*) FROM duckdb_columns()",
                f"SELECT count(*) FROM information_schema.columns WHERE table_name = '{n}_deferred'"):
        assert answer_of(chargewire, ro, sql)[0] == 200, sql
        assert sequence_state(chargewire, duck, seq) == before, sql
        assert prepare(chargewire, ro, sql).status_code == 201, sql
        assert sequence_state(chargewire, duck, seq) == before, "prepare " + sql
    listed = listed_columns(chargewire, ro, table_name_filter_pattern=f"{n}%")  # the server's own table listing, with schemas
    assert listed[f"{n}_deferred"] == [] and listed[f"{n}_deferred_set"] == [] and listed[f"{n}.good"] == ["id"], listed
    assert sequence_state(chargewire, duck, seq) == before
    # Read by name, the view is refused.
    for sql in (f"SELECT * FROM {n}_deferred", f"SELECT * FROM {n}_deferred_set", f"DESCRIBE SELECT * FROM {n}_deferred"):
        refused(chargewire, ro, sql)
        kept_out_when_prepared(chargewire, ro, sql)
    assert sequence_state(chargewire, duck, seq) == before
    assert rows(chargewire, duck, f"SELECT count(*) AS n FROM {table}") == [{"n": 3}]
    # The twin: a read-write session's listing binds the deferred views, and that moves the sequence.
    assert answer_of(chargewire, duck, "SELECT count(*) FROM information_schema.columns")[0] == 200
    assert sequence_state(chargewire, duck, seq) != before


def test_a_view_named_inside_a_string_changes_nothing(chargewire, duck, ro, table, bound_later, unchanged):
    n, seq = bound_later
    before = sequence_state(chargewire, duck, seq)
    with unchanged(subject=ro, tables=[table]):
        for sql in (f"""SELECT * FROM pragma_table_info('"{n}.bad"')""", f"""PRAGMA table_info('"{n}.bad"')""",
                    f"""SELECT count(*) FROM pragma_table_info('main."{n}.bad"')""", f"""SELECT * FROM pragma_table_info('"{n}' || '.bad"')""",
                    f"SELECT * FROM histogram({n}_bad, range)", f"SELECT * FROM histogram_values({n}_bad, range)"):
            refused(chargewire, ro, sql)
            assert sequence_state(chargewire, duck, seq) == before, sql
            kept_out_when_prepared(chargewire, ro, sql)
            assert sequence_state(chargewire, duck, seq) == before, "prepare " + sql
        refused(chargewire, ro, "SELECT * FROM pragma_table_info(?)", [f'"{n}.bad"'])
        assert sequence_state(chargewire, duck, seq) == before
        # The twins: the same functions over a view that only reads, named the same ways.
        assert rows(chargewire, ro, f"""SELECT name FROM pragma_table_info('"{n}.good"')""") == [{"name": "id"}]
        assert rows(chargewire, ro, "SELECT name FROM pragma_table_info(?)", [f'"{n}.good"']) == [{"name": "id"}]
        assert rows(chargewire, ro, f"""SELECT name FROM pragma_table_info('"{n}' || '.good"')""") == [{"name": "id"}]
        assert answer_of(chargewire, ro, f"SELECT * FROM histogram({table}, id)")[0] == 200
    assert answer_of(chargewire, duck, f"""SELECT * FROM pragma_table_info('"{n}.bad"')""")[0] == 200  # a read-write session binds it
    assert sequence_state(chargewire, duck, seq) != before


def test_what_a_macro_or_view_of_someone_elses_binds_changes_nothing(chargewire, duck, ro, table, bound_later, tmp_path, unchanged,
                                                                    logging_stays_off):
    n, seq = bound_later
    before = sequence_state(chargewire, duck, seq)
    with unchanged(subject=ro, tables=[table]):
        for sql in (f"SELECT {n}_series(nextval('{seq}'))", f"SELECT {n}_series(setval('{seq}', 500))", f"SELECT {n}_twice(nextval('{seq}'))",
                    f"SELECT * FROM {table} WHERE id < {n}_series(nextval('{seq}'))", f"SELECT {n}_series(k := nextval('{seq}'))",
                    f"SELECT {n}_rows('{tmp_path}/never' || nextval('{seq}') || '.csv')", f"EXPLAIN SELECT {n}_series(nextval('{seq}'))",
                    # a table function a read-only session may not call, in a view: refused when DuckDB comes to bind it
                    f"SELECT * FROM {n}_runs_sql", f"EXPLAIN SELECT * FROM {n}_runs_sql", f"SELECT count(*) FROM {table}, {n}_runs_sql"):
            refused(chargewire, ro, sql)
            assert sequence_state(chargewire, duck, seq) == before, sql
            kept_out_when_prepared(chargewire, ro, sql)
            assert sequence_state(chargewire, duck, seq) == before, "prepare " + sql
        # The twins: the macros take what does not change anything, a function call included
        # (an earlier server could not tell, and a later one refused every function there).
        assert rows(chargewire, ro, f"SELECT {n}_series(3) AS a, {n}_series(1 + 2) AS b, {n}_twice(3) AS c, {n}_series(length('abc')) AS d") == \
            [{"a": 3, "b": 3, "c": 4, "d": 3}]
        assert rows(chargewire, ro, f"SELECT {n}_series(?) AS a", [4]) == [{"a": 4}]
        assert rows(chargewire, ro, f"SELECT id, {n}_series(id) AS a FROM {table} ORDER BY id") == [{"id": 1, "a": 1}, {"id": 2, "a": 2}, {"id": 3, "a": 3}]
        # And none of these views and macros changes what a statement that does not reach them
        # is answered: a computed argument, a bind parameter, a lateral unnest.
        assert rows(chargewire, ro, "SELECT count(*) AS n FROM range(?)", [3]) == [{"n": 3}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM pragma_table_info('{table}' || '')") == [{"n": 2}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {table}") == [{"n": 3}]
    assert sequence_state(chargewire, duck, seq) == before
    assert answer_of(chargewire, duck, f"SELECT {n}_series(nextval('{seq}'))")[0] == 200  # a read-write session: the sequence moves
    assert sequence_state(chargewire, duck, seq) != before


def test_a_view_named_like_one_of_duckdbs_own_stops_nobody(chargewire, duck, ro, table, made, unchanged):
    # `tables`, `columns`, `views` are views of DuckDB's own (information_schema), and names a
    # schema of someone's metadata has too. A server that refused every statement of every
    # read-only session while such a view had a definition its screen would not pass let
    # anyone who may create a view stop everyone else's readers. A view is only ever what a
    # statement that names it binds.
    schema = name("ro_meta")
    succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
    reader = chargewire.new_session(options={"read_only": True})
    try:
        assert rows(chargewire, reader, "SELECT 1 AS one") == [{"one": 1}]
        for view in ("tables", "columns", "views"):
            made("VIEW", f"{schema}.{view}", f"CREATE VIEW {schema}.{view} AS SELECT * FROM unnest(string_split('a,b', ','))")
        late = chargewire.new_session(options={"read_only": True})  # a session made while the views exist
        with unchanged(tables=[table]):
            for sid in (reader, ro, late):
                for sql in ("SELECT 1", f"SELECT count(*) FROM {table}", "SHOW TABLES", "SELECT count(*) FROM information_schema.tables",
                            "SELECT count(*) FROM information_schema.columns", f"PRAGMA table_info('{table}')", "SELECT count(*) FROM range(3)",
                            "EXPLAIN SELECT 1"):
                    assert answer_of(chargewire, sid, sql)[0] == 200, sql
                assert rows(chargewire, sid, "SELECT count(*) AS n FROM range(?)", [2]) == [{"n": 2}]
                assert prepare(chargewire, sid, f"SELECT count(*) FROM {table}").status_code == 201
                for path in ("tables", "catalogs", "db-schemas"):
                    response = chargewire.http.get(url=f"{chargewire.session_url(sid)}/metadata/{path}", headers=chargewire.headers())
                    assert response.status_code == 200, (path, response.text)
                # The view itself is read like any other (its definition changes nothing).
                assert rows(chargewire, sid, f"SELECT count(*) AS n FROM {schema}.tables") == [{"n": 2}]
        chargewire.http.delete(url=chargewire.session_url(late), headers=chargewire.headers())
    finally:
        chargewire.http.delete(url=chargewire.session_url(reader), headers=chargewire.headers())


def test_a_time_travel_clause_holds_plain_values_only(chargewire, duck, ro, table, unchanged):
    seq = name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    with unchanged(subject=ro, tables=[table]):
        for sql in (f"SELECT * FROM {table} AT (VERSION => nextval('{seq}'))", f"SELECT * FROM {table} AT (VERSION => setval('{seq}', 77))",
                    f"SELECT * FROM {table} AT (TIMESTAMP => now() - INTERVAL (nextval('{seq}')) DAY)",
                    f"SELECT (SELECT count(*) FROM {table} AT (VERSION => nextval('{seq}')))",
                    f"SELECT * FROM (SELECT * FROM {table} AT (VERSION => nextval('{seq}'))) s",
                    f"SELECT * FROM {table} a JOIN (SELECT * FROM {table} AT (VERSION => nextval('{seq}'))) b USING (id)",
                    f"EXPLAIN SELECT * FROM {table} AT (VERSION => nextval('{seq}'))", f"DESCRIBE SELECT * FROM {table} AT (VERSION => nextval('{seq}'))"):
            refused(chargewire, ro, sql)
        assert_problem(response=prepare(chargewire, ro, f"SELECT * FROM {table} AT (VERSION => nextval('{seq}'))"), status=403, code="read_only")
        # With plain values the clause passes the screen, and DuckDB's own answer follows, the same
        # in both kinds of session: its own tables have no time travel (a lake's have: see the
        # DuckLake test).
        for sql in (f"SELECT * FROM {table} AT (VERSION => 1)", f"SELECT * FROM {table} AT (TIMESTAMP => now() - INTERVAL 1 DAY)",
                    f"SELECT * FROM {table} AT (TIMESTAMP => TIMESTAMP '2020-01-01 00:00:00')"):
            for session_id in (ro, duck):
                assert_problem(response=chargewire.submit(session_id=session_id, sql=sql), status=400, code="sql_error")
    assert one(chargewire, duck, f"SELECT nextval('{seq}')") == 1


def test_views_and_macros_are_screened_with_the_statement_that_reaches_them(chargewire, duck, ro, table, made, unchanged):
    # DuckDB expands views and macros into the statement while it binds: what their definitions
    # would evaluate then is screened as the statement's own, for every view and macro the
    # statement names and every one those name, whoever defined them.
    n, seq = name("ro_def"), name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    for kind, suffix, definition in (
            ("VIEW", "bad_view", f"SELECT * FROM range(nextval('{seq}'))"),  # (creating it binds it: the sequence moves once, here)
            ("VIEW", "bad_over", f"SELECT count(*) AS n FROM {n}_bad_view"), ("VIEW", "bad_over_over", f"SELECT n + 1 AS m FROM {n}_bad_over"),
            ("VIEW", "bad_join", f"SELECT * FROM {table} JOIN generate_series(1, nextval('{seq}')) g(i) ON true"),
            ("VIEW", "bad_set", f"SELECT * FROM range(setval('{seq}', 9))"),
            ("VIEW", "good_view", f"SELECT id FROM {table} WHERE id > 1"), ("VIEW", "good_over", f"SELECT count(*) AS n FROM {n}_good_view")):
        made("VIEW", f"{n}_{suffix}", f"CREATE VIEW {n}_{suffix} AS {definition}")
    for kind, signature, definition in (
            ("MACRO TABLE", "bad_runs(q)", "TABLE SELECT * FROM query(q)"), ("MACRO TABLE", "bad_table()", f"TABLE SELECT * FROM range(setval('{seq}', 500))"),
            ("MACRO TABLE", "bad_calls_macro()", f"TABLE SELECT * FROM {n}_bad_table()"),
            ("MACRO", "bad_scalar()", f"(SELECT count(*) FROM range(nextval('{seq}')))"), ("MACRO", "bad_scalar_over_view()", f"(SELECT max(n) FROM {n}_bad_over)"),
            ("MACRO", "nx()", f"nextval('{seq}')"), ("MACRO TABLE", "tm(n)", "TABLE SELECT * FROM range(n)"),
            ("MACRO", "good_scalar(a)", "a + 1"), ("MACRO", "good_sub()", f"(SELECT max(id) FROM {table})"),
            ("MACRO TABLE", "good_table(n)", f"TABLE SELECT * FROM {n}_good_view WHERE id <= n")):
        made(kind, f"{n}_{signature.split('(')[0]}", f"CREATE MACRO {n}_{signature} AS {definition}")
    with unchanged(subject=ro, tables=[table]):
        for sql in (
                # views: read directly, qualified, quoted, described, through a view over them, anywhere in a statement
                f"SELECT * FROM {n}_bad_view", f"SELECT * FROM memory.main.{n}_bad_view", f'SELECT * FROM "{n.upper()}_BAD_VIEW"', f"FROM {n}_bad_view",
                f"TABLE {n}_bad_view", f"SELECT * FROM {n}_bad_over", f"SELECT * FROM {n}_bad_over_over", f"SELECT * FROM {n}_bad_join",
                f"SELECT * FROM {n}_bad_set", f"SELECT count(*) FROM {table} WHERE id IN (SELECT * FROM {n}_bad_view)",
                f"SELECT (SELECT count(*) FROM {n}_bad_view)", f"WITH c AS (SELECT * FROM {n}_bad_view) SELECT count(*) FROM c",
                f"SELECT * FROM {table} a LEFT JOIN {n}_bad_view b ON true", f"EXPLAIN SELECT * FROM {n}_bad_view", f"DESCRIBE {n}_bad_view",
                f"DESCRIBE TABLE {n}_bad_view", f"DESCRIBE SELECT * FROM {n}_bad_view", f"SUMMARIZE {n}_bad_view",
                f"SET VARIABLE ro_x = (SELECT count(*) FROM {n}_bad_view)",
                # table macros
                f"SELECT * FROM {n}_bad_runs('SELECT 1')", f"SELECT * FROM {n}_bad_runs('SELECT * FROM range(nextval(''{seq}''))')",
                f"SELECT * FROM {n}_bad_table()", f"SELECT * FROM {n}_bad_calls_macro()", f"SELECT (SELECT count(*) FROM {n}_bad_table())",
                f"SELECT * FROM {n}_tm({n}_nx())", f"SELECT * FROM {n}_tm(nextval('{seq}'))",
                # scalar macros, wherever an expression stands
                f"SELECT {n}_bad_scalar()", f"SELECT (SELECT {n}_bad_scalar())", f"SELECT id, {n}_bad_scalar() FROM {table}",
                f"SELECT * FROM {table} WHERE id > {n}_bad_scalar()", f"SELECT * FROM {table} ORDER BY {n}_bad_scalar()",
                f"SELECT {n}_bad_scalar_over_view()", f"SELECT {n}_nx()"):
            refused(chargewire, ro, sql)
        for sql in (f"SELECT * FROM {n}_bad_view", f"SELECT * FROM {n}_bad_table()", f"SELECT {n}_bad_scalar()", f"SELECT * FROM {n}_tm({n}_nx())"):
            assert_problem(response=prepare(chargewire, ro, sql), status=403, code="read_only")
        # Views and macros that only read are read, and a table macro takes a plain value.
        assert rows(chargewire, ro, f"SELECT id FROM {n}_good_view ORDER BY id") == [{"id": 2}, {"id": 3}]
        assert rows(chargewire, ro, f"SELECT n FROM {n}_good_over") == [{"n": 2}]
        assert rows(chargewire, ro, f"SELECT {n}_good_scalar(1) AS a, {n}_good_sub() AS b") == [{"a": 2, "b": 3}]
        assert one(chargewire, ro, f"SELECT count(*) FROM {n}_tm(3)") == 3 == one(chargewire, ro, f"SELECT count(*) FROM {n}_tm(1 + 2)")
        assert rows(chargewire, ro, f"SELECT id FROM {n}_good_table(2)") == [{"id": 2}]
    # The twins: a read-write session reads each of them (and moves the sequence).
    for sql in (f"SELECT * FROM {n}_bad_view", f"SELECT * FROM {n}_bad_table()", f"SELECT {n}_bad_scalar()", f"SELECT * FROM {n}_bad_runs('SELECT 1')",
                f"SELECT * FROM {n}_tm({n}_nx())"):
        assert answer_of(chargewire, duck, sql)[0] == 200, sql


def test_definitions_made_after_the_sessions_first_statement_are_screened_too(chargewire, duck, ro, made, unchanged):
    # The server keeps the views' and macros' definitions it has read for a session, and reads
    # them again when a catalog has changed: what a writer defines or replaces after a reader's
    # first statement is screened like what was there before it.
    n, seq, other = name("ro_later"), name("ro_seq"), name("ro_other")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    made("VIEW", f"{n}_view", f"CREATE VIEW {n}_view AS SELECT 0::BIGINT AS n")
    made("MACRO TABLE", f"{n}_macro", f"CREATE MACRO {n}_macro(q) AS TABLE SELECT 0::BIGINT AS n")
    assert rows(chargewire, ro, f"SELECT n FROM {n}_view") == [{"n": 0}]  # the session has read the definitions
    assert rows(chargewire, ro, f"SELECT n FROM {n}_macro('SELECT 1')") == [{"n": 0}]
    # Replaced by ones that evaluate while binding (the writer's own CREATE binds the view once).
    made("VIEW", f"{n}_view", f"CREATE OR REPLACE VIEW {n}_view AS SELECT range AS n FROM range(nextval('{seq}'))")
    made("MACRO TABLE", f"{n}_macro", f"CREATE OR REPLACE MACRO {n}_macro(q) AS TABLE SELECT * FROM query(q)")
    with unchanged():  # (no look at the reader's own state: its next statement is the read itself)
        refused(chargewire, ro, f"SELECT * FROM {n}_view")
        refused(chargewire, ro, f"SELECT * FROM {n}_macro('SELECT * FROM range(setval(''{seq}'', 100))')")
    # Made new, after the session has read the definitions twice.
    made("VIEW", f"{n}_newer_view", f"CREATE VIEW {n}_newer_view AS SELECT * FROM range(setval('{seq}', 200))")
    made("MACRO TABLE", f"{n}_newer_macro", f"CREATE MACRO {n}_newer_macro() AS TABLE SELECT * FROM generate_series(1, nextval('{seq}'))")
    with unchanged():
        refused(chargewire, ro, f"SELECT * FROM {n}_newer_view")
        refused(chargewire, ro, f"SELECT * FROM {n}_newer_macro()")
    # And the reverse: replaced by a harmless one, a view is readable again.
    made("VIEW", f"{n}_view", f"CREATE OR REPLACE VIEW {n}_view AS SELECT 5::BIGINT AS n")
    made("MACRO TABLE", f"{n}_macro", f"CREATE OR REPLACE MACRO {n}_macro(q) AS TABLE SELECT 5::BIGINT AS n")
    with unchanged():
        assert rows(chargewire, ro, f"SELECT n FROM {n}_view") == [{"n": 5}]
        assert rows(chargewire, ro, f"SELECT n FROM {n}_macro('x')") == [{"n": 5}]
    made("VIEW", f"{n}_view", f"CREATE OR REPLACE VIEW {n}_view AS SELECT range AS n FROM range(nextval('{seq}'))")
    with unchanged():
        refused(chargewire, ro, f"SELECT * FROM {n}_view")
    # In a second database, attached after the reader's first statement.
    succeeded(chargewire, duck, f"ATTACH ':memory:' AS {other}")
    try:
        succeeded(chargewire, duck, f"CREATE SEQUENCE {other}.main.far_seq")
        succeeded(chargewire, duck, f"CREATE VIEW {other}.main.{n}_far_view AS SELECT * FROM range(nextval('{other}.main.far_seq'))")
        succeeded(chargewire, duck, f"CREATE MACRO {other}.main.{n}_far_macro() AS TABLE SELECT * FROM range(setval('{other}.main.far_seq', 300))")
        with unchanged():
            refused(chargewire, ro, f"SELECT * FROM {other}.main.{n}_far_view")
            refused(chargewire, ro, f"SELECT * FROM {other}.main.{n}_far_macro()")
        succeeded(chargewire, duck, f"CREATE OR REPLACE VIEW {other}.main.{n}_far_view AS SELECT 7::BIGINT AS n")
        with unchanged():
            assert rows(chargewire, ro, f"SELECT n FROM {other}.main.{n}_far_view") == [{"n": 7}]
    finally:
        succeeded(chargewire, duck, f"DETACH {other}")
    # Temporary views and macros the session made itself, before it became read-only. (Ones that
    # do nothing when they are made: DuckDB 2.0 ends a CREATE TEMP VIEW over range(nextval('s'))
    # with an error that is fatal for the database, in any session.)
    sid = chargewire.new_session()
    for sql in ("SET VARIABLE ro_one = 1", f"CREATE TEMP VIEW {n}_mine_view AS SELECT * FROM query('SELECT 42 AS n')",
                f"CREATE TEMP VIEW {n}_mine_other AS SELECT * FROM range(CASE WHEN getvariable('ro_one') = 1 THEN 1 ELSE nextval('{seq}') END)",
                f"CREATE TEMP MACRO {n}_mine_macro() AS TABLE SELECT * FROM range(setval('{seq}', 400))",
                f"CREATE TEMP MACRO {n}_mine_scalar() AS (SELECT count(*) FROM range(nextval('{seq}')))",
                f"CREATE TEMP VIEW {n}_mine_good AS SELECT 3::BIGINT AS n"):
        succeeded(chargewire, sid, sql)
    assert patch(chargewire, sid, {"read_only": True})["errors"] == {}
    with unchanged():
        for sql in (f"SELECT * FROM {n}_mine_view", f"SELECT * FROM {n}_mine_other", f"SELECT * FROM {n}_mine_macro()", f"SELECT {n}_mine_scalar()"):
            refused(chargewire, sid, sql)
        assert rows(chargewire, sid, f"SELECT n FROM {n}_mine_good") == [{"n": 3}]
    chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


def test_a_view_that_does_not_pass_the_screen_is_caught_however_it_is_named(chargewire, duck, ro, table, made, unchanged):
    # A view is also reached without its name standing in the statement as a name: SHOW, and the
    # functions and pragmas that take a table's name as a string and bind it to describe it.
    # While a view exists whose definition does not pass the screen, a statement that names it in
    # such a string is refused, and so is one whose string is computed (the server cannot tell
    # which table it will name). The schema listing gives such a view no columns.
    n, seq = name("ro_named"), name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    made("VIEW", f"{n}_good", f"CREATE VIEW {n}_good AS SELECT id FROM {table} WHERE id > 1")
    computed = (f"SELECT count(*) AS n FROM pragma_table_info('{n}' || '_good')", "SELECT count(*) AS n FROM pragma_table_info(getvariable('ro_which'))")
    with_parameter = "SELECT count(*) AS n FROM pragma_table_info(?)"
    schema_listing = f"{chargewire.session_url(ro)}/metadata/tables"
    succeeded(chargewire, ro, f"SET VARIABLE ro_which = '{n}_good'")
    for sql in computed:  # no view is unreadable: a computed name is a name like another
        assert rows(chargewire, ro, sql) == [{"n": 1}]
    assert rows(chargewire, ro, with_parameter, [f"{n}_good"]) == [{"n": 1}]
    made("VIEW", f"{n}_bad", f"CREATE VIEW {n}_bad AS SELECT * FROM range(nextval('{seq}'))")  # (creating it binds it once)
    with unchanged(subject=ro, tables=[table]):
        for sql in (f"SHOW {n}_bad", f"SELECT * FROM (SHOW {n}_bad)", f"SHOW memory.main.{n}_bad", f"SET VARIABLE ro_x = (SELECT count(*) FROM (SHOW {n}_bad))",
                    f"PRAGMA table_info('{n}_bad')", f"PRAGMA table_info('main.{n}_bad')", f"SELECT count(*) FROM pragma_table_info('{n}_bad')",
                    f"SELECT count(*) FROM pragma_table_info('main.{n}_bad')", f"SELECT count(*) FROM pragma_table_info('memory.main.{n}_bad')",
                    f"SELECT (SELECT count(*) FROM pragma_table_info('{n}_bad'))", f"SELECT count(*) FROM pragma_table_info('{n.upper()}_BAD')",
                    f"SELECT count(*) FROM pragma_table_info('{n}' || '_bad')", f"DESCRIBE {n}_bad", f"SUMMARIZE {n}_bad"):
            refused(chargewire, ro, sql)
        refused(chargewire, ro, with_parameter, [f"{n}_bad"])
        # A computed name is a name like another, whatever views there are: one that names
        # the good view is answered (an earlier server refused every computed name while a
        # view existed that it would not bind, anyone's view).
        for sql in computed:
            assert rows(chargewire, ro, sql) == [{"n": 1}]
        assert rows(chargewire, ro, with_parameter, [f"{n}_good"]) == [{"n": 1}]
        for sql in (f"SHOW {n}_bad", f"SELECT * FROM pragma_table_info('{n}_bad')"):
            kept_out_when_prepared(chargewire, ro, sql)
        # The schema listing binds every view to describe it: an unreadable one is listed without
        # columns. (The test's own views: the server has every other test's too.)
        for params in ({"table_name_filter_pattern": f"{n}%"}, {"include_schema": "true", "table_name_filter_pattern": f"{n}%"},
                       {"include_schema": "true", "table_name_filter_pattern": f"{n}_bad"}):
            response = chargewire.http.get(url=schema_listing, params=params, headers=chargewire.headers())
            assert response.status_code == 200, (params, response.text)
        # Everything else is read as before, also what names another view.
        for sql in (f"SHOW {n}_good", f"SELECT * FROM (SHOW {n}_good)", f"PRAGMA table_info('{n}_good')", f"SELECT count(*) FROM pragma_table_info('{n}_good')",
                    f"SELECT * FROM {n}_good", "SHOW TABLES", "SHOW ALL TABLES", "DESCRIBE", "SELECT * FROM range(-1 + 3)",
                    "SELECT count(*) FROM duckdb_columns()", "SELECT count(*) FROM information_schema.columns", "SELECT count(*) FROM duckdb_views()"):
            assert answer_of(chargewire, ro, sql)[0] == 200, sql
    # Replaced by a view that only reads, everything about it reads again.
    made("VIEW", f"{n}_bad", f"CREATE OR REPLACE VIEW {n}_bad AS SELECT 1 AS one")
    succeeded(chargewire, ro, f"SET VARIABLE ro_which = '{n}_bad'")
    for sql in (f"SHOW {n}_bad", f"SELECT * FROM (SHOW {n}_bad)", f"PRAGMA table_info('{n}_bad')", f"SELECT * FROM {n}_bad"):
        assert answer_of(chargewire, ro, sql)[0] == 200, sql
    assert rows(chargewire, ro, f"SELECT count(*) AS n FROM pragma_table_info('{n}' || '_bad')") == [{"n": 1}]
    assert rows(chargewire, ro, computed[1]) == [{"n": 1}]
    assert rows(chargewire, ro, with_parameter, [f"{n}_bad"]) == [{"n": 1}]
    assert chargewire.http.get(url=schema_listing, params={"include_schema": "true", "table_name_filter_pattern": f"{n}%"},
                               headers=chargewire.headers()).status_code == 200
    # The twin: a read-write session describes a view by its name in a string. (Not the unreadable
    # one: DuckDB 2.0 ends that, in a read-write session, with an error that is fatal for the database.)
    assert rows(chargewire, duck, f"SELECT count(*) AS n FROM pragma_table_info('{n}' || '_good')") == [{"n": 1}]


def listed_columns(chargewire, session_id: str, **params) -> dict[str, list[str]]:
    """The column names metadata/tables?include_schema=true gives for each table and view."""
    from chargewire import read_table
    response = chargewire.http.get(url=f"{chargewire.session_url(session_id)}/metadata/tables", params={"include_schema": "true", **params},
                                   headers=chargewire.headers())
    assert response.status_code == 200, response.text
    listing = read_table(content=response.content)
    return {table_name: ipc.read_schema(pa.py_buffer(schema)).names
            for table_name, schema in zip(listing.column("table_name").to_pylist(), listing.column("table_schema").to_pylist())}


def test_the_schema_listing_is_answered_whatever_views_there_are(chargewire, duck, ro, table, made, unchanged, logging_stays_off):
    # A driver's column listing (metadata/tables with include_schema) describes every table and
    # view. For a read-only session each is only bound, never run: a view that such a session
    # may not read (it advances a sequence, it calls a function that is not listed) is listed
    # with its columns, one whose binding itself would evaluate something is listed without
    # any, and no view takes the listing of the others away. Nothing changes by it.
    n, seq = name("ro_listed"), name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    made("VIEW", f"{n}_next", f"CREATE VIEW {n}_next AS SELECT nextval('{seq}') AS n")
    made("VIEW", f"{n}_logging", f"CREATE VIEW {n}_logging AS SELECT count(*) AS c FROM enable_logging()")
    made("VIEW", f"{n}_plain", f"CREATE VIEW {n}_plain AS SELECT id, v FROM {table}")
    made("VIEW", f"{n}_unreadable", f"CREATE VIEW {n}_unreadable AS SELECT * FROM range(nextval('{seq}'))")  # (creating it binds it once)
    with unchanged(subject=ro, tables=[table]):
        for _ in range(3):
            assert listed_columns(chargewire, ro, table_name_filter_pattern=f"{n}%") == \
                {f"{n}_next": ["n"], f"{n}_logging": ["c"], f"{n}_plain": ["id", "v"], f"{n}_unreadable": []}
            everything = listed_columns(chargewire, ro)  # with every other test's tables and views
            assert everything[table] == ["id", "v"] and everything[f"{n}_unreadable"] == []
        refused(chargewire, ro, f"SELECT * FROM {n}_next")
        refused(chargewire, ro, f"SELECT * FROM {n}_logging")
        refused(chargewire, ro, f"SELECT * FROM {n}_unreadable")
    # The twin: a read-write session is given the unreadable view's columns too.
    assert listed_columns(chargewire, duck, table_name_filter_pattern=f"{n}%")[f"{n}_unreadable"] == ["range"]


def test_a_macro_named_like_a_function_changes_nothing_through_a_read_only_session(chargewire, duck, ro, made, unchanged):
    # A function called where DuckDB evaluates while binding is on the server's short list of
    # ones that only compute, and a macro of the same name could be what DuckDB calls there
    # instead. Whatever DuckDB binds, a sequence does not move in a read-only session: a macro
    # of that name in a schema the statement does not reach changes nothing for anyone (an
    # earlier server refused such statements for every reader while the macro existed), and
    # one DuckDB does bind is refused where it would advance the sequence.
    schema, seq = name("ro_shadows"), name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
    statements = ("SELECT * FROM generate_series(TIMESTAMP '2020-01-01', TIMESTAMP '2020-01-03', INTERVAL 1 DAY)",  # an interval literal is to_days(1)
                  "SELECT * FROM unnest([1, 2])",                                                                     # a list literal is list_value(1, 2)
                  "SELECT * FROM range(abs(-3))", "SET progress_bar_time = abs(-1000)")
    for sql in statements:
        assert chargewire.submit(session_id=ro, sql=sql).status_code in (200, 201), sql
    for signature in ("to_days(x)", "list_value(x, y)", "abs(x)"):
        made("MACRO", f"{schema}.{signature.split('(')[0]}", f"CREATE MACRO {schema}.{signature} AS nextval('{seq}')")
    with unchanged(subject=ro):
        for sql in statements:  # in a schema of its own, the macro is not what these bind
            assert chargewire.submit(session_id=ro, sql=sql).status_code in (200, 201), sql
        assert rows(chargewire, ro, "SELECT abs(-3) AS a, [1, 2] AS l") == [{"a": 3, "l": [1, 2]}]
        # Named so that DuckDB binds it: refused where it would advance the sequence.
        refused(chargewire, ro, f"SELECT * FROM range({schema}.abs(-3))")
        refused(chargewire, ro, f"SELECT {schema}.abs(-3)")
        kept_out_when_prepared(chargewire, ro, f"SELECT {schema}.abs(-3)")
    for function in ("to_days", "list_value", "abs"):
        succeeded(chargewire, duck, f"DROP MACRO {schema}.{function}")
    for sql in statements:
        assert chargewire.submit(session_id=ro, sql=sql).status_code in (200, 201), sql
    assert one(chargewire, duck, f"SELECT nextval('{seq}')") == 1


def test_arguments_that_are_plain_values_still_work(chargewire, duck, ro, table, files, made, unchanged):
    # What the screen lets through, each with the rows a read-write session gets: constants,
    # arithmetic, casts, lists and structs, columns, bind parameters, the session's variables,
    # a query, and the few functions that only compute.
    n = name("ro_plain")
    made("MACRO TABLE", f"{n}_tm", f"CREATE MACRO {n}_tm(n) AS TABLE SELECT * FROM range(n)")
    made("VIEW", f"{n}_view", f"CREATE VIEW {n}_view AS SELECT id FROM {table} WHERE id > 1")
    made("MACRO", f"{n}_plus", f"CREATE MACRO {n}_plus(a) AS a + 1")
    for session_id in (ro, duck):
        succeeded(chargewire, session_id, "SET VARIABLE ro_n = 3")
    with unchanged(subject=ro, tables=[table]):
        for sql, parameters in (
                ("SELECT * FROM range(1, 10, 2)", None), ("SELECT * FROM range(-1 + 3)", None), ("SELECT * FROM range(CAST('3' AS INTEGER))", None),
                ("SELECT * FROM range('3'::INTEGER)", None), ("SELECT * FROM range(?::INTEGER)", [4]), ("SELECT * FROM range(?)", [4]),
                ("SELECT * FROM range($n::INTEGER)", {"n": 2}), ("SELECT * FROM range(getvariable('ro_n'))", None),
                ("SELECT * FROM range(abs(-3))", None), ("SELECT * FROM range(now()::DATE - today())", None), ("SELECT * FROM unnest([1, 2] || [3])", None),
                ("SELECT * FROM generate_series(TIMESTAMP '2020-01-01', TIMESTAMP '2020-01-03', INTERVAL 1 DAY)", None),
                ("SELECT * FROM generate_series(DATE '2020-01-01', DATE '2020-01-03', INTERVAL '1 day')", None),
                ("SELECT * FROM range(DATE '2020-01-01', DATE '2020-01-03', INTERVAL '1' DAY)", None),
                (f"SELECT * FROM range((SELECT max(id) FROM {table}))", None), (f"SELECT t.id, r.range FROM {table} t, LATERAL range(t.id) r ORDER BY ALL", None),
                ("SELECT * FROM generate_series(1, 3) g(i), LATERAL range(i + 1) ORDER BY ALL", None), ("SELECT * FROM json_each('[1,2]') ORDER BY key", None),
                ("SELECT * FROM repeat_row(1, 2, num_rows = 3)", None), ("SELECT * FROM summary((SELECT 1))", None),
                (f"SELECT * FROM read_parquet(['{files}/data.parquet', '{files}/data.parquet'], filename = true) ORDER BY ALL", None),
                (f"SELECT * FROM read_csv('{files}/data.csv', columns = {{'id': 'INTEGER', 'v': 'VARCHAR'}}) ORDER BY ALL", None),
                (f"SELECT * FROM read_csv('{files}/data' || '.csv') ORDER BY ALL", None), (f"SELECT * FROM read_csv(?) ORDER BY ALL", [f"{files}/data.csv"]),
                (f"SELECT count(*) AS n FROM glob('{files}/*')", None), (f"SELECT * FROM {n}_tm(3)", None), (f"SELECT * FROM {n}_tm(1 + 2)", None),
                (f"SELECT * FROM {n}_tm(?::INTEGER)", [2]), (f"SELECT * FROM {n}_tm(getvariable('ro_n'))", None),
                (f"SELECT * FROM {n}_view ORDER BY id", None), (f"SHOW {n}_view", None), (f"SELECT {n}_plus(id) AS p FROM {n}_view ORDER BY p", None),
                (f"SELECT name FROM pragma_table_info('{n}' || '_view')", None), (f"SELECT name FROM pragma_table_info(?)", [f"{n}_view"])):
            expected = rows(chargewire, duck, sql, parameters)
            assert rows(chargewire, ro, sql, parameters) == expected, sql
            if parameters is None or isinstance(parameters, list):  # and prepared, as a driver sends it
                prepared_id = prepared_in(chargewire, ro, sql)
                assert run_prepared(chargewire, ro, prepared_id, parameters) == (200, expected), sql
    # The value of a setting too.
    succeeded(chargewire, ro, "SET search_path = lower('MAIN')")
    succeeded(chargewire, ro, "SET progress_bar_time = abs(-1000) + 1")
    assert one(chargewire, ro, "SELECT current_setting('progress_bar_time')") == 1001


def test_a_session_with_a_temporary_sequence_cannot_become_read_only(chargewire, duck, unchanged):
    # DuckDB's READ ONLY transaction does not cover the session's temporary catalog, and
    # advancing a sequence is not undone: a session that has one is told to drop it first.
    sid = chargewire.new_session()
    succeeded(chargewire, sid, "CREATE TEMP SEQUENCE ro_temp_seq START 1")
    assert one(chargewire, sid, "SELECT nextval('ro_temp_seq')") == 1
    answer = patch(chargewire, sid, {"read_only": True})
    assert answer["errors"]["read_only"]["code"] == "invalid_option_value", answer
    assert "temporary sequence" in answer["errors"]["read_only"]["detail"], answer
    assert answer["options"]["read_only"] is False and document(chargewire, sid)["options"]["read_only"] is False
    assert one(chargewire, sid, "SELECT nextval('ro_temp_seq')") == 2  # still a session that writes, its sequence untouched
    succeeded(chargewire, sid, "DROP SEQUENCE ro_temp_seq")
    answer = patch(chargewire, sid, {"read_only": True})
    assert answer["errors"] == {} and answer["options"]["read_only"] is True, answer
    with unchanged(subject=sid):
        for sql in ("CREATE TEMP SEQUENCE ro_temp_seq", "SELECT nextval('ro_temp_seq')", "SELECT setval('ro_temp_seq', 1000)"):
            response = chargewire.submit(session_id=sid, sql=sql)
            assert response.status_code in (400, 403), (sql, response.text)  # refused, or: there is no such sequence
    assert one(chargewire, sid, "SELECT count(*) FROM duckdb_sequences() WHERE temporary") == 0


# Functions that run SQL text of their own, or do the server's housekeeping, called the ways a
# statement can reach a function: directly, qualified, through query(), a macro or a view a
# writer made earlier, a CTE, a subquery of every kind, either side of a join, a branch of a
# UNION, EXPLAIN ANALYZE, SUMMARIZE, and the value of a variable.
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
        "PRAGMA json_execute_serialized_sql('<the write, serialized>')",
        "SELECT * FROM checkpoint()",
        "SELECT * FROM force_checkpoint()",
        "SELECT * FROM system.main.checkpoint()",
        "SELECT * FROM query('SELECT * FROM checkpoint()')",
        f"SELECT * FROM {macro}_checkpoint()",
        "SELECT * FROM enable_logging()",
        "SELECT * FROM enable_logging(level = 'debug')",
        "SELECT * FROM disable_logging()",
        "SELECT * FROM truncate_duckdb_logs()",
        "SELECT * FROM enable_profiling()",
        "SELECT * FROM disable_profiling()",
        "SELECT count(*) FROM enable_logging()",
        f"SELECT * FROM {macro}_view",
        f"SELECT * FROM {table} a JOIN (SELECT count(*) AS c FROM enable_logging()) e ON true",
        f"SELECT * FROM (SELECT count(*) AS c FROM enable_logging()) e JOIN {table} a ON true",
        f"SELECT * FROM {table} a LEFT JOIN (SELECT count(*) AS c FROM enable_logging()) e ON a.id = e.c",
        f"SELECT id FROM {table} UNION ALL SELECT count(*) FROM enable_logging()",
        f"SELECT count(*) FROM enable_logging() UNION ALL SELECT id FROM {table}",
        f"SELECT * FROM {table} WHERE id IN (SELECT count(*) FROM enable_logging())",
        f"SELECT * FROM {table} a WHERE EXISTS (SELECT 1 FROM enable_logging() WHERE a.id > 0)",
        f"SELECT * FROM {table} a, LATERAL (SELECT count(*) + a.id FROM enable_logging())",
        f"SELECT id, (SELECT count(*) FROM enable_logging()) FROM {table} ORDER BY id LIMIT 1",
        "SUMMARIZE SELECT * FROM enable_logging()",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r, (SELECT count(*) FROM enable_logging()) WHERE n < 2) SELECT * FROM r",
        f"SELECT 1 AS first; SELECT * FROM enable_logging()",
    ]


@pytest.mark.parametrize("index", range(len(through_functions("t", "m"))))
def test_a_function_that_changes_something_is_refused_however_it_is_called(chargewire, duck, ro, table, unchanged,
                                                                           logging_stays_off, index):
    macro = name("ro_macro")
    succeeded(chargewire, duck, f"CREATE MACRO {macro}_run(q) AS TABLE SELECT * FROM json_execute_serialized_sql(json_serialize_sql(q))")
    succeeded(chargewire, duck, f"CREATE MACRO {macro}_checkpoint() AS TABLE SELECT * FROM checkpoint()")
    succeeded(chargewire, duck, f"CREATE VIEW {macro}_view AS SELECT count(*) AS c FROM enable_logging()")
    sql = through_functions(table, macro)[index]
    if "<the write, serialized>" in sql:  # the pragma takes the JSON itself, as a literal
        write = f"WITH written AS (INSERT INTO {table} VALUES (9, 'z') RETURNING id) SELECT count(*) FROM written"
        serialized = one(chargewire, duck, "SELECT json_serialize_sql(?)", [write])
        sql = sql.replace("<the write, serialized>", serialized.replace("'", "''"))
    with unchanged(subject=ro, tables=[table]):
        refused(chargewire, ro, sql)


def test_the_twins_of_those_functions_work_in_a_read_write_session(chargewire, duck, table):
    # So the refusals above are of calls that do something. On a table and a session of the
    # test's own where that is possible; logging is switched on, seen, and switched off again.
    other = name("ro_twin")
    succeeded(chargewire, duck, f"CREATE TABLE {other} (id INTEGER, v VARCHAR)")
    write = f"WITH written AS (INSERT INTO {other} VALUES (9, ''z'') RETURNING id) SELECT count(*) FROM written"
    assert one(chargewire, duck, f"SELECT * FROM json_execute_serialized_sql(json_serialize_sql('{write}'))") == 1
    assert one(chargewire, duck, f"SELECT count(*) FROM {other}") == 1
    for sql in ("SELECT * FROM checkpoint()", "SELECT * FROM enable_profiling()", "SELECT * FROM disable_profiling()",
                "SELECT * FROM truncate_duckdb_logs()", f"SELECT * FROM {other}, (SELECT count(*) FROM truncate_duckdb_logs())"):
        assert answer_of(chargewire, duck, sql)[0] == 200, sql
    try:
        assert answer_of(chargewire, duck, "SELECT * FROM enable_logging()")[0] == 200
        assert logging_is_on(chargewire, duck)
    finally:
        succeeded(chargewire, duck, "CALL disable_logging()")
        succeeded(chargewire, duck, "CALL truncate_duckdb_logs()")
    assert not logging_is_on(chargewire, duck)


def test_explain_runs_nothing_and_explain_of_a_write_is_refused(chargewire, duck, ro, table, tmp_path, unchanged, logging_stays_off):
    # A plain EXPLAIN shows a plan and runs none of it: allowed for what a read-only session
    # may not run, since nothing runs. Not for a statement that would change a catalog, the
    # session's temporary one included: DuckDB plans those as writes of the catalog.
    with unchanged(subject=ro, tables=[table]):
        for sql in (f"EXPLAIN COPY {table} TO '{tmp_path}/explained.csv'", "EXPLAIN SELECT * FROM checkpoint()",
                    "EXPLAIN SELECT * FROM enable_logging()", f"EXPLAIN SELECT * FROM {table}", "EXPLAIN (SELECT 1)",
                    "EXPLAIN (FORMAT JSON) SELECT * FROM enable_logging()"):
            assert answer_of(chargewire, ro, sql)[0] == 200, sql
        for sql in (f"EXPLAIN INSERT INTO {table} VALUES (4, 'c')", f"EXPLAIN DELETE FROM {table}", f"EXPLAIN UPDATE {table} SET v = 'z'",
                    f"EXPLAIN CREATE TABLE {name()} AS SELECT 1 AS i", "EXPLAIN CREATE TEMP TABLE ro_explained AS SELECT 1 AS i",
                    f"EXPLAIN ANALYZE COPY {table} TO '{tmp_path}/explained.csv'", "EXPLAIN ANALYZE SELECT * FROM checkpoint()",
                    "EXPLAIN ANALYZE SELECT * FROM enable_logging()", f"EXPLAIN ANALYZE INSERT INTO {table} VALUES (4, 'c')"):
            refused(chargewire, ro, sql)
    # The twin of the temporary one: a read-write session gets its plan.
    assert answer_of(chargewire, duck, "EXPLAIN CREATE TEMP TABLE ro_explained AS SELECT 1 AS i")[0] == 200


def test_a_temporary_table_from_before_is_not_written(chargewire, duck, unchanged):
    temp = name("ro_temp")
    succeeded(chargewire, duck, f"CREATE TEMP TABLE {temp} AS SELECT 1 AS i")
    assert patch(chargewire, duck, {"read_only": True})["errors"] == {}
    with unchanged(subject=duck, session_tables=[temp]):
        for sql in (f"INSERT INTO {temp} VALUES (2)", f"UPDATE {temp} SET i = 3", f"DELETE FROM {temp}", f"DROP TABLE {temp}",
                    f"WITH written AS (INSERT INTO {temp} VALUES (2) RETURNING i) SELECT count(*) FROM written",
                    f"EXPLAIN INSERT INTO {temp} VALUES (2)", f"ALTER TABLE {temp} ADD COLUMN j INTEGER", f"TRUNCATE {temp}"):
            refused(chargewire, duck, sql)
    assert rows(chargewire, duck, f"SELECT i FROM {temp}") == [{"i": 1}]  # still readable, unchanged


# ---- Volatile arguments: what is checked is what runs ----
#
# DuckDB evaluates the arguments of query() and query_table() while it binds, and they
# become the plan. An earlier server planned once to check and once to run: with a coin in
# the argument the checked plan read and the plan that ran did not, in a quarter of the
# tries. Neither function runs for a read-only session at all now, and an argument that is
# not a plain value is refused before it is evaluated. A single try proves nothing here, so
# each form is tried 40 times: every answer is the refusal, with the server's logging off.

HARMLESS, HARMFUL = "SELECT 0::BIGINT AS c", "SELECT count(*) AS c FROM enable_logging()"
VOLATILE = [
    ("random()", f"SELECT * FROM query(CASE WHEN random() < 0.5 THEN '{HARMLESS}' ELSE '{HARMFUL}' END)"),
    ("uuid()", f"SELECT * FROM query(CASE WHEN uuid()::VARCHAR < '8' THEN '{HARMLESS}' ELSE '{HARMFUL}' END)"),
    ("the clock", f"SELECT * FROM query(CASE WHEN epoch_us(get_current_timestamp()) % 2 = 0 THEN '{HARMLESS}' ELSE '{HARMFUL}' END)"),
    ("query_table of a view", "SELECT * FROM query_table(CASE WHEN random() < 0.5 THEN '{s}.harmless' ELSE '{s}.harmful' END)"),
    ("query_table of a list", "SELECT * FROM query_table([CASE WHEN random() < 0.5 THEN '{s}.harmless' ELSE '{s}.harmful' END])"),
    ("a subquery around it", f"SELECT (SELECT c FROM query(CASE WHEN random() < 0.5 THEN '{HARMLESS}' ELSE '{HARMFUL}' END)) AS c"),
    ("a coin for a number", "SELECT count(*) AS c FROM range(CASE WHEN random() < 0.5 THEN 1 ELSE 2 END)"),
    ("a random number", "SELECT count(*) AS c FROM range((random() * 3)::INTEGER)"),
]


@pytest.mark.parametrize("sql", [v[1] for v in VOLATILE], ids=[v[0] for v in VOLATILE])
def test_a_volatile_argument_is_refused_every_time(chargewire, duck, ro, unchanged, logging_stays_off, sql):
    schema = name("ro_volatile")
    succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
    succeeded(chargewire, duck, f"CREATE VIEW {schema}.harmless AS {HARMLESS}")
    succeeded(chargewire, duck, f"CREATE VIEW {schema}.harmful AS {HARMFUL}")
    with unchanged(subject=ro):
        for attempt in range(40):
            assert answer_of(chargewire, ro, sql.format(s=schema)) == (403, "read_only"), attempt
            assert not logging_is_on(chargewire, duck), f"try {attempt}: the function ran"


def test_an_argument_that_is_the_sessions_variable_runs_as_it_was_checked(chargewire, duck, ro, table, unchanged):
    # getvariable() reads the session and nothing else: a value. The statement runs, and the
    # plan it gets is the one that was checked.
    succeeded(chargewire, ro, "SET VARIABLE ro_n = 3")
    succeeded(chargewire, ro, f"SET VARIABLE ro_most = (SELECT max(id) FROM {table})")
    with unchanged(subject=ro, tables=[table]):
        for _ in range(40):
            assert rows(chargewire, ro, "SELECT count(*) AS c FROM range(getvariable('ro_n'))") == [{"c": 3}]
        assert rows(chargewire, ro, "SELECT count(*) AS c FROM range(getvariable('ro_most') + 1)") == [{"c": 4}]
    # The twins of the refused forms: a read-write session runs them.
    assert answer_of(chargewire, duck, "SELECT count(*) AS c FROM range((random() * 3)::INTEGER)")[0] == 200
    assert answer_of(chargewire, duck, f"SELECT * FROM query(CASE WHEN random() < 2 THEN '{HARMLESS}' ELSE '{HARMFUL}' END)") == (200, [{"c": 0}])


# ---- Prepared statements (§7): planned, checked and run anew at each execution ----


def prepared_in(chargewire, session_id: str, sql: str) -> str:
    response = prepare(chargewire, session_id, sql)
    assert response.status_code == 201, (sql, response.text)
    return response.json()["prepared_id"]


def run_prepared(chargewire, session_id: str, prepared_id: str, parameters=None) -> tuple:
    """(200, rows) of an execution that ran, or (status, code) of one that did not."""
    response = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared/{prepared_id}/execute",
                                    headers=chargewire.headers(Accept="application/vnd.apache.arrow.stream"),
                                    json={"parameters": parameters or []})
    if response.status_code == 200 and response.headers["content-type"].startswith("application/vnd.apache.arrow.stream"):
        from chargewire import read_table
        return 200, read_table(content=response.content).to_pylist()
    if response.status_code in (200, 201):
        return response.status_code, response.json()["state"]
    return response.status_code, response.json()["code"]


def test_a_prepared_statement_through_such_a_function_is_refused_at_its_execution(chargewire, duck, ro, table, unchanged,
                                                                                  logging_stays_off):
    write = f"WITH written AS (INSERT INTO {table} VALUES (9, ''z'') RETURNING id) SELECT count(*) FROM written"
    with unchanged(subject=ro, tables=[table]):
        for sql, parameters in ((f"SELECT * FROM json_execute_serialized_sql(json_serialize_sql('{write}')) WHERE ? = 1", [1]),
                                ("SELECT count(*) AS c FROM enable_logging() WHERE ? = 1", [1]),
                                ("SELECT count(*) AS c FROM enable_logging()", []), ("SELECT * FROM checkpoint()", [])):
            prepared = prepare(chargewire, ro, sql)
            if prepared.status_code == 201:
                prepared = execute_prepared(chargewire, ro, prepared.json()["prepared_id"], parameters)
            assert_problem(response=prepared, status=403, code="read_only")


@pytest.fixture
def three_schemas(chargewire, duck):
    """Three schemas with a view `v` and a table macro `m()` of the same names: in `harmful`
    over enable_logging(), in the other two returning 0 and 7."""
    base = name("ro_path")
    schemas = {"harmful": f"{base}_harmful", "zero": f"{base}_zero", "seven": f"{base}_seven"}
    queries = {"harmful": HARMFUL, "zero": HARMLESS, "seven": "SELECT 7::BIGINT AS c"}
    for which, schema in schemas.items():
        succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
        succeeded(chargewire, duck, f"CREATE VIEW {schema}.v AS {queries[which]}")
        succeeded(chargewire, duck, f"CREATE MACRO {schema}.m() AS TABLE {queries[which]}")
    return schemas


# The ways a read-only session moves where its unqualified names resolve.
MOVES = {
    "USE": lambda chargewire, sid, schema: succeeded(chargewire, sid, f"USE {schema}"),
    "SET search_path": lambda chargewire, sid, schema: succeeded(chargewire, sid, f"SET search_path = '{schema}'"),
    "SET schema": lambda chargewire, sid, schema: succeeded(chargewire, sid, f"SET schema = '{schema}'"),
    "the schema option": lambda chargewire, sid, schema: patch(chargewire, sid, {"schema": schema}),
}


@pytest.mark.parametrize("move", list(MOVES))
@pytest.mark.parametrize("sql", ["SELECT * FROM v", "SELECT * FROM m()"], ids=["a view", "a table macro"])
def test_a_prepared_statement_runs_under_the_session_as_it_is_at_each_execution(chargewire, duck, three_schemas, unchanged,
                                                                                logging_stays_off, move, sql):
    # An earlier server checked the statement's text as it resolved now and ran the plan
    # DuckDB had bound when it was prepared: prepared where the name was harmless and
    # executed where it was not (or the other way round), the two were different plans.
    sid = chargewire.new_session(options={"read_only": True})
    go = functools.partial(MOVES[move], chargewire, sid)
    go(three_schemas["zero"])
    prepared_id = prepared_in(chargewire, sid, sql)
    assert run_prepared(chargewire, sid, prepared_id) == (200, [{"c": 0}])
    go(three_schemas["seven"])
    assert run_prepared(chargewire, sid, prepared_id) == (200, [{"c": 7}])  # the name as it resolves now
    go(three_schemas["harmful"])
    with unchanged(subject=sid):
        for _ in range(3):
            assert run_prepared(chargewire, sid, prepared_id) == (403, "read_only")
    go(three_schemas["zero"])
    assert run_prepared(chargewire, sid, prepared_id) == (200, [{"c": 0}])
    # And prepared where the name is the function: preparing runs nothing, executing is refused
    # there and works once the session has moved.
    go(three_schemas["harmful"])
    with unchanged(subject=sid):
        prepared = prepare(chargewire, sid, sql)
        if prepared.status_code != 201:
            assert_problem(response=prepared, status=403, code="read_only")
            return
        assert run_prepared(chargewire, sid, prepared.json()["prepared_id"]) == (403, "read_only")
    go(three_schemas["seven"])
    assert run_prepared(chargewire, sid, prepared.json()["prepared_id"]) == (200, [{"c": 7}])


def test_a_statement_prepared_before_the_switch_is_planned_anew_after_it(chargewire, duck, three_schemas, unchanged,
                                                                         logging_stays_off):
    # "Not a prepared statement made earlier" (§5.8): what DuckDB bound for a read-write
    # session is not what a read-only one runs.
    sid = chargewire.new_session()
    succeeded(chargewire, sid, f"USE {three_schemas['harmful']}")
    through_a_view = prepared_in(chargewire, sid, "SELECT * FROM v")
    directly = prepared_in(chargewire, sid, HARMFUL)
    with_a_parameter = prepared_in(chargewire, sid, f"{HARMFUL} WHERE ? = 1")
    assert not logging_is_on(chargewire, duck)  # preparing ran nothing
    assert patch(chargewire, sid, {"read_only": True})["errors"] == {}
    with unchanged(subject=sid):
        assert run_prepared(chargewire, sid, through_a_view) == (403, "read_only")
        assert run_prepared(chargewire, sid, directly) == (403, "read_only")
        assert run_prepared(chargewire, sid, with_a_parameter, [1]) == (403, "read_only")
    # Another session replaces the view by a harmless one: the next execution runs that.
    succeeded(chargewire, duck, f"CREATE OR REPLACE VIEW {three_schemas['harmful']}.v AS SELECT 5::BIGINT AS c")
    assert run_prepared(chargewire, sid, through_a_view) == (200, [{"c": 5}])
    with unchanged(subject=sid):
        assert run_prepared(chargewire, sid, directly) == (403, "read_only")


def test_prepared_transaction_statements_and_settings_work_and_do_what_their_answer_says(chargewire, duck, unchanged):
    # A driver that prepares every statement prepares its BEGIN, its SET and its USE. An
    # earlier server answered 403 to a prepared BEGIN and had opened the transaction.
    sid = chargewire.new_session(options={"read_only": True})
    schema = name("ro_prepared")
    succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
    zone = one(chargewire, sid, "SELECT current_setting('TimeZone')")
    other = "Asia/Tokyo" if zone != "Asia/Tokyo" else "UTC"

    def step(sql: str, answer: tuple, transaction: str) -> None:
        prepared = prepare(chargewire, sid, sql)
        got = (run_prepared(chargewire, sid, prepared.json()["prepared_id"]) if prepared.status_code == 201
               else (prepared.status_code, prepared.json()["code"]))
        assert got == answer, (sql, got)
        assert document(chargewire, sid)["transaction"] == transaction, sql

    step("BEGIN", (200, "succeeded"), "open")
    step(f"SET TimeZone = '{other}'", (200, "succeeded"), "open")
    step(f"USE {schema}", (200, "succeeded"), "open")
    step("SET VARIABLE ro_prepared_variable = 1", (200, "succeeded"), "open")
    step("SELECT current_setting('TimeZone') AS zone, current_schema() AS schema, getvariable('ro_prepared_variable') AS v",
         (200, [{"zone": other, "schema": schema, "v": 1}]), "open")
    assert document(chargewire, sid)["options"]["schema"] == schema
    step("COMMIT", (200, "succeeded"), "none")
    step("RESET TimeZone", (200, "succeeded"), "none")
    assert one(chargewire, sid, "SELECT current_setting('TimeZone')") == zone
    step("BEGIN TRANSACTION READ ONLY", (200, "succeeded"), "open")
    step("ROLLBACK", (200, "succeeded"), "none")
    step("START TRANSACTION", (200, "succeeded"), "open")
    step("SELECT 1 AS one", (200, [{"one": 1}]), "open")
    step("ROLLBACK", (200, "succeeded"), "none")
    # What fails says so, and has done nothing.
    step("COMMIT", (422, "execution_error"), "none")
    step("ROLLBACK", (422, "execution_error"), "none")
    with unchanged(subject=sid):
        step("BEGIN TRANSACTION READ WRITE", (403, "read_only"), "none")
        step("SET memory_limit = '1GB'", (403, "read_only"), "none")
        step("SET GLOBAL default_order = 'desc'", (403, "read_only"), "none")
        step("PRAGMA enable_verification", (403, "read_only"), "none")
        step("INSERT INTO ro_no_such_table VALUES (1)", (403, "read_only"), "none")
    step("BEGIN", (200, "succeeded"), "open")
    step("BEGIN", (422, "execution_error"), "open")  # as the plain statement: there is one already
    step("COMMIT", (200, "succeeded"), "none")


# ---- Settings ----

# Not on the reader's list, each refused and nothing set for anyone.
REFUSED_SETTINGS = [
    "SET GLOBAL default_order = 'desc'", "SET GLOBAL TimeZone = 'Asia/Tokyo'", "RESET GLOBAL default_order",
    "SET memory_limit = '1GB'", "SET max_memory = '1GB'", "SET threads = 1", "PRAGMA threads = 1", "SET temp_directory = '/tmp/x'",
    "SET enable_profiling = 'json'", "SET profiling_output = '/tmp/x.json'", "SET log_query_path = '/tmp/x.log'",
    "PRAGMA enable_profiling", "PRAGMA enable_profile", "SET enable_logging = true", "SET explain_output = 'all'", "SET enable_optimizer = false",
    "PRAGMA disable_optimizer", "SET autoinstall_known_extensions = false", "SET custom_extension_repository = 'http://127.0.0.1:1/x'",
    "SET extension_directory = '/tmp/x'", "SET home_directory = '/tmp/x'", "SET secret_directory = '/tmp/x'", "SET lock_configuration = true",
    "SET enable_external_access = false", "SET allow_persistent_secrets = false",
    "SET SESSION log_query_path = '/tmp/x.log'", "RESET explain_output", "SELECT 1; SET GLOBAL threads = 1",
    # The limits that keep one statement from exhausting the stack or the memory of the process:
    "SET max_expression_depth = 999", "RESET max_expression_depth", "SET SESSION max_expression_depth = 999",
    "SET perfect_ht_threshold = 20", "SET nested_loop_join_threshold = 1", "SET merge_join_threshold = 1",
    "SET ordered_aggregate_threshold = 1", "SET preserve_insertion_order = false", "SET partitioned_write_flush_threshold = 1",
    "SET pivot_limit = 5", "SET pivot_filter_threshold = 1",
    # What prints to the server's own output:
    "SET enable_progress_bar_print = true", "PRAGMA enable_print_progress_bar",
]


@pytest.mark.parametrize("sql", REFUSED_SETTINGS)
def test_a_setting_that_reaches_beyond_the_session_is_refused(chargewire, duck, ro, unchanged, logging_stays_off, sql):
    with unchanged(subject=ro):  # every setting, as the session and as a second session see them
        refused(chargewire, ro, sql)


def test_a_refused_setting_after_one_that_was_set_leaves_the_server_alone(chargewire, duck, ro, observer, unchanged):
    # The statements of a request before the refused one have run: the session has its time
    # zone, and no one else has anything.
    zone = one(chargewire, observer, "SELECT current_setting('TimeZone')")
    other = "Asia/Tokyo" if zone != "Asia/Tokyo" else "UTC"
    with unchanged():
        refused(chargewire, ro, f"SET \"TimeZone\" TO '{other}'; SET GLOBAL threads = 1")
        refused(chargewire, ro, f"SET TimeZone = '{other}'; SET memory_limit = '1GB'")
    assert one(chargewire, ro, "SELECT current_setting('TimeZone')") == other
    assert one(chargewire, observer, "SELECT current_setting('TimeZone')") == zone


def test_the_value_of_a_setting_is_never_a_query(chargewire, duck, ro, unchanged, logging_stays_off):
    # DuckDB itself has no subqueries in SET, in any session: nothing of one runs.
    with unchanged(subject=ro):
        for sql in ("SET TimeZone = (SELECT 'UTC')", "SET default_null_order = (SELECT 'nulls_first' FROM enable_logging())"):
            for session_id in (ro, duck):
                assert_problem(response=chargewire.submit(session_id=session_id, sql=sql), status=400, code="sql_error")
    # A value that is an expression is one: computed, and set for the session.
    succeeded(chargewire, ro, "SET search_path = lower('MAIN')")
    succeeded(chargewire, ro, "SET progress_bar_time = 1000 + 1")
    assert one(chargewire, ro, "SELECT current_setting('progress_bar_time')") == 1001


def test_the_expression_depth_limit_cannot_be_lifted(chargewire, duck, ro, unchanged):
    # DuckDB's guard against statements that exhaust the stack: with max_expression_depth
    # raised, a 10 KB SELECT ended the process for every session. The setting is refused, and
    # the statement is still an error DuckDB reports.
    deep = "SELECT " + "abs(" * 2000 + "1" + ")" * 2000
    with unchanged(subject=ro):
        sql = "SET max_expression_depth = 20000"
        if EXPRESSION_DEPTH_CAPPED:
            # This value is invalid even in a writable session on a server with a cap.
            problem = assert_problem(response=chargewire.submit(session_id=ro, sql=sql), status=400, code="sql_error")
            assert "max_expression_depth can be set to a number up to 1000" in problem["detail"], problem
        else:
            refused(chargewire, ro, sql)
        assert one(chargewire, ro, "SELECT current_setting('max_expression_depth')") == 1000
        response = chargewire.submit(session_id=ro, sql=deep)
        assert_problem(response=response, status=400, code="sql_error")
    assert rows(chargewire, ro, "SELECT 1 AS one") == [{"one": 1}]


# Every setting on the reader's list (ReadOnlyPolicy::Default(), cpp/src/duckdb/duckdb_read_only.cc)
# with a value that differs from the default.
LISTED_SETTINGS = {
    "TimeZone": "'Asia/Tokyo'", "Calendar": "'japanese'", "default_collation": "'nocase'", "default_null_order": "'nulls_first'",
    "null_order": "'nulls_first'", "default_order": "'desc'", "integer_division": "true", "ieee_floating_point_ops": "false",
    "error_on_division_by_zero": "false", "order_by_non_integer_literal": "true", "scalar_subquery_error_on_multiple_rows": "false",
    "preserve_identifier_case": "false", "disable_timestamptz_casts": "true", "errors_as_json": "true",
    "search_path": "'{schema}'", "schema": "'{schema}'",
    "arrow_lossless_conversion": "true", "arrow_large_buffer_size": "true", "arrow_output_list_view": "true",
    "arrow_output_version": "'1.4'", "binary_as_string": "true", "enable_progress_bar": "true", "progress_bar_time": "1000",
}
ALL_SETTINGS = "SELECT md5(string_agg(name || '=' || value, '|' ORDER BY name ASC)) FROM duckdb_settings()"


@pytest.mark.parametrize("setting", list(LISTED_SETTINGS))
def test_a_listed_setting_is_set_for_the_session_only_and_can_be_reset(chargewire, duck, ro, observer, setting):
    schema = name("ro_setting")
    succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
    value = LISTED_SETTINGS[setting].format(schema=schema)
    current = f"SELECT current_setting('{setting}')::VARCHAR"
    everyone_elses = one(chargewire, observer, ALL_SETTINGS)
    default = one(chargewire, ro, current)
    assert default == one(chargewire, observer, current)
    # Embedders may override initial settings (Hardcharger enables its progress
    # reporting). Exercise an actual change, and compare RESET with a fresh reader's
    # RESET rather than assuming the engine default equals the server's initial value.
    if value in ("true", "false") and default.lower() in ("true", "false"):
        value = "false" if default.lower() == "true" else "true"
    resetter = chargewire.new_session(options={"read_only": True})
    succeeded(chargewire, resetter, f"RESET {setting}")
    reset_default = one(chargewire, resetter, current)
    chargewire.http.delete(url=chargewire.session_url(resetter), headers=chargewire.headers()).raise_for_status()
    succeeded(chargewire, ro, f"SET {setting} = {value}")
    try:
        changed = one(chargewire, ro, current)
        assert changed != default, "the SET took no effect in the session"
        # DuckDB's plain SET is for every session for some of these; here it is the session's.
        assert one(chargewire, observer, current) == default
        assert one(chargewire, observer, ALL_SETTINGS) == everyone_elses
        assert one(chargewire, chargewire.new_session(), current) == default  # nor for a session made afterwards
    finally:
        succeeded(chargewire, ro, f"RESET {setting}")
    assert one(chargewire, ro, current) == reset_default
    assert one(chargewire, observer, ALL_SETTINGS) == everyone_elses


@pytest.mark.parametrize("sql,setting,expected", [
    ("SET TIME ZONE 'Asia/Tokyo'", "TimeZone", "Asia/Tokyo"),        # the PostgreSQL spelling
    ("SET \"TimeZone\" = 'Asia/Tokyo'", "TimeZone", "Asia/Tokyo"),   # a quoted name
    ("SET timezone TO 'Asia/Tokyo'", "TimeZone", "Asia/Tokyo"),
    ("SET SESSION TimeZone = 'Asia/Tokyo'", "TimeZone", "Asia/Tokyo"),
    ("PRAGMA TimeZone = 'Asia/Tokyo'", "TimeZone", "Asia/Tokyo"),
    ("SET schema '{schema}'", "schema", "{schema}"),
    ("SET default_null_order = (nulls_first)", "default_null_order", "NULLS_FIRST"),
    ("PRAGMA default_collation = 'nocase'", "default_collation", "nocase"),
    ("SET default_order = 'desc'", "default_order", "DESC"),
    ("SET SESSION default_null_order = 'nulls_first'", "default_null_order", "NULLS_FIRST"),
    ("SET integer_division TO true", "integer_division", "true"),
])
def test_every_spelling_of_a_listed_setting_works(chargewire, duck, ro, observer, sql, setting, expected):
    # The setting decides, not how the statement spells it: an earlier server refused these.
    schema = name("ro_spelled")
    succeeded(chargewire, duck, f"CREATE SCHEMA {schema}")
    current = f"SELECT current_setting('{setting}')::VARCHAR"
    everyone_elses = one(chargewire, observer, ALL_SETTINGS)
    default = one(chargewire, observer, current)
    succeeded(chargewire, ro, sql.format(schema=schema))
    assert one(chargewire, ro, current) == expected.format(schema=schema) != default
    assert one(chargewire, observer, current) == default and one(chargewire, observer, ALL_SETTINGS) == everyone_elses
    succeeded(chargewire, duck, sql.format(schema=schema))  # the twin
    succeeded(chargewire, duck, f"RESET {setting}")
    for reset in (f"RESET {setting}", f"RESET SESSION {setting}", f"SET {setting} TO DEFAULT"):
        succeeded(chargewire, ro, sql.format(schema=schema))
        succeeded(chargewire, ro, reset)
        assert one(chargewire, ro, current) == default, reset


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


@pytest.mark.parametrize("sql", [
    "PRAGMA enable_verification", "PRAGMA disable_verification", "PRAGMA enable_object_cache", "PRAGMA force_checkpoint",
    "PRAGMA verify_parallelism", "PRAGMA enable_checkpoint_on_shutdown", "PRAGMA disable_checkpoint_on_shutdown",
    "PRAGMA add_parquet_key('ro_key', '0123456789112345')", "PRAGMA disable_profiling",
])
def test_a_pragma_that_is_not_on_the_list_is_refused(chargewire, duck, ro, unchanged, sql):
    with unchanged(subject=ro):
        refused(chargewire, ro, sql)
    assert answer_of(chargewire, chargewire.new_session(), sql)[0] == 200  # the twin, in a session of its own


# ---- Every transaction of a read-only session is READ ONLY ----


@contextlib.contextmanager
def query_log(chargewire, session_id: str):
    """DuckDB's own log of the statements it is given, switched on for the block and off
    after it, with every setting of the server put back (asserted)."""
    before = one(chargewire, session_id, ALL_SETTINGS)
    kept = {setting: one(chargewire, session_id, f"SELECT current_setting('{setting}')::VARCHAR")
            for setting in ("enabled_log_types", "logging_level", "logging_mode", "logging_storage")}
    succeeded(chargewire, session_id, "CALL enable_logging('QueryLog')")
    try:
        yield
    finally:
        succeeded(chargewire, session_id, "CALL disable_logging()")
        succeeded(chargewire, session_id, "CALL truncate_duckdb_logs()")
        for setting, value in kept.items():
            succeeded(chargewire, session_id, f"SET GLOBAL {setting} = '{value}'")
        assert one(chargewire, session_id, ALL_SETTINGS) == before, "the server's logging settings were not put back"


def test_every_transaction_of_a_read_only_session_is_started_read_only(chargewire, duck, table, unchanged):
    # The first of the three things that hold a read-only session: DuckDB itself refuses a
    # write of the main catalog in a READ ONLY transaction, whatever else let it through. Seen
    # in DuckDB's query log: on the session's connection every transaction begins READ ONLY,
    # whichever way it came to be started, and no transaction statement of the client's is
    # passed on as it was written. And every one is ended by ROLLBACK, also where the client
    # said COMMIT: a reader has nothing to commit, and DuckDB treats the commit of a READ ONLY
    # transaction in which something was changed after all as fatal for the whole database.
    marker = f"ro-marker-{uuid.uuid4().hex}"
    sid = chargewire.new_session(options={"read_only": True})
    with query_log(chargewire, duck):
        assert one(chargewire, sid, f"SELECT '{marker}'") == marker         # autocommit
        succeeded(chargewire, sid, "BEGIN")                                 # the client's own
        assert one(chargewire, sid, f"SELECT count(*) FROM {table}") == 3
        succeeded(chargewire, sid, "COMMIT")
        succeeded(chargewire, sid, "START TRANSACTION")
        succeeded(chargewire, sid, "ROLLBACK")
        succeeded(chargewire, sid, "BEGIN TRANSACTION READ ONLY")
        succeeded(chargewire, sid, "END")
        succeeded(chargewire, sid, f"BEGIN; SELECT count(*) FROM {table}; COMMIT; SELECT 1")  # in one request
        with unchanged(tables=[table]):
            refused(chargewire, sid, f"INSERT INTO {table} VALUES (4, 'c')")  # after a refusal
        assert chargewire.submit(session_id=sid, sql="SELECT * FROM ro_no_such_table").status_code == 400  # after an error
        assert one(chargewire, sid, "SELECT 1") == 1
        assert patch(chargewire, sid, {"autocommit": False})["errors"] == {}  # manual commit
        for call in ("commit", "rollback"):
            assert one(chargewire, sid, "SELECT 1") == 1
            assert chargewire.http.post(url=f"{chargewire.session_url(sid)}/{call}", headers=chargewire.headers()).status_code == 204
        assert one(chargewire, sid, "SELECT 1") == 1
        assert patch(chargewire, sid, {"autocommit": True})["errors"] == {}
        prepared_id = prepared_in(chargewire, sid, "BEGIN")                 # prepared
        assert run_prepared(chargewire, sid, prepared_id) == (200, "succeeded")
        succeeded(chargewire, sid, "ROLLBACK")
        assert chargewire.http.get(url=f"{chargewire.session_url(sid)}/metadata/tables", headers=chargewire.headers()).status_code == 200
        logged = rows(chargewire, duck, f"""
            SELECT l.message FROM duckdb_logs() l JOIN duckdb_log_contexts() c ON c.context_id = l.context_id
            WHERE l.type = 'QueryLog' AND c.connection_id IN (
              SELECT c2.connection_id FROM duckdb_logs() l2 JOIN duckdb_log_contexts() c2 ON c2.context_id = l2.context_id
              WHERE l2.type = 'QueryLog' AND l2.message = 'SELECT ''{marker}''')
            ORDER BY l.timestamp ASC, l.context_id ASC""")
    statements = [row["message"] for row in logged]
    assert f"SELECT '{marker}'" in statements, "the session's statements are not in DuckDB's query log"
    transaction_words = ("BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT")
    starts = [s for s in statements if s.upper().startswith(("BEGIN", "START"))]
    assert len(starts) >= 12, statements
    assert set(starts) == {"BEGIN TRANSACTION READ ONLY"}, starts
    assert {s for s in statements if s.upper().startswith(transaction_words)} == {"BEGIN TRANSACTION READ ONLY", "ROLLBACK"}, statements
    assert statements.count("ROLLBACK") >= 12, statements
    # No statement ran outside a transaction the server began: the first one logged is a BEGIN.
    assert statements[0] == "BEGIN TRANSACTION READ ONLY", statements[:3]


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
        f"SELECT * FROM {table} UNION ALL BY NAME SELECT v, id FROM {table}",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT sum(n) FROM r",
        f"WITH c AS MATERIALIZED (SELECT * FROM {table}) SELECT * FROM c, c c2", f"WITH c AS (SELECT * FROM {table}) SELECT * FROM c",
        f"SELECT * FROM {table} WHERE id IN (SELECT id FROM {table})", f"SELECT * FROM {table} t WHERE EXISTS (SELECT 1 FROM {table} b WHERE b.id = t.id)",
        f"SELECT (SELECT max(id) FROM {table} b WHERE b.id <= t.id) FROM {table} t", f"SELECT * FROM {table} t, LATERAL (SELECT t.id + 1 AS next) l",
        f"SELECT * FROM {table} ORDER BY id LIMIT 1 OFFSET 1", f"SELECT * FROM {table} LIMIT 50%", f"SELECT * FROM {table} ORDER BY v DESC, id",
        f"SELECT * FROM {table} USING SAMPLE 50%", f"SELECT * FROM {table} TABLESAMPLE reservoir(2)", f"SELECT * FROM {table} WHERE id = 1 AND v LIKE 'a%'",
        f"SELECT v, id, count(*) FROM {table} GROUP BY ROLLUP (v, id)", f"SELECT v, id, count(*) FROM {table} GROUP BY GROUPING SETS ((v), (id), ())",
        "SELECT 1", "VALUES (1, 'a'), (2, 'b')", f"FROM {table}", f"TABLE {table}", "SELECT * FROM (VALUES (1), (2)) v(x)",
        "SELECT * FROM range(10)", "SELECT * FROM generate_series(1, 3)", "SELECT unnest([1, 2, 3])", "SELECT * FROM unnest([1, 2]) WITH ORDINALITY",
        "SELECT unnest([{'a': 1}], recursive := true)", "SELECT * FROM repeat('x', 3)", "SELECT * FROM json_each('[1, 2]')",
        "SELECT * FROM repeat_row(1, 2, num_rows = 3)", "SELECT * FROM json_tree('{\"a\": [1]}')",
        f"PIVOT {table} ON v IN ('a', 'b') USING count(*)", "UNPIVOT (SELECT 1 AS a, 2 AS b) ON a, b",
        "SELECT random() >= 0, now() IS NOT NULL, uuid() IS NOT NULL",
        # a table function called for each row of its input (DuckDB's in-out functions): the
        # function is held to the list as one that is scanned is
        "SELECT * FROM (SELECT '[1, 2]' AS x) s, LATERAL json_each(s.x)", "SELECT * FROM (SELECT '{\"a\": [1]}' AS x) s, LATERAL json_tree(s.x)",
        f"SELECT * FROM {table} t, LATERAL range(t.id)", f"SELECT * FROM {table} t, range(t.id)",
        f"SELECT * FROM {table} t, LATERAL generate_series(1, t.id)", "SELECT * FROM (SELECT [1, 2] AS l) s, LATERAL unnest(s.l)",
        f"SELECT * FROM {table} t, LATERAL summary((SELECT t.id))",
        # describing and showing
        f"DESCRIBE {table}", f"DESCRIBE SELECT * FROM {table}", "SHOW TABLES", "SHOW ALL TABLES", "SHOW DATABASES", f"SHOW {table}",
        f"SUMMARIZE {table}", f"SUMMARIZE SELECT * FROM {table}", f"EXPLAIN SELECT * FROM {table}", f"EXPLAIN ANALYZE SELECT count(*) FROM {table}",
        f"EXPLAIN (FORMAT JSON) SELECT * FROM {table}", f"EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM {table}", "SELECT current_setting('TimeZone')",
        # a parenthesized query: an earlier server took the parenthesis for EXPLAIN's option list
        "EXPLAIN (SELECT 1)", "EXPLAIN ANALYZE (SELECT 1) UNION ALL (SELECT 2)", f"EXPLAIN ANALYZE (SELECT * FROM {table})",
        f"EXPLAIN PIVOT {table} ON v IN ('a', 'b') USING count(*)", f"EXPLAIN ANALYZE PIVOT {table} ON v IN ('a', 'b') USING count(*)",
        # the catalog
        "SELECT * FROM information_schema.tables", "SELECT * FROM information_schema.columns", "SELECT * FROM information_schema.schemata",
        "SELECT * FROM duckdb_tables()", "SELECT * FROM duckdb_columns()", "SELECT * FROM duckdb_views()", "SELECT * FROM duckdb_schemas()",
        "SELECT * FROM duckdb_databases()", "SELECT * FROM duckdb_functions() LIMIT 5", "SELECT * FROM duckdb_types() LIMIT 5",
        "SELECT * FROM duckdb_settings()", "SELECT * FROM duckdb_extensions()", "SELECT * FROM duckdb_constraints()", "SELECT * FROM duckdb_indexes()",
        "SELECT * FROM duckdb_sequences()", "SELECT * FROM duckdb_keywords() LIMIT 5", "SELECT * FROM pg_catalog.pg_tables", "SELECT * FROM sqlite_master",
        f"SELECT * FROM pragma_table_info('{table}')", "SELECT * FROM pragma_database_size()", "SELECT * FROM pragma_version()",
        f"PRAGMA table_info('{table}')", "PRAGMA show_tables", "PRAGMA database_list", "PRAGMA version", "PRAGMA database_size",
        # more of the catalog and the server's own state: readers an earlier list left out
        "SELECT count(*) FROM duckdb_logs()", "SELECT count(*) FROM duckdb_log_contexts()", "SELECT count(*) FROM duckdb_secrets()",
        "SELECT count(*) FROM duckdb_extension_repositories()", "SELECT count(*) FROM duckdb_connection_count()",
        "SELECT count(*) FROM duckdb_external_file_cache()", "SELECT count(*) FROM duckdb_available_metrics()",
        "SELECT count(*) FROM duckdb_grammar_extensions()", f"SELECT count(*) FROM duckdb_table_sample('{table}')",
        "SELECT count(*) FROM which_secret('s3://bucket/key', 's3')",
        "SELECT count(*) FROM duckdb_approx_database_count()", "SELECT count(*) FROM duckdb_eviction_queues()",
        "SELECT count(*) FROM duckdb_external_resources()", "SELECT count(*) FROM duckdb_external_resource_types()",
        "SELECT count(*) FROM test_all_types()", "SELECT count(*) FROM duckdb_prepared_statements()", "SELECT count(*) FROM duckdb_temporary_files()",
        "SELECT count(*) FROM duckdb_memory()", "SELECT count(*) FROM duckdb_variables()", "SELECT count(*) FROM duckdb_dependencies()",
        "SELECT count(*) FROM duckdb_optimizers()", "SELECT count(*) FROM duckdb_triggers()", "SELECT count(*) FROM duckdb_coordinate_systems()",
        "SELECT count(*) FROM duckdb_dialects()", "SELECT count(*) FROM duckdb_secret_types()", "SELECT count(*) FROM duckdb_secret_type_parameters()",
        "SELECT count(*) FROM pragma_collations()", "SELECT count(*) FROM pragma_metadata_info()", "SELECT * FROM pragma_platform()",
        "SELECT * FROM pragma_user_agent()", f"SELECT count(*) FROM pragma_storage_info('{table}')", f"PRAGMA storage_info('{table}')",
        "PRAGMA metadata_info", "PRAGMA platform", "PRAGMA functions", "PRAGMA collations", "PRAGMA user_agent", "PRAGMA extension_versions",
        "PRAGMA show_databases", "PRAGMA show_tables_expanded",
        "SELECT count(*) FROM icu_calendar_names()", "SELECT count(*) FROM pg_timezone_names()", "SELECT count(*) FROM summary((SELECT 1))",
        # files
        f"SELECT * FROM read_csv('{tmp}/data.csv')", f"SELECT * FROM '{tmp}/data.csv'", f"SELECT * FROM read_csv_auto('{tmp}/*.csv')",
        f"SELECT * FROM sniff_csv('{tmp}/data.csv')", f"SELECT * FROM read_parquet('{tmp}/data.parquet')", f"SELECT * FROM '{tmp}/data.parquet'",
        f"SELECT * FROM parquet_scan('{tmp}/*.parquet')", f"SELECT * FROM read_parquet(['{tmp}/data.parquet', '{tmp}/data.parquet'], filename = true)",
        f"SELECT * FROM parquet_metadata('{tmp}/data.parquet')", f"SELECT * FROM parquet_schema('{tmp}/data.parquet')",
        f"SELECT * FROM parquet_file_metadata('{tmp}/data.parquet')", f"SELECT * FROM parquet_kv_metadata('{tmp}/data.parquet')",
        f"SELECT count(*) FROM parquet_full_metadata('{tmp}/data.parquet')", f"SELECT count(*) FROM parquet_bloom_probe('{tmp}/data.parquet', 'id', 1)",
        f"SELECT * FROM read_json('{tmp}/data.json')", f"SELECT * FROM read_json_auto('{tmp}/data.json')", f"SELECT * FROM '{tmp}/data.json'",
        f"SELECT * FROM read_ndjson('{tmp}/data.json')", f"SELECT * FROM read_json_objects('{tmp}/data.json')",
        f"SELECT * FROM read_ndjson_auto('{tmp}/data.json')", f"SELECT * FROM read_json_objects_auto('{tmp}/data.json')",
        f"SELECT * FROM read_ndjson_objects('{tmp}/data.json')",
        f"SELECT * FROM glob('{tmp}/*')", f"SELECT * FROM read_text('{tmp}/data.csv')", f"SELECT size FROM read_blob('{tmp}/data.csv')",
        f"SELECT * FROM read_single_csv_file('{tmp}/data.csv')", f"SELECT * FROM read_single_parquet_file('{tmp}/data.parquet')",
        f"SELECT * FROM read_single_json_file('{tmp}/data.json')",
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


def test_what_a_reader_uses_still_works(chargewire, duck, ro, table, files, unchanged):
    # One session for all of them, as a reader has: a refusal names the statement. Each is
    # also run in the read-write session, so a statement that fails for another reason
    # shows as that, not as a read-only refusal. And reading changes nothing either.
    # Run writable twins before the snapshot: their first call can install/load a
    # known extension (e.g. autocomplete on Windows). The reader's whole run must
    # still leave the database, settings, extensions and files unchanged.
    statements = reads(table, files)
    if not AUTOCOMPLETE_DISABLED:
        statements.extend(AUTOCOMPLETE_READS)
    for sql in statements:
        twin = chargewire.submit(session_id=duck, sql=sql, arrow=True)
        assert twin.status_code == 200, (sql, twin.text)
    failures = []
    with unchanged(tables=[table]):
        for sql in statements:
            response = chargewire.submit(session_id=ro, sql=sql, arrow=True)
            if response.status_code != 200:
                failures.append(f"{sql}\n    -> {response.status_code} {response.text}")
    assert not failures, "refused or failed in a read-only session:\n" + "\n".join(failures)


@pytest.mark.skipif(not AUTOCOMPLETE_DISABLED, reason="the read suite tests enabled autocomplete")
@pytest.mark.parametrize("sql", AUTOCOMPLETE_READS)
def test_disabled_autocomplete_is_refused_in_both_session_modes(chargewire, duck, ro, unchanged, sql):
    with unchanged(subject=ro):
        for session_id in (duck, ro):
            response = chargewire.submit(session_id=session_id, sql=sql, arrow=True)
            problem = assert_problem(response=response, status=403, code="permission_denied")
            assert "the autocomplete extension is turned off on this server" in problem["detail"], problem


# Plans as deep as a BI tool's or a generated query's: an earlier server could not read a
# plan more than 16 operators deep and refused these with read_only. (Sizes that DuckDB
# itself runs in any session with room to spare on every platform's thread stacks.)
def deep_plans(table: str) -> dict[str, str]:
    left_joins = " ".join(f"LEFT JOIN {table} t{i} ON t{i}.id = t0.id + {i}" for i in range(1, 41))
    inner_joins = " ".join(f"JOIN {table} t{i} ON t{i}.id = t0.id" for i in range(1, 33))
    window_ctes = [f"c0 AS (SELECT id, v FROM {table})"] + [
        f"c{i} AS (SELECT id, v, row_number() OVER (PARTITION BY v ORDER BY id) AS rn{i} FROM c{i - 1} WHERE id > 0 QUALIFY rn{i} < 100)"
        for i in range(1, 31)]
    plain_ctes = [f"c0 AS (SELECT * FROM {table})"] + [f"c{i} AS (SELECT * FROM c{i - 1})" for i in range(1, 61)]
    nested = f"SELECT * FROM {table} ORDER BY id LIMIT 100"
    for i in range(60):
        nested = f"SELECT * FROM ({nested}) s{i} WHERE id > 0"
    return {
        "a fact table with 40 LEFT JOINs": f"SELECT count(*) AS n FROM {table} t0 {left_joins}",
        "33 tables joined": f"SELECT count(*) AS n, min(t32.v) AS v FROM {table} t0 {inner_joins}",
        "30 chained CTEs, each with a window function": f"WITH {', '.join(window_ctes)} SELECT v, count(*) AS n FROM c30 GROUP BY v ORDER BY v",
        "60 chained CTEs": f"WITH {', '.join(plain_ctes)} SELECT count(*) AS n FROM c60",
        "60 nested subqueries": f"SELECT count(*) AS n FROM ({nested}) deepest",
        "200 branches of a UNION ALL": "SELECT count(*) AS n FROM (" + " UNION ALL ".join([f"SELECT id FROM {table}"] * 200) + ") u",
        "a 40-deep expression": "SELECT " + "abs(" * 40 + "max(id)" + ")" * 40 + f" AS n FROM {table}",
        "EXPLAIN ANALYZE of a deep plan": f"EXPLAIN ANALYZE SELECT count(*) AS n FROM {table} t0 {left_joins}",
    }


@pytest.mark.parametrize("which", list(deep_plans("t")))
def test_a_deep_plan_runs(chargewire, duck, ro, table, which):
    sql = deep_plans(table)[which]
    expected = answer_of(chargewire, duck, sql)
    assert expected[0] == 200, expected
    # Within seconds: checking a statement must not take longer than running it. (The screen of
    # what DuckDB evaluates while it binds once walked every derived table twice: 2^60 steps for
    # the sixty nested ones, on the session's engine thread, where nothing can cancel it.) Asked
    # on the status path with a wait, so a statement that does not end fails here and now.
    started = time.monotonic()
    bounded = chargewire.submit(session_id=ro, sql=sql, headers={"Prefer": "wait=10"})
    assert bounded.status_code == 201 and bounded.json()["state"] == "succeeded", bounded.text[:300]
    assert time.monotonic() - started < 5, f"{time.monotonic() - started:.1f} s"
    answer = answer_of(chargewire, ro, sql)
    assert answer[0] == 200, answer
    if not sql.startswith("EXPLAIN"):
        assert answer == expected
    prepared_id = prepared_in(chargewire, ro, sql)  # and prepared, as a driver sends it
    assert run_prepared(chargewire, ro, prepared_id)[0] == 200


@pytest.fixture(scope="module")
def warehouse(server):
    """Small tables in the shape of TPC-H's, in a schema of their own; the schema's name."""
    chargewire = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
    sid = chargewire.new_session()
    info = chargewire.http.get(url=f"{chargewire.session_url(sid)}/metadata/server-info", headers=chargewire.headers())
    if "duckdb_version" not in info.json():
        chargewire.close()
        pytest.skip(reason="not a DuckDB server")
    s = name("ro_warehouse")
    for sql in (
        f"CREATE SCHEMA {s}",
        f"CREATE TABLE {s}.region AS SELECT * FROM (VALUES (0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA')) v(r_regionkey, r_name)",
        f"CREATE TABLE {s}.nation AS SELECT * FROM (VALUES (0, 'ALGERIA', 0), (1, 'BRAZIL', 1), (2, 'CANADA', 1), (3, 'CHINA', 2), "
        f"(4, 'INDIA', 2)) v(n_nationkey, n_name, n_regionkey)",
        f"CREATE TABLE {s}.supplier AS SELECT i AS s_suppkey, 'Supplier#' || i AS s_name, i % 5 AS s_nationkey, "
        f"(i * 100.5)::DECIMAL(15, 2) AS s_acctbal FROM range(1, 11) r(i)",
        f"CREATE TABLE {s}.customer AS SELECT i AS c_custkey, 'Customer#' || i AS c_name, i % 5 AS c_nationkey, "
        f"['BUILDING', 'MACHINERY', 'AUTOMOBILE'][i % 3 + 1] AS c_mktsegment, (i * 37.25)::DECIMAL(15, 2) AS c_acctbal, "
        f"(10 + i % 5)::VARCHAR || '-555' AS c_phone FROM range(1, 31) r(i)",
        f"CREATE TABLE {s}.part AS SELECT i AS p_partkey, 'part ' || i AS p_name, 'Brand#' || (i % 3 + 1) AS p_brand, "
        f"['STANDARD BRASS', 'PROMO STEEL', 'ECONOMY TIN'][i % 3 + 1] AS p_type, i % 50 + 1 AS p_size FROM range(1, 21) r(i)",
        f"CREATE TABLE {s}.partsupp AS SELECT p AS ps_partkey, q AS ps_suppkey, (p * q) % 100 AS ps_availqty, "
        f"((p + q) * 1.5)::DECIMAL(15, 2) AS ps_supplycost FROM range(1, 21) a(p), range(1, 11) b(q) WHERE (p + q) % 3 = 0",
        f"CREATE TABLE {s}.orders AS SELECT i AS o_orderkey, i % 30 + 1 AS o_custkey, ['O', 'F', 'P'][i % 3 + 1] AS o_orderstatus, "
        f"DATE '1995-01-01' + (i * 7)::INTEGER AS o_orderdate, ['1-URGENT', '2-HIGH', '3-MEDIUM'][i % 3 + 1] AS o_orderpriority, "
        f"(i * 111.11)::DECIMAL(15, 2) AS o_totalprice FROM range(1, 101) r(i)",
        f"CREATE TABLE {s}.lineitem AS SELECT o AS l_orderkey, l AS l_linenumber, (o * l) % 20 + 1 AS l_partkey, "
        f"(o + l) % 10 + 1 AS l_suppkey, (l * 5)::DECIMAL(15, 2) AS l_quantity, (o * l * 10.0)::DECIMAL(15, 2) AS l_extendedprice, "
        f"((l % 10) / 100.0)::DECIMAL(15, 2) AS l_discount, 0.05::DECIMAL(15, 2) AS l_tax, ['R', 'A', 'N'][(o + l) % 3 + 1] AS l_returnflag, "
        f"['O', 'F'][l % 2 + 1] AS l_linestatus, DATE '1995-01-01' + (o * 7 + l)::INTEGER AS l_shipdate, "
        f"DATE '1995-01-01' + (o * 7 + l + 3)::INTEGER AS l_commitdate, DATE '1995-01-01' + (o * 7 + l * 2)::INTEGER AS l_receiptdate "
        f"FROM range(1, 101) a(o), range(1, 5) b(l)",
    ):
        succeeded(chargewire, sid, sql)
    chargewire.close()
    return s


# Multi-join aggregate queries in the shapes of TPC-H and TPC-DS: what a warehouse's readers send.
WAREHOUSE_QUERIES = {
    "pricing summary (TPC-H 1)": """
        SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
               sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_discount) AS avg_disc, count(*) AS count_order
        FROM lineitem WHERE l_shipdate <= DATE '1996-12-01' - INTERVAL 90 DAY GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus""",
    "minimum cost supplier (TPC-H 2)": """
        SELECT s_acctbal, s_name, n_name, p_partkey FROM part, supplier, partsupp, nation, region
        WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size < 40 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey AND r_name <> 'AFRICA'
          AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey
                               AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name <> 'AFRICA')
        ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100""",
    "shipping priority (TPC-H 3)": """
        SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate FROM customer, orders, lineitem
        WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < DATE '1996-06-01'
        GROUP BY l_orderkey, o_orderdate ORDER BY revenue DESC, o_orderdate, l_orderkey LIMIT 10""",
    "local supplier volume (TPC-H 5)": """
        SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey GROUP BY n_name ORDER BY revenue DESC, n_name""",
    "volume shipping between two nations (TPC-H 7)": """
        SELECT supp_nation, cust_nation, l_year, sum(volume) AS revenue FROM (
          SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, extract(year FROM l_shipdate) AS l_year,
                 l_extendedprice * (1 - l_discount) AS volume
          FROM supplier, lineitem, orders, customer, nation n1, nation n2
          WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey AND n1.n_name <> n2.n_name) shipping
        GROUP BY supp_nation, cust_nation, l_year ORDER BY supp_nation, cust_nation, l_year""",
    "national market share, eight tables (TPC-H 8)": """
        SELECT o_year, sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share FROM (
          SELECT extract(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) AS volume, n2.n_name AS nation
          FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
          WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey AND l_orderkey = o_orderkey AND o_custkey = c_custkey
            AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND s_nationkey = n2.n_nationkey) all_nations
        GROUP BY o_year ORDER BY o_year""",
    "product type profit (TPC-H 9)": """
        SELECT nation, o_year, sum(amount) AS sum_profit FROM (
          SELECT n_name AS nation, extract(year FROM o_orderdate) AS o_year, l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
          FROM part, supplier, lineitem, partsupp, orders, nation
          WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%1%') profit
        GROUP BY nation, o_year ORDER BY nation, o_year DESC""",
    "customer distribution, an outer join under two aggregations (TPC-H 13)": """
        SELECT c_count, count(*) AS custdist FROM (
          SELECT c_custkey, count(o_orderkey) AS c_count FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_orderpriority <> '1-URGENT'
          GROUP BY c_custkey) c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC""",
    "top supplier, a CTE used twice (TPC-H 15)": """
        WITH revenue AS (SELECT l_suppkey AS supplier_no, sum(l_extendedprice * (1 - l_discount)) AS total_revenue FROM lineitem GROUP BY l_suppkey)
        SELECT s_suppkey, s_name, total_revenue FROM supplier, revenue
        WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM revenue) ORDER BY s_suppkey""",
    "large volume customers, IN over a HAVING (TPC-H 18)": """
        SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) AS quantity FROM customer, orders, lineitem
        WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 40)
          AND c_custkey = o_custkey AND o_orderkey = l_orderkey
        GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice ORDER BY o_totalprice DESC, o_orderdate, o_orderkey LIMIT 100""",
    "suppliers who kept orders waiting, EXISTS and NOT EXISTS (TPC-H 21)": """
        SELECT s_name, count(*) AS numwait FROM supplier, lineitem l1, orders, nation
        WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
          AND EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
          AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey
                          AND l3.l_receiptdate > l3.l_commitdate + 100)
          AND s_nationkey = n_nationkey GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100""",
    "global sales opportunity (TPC-H 22)": """
        SELECT cntrycode, count(*) AS numcust, sum(c_acctbal) AS totacctbal FROM (
          SELECT substring(c_phone FROM 1 FOR 2) AS cntrycode, c_acctbal FROM customer
          WHERE substring(c_phone FROM 1 FOR 2) IN ('10', '11', '13')
            AND c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0.00)
            AND NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey AND o_orderstatus = 'P')) custsale
        GROUP BY cntrycode ORDER BY cntrycode""",
    "returns above the average of their group, a correlated CTE (TPC-DS 1)": """
        WITH customer_total AS (SELECT o_custkey AS ctr_customer, c_nationkey AS ctr_nation, sum(o_totalprice) AS ctr_total
                                FROM orders, customer WHERE o_custkey = c_custkey GROUP BY o_custkey, c_nationkey)
        SELECT c_name FROM customer_total ctr1, customer, nation
        WHERE ctr1.ctr_total > (SELECT avg(ctr_total) * 1.2 FROM customer_total ctr2 WHERE ctr1.ctr_nation = ctr2.ctr_nation)
          AND n_nationkey = ctr1.ctr_nation AND ctr1.ctr_customer = c_custkey ORDER BY c_name LIMIT 100""",
    "three channels united under a ROLLUP (TPC-DS 5)": """
        WITH urgent AS (SELECT n_name AS channel, o_orderkey AS id, o_totalprice AS sales FROM orders, customer, nation
                        WHERE o_custkey = c_custkey AND c_nationkey = n_nationkey AND o_orderpriority = '1-URGENT'),
             high AS (SELECT n_name AS channel, o_orderkey AS id, o_totalprice AS sales FROM orders, customer, nation
                      WHERE o_custkey = c_custkey AND c_nationkey = n_nationkey AND o_orderpriority = '2-HIGH'),
             medium AS (SELECT n_name AS channel, o_orderkey AS id, o_totalprice AS sales FROM orders, customer, nation
                        WHERE o_custkey = c_custkey AND c_nationkey = n_nationkey AND o_orderpriority = '3-MEDIUM')
        SELECT channel, id % 3 AS bucket, sum(sales) AS sales FROM (SELECT * FROM urgent UNION ALL SELECT * FROM high UNION ALL SELECT * FROM medium) x
        GROUP BY ROLLUP (channel, bucket) ORDER BY channel NULLS LAST, bucket NULLS LAST""",
    "a share of the class total, a window over an aggregate (TPC-DS 12)": """
        SELECT p_brand, p_type, sum(l_extendedprice) AS itemrevenue,
               sum(l_extendedprice) * 100 / sum(sum(l_extendedprice)) OVER (PARTITION BY p_brand) AS revenueratio,
               rank() OVER (PARTITION BY p_brand ORDER BY sum(l_extendedprice) DESC) AS in_brand
        FROM lineitem, part WHERE l_partkey = p_partkey GROUP BY p_brand, p_type ORDER BY p_brand, p_type""",
    "customers of every channel, INTERSECT and EXCEPT (TPC-DS 38 and 87)": """
        SELECT count(*) AS n FROM (
          (SELECT DISTINCT c_name, o_orderdate FROM orders, customer WHERE o_custkey = c_custkey AND o_orderstatus = 'O'
           INTERSECT SELECT DISTINCT c_name, o_orderdate FROM orders, customer WHERE o_custkey = c_custkey AND o_orderpriority <> '1-URGENT')
          EXCEPT SELECT DISTINCT c_name, o_orderdate FROM orders, customer WHERE o_custkey = c_custkey AND o_totalprice > 9000) hot""",
    "two aggregates met in a FULL OUTER JOIN (TPC-DS 97)": """
        WITH by_supplier AS (SELECT l_suppkey AS k, l_partkey AS item FROM lineitem GROUP BY l_suppkey, l_partkey),
             by_stock AS (SELECT ps_suppkey AS k, ps_partkey AS item FROM partsupp GROUP BY ps_suppkey, ps_partkey)
        SELECT sum(CASE WHEN a.k IS NOT NULL AND b.k IS NULL THEN 1 ELSE 0 END) AS shipped_only,
               sum(CASE WHEN a.k IS NULL AND b.k IS NOT NULL THEN 1 ELSE 0 END) AS stocked_only,
               sum(CASE WHEN a.k IS NOT NULL AND b.k IS NOT NULL THEN 1 ELSE 0 END) AS both_ways
        FROM by_supplier a FULL OUTER JOIN by_stock b ON a.k = b.k AND a.item = b.item""",
    "ranked within GROUPING SETS (TPC-DS 36)": """
        SELECT sum(l_extendedprice * (1 - l_discount)) / sum(l_extendedprice) AS margin, n_name, p_brand, grouping(n_name) + grouping(p_brand) AS lochierarchy,
               rank() OVER (PARTITION BY grouping(n_name) + grouping(p_brand), CASE WHEN grouping(p_brand) = 0 THEN n_name END
                            ORDER BY sum(l_extendedprice * (1 - l_discount)) / sum(l_extendedprice) ASC, p_brand) AS rank_within_parent
        FROM lineitem, part, supplier, nation WHERE l_partkey = p_partkey AND l_suppkey = s_suppkey AND s_nationkey = n_nationkey
        GROUP BY ROLLUP (n_name, p_brand) ORDER BY lochierarchy DESC, n_name NULLS LAST, rank_within_parent, p_brand NULLS LAST""",
}


@pytest.mark.parametrize("which", list(WAREHOUSE_QUERIES))
def test_a_warehouse_query_gives_a_reader_the_rows_it_gives_a_writer(chargewire, duck, warehouse, which):
    sid = chargewire.new_session(options={"read_only": True})
    for session_id in (duck, sid):
        succeeded(chargewire, session_id, f"USE {warehouse}")
    expected = answer_of(chargewire, duck, WAREHOUSE_QUERIES[which])
    assert expected[0] == 200, expected
    assert answer_of(chargewire, sid, WAREHOUSE_QUERIES[which]) == expected
    assert answer_of(chargewire, sid, "EXPLAIN ANALYZE " + WAREHOUSE_QUERIES[which])[0] == 200
    prepared_id = prepared_in(chargewire, sid, WAREHOUSE_QUERIES[which])
    assert run_prepared(chargewire, sid, prepared_id) == expected


def test_a_reader_sees_one_snapshot_inside_its_transaction(chargewire, duck, ro, table):
    succeeded(chargewire, ro, "BEGIN")
    assert one(chargewire, ro, f"SELECT count(*) FROM {table}") == 3
    other = name("ro_snapshot")
    succeeded(chargewire, duck, f"CREATE TABLE {other} AS SELECT 1 AS i")
    response = chargewire.submit(session_id=ro, sql=f"SELECT * FROM {other}")
    assert response.status_code == 400, response.text  # not there in the reader's snapshot
    succeeded(chargewire, ro, "ROLLBACK")
    assert one(chargewire, ro, f"SELECT count(*) FROM {other}") == 1


# ---- Errors: which are read_only, and what they say ----


def test_duckdbs_own_refusal_is_read_only_and_quotes_nothing(chargewire, duck, unchanged):
    # §11: read_only is the code of a read-only server as of a read-only session. An explicit
    # READ ONLY transaction in a read-write session gets it as well: there DuckDB refuses, and
    # its message names the database and can quote the statement. The answer is the server's
    # own words.
    other, marker = name(), "S3CR3T-" + uuid.uuid4().hex
    succeeded(chargewire, duck, f"CREATE TABLE {other} (v VARCHAR)")
    with unchanged(tables=[other]):
        succeeded(chargewire, duck, "BEGIN TRANSACTION READ ONLY")
        response = chargewire.submit(session_id=duck, sql=f"INSERT INTO {other} VALUES ('{marker}')")
        problem = assert_problem(response=response, status=403, code="read_only")
        assert problem.get("sqlstate") == "25006", problem
        assert marker not in response.text and other not in response.text, response.text
        succeeded(chargewire, duck, "ROLLBACK")
    succeeded(chargewire, duck, f"INSERT INTO {other} VALUES ('{marker}')")  # the twin


# Statements that fail for a reason of their own although their text spells DuckDB's
# read-only refusals: an earlier server went by those words, wherever in the message they
# stood, and told a session that was not read-only at all "read_only: never retry".
NOT_READ_ONLY = [
    ('SELECT * FROM "database attached in read-only mode S3CR3T-in-identifier"', 400, "sql_error"),
    ('SELECT * FROM "TransactionContext Error: Cannot write to database S3CR3T - transaction is launched in read-only mode"', 400, "sql_error"),
    ('SELECT * FROM "Invalid Input Error: Cannot execute statement of type which is attached in read-only mode!"', 400, "sql_error"),
    ("SELECT * FROM read_csv('/nonexistent/transaction is launched in read-only mode/S3CR3T-token-123.csv')", 422, "execution_error"),
    ("SELECT * FROM read_csv('/nonexistent/which is attached in read-only mode!')", 422, "execution_error"),
    ("SELECT error('TransactionContext Error: Cannot write to database \"x\" - transaction is launched in read-only mode')", 422, "execution_error"),
    ("SELECT error('Invalid Input Error: Cannot execute statement of type \"INSERT\" on database \"x\" which is attached in read-only mode!')",
     422, "execution_error"),
]


@pytest.mark.parametrize("sql,status,code", NOT_READ_ONLY)
def test_an_error_that_only_spells_a_read_only_refusal_keeps_its_own_code(chargewire, duck, ro, sql, status, code):
    for session_id in (duck, ro):
        response = chargewire.submit(session_id=session_id, sql=sql)
        problem = assert_problem(response=response, status=status, code=code)
        assert problem.get("sqlstate") != "25006", problem


@pytest.mark.parametrize("sql", [
    "SELECT nextval('{q}'), '{m}'", "SET memory_limit = '{m}'", "SET GLOBAL TimeZone = '{m}'",
    "PRAGMA add_parquet_key('{m}', '0123456789112345')", "CREATE SECRET (TYPE s3, KEY_ID 'k', SECRET '{m}')",
    "ATTACH 'postgres://u:{m}@127.0.0.1:1/db' AS ro_pg", "SELECT * FROM query('SELECT ''{m}'' FROM checkpoint()')",
    "COPY {t} TO '/tmp/{m}.csv'", "SET VARIABLE ro_x = (SELECT '{m}' FROM enable_logging())",
    "SELECT '{m}' AS v, (SELECT count(*) FROM enable_logging())", "EXPLAIN INSERT INTO {t} VALUES (0, '{m}')",
    "INSERT INTO {t} VALUES (0, '{m}')", "PIVOT {t} ON v IN (SELECT '{m}') USING count(*)",
    "WITH written AS (INSERT INTO {t} VALUES (0, '{m}') RETURNING id) SELECT count(*) FROM written",
])
def test_a_refusal_quotes_no_literal_of_the_statement(chargewire, duck, ro, table, unchanged, logging_stays_off, sql):
    marker, sequence = "S3CR3T-" + uuid.uuid4().hex, name("ro_seq")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {sequence}")
    with unchanged(subject=ro, tables=[table]):
        response = chargewire.submit(session_id=ro, sql=sql.format(m=marker, t=table, q=sequence))
        assert_problem(response=response, status=403, code="read_only")
        assert marker not in response.text, response.text


# ---- The database file and the server's log (a server of the test's own) ----


@pytest.mark.own_server
def test_nothing_reaches_the_database_file_or_the_servers_log(tmp_path):
    # What a checkpoint, a truncation of the log and switching logging off change is not in a
    # table: on a server with a database file and its logging on, each can be seen. (An
    # earlier server answered 403 to some of these after it had done them.)
    binary = os.environ.get("CHARGEWIRE_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="this test starts a server of its own: set CHARGEWIRE_TEST_SERVER_BINARY")
    data = tmp_path / "data"
    data.mkdir()
    server = start_server(binary=binary, extra_args=["--database", str(data / "ro.duckdb")])
    try:
        chargewire = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
        if "read_only_sessions" not in capabilities(chargewire):
            chargewire.close()
            no_read_only_sessions()
        writer, watcher = chargewire.new_session(), chargewire.new_session()
        succeeded(chargewire, writer, "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'b')) v(id, v)")
        succeeded(chargewire, writer, "INSERT INTO t SELECT i, 'x' FROM range(1000) r(i)")
        succeeded(chargewire, writer, "CREATE MACRO checkpoint_it() AS TABLE SELECT * FROM checkpoint()")
        assert (data / "ro.duckdb.wal").stat().st_size > 0, "nothing is waiting for a checkpoint: the test would prove nothing"
        marker = f"ro-marker-{uuid.uuid4().hex}"
        succeeded(chargewire, writer, "CALL enable_logging()")
        succeeded(chargewire, writer, f"SELECT write_log('{marker}', level := 'error')")
        marked = f"SELECT count(*) FROM duckdb_logs() WHERE message = '{marker}'"
        assert one(chargewire, watcher, marked) == 1
        reader = chargewire.new_session(options={"read_only": True})
        statements = ["CHECKPOINT", "FORCE CHECKPOINT", "CALL checkpoint()", "SELECT * FROM checkpoint()", "SELECT * FROM force_checkpoint()",
                      "SELECT * FROM checkpoint('ro')", "SELECT * FROM query('SELECT * FROM checkpoint()')", "SELECT * FROM checkpoint_it()",
                      "EXPLAIN ANALYZE SELECT * FROM checkpoint()", "PRAGMA force_checkpoint", "SELECT * FROM truncate_duckdb_logs()",
                      "CALL truncate_duckdb_logs()", "SELECT * FROM disable_logging()", "CALL disable_logging()",
                      "SELECT count(*) FROM t, (SELECT count(*) FROM disable_logging())", "VACUUM", "ANALYZE"]
        for function in ("checkpoint()", "force_checkpoint()", "truncate_duckdb_logs()", "disable_logging()"):
            statements += [sql.format(t="t", f=function) for sql in PIVOTS_OVER_A_FUNCTION]
        statements.append("PIVOT t ON v IN (SELECT v FROM t, checkpoint()) USING count(*)")
        with Unchanged(chargewire=chargewire, observer=watcher, subject=reader, tables=["t"], directories=[data], home=server.home):
            for sql in statements:
                refused(chargewire, reader, sql)
                assert one(chargewire, watcher, marked) == 1, f"the server's log was truncated by: {sql}"
                assert logging_is_on(chargewire, watcher), f"the server's logging was switched off by: {sql}"
        # The twins: a read-write session does each, and each shows where the snapshot looks.
        before = sorted((f.name, f.stat().st_size) for f in data.iterdir())
        succeeded(chargewire, writer, "CHECKPOINT")
        assert sorted((f.name, f.stat().st_size) for f in data.iterdir()) != before
        assert answer_of(chargewire, writer, "SELECT * FROM truncate_duckdb_logs()")[0] == 200
        assert one(chargewire, watcher, marked) == 0
        assert answer_of(chargewire, writer, "SELECT * FROM disable_logging()")[0] == 200
        assert not logging_is_on(chargewire, watcher)
        chargewire.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


@pytest.mark.own_server
def test_preparing_a_statement_neither_changes_anything_nor_takes_the_server_down():
    # Preparing a statement describes it, and describing binds it. An earlier server bound a
    # prepared `SELECT * FROM range(nextval('s'))` inside its READ ONLY transaction and then
    # committed that: to DuckDB a fatal error, after which no session of the server got an
    # answer until it was restarted. (A server of the test's own: if that ever comes back, it
    # is this server that is lost, not the one the other tests use.)
    binary = os.environ.get("CHARGEWIRE_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="this test starts a server of its own: set CHARGEWIRE_TEST_SERVER_BINARY")
    server = start_server(binary=binary)
    try:
        chargewire = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
        if "read_only_sessions" not in capabilities(chargewire):
            chargewire.close()
            no_read_only_sessions()
        writer, watcher = chargewire.new_session(), chargewire.new_session()
        for sql in ("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'b')) v(id, v)", "CREATE SEQUENCE ro_seq",
                    "CREATE VIEW ro_bad_view AS SELECT * FROM range(setval('ro_seq', 5))"):
            succeeded(chargewire, writer, sql)
        reader = chargewire.new_session(options={"read_only": True})
        with Unchanged(chargewire=chargewire, observer=watcher, subject=reader, tables=["t"], home=server.home):
            for _ in range(3):
                for sql in ("SELECT * FROM range(nextval('ro_seq'))", "SELECT * FROM ro_bad_view", "SELECT * FROM t AT (VERSION => nextval('ro_seq'))",
                            "SELECT * FROM range(nextval('ro_seq')) WHERE ? = 1", "SET progress_bar_time = nextval('ro_seq')"):
                    assert_problem(response=prepare(chargewire, reader, sql), status=403, code="read_only")
                for sql in ("SHOW ro_bad_view", "SELECT * FROM pragma_table_info('ro_bad_view')"):
                    kept_out_when_prepared(chargewire, reader, sql)
                refused(chargewire, reader, "SELECT * FROM range(nextval('ro_seq'))")
                succeeded(chargewire, reader, "BEGIN")
                refused(chargewire, reader, "SELECT * FROM ro_bad_view")
                succeeded(chargewire, reader, "COMMIT")
                assert chargewire.http.post(url=f"{chargewire.session_url(reader)}/commit", headers=chargewire.headers()).status_code in (204, 422)
        # Every session still gets its answers: this one, the read-write one, a new one.
        assert rows(chargewire, reader, "SELECT count(*) AS n FROM t") == [{"n": 3}]
        succeeded(chargewire, writer, "INSERT INTO t VALUES (4, 'c')")
        fresh = chargewire.new_session()
        succeeded(chargewire, fresh, "INSERT INTO t VALUES (5, 'd')")
        assert rows(chargewire, chargewire.new_session(options={"read_only": True}), "SELECT count(*) AS n FROM t") == [{"n": 5}]
        chargewire.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


# ---- Extensions: other systems' catalogs, lakes, object storage, spatial files ----


@pytest.fixture
def load(chargewire, server, duck):
    """Loads an extension in the read-write session. The server's home directory is a
    scratch one (conftest.py), so the extension is installed there first, from DuckDB's
    extension repository: these tests need the network. (A server the suite did not start
    is only asked to load it.)"""
    def load_extension(extension: str) -> None:
        for sql in ([f"INSTALL {extension}"] if server.process is not None else []) + [f"LOAD {extension}"]:
            response = chargewire.submit(session_id=duck, sql=sql)
            details = response.text
            completed = False
            if response.status_code in (200, 201):
                doc = response.json()
                # INSTALL can outlast the submit response's wait window, especially
                # under sanitizers. An accepted, running statement is not a failure.
                if doc["state"] not in ("succeeded", "failed", "cancelled"):
                    doc = chargewire.wait_done(session_id=duck, statement_id=doc["statement_id"])
                completed = doc["state"] == "succeeded"
                details = json.dumps(doc)
            if not completed:
                pytest.fail(reason=f"`{sql}` failed, so the tests of the {extension} extension cannot run. The extension is "
                                   f"installed from DuckDB's extension repository into the server's scratch home directory "
                                   f"(is the network reachable?), or deselect these tests with -m \"not duckdb_extensions\": "
                                   f"{details}", pytrace=False)
    return load_extension


@pytest.mark.duckdb_extensions
def test_an_attached_sqlite_database_is_read_and_not_written(chargewire, duck, ro, tmp_path, load, unchanged, logging_stays_off):
    path = tmp_path / "lite.db"
    connection = sqlite3.connect(database=path)
    connection.execute("CREATE TABLE st (id INTEGER, v TEXT)")
    connection.execute("INSERT INTO st VALUES (1, 'a')")
    connection.commit()
    connection.close()
    catalog = name("ro_lite")
    pivoted = name("ro_pv")
    load("sqlite_scanner")
    succeeded(chargewire, duck, f"CREATE TABLE {pivoted} AS SELECT * FROM (VALUES ('x', 1), ('y', 2)) v(a, b)")
    succeeded(chargewire, duck, f"ATTACH '{path}' AS {catalog} (TYPE sqlite)")

    def insert(v: str) -> str:
        return f"sqlite_query('{catalog}', 'INSERT INTO st VALUES (3, ''{v}'') RETURNING v')"
    try:
        assert rows(chargewire, ro, f"SELECT * FROM {catalog}.st") == [{"id": 1, "v": "a"}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {catalog}.st s JOIN {catalog}.st t USING (id)") == [{"n": 1}]
        assert rows(chargewire, ro, f"SELECT * FROM sqlite_scan('{path}', 'st')") == [{"id": 1, "v": "a"}]
        with unchanged(subject=ro, tables=[f"{catalog}.st", pivoted]):  # the file itself too: it is in tmp_path
            for sql in (f"INSERT INTO {catalog}.st VALUES (2, 'b')", f"DELETE FROM {catalog}.st", f"CREATE TABLE {catalog}.n (i INTEGER)",
                        f"SELECT * FROM {insert('q')}", f"SELECT * FROM sqlite_query('{catalog}', 'SELECT count(*) FROM st')",
                        f"EXPLAIN ANALYZE SELECT * FROM {insert('e')}", f"SELECT (SELECT count(*) FROM {insert('s')})",
                        # SQL text handed to SQLite from the part of a PIVOT that DuckDB runs first:
                        f"PIVOT {pivoted} ON a IN (SELECT v FROM {insert('p1')}) USING sum(b)",
                        f"PIVOT {pivoted} ON a IN (SELECT 'k' FROM (SELECT count(*) FROM {insert('p2')})) USING sum(b)",
                        f"PIVOT (SELECT a, b FROM {pivoted}, (SELECT count(*) FROM {insert('p3')})) ON a USING sum(b)",
                        f"DESCRIBE PIVOT {pivoted} ON a IN (SELECT v FROM {insert('p4')}) USING sum(b)"):
                refused(chargewire, ro, sql)
            # ... and behind a coin, 40 tries: every answer the refusal or the harmless result.
            coin = (f"SELECT * FROM query(CASE WHEN random() < 0.5 THEN 'SELECT ''harmless'' AS v' "
                    f"ELSE 'SELECT v FROM sqlite_query(''{catalog}'', ''INSERT INTO st VALUES (7, ''''coin'''') RETURNING v'')' END)")
            for attempt in range(40):
                assert answer_of(chargewire, ro, coin) in ((403, "read_only"), (200, [{"v": "harmless"}])), attempt
        # The twin: the same call writes from a read-write session.
        assert rows(chargewire, duck, f"SELECT * FROM {insert('twin')}") == [{"v": "twin"}]
    finally:
        succeeded(chargewire, duck, f"DETACH {catalog}")
    connection = sqlite3.connect(database=path)
    assert connection.execute("SELECT id, v FROM st ORDER BY id ASC").fetchall() == [(1, "a"), (3, "twin")]
    assert [r[0] for r in connection.execute("SELECT name FROM sqlite_master")] == ["st"]
    connection.close()


@pytest.mark.duckdb_extensions
def test_a_view_or_macro_made_in_a_lake_after_the_sessions_first_statement_changes_nothing(chargewire, duck, tmp_path, load):
    # A lake's catalog has no version the server could watch, so an earlier server, which read
    # the definitions of views and macros in advance, did not see what a writer made in a lake
    # after a reader's first statement: the reader's SELECT of a new lake view over
    # range(nextval(...)) moved the sequence. Nothing is read in advance now.
    lake, seq = name("ro_lake"), name("ro_seq")
    load("ducklake")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {seq}")
    succeeded(chargewire, duck, f"ATTACH 'ducklake:{tmp_path}/lake.ducklake' AS {lake} (DATA_PATH '{tmp_path}/lake_data')")
    reader = chargewire.new_session(options={"read_only": True})
    try:
        succeeded(chargewire, duck, f"CREATE TABLE {lake}.lt AS SELECT 1 AS id")
        assert rows(chargewire, reader, f"SELECT count(*) AS n FROM {lake}.lt") == [{"n": 1}]  # the reader's first statements
        # Made now, by the writer. A view over the sequence of another database: DuckDB binds
        # the view to create it, and the sequence's use is a write of a second database, which
        # DuckDB refuses (so such a view cannot be made any more); if a later DuckDB lets it be
        # made, the reader is refused all the same.
        made_views = []
        for view, definition in (("lv_seq", f"SELECT * FROM range(nextval('memory.main.{seq}'))"), ("lv_sql", "SELECT * FROM query('SELECT 42 AS x')"),
                                 ("lv_good", f"SELECT id FROM {lake}.lt")):
            if chargewire.submit(session_id=duck, sql=f"CREATE VIEW {lake}.{view} AS {definition}").status_code in (200, 201):
                made_views.append(view)
        assert "lv_sql" in made_views and "lv_good" in made_views, made_views
        made_macro = chargewire.submit(session_id=duck, sql=f"CREATE MACRO {lake}.lm() AS TABLE SELECT * FROM query('SELECT 42 AS x')").status_code in (200, 201)
        before = sequence_state(chargewire, duck, seq)
        for sql in [f"SELECT * FROM {lake}.{view}" for view in made_views if view != "lv_good"] + ([f"SELECT * FROM {lake}.lm()"] if made_macro else []):
            for _ in range(2):  # the first read of it, and one after the server has seen it
                refused(chargewire, reader, sql)
                kept_out_when_prepared(chargewire, reader, sql)
            assert sequence_state(chargewire, duck, seq) == before, sql
        late = chargewire.new_session(options={"read_only": True})
        refused(chargewire, late, f"SELECT * FROM {lake}.lv_sql")
        chargewire.http.delete(url=chargewire.session_url(late), headers=chargewire.headers())
        # The twins: a lake view that only reads, made just as late, is read; and the writer reads the refused one.
        assert rows(chargewire, reader, f"SELECT id FROM {lake}.lv_good") == [{"id": 1}]
        assert rows(chargewire, duck, f"SELECT x FROM {lake}.lv_sql") == [{"x": 42}]
        assert sequence_state(chargewire, duck, seq) == before
    finally:
        chargewire.http.delete(url=chargewire.session_url(reader), headers=chargewire.headers())
        succeeded(chargewire, duck, f"DETACH {lake}")


@pytest.mark.duckdb_extensions
def test_a_ducklake_is_read_with_time_travel_and_not_changed(chargewire, duck, ro, tmp_path, load, unchanged):
    lake = name("ro_lake")
    pivoted, sequence = name("ro_pv"), name("ro_seq")
    load("ducklake")
    succeeded(chargewire, duck, f"CREATE SEQUENCE {sequence}")
    succeeded(chargewire, duck, f"CREATE TABLE {pivoted} AS SELECT * FROM (VALUES ('x', 1), ('y', 2)) v(a, b)")
    succeeded(chargewire, duck, f"ATTACH 'ducklake:{tmp_path}/lake.ducklake' AS {lake} (DATA_PATH '{tmp_path}/lake_data')")
    try:
        succeeded(chargewire, duck, f"CREATE TABLE {lake}.lt AS SELECT 1 AS id, 'a' AS v")
        first = one(chargewire, duck, f"SELECT max(snapshot_id) FROM {lake}.snapshots()")
        succeeded(chargewire, duck, f"INSERT INTO {lake}.lt VALUES (2, 'b')")
        state = lambda: (one(chargewire, duck, f"SELECT count(*) FROM {lake}.lt"),
                         one(chargewire, duck, f"SELECT count(*) FROM {lake}.snapshots()"),
                         rows(chargewire, duck, f"SELECT option_name, value FROM {lake}.options() ORDER BY option_name ASC, value ASC"),
                         sorted(f for _, _, fs in os.walk(tmp_path / "lake_data") for f in fs))
        before = state()
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {lake}.lt") == [{"n": 2}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {lake}.lt AT (VERSION => {first})") == [{"n": 1}]
        # By timestamp, with interval arithmetic: values the server lets DuckDB evaluate while it binds.
        for clause, expected in (("TIMESTAMP => now() - INTERVAL 0 SECOND", (200, [{"n": 2}])),
                                 ("TIMESTAMP => get_current_timestamp() - INTERVAL 1 MILLISECOND", (200, [{"n": 2}])),
                                 ("TIMESTAMP => now()::TIMESTAMP", (200, [{"n": 2}])), (f"VERSION => {first} + 0", (200, [{"n": 1}])),
                                 ("TIMESTAMP => now() - INTERVAL 1 DAY", (400, "sql_error"))):  # the lake's own: no snapshot that old
            for session_id in (duck, ro):
                assert answer_of(chargewire, session_id, f"SELECT count(*) AS n FROM {lake}.lt AT ({clause})") == expected, clause
        assert one(chargewire, ro, f"SELECT count(*) FROM {lake}.snapshots()") == before[1]
        assert one(chargewire, ro, f"SELECT count(*) FROM ducklake_snapshots('{lake}')") == before[1]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {lake}.lt l JOIN {lake}.lt r USING (id)") == [{"n": 2}]
        maintenance = (f"{lake}.set_option('target_file_size', '10MB')", f"ducklake_expire_snapshots('{lake}', older_than => now())",
                       f"ducklake_flush_inlined_data('{lake}')", f"ducklake_cleanup_old_files('{lake}', cleanup_all => true)")
        with unchanged(subject=ro, tables=[f"{lake}.lt", pivoted], directories=[tmp_path / "lake_data"]):
            for sql in (f"INSERT INTO {lake}.lt VALUES (3, 'c')", f"DELETE FROM {lake}.lt", f"CREATE TABLE {lake}.n (i INTEGER)",
                        f"CALL ducklake_expire_snapshots('{lake}', older_than => now())",
                        # a time-travel clause that is not a plain value (on a lake DuckDB would go on to read with it)
                        f"SELECT count(*) FROM {lake}.lt AT (VERSION => nextval('{sequence}'))",
                        f"SELECT count(*) FROM {lake}.lt AT (TIMESTAMP => now() - INTERVAL (setval('{sequence}', 3)) DAY)"):
                refused(chargewire, ro, sql)
                assert state() == before, sql
            for function in maintenance:
                for sql in (f"SELECT * FROM {function}",
                            # ... and from the part of a PIVOT that DuckDB runs first:
                            f"PIVOT {pivoted} ON a IN (SELECT 'k' FROM (SELECT count(*) FROM {function})) USING sum(b)",
                            f"PIVOT (SELECT a, b FROM {pivoted}, (SELECT count(*) FROM {function})) ON a USING sum(b)"):
                    refused(chargewire, ro, sql)
                    assert state() == before, sql
            # A function that plans to nothing when it has nothing to do runs as nothing; with
            # work to do, its plan has operators that write, and it is refused. Either way:
            chargewire.submit(session_id=ro, sql=f"SELECT * FROM ducklake_merge_adjacent_files('{lake}')")
            assert state() == before
        # The twin: the option is one a read-write session sets.
        succeeded(chargewire, duck, f"CALL {lake}.set_option('target_file_size', '10MB')")
        assert state() != before
    finally:
        succeeded(chargewire, duck, f"DETACH {lake}")


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


@pytest.mark.duckdb_extensions
def test_files_over_http_are_read(chargewire, duck, ro, table, files, load, unchanged):
    # httpfs: the same functions that read local files read object storage and web servers.
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(QuietHandler, directory=files))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        load("httpfs")
        assert one(chargewire, ro, f"SELECT count(*) FROM read_parquet('{base}/data.parquet')") == 3
        assert one(chargewire, ro, f"SELECT count(*) FROM '{base}/data.csv'") == 3
        assert one(chargewire, ro, f"SELECT count(*) FROM parquet_metadata('{base}/data.parquet')") >= 1
        with unchanged(subject=ro, tables=[table]):
            refused(chargewire, ro, f"COPY {table} TO '{base}/upload.parquet'")
    finally:
        server.shutdown()
        thread.join()


@pytest.mark.duckdb_extensions
def test_spatial_files_are_read(chargewire, duck, ro, tmp_path, load, unchanged):
    feature = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"name": "origin"}, "geometry": {"type": "Point", "coordinates": [0, 0]}}]}
    (tmp_path / "points.geojson").write_text(json.dumps(feature))
    load("spatial")
    assert rows(chargewire, ro, f"SELECT name, ST_AsText(geom) AS wkt FROM ST_Read('{tmp_path}/points.geojson')") == \
        [{"name": "origin", "wkt": "POINT (0 0)"}]
    assert one(chargewire, ro, "SELECT ST_Distance(ST_Point(0, 0), ST_Point(3, 4))") == 5.0
    points = "SELECT count(*) FROM ST_GeneratePoints({min_x: 0, min_y: 0, max_x: 1, max_y: 1}::BOX_2D, 5, 42)"
    assert one(chargewire, ro, points) == 5 == one(chargewire, duck, points)
    with unchanged(subject=ro):
        refused(chargewire, ro, f"COPY (SELECT ST_Point(0, 0) AS geom) TO '{tmp_path}/out.geojson' WITH (FORMAT gdal, DRIVER 'GeoJSON')")
    assert sorted(os.listdir(tmp_path)) == ["points.geojson"]


@pytest.mark.duckdb_extensions
def test_an_attached_postgres_database_is_read_and_not_written(chargewire, duck, ro, load, unchanged):
    # CHARGEWIRE_TEST_POSTGRES: a libpq connection string of a PostgreSQL the server can
    # reach and may write (a throwaway one); without it this test skips, saying so.
    dsn = os.environ.get("CHARGEWIRE_TEST_POSTGRES")
    if not dsn:
        pytest.skip(reason="set CHARGEWIRE_TEST_POSTGRES to a throwaway PostgreSQL's connection string")
    catalog, remote, pivoted = name("ro_pg"), name("ro_pt"), name("ro_pv")
    load("postgres_scanner")
    succeeded(chargewire, duck, f"CREATE TABLE {pivoted} AS SELECT * FROM (VALUES ('x', 1), ('y', 2)) v(a, b)")
    succeeded(chargewire, duck, f"ATTACH '{dsn}' AS {catalog} (TYPE postgres)")
    try:
        succeeded(chargewire, duck, f"CREATE TABLE {catalog}.{remote} AS SELECT 1 AS id, 'a' AS v")
        assert rows(chargewire, ro, f"SELECT * FROM {catalog}.{remote}") == [{"id": 1, "v": "a"}]
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {catalog}.{remote} WHERE id = 1") == [{"n": 1}]
        insert = f"postgres_query('{catalog}', 'INSERT INTO {remote} VALUES (4, ''q'') RETURNING id')"
        with unchanged(subject=ro, tables=[f"{catalog}.{remote}", pivoted]):
            for sql in (f"INSERT INTO {catalog}.{remote} VALUES (2, 'b')", f"DELETE FROM {catalog}.{remote}",
                        f"CALL postgres_execute('{catalog}', 'INSERT INTO {remote} VALUES (3, ''e'')')",
                        f"SELECT * FROM {insert}", f"SELECT * FROM postgres_query('{catalog}', 'SELECT count(*) FROM {remote}')",
                        f"PIVOT {pivoted} ON a IN (SELECT 'k' FROM (SELECT count(*) FROM {insert})) USING sum(b)",
                        f"PIVOT (SELECT a, b FROM {pivoted}, (SELECT count(*) FROM {insert})) ON a USING sum(b)"):
                refused(chargewire, ro, sql)
        assert one(chargewire, duck, f"SELECT count(*) FROM {catalog}.{remote}") == 1
        succeeded(chargewire, duck, f"DROP TABLE {catalog}.{remote}")
    finally:
        succeeded(chargewire, duck, f"DETACH {catalog}")
