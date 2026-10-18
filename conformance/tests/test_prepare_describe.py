"""Spec §7: preparing a query describes its result without running it.

A prepare answers with the result's Arrow schema. It must come back at once whatever
the query costs (clients prepare expensive queries: JDBC, ADBC, the JS client, the CLI),
and it must be exactly the schema the executed statement then sends, for every type
and under every session setting that changes the Arrow mapping.
"""

import base64
import concurrent.futures
import io
import threading
import time

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from warp import ARROW_STREAM, Warp


def prepare(warp, session_id: str, sql: str, timeout: float = 120) -> dict:
    response = warp.http.post(url=f"{warp.session_url(session_id)}/prepared",
                              headers=warp.headers(), json={"sql": sql}, timeout=timeout)
    assert response.status_code == 201, response.text
    return response.json()


def schema_of(encoded: str) -> pa.Schema:
    return ipc.open_stream(source=io.BytesIO(initial_bytes=base64.b64decode(encoded))).schema


def executed_schema(warp, session_id: str, prepared_id: str, parameters=None) -> pa.Schema:
    headers = warp.headers()
    headers["Accept"] = ARROW_STREAM
    response = warp.http.post(url=f"{warp.session_url(session_id)}/prepared/{prepared_id}/execute",
                              headers=headers, json={"parameters": parameters or []})
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith(ARROW_STREAM), response.text
    reader = ipc.open_stream(source=io.BytesIO(initial_bytes=response.content))
    reader.read_all()  # the whole stream arrives: no error after the schema
    return reader.schema


# ---- Preparing never runs the query ----

# Hours of work each, if anything ran them: a cross join counted, and one streamed.
EXPENSIVE = [
    "SELECT count(*) AS n FROM range(10000000000) a, range(100000) b",
    "SELECT a.range + b.range AS n FROM range(100000000) a, range(100000000) b",
    "SELECT CAST(? AS BIGINT) + count(*) AS n FROM range(10000000000) a, range(100000) b",
]
PREPARING_SESSIONS = 16
PREPARES_PER_SESSION = 250


def test_preparing_an_expensive_query_returns_at_once(warp, server):
    """Many sessions prepare expensive queries at once while others keep DuckDB's
    threads busy. A prepare that started the query and then waited for it (one that
    executed to learn the result schema, then dropped the result after DuckDB's
    threads had picked the plan up) stalled about once in 150-1000 prepares here."""
    stop = threading.Event()
    stalls: list[str] = []
    slowest = [0.0]
    lock = threading.Lock()

    def busy() -> None:
        client = Warp(base_url=server.base_url, username=server.username, password=server.password)
        session_id = client.new_session()
        try:
            while not stop.is_set():
                client.query(session_id=session_id, sql="SELECT sum(i * i) AS s FROM range(100000000) t(i)")
        finally:
            client.http.delete(url=client.session_url(session_id), headers=client.headers())
            client.close()

    def preparer(k: int) -> None:
        client = Warp(base_url=server.base_url, username=server.username, password=server.password)
        session_id = client.new_session()
        try:
            for i in range(PREPARES_PER_SESSION):
                sql = EXPENSIVE[(k + i) % len(EXPENSIVE)]
                started = time.monotonic()
                try:
                    # A prepare that runs the query never returns: 10 s is a stall, not slowness.
                    doc = prepare(warp=client, session_id=session_id, sql=sql, timeout=10)
                except httpx.TimeoutException:
                    with lock:
                        stalls.append(f"session {k}, prepare {i}: {sql} (no answer in 10 s)")
                    return
                took = time.monotonic() - started
                with lock:
                    slowest[0] = max(slowest[0], took)
                    if took >= 1.0:
                        stalls.append(f"session {k}, prepare {i}: {sql} took {took:.2f} s")
                assert doc["result_kind"] == "rows"
                assert schema_of(encoded=doc["result_schema"]).names == ["n"]
                client.http.delete(url=f"{client.session_url(session_id)}/prepared/{doc['prepared_id']}",
                                   headers=client.headers(), timeout=10)
            # The session is idle, not still working on a query a prepare started.
            assert client.query(session_id=session_id, sql="SELECT 1 AS ok").to_pylist() == [{"ok": 1}]
        finally:
            client.close()

    loaders = [threading.Thread(target=busy, daemon=True) for _ in range(2)]
    for loader in loaders:
        loader.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=PREPARING_SESSIONS) as pool:
            for future in [pool.submit(preparer, k) for k in range(PREPARING_SESSIONS)]:
                future.result()
    finally:
        stop.set()
        for loader in loaders:
            loader.join(timeout=30)
    assert stalls == [], f"{len(stalls)} of {PREPARING_SESSIONS * PREPARES_PER_SESSION} prepares stalled"
    print(f"slowest prepare: {slowest[0] * 1000:.1f} ms")


# ---- The described schema is the executed one ----

# Every DuckDB type, with values (the NULL row of test_all_types() has none to refuse).
TYPED = (
    "SELECT true AS bool, 1::TINYINT AS ti, 1::SMALLINT AS si, 1::INTEGER AS i, 1::BIGINT AS bi, "
    "1::HUGEINT AS hi, 1::UTINYINT AS uti, 1::USMALLINT AS usi, 1::UINTEGER AS ui, 1::UBIGINT AS ubi, "
    "1::UHUGEINT AS uhi, sum(x) AS summed, 1.5::FLOAT AS f, 1.5::DOUBLE AS d, "
    "1.5::DECIMAL(4, 1) AS d4, 1.5::DECIMAL(9, 2) AS d9, 1.5::DECIMAL(18, 3) AS d18, "
    "1.5::DECIMAL(38, 10) AS d38, 'x'::VARCHAR AS v, 'x'::BLOB AS b, '1010'::BIT AS bit, "
    "12345678901234567890123::BIGNUM AS bignum, DATE '2026-10-18' AS dt, TIME '01:02:03' AS t, "
    "TIMETZ '01:02:03+02' AS ttz, TIMESTAMP '2026-10-18 01:02:03' AS ts, "
    "TIMESTAMP_S '2026-10-18 01:02:03' AS ts_s, TIMESTAMP_MS '2026-10-18 01:02:03' AS ts_ms, "
    "TIMESTAMP_NS '2026-10-18 01:02:03' AS ts_ns, TIMESTAMPTZ '2026-10-18 01:02:03+00' AS tstz, "
    "INTERVAL 1 DAY AS iv, [INTERVAL 1 HOUR] AS ivs, uuid() AS u, '{\"a\": 1}'::JSON AS j, "
    "'happy'::mood AS e, 'b'::ENUM('a', 'b') AS e2, [1, 2] AS l, [[1], NULL] AS ll, "
    "['a', NULL] AS ls, [1, 2, 3]::INTEGER[3] AS arr, {'k': 1, 's': 'x'} AS st, "
    "[{'k': 1, 'u': uuid()}] AS lst, MAP {'a': 1} AS m, MAP {1: [2.5::DECIMAL(5, 1)]} AS m2, "
    "union_value(n := 1)::UNION(n INTEGER, s VARCHAR) AS un, NULL AS nothing, 1 + 1, "
    "1 AS dup, 2 AS dup "
    "FROM (VALUES (1), (2)) v(x)"
)

PARAMETERIZED = (
    "SELECT CAST(? AS DECIMAL(10, 2)) + 1 AS d, CAST(? AS UUID) AS u, CAST(CAST(? AS VARCHAR) AS INTEGER[]) AS l, "
    "CAST(? AS TIMESTAMPTZ) AS tz, CAST(? AS HUGEINT) AS h, CAST(? AS VARCHAR) || 'x' AS v"
)

# The session settings DuckDB's Arrow mapping reads (ClientContext::GetClientProperties).
SETTINGS = {
    "default": [],
    "lossless": ["lossless"],
    "string_view": ["SET SESSION produce_arrow_string_view = true", "SET SESSION arrow_output_version = '1.4'"],
    "list_view": ["SET SESSION arrow_output_list_view = true", "SET SESSION arrow_output_version = '1.4'"],
    "large_buffers": ["SET SESSION arrow_large_buffer_size = true"],
    "time_zone": ["SET SESSION TimeZone = 'Pacific/Auckland'"],
    "everything": ["lossless", "SET SESSION produce_arrow_string_view = true", "SET SESSION arrow_output_list_view = true",
                   "SET SESSION arrow_output_version = '1.5'", "SET SESSION TimeZone = 'Asia/Kolkata'"],
}


def session_with(warp, setting: str) -> str:
    steps = SETTINGS[setting]
    session_id = warp.new_session(options={"arrow_lossless_conversion": True} if "lossless" in steps else None)
    for sql in steps:
        if sql != "lossless":
            warp.execute(session_id=session_id, sql=sql)
    warp.execute(session_id=session_id, sql="CREATE TYPE IF NOT EXISTS mood AS ENUM ('sad', 'happy')")
    return session_id


def assert_described_as_executed(warp, session_id: str, sql: str, parameters=None) -> pa.Schema:
    doc = prepare(warp=warp, session_id=session_id, sql=sql)
    assert doc["result_kind"] == "rows", doc
    described = schema_of(encoded=doc["result_schema"])
    executed = executed_schema(warp=warp, session_id=session_id, prepared_id=doc["prepared_id"],
                               parameters=parameters)
    assert len(described) == len(executed)
    for want, got in zip(executed, described):
        assert got.equals(want, check_metadata=True), f"{sql}\n  executed {want!r} {want.metadata}\n" \
                                                      f"  described {got!r} {got.metadata}"
    assert described.equals(executed, check_metadata=True)
    return described


@pytest.mark.parametrize("setting", list(SETTINGS))
def test_described_schema_is_the_executed_one(warp, setting):
    session_id = session_with(warp=warp, setting=setting)
    try:
        typed = assert_described_as_executed(warp=warp, session_id=session_id, sql=TYPED)
        assert typed.names[-3:] == ["(1 + 1)", "dup", "dup"]
        # Every column of DuckDB's own type catalog (its NULL row: nothing to refuse).
        everything = assert_described_as_executed(
            warp=warp, session_id=session_id, sql="SELECT * FROM test_all_types() WHERE bool IS NULL")
        assert len(everything) > 50
        assert_described_as_executed(
            warp=warp, session_id=session_id, sql=PARAMETERIZED,
            parameters=["1.25", "00000000-0000-0000-0000-000000000001", "[1]", "2026-10-18 01:02:03+00", 7, "a"])
        assert_described_as_executed(warp=warp, session_id=session_id, sql="EXPLAIN SELECT 1")
        assert_described_as_executed(warp=warp, session_id=session_id,
                                     sql="SELECT * FROM duckdb_settings() WHERE name = 'threads'")
    finally:
        warp.http.delete(url=warp.session_url(session_id), headers=warp.headers())


def test_described_schema_follows_the_settings(warp):
    # The described schema really is the session's (else the parity above proves little).
    plain = session_with(warp=warp, setting="default")
    views = session_with(warp=warp, setting="everything")
    sql = "SELECT 'x' AS v, 1::HUGEINT AS h, [1] AS l, TIMESTAMPTZ '2026-10-18 01:02:03+00' AS tz"
    a = schema_of(encoded=prepare(warp=warp, session_id=plain, sql=sql)["result_schema"])
    b = schema_of(encoded=prepare(warp=warp, session_id=views, sql=sql)["result_schema"])
    assert a.field("v").type == pa.string() and b.field("v").type == pa.string_view()
    assert a.field("h").type == pa.decimal128(38, 0) and b.field("h").type != pa.decimal128(38, 0)
    assert a.field("l").type == pa.list_(pa.int32()) and b.field("l").type == pa.list_view(pa.int32())
    assert a.field("tz").type.tz != "Asia/Kolkata"  # the server's own time zone
    assert b.field("tz").type == pa.timestamp("us", tz="Asia/Kolkata")


# ---- Writes with RETURNING: described by binding, never executed ----

WRITE_TABLE = ("CREATE TABLE {t} (id INTEGER PRIMARY KEY, amount DECIMAL(10, 2), u UUID, tags VARCHAR[], "
               "stamp TIMESTAMPTZ, big HUGEINT, m mood)")
RETURNING_WRITES = [
    "INSERT INTO {t} VALUES (2, 2.50, uuid(), ['b'], TIMESTAMPTZ '2026-10-18 01:00:00+00', 2, 'sad') RETURNING *",
    "INSERT INTO {t} VALUES (3, ?, uuid(), [CAST(? AS VARCHAR)], now(), 3, 'happy') "
    "RETURNING id, amount * 2 AS doubled, tags, u",
    "UPDATE {t} SET amount = amount + 1 WHERE id = 1 RETURNING id, amount, big + 1 AS bigger, stamp",
    "DELETE FROM {t} WHERE id = 1 RETURNING *, 'gone' AS why",
    "MERGE INTO {t} USING (SELECT 1 AS id) s ON {t}.id = s.id "
    "WHEN MATCHED THEN UPDATE SET amount = 9 RETURNING merge_action, *",
]
RETURNING_PARAMETERS = {1: ["4.25", "c"]}


def fresh_write_table(warp, session_id: str, name: str) -> None:
    warp.execute(session_id=session_id, sql=f"DROP TABLE IF EXISTS {name}")
    warp.execute(session_id=session_id, sql=WRITE_TABLE.format(t=name))
    warp.execute(session_id=session_id, sql=f"INSERT INTO {name} VALUES "
                                            "(1, 1.25, uuid(), ['a'], TIMESTAMPTZ '2026-10-17 01:00:00+00', 1, 'happy')")


def table_rows(warp, session_id: str, name: str) -> list:
    return warp.query(session_id=session_id, sql=f"SELECT id, amount, tags FROM {name} ORDER BY id").to_pylist()


@pytest.mark.parametrize("setting", ["default", "lossless", "everything"])
def test_a_write_with_returning_is_described_without_running(warp, setting):
    session_id = session_with(warp=warp, setting=setting)
    table = f"returning_{setting}"
    try:
        fresh_write_table(warp=warp, session_id=session_id, name=table)
        before = table_rows(warp=warp, session_id=session_id, name=table)
        prepared = []
        for sql in RETURNING_WRITES:
            doc = prepare(warp=warp, session_id=session_id, sql=sql.format(t=table))
            assert doc["result_kind"] == "rows", (sql, doc)
            assert doc["result_schema"], (sql, doc)
            prepared.append(doc)
        # Preparing wrote nothing: not one row inserted, changed or deleted.
        assert table_rows(warp=warp, session_id=session_id, name=table) == before
        # Each one's described schema is what its execution sends.
        for i, (sql, doc) in enumerate(zip(RETURNING_WRITES, prepared)):
            described = schema_of(encoded=doc["result_schema"])
            executed = executed_schema(warp=warp, session_id=session_id, prepared_id=doc["prepared_id"],
                                       parameters=RETURNING_PARAMETERS.get(i))
            assert described.equals(executed, check_metadata=True), f"{sql}\n{described}\n!=\n{executed}"
        assert table_rows(warp=warp, session_id=session_id, name=table) != before  # the executions did write
    finally:
        warp.http.delete(url=warp.session_url(session_id), headers=warp.headers())


def test_a_write_without_returning_is_an_update_and_runs_nothing(warp, session_id):
    # Negative twin: no RETURNING, so no rows and no schema, whatever the text says.
    warp.execute(session_id=session_id, sql="CREATE TYPE IF NOT EXISTS mood AS ENUM ('sad', 'happy')")
    fresh_write_table(warp=warp, session_id=session_id, name="no_returning")
    before = table_rows(warp=warp, session_id=session_id, name="no_returning")
    for sql in ["INSERT INTO no_returning (id, amount) VALUES (5, 5)",
                "INSERT INTO no_returning (id, tags) VALUES (6, ['RETURNING *'])",  # a string says it
                "UPDATE no_returning SET amount = 0 -- RETURNING *",               # a comment says it
                'UPDATE no_returning SET amount = amount WHERE "id" = 1',
                "DELETE FROM no_returning WHERE id IN (SELECT id FROM no_returning)"]:
        doc = prepare(warp=warp, session_id=session_id, sql=sql)
        assert doc["result_kind"] == "update", (sql, doc)
        assert doc["result_schema"] is None, (sql, doc)
    assert table_rows(warp=warp, session_id=session_id, name="no_returning") == before


def test_returning_one_bigint_named_count_is_rows_not_a_count(warp, session_id):
    # Its bound columns look exactly like a write's changed-rows count (one BIGINT,
    # "Count"); the RETURNING clause in the statement says they are rows.
    warp.execute(session_id=session_id, sql="CREATE TABLE counted (n BIGINT)")
    doc = prepare(warp=warp, session_id=session_id, sql='INSERT INTO counted VALUES (7) RETURNING n AS "Count"')
    assert doc["result_kind"] == "rows", doc
    executed = executed_schema(warp=warp, session_id=session_id, prepared_id=doc["prepared_id"])
    assert schema_of(encoded=doc["result_schema"]).equals(executed, check_metadata=True)
    # Positive twin of the count: the same table without RETURNING.
    plain = prepare(warp=warp, session_id=session_id, sql="INSERT INTO counted VALUES (8)")
    assert plain["result_kind"] == "update" and plain["result_schema"] is None, plain


def test_preparing_a_returning_write_inside_a_transaction_writes_nothing(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TYPE IF NOT EXISTS mood AS ENUM ('sad', 'happy')")
    fresh_write_table(warp=warp, session_id=session_id, name="returning_tx")
    warp.http.patch(url=warp.session_url(session_id), headers=warp.headers(),
                    json={"options": {"autocommit": False}})
    doc = prepare(warp=warp, session_id=session_id,
                  sql="DELETE FROM returning_tx RETURNING id")
    assert doc["result_kind"] == "rows"
    # The transaction is still usable, and still sees the row the DELETE would remove.
    assert warp.query(session_id=session_id, sql="SELECT count(*) AS n FROM returning_tx").to_pylist() == [{"n": 1}]
    assert warp.http.post(url=f"{warp.session_url(session_id)}/commit",
                          headers=warp.headers()).status_code == 204
    other = warp.new_session()
    assert warp.query(session_id=other, sql="SELECT count(*) AS n FROM returning_tx").to_pylist() == [{"n": 1}]
