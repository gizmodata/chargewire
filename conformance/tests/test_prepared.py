"""Spec §7: prepared statements, with JSON and Arrow bind parameters."""

import base64
import datetime
import decimal
import io

import pyarrow as pa
import pyarrow.ipc as ipc

from test_discovery_auth import assert_problem
from chargewire import ARROW_STREAM, read_table


def arrow_body(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink=sink, schema=table.schema) as writer:
        writer.write_table(table=table)
    return sink.getvalue()


def prepare(chargewire, session_id: str, sql: str) -> dict:
    response = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared",
                              headers=chargewire.headers(), json={"sql": sql})
    assert response.status_code == 201, response.text
    return response.json()


def execute(chargewire, session_id: str, prepared_id: str, *, json_body=None, arrow: pa.Table | None = None,
            accept_arrow: bool = False):
    headers = chargewire.headers()
    if accept_arrow:
        headers["Accept"] = ARROW_STREAM
    url = f"{chargewire.session_url(session_id)}/prepared/{prepared_id}/execute"
    if arrow is not None:
        headers["Content-Type"] = ARROW_STREAM
        return chargewire.http.post(url=url, headers=headers, content=arrow_body(table=arrow))
    return chargewire.http.post(url=url, headers=headers, json=json_body or {})


def test_prepare_describes_parameters(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id,
                  sql="SELECT CAST(? AS INTEGER) + 1 AS a, CAST(? AS VARCHAR) AS b")
    schema = ipc.open_stream(source=io.BytesIO(initial_bytes=base64.b64decode(doc["parameter_schema"]))).schema
    assert len(schema) == 2
    assert doc["prepared_id"]


def test_execute_with_json_parameters_many_times(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="SELECT ? * 2 AS doubled")
    for value in (1, 21, -4):
        response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"],
                           json_body={"parameters": [value]}, accept_arrow=True)
        assert response.status_code == 200, response.text
        assert read_table(content=response.content).column("doubled").to_pylist() == [value * 2]


def test_arrow_parameters_one_execution_per_row(chargewire, session_id):
    chargewire.execute(session_id=session_id,
                 sql="CREATE TABLE prepared_rows (id INTEGER, name VARCHAR, amount DECIMAL(10, 2), ts TIMESTAMP)")
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="INSERT INTO prepared_rows VALUES (?, ?, ?, ?)")
    rows = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "name": ["a", None, "c"],
        "amount": pa.array([decimal.Decimal("1.50"), decimal.Decimal("2.25"), None], type=pa.decimal128(10, 2)),
        "ts": pa.array([datetime.datetime(2026, 10, 17, 1, 2, 3), None, None], type=pa.timestamp("us")),
    })
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"], arrow=rows)
    assert response.status_code in (200, 201), response.text
    done = response.json()
    if done["state"] != "succeeded":
        done = chargewire.wait_done(session_id=session_id, statement_id=done["statement_id"])
    assert done["result"] == {"kind": "update", "rows_affected": 3}
    stored = chargewire.query(session_id=session_id, sql="SELECT * FROM prepared_rows ORDER BY id")
    assert stored.to_pylist() == [
        {"id": 1, "name": "a", "amount": decimal.Decimal("1.50"), "ts": datetime.datetime(2026, 10, 17, 1, 2, 3)},
        {"id": 2, "name": None, "amount": decimal.Decimal("2.25"), "ts": None},
        {"id": 3, "name": "c", "amount": None, "ts": None},
    ]


def test_arrow_parameters_concatenate_query_results(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="SELECT ? AS v, ? || '!' AS s")
    rows = pa.table({"v": pa.array([1, 2], type=pa.int64()), "s": ["x", "y"]})
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"], arrow=rows,
                       accept_arrow=True)
    assert response.status_code == 200, response.text
    assert read_table(content=response.content).to_pylist() == [{"v": 1, "s": "x!"}, {"v": 2, "s": "y!"}]


def test_named_parameters_in_prepared_statements(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="SELECT $greeting || ', ' || $name AS line")
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"],
                       json_body={"parameters": {"greeting": "hello", "name": "chargewire"}}, accept_arrow=True)
    assert response.status_code == 200, response.text
    assert read_table(content=response.content).to_pylist() == [{"line": "hello, chargewire"}]


def test_closed_prepared_statement_is_gone(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="SELECT 1")
    url = f"{chargewire.session_url(session_id)}/prepared/{doc['prepared_id']}"
    assert chargewire.http.delete(url=url, headers=chargewire.headers()).status_code == 204
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"])
    assert_problem(response=response, status=404, code="prepared_not_found")


def test_prepare_errors_are_sql_errors(chargewire, session_id):
    response = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared", headers=chargewire.headers(),
                              json={"sql": "SELEC 1"})
    assert_problem(response=response, status=400, code="sql_error")


def schema_of(encoded: str) -> pa.Schema:
    return ipc.open_stream(source=io.BytesIO(initial_bytes=base64.b64decode(encoded))).schema


def test_prepared_query_describes_its_result(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id,
                  sql="SELECT CAST(? AS INTEGER) + 1 AS a, CAST(? AS VARCHAR) AS b")
    assert doc["result_kind"] == "rows"
    schema = schema_of(encoded=doc["result_schema"])
    assert schema.names == ["a", "b"]
    assert schema.field("b").type == pa.string()


def test_preparing_runs_nothing(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE prepared_side (x INTEGER)")
    chargewire.execute(session_id=session_id, sql="INSERT INTO prepared_side VALUES (1)")
    insert = prepare(chargewire=chargewire, session_id=session_id, sql="INSERT INTO prepared_side VALUES (?)")
    assert insert["result_kind"] in ("update", None)
    assert insert["result_schema"] is None
    returning = prepare(chargewire=chargewire, session_id=session_id,
                        sql="INSERT INTO prepared_side VALUES (?) RETURNING x")
    assert returning["result_kind"] in ("rows", None)
    prepare(chargewire=chargewire, session_id=session_id, sql="DELETE FROM prepared_side")
    ddl = prepare(chargewire=chargewire, session_id=session_id, sql="CREATE TABLE prepared_never (x INTEGER)")
    assert ddl["result_kind"] in ("update", None)
    assert chargewire.query(session_id=session_id, sql="SELECT x FROM prepared_side").to_pylist() == [{"x": 1}]
    tables = chargewire.query(session_id=session_id,
                        sql="SELECT count(*) AS n FROM information_schema.tables "
                            "WHERE table_name = 'prepared_never'")
    assert tables.to_pylist() == [{"n": 0}]


def test_query_for_no_parameter_rows_returns_no_rows(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="SELECT CAST(? AS INTEGER) * 2 AS v")
    empty = pa.table({"p": pa.array([], type=pa.int32())})
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"], arrow=empty,
                       accept_arrow=True)
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith(ARROW_STREAM), response.text
    table = read_table(content=response.content)
    assert table.num_rows == 0
    assert table.schema.names == ["v"]


def test_prepared_statements_with_typed_parameters_close_cleanly(chargewire):
    # Preparing and closing statements whose parameters have rich types, in sessions
    # that then end, must leave the server healthy (a server once destroyed parameter
    # types it didn't own here; the harness fails the run if the server dies).
    sqls = ["SELECT CAST(? AS INTEGER) AS a", "SELECT ? + 1 AS b", "SELECT CAST(? AS DECIMAL(10, 2)) AS c",
            "SELECT CAST(? AS UUID) AS d", "SELECT CAST(? AS INTEGER[]) AS e"]
    for _ in range(5):
        session_id = chargewire.new_session()
        for sql in sqls:
            doc = prepare(chargewire=chargewire, session_id=session_id, sql=sql)
            closed = chargewire.http.delete(url=f"{chargewire.session_url(session_id)}/prepared/{doc['prepared_id']}",
                                      headers=chargewire.headers())
            assert closed.status_code == 204, closed.text
        prepare(chargewire=chargewire, session_id=session_id, sql="SELECT CAST(? AS DECIMAL(18, 4)) AS kept")
        chargewire.http.delete(url=chargewire.session_url(session_id), headers=chargewire.headers())
    assert chargewire.query(session_id=chargewire.new_session(), sql="SELECT 42 AS ok").to_pylist() == [{"ok": 42}]


def test_preparing_inside_a_transaction_leaves_it_usable(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE prepared_tx (id INTEGER)")
    chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                    json={"options": {"autocommit": False}})
    insert = prepare(chargewire=chargewire, session_id=session_id, sql="INSERT INTO prepared_tx VALUES (?)")
    for sql in ["UPDATE prepared_tx SET id = ?", "DELETE FROM prepared_tx WHERE id = ?",
                "INSERT INTO prepared_tx VALUES (?) RETURNING id", "SELECT CAST(? AS INTEGER) AS v"]:
        prepare(chargewire=chargewire, session_id=session_id, sql=sql)
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=insert["prepared_id"],
                       arrow=pa.table({"id": pa.array([1, 2], type=pa.int32())}))
    assert response.status_code in (200, 201), response.text
    assert chargewire.http.post(url=f"{chargewire.session_url(session_id)}/commit",
                          headers=chargewire.headers()).status_code == 204
    other = chargewire.new_session()
    assert chargewire.query(session_id=other, sql="SELECT count(*) AS n FROM prepared_tx").to_pylist() == [{"n": 2}]


def test_prepared_parameter_schema_describes_rich_types(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id,
                  sql="SELECT CAST(? AS DECIMAL(10, 2)) AS d, CAST(? AS UUID) AS u, "
                      "CAST(? AS INTEGER[]) AS l, CAST(? AS INTERVAL) AS i")
    types = [field.type for field in schema_of(encoded=doc["parameter_schema"])]
    assert types == [pa.decimal128(10, 2), pa.string(), pa.list_(pa.int32()), pa.month_day_nano_interval()]


def test_arrow_null_parameter_keeps_its_arrow_type(chargewire, session_id):
    # DuckDB can't type the parameter of `? + 1`; a typed Arrow NULL still binds.
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="SELECT ? + 1 AS b")
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"],
                       arrow=pa.table({"p": pa.array([None], type=pa.int32())}), accept_arrow=True)
    assert response.status_code == 200, response.text
    assert read_table(content=response.content).column("b").to_pylist() == [None]


def test_list_and_interval_arrow_parameters(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id,
                  sql="SELECT CAST(? AS INTEGER[]) AS l, CAST(? AS INTERVAL) AS i")
    rows = pa.table({
        "l": pa.array([[1, 2], [], None], type=pa.list_(pa.int32())),
        "i": pa.array([pa.MonthDayNano([1, 2, 3000]), None, pa.MonthDayNano([0, 0, 0])],
                      type=pa.month_day_nano_interval()),
    })
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"], arrow=rows,
                       accept_arrow=True)
    assert response.status_code == 200, response.text
    table = read_table(content=response.content)
    assert table.column("l").to_pylist() == [[1, 2], [], None]
    assert table.column("i").to_pylist() == [pa.MonthDayNano([1, 2, 3000]), None, pa.MonthDayNano([0, 0, 0])]


def test_sub_microsecond_interval_parameter_is_refused(chargewire, session_id):
    doc = prepare(chargewire=chargewire, session_id=session_id, sql="SELECT CAST(? AS INTERVAL) AS i")
    response = execute(chargewire=chargewire, session_id=session_id, prepared_id=doc["prepared_id"],
                       arrow=pa.table({"i": pa.array([pa.MonthDayNano([0, 0, 1])],
                                                     type=pa.month_day_nano_interval())}))
    assert_problem(response=response, status=415, code="unsupported_arrow_type")
