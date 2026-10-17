"""Spec §6: statements, the two response paths, and bind parameters."""

import base64
import io
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

from test_discovery_auth import assert_problem
from warp import ARROW_STREAM, read_table


def test_status_document_path(warp, session_id):
    response = warp.submit(session_id=session_id, sql="SELECT 42 AS answer")
    assert response.status_code == 201, response.text
    doc = response.json()
    assert response.headers["location"].endswith(doc["statement_id"])
    if doc["state"] != "succeeded":
        doc = warp.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    assert doc["state"] == "succeeded"
    result = doc["result"]
    assert result["kind"] == "rows"
    schema = ipc.open_stream(source=io.BytesIO(initial_bytes=base64.b64decode(result["schema"]))).schema
    assert schema.names == ["answer"]
    assert result["partitions"][0]["index"] == 0
    assert result["partitions"][0]["rows"] == 1


def test_streamed_path_returns_arrow_in_one_round_trip(warp, session_id):
    response = warp.submit(session_id=session_id, sql="SELECT 1 AS a, 'x' AS b", arrow=True)
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith(ARROW_STREAM)
    assert response.headers["warp-statement-id"]
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["cache-control"] == "no-store"
    assert read_table(content=response.content).to_pylist() == [{"a": 1, "b": "x"}]


def test_statement_without_rows_reports_rows_affected(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TABLE affected (v INTEGER)")
    doc = warp.execute(session_id=session_id, sql="INSERT INTO affected SELECT * FROM range(5)")
    assert doc["result"] == {"kind": "update", "rows_affected": 5}

    # Asking for Arrow doesn't change that: the answer is a JSON document.
    response = warp.submit(session_id=session_id, sql="DELETE FROM affected WHERE v < 2", arrow=True)
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["result"] == {"kind": "update", "rows_affected": 2}


def test_sql_error_before_execution_is_an_error_response(warp, session_id):
    for arrow in (False, True):
        response = warp.submit(session_id=session_id, sql="SELEC 1", arrow=arrow)
        assert_problem(response=response, status=400, code="sql_error")
        response = warp.submit(session_id=session_id, sql="SELECT * FROM no_such_table", arrow=arrow)
        assert_problem(response=response, status=400, code="sql_error")


def test_positional_parameters(warp, session_id):
    table = warp.query(session_id=session_id, sql="SELECT ? AS i, ? AS f, ? AS s, ? AS b, ? AS n",
                       parameters=[7, 2.5, "text", True, None])
    assert table.to_pylist() == [{"i": 7, "f": 2.5, "s": "text", "b": True, "n": None}]


def test_null_parameters_take_the_type_they_need(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TABLE nullable (i INTEGER, d DATE)")
    doc = warp.execute(session_id=session_id, sql="INSERT INTO nullable VALUES (?, ?)",
                       parameters=[None, None])
    assert doc["state"] == "succeeded", doc
    # Where nothing fixes the parameter's type, the client says it in SQL (spec §6.1).
    table = warp.query(session_id=session_id,
                       sql="SELECT CAST(? AS INTEGER) + 1 AS plus, i FROM nullable",
                       parameters=[None])
    assert table.to_pylist() == [{"plus": None, "i": None}]


def test_dollar_positional_parameters(warp, session_id):
    table = warp.query(session_id=session_id, sql="SELECT $2 AS second, $1 AS first",
                       parameters=["a", "b"])
    assert table.to_pylist() == [{"second": "b", "first": "a"}]


def test_named_parameters(warp, session_id):
    table = warp.query(session_id=session_id, sql="SELECT $region AS r, $limit + 1 AS l",
                       parameters={"region": "EU", "limit": 9})
    assert table.to_pylist() == [{"r": "EU", "l": 10}]


def test_parameters_are_values_never_sql(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TABLE victims (name VARCHAR)")
    hostile = "x'); DROP TABLE victims; --"
    warp.execute(session_id=session_id, sql="INSERT INTO victims VALUES (?)", parameters=[hostile])
    table = warp.query(session_id=session_id, sql="SELECT name FROM victims")
    assert table.column("name").to_pylist() == [hostile]


def test_parameters_must_be_an_array_or_object(warp, session_id):
    response = warp.submit(session_id=session_id, sql="SELECT ?", parameters="7")
    assert_problem(response=response, status=400, code="bad_request")


def test_many_batches_arrive_complete_and_in_order(warp, session_id):
    table = warp.query(session_id=session_id, sql="SELECT i FROM range(2000000) t(i)")
    assert table.num_rows == 2_000_000
    assert table.column("i").to_pylist()[:3] == [0, 1, 2]
    assert pc.sum(table.column("i")).as_py() == 2_000_000 * 1_999_999 // 2


def test_bad_request_body(warp, session_id):
    response = warp.http.post(url=f"{warp.session_url(session_id)}/statements",
                              headers=warp.headers(), content=b"not json")
    assert_problem(response=response, status=400, code="bad_request")
    response = warp.http.post(url=f"{warp.session_url(session_id)}/statements",
                              headers=warp.headers(), json={})
    assert_problem(response=response, status=400, code="bad_request")


RFC3339 = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"


def test_statement_document_timestamps_are_rfc3339(warp, session_id):
    doc = warp.wait_done(session_id=session_id,
                         statement_id=warp.submit(session_id=session_id, sql="SELECT 1").json()["statement_id"])
    stamps = {key: doc[key] for key in ("submitted_at", "started_at", "finished_at") if key in doc}
    assert "submitted_at" in stamps, doc
    if doc.get("result", {}).get("expires_at"):
        stamps["expires_at"] = doc["result"]["expires_at"]
    for key, value in stamps.items():
        assert re.fullmatch(pattern=RFC3339, string=value), f"{key} is not RFC 3339: {value!r}"
