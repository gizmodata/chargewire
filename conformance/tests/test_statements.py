"""Spec §6: statements, the two response paths, and bind parameters."""

import base64
import concurrent.futures
import io
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc

from test_discovery_auth import assert_problem
from chargewire import ARROW_STREAM, read_table


def test_status_document_path(chargewire, session_id):
    response = chargewire.submit(session_id=session_id, sql="SELECT 42 AS answer")
    assert response.status_code == 201, response.text
    doc = response.json()
    assert response.headers["location"].endswith(doc["statement_id"])
    if doc["state"] != "succeeded":
        doc = chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    assert doc["state"] == "succeeded"
    result = doc["result"]
    assert result["kind"] == "rows"
    schema = ipc.open_stream(source=io.BytesIO(initial_bytes=base64.b64decode(result["schema"]))).schema
    assert schema.names == ["answer"]
    assert result["partitions"][0]["index"] == 0
    assert result["partitions"][0]["rows"] == 1
    # §6.2: no partition on another server for a client that didn't send Chargewire-Result-Locations.
    assert all(p["location"] is None for p in result["partitions"]), result


def test_streamed_path_returns_arrow_in_one_round_trip(chargewire, session_id):
    response = chargewire.submit(session_id=session_id, sql="SELECT 1 AS a, 'x' AS b", arrow=True)
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith(ARROW_STREAM)
    assert response.headers["chargewire-statement-id"]
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["cache-control"] == "no-store"
    assert read_table(content=response.content).to_pylist() == [{"a": 1, "b": "x"}]


def test_statement_without_rows_reports_rows_affected(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE affected (v INTEGER)")
    doc = chargewire.execute(session_id=session_id, sql="INSERT INTO affected SELECT * FROM range(5)")
    assert doc["result"] == {"kind": "update", "rows_affected": 5}

    # Asking for Arrow doesn't change that: the answer is a JSON document, the finished
    # statement's (§6.1).
    response = chargewire.submit(session_id=session_id, sql="DELETE FROM affected WHERE v < 2", arrow=True)
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["state"] == "succeeded", response.text
    assert response.json()["result"] == {"kind": "update", "rows_affected": 2}


def update_only_with_succeeded(doc: dict, where: str) -> None:
    """§6.2: a statement document carries an `update` result only once it says `succeeded`."""
    if (doc.get("result") or {}).get("kind") == "update":
        assert doc["state"] == "succeeded", (where, doc)


def test_an_update_result_comes_only_with_succeeded(chargewire, session_id):
    """A statement that returns no rows gets its `update` result and its `succeeded` state in
    one step (§6.2), so no answer says `running` with an update result: not a streamed
    submit's (§6.1) or a streamed prepared execution's (§7.2), not a status request's (§6.3),
    not an idempotent replay's (§6.7). A server that set the result first and the state a moment
    later answered {"state": "running", "result": {"kind": "update", "rows_affected": 1}} to a
    streamed `CREATE TABLE ... AS` now and then (Chargewire's C++ SDK before 2.4.2: about 1 answer
    in 700 with four cores, nearly every one with one), and a client that reads `state` then
    polls a statement that is over. Many rounds over each kind of statement and each path, so
    the window shows if it is there."""
    chargewire.execute(session_id=session_id, sql="CREATE TABLE one_step (v INTEGER)")
    prepared = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared", headers=chargewire.headers(),
                                    json={"sql": "INSERT INTO one_step VALUES (?)"})
    assert prepared.status_code == 201, prepared.text
    execute_url = f"{chargewire.session_url(session_id)}/prepared/{prepared.json()['prepared_id']}/execute"
    one_row, no_count = {"kind": "update", "rows_affected": 1}, {"kind": "update"}
    for round_ in range(100):
        statements = [
            (f"CREATE TABLE one_step_{round_} AS SELECT 1 AS id, 'hello' AS body", one_row),
            ("INSERT INTO one_step VALUES (1)", one_row),
            ("SET enable_progress_bar = false", no_count),
            ("BEGIN", no_count),
            ("COMMIT", no_count),
        ]
        for sql, result in statements:
            # Streamed submit: the answer is the finished statement, and so is every later read.
            key = f"one-step-{round_}-{sql}"
            response = chargewire.submit(session_id=session_id, sql=sql, arrow=True, headers={"Idempotency-Key": key})
            assert response.status_code == 200, response.text
            assert response.headers["content-type"].startswith("application/json"), response.text
            doc = response.json()
            assert (doc["state"], doc["result"]) == ("succeeded", result), (round_, sql, doc)
            again = chargewire.status(session_id=session_id, statement_id=doc["statement_id"])
            assert (again["state"], again["result"]) == ("succeeded", result), (round_, sql, again)
            replay = chargewire.submit(session_id=session_id, sql=sql, arrow=True, headers={"Idempotency-Key": key})
            assert replay.status_code == 200, replay.text
            assert replay.json()["statement_id"] == doc["statement_id"], (round_, sql)
            assert (replay.json()["state"], replay.json()["result"]) == ("succeeded", result), (round_, sql)

        # Streamed prepared execution (§7.2): answered as a streamed submit is.
        response = chargewire.http.post(url=execute_url, headers=chargewire.headers(Accept=ARROW_STREAM),
                                        json={"parameters": [2]})
        assert response.status_code == 200, response.text
        assert (response.json()["state"], response.json()["result"]) == ("succeeded", one_row), (round_, response.text)

        # Accepted at once (respond-async), then read until it ends: every document on the way.
        response = chargewire.submit(session_id=session_id, sql="INSERT INTO one_step VALUES (3)",
                                     headers={"Prefer": "respond-async"})
        assert response.status_code == 201, response.text
        doc = response.json()
        update_only_with_succeeded(doc=doc, where=("respond-async", round_))
        while doc["state"] in ("queued", "running"):
            doc = chargewire.status(session_id=session_id, statement_id=doc["statement_id"])
            update_only_with_succeeded(doc=doc, where=("status", round_))
        assert (doc["state"], doc["result"]) == ("succeeded", one_row), (round_, doc)
    rows = chargewire.query(session_id=session_id, sql="SELECT v, count(*) AS n FROM one_step GROUP BY v ORDER BY v")
    assert rows.to_pylist() == [{"v": 1, "n": 100}, {"v": 2, "n": 100}, {"v": 3, "n": 100}]


def test_statement_that_expands_into_several_returns_rows(chargewire, session_id):
    # DuckDB's PIVOT without an IN list first collects the pivot values, then queries:
    # the result's shape is known only once the earlier statements ran.
    chargewire.execute(session_id=session_id, sql="CREATE TABLE sales (city VARCHAR, year INTEGER, amount INTEGER)")
    chargewire.execute(session_id=session_id,
                 sql="INSERT INTO sales VALUES ('Paris', 2025, 10), ('Rome', 2025, 5), ('Paris', 2026, 7)")
    sql = "PIVOT sales ON city USING sum(amount) GROUP BY year ORDER BY year"
    response = chargewire.submit(session_id=session_id, sql=sql, arrow=True)
    assert response.status_code == 200, response.text
    assert read_table(content=response.content).to_pylist() == [
        {"year": 2025, "Paris": 10, "Rome": 5},
        {"year": 2026, "Paris": 7, "Rome": None},
    ]
    doc = chargewire.execute(session_id=session_id, sql=sql)
    assert doc["result"]["kind"] == "rows"
    assert doc["result"]["partitions"][0]["rows"] == 2


def test_sql_error_before_execution_is_an_error_response(chargewire, session_id):
    for arrow in (False, True):
        response = chargewire.submit(session_id=session_id, sql="SELEC 1", arrow=arrow)
        assert_problem(response=response, status=400, code="sql_error")
        response = chargewire.submit(session_id=session_id, sql="SELECT * FROM no_such_table", arrow=arrow)
        assert_problem(response=response, status=400, code="sql_error")


def test_positional_parameters(chargewire, session_id):
    table = chargewire.query(session_id=session_id, sql="SELECT ? AS i, ? AS f, ? AS s, ? AS b, ? AS n",
                       parameters=[7, 2.5, "text", True, None])
    assert table.to_pylist() == [{"i": 7, "f": 2.5, "s": "text", "b": True, "n": None}]


def test_null_parameters_take_the_type_they_need(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE nullable (i INTEGER, d DATE)")
    doc = chargewire.execute(session_id=session_id, sql="INSERT INTO nullable VALUES (?, ?)",
                       parameters=[None, None])
    assert doc["state"] == "succeeded", doc
    # Where nothing fixes the parameter's type, the client says it in SQL (spec §6.1).
    table = chargewire.query(session_id=session_id,
                       sql="SELECT CAST(? AS INTEGER) + 1 AS plus, i FROM nullable",
                       parameters=[None])
    assert table.to_pylist() == [{"plus": None, "i": None}]


def test_dollar_positional_parameters(chargewire, session_id):
    table = chargewire.query(session_id=session_id, sql="SELECT $2 AS second, $1 AS first",
                       parameters=["a", "b"])
    assert table.to_pylist() == [{"second": "b", "first": "a"}]


def test_named_parameters(chargewire, session_id):
    table = chargewire.query(session_id=session_id, sql="SELECT $region AS r, $limit + 1 AS l",
                       parameters={"region": "EU", "limit": 9})
    assert table.to_pylist() == [{"r": "EU", "l": 10}]


def test_parameters_are_values_never_sql(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE victims (name VARCHAR)")
    hostile = "x'); DROP TABLE victims; --"
    chargewire.execute(session_id=session_id, sql="INSERT INTO victims VALUES (?)", parameters=[hostile])
    table = chargewire.query(session_id=session_id, sql="SELECT name FROM victims")
    assert table.column("name").to_pylist() == [hostile]


def test_integer_parameters_keep_their_exact_value(chargewire, session_id):
    # Beyond 64 bits a JSON parser reads an integer as a double; a server binds it exactly
    # or refuses it, never rounded (spec §6.1). 10^38 - 1 as a double is 1e38.
    for value in [2**63 - 1, 2**64 - 1, 2**64, 10**38 - 1, 2**127, 2**128 - 1, -(2**63) - 1, -(2**127)]:
        response = chargewire.submit(session_id=session_id, sql="SELECT CAST(? AS VARCHAR) AS t",
                               parameters=[value], arrow=True)
        if response.status_code == 400:
            assert_problem(response=response, status=400, code="bad_request")
            continue
        assert response.status_code == 200, response.text
        assert read_table(content=response.content).column("t").to_pylist() == [str(value)], value


def test_parameters_must_be_an_array_or_object(chargewire, session_id):
    response = chargewire.submit(session_id=session_id, sql="SELECT ?", parameters="7")
    assert_problem(response=response, status=400, code="bad_request")


def test_many_batches_arrive_complete_and_in_order(chargewire, session_id):
    table = chargewire.query(session_id=session_id, sql="SELECT i FROM range(2000000) t(i)")
    assert table.num_rows == 2_000_000
    assert table.column("i").to_pylist()[:3] == [0, 1, 2]
    assert pc.sum(table.column("i")).as_py() == 2_000_000 * 1_999_999 // 2


def test_bad_request_body(chargewire, session_id):
    response = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/statements",
                              headers=chargewire.headers(), content=b"not json")
    assert_problem(response=response, status=400, code="bad_request")
    response = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/statements",
                              headers=chargewire.headers(), json={})
    assert_problem(response=response, status=400, code="bad_request")


RFC3339 = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"


def test_statement_document_timestamps_are_rfc3339(chargewire, session_id):
    doc = chargewire.wait_done(session_id=session_id,
                         statement_id=chargewire.submit(session_id=session_id, sql="SELECT 1").json()["statement_id"])
    stamps = {key: doc[key] for key in ("submitted_at", "started_at", "finished_at") if key in doc}
    assert "submitted_at" in stamps, doc
    if doc.get("result", {}).get("expires_at"):
        stamps["expires_at"] = doc["result"]["expires_at"]
    for key, value in stamps.items():
        assert re.fullmatch(pattern=RFC3339, string=value), f"{key} is not RFC 3339: {value!r}"


SLOW = "SELECT sum(i) FROM range(100000000000) t(i)"  # runs until stopped


def test_a_finished_statement_says_when_it_finished(chargewire, session_id):
    # §6.2: finished_at in every terminal state, after started_at.
    doc = chargewire.execute(session_id=session_id, sql="SELECT 1")
    assert doc["state"] == "succeeded", doc
    assert doc["submitted_at"] <= doc["started_at"] <= doc["finished_at"], doc
    cancelled = chargewire.submit(session_id=session_id, sql=SLOW, headers={"Prefer": "respond-async"}).json()
    chargewire.http.post(url=f"{chargewire.statement_url(session_id, cancelled['statement_id'])}/cancel", headers=chargewire.headers())
    doc = chargewire.wait_done(session_id=session_id, statement_id=cancelled["statement_id"])
    assert doc["state"] == "cancelled" and doc["finished_at"], doc
    assert doc["result"] is None, doc


def test_a_running_query_already_has_its_rows_result(chargewire, session_id):
    # §6.2: once the schema is known, a running statement's result is a rows result whose
    # partitions say batches and rows are not known yet.
    statement_id = chargewire.submit(session_id=session_id, sql=SLOW, headers={"Prefer": "respond-async"}).json()[
        "statement_id"]
    try:
        deadline = time.monotonic() + 30
        while True:
            doc = chargewire.status(session_id=session_id, statement_id=statement_id, wait=1)
            if doc["result"] is not None:
                break
            assert doc["state"] in ("queued", "running"), doc
            assert time.monotonic() < deadline, doc
        assert doc["state"] == "running", doc
        assert "finished_at" not in doc, doc
        result = doc["result"]
        assert result["kind"] == "rows" and result["schema"], doc
        assert isinstance(result["retained"], bool), doc
        assert result["partitions"] and all(p["batches"] is None and p["rows"] is None for p in result["partitions"]), doc
    finally:
        chargewire.http.post(url=f"{chargewire.statement_url(session_id, statement_id)}/cancel", headers=chargewire.headers())


def test_a_statement_cancelled_before_its_answer_is_an_error_response(chargewire, session_id):
    # §6.1: the submit's answer for a statement cancelled by then is its error (409), with
    # the statement named and its cancel_reason, never a success status with a document.
    response = chargewire.submit(session_id=session_id, sql=SLOW,
                           headers={"Prefer": "wait=30", "Chargewire-Statement-Timeout": "300"})
    problem = assert_problem(response=response, status=409, code="statement_timeout")
    assert problem["cancel_reason"] == "timeout", problem
    statement_id = response.headers["chargewire-statement-id"]
    doc = chargewire.status(session_id=session_id, statement_id=statement_id)
    assert doc["state"] == "cancelled" and doc["error"]["code"] == "statement_timeout", doc

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        waiting = pool.submit(chargewire.submit, session_id=session_id, sql=SLOW, headers={"Prefer": "wait=30"})
        time.sleep(1)
        cancel = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/cancel", headers=chargewire.headers())
        assert cancel.status_code == 202 and len(cancel.json()["cancelled"]) == 1, cancel.text
        response = waiting.result(timeout=60)
    problem = assert_problem(response=response, status=409, code="statement_cancelled")
    assert problem["cancel_reason"] == "client", problem
    assert response.headers["chargewire-statement-id"] == cancel.json()["cancelled"][0]
