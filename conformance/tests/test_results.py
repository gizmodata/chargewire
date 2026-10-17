"""Spec §6.3–§6.6 and §8.2: async results, retention, resumption, incomplete streams,
compression."""

import io
import time

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from test_discovery_auth import assert_problem
from warp import ARROW_STREAM, read_batches, read_table

BIG = "SELECT i, i * 2 AS j FROM range(1000000) t(i)"


def submit_async(warp, session_id, sql, **headers):
    response = warp.submit(session_id=session_id, sql=sql,
                           headers={"Prefer": "respond-async", **headers})
    assert response.status_code == 201, response.text
    return response.json()


def test_respond_async_answers_before_the_statement_finishes(warp, session_id):
    doc = submit_async(warp=warp, session_id=session_id,
                       sql="SELECT count(*) FROM range(300000000) a")
    assert doc["state"] in ("queued", "running", "succeeded")
    done = warp.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    assert done["state"] == "succeeded"


def test_retained_result_can_be_read_again_and_resumed(warp, session_id):
    doc = submit_async(warp=warp, session_id=session_id, sql=BIG)
    statement_id = doc["statement_id"]
    done = warp.wait_done(session_id=session_id, statement_id=statement_id)
    assert done["result"]["retained"] is True
    assert done["result"]["expires_at"]
    batches = done["result"]["partitions"][0]["batches"]
    assert batches >= 2, "the result must span several batches for this test"

    first = warp.result(session_id=session_id, statement_id=statement_id)
    second = warp.result(session_id=session_id, statement_id=statement_id)
    assert first.status_code == 200 and second.status_code == 200
    full = read_table(content=first.content)
    assert full.equals(read_table(content=second.content))
    assert full.num_rows == 1_000_000

    all_batches = read_batches(content=first.content)
    assert len(all_batches) == batches
    tail = warp.result(session_id=session_id, statement_id=statement_id, from_batch=1)
    assert tail.status_code == 200
    resumed = read_batches(content=tail.content)
    assert pa.Table.from_batches(batches=resumed).equals(pa.Table.from_batches(batches=all_batches[1:]))


def test_unretained_result_is_read_once(warp, session_id):
    response = warp.submit(session_id=session_id, sql="SELECT 1 AS v", arrow=True)
    assert response.status_code == 200
    statement_id = response.headers["warp-statement-id"]
    again = warp.result(session_id=session_id, statement_id=statement_id)
    assert_problem(response=again, status=409, code="result_not_resumable")


def test_unretained_async_result_cannot_resume_mid_way(warp, session_id):
    doc = submit_async(warp=warp, session_id=session_id, sql=BIG, **{"Warp-Retain-Result": "false"})
    response = warp.result(session_id=session_id, statement_id=doc["statement_id"], from_batch=1)
    assert_problem(response=response, status=409, code="result_not_resumable")


def test_long_poll_returns_when_the_state_changes(warp, session_id):
    doc = submit_async(warp=warp, session_id=session_id, sql="SELECT 1")
    status = warp.status(session_id=session_id, statement_id=doc["statement_id"], wait=10)
    assert status["state"] in ("running", "succeeded")


def test_failure_mid_stream_leaves_the_stream_incomplete(warp, session_id):
    sql = ("SELECT CASE WHEN i < 300000 THEN i ELSE error('boom') END AS v "
           "FROM range(1000000) t(i)")
    request = warp.http.build_request(
        method="POST", url=f"{warp.session_url(session_id)}/statements",
        headers=warp.headers(Accept=ARROW_STREAM), json={"sql": sql})
    response = warp.http.send(request=request, stream=True)
    assert response.status_code == 200, response.read()
    statement_id = response.headers["warp-statement-id"]
    body = bytearray()
    with pytest.raises(httpx.RemoteProtocolError):
        for chunk in response.iter_raw():
            body.extend(chunk)
    response.close()
    # Whatever arrived (the schema, perhaps some batches) is never a complete stream: it
    # lacks the end-of-stream marker (continuation token, then a zero length).
    assert not bytes(body).endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00")
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id)
    assert doc["state"] == "failed"
    assert doc["error"]["code"] == "execution_error"


def test_zstd_compressed_results(warp, session_id):
    doc = submit_async(warp=warp, session_id=session_id, sql=BIG)
    warp.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    plain = warp.result(session_id=session_id, statement_id=doc["statement_id"])
    packed = warp.result(session_id=session_id, statement_id=doc["statement_id"],
                         headers={"Warp-IPC-Compression": "zstd"})
    assert packed.headers["warp-ipc-compression"] == "zstd"
    assert plain.headers["warp-ipc-compression"] == "none"
    assert len(packed.content) < len(plain.content)
    assert read_table(content=packed.content).equals(read_table(content=plain.content))


def test_result_of_a_statement_without_rows(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TABLE no_rows (v INTEGER)")
    doc = warp.execute(session_id=session_id, sql="INSERT INTO no_rows VALUES (1)")
    response = warp.result(session_id=session_id, statement_id=doc["statement_id"])
    assert_problem(response=response, status=400, code="bad_request")


def test_long_poll_on_a_finished_statement_answers_at_once(warp, session_id):
    doc = warp.execute(session_id=session_id, sql="SELECT 1")
    assert doc["state"] == "succeeded"
    started = time.monotonic()
    status = warp.status(session_id=session_id, statement_id=doc["statement_id"], wait=5)
    assert status["state"] == "succeeded"
    assert time.monotonic() - started < 2, "a finished statement's state can't change: no wait"


def test_streamed_result_sends_its_schema_before_the_query_finishes(warp, session_id):
    # An aggregate yields its one row only at the end; the schema still comes first, so
    # the response isn't silent (and cut by proxy idle timeouts) while the query runs.
    sql = "SELECT sum(i) AS total FROM range(100000000000) t(i)"
    request = warp.http.build_request(
        method="POST", url=f"{warp.session_url(session_id)}/statements",
        headers=warp.headers(Accept=ARROW_STREAM), json={"sql": sql},
        timeout=httpx.Timeout(timeout=120, read=15))
    response = warp.http.send(request=request, stream=True)
    try:
        assert response.status_code == 200, response.read()
        statement_id = response.headers["warp-statement-id"]
        received = bytearray()
        chunks = response.iter_raw()
        while True:
            received.extend(next(chunks))
            if len(received) >= 8:
                length = int.from_bytes(received[4:8], byteorder="little")
                if len(received) >= 8 + length:
                    break
        message = ipc.read_message(source=pa.py_buffer(obj=bytes(received)))
        assert message.type == "schema"
        assert ipc.read_schema(obj=message).names == ["total"]
        status = warp.status(session_id=session_id, statement_id=statement_id)
        assert status["state"] == "running", status
    finally:
        response.close()
    # The client gave up on an unretained result while the query ran: the server notices
    # the closed connection and cancels the statement (spec §6.5).
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id, timeout=30)
    assert doc["state"] == "cancelled", doc
    assert doc["cancel_reason"] == "client"


def test_abandoned_write_runs_to_the_end(warp, session_id):
    # A client that stops reading an INSERT … RETURNING doesn't undo the INSERT.
    warp.execute(session_id=session_id, sql="CREATE TABLE abandoned_write (i BIGINT)")
    request = warp.http.build_request(
        method="POST", url=f"{warp.session_url(session_id)}/statements",
        headers=warp.headers(Accept=ARROW_STREAM),
        json={"sql": "INSERT INTO abandoned_write SELECT * FROM range(3000000) RETURNING i"})
    response = warp.http.send(request=request, stream=True)
    assert response.status_code == 200, response.read()
    statement_id = response.headers["warp-statement-id"]
    next(response.iter_raw())  # the schema, then hang up
    response.close()
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id, timeout=60)
    assert doc["state"] == "succeeded", doc
    count = warp.query(session_id=session_id, sql="SELECT count(*) AS n FROM abandoned_write")
    assert count.to_pylist() == [{"n": 3000000}]
