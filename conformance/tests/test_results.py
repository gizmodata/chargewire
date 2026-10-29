"""Spec §6.3–§6.6 and §8.2: async results, retention, resumption, incomplete streams,
compression."""

import io
import time

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from test_discovery_auth import assert_problem
from chargewire import ARROW_STREAM, read_batches, read_table

BIG = "SELECT i, i * 2 AS j FROM range(1000000) t(i)"


def submit_async(chargewire, session_id, sql, **headers):
    response = chargewire.submit(session_id=session_id, sql=sql,
                           headers={"Prefer": "respond-async", **headers})
    assert response.status_code == 201, response.text
    return response.json()


def test_respond_async_answers_before_the_statement_finishes(chargewire, session_id):
    doc = submit_async(chargewire=chargewire, session_id=session_id,
                       sql="SELECT count(*) FROM range(300000000) a")
    assert doc["state"] in ("queued", "running", "succeeded")
    done = chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    assert done["state"] == "succeeded"


def test_retained_result_can_be_read_again_and_resumed(chargewire, session_id):
    doc = submit_async(chargewire=chargewire, session_id=session_id, sql=BIG)
    statement_id = doc["statement_id"]
    done = chargewire.wait_done(session_id=session_id, statement_id=statement_id)
    assert done["result"]["retained"] is True
    assert done["result"]["expires_at"]
    batches = done["result"]["partitions"][0]["batches"]
    assert batches >= 2, "the result must span several batches for this test"

    first = chargewire.result(session_id=session_id, statement_id=statement_id)
    second = chargewire.result(session_id=session_id, statement_id=statement_id)
    assert first.status_code == 200 and second.status_code == 200
    full = read_table(content=first.content)
    assert full.equals(read_table(content=second.content))
    assert full.num_rows == 1_000_000

    all_batches = read_batches(content=first.content)
    assert len(all_batches) == batches
    tail = chargewire.result(session_id=session_id, statement_id=statement_id, from_batch=1)
    assert tail.status_code == 200
    resumed = read_batches(content=tail.content)
    assert pa.Table.from_batches(batches=resumed).equals(pa.Table.from_batches(batches=all_batches[1:]))


def test_unretained_result_is_read_once(chargewire, session_id):
    response = chargewire.submit(session_id=session_id, sql="SELECT 1 AS v", arrow=True)
    assert response.status_code == 200
    statement_id = response.headers["chargewire-statement-id"]
    again = chargewire.result(session_id=session_id, statement_id=statement_id)
    assert_problem(response=again, status=409, code="result_not_resumable")


def test_unretained_async_result_cannot_resume_mid_way(chargewire, session_id):
    doc = submit_async(chargewire=chargewire, session_id=session_id, sql=BIG, **{"Chargewire-Retain-Result": "false"})
    response = chargewire.result(session_id=session_id, statement_id=doc["statement_id"], from_batch=1)
    assert_problem(response=response, status=409, code="result_not_resumable")


def test_long_poll_returns_when_the_state_changes(chargewire, session_id):
    doc = submit_async(chargewire=chargewire, session_id=session_id, sql="SELECT 1")
    status = chargewire.status(session_id=session_id, statement_id=doc["statement_id"], wait=10)
    assert status["state"] in ("running", "succeeded")


FAILING_SQL = ("SELECT CASE WHEN i < 300000 THEN i ELSE error('boom') END AS v "
               "FROM range(1000000) t(i)")
END_OF_STREAM = b"\xff\xff\xff\xff\x00\x00\x00\x00"


def assert_failed_statement_answer(chargewire, session_id, headers: dict[str, str], url: str | None = None,
                                   body: dict | None = None) -> str:
    """A statement that fails while it runs answers in one of the two ways the spec allows:
    an error response (the failure came before the answer started, §6.1), or a 200 Arrow
    stream that ends without the end-of-stream marker (it came during the stream, §6.6),
    after which the statement document says why. Never a 200 statement document: that is
    what a statement without rows answers, so a client would take the failure for an
    empty success. Returns which of the two it was."""
    request = chargewire.http.build_request(
        method="POST", url=url or f"{chargewire.session_url(session_id)}/statements",
        headers=chargewire.headers(Accept=ARROW_STREAM, **headers),
        json={"sql": FAILING_SQL} if body is None else body)
    response = chargewire.http.send(request=request, stream=True)
    body = bytearray()
    try:
        if response.status_code != 200:
            response.read()
            # §11: a statement's own failure is 422, never a 5xx; §6.1: the answer names it.
            problem = assert_problem(response=response, status=422, code="execution_error")
            assert "boom" in problem["detail"], problem
            assert response.headers.get("chargewire-statement-id"), response.headers
            return "error response"
        content_type = response.headers.get("content-type", "")
        if not content_type.startswith(ARROW_STREAM):
            pytest.fail(reason=f"a failed statement answered 200 with {content_type}, as a statement "
                               f"without rows does: {response.read()[:400]!r}")
        statement_id = response.headers["chargewire-statement-id"]
        try:
            for chunk in response.iter_raw():
                body.extend(chunk)
        except httpx.RemoteProtocolError:
            pass
        else:
            # Say what the server thought happened: a clean end here would hand a client a
            # truncated result as if it were whole.
            doc = chargewire.status(session_id=session_id, statement_id=statement_id)
            pytest.fail(reason=f"the stream ended cleanly after {len(body)} bytes (ends with the "
                               f"end-of-stream marker: {bytes(body).endswith(END_OF_STREAM)}); "
                               f"statement: {doc}")
    finally:
        response.close()
    # Whatever arrived (the schema, perhaps some batches) is never a complete stream: it
    # lacks the end-of-stream marker (continuation token, then a zero length).
    assert not bytes(body).endswith(END_OF_STREAM)
    doc = chargewire.wait_done(session_id=session_id, statement_id=statement_id)
    assert doc["state"] == "failed", doc
    assert doc["error"]["code"] == "execution_error", doc
    assert "boom" in doc["error"]["detail"], doc
    return "incomplete stream"


def test_failure_mid_stream_leaves_the_stream_incomplete(chargewire, session_id):
    assert_failed_statement_answer(chargewire=chargewire, session_id=session_id, headers={})


def test_a_failure_before_the_answer_is_an_error_response(chargewire, session_id):
    # Asking for partitions makes a server that splits results wait to decide the split
    # before it answers (§6.10), so the failure comes first: the answer is the error. One
    # that answers before deciding streams instead, and must leave that stream incomplete.
    assert_failed_statement_answer(chargewire=chargewire, session_id=session_id, headers={"Chargewire-Result-Partitions": "8"})


@pytest.mark.parametrize("partitions", [None, "8"])
def test_a_failed_prepared_execution_is_never_a_success(chargewire, session_id, partitions):
    # Executing a prepared statement answers as submitting one does (§7).
    response = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared",
                              headers=chargewire.headers(), json={"sql": FAILING_SQL})
    assert response.status_code == 201, response.text
    prepared_id = response.json()["prepared_id"]
    assert_failed_statement_answer(
        chargewire=chargewire, session_id=session_id,
        headers={} if partitions is None else {"Chargewire-Result-Partitions": partitions},
        url=f"{chargewire.session_url(session_id)}/prepared/{prepared_id}/execute", body={})


def test_zstd_compressed_results(chargewire, session_id):
    doc = submit_async(chargewire=chargewire, session_id=session_id, sql=BIG)
    chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    plain = chargewire.result(session_id=session_id, statement_id=doc["statement_id"])
    packed = chargewire.result(session_id=session_id, statement_id=doc["statement_id"],
                         headers={"Chargewire-IPC-Compression": "zstd"})
    assert packed.headers["chargewire-ipc-compression"] == "zstd"
    assert plain.headers["chargewire-ipc-compression"] == "none"
    assert len(packed.content) < len(plain.content)
    assert read_table(content=packed.content).equals(read_table(content=plain.content))


# The first bytes of each codec's compressed buffers (spec §8.2): an LZ4 frame and a
# zstd frame, so a test can tell which codec a server really used.
LZ4_FRAME_MAGIC = b"\x04\x22\x4d\x18"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


# Rows every codec shrinks: a server may leave buffers that barely compress as they are
# (spec §8.2), so the tests that look for a codec's frames use data that compresses well.
COMPRESSIBLE = "SELECT i, i % 7 AS small, 'repeated text ' || (i % 3) AS s FROM range(1000000) t(i)"


def compressed_result(chargewire, session_id, statement_id, accept: str):
    response = chargewire.result(session_id=session_id, statement_id=statement_id,
                           headers={"Chargewire-IPC-Compression": accept})
    assert response.status_code == 200, response.text
    return response


def test_discovery_lists_both_codecs(chargewire):
    capabilities = chargewire.http.get(url="/chargewire").json()["capabilities"]
    assert "ipc_compression_zstd" in capabilities
    assert "ipc_compression_lz4_frame" in capabilities


def test_lz4_frame_compressed_results(chargewire, session_id):
    doc = submit_async(chargewire=chargewire, session_id=session_id, sql=COMPRESSIBLE)
    chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    plain = chargewire.result(session_id=session_id, statement_id=doc["statement_id"])
    packed = compressed_result(chargewire=chargewire, session_id=session_id, statement_id=doc["statement_id"],
                               accept="lz4_frame")
    assert packed.headers["chargewire-ipc-compression"] == "lz4_frame"
    assert LZ4_FRAME_MAGIC in packed.content and ZSTD_MAGIC not in packed.content
    assert len(packed.content) < len(plain.content)
    assert read_table(content=packed.content).equals(read_table(content=plain.content))


def test_compression_follows_the_clients_preference(chargewire, session_id):
    doc = submit_async(chargewire=chargewire, session_id=session_id, sql=COMPRESSIBLE)
    chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    plain = read_table(content=chargewire.result(session_id=session_id, statement_id=doc["statement_id"]).content)
    # The first codec the server supports wins; names it doesn't know are skipped, and
    # `none` (or no codec it knows) means uncompressed.
    for accept, used in (("lz4_frame, zstd", "lz4_frame"), ("zstd,lz4_frame", "zstd"),
                         ("brotli, lz4_frame", "lz4_frame"), ("snappy , zstd", "zstd"),
                         ("none, lz4_frame", "none"), ("brotli", "none")):
        response = compressed_result(chargewire=chargewire, session_id=session_id, statement_id=doc["statement_id"],
                                     accept=accept)
        assert response.headers["chargewire-ipc-compression"] == used, accept
        assert (LZ4_FRAME_MAGIC in response.content) == (used == "lz4_frame"), accept
        assert (ZSTD_MAGIC in response.content) == (used == "zstd"), accept
        assert read_table(content=response.content).equals(plain), accept


def test_compressed_results_keep_incompressible_and_null_columns(chargewire, session_id):
    # Buffers that barely compress (hashes as doubles) may go uncompressed inside a
    # compressed stream (spec §8.2); every value and null must still come back.
    sql = ("SELECT i, hash(i)::DOUBLE AS h, CASE WHEN i % 3 = 0 THEN NULL ELSE 'v' || i END AS s, "
           "[i, NULL] AS l FROM range(300000) t(i)")
    doc = submit_async(chargewire=chargewire, session_id=session_id, sql=sql)
    chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    plain = read_table(content=chargewire.result(session_id=session_id, statement_id=doc["statement_id"]).content)
    for codec in ("lz4_frame", "zstd"):
        response = compressed_result(chargewire=chargewire, session_id=session_id, statement_id=doc["statement_id"],
                                     accept=codec)
        assert response.headers["chargewire-ipc-compression"] == codec
        assert read_table(content=response.content).equals(plain), codec


def test_result_of_a_statement_without_rows(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE no_rows (v INTEGER)")
    doc = chargewire.execute(session_id=session_id, sql="INSERT INTO no_rows VALUES (1)")
    response = chargewire.result(session_id=session_id, statement_id=doc["statement_id"])
    assert_problem(response=response, status=400, code="bad_request")


def test_long_poll_on_a_finished_statement_answers_at_once(chargewire, session_id):
    doc = chargewire.execute(session_id=session_id, sql="SELECT 1")
    assert doc["state"] == "succeeded"
    started = time.monotonic()
    status = chargewire.status(session_id=session_id, statement_id=doc["statement_id"], wait=5)
    assert status["state"] == "succeeded"
    assert time.monotonic() - started < 2, "a finished statement's state can't change: no wait"


def test_streamed_result_sends_its_schema_before_the_query_finishes(chargewire, session_id):
    # An aggregate yields its one row only at the end; the schema still comes first, so
    # the response isn't silent (and cut by proxy idle timeouts) while the query runs.
    sql = "SELECT sum(i) AS total FROM range(100000000000) t(i)"
    request = chargewire.http.build_request(
        method="POST", url=f"{chargewire.session_url(session_id)}/statements",
        headers=chargewire.headers(Accept=ARROW_STREAM), json={"sql": sql},
        timeout=httpx.Timeout(timeout=120, read=15))
    response = chargewire.http.send(request=request, stream=True)
    try:
        assert response.status_code == 200, response.read()
        statement_id = response.headers["chargewire-statement-id"]
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
        status = chargewire.status(session_id=session_id, statement_id=statement_id)
        assert status["state"] == "running", status
    finally:
        response.close()
    # The client gave up on an unretained result while the query ran: the server notices
    # the closed connection and cancels the statement (spec §6.5).
    doc = chargewire.wait_done(session_id=session_id, statement_id=statement_id, timeout=30)
    assert doc["state"] == "cancelled", doc
    assert doc["cancel_reason"] == "client"


def test_abandoned_write_runs_to_the_end(chargewire, session_id):
    # A client that stops reading an INSERT … RETURNING doesn't undo the INSERT.
    chargewire.execute(session_id=session_id, sql="CREATE TABLE abandoned_write (i BIGINT)")
    request = chargewire.http.build_request(
        method="POST", url=f"{chargewire.session_url(session_id)}/statements",
        headers=chargewire.headers(Accept=ARROW_STREAM),
        json={"sql": "INSERT INTO abandoned_write SELECT * FROM range(3000000) RETURNING i"})
    response = chargewire.http.send(request=request, stream=True)
    assert response.status_code == 200, response.read()
    statement_id = response.headers["chargewire-statement-id"]
    next(response.iter_raw())  # the schema, then hang up
    response.close()
    doc = chargewire.wait_done(session_id=session_id, statement_id=statement_id, timeout=60)
    assert doc["state"] == "succeeded", doc
    count = chargewire.query(session_id=session_id, sql="SELECT count(*) AS n FROM abandoned_write")
    assert count.to_pylist() == [{"n": 3000000}]


def test_deleting_a_running_write_lets_it_finish(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE deleted_write (i BIGINT)")
    doc = submit_async(chargewire=chargewire, session_id=session_id,
                       sql="INSERT INTO deleted_write SELECT * FROM range(3000000) RETURNING i")
    deleted = chargewire.http.delete(url=chargewire.statement_url(session_id, doc["statement_id"]), headers=chargewire.headers())
    assert deleted.status_code == 204, deleted.text
    count = chargewire.query(session_id=session_id, sql="SELECT count(*) AS n FROM deleted_write")
    assert count.to_pylist() == [{"n": 3000000}]
