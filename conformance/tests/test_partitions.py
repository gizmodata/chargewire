"""Spec §6.10: partitioned results, read concurrently over separate connections."""

import concurrent.futures
import threading
import time

import httpx
import pyarrow as pa

from test_discovery_auth import assert_problem
from warp import ARROW_STREAM, read_batches

EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"
ROWS = 1_000_000
BIG = f"SELECT i, i * 2 AS j FROM range({ROWS}) t(i)"


def partition_headers(n: int, order: str | None = None) -> dict[str, str]:
    headers = {"Warp-Result-Partitions": str(n)}
    if order:
        headers["Warp-Result-Order"] = order
    return headers


def interleave(parts: list[list[pa.RecordBatch]]) -> list[pa.RecordBatch]:
    """Puts an interleaved result back in order (spec §6.10), checking its shape."""
    counts = [len(p) for p in parts]
    # Earlier partitions have as many batches as later ones, or one more.
    assert all(a >= b for a, b in zip(counts, counts[1:])), f"batch counts {counts} are not round robin"
    assert max(counts) - min(counts) <= 1, f"batch counts {counts} are not round robin"
    assert all(batch.num_rows > 0 for part in parts for batch in part), "a partition holds an empty batch"
    ordered = []
    for round_ in range(max(counts) if counts else 0):
        for part in parts:
            if round_ < len(part):
                ordered.append(part[round_])
    return ordered


def read_complete(content: bytes) -> list[pa.RecordBatch]:
    assert content.endswith(EOS), "a partition stream ended without the end-of-stream marker"
    return read_batches(content=content)


class Partitions:
    """Reads partitions of a result, each on its own connection."""

    def __init__(self, warp, session_id: str, statement_id: str):
        self.warp = warp
        self.url = f"{warp.statement_url(session_id, statement_id)}/result"
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=32)

    def get(self, index: int, from_batch: int | None = None, **headers) -> httpx.Response:
        params = {"partition": str(index)}
        if from_batch is not None:
            params["from_batch"] = str(from_batch)
        with httpx.Client(base_url=self.warp.http.base_url, timeout=120) as client:
            response = client.get(url=self.url, params=params, headers=self.warp.headers(**headers))
            response.read()
            return response

    def get_many(self, indexes) -> list[httpx.Response]:
        return list(self.pool.map(self.get, indexes))


def streamed(warp, session_id: str, sql: str, **headers):
    request = warp.http.build_request(
        method="POST", url=f"{warp.session_url(session_id)}/statements",
        headers=warp.headers(Accept=ARROW_STREAM, **headers), json={"sql": sql})
    response = warp.http.send(request=request, stream=True)
    assert response.status_code == 200, response.read()
    assert response.headers["content-type"].startswith(ARROW_STREAM)
    return response


def read_streamed_split(warp, session_id: str, sql: str, n: int, order: str | None = None):
    """Submits on the streamed path and reads every partition at once. Returns the
    partition count, the order the server chose, and each partition's batches."""
    response = streamed(warp, session_id, sql, **partition_headers(n=n, order=order))
    statement_id = response.headers["warp-statement-id"]
    k = int(response.headers.get("warp-result-partitions", "1"))
    chosen = response.headers.get("warp-result-order")
    assert 1 <= k <= n
    reader = Partitions(warp=warp, session_id=session_id, statement_id=statement_id)
    others = reader.pool.submit(reader.get_many, range(1, k))
    try:
        first = response.read()
    finally:
        response.close()
    rest = others.result(timeout=120)
    for r in rest:
        assert r.status_code == 200, r.text
    return k, chosen, [read_complete(first)] + [read_complete(r.content) for r in rest], statement_id


def test_discovery_lists_result_partitions(warp):
    assert "result_partitions" in warp.http.get(url="/warp").json()["capabilities"]


def test_a_client_that_does_not_ask_gets_one_partition(warp, session_id):
    response = streamed(warp, session_id, BIG)
    try:
        assert "warp-result-partitions" not in response.headers
        assert len(read_complete(response.read())) > 1
    finally:
        response.close()


def test_interleaved_partitions_rebuild_the_result_in_order(warp, session_id):
    k, order, parts, _ = read_streamed_split(warp, session_id, BIG + " ORDER BY i DESC", n=4)
    assert k > 1, "a million rows over several batches should be split"
    assert order == "interleaved"
    rebuilt = pa.Table.from_batches(batches=interleave(parts))
    assert rebuilt.num_rows == ROWS
    assert rebuilt.column("i").to_pylist() == list(range(ROWS - 1, -1, -1))
    assert rebuilt.column("j").to_pylist() == [2 * i for i in range(ROWS - 1, -1, -1)]


def test_any_order_partitions_hold_every_row_once(warp, session_id):
    k, order, parts, _ = read_streamed_split(warp, session_id, BIG, n=6, order="any")
    assert k > 1
    assert order == "any"
    values = sorted(v for part in parts for b in part for v in b.column("i").to_pylist())
    assert values == list(range(ROWS))


def test_each_partition_uses_the_compression_its_request_accepts(warp, session_id):
    response = streamed(warp, session_id, BIG + " ORDER BY i",
                        **partition_headers(n=4), **{"Warp-IPC-Compression": "lz4_frame"})
    statement_id = response.headers["warp-statement-id"]
    k = int(response.headers.get("warp-result-partitions", "1"))
    assert k > 1
    assert response.headers["warp-ipc-compression"] == "lz4_frame"
    reader = Partitions(warp=warp, session_id=session_id, statement_id=statement_id)
    # Partition 1 asks for zstd, the others for nothing: each gets what it asked for.
    accepts = {1: "zstd"}
    others = [reader.pool.submit(reader.get, i, None, **({"Warp-IPC-Compression": accepts[i]} if i in accepts else {}))
              for i in range(1, k)]
    try:
        first = response.read()
    finally:
        response.close()
    rest = [f.result(timeout=120) for f in others]
    for i, r in enumerate(rest, start=1):
        assert r.status_code == 200, r.text
        assert r.headers["warp-ipc-compression"] == accepts.get(i, "none")
    parts = [read_complete(first)] + [read_complete(r.content) for r in rest]
    rebuilt = pa.Table.from_batches(batches=interleave(parts))
    assert rebuilt.column("i").to_pylist() == list(range(ROWS))


def test_every_type_and_null_survive_the_split(warp, session_id):
    sql = ("SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END AS n, "
           "CASE WHEN i % 5 = 0 THEN NULL ELSE 'row ' || i END AS s, i / 3.0 AS d, "
           "(i % 1000)::DECIMAL(18, 3) AS dec, DATE '2026-01-01' + (i % 400)::INTEGER AS dt, "
           "TIMESTAMP '2026-01-01' + to_seconds(i) AS ts, i % 2 = 0 AS b, "
           "[i, NULL, i + 1] AS l, {'a': i, 'b': CASE WHEN i % 3 = 0 THEN NULL ELSE 'x' END} AS st, "
           "encode('b' || i) AS bin FROM range(300000) t(i) ORDER BY i")
    whole = warp.query(session_id=session_id, sql=sql)
    k, _, parts, _ = read_streamed_split(warp, session_id, sql, n=5)
    assert k > 1
    rebuilt = pa.Table.from_batches(batches=interleave(parts), schema=whole.schema)
    assert rebuilt.equals(whole)


def test_a_small_result_stays_in_one_partition(warp, session_id):
    response = streamed(warp, session_id, "SELECT 42 AS answer", **partition_headers(n=8))
    try:
        assert "warp-result-partitions" not in response.headers
        assert pa.Table.from_batches(batches=read_complete(response.read())).to_pylist() == [{"answer": 42}]
    finally:
        response.close()


def test_retained_partitions_are_reread_and_resumed(warp, session_id):
    response = warp.submit(session_id=session_id, sql=BIG,
                           headers={"Prefer": "respond-async", **partition_headers(n=3)})
    assert response.status_code == 201, response.text
    statement_id = response.json()["statement_id"]
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id)
    result = doc["result"]
    k = len(result["partitions"])
    assert k > 1 and result["partition_order"] == "interleaved" and result["retained"] is True
    assert sum(p["rows"] for p in result["partitions"]) == ROWS
    reader = Partitions(warp=warp, session_id=session_id, statement_id=statement_id)
    first = [read_complete(r.content) for r in reader.get_many(range(k))]
    again = [read_complete(r.content) for r in reader.get_many(range(k))]
    for p, part in enumerate(first):
        assert len(part) == result["partitions"][p]["batches"]
        assert pa.Table.from_batches(batches=part).equals(pa.Table.from_batches(batches=again[p]))
    rebuilt = pa.Table.from_batches(batches=interleave(first))
    assert rebuilt.column("i").to_pylist() == list(range(ROWS))
    # Resuming partition 1 from its second batch gives that partition's tail.
    tail = reader.get(1, from_batch=1)
    assert tail.status_code == 200
    assert pa.Table.from_batches(batches=read_complete(tail.content)).equals(
        pa.Table.from_batches(batches=first[1][1:], schema=first[1][0].schema))
    assert_problem(response=reader.get(k), status=404, code="not_found")


def test_unretained_partition_is_read_once(warp, session_id):
    response = warp.submit(session_id=session_id, sql=BIG,
                           headers={"Prefer": "respond-async", "Warp-Retain-Result": "false",
                                    **partition_headers(n=2)})
    statement_id = response.json()["statement_id"]
    reader = Partitions(warp=warp, session_id=session_id, statement_id=statement_id)
    # Wait for the split, then read both partitions at once, then once more.
    deadline = time.monotonic() + 30
    while True:
        doc = warp.status(session_id=session_id, statement_id=statement_id, wait=1)
        if doc["result"] and (doc["result"].get("partition_order") or doc["state"] == "succeeded"):
            break
        assert time.monotonic() < deadline
    k = len(doc["result"]["partitions"])
    assert k == 2
    parts = [read_complete(r.content) for r in reader.get_many(range(k))]
    assert sum(b.num_rows for p in parts for b in p) == ROWS
    assert_problem(response=reader.get(0), status=409, code="result_not_resumable")
    assert_problem(response=reader.get(1, from_batch=1), status=409, code="result_not_resumable")


def test_empty_partitions_are_complete_streams(warp, session_id):
    # The server's batch size, from an unsplit result.
    probe = warp.query(session_id=session_id, sql=BIG)
    batch_rows = probe.to_batches()[0].num_rows
    # Two batches' worth of rows, split up to eight ways: some partitions get no batch.
    sql = f"SELECT i FROM range({2 * batch_rows}) t(i)"
    k, _, parts, _ = read_streamed_split(warp, session_id, sql, n=8)
    rows = [v for b in interleave(parts) for v in b.column("i").to_pylist()]
    assert rows == list(range(2 * batch_rows))
    batches = sum(len(part) for part in parts)
    assert k > batches, f"{k} partitions for {batches} batches: no empty partition to check"
    assert all(len(part) == 0 for part in parts[batches:]), [len(p) for p in parts]


def read_until_cut(response: httpx.Response) -> bytes:
    body = bytearray()
    try:
        for chunk in response.iter_raw():
            body.extend(chunk)
    except httpx.HTTPError:
        pass
    finally:
        response.close()
    return bytes(body)


def open_partition(warp, session_id, statement_id, index):
    client = httpx.Client(base_url=warp.http.base_url, timeout=120)
    request = client.build_request(
        method="GET", url=f"{warp.statement_url(session_id, statement_id)}/result",
        params={"partition": str(index)}, headers=warp.headers())
    response = client.send(request=request, stream=True)
    assert response.status_code == 200, response.read()
    return client, response


FAIL = ("SELECT CASE WHEN i < 30000000 THEN i ELSE error('boom') END AS v "
        "FROM range(100000000) t(i)")


def slow_query():
    # Far more rows than a test reads: the statement is still running when it acts.
    return "SELECT i, i % 7 AS m FROM range(3000000000) t(i) WHERE i % 3 = 0"


def all_open(warp, session_id, sql, n):
    response = streamed(warp, session_id, sql, **partition_headers(n=n))
    statement_id = response.headers["warp-statement-id"]
    k = int(response.headers.get("warp-result-partitions", "1"))
    assert k > 1
    opened = [open_partition(warp, session_id, statement_id, i) for i in range(1, k)]
    return statement_id, response, opened


def test_cancel_ends_every_partition_incomplete(warp, session_id):
    statement_id, first, opened = all_open(warp, session_id, slow_query(), n=3)
    bodies = []
    readers = [threading.Thread(target=lambda r=r: bodies.append(read_until_cut(r)))
               for r in [first] + [resp for _, resp in opened]]
    for t in readers:
        t.start()
    time.sleep(1)
    cancelled = warp.http.post(url=f"{warp.statement_url(session_id, statement_id)}/cancel",
                               headers=warp.headers())
    assert cancelled.status_code == 202
    for t in readers:
        t.join(timeout=60)
        assert not t.is_alive()
    for client, _ in opened:
        client.close()
    assert len(bodies) == len(opened) + 1
    assert not any(body.endswith(EOS) for body in bodies), "a partition ended cleanly after a cancel"
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id)
    assert doc["state"] == "cancelled"


def test_one_reader_leaving_cancels_the_whole_result(warp, session_id):
    statement_id, first, opened = all_open(warp, session_id, slow_query(), n=3)
    bodies = []
    readers = [threading.Thread(target=lambda r=r: bodies.append(read_until_cut(r)))
               for r in [first] + [resp for _, resp in opened[1:]]]
    for t in readers:
        t.start()
    # The reader of partition 1 hangs up after its first bytes.
    client, response = opened[0]
    next(response.iter_raw())
    response.close()
    client.close()
    for t in readers:
        t.join(timeout=60)
        assert not t.is_alive(), "a partition kept streaming after another's reader left"
    for c, _ in opened[1:]:
        c.close()
    assert not any(body.endswith(EOS) for body in bodies)
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id, timeout=30)
    assert doc["state"] == "cancelled" and doc["cancel_reason"] == "client", doc


def test_a_failure_mid_stream_fails_every_partition(warp, session_id):
    response = streamed(warp, session_id, FAIL, **partition_headers(n=4))
    statement_id = response.headers["warp-statement-id"]
    k = int(response.headers.get("warp-result-partitions", "1"))
    assert k > 1
    reader = Partitions(warp=warp, session_id=session_id, statement_id=statement_id)

    def read_partition(index):
        client = httpx.Client(base_url=warp.http.base_url, timeout=120)
        try:
            request = client.build_request(
                method="GET", url=f"{warp.statement_url(session_id, statement_id)}/result",
                params={"partition": str(index)}, headers=warp.headers())
            opened = client.send(request=request, stream=True)
            if opened.status_code != 200:
                # Asked for after the failure: the statement's error, never rows.
                opened.read()
                assert opened.status_code >= 400
                return b""
            return read_until_cut(opened)
        finally:
            client.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        others = pool.map(read_partition, range(1, k))
        bodies = [read_until_cut(response)] + list(others)
    assert not any(body.endswith(EOS) for body in bodies), "a partition ended cleanly after a failure"
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id)
    assert doc["state"] == "failed" and doc["error"]["code"] == "execution_error"
    assert reader.get(0).status_code >= 400


def test_bad_partition_headers_are_refused(warp, session_id):
    for headers in ({"Warp-Result-Partitions": "0"}, {"Warp-Result-Partitions": "999"},
                    {"Warp-Result-Partitions": "two"}, {"Warp-Result-Order": "sorted"}):
        response = warp.submit(session_id=session_id, sql="SELECT 1", arrow=True, headers=headers)
        assert_problem(response=response, status=400, code="bad_request")
