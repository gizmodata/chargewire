"""Spec §8.3: partitioned ingest, its parts uploaded at once and committed atomically."""

import concurrent.futures
import io
import socket
import time
import urllib.parse

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from test_discovery_auth import assert_problem
from chargewire import ARROW_STREAM


def ipc_bytes(table: pa.Table, batch_rows: int | None = None, compression: str | None = None) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink=sink, schema=table.schema,
                        options=ipc.IpcWriteOptions(compression=compression)) as writer:
        for batch in table.to_batches(max_chunksize=batch_rows):
            writer.write_batch(batch=batch)
    return sink.getvalue()


def rows(start: int, count: int) -> pa.Table:
    keys = list(range(start, start + count))
    return pa.table({
        "k": pa.array(keys, type=pa.int64()),
        "s": pa.array([None if k % 13 == 0 else f"row {k}" for k in keys]),
        "d": pa.array([None if k % 7 == 0 else k / 4 for k in keys], type=pa.float64()),
    })


class Ingests:
    """Partitioned-ingest calls, each part on its own connection."""

    def __init__(self, chargewire, session_id: str):
        self.chargewire = chargewire
        self.base = f"{chargewire.session_url(session_id)}/ingests"

    def open(self, **target) -> httpx.Response:
        return self.chargewire.http.post(url=self.base, headers=self.chargewire.headers(), json=target)

    def part(self, ingest_id: str, part: int, body: bytes, **headers) -> httpx.Response:
        with httpx.Client(base_url=self.chargewire.http.base_url, timeout=120) as client:
            return client.post(url=f"{self.base}/{ingest_id}/parts/{part}", content=body,
                               headers=self.chargewire.headers(**{"Content-Type": ARROW_STREAM, **headers}))

    def commit(self, ingest_id: str, parts: int) -> httpx.Response:
        return self.chargewire.http.post(url=f"{self.base}/{ingest_id}/commit", headers=self.chargewire.headers(),
                                   json={"part_count": parts})

    def get(self, ingest_id: str) -> httpx.Response:
        return self.chargewire.http.get(url=f"{self.base}/{ingest_id}", headers=self.chargewire.headers())

    def abort(self, ingest_id: str) -> httpx.Response:
        return self.chargewire.http.delete(url=f"{self.base}/{ingest_id}", headers=self.chargewire.headers())

    def upload_all(self, ingest_id: str, bodies: list[bytes]) -> list[httpx.Response]:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(bodies)) as pool:
            return list(pool.map(lambda i: self.part(ingest_id, i, bodies[i]), range(len(bodies))))


def checksum(chargewire, session_id: str, table: str) -> dict:
    return chargewire.query(session_id=session_id, sql=(
        f"SELECT count(*) AS n, sum(k) AS sk, count(s) AS ns, sum(length(s)) AS ls, "
        f"count(d) AS nd, sum(d) AS sd, count(DISTINCT k) AS dk FROM {table}")).to_pylist()[0]


def expected(tables: list[pa.Table]) -> dict:
    whole = pa.concat_tables(tables)
    ks = whole.column("k").to_pylist()
    ss = [s for s in whole.column("s").to_pylist() if s is not None]
    ds = [d for d in whole.column("d").to_pylist() if d is not None]
    return {"n": len(ks), "sk": sum(ks), "ns": len(ss), "ls": sum(len(s) for s in ss), "nd": len(ds),
            "sd": sum(ds), "dk": len(set(ks))}


def table_exists(chargewire, session_id: str, name: str) -> bool:
    # From another session: the ingest's own session runs nothing else while it is open
    # (spec §8.3), and an uncommitted table is invisible elsewhere.
    session_id = chargewire.new_session()
    found = chargewire.query(session_id=session_id,
                       sql=f"SELECT count(*) AS n FROM information_schema.tables WHERE table_name = '{name}'")
    return found.column("n").to_pylist() == [1]


def test_discovery_lists_partitioned_ingest(chargewire):
    assert "partitioned_ingest" in chargewire.http.get(url="/chargewire").json()["capabilities"]


def test_parts_uploaded_at_once_are_committed_together(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    opened = ingests.open(table="pi_all", mode="create")
    assert opened.status_code == 201, opened.text
    ingest_id = opened.json()["ingest_id"]
    assert opened.headers["location"].endswith(f"/ingests/{ingest_id}")
    parts = [rows(start=i * 250_000, count=250_000) for i in range(8)]
    answers = ingests.upload_all(ingest_id, [ipc_bytes(table=t, batch_rows=30_000) for t in parts])
    for i, answer in enumerate(answers):
        assert answer.status_code == 200, answer.text
        assert answer.json() == {"part": i, "rows_received": 250_000}
    # Nothing is visible before the commit.
    assert ingests.get(ingest_id).json()["state"] == "open"
    committed = ingests.commit(ingest_id, parts=8)
    assert committed.status_code == 200, committed.text
    assert committed.json() == {"rows_ingested": 2_000_000}
    got = checksum(chargewire, session_id, "pi_all")
    want = expected(parts)
    assert got.pop("sd") == pytest.approx(want.pop("sd"))
    assert got == want
    doc = ingests.get(ingest_id).json()
    assert doc["state"] == "committed" and doc["rows_ingested"] == 2_000_000
    assert sorted(p["part"] for p in doc["parts"]) == list(range(8))


def test_parts_may_each_use_any_offered_codec(chargewire, session_id):
    # Spec §8.2: an upload may use any codec the server lists; each part says its own.
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_codecs", mode="create").json()["ingest_id"]
    parts = [rows(start=i * 100_000, count=100_000) for i in range(3)]
    bodies = [ipc_bytes(table=t, batch_rows=20_000, compression=c) for t, c in zip(parts, ("lz4", "zstd", None))]
    answers = ingests.upload_all(ingest_id, bodies)
    assert all(a.status_code == 200 for a in answers), [a.text for a in answers]
    committed = ingests.commit(ingest_id, parts=3)
    assert committed.status_code == 200, committed.text
    got = checksum(chargewire, session_id, "pi_codecs")
    want = expected(parts)
    assert got.pop("sd") == pytest.approx(want.pop("sd"))
    assert got == want


def test_append_and_replace_modes(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    chargewire.execute(session_id=session_id, sql="CREATE TABLE pi_modes (k BIGINT, s VARCHAR, d DOUBLE)")
    for mode, total in (("append", 2000), ("append", 4000), ("replace", 2000)):
        ingest_id = ingests.open(table="pi_modes", mode=mode).json()["ingest_id"]
        answers = ingests.upload_all(ingest_id, [ipc_bytes(rows(0, 1000)), ipc_bytes(rows(1000, 1000))])
        assert all(a.status_code == 200 for a in answers), [a.text for a in answers]
        assert ingests.commit(ingest_id, parts=2).status_code == 200
        assert checksum(chargewire, session_id, "pi_modes")["n"] == total, mode


def test_commit_with_a_part_missing_keeps_the_ingest_open(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_missing", mode="create").json()["ingest_id"]
    assert ingests.part(ingest_id, 0, ipc_bytes(rows(0, 100))).status_code == 200
    assert ingests.part(ingest_id, 2, ipc_bytes(rows(200, 100))).status_code == 200
    assert_problem(response=ingests.commit(ingest_id, parts=3), status=409, code="ingest_incomplete")
    assert not table_exists(chargewire, session_id, "pi_missing")
    assert ingests.part(ingest_id, 1, ipc_bytes(rows(100, 100))).status_code == 200
    committed = ingests.commit(ingest_id, parts=3)
    assert committed.status_code == 200, committed.text
    assert committed.json() == {"rows_ingested": 300}
    assert checksum(chargewire, session_id, "pi_missing") == {**expected([rows(0, 300)]), "sd": pytest.approx(expected([rows(0, 300)])["sd"])}


def test_a_part_sent_twice(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_twice", mode="create").json()["ingest_id"]
    body = ipc_bytes(rows(0, 5000))
    first = ingests.part(ingest_id, 0, body, **{"Idempotency-Key": "pi-twice-0"})
    assert first.status_code == 200, first.text
    # A retry with the same key gets the first answer and writes nothing more.
    again = ingests.part(ingest_id, 0, body, **{"Idempotency-Key": "pi-twice-0"})
    assert again.status_code == 200 and again.json() == first.json()
    # Without the key the part number is taken.
    assert_problem(response=ingests.part(ingest_id, 0, body), status=409, code="ingest_part_exists")
    assert ingests.commit(ingest_id, parts=1).json() == {"rows_ingested": 5000}
    assert checksum(chargewire, session_id, "pi_twice")["n"] == 5000


def send_raw(chargewire, path: str, headers: dict[str, str], body: bytes) -> bytes:
    """POSTs body over a socket of its own, the whole body before reading the answer, as most
    clients do; returns the raw answer, or raises the transport error."""
    url = urllib.parse.urlparse(str(chargewire.http.base_url))
    with socket.create_connection((url.hostname, url.port)) as sock:
        head = f"POST {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {len(body)}\r\n"
        head += "".join(f"{name}: {value}\r\n" for name, value in headers.items())
        sock.sendall(head.encode() + b"\r\n" + body)
        answer = b""
        while chunk := sock.recv(65536):
            answer += chunk
            if b"\r\n\r\n" in answer and answer.endswith(b"}"):
                break
        return answer


def test_the_answer_to_a_part_sent_again_arrives(chargewire, session_id):
    # A part sent again is answered from its first upload without reading the body (§8.3), so
    # the answer comes while the client is still sending. The server must take in the rest
    # before closing: closing with unread bytes resets the connection, and the client loses
    # the answer (Linux and macOS: a broken pipe or a reset; Windows discards it).
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_resent", mode="create").json()["ingest_id"]
    body = ipc_bytes(rows(0, 400_000), batch_rows=20_000)  # several MB, more than sockets buffer
    headers = {"Content-Type": ARROW_STREAM, "Idempotency-Key": "pi-resent-0"}
    first = ingests.part(ingest_id, 0, body, **headers)
    assert first.status_code == 200, first.text
    path = f"{urllib.parse.urlparse(ingests.base).path}/{ingest_id}/parts/0"
    for _ in range(5):
        answer = send_raw(chargewire=chargewire, path=path, headers=chargewire.headers(**headers), body=body)
        assert answer.startswith(b"HTTP/1.1 200"), answer[:200]
        assert answer.endswith(first.content), answer[-200:]
    assert ingests.commit(ingest_id, parts=1).json() == {"rows_ingested": 400_000}
    assert checksum(chargewire, session_id, "pi_resent")["n"] == 400_000


def test_the_answer_to_an_upload_sent_again_arrives(chargewire, session_id):
    # The same for a one-request upload (§8.1) sent again with its key.
    body = ipc_bytes(rows(0, 400_000), batch_rows=20_000)
    headers = {"Content-Type": ARROW_STREAM, "Idempotency-Key": "upload-resent"}
    url = f"{chargewire.session_url(session_id)}/ingest"
    params = {"table": "ing_resent", "mode": "create"}
    first = chargewire.http.post(url=url, params=params, headers=chargewire.headers(**headers), content=body)
    assert first.status_code == 200, first.text
    path = f"{urllib.parse.urlparse(url).path}?{urllib.parse.urlencode(params)}"
    for _ in range(5):
        answer = send_raw(chargewire=chargewire, path=path, headers=chargewire.headers(**headers), body=body)
        assert answer.startswith(b"HTTP/1.1 200"), answer[:200]
        assert answer.endswith(first.content), answer[-200:]
    assert checksum(chargewire, session_id, "ing_resent")["n"] == 400_000


def test_a_part_with_another_schema_fails_everything(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_schema", mode="create").json()["ingest_id"]
    good = ingests.part(ingest_id, 0, ipc_bytes(rows(0, 100_000)))
    assert good.status_code == 200, good.text
    bad = ingests.part(ingest_id, 1, ipc_bytes(pa.table({"other": [1, 2, 3]})))
    problem = assert_problem(response=bad, status=400, code="bad_request")
    assert "schema" in problem["detail"], problem
    # The commit gets the ingest's error, and so does its document (§8.3).
    commit = assert_problem(response=ingests.commit(ingest_id, parts=2), status=400, code="bad_request")
    assert commit["detail"] == problem["detail"], (commit, problem)
    doc = ingests.get(ingest_id).json()
    assert doc["state"] == "failed" and doc["error"]["code"] == "bad_request", doc
    assert "rows_ingested" not in doc, doc
    assert not table_exists(chargewire, session_id, "pi_schema")
    # The session goes on.
    assert chargewire.query(session_id=session_id, sql="SELECT 1 AS one").to_pylist() == [{"one": 1}]


def test_invalid_data_in_one_part_writes_nothing(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    chargewire.execute(session_id=session_id, sql="CREATE TABLE pi_invalid (k BIGINT, s VARCHAR, d DOUBLE)")
    ingest_id = ingests.open(table="pi_invalid", mode="append").json()["ingest_id"]
    good = ipc_bytes(rows(0, 200_000), batch_rows=20_000)
    broken = bytearray(ipc_bytes(rows(0, 50_000)))
    broken[len(broken) // 2: len(broken) // 2 + 64] = b"\xff" * 64  # inside a batch's buffers
    answers = ingests.upload_all(ingest_id, [good, good, bytes(broken), good])
    # The broken part is refused as data the server can't read; the others, and the
    # commit, get that same error (§8.3).
    refused = [a for a in answers if a.status_code != 200]
    assert refused, [a.status_code for a in answers]
    codes = {a.json()["code"] for a in refused}
    assert codes <= {"unsupported_arrow_type", "bad_request"} and len(codes) == 1, [a.text for a in refused]
    code = codes.pop()
    status = 415 if code == "unsupported_arrow_type" else 400
    for answer in refused:
        assert_problem(response=answer, status=status, code=code)
    assert_problem(response=ingests.commit(ingest_id, parts=4), status=status, code=code)
    assert checksum(chargewire, session_id, "pi_invalid")["n"] == 0


def test_abort_writes_nothing(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_abort", mode="create").json()["ingest_id"]
    assert all(a.status_code == 200 for a in ingests.upload_all(ingest_id, [ipc_bytes(rows(0, 1000))] * 3))
    assert ingests.abort(ingest_id).status_code == 204
    assert_problem(response=ingests.commit(ingest_id, parts=3), status=404, code="ingest_not_found")
    assert not table_exists(chargewire, session_id, "pi_abort")
    assert chargewire.query(session_id=session_id, sql="SELECT 2 AS two").to_pylist() == [{"two": 2}]


def test_a_client_that_disconnects_in_the_middle_of_a_part(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_cut", mode="create").json()["ingest_id"]
    assert ingests.part(ingest_id, 0, ipc_bytes(rows(0, 100_000))).status_code == 200
    # Part 1: half its declared body, then the connection closes.
    body = ipc_bytes(rows(100_000, 400_000), batch_rows=20_000)
    url = urllib.parse.urlparse(str(chargewire.http.base_url))
    sock = socket.create_connection((url.hostname, url.port))
    sock.sendall((f"POST {ingests.base}/{ingest_id}/parts/1 HTTP/1.1\r\nHost: x\r\n"
                  f"Authorization: Bearer {chargewire.token}\r\nContent-Type: {ARROW_STREAM}\r\n"
                  f"Content-Length: {len(body)}\r\n\r\n").encode() + body[: len(body) // 2])
    sock.close()
    # The server notices the broken part and fails the whole ingest.
    deadline = time.monotonic() + 20
    while (doc := ingests.get(ingest_id).json())["state"] != "failed":
        assert time.monotonic() < deadline, doc
        time.sleep(0.1)
    commit = assert_problem(response=ingests.commit(ingest_id, parts=2), status=422, code="execution_error")
    assert "part 1" in commit["detail"], commit
    assert not table_exists(chargewire, session_id, "pi_cut")


def test_every_part_is_bound_to_its_session_and_principal(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    ingest_id = ingests.open(table="pi_bound", mode="create").json()["ingest_id"]
    body = ipc_bytes(rows(0, 10))
    # Without a token, or with another's, nothing is touched.
    with httpx.Client(base_url=chargewire.http.base_url, timeout=60) as client:
        no_token = client.post(url=f"{ingests.base}/{ingest_id}/parts/0", content=body,
                               headers={"Content-Type": ARROW_STREAM})
        assert_problem(response=no_token, status=401, code="invalid_token")
        forged = client.post(url=f"{ingests.base}/{ingest_id}/parts/0", content=body,
                             headers={"Content-Type": ARROW_STREAM, "Authorization": "Bearer forged"})
        assert_problem(response=forged, status=401, code="invalid_token")
    # From another session of the same principal, the ingest doesn't exist.
    other = chargewire.new_session()
    elsewhere = Ingests(chargewire=chargewire, session_id=other)
    assert_problem(response=elsewhere.part(ingest_id, 0, body), status=404, code="ingest_not_found")
    assert_problem(response=elsewhere.commit(ingest_id, parts=1), status=404, code="ingest_not_found")
    assert_problem(response=elsewhere.get(ingest_id), status=404, code="ingest_not_found")
    # The real session's ingest is untouched and still works.
    assert ingests.part(ingest_id, 0, body).status_code == 200
    assert ingests.commit(ingest_id, parts=1).json() == {"rows_ingested": 10}


def test_bad_requests_are_refused(chargewire, session_id):
    ingests = Ingests(chargewire=chargewire, session_id=session_id)
    assert_problem(response=ingests.open(mode="create"), status=400, code="bad_request")
    assert_problem(response=ingests.open(table="x", mode="sideways"), status=400, code="bad_request")
    ingest_id = ingests.open(table="pi_bad").json()["ingest_id"]
    assert_problem(response=ingests.part(ingest_id, 256, ipc_bytes(rows(0, 1))), status=400, code="bad_request")
    assert_problem(response=ingests.part("i_nope", 0, ipc_bytes(rows(0, 1))), status=404, code="ingest_not_found")
    assert_problem(response=ingests.commit(ingest_id, parts=0), status=400, code="bad_request")
    ingests.abort(ingest_id)
