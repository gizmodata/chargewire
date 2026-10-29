"""Spec §8: bulk ingest."""

import io

import pyarrow as pa
import pyarrow.ipc as ipc

from test_discovery_auth import assert_problem
from chargewire import ARROW_STREAM


def ipc_bytes(table: pa.Table, compression: str | None = None, batch_rows: int | None = None) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink=sink, schema=table.schema,
                        options=ipc.IpcWriteOptions(compression=compression)) as writer:
        for batch in table.to_batches(max_chunksize=batch_rows):
            writer.write_batch(batch=batch)
    return sink.getvalue()


def ingest(chargewire, session_id: str, body: bytes, **params):
    return chargewire.http.post(url=f"{chargewire.session_url(session_id)}/ingest", params=params,
                          headers=chargewire.headers(**{"Content-Type": ARROW_STREAM}), content=body)


def count(chargewire, session_id: str, table: str) -> int:
    return chargewire.query(session_id=session_id, sql=f"SELECT count(*) AS n FROM {table}").column("n").to_pylist()[0]


def test_ingest_creates_then_appends(chargewire, session_id):
    table = pa.table({"k": pa.array([1, 2, 3], type=pa.int64()), "v": ["a", None, "c"]})
    first = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table), table="ing_modes", mode="create")
    assert first.status_code == 200, first.text
    assert first.json() == {"rows_ingested": 3}
    again = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table), table="ing_modes", mode="append")
    assert again.json() == {"rows_ingested": 3}
    assert count(chargewire=chargewire, session_id=session_id, table="ing_modes") == 6
    replaced = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table), table="ing_modes",
                      mode="replace")
    assert replaced.status_code == 200
    assert count(chargewire=chargewire, session_id=session_id, table="ing_modes") == 3


def test_create_fails_when_the_table_exists(chargewire, session_id):
    table = pa.table({"k": [1]})
    assert ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table), table="ing_once",
                  mode="create").status_code == 200
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table), table="ing_once",
                      mode="create")
    problem = assert_problem(response=response, status=400, code="sql_error")
    assert problem.get("sqlstate") == "42P07", problem  # duplicate_table
    assert count(chargewire=chargewire, session_id=session_id, table="ing_once") == 1


def test_append_fails_when_the_table_is_missing(chargewire, session_id):
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=pa.table({"k": [1]})),
                      table="ing_never_created", mode="append")
    problem = assert_problem(response=response, status=400, code="sql_error")
    assert problem.get("sqlstate") == "42P01", problem  # undefined_table


def test_large_compressed_upload_in_many_batches(chargewire, session_id):
    n = 1_000_003
    table = pa.table({"i": pa.array(range(n), type=pa.int64()),
                      "s": pa.array([f"s{i}" if i % 11 else None for i in range(n)])})
    response = ingest(chargewire=chargewire, session_id=session_id,
                      body=ipc_bytes(table=table, compression="zstd", batch_rows=7777),
                      table="ing_big", mode="create")
    assert response.status_code == 200, response.text
    assert response.json() == {"rows_ingested": n}
    stored = chargewire.query(session_id=session_id,
                        sql="SELECT count(*) AS n, count(s) AS s, sum(i) AS total FROM ing_big").to_pylist()[0]
    assert stored["n"] == n
    assert stored["s"] == n - len(range(0, n, 11))
    assert int(stored["total"]) == n * (n - 1) // 2


def test_lz4_frame_compressed_upload(chargewire, session_id):
    import random
    rng = random.Random(7)
    n = 500_001
    table = pa.table({"i": pa.array(range(n), type=pa.int64()),
                      "s": pa.array([f"s{i}" if i % 13 else None for i in range(n)]),
                      "d": pa.array([rng.random() for _ in range(n)], type=pa.float64())})
    response = ingest(chargewire=chargewire, session_id=session_id,
                      body=ipc_bytes(table=table, compression="lz4", batch_rows=65_536),
                      table="ing_lz4", mode="create")
    assert response.status_code == 200, response.text
    assert response.json() == {"rows_ingested": n}
    stored = chargewire.query(session_id=session_id, sql="SELECT * FROM ing_lz4 ORDER BY i")
    assert stored.equals(table)


def test_types_are_kept(chargewire, session_id):
    import datetime
    import decimal
    table = pa.table({
        "d": pa.array([decimal.Decimal("123.45")], type=pa.decimal128(10, 2)),
        "t": pa.array([datetime.datetime(2026, 1, 2, 3, 4, 5, 678901)], type=pa.timestamp("us")),
        "b": pa.array([b"\x00\xff"], type=pa.binary()),
        "l": pa.array([[1, 2]], type=pa.list_(pa.int32())),
    })
    assert ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table), table="ing_types",
                  mode="create").status_code == 200
    assert chargewire.query(session_id=session_id, sql="SELECT * FROM ing_types").to_pylist() == table.to_pylist()


def test_invalid_arrow_data_is_refused(chargewire, session_id):
    # A decimal128(4, 2) holding 123456 (six digits) is not valid Arrow.
    bad = pa.Array.from_buffers(type=pa.decimal128(4, 2), length=1,
                                buffers=[None, pa.py_buffer((123456).to_bytes(16, "little", signed=True))])
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=pa.table({"d": bad})),
                      table="ing_bad", mode="create")
    assert_problem(response=response, status=415, code="unsupported_arrow_type")


def test_sub_microsecond_values_are_refused_not_truncated(chargewire, session_id):
    table = pa.table({"d": pa.array([1_500_000_123], type=pa.duration("ns"))})
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table), table="ing_ns",
                      mode="create")
    assert_problem(response=response, status=415, code="unsupported_arrow_type")


def unsupported_columns() -> dict[str, pa.Array]:
    """Arrow types with no faithful column type in the reference engine (DuckDB): wider
    decimals than 38 digits in 128 bits, and dense unions; at the top level and nested."""
    import decimal
    wide = pa.array([decimal.Decimal("1.25")], type=pa.decimal256(10, 2))  # small values, still a 256-bit type
    dense = pa.UnionArray.from_dense(types=pa.array([0], type=pa.int8()), value_offsets=pa.array([0], type=pa.int32()),
                                     children=[pa.array([1], type=pa.int64())], field_names=["i"])
    return {
        "decimal256": wide,
        "decimal256_76": pa.array([decimal.Decimal("1" * 76)], type=pa.decimal256(76, 0)),
        "dense_union": dense,
        "list_of_decimal256": pa.ListArray.from_arrays(offsets=pa.array([0, 1], type=pa.int32()), values=wide),
        "struct_of_dense_union": pa.StructArray.from_arrays(arrays=[dense], names=["u"]),
        "dictionary_of_decimal256": pa.DictionaryArray.from_arrays(indices=pa.array([0], type=pa.int32()),
                                                                   dictionary=wide),
    }


def test_types_the_engine_cannot_keep_are_refused_with_415(chargewire, session_id):
    # Before, the reference server answered these with 400 sql_error from inside the engine.
    for name, column in unsupported_columns().items():
        table = f"ing_unsupported_{name}"
        body = ipc_bytes(table=pa.table({"k": pa.array([1], type=pa.int64()), "c": column}))
        response = ingest(chargewire=chargewire, session_id=session_id, body=body, table=table, mode="create")
        assert_problem(response=response, status=415, code="unsupported_arrow_type")
        exists = chargewire.query(session_id=session_id,
                            sql=f"SELECT count(*) AS n FROM duckdb_tables() WHERE table_name = '{table}'")
        assert exists.column("n").to_pylist() == [0], name


def test_a_refused_type_appends_nothing(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE ing_unsupported_append (k BIGINT, c DECIMAL(10, 2))")
    body = ipc_bytes(table=pa.table({"k": pa.array([1], type=pa.int64()), "c": unsupported_columns()["decimal256"]}))
    response = ingest(chargewire=chargewire, session_id=session_id, body=body, table="ing_unsupported_append", mode="append")
    assert_problem(response=response, status=415, code="unsupported_arrow_type")
    assert count(chargewire=chargewire, session_id=session_id, table="ing_unsupported_append") == 0


def test_retried_upload_with_the_same_key_runs_once(chargewire, session_id):
    import time
    key = f"upload-{time.time_ns()}"
    body = ipc_bytes(table=pa.table({"k": [1, 2]}))
    headers = chargewire.headers(**{"Content-Type": ARROW_STREAM, "Idempotency-Key": key})
    url = f"{chargewire.session_url(session_id)}/ingest"
    first = chargewire.http.post(url=url, params={"table": "ing_retry", "mode": "create_append"}, headers=headers,
                           content=body)
    retry = chargewire.http.post(url=url, params={"table": "ing_retry", "mode": "create_append"}, headers=headers,
                           content=body)
    assert first.status_code == 200 and retry.status_code == 200, retry.text
    assert retry.json() == first.json()
    assert count(chargewire=chargewire, session_id=session_id, table="ing_retry") == 2


def test_table_is_required(chargewire, session_id):
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=pa.table({"k": [1]})))
    assert_problem(response=response, status=400, code="bad_request")


def raw_upload(server_url: str, token: str, session_id: str, body_chunks: list[bytes], table: str,
               chunked: bool = True, then_get: bool = False) -> list[bytes]:
    """Sends an ingest over a raw socket (chunked framing written by hand), optionally followed
    by a second request on the same connection; returns the raw responses."""
    import socket
    import urllib.parse

    url = urllib.parse.urlparse(server_url)
    sock = socket.create_connection((url.hostname, url.port))
    head = (f"POST /chargewire/v1/sessions/{session_id}/ingest?table={table}&mode=replace HTTP/1.1\r\n"
            f"Host: x\r\nAuthorization: Bearer {token}\r\nContent-Type: {ARROW_STREAM}\r\n")
    if chunked:
        head += "Transfer-Encoding: chunked\r\n\r\n"
        payload = b"".join(body_chunks)
    else:
        payload = b"".join(body_chunks)
        head += f"Content-Length: {len(payload)}\r\n\r\n"
    sock.sendall(head.encode() + payload)
    if then_get:
        sock.sendall(f"GET /chargewire HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".encode())
    else:
        sock.shutdown(socket.SHUT_WR)
    data = b""
    sock.settimeout(60)
    while chunk := sock.recv(65536):
        data += chunk
    sock.close()
    return [part for part in data.split(b"HTTP/1.1 ")[1:]]


def chunk(data: bytes, extension: str = "") -> bytes:
    return f"{len(data):x}{extension}\r\n".encode() + data + b"\r\n"


def test_chunked_upload_in_many_sizes_then_another_request(chargewire, session_id):
    # Chunks of every size from one byte to a megabyte, a chunk extension and a trailer:
    # the body is the same stream however it is cut, and the connection stays usable.
    table = pa.table({"k": pa.array(range(300000), type=pa.int64()), "s": [f"v{i}" for i in range(300000)]})
    body = ipc_bytes(table=table, batch_rows=50000)
    pieces, offset, size = [], 0, 1
    while offset < len(body):
        pieces.append(chunk(body[offset:offset + size], extension=";x=y" if size == 7 else ""))
        offset += size
        size = size * 7 % 1048573 + 1
    pieces.append(b"0\r\nX-Trailer: ignored\r\n\r\n")
    responses = raw_upload(server_url=str(chargewire.http.base_url), token=chargewire.token, session_id=session_id,
                           body_chunks=pieces, table="ing_chunked", then_get=True)
    assert len(responses) == 2, responses
    assert responses[0].startswith(b"200"), responses[0][:300]
    assert b'"rows_ingested":300000' in responses[0]
    assert responses[1].startswith(b"200"), "the connection carried no second request after the upload"
    assert count(chargewire=chargewire, session_id=session_id, table="ing_chunked") == 300000


def test_content_length_upload_then_another_request(chargewire, session_id):
    table = pa.table({"k": pa.array(range(100000), type=pa.int64())})
    responses = raw_upload(server_url=str(chargewire.http.base_url), token=chargewire.token, session_id=session_id,
                           body_chunks=[ipc_bytes(table=table)], table="ing_length", chunked=False,
                           then_get=True)
    assert len(responses) == 2 and responses[0].startswith(b"200") and responses[1].startswith(b"200")
    assert count(chargewire=chargewire, session_id=session_id, table="ing_length") == 100000


def test_malformed_chunked_upload_is_refused_and_writes_nothing(chargewire, session_id):
    body = ipc_bytes(table=pa.table({"k": pa.array(range(1000), type=pa.int64())}))
    for bad in (b"zz\r\n", b"10\n0123456789abcdef\r\n", b"1" * 5000 + b"\r\n",
                chunk(body[:100])[:-2] + b"XX"):
        responses = raw_upload(server_url=str(chargewire.http.base_url), token=chargewire.token,
                               session_id=session_id, body_chunks=[chunk(body[:100]), bad],
                               table="ing_malformed")
        assert responses and not responses[0].startswith(b"200"), responses[:1]
        exists = chargewire.query(session_id=session_id,
                            sql="SELECT count(*) AS n FROM duckdb_tables() WHERE table_name = 'ing_malformed'")
        assert exists.column("n").to_pylist() == [0]


def test_upload_cut_short_writes_nothing(chargewire, session_id):
    body = ipc_bytes(table=pa.table({"k": pa.array(range(500000), type=pa.int64())}), batch_rows=10000)
    # The connection ends in the middle of a chunk.
    responses = raw_upload(server_url=str(chargewire.http.base_url), token=chargewire.token, session_id=session_id,
                           body_chunks=[chunk(body[:len(body) // 2]), b"10000\r\n" + body[:100]],
                           table="ing_cut")
    assert not responses or not responses[0].startswith(b"200")
    exists = chargewire.query(session_id=session_id,
                        sql="SELECT count(*) AS n FROM duckdb_tables() WHERE table_name = 'ing_cut'")
    assert exists.column("n").to_pylist() == [0]


def test_a_large_upload_keeps_its_row_order(chargewire, session_id):
    # Many batches, which a server may insert in parallel: the table still reads back in
    # upload order, and every value survives.
    n = 2_000_000
    table = pa.table({"k": pa.array(range(n), type=pa.int64()),
                      "s": pa.array([None if i % 11 == 0 else f"row {i}" for i in range(n)])})
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=table, batch_rows=7919),
                      table="ing_order", mode="create")
    assert response.status_code == 200, response.text
    assert response.json() == {"rows_ingested": n}
    back = chargewire.query(session_id=session_id, sql="SELECT k, s FROM ing_order")
    assert back.column("k").to_pylist() == list(range(n))
    assert back.column("s").equals(table.column("s"))


def test_a_bad_batch_in_the_middle_writes_nothing(chargewire, session_id):
    # Batch 40 of 80 holds a value the server must refuse: none of the 80 remain, also
    # when the batches before it were already being inserted.
    batches = []
    for b in range(80):
        nanos = [b * 1000 + i * 1000 for i in range(5000)]
        if b == 40:
            nanos[123] += 1  # sub-microsecond: DuckDB would truncate it
        batches.append(pa.record_batch([pa.array(range(b * 5000, (b + 1) * 5000), type=pa.int64()),
                                        pa.array(nanos, type=pa.duration("ns"))], names=["k", "d"]))
    body = ipc_bytes(table=pa.Table.from_batches(batches=batches))
    response = ingest(chargewire=chargewire, session_id=session_id, body=body, table="ing_bad_middle", mode="create")
    assert_problem(response=response, status=415, code="unsupported_arrow_type")
    exists = chargewire.query(session_id=session_id,
                        sql="SELECT count(*) AS n FROM duckdb_tables() WHERE table_name = 'ing_bad_middle'")
    assert exists.column("n").to_pylist() == [0]
    chargewire.execute(session_id=session_id, sql="CREATE TABLE ing_bad_append (k BIGINT, d INTERVAL)")
    response = ingest(chargewire=chargewire, session_id=session_id, body=body, table="ing_bad_append", mode="append")
    assert_problem(response=response, status=415, code="unsupported_arrow_type")
    assert count(chargewire=chargewire, session_id=session_id, table="ing_bad_append") == 0


# §8.1: an upload into an existing table binds its columns by name, never by position.


def test_append_binds_columns_by_name(chargewire, session_id):
    chargewire.execute(session_id=session_id,
                 sql="CREATE TABLE ing_by_name (a INTEGER, b VARCHAR, c INTEGER DEFAULT 99)")
    upload = pa.table({"c": pa.array([7, 8], type=pa.int32()), "b": ["x", "y"],
                       "a": pa.array([1, 2], type=pa.int32())})
    for mode in ("append", "create_append"):
        response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=upload), table="ing_by_name",
                          mode=mode)
        assert response.status_code == 200, (mode, response.text)
    rows = chargewire.query(session_id=session_id, sql="SELECT a, b, c FROM ing_by_name ORDER BY a").to_pylist()
    assert rows == [{"a": 1, "b": "x", "c": 7}, {"a": 1, "b": "x", "c": 7},
                    {"a": 2, "b": "y", "c": 8}, {"a": 2, "b": "y", "c": 8}]


def test_append_of_same_typed_columns_in_another_order_lands_by_name(chargewire, session_id):
    # The case positional binding gets wrong without any error: both columns are
    # INTEGER, so a server that bound by position would store a's values under b.
    chargewire.execute(session_id=session_id, sql="CREATE TABLE ing_same_type (a INTEGER, b INTEGER)")
    upload = pa.table({"b": pa.array([10, 20], type=pa.int32()), "a": pa.array([1, 2], type=pa.int32())})
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=upload), table="ing_same_type",
                      mode="append")
    assert response.status_code == 200, response.text
    rows = chargewire.query(session_id=session_id, sql="SELECT a, b FROM ing_same_type ORDER BY a").to_pylist()
    assert rows == [{"a": 1, "b": 10}, {"a": 2, "b": 20}]


def test_append_omitting_columns_takes_their_defaults(chargewire, session_id):
    chargewire.execute(session_id=session_id,
                 sql="CREATE TABLE ing_defaults (a INTEGER, b VARCHAR, c INTEGER DEFAULT 99, d DATE)")
    upload = pa.table({"b": ["only b"], "a": pa.array([1], type=pa.int32())})
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=upload), table="ing_defaults",
                      mode="append")
    assert response.status_code == 200, response.text
    rows = chargewire.query(session_id=session_id, sql="SELECT a, b, c, d FROM ing_defaults").to_pylist()
    assert rows == [{"a": 1, "b": "only b", "c": 99, "d": None}]


def test_append_refuses_a_column_the_table_lacks(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE ing_extra (a INTEGER, b INTEGER)")
    upload = pa.table({"a": pa.array([1], type=pa.int32()), "b": pa.array([2], type=pa.int32()),
                       "nope": pa.array([3], type=pa.int32())})
    response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=upload), table="ing_extra",
                      mode="append")
    problem = assert_problem(response=response, status=400, code="sql_error")
    assert "nope" in problem["detail"], problem
    assert count(chargewire=chargewire, session_id=session_id, table="ing_extra") == 0


def test_create_keeps_the_upload_column_order(chargewire, session_id):
    upload = pa.table({"b": pa.array([10], type=pa.int32()), "a": pa.array([1], type=pa.int32())})
    for mode, table in (("create", "ing_created_order"), ("replace", "ing_replaced_order"),
                        ("create_append", "ing_create_appended_order")):
        response = ingest(chargewire=chargewire, session_id=session_id, body=ipc_bytes(table=upload), table=table, mode=mode)
        assert response.status_code == 200, (mode, response.text)
        assert chargewire.query(session_id=session_id, sql=f"SELECT * FROM {table}").column_names == ["b", "a"], mode
