"""Spec §8: bulk ingest."""

import io

import pyarrow as pa
import pyarrow.ipc as ipc

from test_discovery_auth import assert_problem
from warp import ARROW_STREAM


def ipc_bytes(table: pa.Table, compression: str | None = None, batch_rows: int | None = None) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink=sink, schema=table.schema,
                        options=ipc.IpcWriteOptions(compression=compression)) as writer:
        for batch in table.to_batches(max_chunksize=batch_rows):
            writer.write_batch(batch=batch)
    return sink.getvalue()


def ingest(warp, session_id: str, body: bytes, **params):
    return warp.http.post(url=f"{warp.session_url(session_id)}/ingest", params=params,
                          headers=warp.headers(**{"Content-Type": ARROW_STREAM}), content=body)


def count(warp, session_id: str, table: str) -> int:
    return warp.query(session_id=session_id, sql=f"SELECT count(*) AS n FROM {table}").column("n").to_pylist()[0]


def test_ingest_creates_then_appends(warp, session_id):
    table = pa.table({"k": pa.array([1, 2, 3], type=pa.int64()), "v": ["a", None, "c"]})
    first = ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=table), table="ing_modes", mode="create")
    assert first.status_code == 200, first.text
    assert first.json() == {"rows_ingested": 3}
    again = ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=table), table="ing_modes", mode="append")
    assert again.json() == {"rows_ingested": 3}
    assert count(warp=warp, session_id=session_id, table="ing_modes") == 6
    replaced = ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=table), table="ing_modes",
                      mode="replace")
    assert replaced.status_code == 200
    assert count(warp=warp, session_id=session_id, table="ing_modes") == 3


def test_create_fails_when_the_table_exists(warp, session_id):
    table = pa.table({"k": [1]})
    assert ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=table), table="ing_once",
                  mode="create").status_code == 200
    response = ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=table), table="ing_once",
                      mode="create")
    assert response.status_code >= 400
    assert count(warp=warp, session_id=session_id, table="ing_once") == 1


def test_large_compressed_upload_in_many_batches(warp, session_id):
    n = 1_000_003
    table = pa.table({"i": pa.array(range(n), type=pa.int64()),
                      "s": pa.array([f"s{i}" if i % 11 else None for i in range(n)])})
    response = ingest(warp=warp, session_id=session_id,
                      body=ipc_bytes(table=table, compression="zstd", batch_rows=7777),
                      table="ing_big", mode="create")
    assert response.status_code == 200, response.text
    assert response.json() == {"rows_ingested": n}
    stored = warp.query(session_id=session_id,
                        sql="SELECT count(*) AS n, count(s) AS s, sum(i) AS total FROM ing_big").to_pylist()[0]
    assert stored["n"] == n
    assert stored["s"] == n - len(range(0, n, 11))
    assert int(stored["total"]) == n * (n - 1) // 2


def test_types_are_kept(warp, session_id):
    import datetime
    import decimal
    table = pa.table({
        "d": pa.array([decimal.Decimal("123.45")], type=pa.decimal128(10, 2)),
        "t": pa.array([datetime.datetime(2026, 1, 2, 3, 4, 5, 678901)], type=pa.timestamp("us")),
        "b": pa.array([b"\x00\xff"], type=pa.binary()),
        "l": pa.array([[1, 2]], type=pa.list_(pa.int32())),
    })
    assert ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=table), table="ing_types",
                  mode="create").status_code == 200
    assert warp.query(session_id=session_id, sql="SELECT * FROM ing_types").to_pylist() == table.to_pylist()


def test_invalid_arrow_data_is_refused(warp, session_id):
    # A decimal128(4, 2) holding 123456 (six digits) is not valid Arrow.
    bad = pa.Array.from_buffers(type=pa.decimal128(4, 2), length=1,
                                buffers=[None, pa.py_buffer((123456).to_bytes(16, "little", signed=True))])
    response = ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=pa.table({"d": bad})),
                      table="ing_bad", mode="create")
    assert_problem(response=response, status=415, code="unsupported_arrow_type")


def test_sub_microsecond_values_are_refused_not_truncated(warp, session_id):
    table = pa.table({"d": pa.array([1_500_000_123], type=pa.duration("ns"))})
    response = ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=table), table="ing_ns",
                      mode="create")
    assert_problem(response=response, status=415, code="unsupported_arrow_type")


def test_retried_upload_with_the_same_key_runs_once(warp, session_id):
    import time
    key = f"upload-{time.time_ns()}"
    body = ipc_bytes(table=pa.table({"k": [1, 2]}))
    headers = warp.headers(**{"Content-Type": ARROW_STREAM, "Idempotency-Key": key})
    url = f"{warp.session_url(session_id)}/ingest"
    first = warp.http.post(url=url, params={"table": "ing_retry", "mode": "create_append"}, headers=headers,
                           content=body)
    retry = warp.http.post(url=url, params={"table": "ing_retry", "mode": "create_append"}, headers=headers,
                           content=body)
    assert first.status_code == 200 and retry.status_code == 200, retry.text
    assert retry.json() == first.json()
    assert count(warp=warp, session_id=session_id, table="ing_retry") == 2


def test_table_is_required(warp, session_id):
    response = ingest(warp=warp, session_id=session_id, body=ipc_bytes(table=pa.table({"k": [1]})))
    assert_problem(response=response, status=400, code="bad_request")
