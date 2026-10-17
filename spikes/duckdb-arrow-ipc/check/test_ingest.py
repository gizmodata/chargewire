"""pyarrow IPC -> nanoarrow reader -> DuckDB (v2 Arrow importer + warp_upload())
must store what DuckDB's own Arrow ingest stores, and values that read back equal
to the upload. A type DuckDB can't ingest must be refused, never stored as text."""

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from cases import ARROW_CASES
from conftest import run_tool
from test_copy import COMPRESSIONS, write_ipc


def read_back(con: duckdb.DuckDBPyConnection, table: str) -> pa.Table:
    # Lossless, so UUID and JSON come back as their Arrow extension types.
    con.execute(query="SET arrow_lossless_conversion = true")
    return con.sql(query=f"SELECT * FROM {table} ORDER BY rowid").arrow().read_all()


def column_types(con: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    return [(row[0], row[1]) for row in con.sql(query=f"DESCRIBE {table}").fetchall()]


def same_values(stored: pa.ChunkedArray, uploaded: pa.ChunkedArray) -> bool:
    """Compares in the upload's type when the stored column casts back to it,
    otherwise in the stored type."""
    if pa.types.is_run_end_encoded(uploaded.type):
        uploaded = pc.run_end_decode(uploaded)
    if pa.types.is_duration(uploaded.type) and pa.types.is_interval(stored.type):
        # DuckDB stores a duration as an INTERVAL; compare exact nanoseconds.
        nanos = uploaded.cast(target_type=pa.duration("ns")).cast(target_type=pa.int64())
        want = [None if v is None else (0, 0, v) for v in nanos.to_pylist()]
        got = [None if v is None else (v.months, v.days, v.nanoseconds) for v in stored.to_pylist()]
        return got == want
    try:
        return stored.cast(target_type=uploaded.type).equals(uploaded)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
        return uploaded.cast(target_type=stored.type).equals(stored)


@pytest.mark.parametrize("compression", COMPRESSIONS)
@pytest.mark.parametrize("case", list(ARROW_CASES))
def test_ingest_matches_duckdb(tool, tmp_path, case, compression):
    upload = pa.table({"v": ARROW_CASES[case]})
    source = tmp_path / "upload.arrows"
    db = tmp_path / "ingest.duckdb"
    write_ipc(table=upload, path=source, compression=compression)

    proc = run_tool(tool=tool, args=["ingest", "--db", str(db), "--table", "got", "--in", str(source),
                                     "--mode", "create"])

    con = duckdb.connect(database=str(db))
    try:
        con.register(view_name="upload", python_object=upload)
        con.execute(query="CREATE TABLE expected AS SELECT * FROM upload")
    except duckdb.Error as e:
        # DuckDB itself can't ingest this type: ours must refuse it too.
        assert proc.returncode != 0, f"stored a type DuckDB refuses ({e})"
        pytest.xfail(reason=f"DuckDB does not ingest {upload.schema.field(0).type}: {e}")

    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == str(upload.num_rows)
    assert column_types(con=con, table="got") == column_types(con=con, table="expected")
    got = read_back(con=con, table="got")
    assert got.equals(read_back(con=con, table="expected"))
    assert same_values(stored=got.column(0), uploaded=upload.column(0)), (
        f"\n stored: {got.column(0).to_pylist()}\nuploaded: {upload.column(0).to_pylist()}")


@pytest.mark.parametrize("mode,first,second,rows", [
    ("append", True, True, 6),
    ("replace", True, True, 3),
    ("create_append", False, True, 6),
])
def test_ingest_modes(tool, tmp_path, mode, first, second, rows):
    upload = pa.table({"i": pa.array([1, 2, 3], type=pa.int32())})
    source = tmp_path / "upload.arrows"
    db = tmp_path / "modes.duckdb"
    write_ipc(table=upload, path=source, compression="none")

    first_mode = "create" if first else mode
    for step_mode in [first_mode, mode]:
        proc = run_tool(tool=tool, args=["ingest", "--db", str(db), "--table", "t", "--in", str(source),
                                         "--mode", step_mode])
        assert proc.returncode == 0, proc.stderr
    con = duckdb.connect(database=str(db))
    assert con.execute(query="SELECT count(*) FROM t").fetchone()[0] == rows


def test_ingest_many_batches(tool, tmp_path):
    """A large upload in uneven batches, wider than DuckDB's chunk size."""
    n = 1_000_003
    upload = pa.table({"i": pa.array(range(n), type=pa.int64()),
                       "s": pa.array([f"s{i}" if i % 11 else None for i in range(n)])})
    source = tmp_path / "upload.arrows"
    db = tmp_path / "big.duckdb"
    with pa.ipc.new_stream(sink=str(source), schema=upload.schema,
                           options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
        for batch in upload.to_batches(max_chunksize=7777):
            writer.write_batch(batch=batch)

    proc = run_tool(tool=tool, args=["ingest", "--db", str(db), "--table", "t", "--in", str(source),
                                     "--mode", "create"])
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == str(n)
    con = duckdb.connect(database=str(db))
    assert con.execute(query="SELECT count(*), count(s), sum(i) FROM t").fetchone() == (
        n, n - len(range(0, n, 11)), n * (n - 1) // 2)
