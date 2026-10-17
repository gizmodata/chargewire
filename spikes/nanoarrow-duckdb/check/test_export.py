"""DuckDB result -> nanoarrow IPC writer -> pyarrow must equal DuckDB's own Arrow
export of the same query, schema metadata included."""

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from cases import ARROW_SETTINGS, DUCKDB_CASES, duckdb_case_sql
from conftest import SESSION_SETUP, run_tool


@pytest.mark.parametrize("compression", ["none", "zstd", "lz4"])
@pytest.mark.parametrize("settings", list(ARROW_SETTINGS))
@pytest.mark.parametrize("case", list(DUCKDB_CASES))
def test_export_matches_duckdb(tool, tmp_path, case, settings, compression):
    sql = duckdb_case_sql(name=case)
    setup = SESSION_SETUP + ARROW_SETTINGS[settings]
    out = tmp_path / "result.arrows"

    proc = run_tool(tool=tool, args=["export", "--db", ":memory:", "--setup", setup, "--sql", sql,
                                     "--out", str(out), "--compression", compression])
    assert proc.returncode == 0, proc.stderr

    got = ipc.open_stream(source=str(out)).read_all()
    con = duckdb.connect()
    con.execute(query=setup)
    expected = con.sql(query=sql).arrow().read_all()

    assert got.schema.equals(expected.schema, check_metadata=True), (
        f"\n got: {got.schema}\nwant: {expected.schema}")
    assert got.equals(expected), f"\n got: {got.to_pylist()}\nwant: {expected.to_pylist()}"


def test_export_many_batches(tool, tmp_path):
    """A result spanning many record batches arrives complete and in order."""
    sql = "SELECT i, 'row ' || i AS s, i % 7 = 0 AS b FROM range(1000000) t(i)"
    out = tmp_path / "result.arrows"
    proc = run_tool(tool=tool, args=["export", "--db", ":memory:", "--sql", sql, "--out", str(out),
                                     "--compression", "zstd", "--batch-rows", "10000"])
    assert proc.returncode == 0, proc.stderr

    reader = ipc.open_stream(source=str(out))
    batches = list(reader)
    assert len(batches) >= 100
    got = pa.Table.from_batches(batches=batches)
    assert got.num_rows == 1_000_000
    assert got.column(0).to_pylist() == list(range(1_000_000))
