"""DuckDB -> Arrow IPC -> DuckDB through the tool, both directions over nanoarrow:
does a column come back with the same type and the same values? This is what a
GizmoSQL-to-GizmoSQL copy (or a client that writes back what it read) sees."""

import duckdb
import pytest

from cases import ARROW_SETTINGS, DUCKDB_CASES, duckdb_case_sql
from conftest import SESSION_SETUP, run_tool


@pytest.mark.parametrize("settings", ["default", "lossless"])
@pytest.mark.parametrize("case", list(DUCKDB_CASES))
def test_roundtrip_keeps_type_and_values(tool, tmp_path, case, settings):
    sql = duckdb_case_sql(name=case)
    setup = SESSION_SETUP + ARROW_SETTINGS[settings]
    stream = tmp_path / "rows.arrows"
    db = tmp_path / "copy.duckdb"

    exported = run_tool(tool=tool, args=["export", "--db", ":memory:", "--setup", setup,
                                         "--sql", sql, "--out", str(stream), "--compression", "zstd"])
    assert exported.returncode == 0, exported.stderr
    ingested = run_tool(tool=tool, args=["ingest", "--db", str(db), "--setup", setup,
                                         "--table", "copy", "--in", str(stream), "--mode", "create"])
    assert ingested.returncode == 0, ingested.stderr

    con = duckdb.connect(database=str(db))
    con.execute(query=setup)
    original_type = con.execute(query=f"SELECT typeof(v) FROM ({sql}) LIMIT 1").fetchone()[0]
    copied_type = con.execute(query="SELECT typeof(v) FROM copy LIMIT 1").fetchone()[0]
    original = con.execute(query=f"SELECT v::VARCHAR FROM ({sql})").fetchall()
    copied = con.execute(query="SELECT v::VARCHAR FROM copy ORDER BY rowid").fetchall()

    assert copied_type == original_type, f"{original_type} came back as {copied_type}"
    assert copied == original, f"\n copied: {copied}\noriginal: {original}"
