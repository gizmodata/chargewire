"""DuckDB result -> nanoarrow IPC -> Arrow JS: a browser client must decode every
value, and writing the table back must give the same table."""

import pathlib
import subprocess

import pyarrow.ipc as ipc
import pytest

from cases import ARROW_SETTINGS, DUCKDB_CASES, duckdb_case_sql
from conftest import SESSION_SETUP, SPIKE_DIR, run_tool

DECODE = SPIKE_DIR / "js" / "decode.mjs"


@pytest.mark.parametrize("settings", list(ARROW_SETTINGS))
@pytest.mark.parametrize("case", list(DUCKDB_CASES))
def test_js_decodes_result(tool, tmp_path: pathlib.Path, case, settings):
    sql = duckdb_case_sql(name=case)
    exported = tmp_path / "result.arrows"
    rewritten = tmp_path / "rewritten.arrows"
    proc = run_tool(tool=tool, args=["export", "--db", ":memory:",
                                     "--setup", SESSION_SETUP + ARROW_SETTINGS[settings],
                                     "--sql", sql, "--out", str(exported)])
    assert proc.returncode == 0, proc.stderr

    js = subprocess.run(args=["node", str(DECODE), str(exported), str(rewritten)],
                        capture_output=True, text=True, check=False)
    assert js.returncode == 0, js.stderr.strip().splitlines()[-1] if js.stderr else "node failed"

    original = ipc.open_stream(source=str(exported)).read_all()
    again = ipc.open_stream(source=str(rewritten)).read_all()
    assert again.schema.equals(original.schema, check_metadata=True), (
        f"\n  js: {again.schema}\nwant: {original.schema}")
    assert again.equals(original)
