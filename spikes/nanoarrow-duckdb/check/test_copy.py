"""pyarrow IPC -> nanoarrow reader -> nanoarrow writer -> pyarrow must give back the
same table, for every Arrow type and every compression on either side."""

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from cases import ARROW_CASES
from conftest import run_tool

COMPRESSIONS = ["none", "zstd", "lz4"]


def write_ipc(table: pa.Table, path, compression: str) -> None:
    options = ipc.IpcWriteOptions(compression=None if compression == "none" else compression)
    with ipc.new_stream(sink=str(path), schema=table.schema, options=options) as writer:
        writer.write_table(table=table)


@pytest.mark.parametrize("out_compression", COMPRESSIONS)
@pytest.mark.parametrize("in_compression", COMPRESSIONS)
@pytest.mark.parametrize("case", list(ARROW_CASES))
def test_copy_preserves_table(tool, tmp_path, case, in_compression, out_compression):
    table = pa.table({"v": ARROW_CASES[case]})
    source = tmp_path / "in.arrows"
    copy = tmp_path / "out.arrows"
    write_ipc(table=table, path=source, compression=in_compression)

    proc = run_tool(tool=tool, args=["copy", "--in", str(source), "--out", str(copy),
                                     "--compression", out_compression])
    assert proc.returncode == 0, proc.stderr

    got = ipc.open_stream(source=str(copy)).read_all()
    assert got.schema.equals(table.schema, check_metadata=True), (
        f"\n got: {got.schema}\nwant: {table.schema}")
    assert got.equals(table), f"\n got: {got.to_pylist()}\nwant: {table.to_pylist()}"
