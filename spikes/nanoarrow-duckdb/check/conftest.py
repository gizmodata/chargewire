import pathlib
import subprocess

import pytest

SPIKE_DIR = pathlib.Path(__file__).resolve().parent.parent
TOOL = SPIKE_DIR / "build" / "warp-spike"

# Both sides render TIMESTAMPTZ in the same zone.
SESSION_SETUP = "SET TimeZone = 'UTC';"


@pytest.fixture(scope="session")
def tool() -> pathlib.Path:
    # A missing tool fails the run rather than skipping it.
    if not TOOL.exists():
        pytest.fail(reason=f"{TOOL} is missing: build it with cmake first")
    return TOOL


def run_tool(tool: pathlib.Path, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(args=[str(tool), *args], capture_output=True, text=True, check=False)
