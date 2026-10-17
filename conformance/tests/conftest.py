"""Where the server under test comes from.

Either point the suite at a running server:

    WARP_SQL_TEST_URL=https://host:port  WARP_SQL_TEST_USERNAME=…  WARP_SQL_TEST_PASSWORD=…

or let it start one, given a binary that takes the reference server's interface
(`--host`, `--port`, password in WARP_DUCKDB_PASSWORD), such as `warp-duckdb`:

    WARP_SQL_TEST_SERVER_BINARY=/path/to/warp-duckdb

The suite fails, never skips, when it has no reachable server.
"""

import dataclasses
import os
import secrets
import signal
import socket
import subprocess
import time

import httpx
import pytest

from warp import Warp


@dataclasses.dataclass
class ServerUnderTest:
    base_url: str
    username: str
    password: str
    process: subprocess.Popen | None = None


def free_port() -> int:
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until_up(base_url: str, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if httpx.get(url=f"{base_url}/warp", timeout=2).status_code == 200:
                return
        except httpx.HTTPError as error:
            last_error = error
        time.sleep(0.2)
    pytest.fail(reason=f"no Warp SQL server answered at {base_url}/warp ({last_error})")


def start_server(binary: str, extra_args: list[str] | None = None) -> ServerUnderTest:
    """Starts a reference-compatible server on a free port with a fresh password."""
    port = free_port()
    # A password with ':' and '@' in it: credentials travel as JSON fields, so nothing
    # may split them (spec §4.1).
    password = secrets.token_urlsafe(nbytes=12) + ":p@ss:word"
    process = subprocess.Popen(
        args=[binary, "--host", "127.0.0.1", "--port", str(port), *(extra_args or [])],
        env={**os.environ, "WARP_DUCKDB_PASSWORD": password},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    server = ServerUnderTest(base_url=f"http://127.0.0.1:{port}", username="warp",
                             password=password, process=process)
    try:
        wait_until_up(base_url=server.base_url)
    except BaseException:
        stop_server(server=server)
        raise
    return server


def stop_server(server: ServerUnderTest) -> None:
    if server.process is None or server.process.poll() is not None:
        return
    server.process.send_signal(signal.SIGTERM)
    server.process.send_signal(signal.SIGTERM)  # the second signal stops without draining
    try:
        server.process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        server.process.kill()


@pytest.fixture(scope="session")
def server():
    binary = os.environ.get("WARP_SQL_TEST_SERVER_BINARY")
    if binary:
        started = start_server(binary=binary)
        yield started
        stop_server(server=started)
        return
    url = os.environ.get("WARP_SQL_TEST_URL")
    if not url:
        pytest.fail(reason="set WARP_SQL_TEST_URL (with WARP_SQL_TEST_USERNAME and "
                           "WARP_SQL_TEST_PASSWORD) or WARP_SQL_TEST_SERVER_BINARY")
    wait_until_up(base_url=url)
    yield ServerUnderTest(base_url=url, username=os.environ.get("WARP_SQL_TEST_USERNAME", ""),
                          password=os.environ.get("WARP_SQL_TEST_PASSWORD", ""))


@pytest.fixture
def warp(server):
    client = Warp(base_url=server.base_url, username=server.username, password=server.password)
    yield client
    client.close()


@pytest.fixture
def session_id(warp):
    sid = warp.new_session()
    yield sid
    warp.http.delete(url=warp.session_url(sid), headers=warp.headers())
