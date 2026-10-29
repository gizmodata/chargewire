"""Where the server under test comes from.

Either point the suite at a running server:

    CHARGEWIRE_TEST_URL=https://host:port  CHARGEWIRE_TEST_USERNAME=…  CHARGEWIRE_TEST_PASSWORD=…

or let it start one, given a binary that takes `--host` and `--port` and reads its
password from an environment variable, such as `chargewire-duckdb`:

    CHARGEWIRE_TEST_SERVER_BINARY=/path/to/chargewire-duckdb
    CHARGEWIRE_TEST_SERVER_PASSWORD_ENV=CHARGEWIRE_DUCKDB_PASSWORD   (the default)
    CHARGEWIRE_TEST_SERVER_USERNAME=chargewire                       (the default)

The suite fails, never skips, when it has no reachable server. A server it started must
also survive every test and stop cleanly (exit code 0) at the end: a crash, including
one a sanitizer reports at shutdown, fails the run with the end of the server's log
(kept in CHARGEWIRE_TEST_SERVER_LOG, or a temporary file).
"""

import dataclasses
import os
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx
import pytest

from openapi_check import OpenApi
from openapi_check import operations as operations_of
from chargewire import Chargewire


@dataclasses.dataclass
class ServerUnderTest:
    base_url: str
    username: str
    password: str
    process: subprocess.Popen | None = None
    log: Path | None = None

    def log_tail(self, lines: int = 60) -> str:
        if self.log is None or not self.log.exists():
            return ""
        return "\n".join(self.log.read_text(errors="replace").splitlines()[-lines:])

    def check_alive(self) -> None:
        if self.process is not None and self.process.poll() is not None:
            pytest.fail(reason=f"the server died (exit code {self.process.returncode}):\n"
                               f"{self.log_tail()}", pytrace=False)


# How the suite asks a server it started to stop, as `kill` does with SIGTERM: on Windows
# a console Ctrl+Break event, which reaches only a process started in its own process
# group (Ctrl+C can't be sent to one).
if sys.platform == "win32":
    STOP_SIGNAL = signal.CTRL_BREAK_EVENT
    POPEN_FLAGS = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
else:
    STOP_SIGNAL = signal.SIGTERM
    POPEN_FLAGS = {}


def free_port() -> int:
    with socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until_up(base_url: str, timeout: float = 20, verify=True) -> None:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if httpx.get(url=f"{base_url}/chargewire", timeout=2, verify=verify).status_code == 200:
                return
        except httpx.HTTPError as error:
            last_error = error
        time.sleep(0.2)
    pytest.fail(reason=f"no Chargewire server answered at {base_url}/chargewire ({last_error})")


def start_server(binary: str, extra_args: list[str] | None = None,
                 scheme: str = "http", verify=True, port: int | None = None,
                 env: dict[str, str] | None = None) -> ServerUnderTest:
    """Starts a reference-compatible server on a free port (or `port`) with a fresh password.
    `scheme` "https" (with `verify`, httpx's TLS setting) for a server started with TLS;
    `env`: more environment variables for it."""
    port = port or free_port()
    # A password with ':' and '@' in it: credentials travel as JSON fields, so nothing
    # may split them (spec §4.1).
    password = secrets.token_urlsafe(nbytes=12) + ":p@ss:word"
    password_env = os.environ.get("CHARGEWIRE_TEST_SERVER_PASSWORD_ENV", "CHARGEWIRE_DUCKDB_PASSWORD")
    username = os.environ.get("CHARGEWIRE_TEST_SERVER_USERNAME", "chargewire")
    log = Path(os.environ.get("CHARGEWIRE_TEST_SERVER_LOG")
               or tempfile.mkstemp(prefix="chargewire-server-", suffix=".log")[1])
    with log.open(mode="ab") as out:
        process = subprocess.Popen(
            args=[binary, "--host", "127.0.0.1", "--port", str(port), *(extra_args or [])],
            env={**os.environ, **(env or {}), password_env: password}, stdout=out, stderr=subprocess.STDOUT,
            **POPEN_FLAGS)
    server = ServerUnderTest(base_url=f"{scheme}://127.0.0.1:{port}", username=username,
                             password=password, process=process, log=log)
    try:
        wait_until_up(base_url=server.base_url, verify=verify)
    except BaseException:
        stop_server(server=server)
        raise
    return server


def stop_server(server: ServerUnderTest) -> int | None:
    """Stops the server; returns its exit code (None if it had to be killed)."""
    if server.process is None:
        return 0
    if server.process.poll() is not None:
        return server.process.returncode
    server.process.send_signal(STOP_SIGNAL)
    server.process.send_signal(STOP_SIGNAL)  # the second signal stops without draining
    try:
        return server.process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        server.process.kill()
        return None


@pytest.fixture(scope="session")
def server():
    binary = os.environ.get("CHARGEWIRE_TEST_SERVER_BINARY")
    if binary:
        started = start_server(binary=binary)
        yield started
        code = stop_server(server=started)
        if code != 0:
            pytest.fail(reason=f"the server did not stop cleanly (exit code {code}):\n"
                               f"{started.log_tail()}", pytrace=False)
        return
    url = os.environ.get("CHARGEWIRE_TEST_URL")
    if not url:
        pytest.fail(reason="set CHARGEWIRE_TEST_URL (with CHARGEWIRE_TEST_USERNAME and "
                           "CHARGEWIRE_TEST_PASSWORD) or CHARGEWIRE_TEST_SERVER_BINARY")
    wait_until_up(base_url=url)
    yield ServerUnderTest(base_url=url, username=os.environ.get("CHARGEWIRE_TEST_USERNAME", ""),
                          password=os.environ.get("CHARGEWIRE_TEST_PASSWORD", ""))


@pytest.fixture
def chargewire(server):
    client = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
    yield client
    client.close()
    server.check_alive()


@pytest.fixture
def session_id(chargewire):
    sid = chargewire.new_session()
    yield sid
    chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


# ---- Every response is checked against the OpenAPI document (spec/openapi.yaml) ----
#
# The check wraps httpx.Client.send, which every request of the suite goes through
# (httpx.get and the like included), so each JSON endpoint the suite exercises has its
# responses validated on every call, errors included. A response that disagrees fails the
# test that made it, with what was wrong. A test that must see a response the document does
# not describe says so with @pytest.mark.openapi_unchecked.

OPENAPI = OpenApi()
_openapi_problems: list[str] = []
_original_send = httpx.Client.send


def _checked_send(self, request, *args, **kwargs):
    response = _original_send(self, request, *args, **kwargs)
    streamed = kwargs.get("stream", False)
    try:
        problems = OPENAPI.check(
            method=request.method, path=request.url.path, status=response.status_code,
            headers={name.lower(): value for name, value in response.headers.items()},
            body=None if streamed else response.content)
    except Exception as error:  # a bug in the check must not hide behind the test's result
        problems = [f"{request.method} {request.url.path}: the OpenAPI check failed: {error!r}"]
    _openapi_problems.extend(problems)
    return response


httpx.Client.send = _checked_send


def _fail_on_openapi_problems(item) -> None:
    problems = list(_openapi_problems)
    _openapi_problems.clear()
    if problems and item.get_closest_marker("openapi_unchecked") is None:
        pytest.fail(reason="responses that disagree with spec/openapi.yaml:\n  "
                           + "\n  ".join(dict.fromkeys(problems)), pytrace=False)


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item):
    # The test's own requests: a disagreement fails the test itself.
    result = yield
    _fail_on_openapi_problems(item=item)
    return result


@pytest.fixture(autouse=True)
def responses_match_the_openapi_document(request):
    # Requests made by fixtures, before and after the test, are held to the document too.
    _fail_on_openapi_problems(item=request.node)
    yield
    _fail_on_openapi_problems(item=request.node)


def pytest_terminal_summary(terminalreporter):
    if not OPENAPI.seen:
        return
    every = sorted(operations_of(OPENAPI.doc))
    missing = [f"{method} {path}" for method, path in every if (method, path) not in OPENAPI.seen]
    terminalreporter.write_line(
        f"OpenAPI: responses of {len(every) - len(missing)} of {len(every)} operations checked"
        + (f"; not exercised in this run: {', '.join(missing)}" if 0 < len(missing) <= 8 else ""))
