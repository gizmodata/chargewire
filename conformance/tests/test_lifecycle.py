"""Spec §11.1 (graceful shutdown) and §3.2 (health that answers when its query hangs).

These start servers of their own from WARP_SQL_TEST_SERVER_BINARY, with these flags
(defaults are warp-duckdb's; Hardcharger's in brackets):

    WARP_SQL_TEST_SERVER_DRAIN_FLAG         --drain-seconds          [--shutdown-grace-period-seconds]
    WARP_SQL_TEST_SERVER_HEALTH_QUERY_FLAG  --health-check-query     [--health-check-query]

Against a server given by URL they cannot run: deselect them with -m "not own_server".
"""

import concurrent.futures
import os
import time

import httpx
import pytest

from conftest import STOP_SIGNAL, start_server, stop_server
from test_discovery_auth import assert_problem
from warp import ARROW_STREAM, Warp

pytestmark = pytest.mark.own_server

SLOW = "SELECT sum(i) FROM range(100000000000) t(i)"  # runs until stopped
# Long enough to be running when the signal comes, short enough to finish in the grace period.
MEDIUM = "SELECT sum(i) AS s FROM range(3000000000) t(i)"
GRACE_SECONDS = 15
EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


def binary() -> str:
    path = os.environ.get("WARP_SQL_TEST_SERVER_BINARY")
    if not path:
        pytest.fail(reason="these tests start their own servers: set WARP_SQL_TEST_SERVER_BINARY, "
                           "or deselect them with -m 'not own_server'")
    return path


def test_one_signal_drains_running_work_and_refuses_new_work():
    flag = os.environ.get("WARP_SQL_TEST_SERVER_DRAIN_FLAG", "--drain-seconds")
    server = start_server(binary=binary(), extra_args=[flag, str(GRACE_SECONDS)])
    try:
        warp = Warp(base_url=server.base_url, username=server.username, password=server.password)
        finishing = warp.new_session()
        slow_session = warp.new_session()
        idle = warp.new_session()
        medium = warp.submit(session_id=finishing, sql=MEDIUM, headers={"Prefer": "respond-async"}).json()
        # The slow query streams: its reader sees the cut when the grace period ends.
        slow_client = httpx.Client(base_url=server.base_url, timeout=GRACE_SECONDS + 60)
        slow_request = slow_client.build_request(method="POST", url=f"{warp.session_url(slow_session)}/statements",
                                                 headers=warp.headers(Accept=ARROW_STREAM), json={"sql": SLOW})
        slow = slow_client.send(request=slow_request, stream=True)
        assert slow.status_code == 200, slow.read()
        time.sleep(0.5)
        assert warp.status(session_id=finishing, statement_id=medium["statement_id"])["state"] == "running"

        signalled = time.monotonic()
        server.process.send_signal(STOP_SIGNAL)  # one signal: drain, don't stop
        deadline = signalled + 10
        while True:
            health = httpx.get(url=f"{server.base_url}/warp/v1/health", timeout=10)
            if health.status_code == 503:
                break
            assert time.monotonic() < deadline, health.text
            time.sleep(0.1)
        assert health.json() == {"status": "not_serving", "reason": "draining"}, health.text

        # New sessions and statements are refused, with when to come back.
        for refused in (warp.http.post(url="/warp/v1/sessions", headers=warp.headers(), json={}),
                        warp.submit(session_id=idle, sql="SELECT 1")):
            assert_problem(response=refused, status=503, code="server_draining")
            assert refused.headers["retry-after"].isdigit(), refused.headers  # §2.6: delta-seconds

        # Running work finishes.
        done = warp.wait_done(session_id=finishing, statement_id=medium["statement_id"], timeout=GRACE_SECONDS)
        assert done["state"] == "succeeded", done
        assert time.monotonic() - signalled < GRACE_SECONDS, "the statement finished only when the grace period ended"

        # What still runs when the grace period ends is cancelled: its stream is cut.
        body = bytearray()
        try:
            for chunk in slow.iter_raw():
                body.extend(chunk)
        except httpx.HTTPError:
            pass
        finally:
            slow.close()
            slow_client.close()
        assert not bytes(body).endswith(EOS)
        assert time.monotonic() - signalled >= GRACE_SECONDS - 1, "the running query was cut before the grace period"
        assert server.process.wait(timeout=60) == 0, server.log_tail()
    finally:
        stop_server(server=server)


def test_health_answers_in_bounded_time_when_its_query_hangs():
    flag = os.environ.get("WARP_SQL_TEST_SERVER_HEALTH_QUERY_FLAG", "--health-check-query")
    server = start_server(binary=binary(), extra_args=[flag, SLOW])
    try:
        deadline = time.monotonic() + 90
        while True:
            started = time.monotonic()
            # Every answer comes within a bounded time, whatever the query does (§3.2).
            response = httpx.get(url=f"{server.base_url}/warp/v1/health", timeout=20)
            assert time.monotonic() - started < 20
            if response.status_code == 503:
                assert response.json() == {"status": "not_serving", "reason": "check_failed"}, response.text
                break
            assert response.status_code == 200, response.text
            assert time.monotonic() < deadline, "health never reported the hanging check"
            time.sleep(0.5)
        # Several probes while it hangs: each answers, none waits for the query.
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            answers = list(pool.map(lambda _: httpx.get(url=f"{server.base_url}/warp/v1/health", timeout=20),
                                    range(4)))
        assert all(a.status_code == 503 for a in answers), [a.text for a in answers]
        # The server itself still serves.
        warp = Warp(base_url=server.base_url, username=server.username, password=server.password)
        assert warp.query(session_id=warp.new_session(), sql="SELECT 1 AS v").to_pylist() == [{"v": 1}]
    finally:
        assert stop_server(server=server) == 0, server.log_tail()
