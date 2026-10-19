"""Spec §11.1 (graceful shutdown), §3.2 (health that answers when its query hangs) and §6.5
(a read under way when its result expires).

These start servers of their own from WARP_SQL_TEST_SERVER_BINARY, with these flags
(defaults are warp-duckdb's; Hardcharger's in brackets):

    WARP_SQL_TEST_SERVER_DRAIN_FLAG         --drain-seconds          [--shutdown-grace-period-seconds]
    WARP_SQL_TEST_SERVER_HEALTH_QUERY_FLAG  --health-check-query     [--health-check-query]
    WARP_SQL_TEST_SERVER_RETENTION_FLAG     --result-retention-seconds [--result-retention-seconds]

Against a server given by URL they cannot run: deselect them with -m "not own_server".
"""

import concurrent.futures
import datetime
import os
import time

import httpx
import pyarrow.ipc as ipc
import pytest

from conftest import STOP_SIGNAL, start_server, stop_server
from test_discovery_auth import assert_problem
from warp import ARROW_STREAM, Warp

pytestmark = pytest.mark.own_server

SLOW = "SELECT sum(i) FROM range(100000000000) t(i)"  # runs until stopped
# Running when the signal comes (it is sent right after the submit), and short enough to
# finish well inside the grace period even on a sanitizer build.
MEDIUM = "SELECT sum(i) AS s FROM range(1000000000) t(i)"
GRACE_SECONDS = 30
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
        # The slow query streams: its reader sees the cut when the grace period ends.
        slow_client = httpx.Client(base_url=server.base_url, timeout=GRACE_SECONDS + 60)
        slow_request = slow_client.build_request(method="POST", url=f"{warp.session_url(slow_session)}/statements",
                                                 headers=warp.headers(Accept=ARROW_STREAM), json={"sql": SLOW})
        slow = slow_client.send(request=slow_request, stream=True)
        assert slow.status_code == 200, slow.read()
        medium = warp.submit(session_id=finishing, sql=MEDIUM, headers={"Prefer": "respond-async"}).json()

        signalled = time.monotonic()
        server.process.send_signal(STOP_SIGNAL)  # one signal: drain, don't stop
        was_running = warp.status(session_id=finishing, statement_id=medium["statement_id"])["state"] == "running"
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

        # Running work finishes (when it was still running at the signal: on a fast machine it
        # may have finished first, which proves less but is no failure).
        done = warp.wait_done(session_id=finishing, statement_id=medium["statement_id"], timeout=GRACE_SECONDS)
        assert done["state"] == "succeeded", done
        if was_running:
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


def test_a_read_under_way_when_its_result_expires_is_delivered_in_full():
    # §6.5: expiry applies to reads that start after it. A client reading a retained result
    # slowly (backpressure holds the server's stream) past its expiry still gets every
    # batch and the end-of-stream marker; a read that starts afterwards gets 410.
    flag = os.environ.get("WARP_SQL_TEST_SERVER_RETENTION_FLAG", "--result-retention-seconds")
    retention = 2
    server = start_server(binary=binary(), extra_args=[flag, str(retention)])
    try:
        warp = Warp(base_url=server.base_url, username=server.username, password=server.password)
        session_id = warp.new_session()
        rows = 40_000_000  # 320 MB of BIGINT: far more than socket buffers hold
        sql = f"SELECT i FROM range({rows}) t(i)"
        doc = warp.submit(session_id=session_id, sql=sql,
                          headers={"Prefer": "wait=60", "Warp-Retain-Result": "true"}).json()
        if doc["state"] != "succeeded":
            doc = warp.wait_done(session_id=session_id, statement_id=doc["statement_id"], timeout=120)
        assert doc["state"] == "succeeded" and doc["result"]["retained"] is True, doc
        url = f"{warp.statement_url(session_id, doc['statement_id'])}/result"
        reader = httpx.Client(base_url=server.base_url, timeout=120)
        try:
            response = reader.send(request=reader.build_request(method="GET", url=url, headers=warp.headers()),
                                   stream=True)
            assert response.status_code == 200, response.read()
            chunks = response.iter_raw(chunk_size=1 << 20)
            body = bytearray(next(chunks))
            # Past the expiry and the server's next look at expired results.
            time.sleep(retention + 8)
            refused = warp.result(session_id=session_id, statement_id=doc["statement_id"])
            assert_problem(response=refused, status=410, code="result_expired")
            for chunk in chunks:
                body.extend(chunk)
        finally:
            reader.close()
        assert bytes(body).endswith(EOS), "the read under way was cut when its result expired"
        table = ipc.open_stream(source=bytes(body)).read_all()
        assert table.num_rows == rows
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


def test_a_result_is_expired_from_its_expires_at_on():
    # §6.5: a read that starts after expires_at gets 410, whenever the server next tidies up.
    # Two results whose expiries are 2.5 s apart, each read 0.2 s after its expires_at: a
    # server that expires results only on a periodic sweep can't have swept just then for both.
    flag = os.environ.get("WARP_SQL_TEST_SERVER_RETENTION_FLAG", "--result-retention-seconds")
    server = start_server(binary=binary(), extra_args=[flag, "2"])
    try:
        warp = Warp(base_url=server.base_url, username=server.username, password=server.password)
        session_id = warp.new_session()
        for attempt in range(2):
            doc = warp.submit(session_id=session_id, sql="SELECT 1 AS one",
                              headers={"Prefer": "wait=30", "Warp-Retain-Result": "true"}).json()
            assert doc["state"] == "succeeded" and doc["result"]["retained"] is True, doc
            expires_at = datetime.datetime.fromisoformat(doc["result"]["expires_at"].replace("Z", "+00:00"))
            # Before expires_at the result can be read again.
            assert warp.result(session_id=session_id, statement_id=doc["statement_id"]).status_code == 200
            wait = (expires_at - datetime.datetime.now(tz=datetime.timezone.utc)).total_seconds() + 0.2
            time.sleep(max(wait, 0))
            refused = warp.result(session_id=session_id, statement_id=doc["statement_id"])
            assert_problem(response=refused, status=410, code="result_expired")
            time.sleep(0.3 if attempt == 0 else 0)
    finally:
        assert stop_server(server=server) == 0, server.log_tail()
