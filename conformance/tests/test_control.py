"""Spec §6.7–§6.9: idempotent submission, cancellation, timeouts."""

import time

from test_discovery_auth import assert_problem

# count(*) over a range is answered without scanning it; a sum has to scan.
SLOW = "SELECT sum(i) FROM range(100000000000) t(i)"  # runs until stopped


def test_cancel_stops_a_running_statement(warp, session_id):
    response = warp.submit(session_id=session_id, sql=SLOW, headers={"Prefer": "respond-async"})
    statement_id = response.json()["statement_id"]
    time.sleep(0.5)
    cancel = warp.http.post(url=f"{warp.statement_url(session_id, statement_id)}/cancel",
                            headers=warp.headers())
    assert cancel.status_code == 202, cancel.text
    doc = warp.wait_done(session_id=session_id, statement_id=statement_id, timeout=20)
    assert doc["state"] == "cancelled"
    assert doc["cancel_reason"] == "client"
    # The session is usable afterwards.
    assert warp.query(session_id=session_id, sql="SELECT 1 AS v").to_pylist() == [{"v": 1}]


def test_cancel_a_finished_statement_is_harmless(warp, session_id):
    doc = warp.execute(session_id=session_id, sql="SELECT 1")
    cancel = warp.http.post(url=f"{warp.statement_url(session_id, doc['statement_id'])}/cancel",
                            headers=warp.headers())
    assert cancel.status_code == 202
    assert warp.status(session_id=session_id, statement_id=doc["statement_id"])["state"] == "succeeded"


def test_statement_timeout(warp, session_id):
    response = warp.submit(session_id=session_id, sql=SLOW,
                           headers={"Prefer": "respond-async", "Warp-Statement-Timeout": "300"})
    doc = warp.wait_done(session_id=session_id, statement_id=response.json()["statement_id"],
                         timeout=20)
    assert doc["state"] == "cancelled"
    assert doc["cancel_reason"] == "timeout"
    assert doc["error"]["code"] == "statement_timeout"


def test_delete_statement(warp, session_id):
    doc = warp.execute(session_id=session_id, sql="SELECT 1")
    url = warp.statement_url(session_id, doc["statement_id"])
    assert warp.http.delete(url=url, headers=warp.headers()).status_code == 204
    assert_problem(response=warp.http.get(url=url, headers=warp.headers()), status=404,
                   code="statement_not_found")


def test_idempotent_retry_runs_once(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TABLE once (v INTEGER)")
    key = f"retry-{time.time_ns()}"
    first = warp.submit(session_id=session_id, sql="INSERT INTO once VALUES (1)",
                        headers={"Idempotency-Key": key})
    assert first.status_code == 201, first.text
    retry = warp.submit(session_id=session_id, sql="INSERT INTO once VALUES (1)",
                        headers={"Idempotency-Key": key})
    assert retry.status_code == 200, retry.text
    assert retry.json()["statement_id"] == first.json()["statement_id"]
    count = warp.query(session_id=session_id, sql="SELECT count(*) AS n FROM once")
    assert count.column("n").to_pylist() == [1]


def test_idempotency_key_reused_for_another_request(warp, session_id):
    key = f"reuse-{time.time_ns()}"
    assert warp.submit(session_id=session_id, sql="SELECT 1",
                       headers={"Idempotency-Key": key}).status_code == 201
    response = warp.submit(session_id=session_id, sql="SELECT 2", headers={"Idempotency-Key": key})
    assert_problem(response=response, status=422, code="idempotency_key_reused")
