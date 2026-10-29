"""Spec §6.7–§6.9: idempotent submission, cancellation, timeouts."""

import time

from test_discovery_auth import assert_problem

# count(*) over a range is answered without scanning it; a sum has to scan.
SLOW = "SELECT sum(i) FROM range(100000000000) t(i)"  # runs until stopped


def test_cancel_stops_a_running_statement(chargewire, session_id):
    response = chargewire.submit(session_id=session_id, sql=SLOW, headers={"Prefer": "respond-async"})
    statement_id = response.json()["statement_id"]
    time.sleep(0.5)
    cancel = chargewire.http.post(url=f"{chargewire.statement_url(session_id, statement_id)}/cancel",
                            headers=chargewire.headers())
    assert cancel.status_code == 202, cancel.text
    doc = chargewire.wait_done(session_id=session_id, statement_id=statement_id, timeout=20)
    assert doc["state"] == "cancelled"
    assert doc["cancel_reason"] == "client"
    # The session is usable afterwards.
    assert chargewire.query(session_id=session_id, sql="SELECT 1 AS v").to_pylist() == [{"v": 1}]


def test_a_cancel_never_reaches_the_sessions_next_statement(chargewire, session_id):
    # A server may repeat its interrupt until a cancelled statement ends; none of it may
    # land on the statement the client runs right after (long enough to span a repeat).
    for _ in range(5):
        response = chargewire.submit(session_id=session_id, sql=SLOW, headers={"Prefer": "respond-async"})
        statement_id = response.json()["statement_id"]
        chargewire.http.post(url=f"{chargewire.statement_url(session_id, statement_id)}/cancel",
                       headers=chargewire.headers())
        doc = chargewire.wait_done(session_id=session_id, statement_id=statement_id, timeout=20)
        assert doc["state"] == "cancelled", doc
        next_doc = chargewire.execute(session_id=session_id,
                                sql="SELECT sum(i) AS s FROM range(300000000) t(i)")
        assert next_doc["state"] == "succeeded", next_doc


def test_cancel_a_finished_statement_is_harmless(chargewire, session_id):
    doc = chargewire.execute(session_id=session_id, sql="SELECT 1")
    cancel = chargewire.http.post(url=f"{chargewire.statement_url(session_id, doc['statement_id'])}/cancel",
                            headers=chargewire.headers())
    assert cancel.status_code == 202
    assert chargewire.status(session_id=session_id, statement_id=doc["statement_id"])["state"] == "succeeded"


def test_statement_timeout(chargewire, session_id):
    response = chargewire.submit(session_id=session_id, sql=SLOW,
                           headers={"Prefer": "respond-async", "Chargewire-Statement-Timeout": "300"})
    doc = chargewire.wait_done(session_id=session_id, statement_id=response.json()["statement_id"],
                         timeout=20)
    assert doc["state"] == "cancelled"
    assert doc["cancel_reason"] == "timeout"
    assert doc["error"]["code"] == "statement_timeout"


def test_delete_statement(chargewire, session_id):
    doc = chargewire.execute(session_id=session_id, sql="SELECT 1")
    url = chargewire.statement_url(session_id, doc["statement_id"])
    assert chargewire.http.delete(url=url, headers=chargewire.headers()).status_code == 204
    assert_problem(response=chargewire.http.get(url=url, headers=chargewire.headers()), status=404,
                   code="statement_not_found")


def test_idempotent_retry_runs_once(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE once (v INTEGER)")
    key = f"retry-{time.time_ns()}"
    first = chargewire.submit(session_id=session_id, sql="INSERT INTO once VALUES (1)",
                        headers={"Idempotency-Key": key})
    assert first.status_code == 201, first.text
    retry = chargewire.submit(session_id=session_id, sql="INSERT INTO once VALUES (1)",
                        headers={"Idempotency-Key": key})
    assert retry.status_code == 200, retry.text
    assert retry.json()["statement_id"] == first.json()["statement_id"]
    count = chargewire.query(session_id=session_id, sql="SELECT count(*) AS n FROM once")
    assert count.column("n").to_pylist() == [1]


def test_idempotency_key_reused_for_another_request(chargewire, session_id):
    key = f"reuse-{time.time_ns()}"
    assert chargewire.submit(session_id=session_id, sql="SELECT 1",
                       headers={"Idempotency-Key": key}).status_code == 201
    response = chargewire.submit(session_id=session_id, sql="SELECT 2", headers={"Idempotency-Key": key})
    assert_problem(response=response, status=422, code="idempotency_key_reused")


def test_session_cancel_stops_what_the_session_is_running(chargewire, session_id):
    # For a client that has no statement id yet (its submit's answer hasn't arrived).
    response = chargewire.submit(session_id=session_id, sql=SLOW, headers={"Prefer": "respond-async"})
    statement_id = response.json()["statement_id"]
    cancelled = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/cancel", headers=chargewire.headers())
    assert cancelled.status_code == 202, cancelled.text
    assert statement_id in cancelled.json()["cancelled"]
    doc = chargewire.wait_done(session_id=session_id, statement_id=statement_id, timeout=30)
    assert doc["state"] == "cancelled" and doc["cancel_reason"] == "client", doc
    again = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/cancel", headers=chargewire.headers())
    assert again.status_code == 202 and again.json()["cancelled"] == []
