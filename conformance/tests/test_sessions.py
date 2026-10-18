"""Spec §5: sessions, options, transactions, and handles bound to their principal."""

from test_discovery_auth import assert_problem


def test_create_and_read_a_session(warp):
    response = warp.http.post(url="/warp/v1/sessions", headers=warp.headers(), json={})
    assert response.status_code == 201, response.text
    doc = response.json()
    assert doc["session_id"]
    assert isinstance(doc["options"], dict)
    assert doc["idle_timeout_s"] > 0
    assert response.headers["location"].endswith(doc["session_id"])

    again = warp.http.get(url=warp.session_url(doc["session_id"]), headers=warp.headers())
    assert again.status_code == 200
    assert again.json()["session_id"] == doc["session_id"]


def test_unknown_session_is_not_found(warp):
    response = warp.http.get(url=warp.session_url("s_doesnotexist"), headers=warp.headers())
    assert_problem(response=response, status=404, code="session_not_found")


def test_closed_session_is_never_recreated(warp):
    session_id = warp.new_session()
    assert warp.http.delete(url=warp.session_url(session_id), headers=warp.headers()).status_code == 204
    response = warp.submit(session_id=session_id, sql="SELECT 1")
    assert_problem(response=response, status=404, code="session_not_found")
    response = warp.http.get(url=warp.session_url(session_id), headers=warp.headers())
    assert_problem(response=response, status=404, code="session_not_found")


def test_session_state_lives_across_statements(warp, session_id):
    doc = warp.execute(session_id=session_id, sql="CREATE TEMPORARY TABLE scratch AS SELECT 7 AS v")
    assert doc["state"] == "succeeded", doc
    assert warp.query(session_id=session_id, sql="SELECT v FROM scratch").column("v").to_pylist() == [7]
    other = warp.new_session()
    response = warp.submit(session_id=other, sql="SELECT v FROM scratch")
    assert_problem(response=response, status=400, code="sql_error")


def test_unknown_option_is_reported_per_option(warp, session_id):
    response = warp.http.patch(url=warp.session_url(session_id), headers=warp.headers(),
                               json={"options": {"no_such_option": 1}})
    assert response.status_code == 200, response.text
    assert response.json()["errors"]["no_such_option"]["code"] == "unknown_option"


def test_autocommit_off_then_rollback(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TABLE tx_rollback (v INTEGER)")
    response = warp.http.patch(url=warp.session_url(session_id), headers=warp.headers(),
                               json={"options": {"autocommit": False}})
    assert response.status_code == 200, response.text
    assert response.json()["options"]["autocommit"] is False
    warp.execute(session_id=session_id, sql="INSERT INTO tx_rollback VALUES (1)")
    assert warp.http.post(url=f"{warp.session_url(session_id)}/rollback",
                          headers=warp.headers()).status_code == 204
    assert warp.query(session_id=session_id,
                      sql="SELECT count(*) AS n FROM tx_rollback").column("n").to_pylist() == [0]


def test_autocommit_off_then_commit(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE TABLE tx_commit (v INTEGER)")
    warp.http.patch(url=warp.session_url(session_id), headers=warp.headers(),
                    json={"options": {"autocommit": False}})
    warp.execute(session_id=session_id, sql="INSERT INTO tx_commit VALUES (1)")
    assert warp.http.post(url=f"{warp.session_url(session_id)}/commit",
                          headers=warp.headers()).status_code == 204
    other = warp.new_session()
    assert warp.query(session_id=other,
                      sql="SELECT count(*) AS n FROM tx_commit").column("n").to_pylist() == [1]


def test_statement_ids_are_not_shared_between_sessions(warp, session_id):
    doc = warp.execute(session_id=session_id, sql="SELECT 1")
    other = warp.new_session()
    response = warp.http.get(url=warp.statement_url(other, doc["statement_id"]),
                             headers=warp.headers())
    assert_problem(response=response, status=404, code="statement_not_found")


def test_session_document_follows_sql_use(warp, session_id):
    warp.execute(session_id=session_id, sql="CREATE SCHEMA IF NOT EXISTS moved_here")
    before = warp.http.get(url=warp.session_url(session_id), headers=warp.headers()).json()["options"]
    assert before["schema"] != "moved_here"
    warp.execute(session_id=session_id, sql="USE moved_here")
    after = warp.http.get(url=warp.session_url(session_id), headers=warp.headers()).json()["options"]
    assert after["schema"] == "moved_here"
    assert after["catalog"] == before["catalog"]


def test_session_create_reports_the_option_failure_by_kind(warp):
    unknown = warp.http.post(url="/warp/v1/sessions", headers=warp.headers(),
                             json={"options": {"no_such_option": 1}})
    assert_problem(response=unknown, status=400, code="unknown_option")
    invalid = warp.http.post(url="/warp/v1/sessions", headers=warp.headers(),
                             json={"options": {"autocommit": "maybe"}})
    assert_problem(response=invalid, status=400, code="invalid_option_value")


# ---- Sessions that no longer exist (spec §5.6), each from a server of the test's own ----

import os
import time

import pytest

from conftest import start_server, stop_server
from test_discovery_auth import assert_problem
from warp import Warp


def own_server(*args: str):
    binary = os.environ.get("WARP_SQL_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="these tests start a server of their own: set WARP_SQL_TEST_SERVER_BINARY")
    return start_server(binary=binary, extra_args=list(args))


def test_an_idle_session_is_not_found_and_never_recreated():
    flag = os.environ.get("WARP_SQL_TEST_SERVER_IDLE_FLAG", "--session-idle-timeout-seconds")
    server = own_server(flag, "1")
    try:
        warp = Warp(base_url=server.base_url, username=server.username, password=server.password)
        sid = warp.new_session()
        # Eviction is the server's to schedule (§5.7: an idle session MAY be evicted), and
        # every probe is a request that keeps the session alive. Probing at a fixed short
        # interval only finds it gone if the server's sweep falls between two probes, which
        # a server sweeping every few seconds may not do for a long time. So the silences
        # double (2, 4, 8, 16 s): one soon outlasts the timeout plus any sweep interval.
        gaps = [2, 4, 8, 16]
        silences = []
        while True:
            response = warp.submit(session_id=sid, sql="SELECT 1", arrow=True)
            if response.status_code == 404:
                break
            assert response.status_code == 200, response.text
            assert gaps, f"the idle session was never evicted (silent for {silences} s in turn)"
            silences.append(gaps.pop(0))
            time.sleep(silences[-1])
        assert_problem(response=response, status=404, code="session_not_found")
        assert_problem(response=warp.http.get(url=warp.session_url(sid), headers=warp.headers()),
                       status=404, code="session_not_found")
        warp.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


def tampered(token: str) -> str:
    """The token with one character changed, well inside it (the last base64url
    character of a signature can carry only padding bits)."""
    at = len(token) - 10
    return token[:at] + ("B" if token[at] == "A" else "A") + token[at + 1:]


def test_an_expired_token_is_refused_until_the_client_signs_in_again():
    flag = os.environ.get("WARP_SQL_TEST_SERVER_TOKEN_LIFETIME_FLAG", "--token-lifetime-seconds")
    server = own_server(flag, "2")
    try:
        warp = Warp(base_url=server.base_url, username=server.username, password=server.password)
        sid = warp.new_session()
        time.sleep(3)
        # §4.2: the server's own token, expired, says why; the session itself still stands.
        response = warp.submit(session_id=sid, sql="SELECT 1", arrow=True)
        problem = assert_problem(response=response, status=401, code="invalid_token")
        assert problem.get("reason") == "token_expired", problem
        assert response.headers.get("www-authenticate") == "Bearer"
        no_session = warp.http.post(url="/warp/v1/sessions", headers=warp.headers(), json={})
        assert assert_problem(response=no_session, status=401, code="invalid_token").get("reason") == "token_expired"
        # The same token altered is not one the server can tell is its own: no reason,
        # so nothing about its principal reaches someone who doesn't hold the real one.
        forged = warp.http.get(url=warp.session_url(sid), headers={"Authorization": f"Bearer {tampered(warp.token)}"})
        assert "reason" not in assert_problem(response=forged, status=401, code="invalid_token"), forged.text
        warp.token = warp.login()
        assert warp.http.post(url="/warp/v1/sessions", headers=warp.headers(), json={}).status_code == 201
        assert warp.submit(session_id=sid, sql="SELECT 1", arrow=True).status_code == 200
        warp.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()
