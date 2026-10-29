"""Spec §5: sessions, options, transactions, and handles bound to their principal."""

from test_discovery_auth import assert_problem


def test_create_and_read_a_session(chargewire):
    response = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(), json={})
    assert response.status_code == 201, response.text
    doc = response.json()
    assert doc["session_id"]
    assert isinstance(doc["options"], dict)
    assert doc["idle_timeout_s"] > 0
    assert response.headers["location"].endswith(doc["session_id"])

    again = chargewire.http.get(url=chargewire.session_url(doc["session_id"]), headers=chargewire.headers())
    assert again.status_code == 200
    assert again.json()["session_id"] == doc["session_id"]


def test_unknown_session_is_not_found(chargewire):
    response = chargewire.http.get(url=chargewire.session_url("s_doesnotexist"), headers=chargewire.headers())
    assert_problem(response=response, status=404, code="session_not_found")


def test_a_new_sign_in_of_the_same_principal_reaches_its_session(chargewire):
    """§9.2: a principal is its identity and its authority, and what changes with every
    sign-in (the token itself) is neither. A server that binds sessions to a token's
    authority must still let the same user's next token, with the same authority, reach
    them. (A token of the same name with another authority getting session_not_found is
    a server's own matter: the protocol has no generic way to ask for one.)"""
    session_id = chargewire.new_session()
    first = chargewire.token
    chargewire.token = chargewire.login()
    assert chargewire.token != first
    response = chargewire.submit(session_id=session_id, sql="SELECT 1", arrow=True)
    assert response.status_code == 200, response.text
    assert chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers()).status_code == 200
    assert chargewire.http.delete(url=chargewire.session_url(session_id), headers=chargewire.headers()).status_code == 204


def test_closed_session_is_never_recreated(chargewire):
    session_id = chargewire.new_session()
    assert chargewire.http.delete(url=chargewire.session_url(session_id), headers=chargewire.headers()).status_code == 204
    response = chargewire.submit(session_id=session_id, sql="SELECT 1")
    assert_problem(response=response, status=404, code="session_not_found")
    response = chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers())
    assert_problem(response=response, status=404, code="session_not_found")


def test_session_state_lives_across_statements(chargewire, session_id):
    doc = chargewire.execute(session_id=session_id, sql="CREATE TEMPORARY TABLE scratch AS SELECT 7 AS v")
    assert doc["state"] == "succeeded", doc
    assert chargewire.query(session_id=session_id, sql="SELECT v FROM scratch").column("v").to_pylist() == [7]
    other = chargewire.new_session()
    response = chargewire.submit(session_id=other, sql="SELECT v FROM scratch")
    assert_problem(response=response, status=400, code="sql_error")


def test_unknown_option_is_reported_per_option(chargewire, session_id):
    response = chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                               json={"options": {"no_such_option": 1}})
    assert response.status_code == 200, response.text
    assert response.json()["errors"]["no_such_option"]["code"] == "unknown_option"


def test_patch_answers_every_option_and_its_errors(chargewire, session_id):
    # §5.3: 200, every option the session has (not only the ones set), and errors ({} when
    # none); a bad option doesn't stop the others and keeps its earlier value.
    current = chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers()).json()["options"]
    response = chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                               json={"options": {"schema": current["schema"]}})
    assert response.status_code == 200, response.text
    doc = response.json()
    assert doc["errors"] == {}, doc
    assert set(doc["options"]) == set(current), (doc, current)
    chargewire.execute(session_id=session_id, sql="CREATE SCHEMA IF NOT EXISTS patched_into")
    mixed = chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                            json={"options": {"schema": "patched_into", "autocommit": "maybe"}})
    assert mixed.status_code == 200, mixed.text
    doc = mixed.json()
    assert set(doc["errors"]) == {"autocommit"}, doc
    assert doc["errors"]["autocommit"]["code"] == "invalid_option_value" and doc["errors"]["autocommit"]["detail"]
    assert doc["options"]["schema"] == "patched_into" and doc["options"]["autocommit"] == current["autocommit"]
    assert doc["options"] == chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers()).json()["options"]


def test_autocommit_off_then_rollback(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE tx_rollback (v INTEGER)")
    response = chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                               json={"options": {"autocommit": False}})
    assert response.status_code == 200, response.text
    assert response.json()["options"]["autocommit"] is False
    chargewire.execute(session_id=session_id, sql="INSERT INTO tx_rollback VALUES (1)")
    assert chargewire.http.post(url=f"{chargewire.session_url(session_id)}/rollback",
                          headers=chargewire.headers()).status_code == 204
    assert chargewire.query(session_id=session_id,
                      sql="SELECT count(*) AS n FROM tx_rollback").column("n").to_pylist() == [0]


def test_autocommit_off_then_commit(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE tx_commit (v INTEGER)")
    chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                    json={"options": {"autocommit": False}})
    chargewire.execute(session_id=session_id, sql="INSERT INTO tx_commit VALUES (1)")
    assert chargewire.http.post(url=f"{chargewire.session_url(session_id)}/commit",
                          headers=chargewire.headers()).status_code == 204
    other = chargewire.new_session()
    assert chargewire.query(session_id=other,
                      sql="SELECT count(*) AS n FROM tx_commit").column("n").to_pylist() == [1]


def test_statement_ids_are_not_shared_between_sessions(chargewire, session_id):
    doc = chargewire.execute(session_id=session_id, sql="SELECT 1")
    other = chargewire.new_session()
    response = chargewire.http.get(url=chargewire.statement_url(other, doc["statement_id"]),
                             headers=chargewire.headers())
    assert_problem(response=response, status=404, code="statement_not_found")


def test_session_document_follows_sql_use(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE SCHEMA IF NOT EXISTS moved_here")
    before = chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers()).json()["options"]
    assert before["schema"] != "moved_here"
    chargewire.execute(session_id=session_id, sql="USE moved_here")
    after = chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers()).json()["options"]
    assert after["schema"] == "moved_here"
    assert after["catalog"] == before["catalog"]


def test_session_create_reports_the_option_failure_by_kind(chargewire):
    unknown = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                             json={"options": {"no_such_option": 1}})
    problem = assert_problem(response=unknown, status=400, code="unknown_option")
    assert "no_such_option" in problem["detail"], problem  # §5.1: the detail names the option
    invalid = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                             json={"options": {"autocommit": "maybe"}})
    problem = assert_problem(response=invalid, status=400, code="invalid_option_value")
    assert "autocommit" in problem["detail"], problem


# ---- Sessions that no longer exist (spec §5.6), each from a server of the test's own ----

import os
import time

import pytest

from conftest import start_server, stop_server
from test_discovery_auth import assert_problem
from chargewire import Chargewire


def own_server(*args: str):
    binary = os.environ.get("CHARGEWIRE_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="these tests start a server of their own: set CHARGEWIRE_TEST_SERVER_BINARY")
    return start_server(binary=binary, extra_args=list(args))


def test_an_idle_session_is_not_found_and_never_recreated():
    flag = os.environ.get("CHARGEWIRE_TEST_SERVER_IDLE_FLAG", "--session-idle-timeout-seconds")
    server = own_server(flag, "1")
    try:
        chargewire = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
        sid = chargewire.new_session()
        # Eviction is the server's to schedule (§5.7: an idle session MAY be evicted), and
        # every probe is a request that keeps the session alive. Probing at a fixed short
        # interval only finds it gone if the server's sweep falls between two probes, which
        # a server sweeping every few seconds may not do for a long time. So the silences
        # double (2, 4, 8, 16 s): one soon outlasts the timeout plus any sweep interval.
        gaps = [2, 4, 8, 16]
        silences = []
        while True:
            response = chargewire.submit(session_id=sid, sql="SELECT 1", arrow=True)
            if response.status_code == 404:
                break
            assert response.status_code == 200, response.text
            assert gaps, f"the idle session was never evicted (silent for {silences} s in turn)"
            silences.append(gaps.pop(0))
            time.sleep(silences[-1])
        assert_problem(response=response, status=404, code="session_not_found")
        assert_problem(response=chargewire.http.get(url=chargewire.session_url(sid), headers=chargewire.headers()),
                       status=404, code="session_not_found")
        chargewire.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


def tampered(token: str) -> str:
    """The token with one character changed, well inside it (the last base64url
    character of a signature can carry only padding bits)."""
    at = len(token) - 10
    return token[:at] + ("B" if token[at] == "A" else "A") + token[at + 1:]


def test_an_expired_token_is_refused_until_the_client_signs_in_again():
    flag = os.environ.get("CHARGEWIRE_TEST_SERVER_TOKEN_LIFETIME_FLAG", "--token-lifetime-seconds")
    server = own_server(flag, "2")
    try:
        chargewire = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
        sid = chargewire.new_session()
        time.sleep(3)
        # §4.2: the server's own token, expired, says why; the session itself still stands.
        response = chargewire.submit(session_id=sid, sql="SELECT 1", arrow=True)
        problem = assert_problem(response=response, status=401, code="invalid_token")
        assert problem.get("reason") == "token_expired", problem
        assert response.headers.get("www-authenticate") == "Bearer"
        no_session = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(), json={})
        assert assert_problem(response=no_session, status=401, code="invalid_token").get("reason") == "token_expired"
        # The same token altered is not one the server can tell is its own: no reason,
        # so nothing about its principal reaches someone who doesn't hold the real one.
        forged = chargewire.http.get(url=chargewire.session_url(sid), headers={"Authorization": f"Bearer {tampered(chargewire.token)}"})
        assert "reason" not in assert_problem(response=forged, status=401, code="invalid_token"), forged.text
        chargewire.token = chargewire.login()
        assert chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(), json={}).status_code == 201
        assert chargewire.submit(session_id=sid, sql="SELECT 1", arrow=True).status_code == 200
        chargewire.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


# ---- §5.2: the session's transaction ----------------------------------------------------

def transaction_of(chargewire, session_id: str) -> str:
    response = chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers())
    assert response.status_code == 200, response.text
    return response.json()["transaction"]


def outcome(chargewire, session_id: str, sql: str) -> str:
    """The statement's end: "succeeded", or "failed" (a failure may come back as the submit's
    own problem document, or as a failed statement)."""
    response = chargewire.submit(session_id=session_id, sql=sql)
    if response.status_code >= 400:
        # The submit's own error: the engine's refusal, with its code's status (§11).
        expected = {"sql_error": 400, "execution_error": 422}
        problem = response.json()
        assert problem.get("code") in expected, response.text
        assert_problem(response=response, status=expected[problem["code"]], code=problem["code"])
        return "failed"
    doc = response.json()
    if doc["state"] not in ("succeeded", "failed", "cancelled"):
        doc = chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
    return doc["state"]


def refused_as_aborted(chargewire, session_id: str) -> bool:
    """Whether the engine refuses the next statement because the transaction is aborted."""
    return outcome(chargewire=chargewire, session_id=session_id, sql="SELECT 1") == "failed"


def test_a_new_session_has_no_transaction(chargewire):
    response = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(), json={})
    assert response.status_code == 201, response.text
    assert response.json()["transaction"] == "none"


def test_a_session_opened_with_autocommit_off_is_in_a_transaction(chargewire):
    response = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                              json={"options": {"autocommit": False}})
    assert response.status_code == 201, response.text
    assert response.json()["transaction"] == "open"


def test_sql_begin_opens_and_commit_ends_a_transaction(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE tx_sql_commit (v INTEGER)")
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"
    assert chargewire.execute(session_id=session_id, sql="BEGIN")["state"] == "succeeded"
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "open"
    chargewire.execute(session_id=session_id, sql="INSERT INTO tx_sql_commit VALUES (1)")
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "open"
    assert chargewire.execute(session_id=session_id, sql="COMMIT")["state"] == "succeeded"
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"


def test_every_spelling_of_begin_and_end_is_followed(chargewire, session_id):
    for begin, end in [("START TRANSACTION", "END"), ("BEGIN TRANSACTION", "ROLLBACK"),
                       ("/* a comment first */ BEGIN", "ABORT"), ("begin", "commit")]:
        assert chargewire.execute(session_id=session_id, sql=begin)["state"] == "succeeded", begin
        assert transaction_of(chargewire=chargewire, session_id=session_id) == "open", begin
        assert chargewire.execute(session_id=session_id, sql=end)["state"] == "succeeded", end
        assert transaction_of(chargewire=chargewire, session_id=session_id) == "none", end


def test_a_transaction_opened_in_a_multi_statement_request_is_open(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE tx_multi (v INTEGER)")
    doc = chargewire.execute(session_id=session_id, sql="BEGIN; INSERT INTO tx_multi VALUES (1)")
    assert doc["state"] == "succeeded", doc
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "open"
    chargewire.execute(session_id=session_id, sql="ROLLBACK")
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"


def test_a_statement_that_aborts_the_transaction_says_so_until_rollback(chargewire, session_id):
    chargewire.execute(session_id=session_id, sql="CREATE TABLE tx_abort (v INTEGER PRIMARY KEY)")
    chargewire.execute(session_id=session_id, sql="BEGIN")
    assert outcome(chargewire=chargewire, session_id=session_id, sql="INSERT INTO tx_abort VALUES (1), (1)") == "failed"
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "aborted"
    assert refused_as_aborted(chargewire=chargewire, session_id=session_id)
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "aborted"
    assert chargewire.execute(session_id=session_id, sql="ROLLBACK")["state"] == "succeeded"
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"
    assert not refused_as_aborted(chargewire=chargewire, session_id=session_id)


def test_the_transaction_reported_after_a_failure_is_the_one_the_engine_has(chargewire, session_id):
    # Engines differ in which errors abort a transaction (DuckDB keeps it usable after a
    # syntax error, not after a missing table): "aborted" must mean the next statement is
    # refused, and "open" that it runs.
    for bad in ["SELEC 1", "SELECT * FROM tx_no_such_table", "SELECT 'x'::INTEGER"]:
        chargewire.execute(session_id=session_id, sql="BEGIN")
        assert outcome(chargewire=chargewire, session_id=session_id, sql=bad) == "failed", bad
        reported = transaction_of(chargewire=chargewire, session_id=session_id)
        assert reported in ("open", "aborted"), (bad, reported)
        assert refused_as_aborted(chargewire=chargewire, session_id=session_id) == (reported == "aborted"), (bad, reported)
        chargewire.execute(session_id=session_id, sql="ROLLBACK")
        assert transaction_of(chargewire=chargewire, session_id=session_id) == "none", bad


def test_a_failure_outside_a_transaction_leaves_none(chargewire, session_id):
    assert outcome(chargewire=chargewire, session_id=session_id, sql="SELECT * FROM tx_missing") == "failed"
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"
    # ROLLBACK with nothing to roll back fails and changes nothing.
    outcome(chargewire=chargewire, session_id=session_id, sql="ROLLBACK")
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"


def test_a_cancelled_statement_reports_what_it_left(chargewire, session_id):
    import time

    from test_control import SLOW

    chargewire.execute(session_id=session_id, sql="BEGIN")
    response = chargewire.submit(session_id=session_id, sql=SLOW, headers={"Prefer": "respond-async"})
    statement_id = response.json()["statement_id"]
    time.sleep(0.5)
    chargewire.http.post(url=f"{chargewire.statement_url(session_id, statement_id)}/cancel", headers=chargewire.headers())
    doc = chargewire.wait_done(session_id=session_id, statement_id=statement_id, timeout=20)
    assert doc["state"] == "cancelled", doc
    reported = transaction_of(chargewire=chargewire, session_id=session_id)
    assert reported in ("open", "aborted"), reported
    assert refused_as_aborted(chargewire=chargewire, session_id=session_id) == (reported == "aborted"), reported
    chargewire.execute(session_id=session_id, sql="ROLLBACK")
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"


def test_autocommit_off_keeps_a_transaction_open_across_commits(chargewire, session_id):
    chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                    json={"options": {"autocommit": False}})
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "open"
    assert chargewire.http.post(url=f"{chargewire.session_url(session_id)}/commit",
                          headers=chargewire.headers()).status_code == 204
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "open"
    # A SQL COMMIT ends it the same way: the next statement is in a new one.
    assert chargewire.execute(session_id=session_id, sql="COMMIT")["state"] == "succeeded"
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "open"
    assert outcome(chargewire=chargewire, session_id=session_id, sql="SELECT * FROM tx_missing_too") == "failed"
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "aborted"
    assert chargewire.http.post(url=f"{chargewire.session_url(session_id)}/rollback",
                          headers=chargewire.headers()).status_code == 204
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "open"
    chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                    json={"options": {"autocommit": True}})
    assert transaction_of(chargewire=chargewire, session_id=session_id) == "none"
