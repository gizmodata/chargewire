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
