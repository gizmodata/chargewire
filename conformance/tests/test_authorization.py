"""Spec §9.4 (metadata follows authorization) and §7.2 (a prepared statement is authorized
at every execution, not once at prepare time).

These need a server with SQL privileges and a way to create a second principal, which the
protocol leaves to the engine. They start a server of their own (WARP_SQL_TEST_SERVER_BINARY)
and are told how through:

    WARP_SQL_TEST_AUTHZ_SERVER_ENV   NAME=VALUE pairs (space-separated) for that server;
                                     {tmp} is a fresh directory
    WARP_SQL_TEST_AUTHZ_CREATE_USER  SQL creating {user} with password {password}
    WARP_SQL_TEST_AUTHZ_GRANT        SQL letting {user} SELECT from table {table}
    WARP_SQL_TEST_AUTHZ_REVOKE       SQL taking that back

Each may be several statements separated by `;`, run one by one. Hardcharger, for example
(privileges go to roles):

    WARP_SQL_TEST_AUTHZ_SERVER_ENV='HARDCHARGER_RBAC_STORE_URI=duckdb:{tmp}/rbac.duckdb HARDCHARGER_SECRET_KEY=...'
    WARP_SQL_TEST_AUTHZ_CREATE_USER="CREATE ROLE {user}_role; CREATE USER {user} WITH PASSWORD '{password}'; GRANT {user}_role TO {user}"
    WARP_SQL_TEST_AUTHZ_GRANT='GRANT SELECT ON {table} TO {user}_role'
    WARP_SQL_TEST_AUTHZ_REVOKE='REVOKE SELECT ON {table} FROM {user}_role'

A server without privileges (warp-duckdb: whoever signs in may do everything) has nothing
to hide, so these MUSTs hold for it trivially: without the variables the tests skip,
saying so. Set all four or none.
"""

import os
import secrets
import tempfile

import pytest

from conftest import start_server, stop_server
from test_discovery_auth import assert_problem
from warp import ARROW_STREAM, Warp, read_table

pytestmark = pytest.mark.own_server

NAMES = ("WARP_SQL_TEST_AUTHZ_SERVER_ENV", "WARP_SQL_TEST_AUTHZ_CREATE_USER", "WARP_SQL_TEST_AUTHZ_GRANT",
         "WARP_SQL_TEST_AUTHZ_REVOKE")


@pytest.fixture(scope="module")
def authz():
    given = {name: os.environ.get(name) for name in NAMES}
    if not any(given.values()):
        pytest.skip(reason="no SQL privileges to test: set WARP_SQL_TEST_AUTHZ_* for a server that has them")
    missing = [name for name, value in given.items() if not value]
    if missing:
        pytest.fail(reason=f"set all of {', '.join(NAMES)} (missing: {', '.join(missing)})")
    binary = os.environ.get("WARP_SQL_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="the authorization tests start their own server: set WARP_SQL_TEST_SERVER_BINARY")
    tmp = tempfile.mkdtemp(prefix="warp-sql-authz-")
    env = dict(pair.split("=", 1) for pair in given["WARP_SQL_TEST_AUTHZ_SERVER_ENV"].replace("{tmp}", tmp).split())
    saved = {name: os.environ.get(name) for name in env}
    os.environ.update(env)
    try:
        server = start_server(binary=binary)
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    admin = Warp(base_url=server.base_url, username=server.username, password=server.password)
    session_id = admin.new_session()
    user, password = "conformance_reader", "Pw-" + secrets.token_urlsafe(nbytes=12)
    run_all(admin=admin, session_id=session_id, sql=";".join([
        "CREATE SCHEMA authz_s",
        "CREATE TABLE authz_s.visible (id INTEGER, note VARCHAR)",
        "CREATE TABLE authz_s.hidden (id INTEGER, secret VARCHAR)",
        "INSERT INTO authz_s.visible VALUES (1, 'shown')",
        "INSERT INTO authz_s.hidden VALUES (1, 'top secret')",
        given["WARP_SQL_TEST_AUTHZ_CREATE_USER"].format(user=user, password=password),
        given["WARP_SQL_TEST_AUTHZ_GRANT"].format(table="authz_s.visible", user=user),
    ]))
    yield {"server": server, "admin": admin, "admin_session": session_id, "user": user, "password": password,
           "revoke": given["WARP_SQL_TEST_AUTHZ_REVOKE"], "grant": given["WARP_SQL_TEST_AUTHZ_GRANT"]}
    admin.close()
    assert stop_server(server=server) == 0, server.log_tail()


def run_all(admin: Warp, session_id: str, sql: str) -> None:
    for statement in (part.strip() for part in sql.split(";")):
        if statement:
            doc = admin.execute(session_id=session_id, sql=statement)
            assert doc["state"] == "succeeded", (statement, doc)


def reader(authz) -> Warp:
    return Warp(base_url=authz["server"].base_url, username=authz["user"], password=authz["password"])


def table_names(warp: Warp, session_id: str) -> set[str]:
    response = warp.http.get(url=f"{warp.session_url(session_id)}/metadata/tables", headers=warp.headers(),
                             params={"db_schema_filter_pattern": "authz_s"})
    assert response.status_code == 200, response.text
    return set(read_table(content=response.content).column("table_name").to_pylist())


def test_metadata_lists_only_what_the_principal_may_see(authz):
    user = reader(authz=authz)
    session_id = user.new_session()
    assert table_names(warp=user, session_id=session_id) == {"visible"}
    # Nor do its columns or the table's schema show the hidden table.
    response = user.http.get(url=f"{user.session_url(session_id)}/metadata/tables", headers=user.headers(),
                             params={"db_schema_filter_pattern": "authz_s", "include_schema": "true"})
    assert response.status_code == 200, response.text
    assert read_table(content=response.content).column("table_name").to_pylist() == ["visible"]
    # The twin: the administrator sees both.
    admin = authz["admin"]
    assert table_names(warp=admin, session_id=authz["admin_session"]) == {"visible", "hidden"}
    # And reading what it may not see is refused, by the same rules as SQL.
    refused = user.submit(session_id=session_id, sql="SELECT * FROM authz_s.hidden")
    assert_problem(response=refused, status=403, code="permission_denied")
    user.close()


def test_a_prepared_statement_is_authorized_at_every_execution(authz):
    user = reader(authz=authz)
    session_id = user.new_session()
    prepared = user.http.post(url=f"{user.session_url(session_id)}/prepared", headers=user.headers(),
                              json={"sql": "SELECT note FROM authz_s.visible"})
    assert prepared.status_code == 201, prepared.text
    execute = f"{user.session_url(session_id)}/prepared/{prepared.json()['prepared_id']}/execute"
    allowed = user.http.post(url=execute, headers=user.headers(Accept=ARROW_STREAM), json={})
    assert allowed.status_code == 200, allowed.text
    assert read_table(content=allowed.content).to_pylist() == [{"note": "shown"}]
    admin = authz["admin"]
    run_all(admin=admin, session_id=authz["admin_session"],
            sql=authz["revoke"].format(table="authz_s.visible", user=authz["user"]))
    try:
        refused = user.http.post(url=execute, headers=user.headers(Accept=ARROW_STREAM), json={})
        assert_problem(response=refused, status=403, code="permission_denied")
    finally:
        run_all(admin=admin, session_id=authz["admin_session"],
                sql=authz["grant"].format(table="authz_s.visible", user=authz["user"]))
    # Granted again, the same prepared statement runs again.
    again = user.http.post(url=execute, headers=user.headers(Accept=ARROW_STREAM), json={})
    assert again.status_code == 200, again.text
    user.close()
