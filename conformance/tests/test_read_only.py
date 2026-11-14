"""Spec §5.8: read-only sessions (capability `read_only_sessions`).

A session whose `read_only` option is true changes nothing, whatever is sent through it,
and nothing sent through it makes it read-write again. Every refusal is `403 read_only`,
and every refused write has a twin here that works in a read-write session, so a refusal
is never a statement that would have failed anyway.

A server without the capability must say so (`unknown_option`), which the first tests
check; the rest skip on such a server, saying so."""

import os
import time
import uuid

import pytest

from conftest import start_server, stop_server
from test_discovery_auth import assert_problem
from test_ingest import ingest, ipc_bytes
from chargewire import Chargewire

import pyarrow as pa


def capabilities(chargewire) -> list[str]:
    return chargewire.http.get(url="/chargewire").json()["capabilities"]


def document(chargewire, session_id: str) -> dict:
    response = chargewire.http.get(url=chargewire.session_url(session_id), headers=chargewire.headers())
    assert response.status_code == 200, response.text
    return response.json()


def patch(chargewire, session_id: str, options: dict) -> dict:
    response = chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                                     json={"options": options})
    assert response.status_code == 200, response.text
    return response.json()


def refused(chargewire, session_id: str, sql: str, parameters=None) -> dict:
    """Runs `sql` and asserts it is refused with 403 read_only; returns the problem."""
    response = chargewire.submit(session_id=session_id, sql=sql, parameters=parameters)
    problem = assert_problem(response=response, status=403, code="read_only")
    assert problem.get("sqlstate") == "25006", problem  # read-only SQL transaction
    return problem


def rows(chargewire, session_id: str, sql: str, parameters=None) -> list[dict]:
    return chargewire.query(session_id=session_id, sql=sql, parameters=parameters).to_pylist()


def succeeded(chargewire, session_id: str, sql: str) -> dict:
    doc = chargewire.execute(session_id=session_id, sql=sql)
    assert doc["state"] == "succeeded", doc
    return doc


def name(prefix: str = "ro") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def no_read_only_sessions() -> None:
    """A server without the capability: its tests skip, saying so, unless the run is of a
    server that must have it (CHARGEWIRE_TEST_REQUIRE_READ_ONLY=1), where that is a failure."""
    reason = "the server has no read-only sessions (capability read_only_sessions)"
    if os.environ.get("CHARGEWIRE_TEST_REQUIRE_READ_ONLY") == "1":
        pytest.fail(reason=reason + ", and CHARGEWIRE_TEST_REQUIRE_READ_ONLY=1 says it must", pytrace=False)
    pytest.skip(reason=reason)


@pytest.fixture
def offered(chargewire):
    if "read_only_sessions" not in capabilities(chargewire):
        no_read_only_sessions()


@pytest.fixture
def rw(chargewire):
    sid = chargewire.new_session()
    yield sid
    chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


@pytest.fixture
def ro(chargewire, offered):
    sid = chargewire.new_session(options={"read_only": True})
    yield sid
    chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


# ---- A server says whether it has the option, and never accepts it without having it ----


def test_the_capability_and_the_session_document_agree(chargewire, rw):
    # §5.8: a server with read-only sessions lists the capability and shows `read_only` in
    # every session's options; one without does neither.
    has = "read_only_sessions" in capabilities(chargewire)
    options = document(chargewire, rw)["options"]
    assert ("read_only" in options) == has, options
    if has:
        assert options["read_only"] is False


def test_a_server_without_read_only_sessions_refuses_the_option(chargewire, rw):
    # The client rule of §5.8 rests on this: asking a server that has no read-only sessions
    # is an error, never a session that silently writes.
    if "read_only_sessions" in capabilities(chargewire):
        return
    created = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                                   json={"options": {"read_only": True}})
    problem = assert_problem(response=created, status=400, code="unknown_option")
    assert "read_only" in problem["detail"], problem
    answer = patch(chargewire, rw, {"read_only": True})
    assert answer["errors"]["read_only"]["code"] == "unknown_option", answer
    assert "read_only" not in answer["options"]


# ---- The option ----


def test_a_session_is_created_read_only(chargewire, offered):
    response = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                                    json={"options": {"read_only": True}})
    assert response.status_code == 201, response.text
    sid = response.json()["session_id"]
    assert response.json()["options"]["read_only"] is True
    assert document(chargewire, sid)["options"]["read_only"] is True
    answer = patch(chargewire, sid, {})
    assert answer["options"]["read_only"] is True and answer["errors"] == {}, answer
    refused(chargewire, sid, f"CREATE TABLE {name()} (i INTEGER)")


def test_read_only_false_at_creation_is_a_read_write_session(chargewire, offered):
    sid = chargewire.new_session(options={"read_only": False})
    assert document(chargewire, sid)["options"]["read_only"] is False
    succeeded(chargewire, sid, f"CREATE TABLE {name()} (i INTEGER)")


def test_the_options_request_makes_a_session_read_only(chargewire, offered, rw):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    answer = patch(chargewire, rw, {"read_only": True})
    assert answer["errors"] == {} and answer["options"]["read_only"] is True, answer
    assert document(chargewire, rw)["options"]["read_only"] is True
    refused(chargewire, rw, f"INSERT INTO {table} VALUES (1)")
    # Asking again changes nothing and is no error.
    again = patch(chargewire, rw, {"read_only": True})
    assert again["errors"] == {} and again["options"]["read_only"] is True, again


def test_a_value_that_is_not_a_boolean_is_invalid(chargewire, offered, rw):
    created = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                                   json={"options": {"read_only": "yes"}})
    problem = assert_problem(response=created, status=400, code="invalid_option_value")
    assert "read_only" in problem["detail"], problem
    answer = patch(chargewire, rw, {"read_only": 1})
    assert answer["errors"]["read_only"]["code"] == "invalid_option_value", answer
    assert answer["options"]["read_only"] is False


# ---- One refusal per write path, each with its twin in a read-write session ----

# (what, statements a read-write session runs first, the write, a query whose answer the
# refused write must leave as it was). {t} is a table of the test's own, {n} a free name.
WRITES = [
    ("INSERT", ["CREATE TABLE {t} (i INTEGER)"], "INSERT INTO {t} VALUES (1)", "SELECT count(*) AS n FROM {t}"),
    ("UPDATE", ["CREATE TABLE {t} AS SELECT 1 AS i"], "UPDATE {t} SET i = 2", "SELECT sum(i) AS n FROM {t}"),
    ("DELETE", ["CREATE TABLE {t} AS SELECT 1 AS i"], "DELETE FROM {t}", "SELECT count(*) AS n FROM {t}"),
    ("INSERT ... RETURNING", ["CREATE TABLE {t} (i INTEGER)"], "INSERT INTO {t} VALUES (1) RETURNING i",
     "SELECT count(*) AS n FROM {t}"),
    ("a write inside a CTE", ["CREATE TABLE {t} (i INTEGER)"],
     "WITH written AS (INSERT INTO {t} VALUES (1) RETURNING i) SELECT count(*) AS n FROM written",
     "SELECT count(*) AS n FROM {t}"),
    ("the second statement of a request", ["CREATE TABLE {t} (i INTEGER)"], "SELECT 1; INSERT INTO {t} VALUES (1)",
     "SELECT count(*) AS n FROM {t}"),
    ("CREATE TABLE", [], "CREATE TABLE {n} (i INTEGER)", None),
    ("CREATE TABLE AS", [], "CREATE TABLE {n} AS SELECT 1 AS i", None),
    ("DROP TABLE", ["CREATE TABLE {t} (i INTEGER)"], "DROP TABLE {t}", "SELECT count(*) AS n FROM {t}"),
    ("ALTER TABLE", ["CREATE TABLE {t} (i INTEGER)"], "ALTER TABLE {t} ADD COLUMN j INTEGER", "SELECT * FROM {t}"),
    ("CREATE VIEW", [], "CREATE VIEW {n} AS SELECT 1 AS i", None),
    ("CREATE SCHEMA", [], "CREATE SCHEMA {n}", None),
    # Temporary objects are refused too (§5.8): a read-only session leaves nothing behind.
    ("CREATE TEMPORARY TABLE", [], "CREATE TEMPORARY TABLE {n} (i INTEGER)", None),
]


@pytest.mark.parametrize("what,setup,write,probe", WRITES, ids=[w[0] for w in WRITES])
def test_a_write_is_refused(chargewire, rw, ro, what, setup, write, probe):
    names = {"t": name("ro_t"), "n": name("ro_n")}
    for sql in setup:
        succeeded(chargewire, rw, sql.format(**names))
    before = rows(chargewire, rw, probe.format(**names)) if probe else None
    refused(chargewire, ro, write.format(**names))
    if probe:
        assert rows(chargewire, rw, probe.format(**names)) == before, what
    # The twin: the same statement is a statement that works.
    response = chargewire.submit(session_id=rw, sql=write.format(**names))
    assert response.status_code in (200, 201), response.text
    # And the read-only session reads on.
    assert rows(chargewire, ro, "SELECT 1 AS one") == [{"one": 1}]


def test_explain_analyze_of_a_write_is_refused(chargewire, rw, ro):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    refused(chargewire, ro, f"EXPLAIN ANALYZE INSERT INTO {table} VALUES (1)")
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 0}]
    assert chargewire.submit(session_id=rw, sql=f"EXPLAIN ANALYZE INSERT INTO {table} VALUES (1)").status_code in (200, 201)


def prepare(chargewire, session_id: str, sql: str):
    return chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared", headers=chargewire.headers(),
                                json={"sql": sql})


def execute_prepared(chargewire, session_id: str, prepared_id: str, parameters=None):
    return chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared/{prepared_id}/execute",
                                headers=chargewire.headers(), json={"parameters": parameters or []})


def test_a_prepared_write_is_refused_at_prepare_or_at_execute(chargewire, rw, ro):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    prepared = prepare(chargewire, ro, f"INSERT INTO {table} VALUES (?)")
    if prepared.status_code == 201:  # §7.1: SHOULD refuse here; MUST at the execution
        prepared = execute_prepared(chargewire, ro, prepared.json()["prepared_id"], [1])
    assert_problem(response=prepared, status=403, code="read_only")
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 0}]


def test_a_write_prepared_before_the_session_became_read_only_is_refused(chargewire, offered, rw):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    prepared = prepare(chargewire, rw, f"INSERT INTO {table} VALUES (?)")
    assert prepared.status_code == 201, prepared.text
    prepared_id = prepared.json()["prepared_id"]
    assert execute_prepared(chargewire, rw, prepared_id, [1]).status_code in (200, 201)  # the twin
    assert patch(chargewire, rw, {"read_only": True})["errors"] == {}
    assert_problem(response=execute_prepared(chargewire, rw, prepared_id, [2]), status=403, code="read_only")
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 1}]


@pytest.mark.parametrize("mode", ["create", "append", "replace", "create_append"])
def test_bulk_ingest_is_refused(chargewire, rw, ro, mode):
    if "ingest" not in capabilities(chargewire):
        pytest.skip(reason="the server has no bulk ingest")
    table = name()
    if mode in ("append", "replace"):
        succeeded(chargewire, rw, f"CREATE TABLE {table} (i BIGINT)")
    body = ipc_bytes(table=pa.table({"i": pa.array([1, 2, 3], pa.int64())}))
    assert_problem(response=ingest(chargewire, ro, body, table=table, mode=mode), status=403, code="read_only")
    listed = rows(chargewire, rw, "SELECT count(*) AS n FROM information_schema.tables WHERE table_name = ?", [table])
    assert listed == [{"n": 1 if mode in ("append", "replace") else 0}]
    if mode in ("append", "replace"):
        assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 0}]
    assert ingest(chargewire, rw, body, table=table, mode=mode).status_code == 200  # the twin
    assert_problem(response=ingest(chargewire, ro, body, table=name(), temporary="true"), status=403, code="read_only")


def test_a_partitioned_ingest_is_refused_when_it_is_opened(chargewire, rw, ro):
    if "partitioned_ingest" not in capabilities(chargewire):
        pytest.skip(reason="the server has no partitioned ingest")
    table = name()
    opened = chargewire.http.post(url=f"{chargewire.session_url(ro)}/ingests", headers=chargewire.headers(),
                                  json={"table": table})
    assert_problem(response=opened, status=403, code="read_only")
    twin = chargewire.http.post(url=f"{chargewire.session_url(rw)}/ingests", headers=chargewire.headers(),
                                json={"table": table})
    assert twin.status_code == 201, twin.text
    chargewire.http.delete(url=f"{chargewire.session_url(rw)}/ingests/{twin.json()['ingest_id']}",
                           headers=chargewire.headers())


# ---- One-way: nothing sent through a read-only session makes it read-write ----


def still_read_only(chargewire, session_id: str, table: str) -> None:
    assert document(chargewire, session_id)["options"]["read_only"] is True
    refused(chargewire, session_id, f"INSERT INTO {table} VALUES (1)")


@pytest.fixture
def table(chargewire, rw):
    created = name("ro_door")
    succeeded(chargewire, rw, f"CREATE TABLE {created} (i INTEGER)")
    yield created
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {created}") == [{"n": 0}], "a door was open"


def test_the_options_request_cannot_make_it_read_write(chargewire, ro, table):
    answer = patch(chargewire, ro, {"read_only": False})
    assert answer["errors"]["read_only"]["code"] == "read_only", answer
    assert answer["options"]["read_only"] is True
    # Not next to another option either, and the other option is still applied.
    answer = patch(chargewire, ro, {"read_only": False, "session_tag": {"job": "door"}})
    assert set(answer["errors"]) == {"read_only"}, answer
    assert answer["options"]["session_tag"] == {"job": "door"}
    still_read_only(chargewire, ro, table)


def test_ending_a_transaction_does_not_open_a_read_write_one(chargewire, ro, table):
    # Each of these gets a write out of a read-only transaction a client starts itself on a
    # server that only passes transaction statements on.
    succeeded(chargewire, ro, "BEGIN")
    succeeded(chargewire, ro, "COMMIT")
    still_read_only(chargewire, ro, table)
    succeeded(chargewire, ro, "BEGIN")
    refused(chargewire, ro, f"COMMIT; INSERT INTO {table} VALUES (1)")
    if document(chargewire, ro)["transaction"] != "none":
        succeeded(chargewire, ro, "ROLLBACK")
    succeeded(chargewire, ro, "BEGIN")
    refused(chargewire, ro, f"ROLLBACK; BEGIN; INSERT INTO {table} VALUES (1); COMMIT")
    if document(chargewire, ro)["transaction"] != "none":
        succeeded(chargewire, ro, "ROLLBACK")
    still_read_only(chargewire, ro, table)


def test_a_read_write_transaction_cannot_be_asked_for(chargewire, ro, table):
    refused(chargewire, ro, "BEGIN TRANSACTION READ WRITE")
    refused(chargewire, ro, "START TRANSACTION READ WRITE")
    assert document(chargewire, ro)["transaction"] == "none"
    still_read_only(chargewire, ro, table)


def test_the_commit_and_rollback_calls_start_read_only_transactions(chargewire, ro, table):
    if "transactions" not in capabilities(chargewire):
        pytest.skip(reason="the server has no transactions capability")
    assert patch(chargewire, ro, {"autocommit": False})["errors"] == {}
    for call in ("commit", "rollback", "commit"):
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {table}") == [{"n": 0}]
        response = chargewire.http.post(url=f"{chargewire.session_url(ro)}/{call}", headers=chargewire.headers())
        assert response.status_code == 204, response.text
        assert document(chargewire, ro)["transaction"] == "open"
        refused(chargewire, ro, f"INSERT INTO {table} VALUES (1)")
        # A write the engine refuses may abort the transaction, as any failed statement can.
        chargewire.http.post(url=f"{chargewire.session_url(ro)}/rollback", headers=chargewire.headers())
    assert patch(chargewire, ro, {"autocommit": True})["errors"] == {}
    still_read_only(chargewire, ro, table)


def test_the_session_options_do_not_open_it(chargewire, rw, ro, table):
    schema = name("ro_schema")
    succeeded(chargewire, rw, f"CREATE SCHEMA {schema}")
    assert patch(chargewire, ro, {"schema": schema})["errors"] == {}
    assert document(chargewire, ro)["options"]["schema"] == schema
    refused(chargewire, ro, "CREATE TABLE in_the_other_schema (i INTEGER)")
    catalog = document(chargewire, ro)["options"]["catalog"]
    assert patch(chargewire, ro, {"catalog": catalog, "schema": document(chargewire, rw)["options"]["schema"],
                                  "autocommit": True})["errors"] == {}
    still_read_only(chargewire, ro, table)


def test_an_options_request_waits_for_the_running_statement(chargewire, offered, rw, table):
    # The statement that runs when the request comes ends as it began; every statement
    # after the answer is in a read-only session.
    slow = chargewire.submit(session_id=rw, sql="SELECT sum(i) AS s FROM range(300000000) t(i)",
                             headers={"Prefer": "respond-async"})
    assert slow.status_code == 201, slow.text
    answer = patch(chargewire, rw, {"read_only": True})
    assert answer["errors"] == {} and answer["options"]["read_only"] is True, answer
    done = chargewire.wait_done(session_id=rw, statement_id=slow.json()["statement_id"])
    assert done["state"] == "succeeded", done
    still_read_only(chargewire, rw, table)


# ---- Becoming read-only with a transaction open ----


def test_a_transaction_with_work_in_it_refuses_the_switch_and_is_left_alone(chargewire, offered, rw):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    succeeded(chargewire, rw, "BEGIN")
    succeeded(chargewire, rw, f"INSERT INTO {table} VALUES (1)")
    answer = patch(chargewire, rw, {"read_only": True})
    assert answer["errors"]["read_only"]["code"] == "invalid_option_value", answer
    assert answer["options"]["read_only"] is False
    assert document(chargewire, rw)["transaction"] == "open"
    # The server ended nothing for the client: the work is still its to commit.
    succeeded(chargewire, rw, f"INSERT INTO {table} VALUES (2)")
    succeeded(chargewire, rw, "COMMIT")
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 2}]
    assert patch(chargewire, rw, {"read_only": True})["errors"] == {}
    refused(chargewire, rw, f"INSERT INTO {table} VALUES (3)")


def test_manual_commit_and_read_only_can_be_set_together(chargewire, offered, rw):
    if "transactions" not in capabilities(chargewire):
        pytest.skip(reason="the server has no transactions capability")
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    # In one request, and one after the other: a transaction in which nothing ran holds nothing.
    for first, second in (({"autocommit": False, "read_only": True}, None), ({"autocommit": False}, {"read_only": True})):
        sid = chargewire.new_session()
        assert patch(chargewire, sid, first)["errors"] == {}
        if second:
            assert patch(chargewire, sid, second)["errors"] == {}
        doc = document(chargewire, sid)
        assert doc["options"]["read_only"] is True and doc["options"]["autocommit"] is False, doc
        assert doc["transaction"] == "open"
        assert rows(chargewire, sid, f"SELECT count(*) AS n FROM {table}") == [{"n": 0}]
        refused(chargewire, sid, f"INSERT INTO {table} VALUES (1)")
        chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())
    created = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                                   json={"options": {"autocommit": False, "read_only": True}})
    assert created.status_code == 201, created.text
    assert created.json()["transaction"] == "open" and created.json()["options"]["read_only"] is True
    chargewire.http.delete(url=chargewire.session_url(created.json()["session_id"]), headers=chargewire.headers())


# ---- What a reader keeps ----


def test_reads_work_in_a_read_only_session(chargewire, rw, ro):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} AS SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'b')) t(i, v)")
    assert rows(chargewire, ro, f"SELECT v, count(*) AS n FROM {table} GROUP BY v ORDER BY v") == \
        [{"v": "a", "n": 1}, {"v": "b", "n": 2}]
    assert rows(chargewire, ro, f"SELECT i FROM {table} WHERE v = ? ORDER BY i", ["b"]) == [{"i": 2}, {"i": 3}]
    assert rows(chargewire, ro, f"SELECT a.i FROM {table} a JOIN {table} b ON a.i = b.i + 1 ORDER BY 1") == \
        [{"i": 2}, {"i": 3}]
    assert chargewire.submit(session_id=ro, sql=f"EXPLAIN SELECT * FROM {table}", arrow=True).status_code == 200
    assert chargewire.submit(session_id=ro, sql=f"EXPLAIN ANALYZE SELECT * FROM {table}", arrow=True).status_code == 200
    # What another session commits is seen by the next statement: a read-only session in
    # autocommit holds no snapshot between statements.
    succeeded(chargewire, rw, f"INSERT INTO {table} VALUES (4, 'c')")
    assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {table}") == [{"n": 4}]


def test_prepared_reads_work(chargewire, rw, ro):
    if "prepared_statements" not in capabilities(chargewire):
        pytest.skip(reason="the server has no prepared statements")
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} AS SELECT * FROM (VALUES (1), (2), (3)) t(i)")
    prepared = prepare(chargewire, ro, f"SELECT count(*) AS n FROM {table} WHERE i >= ?")
    assert prepared.status_code == 201, prepared.text
    for least, expected in ((1, 3), (3, 1)):
        response = chargewire.http.post(
            url=f"{chargewire.session_url(ro)}/prepared/{prepared.json()['prepared_id']}/execute",
            headers=chargewire.headers(Accept="application/vnd.apache.arrow.stream"), json={"parameters": [least]})
        assert response.status_code == 200, response.text
        from chargewire import read_table
        assert read_table(content=response.content).to_pylist() == [{"n": expected}]


def test_metadata_works(chargewire, rw, ro):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    for path, params in (("catalogs", {}), ("db-schemas", {}), ("tables", {"table_name_filter_pattern": table}),
                         ("table-types", {}), ("type-info", {}), ("server-info", {})):
        response = chargewire.http.get(url=f"{chargewire.session_url(ro)}/metadata/{path}", params=params,
                                       headers=chargewire.headers())
        assert response.status_code == 200, (path, response.text)


def test_transactions_work_and_are_reported_as_before(chargewire, ro):
    assert document(chargewire, ro)["transaction"] == "none"
    succeeded(chargewire, ro, "BEGIN")
    assert document(chargewire, ro)["transaction"] == "open"
    assert rows(chargewire, ro, "SELECT 1 AS one") == [{"one": 1}]
    succeeded(chargewire, ro, "COMMIT")
    assert document(chargewire, ro)["transaction"] == "none"
    succeeded(chargewire, ro, "BEGIN TRANSACTION READ ONLY")
    succeeded(chargewire, ro, "ROLLBACK")
    assert document(chargewire, ro)["transaction"] == "none"
    assert rows(chargewire, ro, "SELECT 2 AS two") == [{"two": 2}]


def test_a_retained_result_cancel_and_close_work(chargewire, ro):
    response = chargewire.submit(session_id=ro, sql="SELECT 7 AS v", headers={"Chargewire-Retain-Result": "true"})
    assert response.status_code in (200, 201), response.text
    statement_id = response.json()["statement_id"]
    chargewire.wait_done(session_id=ro, statement_id=statement_id)
    from chargewire import read_table
    for _ in range(2):
        result = chargewire.result(session_id=ro, statement_id=statement_id)
        assert result.status_code == 200 and read_table(content=result.content).to_pylist() == [{"v": 7}]
    cancelled = chargewire.http.post(url=f"{chargewire.session_url(ro)}/cancel", headers=chargewire.headers())
    assert cancelled.status_code == 202, cancelled.text
    assert chargewire.http.delete(url=chargewire.session_url(ro), headers=chargewire.headers()).status_code == 204


# ---- Errors ----


def test_the_refusal_names_the_kind_of_statement_and_never_its_text(chargewire, ro):
    marker = "S3CR3T-" + uuid.uuid4().hex
    response = chargewire.submit(session_id=ro, sql=f"CREATE TABLE {name()} AS SELECT '{marker}' AS v")
    problem = assert_problem(response=response, status=403, code="read_only")
    assert marker not in response.text, "the refusal quotes the statement"
    assert "read-only" in problem["detail"], problem
    # Not retriable: the same request on the same session fails the same way, and the
    # session goes on reading.
    refused(chargewire, ro, f"CREATE TABLE {name()} AS SELECT '{marker}' AS v")
    assert rows(chargewire, ro, "SELECT 1 AS one") == [{"one": 1}]


# ---- A session that replaces a lost one (§5.6): the rule is the client's ----


@pytest.mark.own_server
def test_a_replacement_is_read_only_only_when_the_client_creates_it_so():
    binary = os.environ.get("CHARGEWIRE_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="this test starts a server of its own: set CHARGEWIRE_TEST_SERVER_BINARY")
    flag = os.environ.get("CHARGEWIRE_TEST_SERVER_IDLE_FLAG", "--session-idle-timeout-seconds")
    server = start_server(binary=binary, extra_args=[flag, "1"])
    try:
        chargewire = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
        if "read_only_sessions" not in capabilities(chargewire):
            chargewire.close()
            no_read_only_sessions()
        table = name()
        writer = chargewire.new_session()
        succeeded(chargewire, writer, f"CREATE TABLE {table} (i INTEGER)")
        lost = chargewire.new_session(options={"read_only": True, "session_tag": {"job": "dashboard"}})
        gaps = [2, 4, 8, 16]
        while True:
            response = chargewire.submit(session_id=lost, sql="SELECT 1", arrow=True)
            if response.status_code == 404:
                break
            assert response.status_code == 200, response.text
            assert gaps, "the idle session was never evicted"
            time.sleep(gaps.pop(0))
        # The server has nothing to inherit from: a creation request without the option is
        # a read-write session. That is why §5.8 makes the replay the client's MUST.
        forgetful = chargewire.new_session(options={"session_tag": {"job": "dashboard"}})
        assert document(chargewire, forgetful)["options"]["read_only"] is False
        assert chargewire.submit(session_id=forgetful, sql=f"INSERT INTO {table} VALUES (1)").status_code in (200, 201)
        # The replacement as §5.8 says: read_only in the creation request, with the other
        # options the client replays, and checked in the answer.
        created = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                                       json={"options": {"read_only": True, "session_tag": {"job": "dashboard"}}})
        assert created.status_code == 201, created.text
        assert created.json()["options"]["read_only"] is True
        refused(chargewire, created.json()["session_id"], f"INSERT INTO {table} VALUES (2)")
        chargewire.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()
