"""Spec §5.8: read-only sessions (capability `read_only_sessions`).

A session whose `read_only` option is true changes nothing, whatever is sent through it,
and nothing sent through it makes it read-write again. Every refusal is `403 read_only`,
and every refused write has a twin here that works in a read-write session, so a refusal
is never a statement that would have failed anyway.

A refusal that is answered after the change was made is worse than none, so every refusal
here is checked against a snapshot of what a second session sees (`unchanged`): the catalog,
the rows of the tables involved, files, and on a DuckDB server its settings, sequences,
attached databases, secrets and extensions.

A server without the capability must say so (`unknown_option`), which the first tests
check; the rest skip on such a server, saying so."""

import hashlib
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


# ---- What a refused statement must leave as it was ----

_duckdb_servers: dict[str, bool] = {}


def on_duckdb(chargewire, session_id: str) -> bool:
    key = str(chargewire.http.base_url)
    if key not in _duckdb_servers:
        info = chargewire.http.get(url=f"{chargewire.session_url(session_id)}/metadata/server-info",
                                   headers=chargewire.headers())
        _duckdb_servers[key] = info.status_code == 200 and "duckdb_version" in info.json()
    return _duckdb_servers[key]


def files_in(root, contents: bool = True) -> list[tuple]:
    """Every file under `root`: its path, size and (with `contents`) a hash of what is in it."""
    found = []
    for base, _dirs, names in os.walk(root):
        for file_name in names:
            path = os.path.join(base, file_name)
            try:
                with open(path, mode="rb") as file:
                    digest = hashlib.sha256(file.read()).hexdigest() if contents else ""
                found.append((os.path.relpath(path, root), os.path.getsize(path), digest))
            except OSError:
                pass  # gone between the listing and the read: the next listing shows it
    return sorted(found)


# DuckDB's catalog as one hash: tables and their columns, views and their text, schemas,
# indexes, sequences with their last value, macros and types. {where} picks the databases.
DUCKDB_CATALOG = """
  SELECT md5(coalesce(string_agg(entry, '|' ORDER BY entry ASC), '')) FROM (
    SELECT 'table ' || database_name || '.' || schema_name || '.' || table_name || ' ' || column_count::VARCHAR || ' ' || coalesce(comment, '') AS entry
      FROM duckdb_tables() WHERE {where}
    UNION ALL SELECT 'column ' || database_name || '.' || schema_name || '.' || table_name || '.' || column_name || ' ' || data_type
      FROM duckdb_columns() WHERE {where} AND NOT internal
    UNION ALL SELECT 'view ' || database_name || '.' || schema_name || '.' || view_name || ' ' || coalesce(sql, '')
      FROM duckdb_views() WHERE {where} AND NOT internal
    UNION ALL SELECT 'schema ' || database_name || '.' || schema_name FROM duckdb_schemas() WHERE {where} AND NOT internal
    UNION ALL SELECT 'index ' || database_name || '.' || schema_name || '.' || index_name FROM duckdb_indexes() WHERE {where}
    UNION ALL SELECT 'sequence ' || database_name || '.' || schema_name || '.' || sequence_name || ' ' || coalesce(last_value::VARCHAR, 'unused')
      FROM duckdb_sequences() WHERE {where}
    UNION ALL SELECT 'function ' || database_name || '.' || schema_name || '.' || function_name FROM duckdb_functions() WHERE {where} AND NOT internal
    UNION ALL SELECT 'type ' || database_name || '.' || schema_name || '.' || type_name FROM duckdb_types() WHERE {where} AND NOT internal
  ) AS entries"""

# What a second session sees of a DuckDB server, one column for each thing that could change.
DUCKDB_SERVER_STATE = f"""
SELECT
  ({DUCKDB_CATALOG.format(where="database_name NOT IN ('system', 'temp')")}) AS catalog,
  (SELECT coalesce(string_agg(database_name || '.' || schema_name || '.' || sequence_name || '=' || coalesce(last_value::VARCHAR, 'unused'), ', '
                              ORDER BY database_name ASC, schema_name ASC, sequence_name ASC), '')
     FROM duckdb_sequences() WHERE database_name NOT IN ('system', 'temp')) AS sequences,
  (SELECT string_agg(database_name || ' at ' || coalesce(path, '-') || ' read-only ' || readonly::VARCHAR, ', ' ORDER BY database_name ASC)
     FROM duckdb_databases()) AS attached_databases,
  (SELECT md5(string_agg(name || '=' || value, '|' ORDER BY name ASC)) FROM duckdb_settings()) AS all_settings,
  current_setting('enable_logging')::VARCHAR AS enable_logging,
  current_setting('logging_level')::VARCHAR AS logging_level,
  (SELECT coalesce(string_agg(name || ' persistent ' || persistent::VARCHAR, ', ' ORDER BY name ASC), '') FROM duckdb_secrets()) AS secrets,
  (SELECT string_agg(extension_name || ' loaded ' || loaded::VARCHAR || ' installed ' || installed::VARCHAR, ', ' ORDER BY extension_name ASC)
     FROM duckdb_extensions()) AS extensions"""

# What a DuckDB session has of its own: its temporary objects, settings and variables.
DUCKDB_SESSION_STATE = f"""
SELECT
  ({DUCKDB_CATALOG.format(where="database_name = 'temp'")}) AS temporary_objects,
  (SELECT md5(string_agg(name || '=' || value, '|' ORDER BY name ASC)) FROM duckdb_settings()) AS settings,
  (SELECT coalesce(string_agg(name || '=' || value::VARCHAR, ', ' ORDER BY name ASC), '') FROM duckdb_variables()) AS variables"""


class Unchanged:
    """`with Unchanged(...):` around statements that must be refused: a snapshot before, one
    after, and a failure that names what differs.

    `observer`: a read-write session that only looks. `subject`: the session the refused
    statements are sent through (on DuckDB its own temporary objects, settings and variables
    are part of the snapshot; leave it out when the statements leave its transaction
    aborted). `tables`: read in full by the observer; `session_tables`: read by the subject
    (its temporary tables). `directories`: every file in them, by content; `home`: the
    server's own home directory, by name and size."""

    def __init__(self, chargewire, observer: str, subject: str | None = None, tables=(), session_tables=(),
                 directories=(), home=None):
        self.chargewire, self.observer, self.subject = chargewire, observer, subject
        self.tables, self.session_tables = list(tables), list(session_tables)
        self.directories, self.home = list(directories), home
        self.before = None

    def everything_in(self, session_id: str, table: str) -> list[str]:
        return sorted(repr(row) for row in rows(self.chargewire, session_id, f"SELECT * FROM {table}"))

    def snapshot(self) -> dict:
        cw, observer = self.chargewire, self.observer
        state = {
            "the tables and views (information_schema.tables)": rows(
                cw, observer, "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables "
                              "ORDER BY table_catalog ASC, table_schema ASC, table_name ASC"),
            "the columns (information_schema.columns)": rows(
                cw, observer, "SELECT table_catalog, table_schema, table_name, column_name, data_type FROM information_schema.columns "
                              "ORDER BY table_catalog ASC, table_schema ASC, table_name ASC, ordinal_position ASC"),
            "the schemas (information_schema.schemata)": rows(
                cw, observer, "SELECT catalog_name, schema_name FROM information_schema.schemata "
                              "ORDER BY catalog_name ASC, schema_name ASC"),
        }
        for table in self.tables:
            state[f"the rows of {table}"] = self.everything_in(observer, table)
        for table in self.session_tables:
            state[f"the rows of the session's {table}"] = self.everything_in(self.subject, table)
        for directory in self.directories:
            state[f"the files in {directory}"] = files_in(root=directory)
        if self.home is not None:
            state["the files in the server's home directory"] = files_in(root=self.home, contents=False)
        if on_duckdb(cw, observer):
            for facet, value in rows(cw, observer, DUCKDB_SERVER_STATE)[0].items():
                state[f"DuckDB: {facet}"] = value
            if self.subject is not None:
                for facet, value in rows(cw, self.subject, DUCKDB_SESSION_STATE)[0].items():
                    state[f"DuckDB, the session's own {facet}"] = value
                state["the session's options"] = document(cw, self.subject)["options"]
        return state

    def __enter__(self):
        self.before = self.snapshot()
        return self

    def __exit__(self, kind, value, traceback):
        if kind is None:
            self.check()
        return False

    def check(self) -> None:
        after = self.snapshot()
        changed = [f"{facet}:\n      before: {str(self.before[facet])[:600]}\n      after:  {str(after[facet])[:600]}"
                   for facet in self.before if self.before[facet] != after[facet]]
        assert not changed, "something changed although the statement was refused:\n  " + "\n  ".join(changed)


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


@pytest.fixture
def observer(chargewire):
    """A read-write session that only looks: the second session of every snapshot."""
    sid = chargewire.new_session()
    yield sid
    chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())


@pytest.fixture
def unchanged(chargewire, server, observer, tmp_path):
    """`with unchanged(subject=..., tables=[...]):` around what must be refused (Unchanged)."""
    def watch(subject: str | None = None, tables=(), session_tables=(), directories=None):
        return Unchanged(chargewire=chargewire, observer=observer, subject=subject, tables=tables,
                         session_tables=session_tables, home=server.home,
                         directories=[tmp_path] if directories is None else directories)
    return watch


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


def test_a_session_is_created_read_only(chargewire, offered, unchanged):
    response = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                                    json={"options": {"read_only": True}})
    assert response.status_code == 201, response.text
    sid = response.json()["session_id"]
    assert response.json()["options"]["read_only"] is True
    assert document(chargewire, sid)["options"]["read_only"] is True
    answer = patch(chargewire, sid, {})
    assert answer["options"]["read_only"] is True and answer["errors"] == {}, answer
    with unchanged(subject=sid):
        refused(chargewire, sid, f"CREATE TABLE {name()} (i INTEGER)")


def test_read_only_false_at_creation_is_a_read_write_session(chargewire, offered):
    sid = chargewire.new_session(options={"read_only": False})
    assert document(chargewire, sid)["options"]["read_only"] is False
    succeeded(chargewire, sid, f"CREATE TABLE {name()} (i INTEGER)")


def test_the_options_request_makes_a_session_read_only(chargewire, offered, rw, unchanged):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    answer = patch(chargewire, rw, {"read_only": True})
    assert answer["errors"] == {} and answer["options"]["read_only"] is True, answer
    assert document(chargewire, rw)["options"]["read_only"] is True
    with unchanged(subject=rw, tables=[table]):
        refused(chargewire, rw, f"INSERT INTO {table} VALUES (1)")
        # Asking again changes nothing and is no error.
        again = patch(chargewire, rw, {"read_only": True})
        assert again["errors"] == {} and again["options"]["read_only"] is True, again
        refused(chargewire, rw, f"INSERT INTO {table} VALUES (1)")


def test_a_value_that_is_not_a_boolean_is_invalid(chargewire, offered, rw):
    created = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(),
                                   json={"options": {"read_only": "yes"}})
    problem = assert_problem(response=created, status=400, code="invalid_option_value")
    assert "read_only" in problem["detail"], problem
    answer = patch(chargewire, rw, {"read_only": 1})
    assert answer["errors"]["read_only"]["code"] == "invalid_option_value", answer
    assert answer["options"]["read_only"] is False
    succeeded(chargewire, rw, f"CREATE TABLE {name()} (i INTEGER)")  # still a session that writes


# ---- One refusal per write path, each with its twin in a read-write session ----

# (what, statements a read-write session runs first, the write). {t} is a table of the
# test's own, {n} a free name. The snapshot around the refusal holds the rows of {t} and
# the whole catalog, so a table, view or schema that was made all the same shows.
WRITES = [
    ("INSERT", ["CREATE TABLE {t} (i INTEGER)"], "INSERT INTO {t} VALUES (1)"),
    ("UPDATE", ["CREATE TABLE {t} AS SELECT 1 AS i"], "UPDATE {t} SET i = 2"),
    ("DELETE", ["CREATE TABLE {t} AS SELECT 1 AS i"], "DELETE FROM {t}"),
    ("INSERT ... RETURNING", ["CREATE TABLE {t} (i INTEGER)"], "INSERT INTO {t} VALUES (1) RETURNING i"),
    ("a write inside a CTE", ["CREATE TABLE {t} (i INTEGER)"],
     "WITH written AS (INSERT INTO {t} VALUES (1) RETURNING i) SELECT count(*) AS n FROM written"),
    ("the second statement of a request", ["CREATE TABLE {t} (i INTEGER)"], "SELECT 1; INSERT INTO {t} VALUES (1)"),
    ("CREATE TABLE", ["CREATE TABLE {t} (i INTEGER)"], "CREATE TABLE {n} (i INTEGER)"),
    ("CREATE TABLE AS", ["CREATE TABLE {t} (i INTEGER)"], "CREATE TABLE {n} AS SELECT 1 AS i"),
    ("DROP TABLE", ["CREATE TABLE {t} (i INTEGER)"], "DROP TABLE {t}"),
    ("ALTER TABLE", ["CREATE TABLE {t} (i INTEGER)"], "ALTER TABLE {t} ADD COLUMN j INTEGER"),
    ("CREATE VIEW", ["CREATE TABLE {t} (i INTEGER)"], "CREATE VIEW {n} AS SELECT 1 AS i"),
    ("CREATE SCHEMA", ["CREATE TABLE {t} (i INTEGER)"], "CREATE SCHEMA {n}"),
    # Temporary objects are refused too (§5.8): a read-only session leaves nothing behind.
    ("CREATE TEMPORARY TABLE", ["CREATE TABLE {t} (i INTEGER)"], "CREATE TEMPORARY TABLE {n} (i INTEGER)"),
]


@pytest.mark.parametrize("what,setup,write", WRITES, ids=[w[0] for w in WRITES])
def test_a_write_is_refused(chargewire, rw, ro, unchanged, what, setup, write):
    names = {"t": name("ro_t"), "n": name("ro_n")}
    for sql in setup:
        succeeded(chargewire, rw, sql.format(**names))
    with unchanged(subject=ro, tables=[names["t"]]):
        refused(chargewire, ro, write.format(**names))
        refused(chargewire, ro, write.format(**names))  # and again: a refusal changes nothing for the next try
    # The twin: the same statement is a statement that works.
    response = chargewire.submit(session_id=rw, sql=write.format(**names))
    assert response.status_code in (200, 201), response.text
    # And the read-only session reads on.
    assert rows(chargewire, ro, "SELECT 1 AS one") == [{"one": 1}]


def test_explain_analyze_of_a_write_is_refused(chargewire, rw, ro, unchanged):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    with unchanged(subject=ro, tables=[table]):
        refused(chargewire, ro, f"EXPLAIN ANALYZE INSERT INTO {table} VALUES (1)")
    assert chargewire.submit(session_id=rw, sql=f"EXPLAIN ANALYZE INSERT INTO {table} VALUES (1)").status_code in (200, 201)
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 1}]  # the twin wrote


def prepare(chargewire, session_id: str, sql: str):
    return chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared", headers=chargewire.headers(),
                                json={"sql": sql})


def execute_prepared(chargewire, session_id: str, prepared_id: str, parameters=None):
    return chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared/{prepared_id}/execute",
                                headers=chargewire.headers(), json={"parameters": parameters or []})


def test_a_prepared_write_is_refused_at_prepare_or_at_execute(chargewire, rw, ro, unchanged):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    with unchanged(subject=ro, tables=[table]):
        prepared = prepare(chargewire, ro, f"INSERT INTO {table} VALUES (?)")
        if prepared.status_code == 201:  # §7.1: SHOULD refuse here; MUST at the execution
            prepared = execute_prepared(chargewire, ro, prepared.json()["prepared_id"], [1])
        assert_problem(response=prepared, status=403, code="read_only")
    twin = prepare(chargewire, rw, f"INSERT INTO {table} VALUES (?)")
    assert twin.status_code == 201, twin.text
    assert execute_prepared(chargewire, rw, twin.json()["prepared_id"], [1]).status_code in (200, 201)
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 1}]


def test_a_write_prepared_before_the_session_became_read_only_is_refused(chargewire, offered, rw, unchanged):
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (i INTEGER)")
    prepared = prepare(chargewire, rw, f"INSERT INTO {table} VALUES (?)")
    assert prepared.status_code == 201, prepared.text
    prepared_id = prepared.json()["prepared_id"]
    assert execute_prepared(chargewire, rw, prepared_id, [1]).status_code in (200, 201)  # the twin
    assert patch(chargewire, rw, {"read_only": True})["errors"] == {}
    with unchanged(subject=rw, tables=[table]):
        assert_problem(response=execute_prepared(chargewire, rw, prepared_id, [2]), status=403, code="read_only")
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {table}") == [{"n": 1}]


def test_a_prepared_transaction_statement_does_what_its_answer_says(chargewire, ro):
    # A driver that prepares every statement prepares its BEGIN and COMMIT too. Whatever a
    # server answers to them, the session's transaction is what the answer says: a refused
    # or failed BEGIN opened nothing, a succeeded one did.
    if not {"prepared_statements", "transactions"} <= set(capabilities(chargewire)):
        pytest.skip(reason="the server has no prepared statements or no transactions")

    def run_prepared(sql: str) -> bool:
        response = prepare(chargewire, ro, sql)
        if response.status_code == 201:
            response = execute_prepared(chargewire, ro, response.json()["prepared_id"])
        if response.status_code in (200, 201):
            assert response.json()["state"] == "succeeded", response.text
            return True
        assert response.status_code in (400, 403, 422), response.text
        return False

    for sql, when_done in (("BEGIN", "open"), ("COMMIT", "none"), ("BEGIN", "open"), ("ROLLBACK", "none")):
        before = document(chargewire, ro)["transaction"]
        done = run_prepared(sql)
        assert document(chargewire, ro)["transaction"] == (when_done if done else before), (sql, done)
        if not done and sql == "BEGIN":
            succeeded(chargewire, ro, "BEGIN")  # so the COMMIT or ROLLBACK that follows has one to end
    assert rows(chargewire, ro, "SELECT 1 AS one") == [{"one": 1}]


@pytest.mark.parametrize("mode", ["create", "append", "replace", "create_append"])
def test_bulk_ingest_is_refused(chargewire, rw, ro, unchanged, mode):
    if "ingest" not in capabilities(chargewire):
        pytest.skip(reason="the server has no bulk ingest")
    table, kept = name(), name()
    succeeded(chargewire, rw, f"CREATE TABLE {kept} (i BIGINT)")
    if mode in ("append", "replace"):
        succeeded(chargewire, rw, f"CREATE TABLE {table} AS SELECT 7::BIGINT AS i")
    body = ipc_bytes(table=pa.table({"i": pa.array([1, 2, 3], pa.int64())}))
    with unchanged(subject=ro, tables=[table] if mode in ("append", "replace") else [kept]):
        assert_problem(response=ingest(chargewire, ro, body, table=table, mode=mode), status=403, code="read_only")
        assert_problem(response=ingest(chargewire, ro, body, table=name(), temporary="true"), status=403, code="read_only")
    assert ingest(chargewire, rw, body, table=table, mode=mode).status_code == 200  # the twin


def test_a_partitioned_ingest_is_refused_when_it_is_opened(chargewire, rw, ro, unchanged):
    if "partitioned_ingest" not in capabilities(chargewire):
        pytest.skip(reason="the server has no partitioned ingest")
    table = name()
    with unchanged(subject=ro):
        opened = chargewire.http.post(url=f"{chargewire.session_url(ro)}/ingests", headers=chargewire.headers(),
                                      json={"table": table})
        assert_problem(response=opened, status=403, code="read_only")
    twin = chargewire.http.post(url=f"{chargewire.session_url(rw)}/ingests", headers=chargewire.headers(),
                                json={"table": table})
    assert twin.status_code == 201, twin.text
    chargewire.http.delete(url=f"{chargewire.session_url(rw)}/ingests/{twin.json()['ingest_id']}",
                           headers=chargewire.headers())


def test_a_partitioned_ingest_opened_before_the_session_became_read_only_writes_nothing(chargewire, offered, unchanged):
    # Opened while the session wrote, then the session is made read-only: the parts and the
    # commit that follow are an ingest of a read-only session, and nothing of it is written.
    if "partitioned_ingest" not in capabilities(chargewire):
        pytest.skip(reason="the server has no partitioned ingest")
    body = ipc_bytes(table=pa.table({"i": pa.array(list(range(1000)), pa.int64())}))

    def run(make_read_only: bool, table: str) -> tuple:
        sid = chargewire.new_session()
        base = f"{chargewire.session_url(sid)}/ingests"
        opened = chargewire.http.post(url=base, headers=chargewire.headers(), json={"table": table})
        assert opened.status_code == 201, opened.text
        if make_read_only:
            answer = patch(chargewire, sid, {"read_only": True})
            assert answer["errors"] == {} and answer["options"]["read_only"] is True, answer
        ingest_id = opened.json()["ingest_id"]
        part = chargewire.http.post(url=f"{base}/{ingest_id}/parts/0", content=body,
                                    headers=chargewire.headers(**{"Content-Type": "application/vnd.apache.arrow.stream"}))
        commit = chargewire.http.post(url=f"{base}/{ingest_id}/commit", headers=chargewire.headers(), json={"part_count": 1})
        chargewire.http.delete(url=f"{base}/{ingest_id}", headers=chargewire.headers())
        return sid, part, commit

    with unchanged():
        sid, part, commit = run(make_read_only=True, table=name())
        if part.status_code != 200:  # refused with the first part, or only at the commit
            assert_problem(response=part, status=403, code="read_only")
        assert_problem(response=commit, status=403, code="read_only")
        assert document(chargewire, sid)["options"]["read_only"] is True
    # The twin: the same requests without the switch write the table.
    table = name()
    sid, part, commit = run(make_read_only=False, table=table)
    assert part.status_code == 200 and commit.status_code == 200, (part.text, commit.text)
    assert rows(chargewire, sid, f"SELECT count(*) AS n FROM {table}") == [{"n": 1000}]


# ---- One-way: nothing sent through a read-only session makes it read-write ----


@pytest.fixture
def table(chargewire, rw):
    created = name("ro_door")
    succeeded(chargewire, rw, f"CREATE TABLE {created} (i INTEGER)")
    yield created
    assert rows(chargewire, rw, f"SELECT count(*) AS n FROM {created}") == [{"n": 0}], "a door was open"


@pytest.fixture
def still_read_only(chargewire, unchanged):
    """Checks that a session is read-only: its document says so, and a write is refused and
    changes nothing."""
    def check(session_id: str, table: str) -> None:
        assert document(chargewire, session_id)["options"]["read_only"] is True
        with unchanged(subject=session_id, tables=[table]):
            refused(chargewire, session_id, f"INSERT INTO {table} VALUES (1)")
    return check


def test_the_options_request_cannot_make_it_read_write(chargewire, ro, table, still_read_only):
    answer = patch(chargewire, ro, {"read_only": False})
    assert answer["errors"]["read_only"]["code"] == "read_only", answer
    assert answer["options"]["read_only"] is True
    # Not next to another option either, and the other option is still applied.
    answer = patch(chargewire, ro, {"read_only": False, "session_tag": {"job": "door"}})
    assert set(answer["errors"]) == {"read_only"}, answer
    assert answer["errors"]["read_only"]["code"] == "read_only", answer
    assert answer["options"]["session_tag"] == {"job": "door"}
    still_read_only(ro, table)


def test_ending_a_transaction_does_not_open_a_read_write_one(chargewire, ro, table, still_read_only, unchanged):
    # Each of these gets a write out of a read-only transaction a client starts itself on a
    # server that only passes transaction statements on.
    succeeded(chargewire, ro, "BEGIN")
    succeeded(chargewire, ro, "COMMIT")
    still_read_only(ro, table)
    with unchanged(tables=[table]):
        succeeded(chargewire, ro, "BEGIN")
        refused(chargewire, ro, f"COMMIT; INSERT INTO {table} VALUES (1)")
        if document(chargewire, ro)["transaction"] != "none":
            succeeded(chargewire, ro, "ROLLBACK")
        succeeded(chargewire, ro, "BEGIN")
        refused(chargewire, ro, f"ROLLBACK; BEGIN; INSERT INTO {table} VALUES (1); COMMIT")
        if document(chargewire, ro)["transaction"] != "none":
            succeeded(chargewire, ro, "ROLLBACK")
    still_read_only(ro, table)


def test_a_read_write_transaction_cannot_be_asked_for(chargewire, ro, table, still_read_only, unchanged):
    with unchanged(subject=ro, tables=[table]):
        refused(chargewire, ro, "BEGIN TRANSACTION READ WRITE")
        refused(chargewire, ro, "START TRANSACTION READ WRITE")
    assert document(chargewire, ro)["transaction"] == "none"
    still_read_only(ro, table)


def test_the_commit_and_rollback_calls_start_read_only_transactions(chargewire, ro, table, still_read_only, unchanged):
    if "transactions" not in capabilities(chargewire):
        pytest.skip(reason="the server has no transactions capability")
    assert patch(chargewire, ro, {"autocommit": False})["errors"] == {}
    for call in ("commit", "rollback", "commit"):
        assert rows(chargewire, ro, f"SELECT count(*) AS n FROM {table}") == [{"n": 0}]
        response = chargewire.http.post(url=f"{chargewire.session_url(ro)}/{call}", headers=chargewire.headers())
        assert response.status_code == 204, response.text
        assert document(chargewire, ro)["transaction"] == "open"
        with unchanged(tables=[table]):
            refused(chargewire, ro, f"INSERT INTO {table} VALUES (1)")
        # A write the engine refuses may abort the transaction, as any failed statement can.
        chargewire.http.post(url=f"{chargewire.session_url(ro)}/rollback", headers=chargewire.headers())
    assert patch(chargewire, ro, {"autocommit": True})["errors"] == {}
    still_read_only(ro, table)


def test_the_session_options_do_not_open_it(chargewire, rw, ro, table, still_read_only, unchanged):
    schema = name("ro_schema")
    succeeded(chargewire, rw, f"CREATE SCHEMA {schema}")
    assert patch(chargewire, ro, {"schema": schema})["errors"] == {}
    assert document(chargewire, ro)["options"]["schema"] == schema
    with unchanged(subject=ro, tables=[table]):
        refused(chargewire, ro, "CREATE TABLE in_the_other_schema (i INTEGER)")
    catalog = document(chargewire, ro)["options"]["catalog"]
    assert patch(chargewire, ro, {"catalog": catalog, "schema": document(chargewire, rw)["options"]["schema"],
                                  "autocommit": True})["errors"] == {}
    still_read_only(ro, table)


TERMINAL = ("succeeded", "failed", "cancelled")


def test_an_options_request_waits_for_the_running_statement(chargewire, offered, observer, unchanged):
    # One request: a statement that takes a while, then a write. The options request comes
    # while the first runs. The request that was running ends as it began, so its write is
    # made; the options request is answered only once it has ended; and every statement after
    # that answer is in a read-only session. (A server that switched when the request came
    # would refuse the write of a request it had accepted as read-write, or would answer
    # before the request had ended.)
    for size in (300_000_000, 1_500_000_000, 6_000_000_000):
        written = name("ro_later")
        succeeded(chargewire, observer, f"CREATE TABLE {written} (i INTEGER)")
        sid = chargewire.new_session()
        slow = chargewire.submit(session_id=sid, headers={"Prefer": "respond-async"},
                                 sql=f"SELECT sum(i) AS s FROM range({size}) t(i); INSERT INTO {written} VALUES (1)")
        assert slow.status_code == 201, slow.text
        statement_id = slow.json()["statement_id"]
        running = chargewire.status(session_id=sid, statement_id=statement_id)["state"] not in TERMINAL
        answer = patch(chargewire, sid, {"read_only": True})
        after = chargewire.status(session_id=sid, statement_id=statement_id)
        if not running:
            # The statement had ended before the options request was sent: nothing was
            # tested. A longer one, in a new session.
            chargewire.http.delete(url=chargewire.session_url(sid), headers=chargewire.headers())
            continue
        assert answer["errors"] == {} and answer["options"]["read_only"] is True, answer
        assert after["state"] == "succeeded", ("the options request was answered before the request that was "
                                               "running had ended, or that request was not left to end as it began", after)
        assert rows(chargewire, observer, f"SELECT i FROM {written}") == [{"i": 1}], "the running request's write is missing"
        assert document(chargewire, sid)["options"]["read_only"] is True
        with unchanged(subject=sid, tables=[written]):
            refused(chargewire, sid, f"INSERT INTO {written} VALUES (2)")
        return
    pytest.fail(reason="no statement ran long enough for an options request to arrive while it ran")


# ---- Becoming read-only with a transaction open ----


def test_a_transaction_with_work_in_it_refuses_the_switch_and_is_left_alone(chargewire, offered, rw, unchanged):
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
    with unchanged(subject=rw, tables=[table]):
        refused(chargewire, rw, f"INSERT INTO {table} VALUES (3)")


def test_read_only_is_applied_before_the_other_options_of_its_request(chargewire, offered, rw, observer):
    # §5.8: read_only is applied before the request's other options. With work in an open
    # transaction and {"autocommit": true, "read_only": true} in one request, read_only finds
    # the transaction and is refused; applied after autocommit (which commits the work) it
    # would be accepted, and the answer would depend on the order of a JSON object's members.
    if "transactions" not in capabilities(chargewire):
        pytest.skip(reason="the server has no transactions capability")
    table = name()
    succeeded(chargewire, observer, f"CREATE TABLE {table} (i INTEGER)")
    assert patch(chargewire, rw, {"autocommit": False})["errors"] == {}
    succeeded(chargewire, rw, f"INSERT INTO {table} VALUES (1)")
    for options in ({"autocommit": True, "read_only": True}, {"read_only": True, "autocommit": True}):
        answer = patch(chargewire, rw, options)
        assert set(answer["errors"]) == {"read_only"}, answer
        assert answer["errors"]["read_only"]["code"] == "invalid_option_value", answer
        assert answer["options"]["read_only"] is False and answer["options"]["autocommit"] is True, answer
        # The other option was applied (the work is committed), and the session still writes.
        assert rows(chargewire, observer, f"SELECT count(*) AS n FROM {table}") == [{"n": 1}]
        assert patch(chargewire, rw, {"autocommit": False})["errors"] == {}
        succeeded(chargewire, rw, f"DELETE FROM {table}")
        succeeded(chargewire, rw, f"INSERT INTO {table} VALUES (1)")
    succeeded(chargewire, rw, "COMMIT")


def test_manual_commit_and_read_only_can_be_set_together(chargewire, offered, rw, unchanged):
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
        with unchanged(tables=[table]):
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


def test_the_refusal_never_quotes_the_statement(chargewire, rw, ro, unchanged):
    # §5.8: a statement's text may hold a credential. Whoever words the refusal (the server
    # for the kind of statement, the engine for a write it finds itself), nothing of the
    # statement's literals is in the answer.
    marker = "S3CR3T-" + uuid.uuid4().hex
    table = name()
    succeeded(chargewire, rw, f"CREATE TABLE {table} (v VARCHAR)")
    with unchanged(subject=ro, tables=[table]):
        for sql in (f"CREATE TABLE {name()} AS SELECT '{marker}' AS v", f"INSERT INTO {table} VALUES ('{marker}')",
                    f"UPDATE {table} SET v = '{marker}'", f"DELETE FROM {table} WHERE v = '{marker}'",
                    f"WITH written AS (INSERT INTO {table} VALUES ('{marker}') RETURNING v) SELECT count(*) FROM written",
                    f"SELECT '{marker}'; INSERT INTO {table} VALUES ('{marker}')"):
            response = chargewire.submit(session_id=ro, sql=sql)
            problem = assert_problem(response=response, status=403, code="read_only")
            assert marker not in response.text, ("the refusal quotes the statement", sql)
            assert "read-only" in problem["detail"], problem
        # Not retriable: the same request on the same session fails the same way, and the
        # session goes on reading.
        refused(chargewire, ro, f"CREATE TABLE {name()} AS SELECT '{marker}' AS v")
    assert rows(chargewire, ro, "SELECT 1 AS one") == [{"one": 1}]


def test_a_statement_that_fails_for_another_reason_is_not_a_read_only_refusal(chargewire, rw, ro):
    # read_only says "this would change something; never worth retrying". A table that is not
    # there is not that, in either kind of session, whatever its name spells.
    for session_id in (rw, ro):
        for sql in (f"SELECT * FROM {name('ro_missing')}", 'SELECT * FROM "a read-only session"',
                    'SELECT * FROM "transaction is launched in read-only mode"'):
            response = chargewire.submit(session_id=session_id, sql=sql)
            assert response.status_code in (400, 404, 422), (sql, response.text)
            assert response.json()["code"] != "read_only", (sql, response.text)
            assert response.json().get("sqlstate") != "25006", (sql, response.text)
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
        with Unchanged(chargewire=chargewire, observer=chargewire.new_session(), subject=created.json()["session_id"],
                       tables=[table], home=server.home):
            refused(chargewire, created.json()["session_id"], f"INSERT INTO {table} VALUES (2)")
        chargewire.close()
    finally:
        assert stop_server(server=server) == 0, server.log_tail()
