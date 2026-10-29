"""Spec §2.3, §2.4, §2.5 and §12: what keeps version 1 open to additions. Servers ignore
request fields they don't know, ids are long random strings, and Chargewire-Query-Tag labels a
statement without changing it."""

import io
import math
import re

import pyarrow as pa
import pyarrow.ipc as ipc

from test_discovery_auth import assert_problem
from chargewire import ARROW_STREAM

ID = re.compile(r"^[A-Za-z0-9._~-]{1,128}$")
FUTURE = {"x_future_field": {"nested": [1, 2, 3]}, "another_new_field": "ignored"}


def arrow_body(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink=sink, schema=table.schema) as writer:
        writer.write_table(table=table)
    return sink.getvalue()


def test_unknown_request_fields_are_ignored(chargewire):
    # §2.4: a server MUST NOT refuse a request because its JSON body has a field it doesn't
    # know, so a later minor version can add one.
    token = chargewire.http.post(url="/chargewire/v1/auth/token",
                           json={"grant_type": "password", "username": chargewire.username, "password": chargewire.password,
                                 **FUTURE})
    assert token.status_code == 200, token.text
    created = chargewire.http.post(url="/chargewire/v1/sessions", headers=chargewire.headers(), json={"options": {}, **FUTURE})
    assert created.status_code == 201, created.text
    session_id = created.json()["session_id"]
    try:
        patched = chargewire.http.patch(url=chargewire.session_url(session_id), headers=chargewire.headers(),
                                  json={"options": {}, **FUTURE})
        assert patched.status_code == 200 and patched.json()["errors"] == {}, patched.text
        submitted = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/statements", headers=chargewire.headers(),
                                   json={"sql": "SELECT 41 + 1 AS v", **FUTURE})
        assert submitted.status_code in (200, 201), submitted.text
        doc = submitted.json()
        if doc["state"] != "succeeded":
            doc = chargewire.wait_done(session_id=session_id, statement_id=doc["statement_id"])
        assert doc["state"] == "succeeded", doc

        prepared = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared", headers=chargewire.headers(),
                                  json={"sql": "SELECT CAST(? AS INTEGER) AS v", **FUTURE})
        assert prepared.status_code == 201, prepared.text
        executed = chargewire.http.post(
            url=f"{chargewire.session_url(session_id)}/prepared/{prepared.json()['prepared_id']}/execute",
            headers=chargewire.headers(Accept=ARROW_STREAM), json={"parameters": [7], **FUTURE})
        assert executed.status_code == 200, executed.text

        ingests = f"{chargewire.session_url(session_id)}/ingests"
        opened = chargewire.http.post(url=ingests, headers=chargewire.headers(),
                                json={"table": "ext_ignored", "mode": "create", **FUTURE})
        assert opened.status_code == 201, opened.text
        ingest_id = opened.json()["ingest_id"]
        part = chargewire.http.post(url=f"{ingests}/{ingest_id}/parts/0",
                              headers=chargewire.headers(**{"Content-Type": ARROW_STREAM}),
                              content=arrow_body(table=pa.table({"v": [1, 2, 3]})))
        assert part.status_code == 200, part.text
        committed = chargewire.http.post(url=f"{ingests}/{ingest_id}/commit", headers=chargewire.headers(),
                                   json={"part_count": 1, **FUTURE})
        assert committed.status_code == 200 and committed.json()["rows_ingested"] == 3, committed.text
    finally:
        chargewire.http.delete(url=chargewire.session_url(session_id), headers=chargewire.headers())


def bits_of_randomness(ids: list[str]) -> float:
    """A lower-bound estimate: what follows the ids' common prefix (a type marker like
    "s_"), at the bits per character of the smallest usual alphabet its characters fit."""
    prefix = 0
    while all(len(i) > prefix for i in ids) and len({i[prefix] for i in ids}) == 1:
        prefix += 1
    rest = [i[prefix:] for i in ids]
    alphabet = set("".join(rest))
    if alphabet <= set("0123456789abcdef") or alphabet <= set("0123456789ABCDEF"):
        per_char = 4.0
    elif alphabet <= set("0123456789abcdefghijklmnopqrstuvwxyz"):
        per_char = math.log2(36)
    else:
        per_char = 6.0  # base64url, or anything wider: at most this much per character
    return min(len(r) for r in rest) * per_char


def test_ids_are_long_random_strings(chargewire):
    # §2.3: at most 128 characters of [A-Za-z0-9._~-], with at least 128 bits of randomness.
    # A black box can't measure entropy; it can refuse ids too short or too regular to hold it.
    sessions, statements, prepared, ingests = [], [], [], []
    try:
        for _ in range(12):
            session_id = chargewire.new_session()
            sessions.append(session_id)
            statements.append(chargewire.execute(session_id=session_id, sql="SELECT 1")["statement_id"])
            answer = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/prepared", headers=chargewire.headers(),
                                    json={"sql": "SELECT 1"})
            assert answer.status_code == 201, answer.text
            prepared.append(answer.json()["prepared_id"])
            opened = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/ingests", headers=chargewire.headers(),
                                    json={"table": "ids_t", "mode": "create"})
            assert opened.status_code == 201, opened.text
            ingests.append(opened.json()["ingest_id"])
        for kind, ids in (("session", sessions), ("statement", statements), ("prepared", prepared),
                          ("ingest", ingests)):
            assert all(ID.fullmatch(i) for i in ids), (kind, ids)
            assert len(set(ids)) == len(ids), (kind, ids)
            assert bits_of_randomness(ids) >= 128, (kind, ids)
    finally:
        for session_id in sessions:
            chargewire.http.delete(url=chargewire.session_url(session_id), headers=chargewire.headers())


def test_a_query_tag_labels_a_statement_without_changing_it(chargewire, session_id):
    # §2.5: an ASCII JSON object of at most 4,096 bytes; the statement runs as without it.
    tag = '{"job":"nightly-load","run":"2026-10-19","note":"caf\\u00e9"}'
    response = chargewire.submit(session_id=session_id, sql="SELECT 7 AS v", arrow=True, headers={"Chargewire-Query-Tag": tag})
    assert response.status_code == 200, response.text
    longest = '{"k":"' + "x" * (4096 - len('{"k":""}')) + '"}'
    assert len(longest) == 4096
    response = chargewire.submit(session_id=session_id, sql="SELECT 8 AS v", arrow=True,
                           headers={"Chargewire-Query-Tag": longest})
    assert response.status_code == 200, response.text
    ingest = chargewire.http.post(url=f"{chargewire.session_url(session_id)}/ingest",
                            params={"table": "tagged_ingest", "mode": "create"},
                            headers=chargewire.headers(**{"Content-Type": ARROW_STREAM, "Chargewire-Query-Tag": tag}),
                            content=arrow_body(table=pa.table({"v": [1]})))
    assert ingest.status_code == 200, ingest.text


def test_a_malformed_query_tag_is_refused_or_ignored_never_half_read(chargewire, session_id):
    # A server that reads the tag SHOULD refuse a malformed one with bad_request; one that
    # ignores tags ignores them all. Either way, consistently.
    malformed = ['[1, 2]', '"text"', 'not json', '{"k":"' + "x" * 4096 + '"}', '{"k": "café"}'.encode()]
    answers = []
    for tag in malformed:
        response = chargewire.submit(session_id=session_id, sql="SELECT 1 AS v", arrow=True,
                               headers={"Chargewire-Query-Tag": tag})
        answers.append(response)
    refused = [a for a in answers if a.status_code != 200]
    assert not refused or len(refused) == len(answers), [a.status_code for a in answers]
    for answer in refused:
        assert_problem(response=answer, status=400, code="bad_request")
