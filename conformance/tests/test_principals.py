"""Spec §9.2 and §9.3: every handle is bound to its principal, and §4.4: a forwarded
certificate header is no certificate.

Two principals need two identities. Certificate sign-in gives them to any server the
suite can start with mutual TLS (alice@example.com and bob@example.com, each its own
principal), so these tests run on the mutual-TLS servers of test_mtls.py (marker mtls,
CHARGEWIRE_TEST_SERVER_BINARY).
"""

import base64
import io
import urllib.parse

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from cryptography.hazmat.primitives import serialization

from test_discovery_auth import assert_problem
from test_mtls import bearer, certificate_grant, certificate_token, client, new_session, optional, pki, strict  # noqa: F401
from chargewire import ARROW_STREAM, read_table

pytestmark = pytest.mark.mtls


def arrow_body(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    with ipc.new_stream(sink=sink, schema=table.schema) as writer:
        writer.write_table(table=table)
    return sink.getvalue()


def wait_done(http, token: str, url: str) -> dict:
    for _ in range(60):
        doc = http.get(url=url, headers={**bearer(token=token), "Prefer": "wait=5"}).json()
        if doc["state"] in ("succeeded", "failed", "cancelled"):
            return doc
    raise AssertionError(doc)


def test_another_principals_handles_are_not_found(pki, strict):  # noqa: F811
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        token = certificate_token(http=alice)
        session_id = new_session(http=alice, token=token)
        base = f"/chargewire/v1/sessions/{session_id}"
        # A retained result, a prepared statement and an open ingest of alice's.
        submitted = alice.post(url=f"{base}/statements", json={"sql": "SELECT 42 AS answer"},
                               headers={**bearer(token=token), "Chargewire-Retain-Result": "true"})
        assert submitted.status_code == 201, submitted.text
        statement_id = submitted.json()["statement_id"]
        doc = wait_done(http=alice, token=token, url=f"{base}/statements/{statement_id}")
        assert doc["state"] == "succeeded" and doc["result"]["retained"] is True, doc
        prepared = alice.post(url=f"{base}/prepared", headers=bearer(token=token), json={"sql": "SELECT 1 AS one"})
        assert prepared.status_code == 201, prepared.text
        prepared_id = prepared.json()["prepared_id"]
        opened = alice.post(url=f"{base}/ingests", headers=bearer(token=token), json={"table": "alices", "mode": "create"})
        assert opened.status_code == 201, opened.text
        ingest_id = opened.json()["ingest_id"]

    statement = f"{base}/statements/{statement_id}"
    rows = arrow_body(table=pa.table({"v": [1]}))
    with client(server=strict, pki=pki, cert=pki.bob) as bob:
        bob_token = certificate_token(http=bob)
        h = bearer(token=bob_token)
        attempts = {
            "GET session": bob.get(url=base, headers=h),
            "PATCH session": bob.patch(url=base, headers=h, json={"options": {}}),
            "submit": bob.post(url=f"{base}/statements", headers=h, json={"sql": "SELECT 1"}),
            "GET statement": bob.get(url=statement, headers=h),
            "GET retained result": bob.get(url=f"{statement}/result", headers=h),
            "cancel statement": bob.post(url=f"{statement}/cancel", headers=h),
            "prepare": bob.post(url=f"{base}/prepared", headers=h, json={"sql": "SELECT 1"}),
            "execute prepared": bob.post(url=f"{base}/prepared/{prepared_id}/execute", headers=h, json={}),
            "GET ingest": bob.get(url=f"{base}/ingests/{ingest_id}", headers=h),
            "ingest part": bob.post(url=f"{base}/ingests/{ingest_id}/parts/0", content=rows,
                                    headers={**h, "Content-Type": ARROW_STREAM}),
            "commit ingest": bob.post(url=f"{base}/ingests/{ingest_id}/commit", headers=h, json={"part_count": 1}),
            "bulk ingest": bob.post(url=f"{base}/ingest", params={"table": "bobs"}, content=rows,
                                    headers={**h, "Content-Type": ARROW_STREAM}),
            "metadata": bob.get(url=f"{base}/metadata/catalogs", headers=h),
            "server-info": bob.get(url=f"{base}/metadata/server-info", headers=h),
            "commit": bob.post(url=f"{base}/commit", headers=h),
            "cancel session": bob.post(url=f"{base}/cancel", headers=h),
            "abort ingest": bob.delete(url=f"{base}/ingests/{ingest_id}", headers=h),
            "close prepared": bob.delete(url=f"{base}/prepared/{prepared_id}", headers=h),
            "DELETE statement": bob.delete(url=statement, headers=h),
            "DELETE session": bob.delete(url=base, headers=h),
        }
        for what, response in attempts.items():
            # The same 404 as a session that doesn't exist (§9.2): nothing to probe.
            try:
                assert_problem(response=response, status=404, code="session_not_found")
            except AssertionError as failure:
                raise AssertionError(f"{what}: {failure}") from None
        never = bob.get(url="/chargewire/v1/sessions/s_" + "x" * 30, headers=h)
        assert_problem(response=never, status=404, code="session_not_found")
        # In bob's own session, alice's ids name nothing.
        own = new_session(http=bob, token=bob_token)
        mine = f"/chargewire/v1/sessions/{own}"
        assert_problem(response=bob.get(url=f"{mine}/statements/{statement_id}", headers=h),
                       status=404, code="statement_not_found")
        assert_problem(response=bob.post(url=f"{mine}/prepared/{prepared_id}/execute", headers=h, json={}),
                       status=404, code="prepared_not_found")
        assert_problem(response=bob.get(url=f"{mine}/ingests/{ingest_id}", headers=h),
                       status=404, code="ingest_not_found")

    # The twin: nothing bob did touched alice's handles, and she still uses them all.
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        h = bearer(token=token)
        assert alice.get(url=base, headers=h).status_code == 200
        result = alice.get(url=f"{statement}/result", headers=h)
        assert result.status_code == 200, result.text
        assert read_table(content=result.content).to_pylist() == [{"answer": 42}]
        executed = alice.post(url=f"{base}/prepared/{prepared_id}/execute", headers={**h, "Accept": ARROW_STREAM},
                              json={})
        assert executed.status_code == 200 and read_table(content=executed.content).to_pylist() == [{"one": 1}]
        assert alice.get(url=f"{base}/ingests/{ingest_id}", headers=h).json()["state"] == "open"
        assert alice.delete(url=base, headers=h).status_code == 204


def forwarded_headers(certificate) -> dict[str, str]:
    """The headers proxies use to pass a client certificate on, each carrying this one."""
    pem = certificate.public_bytes(encoding=serialization.Encoding.PEM).decode().strip()
    der = certificate.public_bytes(encoding=serialization.Encoding.DER)
    return {
        "X-Forwarded-Client-Cert": f'Hash=x;Cert="{urllib.parse.quote(pem)}";Subject="CN=alice"',
        "X-SSL-Client-Cert": urllib.parse.quote(pem),
        "X-Client-Cert": pem.replace("\n", " "),
        "SSL-Client-Cert": pem.replace("\n", " "),
        "X-ARR-ClientCert": base64.b64encode(der).decode(),
        "Client-Cert": f":{base64.b64encode(der).decode()}:",  # RFC 9440
    }


def test_a_forwarded_certificate_header_is_no_certificate(pki, optional):  # noqa: F811
    # §4.4: a server not configured to trust a proxy's header takes no identity from it.
    headers = forwarded_headers(certificate=pki.alice.cert)
    # A connection of its own for each: a server closes one that keeps failing to sign in.
    for name, value in [*headers.items(), ("all of them", None)]:
        with client(server=optional, pki=pki, cert=None) as anonymous:
            response = anonymous.post(url="/chargewire/v1/auth/token", json={"grant_type": "client_certificate"},
                                      headers=headers if value is None else {name: value})
            try:
                assert_problem(response=response, status=401, code="certificate_required")
            except AssertionError as failure:
                raise AssertionError(f"{name}: {failure}") from None
    # Nor does it let a token bound to that certificate through.
    with client(server=optional, pki=pki, cert=pki.alice) as alice:
        token = certificate_token(http=alice)
    with client(server=optional, pki=pki, cert=None) as anonymous:
        response = anonymous.post(url="/chargewire/v1/sessions", headers={**bearer(token=token), **headers}, json={})
        assert_problem(response=response, status=401, code="certificate_mismatch")
    # The twin: the real certificate signs in.
    with client(server=optional, pki=pki, cert=pki.alice) as alice:
        assert certificate_grant(http=alice).status_code == 200
