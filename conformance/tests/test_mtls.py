"""Spec §2.1 and §4.4: mutual TLS that carries identity. The client_certificate grant,
certificate-bound tokens, revocation, identity match.

These tests start servers of their own with TLS, from WARP_SQL_TEST_SERVER_BINARY. The
flags are warp-duckdb's, which Hardcharger shares for the --mtls-* options; only the flag
naming the client CA differs (WARP_SQL_TEST_SERVER_MTLS_FLAG, default --tls-client-ca;
Hardcharger's is --mtls-ca-cert). Against a server given by URL they cannot run: deselect
them with `-m "not mtls"`, visibly, rather than let them skip.
"""

import concurrent.futures
import json
import os
import socket
import ssl
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pyarrow as pa
import pytest

from conftest import ServerUnderTest, start_server, stop_server
from pki import Pki
from test_discovery_auth import assert_problem
from warp import ARROW_STREAM, Warp, read_batches

pytestmark = pytest.mark.mtls

KNOWN = ["alice@example.com", "bob@example.com", "carol@example.com", "dave@example.com"]
EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


@pytest.fixture(scope="module")
def pki(tmp_path_factory) -> Pki:
    return Pki(directory=tmp_path_factory.mktemp("pki"))


def binary() -> str:
    path = os.environ.get("WARP_SQL_TEST_SERVER_BINARY")
    if not path:
        pytest.fail(reason="the mutual-TLS tests start their own servers: set WARP_SQL_TEST_SERVER_BINARY, "
                           "or deselect them with -m 'not mtls'")
    return path


def tls_args(pki: Pki, crl: Path | None = None) -> list[str]:
    ca_flag = os.environ.get("WARP_SQL_TEST_SERVER_MTLS_FLAG", "--tls-client-ca")
    args = ["--tls-cert", str(pki.server.cert_path), "--tls-key", str(pki.server.key_path),
            ca_flag, str(pki.ca.cert_path)]
    if crl is not None:
        args += ["--mtls-crl-file", str(crl)]
    return args


def started(pki: Pki, args: list[str], probe) -> ServerUnderTest:
    return start_server(binary=binary(), extra_args=args, scheme="https", verify=pki.context(client=probe))


def finish(server: ServerUnderTest) -> None:
    code = stop_server(server=server)
    if code != 0:
        pytest.fail(reason=f"the server did not stop cleanly (exit code {code}):\n{server.log_tail()}",
                    pytrace=False)


@pytest.fixture(scope="module")
def strict(pki):
    """Client certificates required; e-mail identities; carol's certificate revoked."""
    server = started(pki=pki, probe=pki.alice,
                     args=[*tls_args(pki=pki, crl=pki.crl_path), "--mtls-certificate-sign-in", "--mtls-admin-identities", ",".join(KNOWN)])
    yield server
    finish(server=server)


@pytest.fixture(scope="module")
def optional(pki):
    """Client certificates optional: a client may connect without one."""
    server = started(pki=pki, probe=None,
                     args=[*tls_args(pki=pki), "--mtls-require-client-cert=false",
                           "--mtls-certificate-sign-in", "--mtls-admin-identities", "alice@example.com,bob@example.com"])
    yield server
    finish(server=server)


@pytest.fixture(scope="module")
def matching(pki):
    """Identity from the subject CN, and a password sign-in must match it."""
    username = os.environ.get("WARP_SQL_TEST_SERVER_USERNAME", "warp")
    pki.warp_cn = pki.ca.issue(file_name="cn-user", common_name=username)
    matched = started(pki=pki, probe=pki.warp_cn,
                      args=[*tls_args(pki=pki), "--mtls-identity-source", "subject_cn",
                            "--mtls-require-identity-match", "--mtls-certificate-sign-in", "--mtls-admin-identities", username])
    yield matched
    finish(server=matched)


@pytest.fixture(scope="module")
def spiffe(pki):
    """Identity from the URI subject alternative name (a SPIFFE ID)."""
    server = started(pki=pki, probe=pki.spiffe,
                     args=[*tls_args(pki=pki), "--mtls-identity-source", "san_uri",
                           "--mtls-certificate-sign-in", "--mtls-admin-identities", "spiffe://example.org/ns/etl/sa/loader"])
    yield server
    finish(server=server)


def client(server: ServerUnderTest, pki: Pki, cert) -> httpx.Client:
    """One keep-alive connection (at most), presenting `cert` (None: no certificate)."""
    return httpx.Client(base_url=server.base_url, verify=pki.context(client=cert), timeout=60,
                        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1,
                                            keepalive_expiry=600))


def certificate_grant(http: httpx.Client) -> httpx.Response:
    return http.post(url="/warp/v1/auth/token", json={"grant_type": "client_certificate"})


def certificate_token(http: httpx.Client) -> str:
    response = certificate_grant(http=http)
    assert response.status_code == 200, response.text
    doc = response.json()
    assert doc["token_type"] == "Bearer" and doc["expires_in"] > 0
    return doc["access_token"]


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def new_session(http: httpx.Client, token: str) -> str:
    response = http.post(url="/warp/v1/sessions", headers=bearer(token=token), json={})
    assert response.status_code == 201, response.text
    return response.json()["session_id"]


def handshake_refusal(server: ServerUnderTest, context: ssl.SSLContext) -> str | None:
    """Connects and sends one request; returns the TLS failure's reason, or None when the
    server answered (it accepted the connection)."""
    url = urlparse(server.base_url)
    try:
        with socket.create_connection(address=(url.hostname, url.port), timeout=10) as raw:
            with context.wrap_socket(sock=raw, server_hostname=url.hostname) as tls:
                tls.sendall(b"GET /warp HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                answer = tls.recv(4096)
                return None if answer.startswith(b"HTTP/1.1 ") else f"no answer ({answer!r})"
    except ssl.SSLError as error:
        return f"{error.reason}: {error}"
    except OSError as error:
        return f"connection failed: {error}"


def raw_request(server: ServerUnderTest, context: ssl.SSLContext, method: str, path: str,
                token: str | None = None, body: bytes = b"", session: ssl.SSLSession | None = None):
    """One HTTP/1.1 request on a new TLS connection (resuming `session` if given). Returns
    (status, body, the connection's TLS session, whether it was resumed)."""
    url = urlparse(server.base_url)
    with socket.create_connection(address=(url.hostname, url.port), timeout=10) as raw:
        with context.wrap_socket(sock=raw, server_hostname=url.hostname, session=session) as tls:
            head = [f"{method} {path} HTTP/1.1", "Host: localhost", "Connection: close",
                    f"Content-Length: {len(body)}"]
            if body:
                head.append("Content-Type: application/json")
            if token:
                head.append(f"Authorization: Bearer {token}")
            tls.sendall("\r\n".join(head).encode() + b"\r\n\r\n" + body)
            data = b""
            while chunk := tls.recv(65536):
                data += chunk
            status = int(data.split(b" ", 2)[1])
            # TLS 1.3 sends its session ticket after the handshake: read it with the answer.
            return status, data.split(b"\r\n\r\n", 1)[1], tls.session, tls.session_reused


# ---- Discovery ----

def test_discovery_offers_certificate_sign_in(pki, strict):
    with client(server=strict, pki=pki, cert=pki.alice) as http:
        doc = http.get(url="/warp").json()
    assert "mtls_identity" in doc["capabilities"]
    assert "client_certificate" in doc["auth"]["grant_types"]


def test_a_server_without_client_cas_offers_no_certificate_sign_in(server):
    doc = httpx.get(url=f"{server.base_url}/warp").json()
    assert "mtls_identity" not in doc["capabilities"]
    assert "client_certificate" not in doc["auth"]["grant_types"]
    response = httpx.post(url=f"{server.base_url}/warp/v1/auth/token", json={"grant_type": "client_certificate"})
    assert_problem(response=response, status=400, code="unsupported_grant_type")


def test_certificate_sign_in_is_opt_in(pki):
    # Client CAs alone make mutual TLS admit connections only; certificates sign no one in.
    server = started(pki=pki, probe=pki.alice, args=tls_args(pki=pki))
    try:
        with client(server=server, pki=pki, cert=pki.alice) as http:
            doc = http.get(url="/warp").json()
            assert "mtls_identity" not in doc["capabilities"]
            assert "client_certificate" not in doc["auth"]["grant_types"]
            assert_problem(response=certificate_grant(http=http), status=400, code="unsupported_grant_type")
            # A password still signs in over the certificate's connection.
            assert password_grant(http=http, server=server).status_code == 200
    finally:
        finish(server=server)


# ---- Signing in with a certificate ----

def test_certificate_sign_in_needs_no_password(pki, strict):
    with client(server=strict, pki=pki, cert=pki.alice) as http:
        token = certificate_token(http=http)
        session_id = new_session(http=http, token=token)
        response = http.post(url=f"/warp/v1/sessions/{session_id}/statements",
                             headers={**bearer(token=token), "Accept": ARROW_STREAM}, json={"sql": "SELECT 42 AS a"})
        assert response.status_code == 200, response.text
        assert pa.Table.from_batches(batches=read_batches(content=response.content)).to_pylist() == [{"a": 42}]


def test_no_certificate_is_refused_at_the_handshake_when_required(pki, strict):
    reason = handshake_refusal(server=strict, context=pki.context(client=None))
    assert reason is not None and "CERTIFICATE_REQUIRED" in reason, reason
    # The twin: the same connection with a certificate is accepted.
    assert handshake_refusal(server=strict, context=pki.context(client=pki.alice)) is None


def test_no_certificate_cannot_use_the_grant_when_optional(pki, optional):
    with client(server=optional, pki=pki, cert=None) as http:
        assert_problem(response=certificate_grant(http=http), status=401, code="client_certificate_required")
        # Without a certificate a client still signs in with its password.
        response = http.post(url="/warp/v1/auth/token",
                             json={"grant_type": "password", "username": optional.username,
                                   "password": optional.password})
        assert response.status_code == 200, response.text
    with client(server=optional, pki=pki, cert=pki.alice) as http:
        certificate_token(http=http)


def test_a_certificate_from_another_ca_is_refused_at_the_handshake(pki, strict):
    reason = handshake_refusal(server=strict, context=pki.context(client=pki.alice_other_ca))
    assert reason is not None and "UNKNOWN_CA" in reason, reason
    assert handshake_refusal(server=strict, context=pki.context(client=pki.alice)) is None


def test_a_certificate_that_does_not_verify_is_refused_even_when_certificates_are_optional(pki, optional):
    # Optional means a client may present none, never that a bad one is ignored.
    reason = handshake_refusal(server=optional, context=pki.context(client=pki.alice_other_ca))
    assert reason is not None and "UNKNOWN_CA" in reason, reason
    assert handshake_refusal(server=optional, context=pki.context(client=None)) is None
    assert handshake_refusal(server=optional, context=pki.context(client=pki.alice)) is None


def test_an_unknown_identity_is_refused(pki, strict):
    with client(server=strict, pki=pki, cert=pki.mallory) as http:
        problem = assert_problem(response=certificate_grant(http=http), status=401,
                                 code="unknown_certificate_identity")
    assert "mallory@example.com" in problem["detail"]
    with client(server=strict, pki=pki, cert=pki.bob) as http:
        certificate_token(http=http)


def test_a_certificate_with_two_identities_is_refused_as_ambiguous(pki, strict):
    with client(server=strict, pki=pki, cert=pki.two_emails) as http:
        problem = assert_problem(response=certificate_grant(http=http), status=401,
                                 code="unknown_certificate_identity")
    assert "exactly one" in problem["detail"], problem


def test_a_certificate_without_that_kind_of_identity_is_refused(pki, strict):
    # Its common name is an e-mail address, but the server maps the e-mail SAN only.
    with client(server=strict, pki=pki, cert=pki.cn_only) as http:
        problem = assert_problem(response=certificate_grant(http=http), status=401,
                                 code="unknown_certificate_identity")
    assert "no e-mail" in problem["detail"], problem


def test_an_identity_with_a_control_character_is_refused(pki, strict):
    with client(server=strict, pki=pki, cert=pki.control) as http:
        problem = assert_problem(response=certificate_grant(http=http), status=401,
                                 code="unknown_certificate_identity")
    assert "control characters" in problem["detail"], problem


def test_a_uri_identity_signs_in_spiffe_ids(pki, spiffe):
    with client(server=spiffe, pki=pki, cert=pki.spiffe) as http:
        new_session(http=http, token=certificate_token(http=http))
    with client(server=spiffe, pki=pki, cert=pki.spiffe_other) as http:
        assert_problem(response=certificate_grant(http=http), status=401, code="unknown_certificate_identity")
    with client(server=spiffe, pki=pki, cert=pki.alice) as http:
        problem = assert_problem(response=certificate_grant(http=http), status=401,
                                 code="unknown_certificate_identity")
    assert "no URI" in problem["detail"], problem


# ---- Certificate-bound tokens ----

def test_a_bound_token_fails_over_another_certificate(pki, strict):
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        token = certificate_token(http=alice)
        session_id = new_session(http=alice, token=token)
        assert alice.get(url=f"/warp/v1/sessions/{session_id}", headers=bearer(token=token)).status_code == 200
    with client(server=strict, pki=pki, cert=pki.bob) as bob:
        for response in [bob.get(url=f"/warp/v1/sessions/{session_id}", headers=bearer(token=token)),
                         bob.post(url="/warp/v1/sessions", headers=bearer(token=token), json={})]:
            assert_problem(response=response, status=401, code="certificate_mismatch")
            assert response.headers["www-authenticate"] == "Bearer"


def test_a_bound_token_fails_without_a_certificate(pki, optional):
    with client(server=optional, pki=pki, cert=pki.alice) as alice:
        token = certificate_token(http=alice)
        session_id = new_session(http=alice, token=token)
    with client(server=optional, pki=pki, cert=None) as anonymous:
        response = anonymous.get(url=f"/warp/v1/sessions/{session_id}", headers=bearer(token=token))
        assert_problem(response=response, status=401, code="certificate_mismatch")
    with client(server=optional, pki=pki, cert=pki.alice) as alice:
        assert alice.get(url=f"/warp/v1/sessions/{session_id}", headers=bearer(token=token)).status_code == 200


def test_a_password_token_is_not_bound(pki, strict):
    # The binding comes from the client_certificate grant only (spec §9.6).
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        response = alice.post(url="/warp/v1/auth/token",
                              json={"grant_type": "password", "username": strict.username,
                                    "password": strict.password})
        assert response.status_code == 200, response.text
        token = response.json()["access_token"]
    with client(server=strict, pki=pki, cert=pki.bob) as bob:
        new_session(http=bob, token=token)


def test_partitions_need_the_bound_certificate_on_every_connection(pki, strict):
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        token = certificate_token(http=alice)
        session_id = new_session(http=alice, token=token)
        response = alice.post(url=f"/warp/v1/sessions/{session_id}/statements",
                              headers={**bearer(token=token), "Prefer": "respond-async, wait=60",
                                       "Warp-Retain-Result": "true", "Warp-Result-Partitions": "4"},
                              json={"sql": "SELECT i FROM range(1000000) t(i)"})
        assert response.status_code in (200, 201, 202), response.text
        statement_id = response.json()["statement_id"]
        statement_url = f"/warp/v1/sessions/{session_id}/statements/{statement_id}"
        deadline = time.monotonic() + 60
        doc = response.json()
        while doc["state"] not in ("succeeded", "failed", "cancelled") and time.monotonic() < deadline:
            doc = alice.get(url=statement_url, headers={**bearer(token=token), "Prefer": "wait=10"}).json()
        assert doc["state"] == "succeeded", doc
        partitions = len(doc["result"]["partitions"])
        assert partitions > 1, "a million rows should be split over several partitions"

    def read(index: int, cert) -> httpx.Response:
        with client(server=strict, pki=pki, cert=cert) as http:
            return http.get(url=f"{statement_url}/result", params={"partition": str(index)},
                            headers=bearer(token=token))

    with concurrent.futures.ThreadPoolExecutor(max_workers=partitions) as pool:
        answers = list(pool.map(lambda i: read(index=i, cert=pki.alice), range(partitions)))
    rows = 0
    for answer in answers:
        assert answer.status_code == 200, answer.text
        assert answer.content.endswith(EOS)
        rows += sum(batch.num_rows for batch in read_batches(content=answer.content))
    assert rows == 1_000_000
    # A partition read over another certificate, or none, is refused.
    assert_problem(response=read(index=1, cert=pki.bob), status=401, code="certificate_mismatch")


def test_ingest_parts_need_the_bound_certificate_on_every_connection(pki, strict):
    from test_partitioned_ingest import ipc_bytes, rows
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        token = certificate_token(http=alice)
        session_id = new_session(http=alice, token=token)
        base = f"/warp/v1/sessions/{session_id}/ingests"
        opened = alice.post(url=base, headers=bearer(token=token), json={"table": "mtls_parts", "mode": "create"})
        assert opened.status_code == 201, opened.text
        ingest_id = opened.json()["ingest_id"]

    def upload(part: int, cert) -> httpx.Response:
        with client(server=strict, pki=pki, cert=cert) as http:
            return http.post(url=f"{base}/{ingest_id}/parts/{part}", content=ipc_bytes(table=rows(start=part * 1000, count=1000)),
                             headers={**bearer(token=token), "Content-Type": ARROW_STREAM})

    # Each part on a connection of its own; one over bob's certificate is refused, and
    # refused before it touches the ingest.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        answers = list(pool.map(lambda part: upload(part=part, cert=pki.alice), [0, 1]))
    assert [a.status_code for a in answers] == [200, 200], [a.text for a in answers]
    assert_problem(response=upload(part=2, cert=pki.bob), status=401, code="certificate_mismatch")
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        committed = alice.post(url=f"{base}/{ingest_id}/commit", headers=bearer(token=token), json={"parts": 2})
        assert committed.status_code == 200, committed.text
        assert committed.json()["rows_ingested"] == 2000


def test_a_session_keeps_the_binding_of_the_token_that_opened_it(pki, strict):
    # One principal (alice), two certificates of hers: a session opened with one's bound
    # token is used only with tokens bound to that certificate. (A password token is the
    # next test's.)
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        token = certificate_token(http=alice)
        session_id = new_session(http=alice, token=token)
    with client(server=strict, pki=pki, cert=pki.alice_again) as again:
        other_token = certificate_token(http=again)
        url = f"/warp/v1/sessions/{session_id}"
        for response in [again.get(url=url, headers=bearer(token=other_token)),
                         again.post(url=f"{url}/statements", headers=bearer(token=other_token), json={"sql": "SELECT 1"}),
                         again.delete(url=url, headers=bearer(token=other_token))]:
            assert_problem(response=response, status=401, code="certificate_mismatch")
        # Her other certificate opens sessions of its own.
        new_session(http=again, token=other_token)
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        response = alice.post(url=f"/warp/v1/sessions/{session_id}/statements", headers=bearer(token=token),
                              json={"sql": "SELECT 1"})
        assert response.status_code in (200, 201, 202), response.text


def test_a_password_token_cannot_use_a_certificate_session(pki, matching):
    # Identity from the subject CN, which is the password user's name: one principal.
    with client(server=matching, pki=pki, cert=pki.warp_cn) as http:
        cert_token = certificate_token(http=http)
        password = password_grant(http=http, server=matching)
        assert password.status_code == 200, password.text
        password_token = password.json()["access_token"]
        bound_session = new_session(http=http, token=cert_token)
        response = http.get(url=f"/warp/v1/sessions/{bound_session}", headers=bearer(token=password_token))
        assert_problem(response=response, status=401, code="certificate_mismatch")
        assert http.get(url=f"/warp/v1/sessions/{bound_session}", headers=bearer(token=cert_token)).status_code == 200
        # A session opened with the password token is unbound, and only unbound tokens use it:
        # what runs there (a scoped token minted, say) then never runs under a binding it lacks.
        plain_session = new_session(http=http, token=password_token)
        assert http.get(url=f"/warp/v1/sessions/{plain_session}", headers=bearer(token=password_token)).status_code == 200
        response = http.get(url=f"/warp/v1/sessions/{plain_session}", headers=bearer(token=cert_token))
        assert_problem(response=response, status=401, code="certificate_mismatch")


# ---- TLS session resumption ----

def test_a_resumed_session_keeps_its_certificate_and_its_bound_token(pki, strict):
    context = pki.context(client=pki.alice)
    status, body, session, _ = raw_request(server=strict, context=context, method="POST", path="/warp/v1/auth/token",
                                           body=b'{"grant_type":"client_certificate"}')
    assert status == 200, body
    token = json.loads(body)["access_token"]
    status, body, _, reused = raw_request(server=strict, context=context, method="POST", path="/warp/v1/sessions",
                                          token=token, body=b"{}", session=session)
    assert reused, "the server did not resume the TLS session"
    assert status == 201, body


def test_resumption_cannot_change_the_certificate(pki, strict):
    # A resumed session carries the certificate of its first handshake, whatever the
    # client holds now: resuming alice's session with bob's certificate loaded is still
    # alice's connection. Bob's bound token is refused on it; alice's works.
    with client(server=strict, pki=pki, cert=pki.bob) as bob:
        bob_token = certificate_token(http=bob)
    context = pki.context(client=pki.alice)
    status, body, session, _ = raw_request(server=strict, context=context, method="POST", path="/warp/v1/auth/token",
                                           body=b'{"grant_type":"client_certificate"}')
    assert status == 200, body
    alice_token = json.loads(body)["access_token"]
    context.load_cert_chain(certfile=str(pki.bob.cert_path), keyfile=str(pki.bob.key_path))
    status, body, session, reused = raw_request(server=strict, context=context, method="POST",
                                                path="/warp/v1/sessions", token=bob_token, body=b"{}",
                                                session=session)
    assert reused, "the server did not resume the TLS session"
    assert status == 401 and json.loads(body)["code"] == "certificate_mismatch", body
    status, body, _, reused = raw_request(server=strict, context=context, method="POST", path="/warp/v1/sessions",
                                          token=alice_token, body=b"{}", session=session)
    assert reused and status == 201, body


# ---- Revocation ----

def test_a_revoked_certificate_is_refused_at_the_handshake(pki, strict):
    reason = handshake_refusal(server=strict, context=pki.context(client=pki.carol))
    assert reason is not None and "CERTIFICATE_REVOKED" in reason, reason
    # Same CA, same CRL, not revoked:
    assert handshake_refusal(server=strict, context=pki.context(client=pki.dave)) is None


def test_revocation_reaches_open_connections_without_a_restart(pki, tmp_path):
    crl = tmp_path / "crl.pem"
    pki.ca.write_crl(path=crl, revoked=[])
    server = started(pki=pki, probe=pki.alice,
                     args=[*tls_args(pki=pki, crl=crl), "--mtls-certificate-sign-in", "--mtls-admin-identities", ",".join(KNOWN)])
    try:
        # A TLS session dave could resume later (it must not outlive the revocation).
        dave_context = pki.context(client=pki.dave)
        status, _, dave_session, _ = raw_request(server=server, context=dave_context, method="GET", path="/warp")
        assert status == 200
        with client(server=server, pki=pki, cert=pki.dave) as dave:
            token = certificate_token(http=dave)
            session_id = new_session(http=dave, token=token)
            url = f"/warp/v1/sessions/{session_id}"
            assert dave.get(url=url, headers=bearer(token=token)).status_code == 200
            pki.ca.write_crl(path=crl, revoked=[pki.dave.cert])
            # The server looks at the CRL file every few seconds; the open (keep-alive)
            # connection's next request after the reload is refused.
            deadline = time.monotonic() + 60
            while True:
                response = dave.get(url=url, headers=bearer(token=token))
                if response.status_code != 200 or time.monotonic() > deadline:
                    break
                time.sleep(0.5)
            assert_problem(response=response, status=401, code="certificate_revoked")
        reason = handshake_refusal(server=server, context=pki.context(client=pki.dave))
        assert reason is not None and "CERTIFICATE_REVOKED" in reason, reason
        # Nor does a session from before the reload resume: each CRL generation has its own
        # session cache and ticket keys, so dave meets a full handshake and the new CRL.
        with pytest.raises(ssl.SSLError, match="CERTIFICATE_REVOKED"):
            raw_request(server=server, context=dave_context, method="GET", path="/warp", session=dave_session)
        # Everyone else is unaffected by the reload.
        with client(server=server, pki=pki, cert=pki.alice) as alice:
            new_session(http=alice, token=certificate_token(http=alice))
    finally:
        finish(server=server)


# ---- Identity match on password sign-in ----

def password_grant(http: httpx.Client, server: ServerUnderTest, password: str | None = None) -> httpx.Response:
    return http.post(url="/warp/v1/auth/token",
                     json={"grant_type": "password", "username": server.username,
                           "password": server.password if password is None else password})


def test_identity_match_refuses_a_password_for_someone_else(pki, matching):
    with client(server=matching, pki=pki, cert=pki.alice) as alice:
        assert_problem(response=password_grant(http=alice, server=matching), status=401,
                       code="certificate_identity_mismatch")
        # Decided before the password is checked: a wrong one gets the same answer.
        assert_problem(response=password_grant(http=alice, server=matching, password="wrong"), status=401,
                       code="certificate_identity_mismatch")
    with client(server=matching, pki=pki, cert=pki.warp_cn) as same:
        assert password_grant(http=same, server=matching).status_code == 200
        assert_problem(response=password_grant(http=same, server=matching, password="wrong"), status=401,
                       code="invalid_credentials")
        # The subject CN is the identity for the certificate grant here.
        new_session(http=same, token=certificate_token(http=same))


def test_without_identity_match_a_password_signs_in_over_any_certificate(pki, strict):
    with client(server=strict, pki=pki, cert=pki.alice) as alice:
        assert password_grant(http=alice, server=strict).status_code == 200


# ---- Chains: every CA's revocation counts ----

@pytest.fixture
def chain(pki, tmp_path):
    """A root (the server's client CA), an intermediate it signs, a leaf the intermediate
    signs (sent with the intermediate), and the CRL file to fill."""
    from pki import Authority
    from cryptography import x509 as _x509
    inter = Authority(directory=tmp_path, common_name="warp-test-intermediate", parent=pki.ca)
    leaf = inter.issue(file_name="via-intermediate", common_name="alice",
                       sans=[_x509.RFC822Name("alice@example.com")])
    return inter, leaf, tmp_path / "chain.crl.pem"


def chain_server(pki, crl, *extra):
    # Certificates optional, so the harness sees the server come up whatever the CRLs refuse.
    return started(pki=pki, probe=None,
                   args=[*tls_args(pki=pki, crl=crl), *extra, "--mtls-require-client-cert=false",
                         "--mtls-certificate-sign-in",
                         "--mtls-admin-identities", "alice@example.com"])


def test_a_revoked_intermediate_ca_revokes_what_it_issued(pki, chain):
    from pki import write_atomically
    inter, leaf, crl = chain
    # The twin first: neither revoked, both CAs' CRLs present.
    write_atomically(path=crl, data=pki.ca.crl_pem(revoked=[]) + inter.crl_pem(revoked=[]))
    server = chain_server(pki, crl)
    try:
        with client(server=server, pki=pki, cert=leaf) as http:
            certificate_token(http=http)
    finally:
        finish(server=server)
    # The root's CRL revokes the intermediate; the intermediate's own CRL is clean.
    write_atomically(path=crl, data=pki.ca.crl_pem(revoked=[inter.cert]) + inter.crl_pem(revoked=[]))
    server = chain_server(pki, crl)
    try:
        reason = handshake_refusal(server=server, context=pki.context(client=leaf))
        assert reason is not None and "CERTIFICATE_REVOKED" in reason, reason
        assert handshake_refusal(server=server, context=pki.context(client=pki.alice)) is None
    finally:
        finish(server=server)


def test_a_ca_without_a_crl_fails_closed_unless_allowed(pki, chain):
    from pki import write_atomically
    inter, leaf, crl = chain
    # Only the intermediate's CRL: the root (which issued the intermediate) has none.
    write_atomically(path=crl, data=inter.crl_pem(revoked=[]))
    server = chain_server(pki, crl)
    try:
        reason = handshake_refusal(server=server, context=pki.context(client=leaf))
        assert reason is not None and "ALERT" in reason, reason
    finally:
        finish(server=server)
    server = chain_server(pki, crl, "--mtls-crl-allow-missing-ca-crls")
    try:
        with client(server=server, pki=pki, cert=leaf) as http:
            certificate_token(http=http)
        # Allowing a CA above the leaf to lack a CRL never excuses the leaf's issuer: a
        # leaf of the root (whose CRL is missing) is still refused.
        reason = handshake_refusal(server=server, context=pki.context(client=pki.alice))
        assert reason is not None and "ALERT" in reason, reason
    finally:
        finish(server=server)


def test_a_crl_that_passes_its_next_update_fails_open_connections_closed(pki, tmp_path):
    import datetime
    from pki import write_atomically
    crl = tmp_path / "expiring.crl.pem"
    write_atomically(path=crl, data=pki.ca.crl_pem(revoked=[]))
    server = started(pki=pki, probe=pki.alice,
                     args=[*tls_args(pki=pki, crl=crl), "--mtls-certificate-sign-in",
                           "--mtls-admin-identities", ",".join(KNOWN)])
    try:
        with client(server=server, pki=pki, cert=pki.dave) as dave:
            token = certificate_token(http=dave)
            url = f"/warp/v1/sessions/{new_session(http=dave, token=token)}"
            # A CRL that expires soon replaces the long-lived one (however slow the server is
            # to start), and is picked up while still current.
            expires = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(seconds=20)
            write_atomically(path=crl, data=pki.ca.crl_pem(revoked=[], next_update=expires))
            time.sleep(7)
            assert dave.get(url=url, headers=bearer(token=token)).status_code == 200  # the twin, before
            time.sleep(max(0.0, (expires - datetime.datetime.now(tz=datetime.UTC)).total_seconds()) + 1.5)
            # Nothing changed on disk: the open connection is verified again at the CRL's
            # nextUpdate, and an expired CRL verifies nothing.
            response = dave.get(url=url, headers=bearer(token=token))
            problem = assert_problem(response=response, status=401, code="certificate_revoked")
            assert "expired" in problem["detail"], problem
        reason = handshake_refusal(server=server, context=pki.context(client=pki.dave))
        assert reason is not None and "ALERT" in reason, reason
    finally:
        finish(server=server)


# ---- Identities are printable ASCII ----

@pytest.mark.parametrize("common_name", ["warp​", "‮warp", "warp\u0085", "wаrp", "warp "],
                         ids=["zero-width-space", "right-to-left-override", "next-line-c1", "cyrillic-a",
                              "line-separator"])
def test_an_identity_must_be_printable_ascii(pki, matching, common_name):
    odd = pki.ca.issue(file_name=f"cn-{abs(hash(common_name))}", common_name=common_name)
    with client(server=matching, pki=pki, cert=odd) as http:
        problem = assert_problem(response=certificate_grant(http=http), status=401,
                                 code="unknown_certificate_identity")
    assert "printable ASCII" in problem["detail"], problem
    # The value is escaped in the detail: nothing invisible or reordering reaches a reader.
    assert all(0x20 <= ord(c) <= 0x7e for c in problem["detail"]), repr(problem["detail"])
    assert "\\x" in problem["detail"], problem
    with client(server=matching, pki=pki, cert=pki.warp_cn) as http:  # the twin: plain "warp"
        certificate_token(http=http)


# ---- CRL files: deltas, several CRLs per CA, forgeries ----

def crl_server(pki, crl):
    return started(pki=pki, probe=None,
                   args=[*tls_args(pki=pki, crl=crl), "--mtls-require-client-cert=false",
                         "--mtls-certificate-sign-in", "--mtls-admin-identities", ",".join(KNOWN)])


def refused_at_startup(pki, crl) -> str:
    """The server must refuse to start with this CRL file; returns what it said."""
    import subprocess
    out = subprocess.run(args=[binary(), "--host", "127.0.0.1", "--port", "0", *tls_args(pki=pki, crl=crl),
                               "--mtls-certificate-sign-in", "--mtls-admin-identities", "alice@example.com"],
                         env={**os.environ, os.environ.get("WARP_SQL_TEST_SERVER_PASSWORD_ENV", "WARP_DUCKDB_PASSWORD"): "pw"},
                         capture_output=True, text=True, timeout=30)
    assert out.returncode != 0, f"started with {crl.name}: {out.stdout} {out.stderr}"
    return out.stdout + out.stderr


def test_a_delta_crl_is_refused_and_fails_closed(pki, tmp_path):
    # Deltas are not applied (OpenSSL would skip one silently without a Freshest CRL
    # extension), so a file holding one is refused rather than half-read.
    from pki import write_atomically
    crl = tmp_path / "delta.crl.pem"
    delta_file = pki.ca.crl_pem(revoked=[], number=1) + pki.ca.crl_pem(revoked=[pki.alice.cert], number=2, delta_of=1)
    write_atomically(path=crl, data=delta_file)
    assert "delta CRLs are not supported" in refused_at_startup(pki=pki, crl=crl)
    # On a reload the last good CRLs stay in force: dave stays revoked, and the server says why.
    write_atomically(path=crl, data=pki.ca.crl_pem(revoked=[pki.dave.cert], number=1))
    server = crl_server(pki=pki, crl=crl)
    try:
        write_atomically(path=crl, data=delta_file)
        time.sleep(7)
        reason = handshake_refusal(server=server, context=pki.context(client=pki.dave))
        assert reason is not None and "CERTIFICATE_REVOKED" in reason, reason
        with client(server=server, pki=pki, cert=pki.alice) as http:  # the delta's revocation isn't half-applied either
            certificate_token(http=http)
        assert "delta CRLs are not supported" in server.log_tail(lines=200), server.log_tail()
    finally:
        finish(server=server)


@pytest.mark.parametrize("newest_first", [False, True], ids=["older-first", "newer-first"])
def test_the_newest_crl_of_a_ca_wins(pki, tmp_path, newest_first):
    from pki import write_atomically
    older, newer = pki.ca.crl_pem(revoked=[], number=2), pki.ca.crl_pem(revoked=[pki.dave.cert], number=3)
    crl = tmp_path / "two.crl.pem"
    write_atomically(path=crl, data=newer + older if newest_first else older + newer)
    server = crl_server(pki=pki, crl=crl)
    try:
        reason = handshake_refusal(server=server, context=pki.context(client=pki.dave))
        assert reason is not None and "CERTIFICATE_REVOKED" in reason, reason
        assert handshake_refusal(server=server, context=pki.context(client=pki.alice)) is None
    finally:
        finish(server=server)


def test_a_crl_not_signed_by_its_ca_is_refused(pki, tmp_path):
    from pki import write_atomically
    crl = tmp_path / "forged.crl.pem"
    # Names the client CA as its issuer, signed by another key (a newer number, to win).
    write_atomically(path=crl, data=pki.ca.crl_pem(revoked=[], number=9, signer=pki.other_ca.key)
                     + pki.ca.crl_pem(revoked=[pki.dave.cert], number=2))
    assert "not signed by that CA's key" in refused_at_startup(pki=pki, crl=crl)


# ---- Refusal floods ----

def test_a_connection_that_keeps_failing_authentication_is_closed(pki, optional):
    with client(server=optional, pki=pki, cert=None) as http:
        answers = [certificate_grant(http=http) for _ in range(5)]
    for answer in answers:
        assert_problem(response=answer, status=401, code="client_certificate_required")
    assert [a.headers.get("connection") for a in answers][:4] == [None] * 4 or \
        all(a.headers.get("connection") != "close" for a in answers[:4]), [a.headers for a in answers]
    assert answers[4].headers.get("connection") == "close", answers[4].headers
    # A connection that signs in fine is never closed for it (the twin).
    with client(server=optional, pki=pki, cert=pki.alice) as http:
        answers = [certificate_grant(http=http) for _ in range(6)]
    assert all(a.status_code == 200 and a.headers.get("connection") != "close" for a in answers)
