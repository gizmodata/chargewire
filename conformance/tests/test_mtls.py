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
                     args=[*tls_args(pki=pki, crl=pki.crl_path), "--mtls-admin-identities", ",".join(KNOWN)])
    yield server
    finish(server=server)


@pytest.fixture(scope="module")
def optional(pki):
    """Client certificates optional: a client may connect without one."""
    server = started(pki=pki, probe=None,
                     args=[*tls_args(pki=pki), "--mtls-require-client-cert=false",
                           "--mtls-admin-identities", "alice@example.com,bob@example.com"])
    yield server
    finish(server=server)


@pytest.fixture(scope="module")
def matching(pki):
    """Identity from the subject CN, and a password sign-in must match it."""
    username = os.environ.get("WARP_SQL_TEST_SERVER_USERNAME", "warp")
    pki.warp_cn = pki.ca.issue(file_name="cn-user", common_name=username)
    matched = started(pki=pki, probe=pki.warp_cn,
                      args=[*tls_args(pki=pki), "--mtls-identity-source", "subject_cn",
                            "--mtls-require-identity-match", "--mtls-admin-identities", username])
    yield matched
    finish(server=matched)


@pytest.fixture(scope="module")
def spiffe(pki):
    """Identity from the URI subject alternative name (a SPIFFE ID)."""
    server = started(pki=pki, probe=pki.spiffe,
                     args=[*tls_args(pki=pki), "--mtls-identity-source", "san_uri",
                           "--mtls-admin-identities", "spiffe://example.org/ns/etl/sa/loader"])
    yield server
    finish(server=server)


def client(server: ServerUnderTest, pki: Pki, cert) -> httpx.Client:
    """One keep-alive connection (at most), presenting `cert` (None: no certificate)."""
    return httpx.Client(base_url=server.base_url, verify=pki.context(client=cert), timeout=60,
                        limits=httpx.Limits(max_connections=1, max_keepalive_connections=1))


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
    assert_problem(response=response, status=400, code="bad_request")


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
                     args=[*tls_args(pki=pki, crl=crl), "--mtls-admin-identities", ",".join(KNOWN)])
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
            deadline = time.monotonic() + 30
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
