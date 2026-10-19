"""Spec §2 and §9.5: HTTP conventions, limits, browser rules."""

import os

import httpx
import pytest

from conftest import start_server, stop_server
from test_discovery_auth import assert_problem


def test_every_response_carries_nosniff_and_a_request_id(server, warp, session_id):
    for response in [httpx.get(url=f"{server.base_url}/warp/v1/health"),
                     warp.http.get(url=warp.session_url(session_id), headers=warp.headers()),
                     warp.http.get(url=warp.session_url("s_missing"), headers=warp.headers())]:
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["warp-request-id"]


def test_unknown_endpoint_and_wrong_method(server, warp):
    assert_problem(response=httpx.get(url=f"{server.base_url}/warp/v1/nothing"), status=404,
                   code="not_found")
    assert_problem(response=httpx.delete(url=f"{server.base_url}/warp/v1/health"), status=405,
                   code="method_not_allowed")


def test_large_headers_are_accepted_up_to_16_kib(warp, session_id):
    response = warp.http.get(url=warp.session_url(session_id),
                             headers=warp.headers(**{"X-Padding": "a" * 15000}))
    assert response.status_code == 200


def test_oversized_header_block_gets_431(server):
    response = httpx.get(url=f"{server.base_url}/warp", headers={"X-Padding": "a" * 100_000})
    assert_problem(response=response, status=431, code="headers_too_large")


def test_every_response_with_query_results_says_no_store(warp, session_id):
    # §9.5: results are not cached, whichever way they are read.
    doc = warp.execute(session_id=session_id, sql="SELECT 1 AS v")
    retained = warp.submit(session_id=session_id, sql="SELECT 2 AS v",
                           headers={"Prefer": "wait=30", "Warp-Retain-Result": "true"}).json()
    responses = {
        "streamed submit": warp.submit(session_id=session_id, sql="SELECT 3 AS v", arrow=True),
        "statement document": warp.http.get(url=warp.statement_url(session_id, doc["statement_id"]),
                                            headers=warp.headers()),
        "result": warp.result(session_id=session_id, statement_id=retained["statement_id"]),
        "metadata": warp.http.get(url=f"{warp.session_url(session_id)}/metadata/catalogs", headers=warp.headers()),
        "server-info": warp.http.get(url=f"{warp.session_url(session_id)}/metadata/server-info",
                                     headers=warp.headers()),
    }
    for what, response in responses.items():
        assert response.status_code == 200, (what, response.text)
        assert response.headers.get("cache-control") == "no-store", (what, response.headers)


def test_no_cross_origin_access_by_default(server):
    response = httpx.get(url=f"{server.base_url}/warp", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in response.headers


def test_oversized_json_body_gets_413(warp, session_id):
    body = b'{"sql": "SELECT 1", "padding": "' + b"a" * (70 * 1024 * 1024) + b'"}'
    response = warp.http.post(url=f"{warp.session_url(session_id)}/statements",
                              headers=warp.headers(**{"Content-Type": "application/json"}), content=body)
    assert_problem(response=response, status=413, code="payload_too_large")


# ---- §9.5: an allowed origin, with credentials ----
#
# These start a server of their own, allowing one origin (warp-duckdb's --cors-origin;
# WARP_SQL_TEST_SERVER_CORS_FLAG names another server's flag, Hardcharger's --cors-origins).

ALLOWED_ORIGIN = "https://app.example"


@pytest.fixture(scope="module")
def cors_server():
    binary = os.environ.get("WARP_SQL_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="the CORS tests start their own server: set WARP_SQL_TEST_SERVER_BINARY, "
                           "or deselect them with -m 'not own_server'")
    flag = os.environ.get("WARP_SQL_TEST_SERVER_CORS_FLAG", "--cors-origin")
    started = start_server(binary=binary, extra_args=[flag, ALLOWED_ORIGIN])
    yield started
    code = stop_server(server=started)
    if code != 0:
        pytest.fail(reason=f"the server did not stop cleanly (exit code {code}):\n{started.log_tail()}",
                    pytrace=False)


@pytest.mark.own_server
def test_an_allowed_origin_may_send_credentials(cors_server):
    # A browser presents a client certificate (§4.4) on a cross-origin request only with
    # credentials "include", and reads the answer only with Access-Control-Allow-Credentials
    # and the origin itself (never "*") in Access-Control-Allow-Origin.
    response = httpx.get(url=f"{cors_server.base_url}/warp", headers={"Origin": ALLOWED_ORIGIN})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == ALLOWED_ORIGIN
    assert response.headers["access-control-allow-credentials"] == "true"
    assert "origin" in response.headers["vary"].lower()
    # An error answer too: the browser shows its code only if it may read it.
    refused = httpx.post(url=f"{cors_server.base_url}/warp/v1/auth/token", headers={"Origin": ALLOWED_ORIGIN},
                         json={"grant_type": "password", "username": "nobody", "password": "wrong"})
    assert_problem(response=refused, status=401, code="invalid_credentials")
    assert refused.headers["access-control-allow-credentials"] == "true"


@pytest.mark.own_server
def test_a_preflight_from_an_allowed_origin_allows_credentials(cors_server):
    response = httpx.request(method="OPTIONS", url=f"{cors_server.base_url}/warp/v1/auth/token", headers={
        "Origin": ALLOWED_ORIGIN, "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "content-type"})
    assert response.status_code in (200, 204)
    assert response.headers["access-control-allow-origin"] == ALLOWED_ORIGIN
    assert response.headers["access-control-allow-credentials"] == "true"
    assert "POST" in response.headers["access-control-allow-methods"]


@pytest.mark.own_server
def test_another_origin_gets_no_cors_headers_at_all(cors_server):
    for method in ("GET", "OPTIONS"):
        response = httpx.request(method=method, url=f"{cors_server.base_url}/warp",
                                 headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
        assert "access-control-allow-origin" not in response.headers
        assert "access-control-allow-credentials" not in response.headers
    # The twin: the allowed origin, on the same server.
    allowed = httpx.get(url=f"{cors_server.base_url}/warp", headers={"Origin": ALLOWED_ORIGIN})
    assert allowed.headers["access-control-allow-origin"] == ALLOWED_ORIGIN


def test_small_requests_answer_without_waiting_for_acks(warp):
    # A server that sends an answer in more than one write must not hold the later ones
    # back for the client's ACK (Nagle's algorithm against a delayed ACK): on Linux that
    # added 40 ms to every small request. Twenty discovery requests on one connection.
    import statistics
    import time

    timings = []
    for _ in range(20):
        started = time.perf_counter()
        response = warp.http.get(url="/warp")
        timings.append(time.perf_counter() - started)
        assert response.status_code == 200
    assert statistics.median(timings) < 0.02, f"median {statistics.median(timings) * 1000:.1f} ms"
