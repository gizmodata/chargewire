"""Spec §2 and §9.5: HTTP conventions, limits, browser rules."""

import httpx

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
    assert response.status_code == 431


def test_no_cross_origin_access_by_default(server):
    response = httpx.get(url=f"{server.base_url}/warp", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in response.headers
