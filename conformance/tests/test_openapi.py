"""Spec §3.3 and Appendix B: the OpenAPI document, the endpoint that serves it, and the
check (openapi_check.py) that holds every response of the suite to it."""

import copy
import json

import httpx
import pytest
from openapi_spec_validator import validate

from openapi_check import (OpenApi, load_document, operations, spec_body_endpoints,
                           spec_endpoints, without_extensions)

ARROW = "application/vnd.apache.arrow.stream"


# ---- The document and the specification agree ----

def test_the_openapi_document_is_valid_openapi_3_1():
    doc = load_document()
    validate(doc)
    assert doc["openapi"].startswith("3.1."), doc["openapi"]


def test_appendix_b_and_the_openapi_document_list_the_same_endpoints():
    in_spec = spec_endpoints()
    in_openapi = operations(doc=load_document())
    assert in_spec - in_openapi == set(), "in Appendix B, missing from spec/openapi.yaml"
    assert in_openapi - in_spec == set(), "in spec/openapi.yaml, missing from Appendix B"
    assert len(in_spec) == 38, sorted(in_spec)


def test_every_endpoint_the_sections_name_is_in_appendix_b():
    named = spec_body_endpoints()
    assert len(named) >= 25, sorted(named)  # the pattern still finds them (29 in 1.0.0)
    assert named - spec_endpoints() == set(), "named in the specification, missing from Appendix B"


def test_only_discovery_health_sign_in_and_the_document_need_no_token():
    doc = load_document()
    assert doc["security"] == [{"bearerAuth": []}]
    assert doc["components"]["securitySchemes"]["bearerAuth"] == {
        **doc["components"]["securitySchemes"]["bearerAuth"], "type": "http", "scheme": "bearer"}
    open_ops = {(method.upper(), path) for path, item in doc["paths"].items()
                for method, op in item.items() if isinstance(op, dict) and op.get("security") == []}
    assert open_ops == {("GET", "/warp"), ("GET", "/warp/v1/health"), ("GET", "/warp/v1/openapi.json"),
                        ("POST", "/warp/v1/auth/token"), ("POST", "/warp/v1/auth/oauth/flows"),
                        ("POST", "/warp/v1/auth/oauth/flows/{flow_id}"),
                        ("GET", "/warp/v1/auth/oauth/callback")}


# ---- GET /warp/v1/openapi.json (§3.3) ----

def test_the_server_serves_the_document_if_discovery_says_so(server):
    discovery = httpx.get(url=f"{server.base_url}/warp").json()
    response = httpx.get(url=f"{server.base_url}/warp/v1/openapi.json")  # no credentials
    if "openapi" not in discovery["capabilities"]:
        assert response.status_code == 404, response.text
        assert response.json()["code"] == "not_found"
        assert "openapi" not in discovery.get("links", {}), discovery
        return
    assert discovery["links"]["openapi"] == "/warp/v1/openapi.json", discovery
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].split(";")[0] == "application/json"
    served = response.json()
    validate(served)
    assert served["openapi"].startswith("3.1."), served["openapi"]
    # The same routes as the specification's, and otherwise the published document: a
    # server may change only `servers` and add x- extensions.
    assert operations(doc=served) == spec_endpoints()
    assert without_extensions(served) == without_extensions(load_document())


def test_the_document_needs_no_token_even_with_a_bad_one(server, warp):
    if "openapi" not in httpx.get(url=f"{server.base_url}/warp").json()["capabilities"]:
        pytest.skip("the server does not offer the OpenAPI document (capability openapi)")
    for headers in [{}, {"Authorization": "Bearer forged"}, {"Authorization": f"Bearer {warp.token}"}]:
        response = httpx.get(url=f"{server.base_url}/warp/v1/openapi.json", headers=headers)
        assert response.status_code == 200, (headers, response.text)


def test_the_document_is_cacheable(server):
    if "openapi" not in httpx.get(url=f"{server.base_url}/warp").json()["capabilities"]:
        pytest.skip("the server does not offer the OpenAPI document (capability openapi)")
    response = httpx.get(url=f"{server.base_url}/warp/v1/openapi.json")
    cache_control = response.headers.get("cache-control", "")
    assert "max-age=" in cache_control and "no-store" not in cache_control, cache_control
    etag = response.headers.get("etag")
    assert etag, response.headers
    again = httpx.get(url=f"{server.base_url}/warp/v1/openapi.json", headers={"If-None-Match": etag})
    assert again.status_code == 304, again.text
    assert again.content == b""
    other = httpx.get(url=f"{server.base_url}/warp/v1/openapi.json", headers={"If-None-Match": '"not-it"'})
    assert other.status_code == 200
    assert other.content == response.content


# ---- The check bites: real responses, broken on purpose ----
#
# The suite's other tests pass only because their responses validate. These show the check
# rejects a response that disagrees with the document, and says why.

def real_session_response(warp) -> httpx.Response:
    return warp.http.post(url="/warp/v1/sessions", headers=warp.headers(), json={})


def check(openapi: OpenApi, response: httpx.Response, body: bytes | None = None,
          headers: dict | None = None, status: int | None = None) -> list[str]:
    return openapi.check(
        method=response.request.method, path=response.request.url.path,
        status=status or response.status_code,
        headers=headers if headers is not None else {k.lower(): v for k, v in response.headers.items()},
        body=response.content if body is None else body)


def test_the_check_accepts_a_real_session_response(warp):
    assert check(openapi=OpenApi(), response=real_session_response(warp=warp)) == []


def test_a_field_renamed_in_the_document_fails_a_real_response(warp):
    doc = copy.deepcopy(load_document())
    required = doc["components"]["schemas"]["Session"]["required"]
    required[required.index("session_id")] = "session_ident"
    problems = check(openapi=OpenApi(doc=doc), response=real_session_response(warp=warp))
    assert any("'session_ident' is a required property" in problem for problem in problems), problems


def test_a_response_missing_a_field_fails(warp):
    response = real_session_response(warp=warp)
    body = response.json()
    del body["transaction"]
    problems = check(openapi=OpenApi(), response=response, body=json.dumps(body).encode())
    assert any("'transaction' is a required property" in problem for problem in problems), problems


def test_a_value_outside_its_enumeration_fails(warp):
    response = real_session_response(warp=warp)
    body = {**response.json(), "transaction": "maybe"}
    problems = check(openapi=OpenApi(), response=response, body=json.dumps(body).encode())
    assert any("'maybe' is not one of" in problem for problem in problems), problems


def test_an_undocumented_status_fails(warp):
    problems = check(openapi=OpenApi(), response=real_session_response(warp=warp), status=202)
    assert any("status 202 is not documented" in problem for problem in problems), problems


def test_an_undocumented_content_type_fails(warp):
    response = real_session_response(warp=warp)
    headers = {**{k.lower(): v for k, v in response.headers.items()}, "content-type": ARROW}
    problems = check(openapi=OpenApi(), response=response, headers=headers)
    assert any(f"content type {ARROW} is not documented" in problem for problem in problems), problems


def test_a_missing_required_header_fails(warp):
    response = real_session_response(warp=warp)
    headers = {k.lower(): v for k, v in response.headers.items() if k.lower() != "location"}
    problems = check(openapi=OpenApi(), response=response, headers=headers)
    assert any("the required header Location is missing" in problem for problem in problems), problems


def test_a_problem_document_must_agree_with_its_status(server):
    response = httpx.get(url=f"{server.base_url}/warp/v1/sessions/nope")  # 401 invalid_token
    assert response.status_code == 401
    body = {**response.json(), "status": 400}
    problems = check(openapi=OpenApi(), response=response, body=json.dumps(body).encode())
    assert any("status 400 is not the response's 401" in problem for problem in problems), problems
    body = {**response.json(), "type": "about:blank"}
    problems = check(openapi=OpenApi(), response=response, body=json.dumps(body).encode())
    assert any("does not match" in problem or "does not name its code" in problem
               for problem in problems), problems
