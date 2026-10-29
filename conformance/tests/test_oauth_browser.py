"""Spec §4.3 and §9.7: browser sign-in (capability oauth_browser).

These tests start servers of their own, from CHARGEWIRE_TEST_SERVER_BINARY, configured for a
mock OpenID Connect provider (idp.py) that approves every sign-in at once. How a server is
told about the provider is its own business, so the flags are a template,
CHARGEWIRE_TEST_SERVER_OAUTH_ARGS (default: chargewire-duckdb's), with the placeholders {issuer},
{client_id}, {identity} (the one user allowed to sign in), {base_url} (the server's public
URL, for its redirect URI), {provider} (the display name) and {max_flows}. The client secret
travels in the environment variable CHARGEWIRE_TEST_SERVER_OAUTH_SECRET_ENV names (default
CHARGEWIRE_DUCKDB_OAUTH_CLIENT_SECRET). For Hardcharger:

    CHARGEWIRE_TEST_SERVER_OAUTH_ARGS='--token-allowed-issuer {issuer} --token-allowed-audience {client_id}
      --token-default-role admin --token-authorized-emails {identity} --oauth-client-id {client_id}
      --oauth-base-url {base_url} --oauth-provider-name {provider} --oauth-max-flows {max_flows}'
    CHARGEWIRE_TEST_SERVER_OAUTH_SECRET_ENV=HARDCHARGER_OAUTH_CLIENT_SECRET

Against a server given by URL they cannot run: deselect them with `-m "not oauth"`.
"""

import os
import shlex
import urllib.parse

import httpx
import pytest

from conftest import free_port, start_server, stop_server
from idp import IdentityProvider
from test_discovery_auth import assert_problem
from chargewire import Chargewire

pytestmark = pytest.mark.oauth

PROVIDER = "Conformance IdP"
IDENTITY = "alice@example.com"
MAX_FLOWS = 8
DEFAULT_ARGS = ("--oauth-issuer {issuer} --oauth-client-id {client_id} --oauth-identities {identity} "
                "--oauth-base-url {base_url} --oauth-provider-name {provider} --oauth-max-flows {max_flows}")


@pytest.fixture(scope="module")
def idp():
    provider = IdentityProvider()
    yield provider
    provider.stop()


def start(idp: IdentityProvider, max_flows: int = 1000):
    binary = os.environ.get("CHARGEWIRE_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="the browser sign-in tests start their own servers: set CHARGEWIRE_TEST_SERVER_BINARY, "
                           "or deselect them with -m 'not oauth'")
    port = free_port()
    template = os.environ.get("CHARGEWIRE_TEST_SERVER_OAUTH_ARGS", DEFAULT_ARGS)
    args = [arg.format(issuer=idp.issuer, client_id=idp.client_id, identity=IDENTITY,
                       base_url=f"http://127.0.0.1:{port}", provider=PROVIDER, max_flows=max_flows)
            for arg in shlex.split(template)]
    secret_env = os.environ.get("CHARGEWIRE_TEST_SERVER_OAUTH_SECRET_ENV", "CHARGEWIRE_DUCKDB_OAUTH_CLIENT_SECRET")
    return start_server(binary=binary, extra_args=args, port=port, env={secret_env: idp.client_secret})


@pytest.fixture(scope="module")
def server(idp):
    running = start(idp=idp)
    yield running
    code = stop_server(server=running)
    assert code == 0, running.log_tail()


@pytest.fixture
def http(server):
    with httpx.Client(base_url=server.base_url, timeout=30, follow_redirects=False) as client:
        yield client
    server.check_alive()


@pytest.fixture(autouse=True)
def reset_idp(idp):
    idp.email, idp.deny, idp.nonce_override = IDENTITY, False, None
    yield


def create(http: httpx.Client, body: dict | None = None) -> dict:
    response = http.post(url="/chargewire/v1/auth/oauth/flows", json=body) if body is not None else \
        http.post(url="/chargewire/v1/auth/oauth/flows")
    assert response.status_code == 201, response.text
    assert "no-store" in response.headers["cache-control"]
    flow = response.json()
    assert response.headers["location"].endswith(f"/auth/oauth/flows/{flow['flow_id']}")
    return flow


def sign_in(http: httpx.Client, flow: dict) -> httpx.Response:
    """What a browser does: open authorize_url, follow the provider back to the callback."""
    at_idp = httpx.get(url=flow["authorize_url"], follow_redirects=False)
    assert at_idp.status_code == 302, at_idp.text
    callback = at_idp.headers["location"]
    assert callback.startswith(http.base_url.scheme + "://"), callback
    return http.get(url=callback)


def poll(http: httpx.Client, flow: dict, secret: str | None = None) -> httpx.Response:
    return http.post(url=f"/chargewire/v1/auth/oauth/flows/{flow['flow_id']}",
                     json={"poll_secret": flow["poll_secret"] if secret is None else secret})


def test_discovery_advertises_browser_sign_in(http):
    doc = http.get(url="/chargewire").json()
    assert "oauth_browser" in doc["capabilities"]
    assert doc["auth"]["oauth_browser"]["provider"] == PROVIDER
    # It is not a grant of the token endpoint (§4.1).
    assert "oauth_browser" not in doc["auth"]["grant_types"]


def test_a_flow_completes_once_and_its_token_works(server, http, idp):
    flow = create(http=http)
    assert set(flow) >= {"flow_id", "poll_secret", "authorize_url", "poll_interval_s", "expires_at"}
    assert poll(http=http, flow=flow).json() == {"state": "pending"}

    page = sign_in(http=http, flow=flow)
    assert page.status_code == 200, page.text
    assert page.headers["content-type"].startswith("text/html")
    assert IDENTITY in page.text

    done = poll(http=http, flow=flow)
    assert done.status_code == 200 and "no-store" in done.headers["cache-control"]
    body = done.json()
    assert body["state"] == "complete" and body["token_type"] == "Bearer" and body["expires_in"] > 0, body
    # The token is the server's own: it opens sessions and runs SQL.
    session = http.post(url="/chargewire/v1/sessions", headers={"Authorization": f"Bearer {body['access_token']}"},
                        json={"options": {}})
    assert session.status_code == 201, session.text
    # Exactly once (§4.3.2).
    assert_problem(response=poll(http=http, flow=flow), status=404, code="flow_not_found")


def test_the_authorize_url_carries_state_pkce_and_nonce_but_no_secret(server, http, idp):
    flow = create(http=http)
    url = httpx.URL(flow["authorize_url"])
    assert str(url).startswith(idp.issuer + "/authorize")
    params = url.params
    assert params["response_type"] == "code" and params["client_id"] == idp.client_id
    assert params["code_challenge_method"] == "S256" and len(params["code_challenge"]) == 43
    assert params["nonce"] and params["state"]
    assert params["state"] not in (flow["flow_id"], flow["poll_secret"])
    assert params["redirect_uri"].startswith(server.base_url + "/chargewire/")
    assert flow["poll_secret"] not in str(url)
    sign_in(http=http, flow=flow)
    # The code went back with the PKCE verifier and the redirect URI it was issued for.
    exchange = idp.token_requests[-1]
    assert exchange["code_verifier"] and exchange["redirect_uri"] == params["redirect_uri"]
    assert poll(http=http, flow=flow).json()["state"] == "complete"


def test_polling_needs_the_poll_secret(http):
    flow = create(http=http)
    sign_in(http=http, flow=flow)
    for wrong in ("", "x", flow["flow_id"], flow["poll_secret"][:-1], flow["poll_secret"].upper()):
        assert_problem(response=poll(http=http, flow=flow, secret=wrong), status=404, code="flow_not_found")
    url = f"/chargewire/v1/auth/oauth/flows/{flow['flow_id']}"
    assert_problem(response=http.post(url=url, json={}), status=400, code="bad_request")
    assert_problem(response=http.post(url=url, json={"poll_secret": 7}), status=400, code="bad_request")
    # Not in the URL, not in a header.
    assert_problem(response=http.get(url=url, params={"poll_secret": flow["poll_secret"]}), status=405,
                   code="method_not_allowed")
    assert_problem(response=http.post(url=url, headers={"Authorization": f"Bearer {flow['poll_secret']}"}),
                   status=400, code="bad_request")
    # Still there for the right secret.
    assert poll(http=http, flow=flow).json()["state"] == "complete"


def test_an_unknown_flow_is_not_found(http):
    assert_problem(response=http.post(url="/chargewire/v1/auth/oauth/flows/f_nosuchflow",
                                      json={"poll_secret": "abc"}), status=404, code="flow_not_found")


def test_a_callback_completes_a_flow_once(http, idp):
    flow = create(http=http)
    at_idp = httpx.get(url=flow["authorize_url"], follow_redirects=False)
    callback = at_idp.headers["location"]
    assert http.get(url=callback).status_code == 200
    # A replayed redirect (same code and state) changes nothing, before or after collection.
    assert http.get(url=callback).status_code in (400, 409)
    assert poll(http=http, flow=flow).json()["state"] == "complete"
    assert http.get(url=callback).status_code in (400, 409)
    assert_problem(response=poll(http=http, flow=flow), status=404, code="flow_not_found")


def test_a_callback_with_an_unknown_state_completes_nothing(http):
    flow = create(http=http)
    response = http.get(url="/chargewire/v1/auth/oauth/callback", params={"code": "x", "state": flow["flow_id"]})
    assert response.status_code in (400, 404, 409)
    assert poll(http=http, flow=flow).json() == {"state": "pending"}


def test_callback_answers_are_not_stored_and_run_no_script(http, idp):
    idp.deny = True
    flow = create(http=http)
    page = sign_in(http=http, flow=flow)
    assert "no-store" in page.headers["cache-control"]
    assert page.headers["referrer-policy"] == "no-referrer"
    assert page.headers["x-frame-options"].upper() == "DENY"
    assert "default-src 'none'" in page.headers["content-security-policy"]
    assert "script-src" not in page.headers["content-security-policy"]
    # The provider's error text is escaped (the mock sends HTML in it).
    assert "<b>" not in page.text and "&lt;b&gt;" in page.text


def test_a_refusal_at_the_provider_fails_the_flow(http, idp):
    idp.deny = True
    flow = create(http=http)
    sign_in(http=http, flow=flow)
    body = poll(http=http, flow=flow).json()
    assert body["state"] == "failed" and body["error"]["code"] == "access_denied", body
    assert "The user said no" in body["error"]["detail"]
    assert_problem(response=poll(http=http, flow=flow), status=404, code="flow_not_found")


def test_a_user_who_may_not_sign_in_fails_the_flow(http, idp):
    idp.email = "mallory@example.org"
    flow = create(http=http)
    sign_in(http=http, flow=flow)
    body = poll(http=http, flow=flow).json()
    assert body["state"] == "failed" and body["error"]["code"] == "invalid_credentials", body
    assert "access_token" not in body


def test_an_id_token_for_another_flow_is_refused(http, idp):
    # An ID token whose nonce isn't this flow's (replayed from another sign-in) completes nothing.
    idp.nonce_override = "someone-elses-nonce"
    flow = create(http=http)
    sign_in(http=http, flow=flow)
    body = poll(http=http, flow=flow).json()
    assert body["state"] == "failed" and body["error"]["code"] == "invalid_credentials", body


def test_return_to_gets_the_flow_id_in_the_fragment_only(http):
    flow = create(http=http, body={"return_to": "/ui/?tab=2"})
    redirect = sign_in(http=http, flow=flow)
    assert redirect.status_code in (302, 303), redirect.text
    location = urllib.parse.urlsplit(redirect.headers["location"])
    assert location.netloc in ("", http.base_url.netloc.decode())
    assert location.path == "/ui/" and location.query == "tab=2"
    assert location.fragment == f"chargewire_oauth_flow={flow['flow_id']}"
    for secret in (flow["poll_secret"], "code=", "access_token"):
        assert secret not in redirect.headers["location"]
    assert "no-store" in redirect.headers["cache-control"]
    assert poll(http=http, flow=flow).json()["state"] == "complete"


def test_return_to_redirects_on_failure_too(http, idp):
    idp.deny = True
    flow = create(http=http, body={"return_to": "/ui/"})
    redirect = sign_in(http=http, flow=flow)
    assert redirect.status_code in (302, 303)
    assert redirect.headers["location"].endswith(f"/ui/#chargewire_oauth_flow={flow['flow_id']}")
    assert poll(http=http, flow=flow).json()["error"]["code"] == "access_denied"


@pytest.mark.parametrize("return_to", [
    "https://evil.example/", "http://evil.example", "//evil.example/ui/", "///evil.example", "/\\evil.example",
    "\\\\evil.example", "/ui\\..\\x", "evil.example", "ui/", "javascript:alert(1)", "/ui/#x", "/\t/evil.example",
    "/\n/evil.example", "/ ui", "/ui/é", "/" + "a" * 2048, "",
])
def test_return_to_must_be_a_path_on_the_servers_own_origin(http, return_to):
    response = http.post(url="/chargewire/v1/auth/oauth/flows", json={"return_to": return_to})
    assert_problem(response=response, status=400, code="invalid_return_to")


def test_return_to_must_be_a_string(http):
    response = http.post(url="/chargewire/v1/auth/oauth/flows", json={"return_to": 7})
    assert_problem(response=response, status=400, code="bad_request")


def test_pending_flows_are_bounded(idp):
    server = start(idp=idp, max_flows=MAX_FLOWS)
    try:
        with httpx.Client(base_url=server.base_url, timeout=30) as http:
            for _ in range(MAX_FLOWS):
                assert http.post(url="/chargewire/v1/auth/oauth/flows").status_code == 201
            refused = http.post(url="/chargewire/v1/auth/oauth/flows")
            assert_problem(response=refused, status=429, code="rate_limited")
            assert int(refused.headers["retry-after"]) > 0
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


def test_without_the_capability_flows_are_not_implemented():
    binary = os.environ.get("CHARGEWIRE_TEST_SERVER_BINARY")
    if not binary:
        pytest.fail(reason="set CHARGEWIRE_TEST_SERVER_BINARY, or deselect with -m 'not oauth'")
    server = start_server(binary=binary)
    try:
        with httpx.Client(base_url=server.base_url, timeout=30) as http:
            assert "oauth_browser" not in http.get(url="/chargewire").json()["capabilities"]
            assert_problem(response=http.post(url="/chargewire/v1/auth/oauth/flows"), status=501, code="not_implemented")
    finally:
        assert stop_server(server=server) == 0, server.log_tail()


def test_the_password_grant_still_works_beside_it(server):
    chargewire = Chargewire(base_url=server.base_url, username=server.username, password=server.password)
    try:
        assert chargewire.new_session()
    finally:
        chargewire.close()
