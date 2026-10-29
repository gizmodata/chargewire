"""Spec §3 (discovery, health) and §4 (authentication)."""

import httpx


def test_discovery_describes_the_server(server):
    response = httpx.get(url=f"{server.base_url}/chargewire")
    assert response.status_code == 200
    doc = response.json()
    assert doc["protocol"] == "chargewire"
    assert "1" in doc["protocol_versions"]
    assert isinstance(doc["capabilities"], list)
    assert all(isinstance(name, str) for name in doc["capabilities"])
    # Gone before 1.0: retention is core, and split results are result_partitions (§3.1).
    assert not {"result_retention", "partitioned_results"} & set(doc["capabilities"]), doc
    assert "draft" not in doc, doc
    assert doc["server"]["name"]
    assert "password" in doc["auth"]["grant_types"] or doc["auth"]["grant_types"]


def test_health_needs_no_credentials(server):
    response = httpx.get(url=f"{server.base_url}/chargewire/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "serving"}


def test_password_exchange_issues_a_bearer_token(server):
    response = httpx.post(url=f"{server.base_url}/chargewire/v1/auth/token",
                          json={"grant_type": "password", "username": server.username,
                                "password": server.password})
    assert response.status_code == 200, response.text
    doc = response.json()
    assert doc["token_type"] == "Bearer"
    assert doc["expires_in"] > 0
    assert len(doc["access_token"]) >= 32


def assert_problem(response: httpx.Response, status: int, code: str) -> dict:
    assert response.status_code == status, response.text
    assert response.headers["content-type"].startswith("application/problem+json")
    problem = response.json()
    assert problem["code"] == code, problem
    assert problem["status"] == status
    assert problem["type"] == f"urn:chargewire:error:{code}"
    # §11: the title is for people, not the code again.
    assert isinstance(problem["title"], str) and problem["title"] and problem["title"] != code, problem
    return problem


def test_wrong_password_is_refused_without_saying_which_part(server):
    for username, password in [(server.username, server.password + "x"),
                               (server.username + "x", server.password),
                               ("", ""),
                               (server.username, server.password.split(":")[0])]:
        response = httpx.post(url=f"{server.base_url}/chargewire/v1/auth/token",
                              json={"grant_type": "password", "username": username,
                                    "password": password})
        problem = assert_problem(response=response, status=401, code="invalid_credentials")
        assert "password" not in problem["detail"].lower().replace("user name or password", "")


def test_requests_without_a_valid_token_are_refused(server, chargewire):
    session_id = chargewire.new_session()
    url = f"{server.base_url}/chargewire/v1/sessions/{session_id}"
    for headers in [{}, {"Authorization": "Bearer"}, {"Authorization": "Bearer forged"},
                    {"Authorization": f"Bearer {chargewire.token[:-1]}"},
                    {"Authorization": f"Basic {chargewire.token}"}]:
        response = httpx.get(url=url, headers=headers)
        problem = assert_problem(response=response, status=401, code="invalid_token")
        assert response.headers["www-authenticate"] == "Bearer"
        # §4.2: a token the server can't verify as its own carries no reason.
        assert "reason" not in problem, problem


def test_unknown_grant_type_is_a_bad_request(server):
    response = httpx.post(url=f"{server.base_url}/chargewire/v1/auth/token",
                          json={"grant_type": "magic"})
    assert_problem(response=response, status=400, code="bad_request")


def test_credentials_never_come_from_cookies(server, chargewire):
    session_id = chargewire.new_session()
    response = httpx.get(url=f"{server.base_url}/chargewire/v1/sessions/{session_id}",
                         cookies={"access_token": chargewire.token, "token": chargewire.token})
    assert_problem(response=response, status=401, code="invalid_token")
