"""A minimal Warp SQL client for the conformance tests: plain HTTP calls, nothing
hidden, so a failing test points at the protocol rather than at a driver."""

import io
import time

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc

ARROW_STREAM = "application/vnd.apache.arrow.stream"


def read_table(content: bytes) -> pa.Table:
    return ipc.open_stream(source=io.BytesIO(initial_bytes=content)).read_all()


def read_batches(content: bytes) -> list[pa.RecordBatch]:
    return list(ipc.open_stream(source=io.BytesIO(initial_bytes=content)))


class Warp:
    def __init__(self, base_url: str, username: str, password: str, verify=True,
                 grant: str = "password"):
        """`verify`: httpx's TLS setting (an ssl.SSLContext for mutual TLS). `grant`:
        "password", or "client_certificate" (spec §4.4) to sign in with the context's
        client certificate."""
        self.http = httpx.Client(base_url=base_url, timeout=120, verify=verify)
        self.username = username
        self.password = password
        self.token = self.login() if grant == "password" else self.login_with_certificate()

    def close(self) -> None:
        self.http.close()

    # ---- Authentication ----

    def login(self, username: str | None = None, password: str | None = None) -> str:
        response = self.http.post(
            url="/warp/v1/auth/token",
            json={"grant_type": "password",
                  "username": self.username if username is None else username,
                  "password": self.password if password is None else password})
        assert response.status_code == 200, response.text
        return response.json()["access_token"]

    def login_with_certificate(self) -> str:
        response = self.http.post(url="/warp/v1/auth/token", json={"grant_type": "client_certificate"})
        assert response.status_code == 200, response.text
        return response.json()["access_token"]

    def headers(self, **extra: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}", **extra}

    # ---- Sessions ----

    def new_session(self, options: dict | None = None) -> str:
        response = self.http.post(url="/warp/v1/sessions", headers=self.headers(),
                                  json={"options": options or {}})
        assert response.status_code == 201, response.text
        return response.json()["session_id"]

    def session_url(self, session_id: str) -> str:
        return f"/warp/v1/sessions/{session_id}"

    # ---- Statements ----

    def submit(self, session_id: str, sql: str, parameters=None, arrow: bool = False,
               headers: dict[str, str] | None = None) -> httpx.Response:
        body: dict = {"sql": sql}
        if parameters is not None:
            body["parameters"] = parameters
        all_headers = self.headers(**(headers or {}))
        if arrow:
            all_headers["Accept"] = ARROW_STREAM
        return self.http.post(url=f"{self.session_url(session_id)}/statements", headers=all_headers,
                              json=body)

    def statement_url(self, session_id: str, statement_id: str) -> str:
        return f"{self.session_url(session_id)}/statements/{statement_id}"

    def status(self, session_id: str, statement_id: str, wait: int | None = None) -> dict:
        headers = self.headers()
        if wait is not None:
            headers["Prefer"] = f"wait={wait}"
        response = self.http.get(url=self.statement_url(session_id, statement_id), headers=headers)
        assert response.status_code == 200, response.text
        return response.json()

    def wait_done(self, session_id: str, statement_id: str, timeout: float = 60) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            doc = self.status(session_id=session_id, statement_id=statement_id, wait=5)
            if doc["state"] in ("succeeded", "failed", "cancelled"):
                return doc
            assert time.monotonic() < deadline, f"statement still {doc['state']} after {timeout}s"

    def result(self, session_id: str, statement_id: str, from_batch: int | None = None,
               headers: dict[str, str] | None = None) -> httpx.Response:
        params = {} if from_batch is None else {"from_batch": str(from_batch)}
        return self.http.get(url=f"{self.statement_url(session_id, statement_id)}/result",
                             params=params, headers=self.headers(**(headers or {})))

    def query(self, session_id: str, sql: str, parameters=None) -> pa.Table:
        """Runs a query on the one-round-trip streamed path and returns its rows."""
        response = self.submit(session_id=session_id, sql=sql, parameters=parameters, arrow=True)
        assert response.status_code == 200, response.text
        assert response.headers["content-type"].startswith(ARROW_STREAM), response.text
        return read_table(content=response.content)

    def execute(self, session_id: str, sql: str, parameters=None) -> dict:
        """Runs a statement and returns its finished statement document."""
        response = self.submit(session_id=session_id, sql=sql, parameters=parameters)
        assert response.status_code in (200, 201), response.text
        doc = response.json()
        if doc["state"] not in ("succeeded", "failed", "cancelled"):
            doc = self.wait_done(session_id=session_id, statement_id=doc["statement_id"])
        return doc
