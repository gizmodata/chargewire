"""Extension setup must follow asynchronous statement completion, including errors."""

import json
from types import SimpleNamespace

import pytest

from test_read_only_duckdb import load


class Client:
    def __init__(self, initial: str, final: str):
        self.initial = initial
        self.final = final
        self.submitted = []
        self.waited = []

    def submit(self, *, session_id, sql):
        self.submitted.append((session_id, sql))
        doc = {"statement_id": f"q_{len(self.submitted)}", "state": self.initial}
        return SimpleNamespace(status_code=201, json=lambda: doc, text=json.dumps(doc))

    def wait_done(self, *, session_id, statement_id):
        self.waited.append((session_id, statement_id))
        return {"statement_id": statement_id, "state": self.final}


@pytest.mark.parametrize("initial", ["queued", "running", "succeeded"])
def test_extension_setup_waits_for_completion(initial):
    client = Client(initial=initial, final="succeeded")
    loader = load.__wrapped__(chargewire=client, server=SimpleNamespace(process=True), duck="s_setup")
    loader(extension="spatial")
    assert client.submitted == [("s_setup", "INSTALL spatial"), ("s_setup", "LOAD spatial")]
    assert client.waited == ([] if initial == "succeeded" else [("s_setup", "q_1"), ("s_setup", "q_2")])


@pytest.mark.parametrize("final", ["failed", "cancelled"])
def test_extension_setup_reports_terminal_failure(final):
    client = Client(initial="running", final=final)
    loader = load.__wrapped__(chargewire=client, server=SimpleNamespace(process=True), duck="s_setup")
    with pytest.raises(pytest.fail.Exception, match=final):
        loader(extension="spatial")
    assert client.submitted == [("s_setup", "INSTALL spatial")]
    assert client.waited == [("s_setup", "q_1")]
