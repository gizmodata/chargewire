"""Checks real server responses against the protocol's OpenAPI document (spec/openapi.yaml).

`OpenApi.check()` finds the operation a request was for and checks the response against
it: a documented status (or its 4XX/5XX range), a documented content type, the required
headers and their values, and a JSON body that validates against the response's schema
(OpenAPI 3.1 schemas are JSON Schema 2020-12). A problem document must also agree with
the HTTP status and carry `urn:warp-sql:error:<code>` as its type (spec §11).

conftest.py installs it on every httpx request the suite makes, so every JSON endpoint the
suite exercises is checked on every response, success or error, without each test asking.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

import jsonschema
import referencing
import referencing.jsonschema
import yaml

REPO = Path(__file__).resolve().parents[2]
OPENAPI_PATH = Path(os.environ.get("WARP_SQL_TEST_OPENAPI", REPO / "spec" / "openapi.yaml"))
SPEC_PATH = REPO / "spec" / "warp-sql.md"
DOC_URI = "urn:warp-sql:openapi"
METHODS = ("get", "put", "post", "delete", "options", "head", "patch", "trace")
ARROW_STREAM = "application/vnd.apache.arrow.stream"
# An Arrow IPC stream begins with a message, framed by the continuation marker.
ARROW_CONTINUATION = b"\xff\xff\xff\xff"


def load_document(path: Path = OPENAPI_PATH) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def escape_pointer(token: str) -> str:
    return token.replace("~", "~0").replace("/", "~1")


def operations(doc: dict) -> set[tuple[str, str]]:
    """Every (METHOD, path) the document describes."""
    return {(method.upper(), path) for path, item in doc["paths"].items()
            for method in item if method in METHODS}


def spec_endpoints(spec_text: str | None = None) -> set[tuple[str, str]]:
    """Appendix B of the specification: every endpoint of version 1, as (METHOD, path)."""
    text = spec_text if spec_text is not None else SPEC_PATH.read_text(encoding="utf-8")
    appendix = text.split("## Appendix B. Endpoints", 1)[1]
    rows = re.findall(r"^\| `([A-Z]+)` \| `([^`]+)` \|", appendix, flags=re.MULTILINE)
    assert rows, "Appendix B lists no endpoints"
    return {(method, expand(path=path)) for method, path in rows}


def spec_body_endpoints(spec_text: str | None = None) -> set[tuple[str, str]]:
    """The endpoints the specification's sections name as `METHOD path`, before Appendix B."""
    text = spec_text if spec_text is not None else SPEC_PATH.read_text(encoding="utf-8")
    body = text.split("## Appendix B. Endpoints", 1)[0]
    found = re.findall(r"`(GET|POST|PATCH|DELETE|PUT) ((?:/warp|…)[^`?\s]*)", body)
    return {(method, expand(path=path)) for method, path in found}


def expand(path: str) -> str:
    """Appendix B and the sections write `…` for /warp/v1/sessions/{session_id}."""
    return path.replace("…", "/warp/v1/sessions/{session_id}", 1) if path.startswith("…") else path


@dataclass
class Route:
    template: str
    pattern: re.Pattern
    item: dict


class OpenApi:
    def __init__(self, doc: dict | None = None):
        self.doc = doc if doc is not None else load_document()
        resource = referencing.Resource.from_contents(
            contents=self.doc, default_specification=referencing.jsonschema.DRAFT202012)
        self.registry = referencing.Registry().with_resource(uri=DOC_URI, resource=resource)
        self.routes = []
        for template, item in self.doc["paths"].items():
            regex = "^" + re.sub(r"\\\{[^}]+\\\}", "[^/]+", re.escape(template)) + "$"
            self.routes.append(Route(template=template, pattern=re.compile(regex), item=item))
        # Literal paths before templated ones (…/metadata/catalogs before …/{id}).
        self.routes.sort(key=lambda route: route.template.count("{"))
        self._validators: dict[str, jsonschema.Draft202012Validator] = {}
        # The operations check() has seen answered, as (METHOD, path template): the suite's
        # coverage of the document, printed at the end of a run.
        self.seen: set[tuple[str, str]] = set()

    # ---- References ----

    def resolve(self, pointer: str) -> tuple[object, str]:
        """The object at `pointer` (a JSON pointer into the document), following `$ref`s
        to non-schema objects (responses, headers, parameters); returns it and its pointer."""
        node = self.doc
        for token in pointer.lstrip("/").split("/") if pointer.strip("/") else []:
            token = token.replace("~1", "/").replace("~0", "~")
            node = node[int(token)] if isinstance(node, list) else node[token]
        if isinstance(node, dict) and set(node) == {"$ref"}:
            return self.resolve(pointer=node["$ref"].removeprefix("#"))
        return node, pointer

    def validator(self, pointer: str) -> jsonschema.Draft202012Validator:
        if pointer not in self._validators:
            self._validators[pointer] = jsonschema.Draft202012Validator(
                schema={"$ref": f"{DOC_URI}#{pointer}"}, registry=self.registry)
        return self._validators[pointer]

    def schema_errors(self, pointer: str, instance) -> list[str]:
        errors = sorted(self.validator(pointer=pointer).iter_errors(instance=instance),
                        key=lambda error: list(error.absolute_path))
        return [f"{'/'.join(str(p) for p in error.absolute_path) or '(root)'}: {error.message}"
                for error in errors]

    # ---- Matching ----

    def find(self, path: str) -> Route | None:
        for route in self.routes:
            if route.pattern.match(path):
                return route
        return None

    # ---- The check ----

    def check(self, method: str, path: str, status: int, headers: dict[str, str],
              body: bytes | None) -> list[str]:
        """What is wrong with this response, by the document; [] when nothing is. `headers`
        has lower-case names; `body` is None when it was not read (a streamed response)."""
        method = method.lower()
        path = unquote(path)
        where = f"{method.upper()} {path} -> {status}"
        if method in ("options", "head") or not path.startswith("/warp"):
            return []
        route = self.find(path=path)
        if route is None or method not in route.item:
            # No such endpoint (404) or not this method (405): still a problem document.
            expected = 404 if route is None else 405
            if status != expected:
                return [f"{where}: no operation in the OpenAPI document, so {expected} expected"]
            return [f"{where}: {problem}" for problem in
                    self.check_body(pointer="/components/responses/Problem", status=status,
                                    headers=headers, body=body)]
        self.seen.add((method.upper(), route.template))
        op_pointer = f"/paths/{escape_pointer(route.template)}/{method}/responses"
        responses = route.item[method]["responses"]
        for key in (str(status), f"{str(status)[0]}XX", "default"):
            if key in responses:
                return [f"{where}: {problem}" for problem in
                        self.check_body(pointer=f"{op_pointer}/{escape_pointer(key)}", status=status,
                                        headers=headers, body=body)]
        return [f"{where}: status {status} is not documented for this operation "
                f"(documented: {', '.join(responses)})"]

    def check_body(self, pointer: str, status: int, headers: dict[str, str],
                   body: bytes | None) -> list[str]:
        response, pointer = self.resolve(pointer=pointer)
        problems = []
        for name, _ in (response.get("headers") or {}).items():
            header, header_pointer = self.resolve(pointer=f"{pointer}/headers/{escape_pointer(name)}")
            value = headers.get(name.lower())
            if value is None:
                if header.get("required"):
                    problems.append(f"the required header {name} is missing")
                continue
            if "schema" in header:
                instance = header_value(value=value, schema=self.resolve_schema(header["schema"]))
                problems += [f"header {name}: {error}" for error in
                             self.schema_errors(pointer=f"{header_pointer}/schema", instance=instance)]
        content = response.get("content") or {}
        content_type = headers.get("content-type", "").split(";")[0].strip().lower()
        if not content:
            if body:
                problems.append(f"a body ({content_type or 'no content type'}) where none is documented")
            return problems
        if content_type not in content:
            problems.append(f"content type {content_type or '(none)'} is not documented "
                            f"(documented: {', '.join(content)})")
            return problems
        if body is None:
            return problems  # streamed: the body is the test's to read
        schema_pointer = f"{pointer}/content/{escape_pointer(content_type)}/schema"
        if content_type == ARROW_STREAM:
            if body and not body.startswith(ARROW_CONTINUATION):
                problems.append("the body does not begin like an Arrow IPC stream")
            return problems
        if content_type.endswith("json"):
            try:
                instance = json.loads(body)
            except ValueError as error:
                return problems + [f"the body is not JSON ({error})"]
            problems += self.schema_errors(pointer=schema_pointer, instance=instance)
            if content_type == "application/problem+json" and isinstance(instance, dict):
                if instance.get("status") != status:
                    problems.append(f"the problem document's status {instance.get('status')} "
                                    f"is not the response's {status}")
                if instance.get("type") != f"urn:warp-sql:error:{instance.get('code')}":
                    problems.append(f"the problem document's type {instance.get('type')!r} does not "
                                    f"name its code {instance.get('code')!r}")
        return problems

    def resolve_schema(self, schema: dict) -> dict:
        while isinstance(schema, dict) and "$ref" in schema:
            schema, _ = self.resolve(pointer=schema["$ref"].removeprefix("#"))
        return schema


def header_value(value: str, schema: dict):
    """A header's text as the JSON value its schema describes (an integer, a boolean)."""
    kind = schema.get("type")
    if kind == "integer" and re.fullmatch(pattern=r"-?[0-9]+", string=value.strip()):
        return int(value)
    if kind == "boolean" and value.strip() in ("true", "false"):
        return value.strip() == "true"
    return value


def without_extensions(node):
    """The document without `x-` fields and `servers`, which a server may change (spec §3.3)."""
    if isinstance(node, dict):
        return {key: without_extensions(value) for key, value in node.items()
                if not key.startswith("x-") and key != "servers"}
    if isinstance(node, list):
        return [without_extensions(value) for value in node]
    return node
