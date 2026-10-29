#!/usr/bin/env python3
"""Runs every example marked `test` on the site's pages against real Chargewire servers.

    site/scripts/verify_examples.py [--page FRAGMENT] [--list]
        [--chargewire-duckdb PATH] [--hardcharger PATH]

A fenced block takes part when its info string has the word `test`:

    ```bash test
    ```bash test profile=oauth

and the block right after it, if its info string has the word `output`, is what the block
must print. In expected output `…` matches any text within a line, and a line that is only
`…` matches any number of lines.

Every page runs in a fresh directory, which is also its HOME, with a clean environment: PATH
(curl, jq, and a `python3` with pyarrow), HOME, LANG, TMPDIR and what the profile adds.
Nothing from the caller's shell, credentials included, reaches an example. A page's blocks run
in order in one bash process (`set -euo pipefail`), so a block sees the variables the blocks
before it set. Pages write `http://127.0.0.1:31337`, a server's default address; each block
runs against its profile's server on a free port, and that address is rewritten to it in the
block and in its expected output.

Profiles (`profile=NAME`; default `default`). Each starts with user `joe`, password `s3:cret`:

    default      chargewire-duckdb, the reference server in ../cpp
    retention    chargewire-duckdb keeping results 3 seconds (--result-retention-seconds 3)
    drain        chargewire-duckdb with a 30-second drain, its own for the page; SERVER_PID is its
                 process id, and the page is expected to stop it
    oauth        chargewire-duckdb with browser sign-in through a stand-in OpenID Connect provider
                 (conformance/tests/idp.py) that approves every sign-in as alice@example.com
    hardcharger  Hardcharger, trusting that provider's ID tokens (token exchange); ID_TOKEN is
                 a fresh ID token for alice@example.com

Needs chargewire-duckdb (default ../cpp/build/chargewire-duckdb), Hardcharger (--hardcharger or
CHARGEWIRE_SITE_HARDCHARGER) for the pages that use it, curl and jq, and Python 3.12+ with the
conformance suite's packages (run it with ../conformance/.venv/bin/python). A missing
prerequisite fails the run, and so does a run in which no example ran.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import pathlib
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

SITE = pathlib.Path(__file__).resolve().parent.parent
REPO = SITE.parent
DOCS = SITE / "src" / "content" / "docs"
FENCE = re.compile(r"^(?P<fence>`{3,}|~{3,})(?P<info>.*)$")
ELLIPSIS = "…"
DOC_ADDRESS = "127.0.0.1:31337"
USERNAME, PASSWORD = "joe", "s3:cret"
OAUTH_IDENTITY = "alice@example.com"
MARKER = "@@chargewire-example@@"
PROFILES = ("default", "retention", "drain", "oauth", "hardcharger")


class Failure(Exception):
    pass


@dataclasses.dataclass
class Block:
    page: pathlib.Path
    line: int
    profile: str
    code: str
    expected: str | None = None

    @property
    def where(self) -> str:
        return f"{self.page.relative_to(SITE)}:{self.line}"


def extract(page: pathlib.Path) -> list[Block]:
    lines = page.read_text(encoding="utf-8").splitlines()
    blocks: list[Block] = []
    i = 0
    last: Block | None = None
    while i < len(lines):
        match = FENCE.match(lines[i].strip())
        if not match:
            if lines[i].strip():
                last = None  # an output block comes right after its example, blank lines apart
            i += 1
            continue
        fence, info = match.group("fence"), match.group("info").strip()
        start = i + 1
        i += 1
        body = []
        while i < len(lines) and lines[i].strip() != fence:
            body.append(lines[i])
            i += 1
        i += 1
        words = info.split()
        options = dict(word.split("=", 1) for word in words[1:] if "=" in word)
        if "test" in words[1:]:
            profile = options.get("profile", "default")
            if profile not in PROFILES:
                raise Failure(f"{page.relative_to(SITE)}:{start}: unknown profile {profile}")
            last = Block(page=page, line=start, profile=profile, code="\n".join(body))
            blocks.append(last)
        elif "output" in words[1:]:
            if last is None or last.expected is not None:
                raise Failure(f"{page.relative_to(SITE)}:{start}: an output block with no tested block before it")
            last.expected = "\n".join(body)
            last = None
        else:
            last = None
    return blocks


def matches(expected: str, actual: str) -> bool:
    """Line by line; `…` within a line matches any text, a line of only `…` any lines."""
    want = [line.rstrip() for line in expected.strip("\n").splitlines()]
    got = [line.rstrip() for line in actual.strip("\n").splitlines()]

    def line_matches(pattern: str, text: str) -> bool:
        regex = ".*".join(re.escape(part) for part in pattern.split(ELLIPSIS))
        return re.fullmatch(regex, text) is not None

    def walk(w: int, g: int) -> bool:
        if w == len(want):
            return g == len(got)
        if want[w].strip() == ELLIPSIS:
            return any(walk(w + 1, k) for k in range(g, len(got) + 1))
        return g < len(got) and line_matches(want[w], got[g]) and walk(w + 1, g + 1)

    return walk(0, 0)


# ---- Servers ----

def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclasses.dataclass
class Server:
    profile: str
    port: int
    process: subprocess.Popen
    log: pathlib.Path
    env: dict[str, str] = dataclasses.field(default_factory=dict)  # what the page gets
    idp: object = None

    def log_tail(self, lines: int = 25) -> str:
        return "\n".join(self.log.read_text(errors="replace").splitlines()[-lines:])

    def stop(self) -> int | None:
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            self.process.send_signal(signal.SIGTERM)  # the second stops without draining
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                return None
        if self.idp is not None:
            self.idp.stop()
        return self.process.returncode


def wait_up(server: Server) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if server.process.poll() is not None:
            raise Failure(f"the {server.profile} server exited ({server.process.returncode}):\n{server.log_tail()}")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{server.port}/chargewire", timeout=2) as answer:
                if answer.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.2)
    raise Failure(f"the {server.profile} server did not answer:\n{server.log_tail()}")


def start(profile: str, args: argparse.Namespace, scratch: pathlib.Path) -> Server:
    port = free_port()
    home = scratch / f"server-{profile}"
    home.mkdir(parents=True, exist_ok=True)
    log = home / "server.log"
    # The server's own HOME: DuckDB keeps secrets under ~/.duckdb, which must not be the caller's.
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(home), "LANG": "C.UTF-8"}
    idp = None
    page_env: dict[str, str] = {}
    if profile == "hardcharger":
        if not args.hardcharger:
            raise Failure("the hardcharger profile needs --hardcharger PATH (or CHARGEWIRE_SITE_HARDCHARGER)")
        idp = identity_provider()
        command = [args.hardcharger, "--host", "127.0.0.1", "--port", str(port),
                   "--token-allowed-issuer", idp.issuer, "--token-allowed-audience", idp.client_id,
                   "--token-default-role", "admin", "--token-authorized-emails", OAUTH_IDENTITY]
        env.update(HARDCHARGER_ADMIN_USERNAME=USERNAME, HARDCHARGER_ADMIN_PASSWORD=PASSWORD,
                   HARDCHARGER_PRINT_QUERIES="false")
        page_env["ID_TOKEN"] = idp.id_token(email=OAUTH_IDENTITY, nonce=None)
    else:
        command = [args.chargewire_duckdb, "--host", "127.0.0.1", "--port", str(port), "--username", USERNAME]
        env["CHARGEWIRE_DUCKDB_PASSWORD"] = PASSWORD
        if profile == "retention":
            command += ["--result-retention-seconds", "3"]
        elif profile == "drain":
            command += ["--drain-seconds", "30"]
        elif profile == "oauth":
            idp = identity_provider()
            command += ["--oauth-issuer", idp.issuer, "--oauth-client-id", idp.client_id,
                        "--oauth-identities", OAUTH_IDENTITY, "--oauth-provider-name", "Example IdP"]
            env["CHARGEWIRE_DUCKDB_OAUTH_CLIENT_SECRET"] = idp.client_secret
    with log.open("wb") as out:
        process = subprocess.Popen(command, env=env, stdout=out, stderr=subprocess.STDOUT, cwd=home)
    server = Server(profile=profile, port=port, process=process, log=log, env=page_env, idp=idp)
    try:
        wait_up(server)
    except BaseException:
        server.stop()
        raise
    if profile == "drain":
        server.env["SERVER_PID"] = str(process.pid)
    return server


def identity_provider():
    sys.path.insert(0, str(REPO / "conformance" / "tests"))
    from idp import IdentityProvider  # the conformance suite's stand-in provider
    return IdentityProvider()


# ---- Pages ----

def page_environment(home: pathlib.Path, servers: dict[str, Server]) -> dict[str, str]:
    tools = []
    for tool in ("curl", "jq", "od", "tail"):
        found = shutil.which(tool)
        if not found:
            raise Failure(f"{tool} is not installed")
        tools.append(str(pathlib.Path(found).parent))
    python_dir = str(pathlib.Path(sys.executable).parent)  # a python3 with pyarrow
    path = ":".join(dict.fromkeys([python_dir, *tools, "/usr/bin", "/bin"]))
    env = {"PATH": path, "HOME": str(home), "LANG": "C.UTF-8", "TMPDIR": str(home)}
    for server in servers.values():
        env.update(server.env)
    return env


def run_page(page: pathlib.Path, blocks: list[Block], args: argparse.Namespace) -> None:
    with tempfile.TemporaryDirectory(prefix="chargewire-site-") as tmp:
        scratch = pathlib.Path(tmp)
        home = scratch / "home"
        home.mkdir()
        servers: dict[str, Server] = {}
        try:
            for block in blocks:
                if block.profile not in servers:
                    servers[block.profile] = start(profile=block.profile, args=args, scratch=scratch)
            parts = ["set -euo pipefail"]
            for i, block in enumerate(blocks):
                code = block.code.replace(DOC_ADDRESS, f"127.0.0.1:{servers[block.profile].port}")
                parts.append(f"printf '\\n{MARKER} {i}\\n'")
                parts.append(code)
            parts.append(f"printf '\\n{MARKER} end\\n'")
            script = scratch / "page.sh"
            script.write_text("\n".join(parts) + "\n", encoding="utf-8")
            result = subprocess.run(["bash", str(script)], cwd=home, env=page_environment(home=home, servers=servers),
                                    capture_output=True, text=True, timeout=600)
            outputs: dict[str, str] = {}
            current = None
            for line in result.stdout.splitlines():
                if line.startswith(MARKER):
                    current = line.split()[-1]
                    outputs[current] = ""
                elif current is not None:
                    outputs[current] += line + "\n"
            if "end" not in outputs:
                failed = max((int(k) for k in outputs if k != "end"), default=0)
                block = blocks[failed]
                raise Failure(f"{block.where}: the example failed (exit {result.returncode})\n"
                              f"--- its output ---\n{outputs.get(str(failed), '').strip()}\n"
                              f"--- stderr ---\n{result.stderr.strip()[-3000:]}\n"
                              f"--- {block.profile} server log ---\n{servers[block.profile].log_tail()}")
            for i, block in enumerate(blocks):
                if block.expected is None:
                    continue
                expected = block.expected.replace(DOC_ADDRESS, f"127.0.0.1:{servers[block.profile].port}")
                if not matches(expected=expected, actual=outputs[str(i)]):
                    raise Failure(f"{block.where}: the output differs\n--- expected ---\n{expected.strip()}\n"
                                  f"--- got ---\n{outputs[str(i)].strip()}\n--- stderr ---\n{result.stderr.strip()[-2000:]}")
        finally:
            for server in servers.values():
                code = server.stop()
                if code not in (0, None) and server.profile != "drain":
                    print(f"  warning: the {server.profile} server exited {code}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--page", help="only pages whose path contains this")
    parser.add_argument("--list", action="store_true", help="list the examples and stop")
    parser.add_argument("--chargewire-duckdb", default=os.environ.get("CHARGEWIRE_SITE_CHARGEWIRE_DUCKDB",
                                                                str(REPO / "cpp" / "build" / "chargewire-duckdb")))
    parser.add_argument("--hardcharger", default=os.environ.get("CHARGEWIRE_SITE_HARDCHARGER"))
    args = parser.parse_args()
    if not pathlib.Path(args.chargewire_duckdb).is_file():
        print(f"no chargewire-duckdb at {args.chargewire_duckdb}: build ../cpp or pass --chargewire-duckdb", file=sys.stderr)
        return 1
    try:
        import pyarrow  # noqa: F401  the examples read Arrow with it
    except ImportError:
        print("run this with a Python that has pyarrow (../conformance/.venv/bin/python)", file=sys.stderr)
        return 1
    pages = sorted(p for p in DOCS.rglob("*.md*") if args.page is None or args.page in str(p))
    total = 0
    failures = []
    for page in pages:
        try:
            blocks = extract(page=page)
        except Failure as failure:
            failures.append(str(failure))
            continue
        if not blocks:
            continue
        total += len(blocks)
        checked = sum(1 for b in blocks if b.expected is not None)
        if args.list:
            for block in blocks:
                print(f"{block.where} profile={block.profile}{' (output checked)' if block.expected else ''}")
            continue
        began = time.monotonic()
        try:
            run_page(page=page, blocks=blocks, args=args)
            print(f"ok   {page.relative_to(SITE)}: {len(blocks)} examples, {checked} outputs checked "
                  f"({time.monotonic() - began:.1f}s)")
        except (Failure, subprocess.TimeoutExpired) as failure:
            print(f"FAIL {page.relative_to(SITE)}")
            failures.append(str(failure))
    if failures:
        print("\n\n".join(failures), file=sys.stderr)
        return 1
    if total == 0:
        print("no example ran", file=sys.stderr)
        return 1
    if not args.list:
        print(f"{total} examples on {len([p for p in pages if extract(page=p)])} pages: all passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
