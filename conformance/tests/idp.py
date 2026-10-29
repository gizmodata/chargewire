"""A small OpenID Connect identity provider for the browser sign-in tests (spec §4.3).

It serves discovery, a JWKS, an authorization endpoint that approves at once (redirecting
the browser back with a code, as a provider does after the person signs in) and a token
endpoint that trades the code for an RS256 ID token. It checks what a real provider checks:
the client id and secret, the redirect URI, single-use codes and PKCE (S256), and it puts
the request's nonce in the ID token. `email`, `deny` and `nonce_override` decide what the
next sign-in gets; `authorize_requests` and `token_requests` record what the server sent.
"""

import base64
import hashlib
import http.server
import json
import secrets
import threading
import time
import urllib.parse

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def b64url_uint(value: int) -> str:
    return b64url(value.to_bytes(length=(value.bit_length() + 7) // 8, byteorder="big"))


class IdentityProvider:
    def __init__(self, client_id: str = "chargewire-conformance", client_secret: str | None = None):
        self.client_id = client_id
        self.client_secret = client_secret or secrets.token_urlsafe(nbytes=16) + "+/="
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.kid = "conformance-1"
        self.email = "alice@example.com"
        self.deny = False
        self.nonce_override: str | None = None
        self.authorize_requests: list[dict] = []
        self.token_requests: list[dict] = []
        self.codes: dict[str, dict] = {}
        self.lock = threading.Lock()
        idp = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format, *args):  # quiet
                pass

            def reply(self, status: int, body: dict | None = None, location: str | None = None):
                data = json.dumps(body or {}).encode()
                self.send_response(status)
                if location:
                    self.send_header("Location", location)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                url = urllib.parse.urlparse(self.path)
                if url.path == "/.well-known/openid-configuration":
                    return self.reply(status=200, body={
                        "issuer": idp.issuer, "jwks_uri": f"{idp.issuer}/jwks",
                        "authorization_endpoint": f"{idp.issuer}/authorize",
                        "token_endpoint": f"{idp.issuer}/token",
                        "code_challenge_methods_supported": ["S256"]})
                if url.path == "/jwks":
                    return self.reply(status=200, body={"keys": [idp.jwk()]})
                if url.path == "/authorize":
                    return self.authorize(query=dict(urllib.parse.parse_qsl(url.query)))
                self.reply(status=404, body={"error": "not_found"})

            def authorize(self, query: dict):
                idp.authorize_requests.append(query)
                if query.get("client_id") != idp.client_id or not query.get("redirect_uri"):
                    return self.reply(status=400, body={"error": "invalid_request"})
                redirect = query["redirect_uri"]
                state = query.get("state", "")
                if idp.deny:
                    return self.reply(status=302, location=redirect + "?" + urllib.parse.urlencode(
                        {"error": "access_denied", "error_description": "<b>The user said no</b>", "state": state}))
                if query.get("response_type") != "code" or query.get("code_challenge_method") != "S256":
                    return self.reply(status=400, body={"error": "invalid_request"})
                code = secrets.token_urlsafe(nbytes=16)
                with idp.lock:
                    idp.codes[code] = {"challenge": query.get("code_challenge"), "nonce": query.get("nonce"),
                                       "redirect_uri": redirect, "email": idp.email}
                self.reply(status=302, location=redirect + "?" + urllib.parse.urlencode({"code": code, "state": state}))

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                form = dict(urllib.parse.parse_qsl(self.rfile.read(length).decode()))
                idp.token_requests.append(form)
                if urllib.parse.urlparse(self.path).path != "/token":
                    return self.reply(status=404, body={"error": "not_found"})
                if form.get("grant_type") != "authorization_code":
                    return self.reply(status=400, body={"error": "unsupported_grant_type"})
                if form.get("client_id") != idp.client_id or form.get("client_secret") != idp.client_secret:
                    return self.reply(status=401, body={"error": "invalid_client"})
                with idp.lock:
                    grant = idp.codes.pop(form.get("code", ""), None)  # single use
                if grant is None or grant["redirect_uri"] != form.get("redirect_uri"):
                    return self.reply(status=400, body={"error": "invalid_grant", "error_description": "bad code"})
                verifier = form.get("code_verifier", "")
                if b64url(hashlib.sha256(verifier.encode()).digest()) != grant["challenge"]:
                    return self.reply(status=400, body={"error": "invalid_grant",
                                                        "error_description": "PKCE verification failed"})
                nonce = idp.nonce_override if idp.nonce_override is not None else grant["nonce"]
                self.reply(status=200, body={"access_token": "unused", "token_type": "Bearer",
                                             "id_token": idp.id_token(email=grant["email"], nonce=nonce)})

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.issuer = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def jwk(self) -> dict:
        public = self.key.public_key().public_numbers()
        return {"kty": "RSA", "kid": self.kid, "alg": "RS256", "use": "sig",
                "n": b64url_uint(public.n), "e": b64url_uint(public.e)}

    def id_token(self, email: str, nonce: str | None) -> str:
        now = int(time.time())
        header = {"alg": "RS256", "typ": "JWT", "kid": self.kid}
        payload = {"iss": self.issuer, "aud": self.client_id, "sub": email, "email": email,
                   "iat": now, "exp": now + 300}
        if nonce is not None:
            payload["nonce"] = nonce
        signing_input = b64url(json.dumps(header).encode()) + "." + b64url(json.dumps(payload).encode())
        signature = self.key.sign(data=signing_input.encode(), padding=padding.PKCS1v15(), algorithm=hashes.SHA256())
        return signing_input + "." + b64url(signature)

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
