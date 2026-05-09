#!/usr/bin/env python3
import contextlib
import fcntl
import http.server
import json
import os
from pathlib import Path
import secrets
import socketserver
import sys
import threading
import time
import urllib.parse
import webbrowser

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SECRETS_ROOT = PROJECT_ROOT / ".secrets"
CACHE_FILE = SECRETS_ROOT / "agora-oauth-cache.json"
LOCK_FILE = SECRETS_ROOT / "agora-oauth.lock"
REDIRECT_URI = os.getenv("AGORA_OAUTH_REDIRECT_URI", "http://localhost:18082")
AUTHORIZE_URL = os.getenv("AGORA_OAUTH_AUTHORIZE_URL")
OAUTH_BASE = os.getenv("AGORA_OAUTH_BASE_URL", "").rstrip("/")
if not AUTHORIZE_URL and OAUTH_BASE:
    AUTHORIZE_URL = f"{OAUTH_BASE}/authorize"
TOKEN_URL = os.getenv("AGORA_OAUTH_TOKEN_URL")
if not TOKEN_URL and OAUTH_BASE:
    TOKEN_URL = f"{OAUTH_BASE}/token"
CLIENT_ID = os.environ["AGORA_OAUTH_CLIENT_ID"]
CLIENT_SECRET = os.environ["AGORA_OAUTH_CLIENT_SECRET"]
SCOPE = os.getenv("AGORA_OAUTH_SCOPE", "read")


def _stderr(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def _load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        return json.loads(CACHE_FILE.read_text())
    except Exception:
        return {}


def _save_cache(data: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(data))


def _token_is_valid(data: dict) -> bool:
    access_token = data.get("access_token")
    expires_at = data.get("expires_at", 0)
    return bool(access_token) and float(expires_at) > time.time() + 60


class _CallbackHandler(http.server.BaseHTTPRequestHandler):
    authorization_code = None
    received_state = None
    error = None
    done = threading.Event()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "error" in params:
            _CallbackHandler.error = params["error"][0]
            self._respond(
                f"Authorization failed: {_CallbackHandler.error}",
                status=400,
            )
        elif "code" in params:
            _CallbackHandler.authorization_code = params["code"][0]
            _CallbackHandler.received_state = params.get("state", [None])[0]
            self._respond("Authorization successful. You can close this window.")
        else:
            self._respond("Missing authorization code.", status=400)

        _CallbackHandler.done.set()

    def _respond(self, message: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        html = (
            "<!DOCTYPE html><html><body style='font-family:sans-serif;"
            "text-align:center;padding:40px'>"
            f"<p>{message}</p>"
            "<script>setTimeout(()=>window.close(),3000)</script>"
            "</body></html>"
        )
        self.wfile.write(html.encode())

    def log_message(self, format: str, *args: object) -> None:
        return


def _exchange_code(code: str) -> dict:
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
        timeout=15,
    )
    response.raise_for_status()
    token_data = response.json()
    return {
        "access_token": token_data["access_token"],
        "refresh_token": token_data.get("refresh_token"),
        "expires_at": time.time() + token_data.get("expires_in", 3600),
    }


def _refresh_tokens(refresh_token: str) -> dict:
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": refresh_token,
        },
        timeout=15,
    )
    response.raise_for_status()
    token_data = response.json()
    return {
        "access_token": token_data["access_token"],
        "refresh_token": token_data.get("refresh_token", refresh_token),
        "expires_at": time.time() + token_data.get("expires_in", 3600),
    }


def _authorize_via_browser(timeout: int = 300) -> dict:
    _CallbackHandler.authorization_code = None
    _CallbackHandler.received_state = None
    _CallbackHandler.error = None
    _CallbackHandler.done.clear()

    parsed = urllib.parse.urlparse(REDIRECT_URI)
    port = parsed.port or 18082

    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    httpd = ReusableTCPServer(("", port), _CallbackHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    state = secrets.token_urlsafe(16)
    params = urllib.parse.urlencode(
        {
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPE,
            "state": state,
        }
    )
    url = f"{AUTHORIZE_URL}?{params}"

    _stderr("Authorize `mcp-atlassian` by opening this URL in your browser:")
    _stderr(url)
    if not webbrowser.open(url):
        _stderr("(Browser launch failed; please copy the URL above manually.)")

    try:
        if not _CallbackHandler.done.wait(timeout):
            raise RuntimeError("Timed out waiting for authorization callback")
        if _CallbackHandler.error:
            raise RuntimeError(f"Authorization error: {_CallbackHandler.error}")
        if _CallbackHandler.received_state != state:
            raise RuntimeError("State mismatch — possible CSRF attack")
        code = _CallbackHandler.authorization_code
        if not code:
            raise RuntimeError("No authorization code received")
        return _exchange_code(code)
    finally:
        httpd.shutdown()
        httpd.server_close()


class CachedAgoraTokenRefresher:
    @contextlib.contextmanager
    def _exclusive_lock(self):
        LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
        with LOCK_FILE.open("a+") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def get_token(self) -> str:
        with self._exclusive_lock():
            token_data = _load_cache()
            if _token_is_valid(token_data):
                return token_data["access_token"]

            refresh_token = token_data.get("refresh_token")
            if refresh_token:
                try:
                    token_data = _refresh_tokens(refresh_token)
                    _save_cache(token_data)
                    return token_data["access_token"]
                except requests.RequestException as exc:
                    _stderr(
                        "Refresh token failed, falling back to browser auth: "
                        f"{exc}"
                    )

            token_data = _authorize_via_browser()
            _save_cache(token_data)
            return token_data["access_token"]

    def start_auto_refresh(self) -> None:
        return

    def stop(self) -> None:
        return


def _install_monkey_patch() -> None:
    import mcp_atlassian.utils.agora_oauth as agora_oauth

    def create_cached_refresher():
        return CachedAgoraTokenRefresher()

    agora_oauth.create_agora_token_refresher = create_cached_refresher
    agora_oauth._agora_token_refresher = None


def _init_oauth_only() -> int:
    token = CachedAgoraTokenRefresher().get_token()
    _stderr(f"OAuth cache initialized: {token[:16]}...")
    return 0


def main() -> int:
    if "--init-oauth" in sys.argv:
        return _init_oauth_only()

    _install_monkey_patch()
    from mcp_atlassian import main as mcp_main

    return int(mcp_main() or 0)


if __name__ == "__main__":
    raise SystemExit(main())
