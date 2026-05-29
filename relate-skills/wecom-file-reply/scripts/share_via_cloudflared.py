#!/usr/bin/env python3
"""Expose a local file via a persistent local HTTP server + Cloudflare tunnel.

This is intended for bridge sessions where a user needs a mobile-openable link
and third-party temporary file hosts or one-shot tunnels are unreliable.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path


STATE_DIRNAME = ".cloudflared_share"
PORT_RANGE = range(18280, 18321)
HTTP_START_TIMEOUT = 10.0
TUNNEL_START_TIMEOUT = 30.0
HEALTHCHECK_TIMEOUT = 60.0
DEFAULT_MAX_AGE_SECONDS = 24 * 60 * 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="Local file to publish")
    parser.add_argument("--export-dir", help="User-visible directory to serve")
    parser.add_argument(
        "--max-age-seconds",
        type=int,
        help="How long a shared link may be reused before rotating to a new tunnel",
    )
    parser.add_argument(
        "--tools-dir",
        default="/home/jenkins/.codex/tools",
        help="Directory containing cloudflared",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def reserve_port() -> int:
    for candidate in PORT_RANGE:
        with socket.socket() as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", candidate))
            except OSError:
                continue
            return candidate
    raise RuntimeError("no free port available for local share")


def copy_into_export_dir(src: Path, export_dir: Path) -> Path:
    ensure_dir(export_dir)
    dst = export_dir / src.name
    if src.resolve() != dst.resolve():
        shutil.copy2(src, dst)
    return dst


def load_state(state_path: Path) -> dict | None:
    if not state_path.exists():
        return None
    try:
        return json.loads(state_path.read_text())
    except Exception:
        return None


def save_state(state_path: Path, payload: dict) -> None:
    state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def resolve_max_age_seconds(raw_value: str | None, cli_value: int | None) -> int:
    if cli_value is not None:
        if cli_value <= 0:
            raise ValueError("--max-age-seconds must be positive")
        return cli_value
    if raw_value:
        try:
            value = int(raw_value)
        except ValueError as exc:
            raise ValueError("WECOM_FILE_CLOUDFLARED_MAX_AGE_SECONDS must be an integer") from exc
        if value <= 0:
            raise ValueError("WECOM_FILE_CLOUDFLARED_MAX_AGE_SECONDS must be positive")
        return value
    return DEFAULT_MAX_AGE_SECONDS


def start_http_server(root: Path, state_dir: Path) -> tuple[int, int, Path]:
    log_path = state_dir / "http_server.log"
    port = reserve_port()
    logf = open(log_path, "ab", buffering=0)
    proc = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(port), "--bind", "127.0.0.1", "--directory", str(root)],
        stdout=logf,
        stderr=logf,
        start_new_session=True,
    )
    deadline = time.time() + HTTP_START_TIMEOUT
    while time.time() < deadline:
        if port_open(port):
            return proc.pid, port, log_path
        time.sleep(0.1)
    raise RuntimeError("local http server did not start")


def start_cloudflared(local_port: int, tools_dir: Path, state_dir: Path) -> tuple[int, str, Path]:
    binary = tools_dir / "cloudflared"
    if not binary.exists():
        raise RuntimeError(f"cloudflared not found at {binary}")

    log_path = state_dir / "cloudflared.log"
    logf = open(log_path, "ab", buffering=0)
    proc = subprocess.Popen(
        [str(binary), "tunnel", "--url", f"http://127.0.0.1:{local_port}"],
        stdout=logf,
        stderr=logf,
        text=True,
        start_new_session=True,
    )

    deadline = time.time() + TUNNEL_START_TIMEOUT
    url = None
    while time.time() < deadline:
        if log_path.exists():
            text = log_path.read_text(errors="ignore")
            for line in text.splitlines():
                marker = "https://"
                if marker in line and "trycloudflare.com" in line:
                    start = line.index(marker)
                    url = line[start:].strip().rstrip("|").strip()
                    break
        if url:
            return proc.pid, url, log_path
        if proc.poll() is not None:
            raise RuntimeError("cloudflared exited before producing a URL")
        time.sleep(0.2)

    raise RuntimeError("cloudflared did not produce a public URL in time")


def stop_pid(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return


def healthcheck(url: str, filename: str) -> None:
    import urllib.request

    full_url = f"{url.rstrip('/')}/{urllib.parse.quote(filename)}"
    deadline = time.time() + HEALTHCHECK_TIMEOUT
    last_error = None
    while time.time() < deadline:
        try:
            req = urllib.request.Request(full_url, method="HEAD")
            with urllib.request.urlopen(req, timeout=20) as resp:
                if getattr(resp, "status", 200) < 400:
                    return
                last_error = RuntimeError(f"healthcheck failed: HTTP {resp.status}")
        except Exception as exc:
            last_error = exc
        time.sleep(1.0)
    if last_error is None:
        raise RuntimeError("healthcheck failed")
    raise RuntimeError(str(last_error))


def ensure_share(file_path: Path, export_dir: Path, tools_dir: Path, max_age_seconds: int) -> str:
    staged = copy_into_export_dir(file_path, export_dir)
    state_dir = export_dir / STATE_DIRNAME
    ensure_dir(state_dir)
    state_path = state_dir / "state.json"
    state = load_state(state_path)

    if state:
        http_pid = int(state.get("http_pid", 0))
        tunnel_pid = int(state.get("tunnel_pid", 0))
        base_url = state.get("base_url")
        http_port = int(state.get("http_port", 0))
        created_at = float(state.get("created_at", 0))
        expires_at = float(state.get("expires_at", 0))
        is_expired = not created_at or not expires_at or time.time() >= expires_at
        if (
            http_pid
            and tunnel_pid
            and base_url
            and http_port
            and not is_expired
            and pid_alive(http_pid)
            and pid_alive(tunnel_pid)
            and port_open(http_port)
        ):
            healthcheck(base_url, staged.name)
            return f"{base_url.rstrip('/')}/{urllib.parse.quote(staged.name)}"

        if http_pid:
            stop_pid(http_pid)
        if tunnel_pid:
            stop_pid(tunnel_pid)

    http_pid, http_port, http_log = start_http_server(export_dir, state_dir)
    tunnel_pid, base_url, tunnel_log = start_cloudflared(http_port, tools_dir, state_dir)
    healthcheck(base_url, staged.name)
    created_at = time.time()
    expires_at = created_at + max_age_seconds
    save_state(
        state_path,
        {
            "http_pid": http_pid,
            "http_port": http_port,
            "http_log": str(http_log),
            "tunnel_pid": tunnel_pid,
            "tunnel_log": str(tunnel_log),
            "base_url": base_url,
            "export_dir": str(export_dir),
            "created_at": created_at,
            "expires_at": expires_at,
            "max_age_seconds": max_age_seconds,
        },
    )
    return f"{base_url.rstrip('/')}/{urllib.parse.quote(staged.name)}"


def main() -> int:
    try:
        args = parse_args()
        file_path = Path(args.file).expanduser().resolve()
        max_age_seconds = resolve_max_age_seconds(
            raw_value=os.getenv("WECOM_FILE_CLOUDFLARED_MAX_AGE_SECONDS"),
            cli_value=args.max_age_seconds,
        )
        export_dir_raw = (
            args.export_dir
            or os.getenv("EXPORT_DIR")
            or os.getenv("WECOM_BRIDGE_EXPORT_DIR")
            or os.getenv("CHATFILE_DIR")
            or os.getenv("WECOM_BRIDGE_CHATFILE_DIR")
        )
        if not export_dir_raw:
            raise ValueError("missing export dir; pass --export-dir or set EXPORT_DIR/WECOM_BRIDGE_EXPORT_DIR")
        export_dir = Path(export_dir_raw).expanduser().resolve()
        tools_dir = Path(args.tools_dir).expanduser().resolve()

        if not file_path.is_file():
            raise ValueError("file does not exist or is not a regular file")

        url = ensure_share(
            file_path=file_path,
            export_dir=export_dir,
            tools_dir=tools_dir,
            max_age_seconds=max_age_seconds,
        )
        print(url)
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
