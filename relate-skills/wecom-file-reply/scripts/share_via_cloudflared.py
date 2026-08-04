#!/usr/bin/env python3
"""Expose a local file via a persistent local HTTP server + Cloudflare tunnel.

This is intended for bridge sessions where a user needs a mobile-openable link
and third-party temporary file hosts or one-shot tunnels are unreliable.
"""

from __future__ import annotations

import argparse
import fcntl
import http.server
import json
import os
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Iterator


STATE_DIRNAME = ".cloudflared_share"
PORT_RANGE = range(18280, 18321)
HTTP_START_TIMEOUT = 10.0
TUNNEL_START_TIMEOUT = 30.0
HEALTHCHECK_TIMEOUT = 60.0
DEFAULT_MAX_AGE_SECONDS = 24 * 60 * 60
PUBLIC_DIRNAME = "public"
INTERNAL_HTTP_SERVER = "--internal-http-server"
INTERNAL_EXPIRE_STATE = "--internal-expire-state"


def script_path() -> Path:
    return Path(__file__).resolve()


class FileOnlyRequestHandler(http.server.SimpleHTTPRequestHandler):
    def list_directory(self, path):
        del path
        self.send_error(404, "File not found")
        return None

    def send_head(self):
        requested = Path(self.translate_path(self.path))
        root = Path(self.directory).resolve()
        try:
            resolved = requested.resolve()
            resolved.relative_to(root)
        except (OSError, ValueError):
            self.send_error(404, "File not found")
            return None
        return super().send_head()


def run_internal_http_server(root: Path, port: int) -> int:
    resolved_root = root.expanduser().resolve()
    if root.is_symlink() or not resolved_root.is_dir():
        raise RuntimeError(f"invalid HTTP share root: {root}")
    handler = partial(FileOnlyRequestHandler, directory=str(resolved_root))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


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


def ensure_private_dir(path: Path) -> None:
    if path.is_symlink():
        raise OSError(f"private state directory must not be a symlink: {path}")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise OSError(f"private state directory is not a directory: {path}")
    path.chmod(0o700)


def open_private_append_log(path: Path):
    if not hasattr(os, "O_NOFOLLOW"):
        raise OSError("secure share logs require O_NOFOLLOW support")
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(f"share log is not a regular file: {path}")
        os.fchmod(fd, 0o600)
        return os.fdopen(fd, "ab", buffering=0)
    except Exception:
        os.close(fd)
        raise


@contextmanager
def share_state_lock(state_dir: Path) -> Iterator[None]:
    lock_path = state_dir / "state.lock"
    if not hasattr(os, "O_NOFOLLOW"):
        raise OSError("secure share state locking requires O_NOFOLLOW support")
    flags = os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(lock_path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(f"share state lock is not a regular file: {lock_path}")
        os.fchmod(fd, 0o600)
        os.set_blocking(fd, True)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def process_argv(pid: int) -> list[str]:
    if pid <= 0:
        return []
    try:
        return [
            item.decode("utf-8", errors="replace")
            for item in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            if item
        ]
    except OSError:
        return []


def owns_http_process(pid: int, port: int, serve_dir: Path) -> bool:
    return process_argv(pid) == [
        sys.executable,
        str(script_path()),
        INTERNAL_HTTP_SERVER,
        str(port),
        str(serve_dir),
    ]


def owns_tunnel_process(pid: int, port: int, tools_dir: Path) -> bool:
    return process_argv(pid) == [
        str(tools_dir / "cloudflared"),
        "tunnel",
        "--url",
        f"http://127.0.0.1:{port}",
    ]


def owns_expiry_reaper(pid: int, state_dir: Path, generation: str) -> bool:
    return process_argv(pid) == [
        sys.executable,
        str(script_path()),
        INTERNAL_EXPIRE_STATE,
        str(state_dir),
        generation,
    ]


def iter_process_argv() -> Iterator[tuple[int, list[str]]]:
    try:
        proc_entries = list(Path("/proc").iterdir())
    except OSError:
        return
    for entry in proc_entries:
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        argv = process_argv(pid)
        if argv:
            yield pid, argv


def owns_legacy_http_process(pid: int, port: int, export_dir: Path) -> bool:
    return process_argv(pid) == [
        sys.executable,
        "-m",
        "http.server",
        str(port),
        "--bind",
        "127.0.0.1",
        "--directory",
        str(export_dir),
    ]


def legacy_http_processes(export_dir: Path) -> list[tuple[int, int]]:
    matches: list[tuple[int, int]] = []
    expected_prefix = [sys.executable, "-m", "http.server"]
    expected_suffix = ["--bind", "127.0.0.1", "--directory", str(export_dir)]
    for pid, argv in iter_process_argv():
        if len(argv) != 9 or argv[:3] != expected_prefix or argv[4:] != expected_suffix:
            continue
        try:
            port = int(argv[3])
        except ValueError:
            continue
        if port in PORT_RANGE:
            matches.append((pid, port))
    return matches


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


def terminate_process(proc: subprocess.Popen, timeout: float = 3.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def terminate_owned_pid(pid: int, owns_process, timeout: float = 3.0) -> None:
    if pid <= 0 or not owns_process(pid):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not pid_alive(pid) or not owns_process(pid):
            return
        time.sleep(0.05)
    if not owns_process(pid):
        return
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def cleanup_legacy_share_processes(export_dir: Path, tools_dir: Path) -> None:
    for http_pid, port in legacy_http_processes(export_dir):
        tunnel_match = next(
            (
                tunnel_pid
                for tunnel_pid, argv in iter_process_argv()
                if argv
                == [
                    str(tools_dir / "cloudflared"),
                    "tunnel",
                    "--url",
                    f"http://127.0.0.1:{port}",
                ]
            ),
            0,
        )
        terminate_owned_pid(tunnel_match, lambda pid, expected_port=port: owns_tunnel_process(pid, expected_port, tools_dir))
        terminate_owned_pid(
            http_pid,
            lambda pid, expected_port=port: owns_legacy_http_process(pid, expected_port, export_dir),
        )


def copy_into_export_dir(src: Path, export_dir: Path) -> Path:
    ensure_dir(export_dir)
    dst = export_dir / src.name
    if src.resolve() != dst.resolve():
        fd, temp_name = tempfile.mkstemp(prefix=f".{dst.name}.", suffix=".tmp", dir=export_dir)
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            shutil.copy2(src, temp_path)
            os.replace(temp_path, dst)
        finally:
            temp_path.unlink(missing_ok=True)
    return dst


def load_state(state_path: Path) -> dict | None:
    if state_path.is_symlink() or not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text())
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def save_state(state_path: Path, payload: dict) -> None:
    fd = -1
    temp_path: Path | None = None
    try:
        fd, temp_name = tempfile.mkstemp(prefix=f".{state_path.name}.", suffix=".tmp", dir=state_path.parent)
        temp_path = Path(temp_name)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=True) as handle:
            fd = -1
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, state_path)
        temp_path = None
        directory_fd = os.open(state_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


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


def start_http_server(root: Path, state_dir: Path, generation: str) -> tuple[subprocess.Popen, int, Path]:
    log_path = state_dir / f"http_server-{generation}.log"
    port = reserve_port()
    with open_private_append_log(log_path) as logf:
        proc = subprocess.Popen(
            [sys.executable, str(script_path()), INTERNAL_HTTP_SERVER, str(port), str(root)],
            stdout=logf,
            stderr=logf,
            start_new_session=True,
        )
    try:
        deadline = time.time() + HTTP_START_TIMEOUT
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError("local http server exited during startup")
            if port_open(port):
                return proc, port, log_path
            time.sleep(0.1)
        raise RuntimeError("local http server did not start")
    except Exception:
        terminate_process(proc)
        raise


def start_cloudflared(
    local_port: int,
    tools_dir: Path,
    state_dir: Path,
    generation: str,
) -> tuple[subprocess.Popen, str, Path]:
    binary = tools_dir / "cloudflared"
    if not binary.exists():
        raise RuntimeError(f"cloudflared not found at {binary}")

    log_path = state_dir / f"cloudflared-{generation}.log"
    with open_private_append_log(log_path) as logf:
        proc = subprocess.Popen(
            [str(binary), "tunnel", "--url", f"http://127.0.0.1:{local_port}"],
            stdout=logf,
            stderr=logf,
            text=True,
            start_new_session=True,
        )

    deadline = time.time() + TUNNEL_START_TIMEOUT
    url = None
    try:
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
                return proc, url, log_path
            if proc.poll() is not None:
                raise RuntimeError("cloudflared exited before producing a URL")
            time.sleep(0.2)
        raise RuntimeError("cloudflared did not produce a public URL in time")
    except Exception:
        terminate_process(proc)
        raise


def stop_pid(pid: int) -> None:
    if pid <= 0:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return


def start_expiry_reaper(state_dir: Path, generation: str) -> subprocess.Popen:
    log_path = state_dir / f"expiry_reaper-{generation}.log"
    with open_private_append_log(log_path) as logf:
        return subprocess.Popen(
            [sys.executable, str(script_path()), INTERNAL_EXPIRE_STATE, str(state_dir), generation],
            stdout=logf,
            stderr=logf,
            start_new_session=True,
        )


def _state_process_values(state: dict) -> tuple[int, int, int, int, float, float]:
    try:
        return (
            max(0, int(state.get("http_pid", 0))),
            max(0, int(state.get("tunnel_pid", 0))),
            max(0, int(state.get("reaper_pid", 0))),
            max(0, int(state.get("http_port", 0))),
            float(state.get("created_at", 0)),
            float(state.get("expires_at", 0)),
        )
    except (TypeError, ValueError):
        return 0, 0, 0, 0, 0, 0


def _state_path(state: dict, key: str) -> Path | None:
    raw = str(state.get(key) or "").strip()
    if not raw:
        return None
    try:
        return Path(raw).expanduser().resolve()
    except OSError:
        return None


def stop_state_processes(
    state: dict,
    *,
    state_dir: Path,
    allowed_serve_roots: set[Path],
    tools_dir: Path,
) -> None:
    http_pid, tunnel_pid, reaper_pid, http_port, _created_at, _expires_at = _state_process_values(state)
    generation = str(state.get("generation") or "").strip()
    serve_root = _state_path(state, "serve_dir") or _state_path(state, "export_dir")
    normalized_roots = {path.expanduser().resolve() for path in allowed_serve_roots}
    root_allowed = serve_root is not None and serve_root in normalized_roots

    if generation and reaper_pid != os.getpid():
        terminate_owned_pid(reaper_pid, lambda pid: owns_expiry_reaper(pid, state_dir, generation))
    terminate_owned_pid(tunnel_pid, lambda pid: owns_tunnel_process(pid, http_port, tools_dir))
    if root_allowed and serve_root is not None:
        terminate_owned_pid(
            http_pid,
            lambda pid: owns_http_process(pid, http_port, serve_root)
            or owns_legacy_http_process(pid, http_port, serve_root),
        )


def expire_share_state(state_dir: Path, generation: str) -> int:
    ensure_private_dir(state_dir)
    state_path = state_dir / "state.json"
    expected_serve_dir = (state_dir / PUBLIC_DIRNAME).resolve()
    while True:
        delay = 0.0
        with share_state_lock(state_dir):
            state = load_state(state_path)
            if not state or str(state.get("generation") or "").strip() != generation:
                return 0
            _http_pid, _tunnel_pid, _reaper_pid, _http_port, _created_at, expires_at = _state_process_values(state)
            if not expires_at:
                return 0
            delay = expires_at - time.time()
            if delay <= 0:
                tools_dir = _state_path(state, "tools_dir")
                serve_dir = _state_path(state, "serve_dir")
                if tools_dir is None or serve_dir != expected_serve_dir:
                    return 0
                stop_state_processes(
                    state,
                    state_dir=state_dir,
                    allowed_serve_roots={expected_serve_dir},
                    tools_dir=tools_dir,
                )
                current = load_state(state_path)
                if current and str(current.get("generation") or "").strip() == generation:
                    state_path.unlink(missing_ok=True)
                return 0
        time.sleep(min(max(delay, 0.05), 60.0))


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


def _ensure_share_locked(
    staged: Path,
    export_dir: Path,
    serve_dir: Path,
    tools_dir: Path,
    max_age_seconds: int,
    state_dir: Path,
) -> str:
    state_path = state_dir / "state.json"
    state = load_state(state_path)

    if state:
        http_pid, tunnel_pid, reaper_pid, http_port, created_at, expires_at = _state_process_values(state)
        base_url = state.get("base_url")
        generation = str(state.get("generation") or "").strip()
        state_serve_dir = _state_path(state, "serve_dir")
        is_expired = not created_at or not expires_at or time.time() >= expires_at
        owns_http = state_serve_dir == serve_dir.resolve() and owns_http_process(http_pid, http_port, serve_dir)
        owns_tunnel = owns_tunnel_process(tunnel_pid, http_port, tools_dir)
        owns_reaper = bool(generation) and owns_expiry_reaper(reaper_pid, state_dir, generation)
        if (
            http_pid
            and tunnel_pid
            and base_url
            and http_port
            and not is_expired
            and pid_alive(http_pid)
            and pid_alive(tunnel_pid)
            and owns_http
            and owns_tunnel
            and owns_reaper
            and port_open(http_port)
        ):
            try:
                healthcheck(base_url, staged.name)
            except Exception:
                pass
            else:
                return f"{base_url.rstrip('/')}/{urllib.parse.quote(staged.name)}"

        stop_state_processes(
            state,
            state_dir=state_dir,
            allowed_serve_roots={serve_dir, export_dir},
            tools_dir=tools_dir,
        )

    http_proc = None
    tunnel_proc = None
    reaper_proc = None
    generation = secrets.token_hex(12)
    try:
        http_proc, http_port, http_log = start_http_server(serve_dir, state_dir, generation)
        tunnel_proc, base_url, tunnel_log = start_cloudflared(http_port, tools_dir, state_dir, generation)
        healthcheck(base_url, staged.name)
        created_at = time.time()
        expires_at = created_at + max_age_seconds
        next_state = {
            "generation": generation,
            "http_pid": http_proc.pid,
            "http_port": http_port,
            "http_log": str(http_log),
            "tunnel_pid": tunnel_proc.pid,
            "tunnel_log": str(tunnel_log),
            "base_url": base_url,
            "export_dir": str(export_dir),
            "serve_dir": str(serve_dir),
            "tools_dir": str(tools_dir),
            "created_at": created_at,
            "expires_at": expires_at,
            "max_age_seconds": max_age_seconds,
        }
        save_state(state_path, next_state)
        reaper_proc = start_expiry_reaper(state_dir, generation)
        next_state["reaper_pid"] = reaper_proc.pid
        save_state(state_path, next_state)
    except Exception:
        if reaper_proc is not None:
            terminate_process(reaper_proc)
        if tunnel_proc is not None:
            terminate_process(tunnel_proc)
        if http_proc is not None:
            terminate_process(http_proc)
        current = load_state(state_path)
        if current and str(current.get("generation") or "").strip() == generation:
            state_path.unlink(missing_ok=True)
        raise
    return f"{base_url.rstrip('/')}/{urllib.parse.quote(staged.name)}"


def ensure_share(file_path: Path, export_dir: Path, tools_dir: Path, max_age_seconds: int) -> str:
    state_dir = export_dir / STATE_DIRNAME
    ensure_private_dir(state_dir)
    with share_state_lock(state_dir):
        cleanup_legacy_share_processes(export_dir, tools_dir)
        exported = copy_into_export_dir(file_path, export_dir)
        serve_dir = state_dir / PUBLIC_DIRNAME
        ensure_private_dir(serve_dir)
        staged = copy_into_export_dir(exported, serve_dir)
        return _ensure_share_locked(staged, export_dir, serve_dir, tools_dir, max_age_seconds, state_dir)


def run_internal_command(argv: list[str]) -> int | None:
    if len(argv) == 3 and argv[0] == INTERNAL_HTTP_SERVER:
        port = int(argv[1])
        if port not in PORT_RANGE:
            raise ValueError(f"invalid internal HTTP port: {port}")
        return run_internal_http_server(Path(argv[2]), port)
    if len(argv) == 3 and argv[0] == INTERNAL_EXPIRE_STATE:
        return expire_share_state(Path(argv[1]).expanduser().resolve(), argv[2])
    return None


def main() -> int:
    try:
        internal_result = run_internal_command(sys.argv[1:])
        if internal_result is not None:
            return internal_result
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
