#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import fcntl
import json
import math
import os
import secrets
import shlex
import signal
import socket
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

from workspace_bridge.atomic_io import atomic_write_text
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent
START_SCRIPT = REPO_ROOT / "start.sh"


def positive_int_file(path: Path) -> int:
    text = path.read_text(encoding="utf-8").strip()
    if not text.isdigit() or int(text) <= 0:
        raise ValueError(f"invalid PID file: {path}")
    return int(text)


def process_environment(pid: int) -> dict[str, str]:
    entries = Path(f"/proc/{pid}/environ").read_bytes().split(b"\0")
    result: dict[str, str] = {}
    for entry in entries:
        if b"=" not in entry:
            continue
        key, value = entry.split(b"=", 1)
        result[key.decode("utf-8", errors="replace")] = value.decode("utf-8", errors="replace")
    return result


def process_argv(pid: int) -> list[str]:
    return [
        item.decode("utf-8", errors="replace")
        for item in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        if item
    ]


def is_owned_process(pid: int, *, kind: str, control_root: Path, repo_root: Path = REPO_ROOT) -> bool:
    if pid <= 0:
        return False
    expected_script = repo_root / ("bridge.py" if kind == "bridge" else "bridge_watchdog.sh")
    try:
        argv = process_argv(pid)
        process_root = Path(process_environment(pid).get("BRIDGE_CONTROL_ROOT", "")).expanduser().resolve()
    except (OSError, ValueError):
        return False
    return str(expected_script.resolve()) in argv and process_root == control_root.resolve()


def choose_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def directory_flock_available(path: Path) -> bool:
    fd = os.open(path, os.O_RDONLY)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    finally:
        os.close(fd)


def request_json(url: str, *, token: str, timeout: float) -> Any:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"unexpected HTTP status {response.status}: {url}")
        return json.loads(response.read().decode("utf-8"))


def wait_for_health(base_url: str, *, token: str, timeout_sec: float, request_timeout_sec: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_sec
    last_error = "health check did not run"
    while time.monotonic() < deadline:
        try:
            payload = request_json(f"{base_url}/livez", token=token, timeout=request_timeout_sec)
            if isinstance(payload, dict) and payload.get("ok") is True:
                return payload
            last_error = f"unexpected health payload: {payload!r}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.1)
    raise TimeoutError(f"health check failed: {last_error}")


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))
    return ordered[index]


def run_http_load(
    url: str,
    *,
    token: str,
    duration_sec: float,
    concurrency: int,
    request_timeout_sec: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + duration_sec
    latencies_ms: list[float] = []
    failures: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        while time.monotonic() < deadline:
            started = time.perf_counter()
            try:
                payload = request_json(url, token=token, timeout=request_timeout_sec)
                if not isinstance(payload, (dict, list)):
                    raise RuntimeError("HTTP response was not JSON object/array")
            except Exception as exc:
                with lock:
                    failures.append(str(exc))
            finally:
                elapsed_ms = (time.perf_counter() - started) * 1000
                with lock:
                    latencies_ms.append(elapsed_ms)

    wall_started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(concurrency)]
        for future in futures:
            future.result()
    wall_sec = max(0.001, time.perf_counter() - wall_started)
    total = len(latencies_ms)
    return {
        "requests": total,
        "failures": len(failures),
        "firstErrors": failures[:5],
        "durationSec": round(wall_sec, 3),
        "requestsPerSec": round(total / wall_sec, 2),
        "latencyMs": {
            "mean": round(statistics.fmean(latencies_ms), 3) if latencies_ms else 0.0,
            "p50": round(percentile(latencies_ms, 0.50), 3),
            "p95": round(percentile(latencies_ms, 0.95), 3),
            "p99": round(percentile(latencies_ms, 0.99), 3),
            "max": round(max(latencies_ms), 3) if latencies_ms else 0.0,
        },
    }


def process_tree(root_pid: int) -> set[int]:
    parent_children: dict[int, set[int]] = {}
    for proc_path in Path("/proc").iterdir():
        if not proc_path.name.isdigit():
            continue
        try:
            stat_text = (proc_path / "stat").read_text(encoding="utf-8")
            fields = stat_text[stat_text.rfind(")") + 2 :].split()
            parent_pid = int(fields[1])
            pid = int(proc_path.name)
        except (OSError, ValueError, IndexError):
            continue
        parent_children.setdefault(parent_pid, set()).add(pid)
    discovered = {root_pid}
    pending = [root_pid]
    while pending:
        parent = pending.pop()
        for child in parent_children.get(parent, set()):
            if child not in discovered:
                discovered.add(child)
                pending.append(child)
    return discovered


def process_rss_kib(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return 0


def process_cpu_ticks(pid: int) -> int:
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        fields = stat_text[stat_text.rfind(")") + 2 :].split()
        return int(fields[11]) + int(fields[12])
    except (OSError, ValueError, IndexError):
        return 0


def filesystem_queue_depth(shared_root: Path, runtime_root: Path) -> int:
    count = 0
    for root in (shared_root / ".scheduled-messages", runtime_root / ".local-file-send-queue", runtime_root / ".local-send-message-queue"):
        if not root.exists():
            continue
        for path in root.rglob("*.json"):
            if path.parent.name in {"pending", "processing"}:
                count += 1
    return count


def sample_resources(bridge_pid: int, *, shared_root: Path, runtime_root: Path) -> dict[str, Any]:
    pids = process_tree(bridge_pid)
    return {
        "atMs": int(time.time() * 1000),
        "rssKiB": sum(process_rss_kib(pid) for pid in pids),
        "cpuTicks": sum(process_cpu_ticks(pid) for pid in pids),
        "processCount": len(pids),
        "descendantCount": max(0, len(pids) - 1),
        "filesystemQueueDepth": filesystem_queue_depth(shared_root, runtime_root),
    }


def monitor_resources(
    bridge_pid: int,
    *,
    shared_root: Path,
    runtime_root: Path,
    stop_event: threading.Event,
    interval_sec: float,
    samples: list[dict[str, Any]],
) -> None:
    while not stop_event.is_set():
        samples.append(sample_resources(bridge_pid, shared_root=shared_root, runtime_root=runtime_root))
        stop_event.wait(interval_sec)
    samples.append(sample_resources(bridge_pid, shared_root=shared_root, runtime_root=runtime_root))


def summarize_resources(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {"sampleCount": 0, "maxRssKiB": 0, "maxCpuPercent": 0.0, "maxDescendants": 0, "maxQueueDepth": 0}
    clock_ticks = max(1, int(os.sysconf("SC_CLK_TCK")))
    cpu_percentages: list[float] = []
    for previous, current in zip(samples, samples[1:]):
        elapsed = max(0.001, (int(current["atMs"]) - int(previous["atMs"])) / 1000)
        ticks = max(0, int(current["cpuTicks"]) - int(previous["cpuTicks"]))
        cpu_percentages.append((ticks / clock_ticks) / elapsed * 100)
    return {
        "sampleCount": len(samples),
        "maxRssKiB": max(int(item["rssKiB"]) for item in samples),
        "maxCpuPercent": round(max(cpu_percentages, default=0.0), 2),
        "maxDescendants": max(int(item["descendantCount"]) for item in samples),
        "maxQueueDepth": max(int(item["filesystemQueueDepth"]) for item in samples),
    }


def wait_for_replacement_pid(
    pid_file: Path,
    old_pid: int,
    *,
    control_root: Path,
    timeout_sec: float,
) -> int:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            candidate = positive_int_file(pid_file)
        except (OSError, ValueError):
            candidate = 0
        if candidate != old_pid and is_owned_process(candidate, kind="bridge", control_root=control_root):
            return candidate
        time.sleep(0.1)
    raise TimeoutError("watchdog did not replace the killed bridge process")


def wait_for_exit(pid: int, timeout_sec: float) -> bool:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if not Path(f"/proc/{pid}").exists():
            return True
        time.sleep(0.1)
    return not Path(f"/proc/{pid}").exists()


def signal_owned_process(pid: int, sig: signal.Signals, *, kind: str, control_root: Path) -> bool:
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("safe readiness-process signaling requires pidfd support")
    try:
        pidfd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return False
    try:
        if not is_owned_process(pid, kind=kind, control_root=control_root):
            return False
        try:
            signal.pidfd_send_signal(pidfd, sig)
        except ProcessLookupError:
            return False
        return True
    finally:
        os.close(pidfd)


def terminate_owned_process(pid: int, *, kind: str, control_root: Path, timeout_sec: float = 12) -> bool:
    if not is_owned_process(pid, kind=kind, control_root=control_root):
        return not Path(f"/proc/{pid}").exists()
    try:
        signaled = signal_owned_process(pid, signal.SIGTERM, kind=kind, control_root=control_root)
    except (PermissionError, RuntimeError):
        return False
    if not signaled:
        return not Path(f"/proc/{pid}").exists()
    if wait_for_exit(pid, timeout_sec):
        return True
    if is_owned_process(pid, kind=kind, control_root=control_root):
        try:
            signaled = signal_owned_process(pid, signal.SIGKILL, kind=kind, control_root=control_root)
        except (PermissionError, RuntimeError):
            return False
        if not signaled:
            return not Path(f"/proc/{pid}").exists()
    return wait_for_exit(pid, 5)


def write_env_file(path: Path, values: dict[str, str]) -> None:
    path.write_text("".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items()), encoding="utf-8")
    path.chmod(0o600)


def sanitized_launcher_environment(env_file: Path) -> dict[str, str]:
    environment = dict(os.environ)
    for key in list(environment):
        if key.startswith(("BRIDGE_", "WECOM_")) or key in {
            "BOTS_FILE",
            "CODEX_HOME",
            "FILE_SEND_ROOTS",
            "HOST",
            "LOCAL_FILE_SEND_QUEUE_ROOT",
            "LOCAL_SEND_MESSAGE_QUEUE_ROOT",
            "PORT",
            "WORK_DIR",
        }:
            environment.pop(key, None)
    environment["BRIDGE_ENV_FILE"] = str(env_file)
    environment["NO_PROXY"] = "127.0.0.1,localhost"
    environment["no_proxy"] = "127.0.0.1,localhost"
    return environment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Isolated production-like bridge readiness probe")
    parser.add_argument("--duration-sec", type=float, default=10.0)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--startup-timeout-sec", type=float, default=30.0)
    parser.add_argument("--recovery-timeout-sec", type=float, default=30.0)
    parser.add_argument("--request-timeout-sec", type=float, default=3.0)
    parser.add_argument("--sample-interval-sec", type=float, default=0.5)
    parser.add_argument("--min-requests", type=int, default=100)
    parser.add_argument("--max-rss-mib", type=float, default=512.0)
    parser.add_argument("--max-p95-ms", type=float, default=2000.0)
    parser.add_argument("--max-descendants", type=int, default=4)
    parser.add_argument("--output", type=Path)
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.duration_sec <= 0:
        raise ValueError("duration-sec must be > 0")
    if args.concurrency <= 0:
        raise ValueError("concurrency must be > 0")
    if min(args.startup_timeout_sec, args.recovery_timeout_sec, args.request_timeout_sec, args.sample_interval_sec) <= 0:
        raise ValueError("timeouts and sample interval must be > 0")
    if args.min_requests <= 0 or args.max_rss_mib <= 0 or args.max_p95_ms <= 0 or args.max_descendants < 0:
        raise ValueError("thresholds must be positive")


def run_probe(args: argparse.Namespace) -> dict[str, Any]:
    validate_args(args)
    result: dict[str, Any] = {"pass": False, "startedAtMs": int(time.time() * 1000)}
    with tempfile.TemporaryDirectory(prefix="bridge-production-readiness-") as temp_dir_text:
        temp_dir = Path(temp_dir_text).resolve()
        control_root = temp_dir / "control"
        runtime_root = temp_dir / "runtime"
        shared_root = temp_dir / "shared"
        codex_home = temp_dir / "codex-home"
        work_dir = temp_dir / "work"
        for root in (control_root, runtime_root, shared_root, codex_home, work_dir):
            root.mkdir(parents=True, exist_ok=True)
        port = choose_free_port()
        token = secrets.token_urlsafe(24)
        env_file = temp_dir / "instance.env"
        write_env_file(
            env_file,
            {
                "BRIDGE_BIND": f"127.0.0.1:{port}",
                "BRIDGE_TOKEN": token,
                "BRIDGE_CONTROL_ROOT": str(control_root),
                "BRIDGE_RUNTIME_ROOT": str(runtime_root),
                "BRIDGE_SHARED_RUNTIME_ROOT": str(shared_root),
                "BOTS_FILE": str(shared_root / "bots.json"),
                "CODEX_HOME": str(codex_home),
                "WORK_DIR": str(work_dir),
                "FILE_SEND_ROOTS": str(runtime_root),
                "WECOM_BOOTSTRAP_BOTS_JSON": "[]",
                "WECOM_BOT_ENABLED": "false",
                "BRIDGE_WATCHDOG_ENABLED": "true",
                "BRIDGE_WATCHDOG_POLL_SEC": "1",
                "BRIDGE_WATCHDOG_HEALTH_TIMEOUT_SEC": "1",
                "BRIDGE_WATCHDOG_STARTUP_GRACE_SEC": "1",
                "BRIDGE_WATCHDOG_FAIL_THRESHOLD": "1",
                "BRIDGE_WATCHDOG_RESTART_BACKOFF_SEC": "1",
                "BRIDGE_WATCHDOG_RESTART_WINDOW_SEC": "60",
                "BRIDGE_WATCHDOG_MAX_RESTART_STREAK": "5",
                "BRIDGE_WATCHDOG_COOLDOWN_SEC": "1",
            },
        )
        environment = sanitized_launcher_environment(env_file)
        guard_pid = 0
        bridge_pid = 0
        recovered_pid = 0
        cleanup_ok = False
        try:
            started = subprocess.run(
                ["sh", str(START_SCRIPT)],
                cwd=REPO_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=args.startup_timeout_sec,
            )
            result["launcher"] = {
                "returnCode": started.returncode,
                "stdout": started.stdout.strip(),
                "stderr": started.stderr.strip(),
            }
            if started.returncode != 0:
                raise RuntimeError("isolated start.sh failed")
            guard_pid = positive_int_file(control_root / ".bridge.guard.pid")
            bridge_pid = positive_int_file(control_root / ".bridge.pid")
            if not is_owned_process(guard_pid, kind="watchdog", control_root=control_root):
                raise RuntimeError("watchdog PID ownership validation failed")
            if not is_owned_process(bridge_pid, kind="bridge", control_root=control_root):
                raise RuntimeError("bridge PID ownership validation failed")
            start_lock_released = directory_flock_available(control_root)
            if not start_lock_released:
                raise RuntimeError("start.sh instance lock remained held after launcher exit")
            base_url = f"http://127.0.0.1:{port}"
            wait_for_health(
                base_url,
                token=token,
                timeout_sec=args.startup_timeout_sec,
                request_timeout_sec=args.request_timeout_sec,
            )
            bots_payload = request_json(f"{base_url}/api/bots", token=token, timeout=args.request_timeout_sec)
            result["startup"] = {
                "healthy": True,
                "guardPid": guard_pid,
                "bridgePid": bridge_pid,
                "botCount": len(bots_payload) if isinstance(bots_payload, list) else None,
                "startLockReleased": start_lock_released,
            }

            recovery_started = time.perf_counter()
            current_bridge_pid = positive_int_file(control_root / ".bridge.pid")
            if current_bridge_pid != bridge_pid:
                raise RuntimeError("bridge PID changed before watchdog recovery test")
            if not signal_owned_process(
                bridge_pid,
                signal.SIGKILL,
                kind="bridge",
                control_root=control_root,
            ):
                raise RuntimeError("isolated bridge exited before watchdog recovery test")
            recovered_pid = wait_for_replacement_pid(
                control_root / ".bridge.pid",
                bridge_pid,
                control_root=control_root,
                timeout_sec=args.recovery_timeout_sec,
            )
            wait_for_health(
                base_url,
                token=token,
                timeout_sec=args.recovery_timeout_sec,
                request_timeout_sec=args.request_timeout_sec,
            )
            restart_state = control_root / ".bridge.watchdog.restarts"
            restart_count = len([line for line in restart_state.read_text(encoding="utf-8").splitlines() if line.strip()])
            result["watchdogRecovery"] = {
                "oldBridgePid": bridge_pid,
                "newBridgePid": recovered_pid,
                "recoveryMs": round((time.perf_counter() - recovery_started) * 1000, 2),
                "restartCount": restart_count,
                "healthy": True,
            }

            samples: list[dict[str, Any]] = []
            monitor_stop = threading.Event()
            monitor = threading.Thread(
                target=monitor_resources,
                kwargs={
                    "bridge_pid": recovered_pid,
                    "shared_root": shared_root,
                    "runtime_root": runtime_root,
                    "stop_event": monitor_stop,
                    "interval_sec": args.sample_interval_sec,
                    "samples": samples,
                },
                daemon=True,
            )
            monitor.start()
            try:
                load_result = run_http_load(
                    f"{base_url}/api/bots",
                    token=token,
                    duration_sec=args.duration_sec,
                    concurrency=args.concurrency,
                    request_timeout_sec=args.request_timeout_sec,
                )
            finally:
                monitor_stop.set()
                monitor.join(timeout=max(5.0, args.sample_interval_sec * 3))
            resource_result = summarize_resources(samples)
            result["load"] = load_result
            result["resources"] = resource_result
            checks = {
                "minimumRequests": int(load_result["requests"]) >= args.min_requests,
                "zeroHttpFailures": int(load_result["failures"]) == 0,
                "p95Latency": float(load_result["latencyMs"]["p95"]) <= args.max_p95_ms,
                "rssLimit": int(resource_result["maxRssKiB"]) <= args.max_rss_mib * 1024,
                "descendantLimit": int(resource_result["maxDescendants"]) <= args.max_descendants,
                "queueDrained": int(resource_result["maxQueueDepth"]) == 0,
                "watchdogRestarted": recovered_pid != bridge_pid and restart_count >= 1,
                "startLockReleased": start_lock_released,
            }
            result["checks"] = checks
            result["pass"] = all(checks.values())
        except Exception as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            current_guard_pid = 0
            try:
                current_guard_pid = positive_int_file(control_root / ".bridge.guard.pid")
            except (OSError, ValueError):
                pass
            cleanup_ok = True
            for candidate in dict.fromkeys((current_guard_pid, guard_pid)):
                if candidate and is_owned_process(candidate, kind="watchdog", control_root=control_root):
                    cleanup_ok = terminate_owned_process(candidate, kind="watchdog", control_root=control_root) and cleanup_ok
            current_bridge_pid = 0
            try:
                current_bridge_pid = positive_int_file(control_root / ".bridge.pid")
            except (OSError, ValueError):
                current_bridge_pid = recovered_pid or bridge_pid
            for candidate in dict.fromkeys((current_bridge_pid, recovered_pid, bridge_pid)):
                if candidate and is_owned_process(candidate, kind="bridge", control_root=control_root):
                    cleanup_ok = terminate_owned_process(candidate, kind="bridge", control_root=control_root) and cleanup_ok
            remaining_owned = [
                {"pid": pid, "kind": kind}
                for pid, kind in (
                    (current_guard_pid, "watchdog"),
                    (guard_pid, "watchdog"),
                    (bridge_pid, "bridge"),
                    (recovered_pid, "bridge"),
                    (current_bridge_pid, "bridge"),
                )
                if pid and is_owned_process(pid, kind=kind, control_root=control_root)
            ]
            result["cleanup"] = {"ok": cleanup_ok and not remaining_owned, "remainingOwnedProcesses": remaining_owned}
            result["pass"] = bool(result.get("pass")) and bool(result["cleanup"]["ok"])
    result["finishedAtMs"] = int(time.time() * 1000)
    return result


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        result = run_probe(args)
    except Exception as exc:
        result = {"pass": False, "error": f"{type(exc).__name__}: {exc}"}
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        output_path = args.output.expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(output_path, rendered + "\n")
    return 0 if result.get("pass") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
