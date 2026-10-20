from __future__ import annotations

import asyncio
import os
import signal
import time
from contextlib import suppress
from pathlib import Path

_PROCESS_SESSION_ID_ATTR = "_bridge_process_session_id"
_PROCESS_START_TIME_ATTR = "_bridge_process_start_time"
_PROCESS_POLL_INTERVAL_SEC = 0.05


def isolated_subprocess_kwargs() -> dict[str, bool]:
    """Return subprocess options that isolate an agent from the bridge process group."""
    if os.name != "posix":
        return {}
    return {"start_new_session": True}


def remember_isolated_process(process) -> None:
    """Record the process session while its leader PID is still authoritative."""
    if os.name != "posix":
        return
    pid = _process_pid(process)
    if pid is None:
        return
    try:
        if os.getsid(pid) != pid:
            return
    except (OSError, ProcessLookupError):
        return
    with suppress(AttributeError, TypeError):
        setattr(process, _PROCESS_SESSION_ID_ATTR, pid)
        setattr(process, _PROCESS_START_TIME_ATTR, _process_start_time(pid))


def _process_pid(process) -> int | None:
    try:
        pid = int(getattr(process, "pid", 0) or 0)
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _process_session_id(process) -> int | None:
    if os.name != "posix":
        return None
    try:
        recorded = int(getattr(process, _PROCESS_SESSION_ID_ATTR, 0) or 0)
    except (TypeError, ValueError):
        recorded = 0
    if recorded > 0:
        recorded_start_time = getattr(process, _PROCESS_START_TIME_ATTR, None)
        current_start_time = _process_start_time(recorded)
        if current_start_time is not None and recorded_start_time not in (None, current_start_time):
            return None
        return recorded
    pid = _process_pid(process)
    if pid is None:
        return None
    try:
        return pid if os.getsid(pid) == pid else None
    except (OSError, ProcessLookupError):
        return None


def _process_start_time(pid: int) -> str | None:
    try:
        stat_text = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    closing_paren = stat_text.rfind(")")
    if closing_paren < 0:
        return None
    fields_after_name = stat_text[closing_paren + 2 :].split()
    return fields_after_name[19] if len(fields_after_name) > 19 else None


def _session_process_ids(session_id: int) -> list[int]:
    proc_root = Path("/proc")
    if os.name != "posix" or not proc_root.is_dir():
        return []
    process_ids: list[int] = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            if os.getsid(pid) == session_id:
                process_ids.append(pid)
        except (OSError, ProcessLookupError, PermissionError):
            continue
    return process_ids


def _signal_process_session(session_id: int, sig: signal.Signals) -> None:
    process_ids = _session_process_ids(session_id)
    if not process_ids:
        with suppress(OSError, ProcessLookupError):
            os.killpg(session_id, sig)
        return
    for pid in process_ids:
        with suppress(OSError, ProcessLookupError, PermissionError):
            os.kill(pid, sig)


async def _wait_for_process_session_exit(session_id: int, timeout_sec: float) -> bool:
    deadline = time.monotonic() + max(0.0, timeout_sec)
    while _session_process_ids(session_id):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(_PROCESS_POLL_INTERVAL_SEC, remaining))
    return True


async def _wait_for_process_handle(process, timeout_sec: float) -> bool:
    wait_method = getattr(process, "wait", None)
    if not callable(wait_method):
        return getattr(process, "returncode", None) is not None or not hasattr(process, "returncode")
    try:
        await asyncio.wait_for(wait_method(), timeout=max(0.0, timeout_sec))
    except asyncio.TimeoutError:
        return False
    except ProcessLookupError:
        return True
    except Exception:
        return False
    return True


async def terminate_process_tree(process, timeout_sec: float = 5.0) -> bool:
    """Terminate an isolated subprocess and every process in its OS session."""
    if process is None:
        return True

    session_id = _process_session_id(process)
    if session_id is not None and _session_process_ids(session_id):
        _signal_process_session(session_id, signal.SIGTERM)
        if not await _wait_for_process_session_exit(session_id, timeout_sec):
            _signal_process_session(session_id, signal.SIGKILL)
            if not await _wait_for_process_session_exit(session_id, timeout_sec):
                return False
        return await _wait_for_process_handle(process, timeout_sec)

    if getattr(process, "returncode", None) is not None:
        return True
    terminate = getattr(process, "terminate", None)
    if callable(terminate):
        try:
            terminate()
        except ProcessLookupError:
            return True
    else:
        kill = getattr(process, "kill", None)
        if not callable(kill):
            return False
        try:
            kill()
        except ProcessLookupError:
            return True
    if await _wait_for_process_handle(process, timeout_sec):
        return True
    kill = getattr(process, "kill", None)
    if callable(kill):
        try:
            kill()
        except ProcessLookupError:
            return True
    return await _wait_for_process_handle(process, timeout_sec)
