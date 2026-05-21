#!/usr/bin/env python3

import asyncio
import base64
import fcntl
import hashlib
import json
import os
import random
import re
import shutil
import signal
import sys
import tempfile
import threading
import time
from collections import deque
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, unquote, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
from aiohttp import WSMsgType, web
from Crypto.Cipher import AES


BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from bridge_runtime_config import build_bridge_api_base, resolve_host_port


def resolve_shared_runtime_root(base_dir: Optional[Path] = None) -> Path:
    root_base_dir = (base_dir or BASE_DIR).resolve()
    raw = str(os.environ.get("BRIDGE_SHARED_RUNTIME_ROOT") or "").strip()
    if not raw:
        return root_base_dir
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (root_base_dir / path).resolve()
    else:
        path = path.resolve()
    return path


def resolve_runtime_root(base_dir: Optional[Path] = None) -> Path:
    root_base_dir = (base_dir or BASE_DIR).resolve()
    raw = str(os.environ.get("BRIDGE_RUNTIME_ROOT") or "").strip()
    if not raw:
        return root_base_dir
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (root_base_dir / path).resolve()
    else:
        path = path.resolve()
    return path


def resolve_local_file_send_queue_root(base_dir: Optional[Path] = None) -> Path:
    runtime_root = resolve_runtime_root(base_dir)
    raw = str(os.environ.get("LOCAL_FILE_SEND_QUEUE_ROOT") or "").strip()
    if not raw:
        return (runtime_root / ".local-file-send-queue").resolve()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (runtime_root / path).resolve()
    else:
        path = path.resolve()
    return path


HOST, PORT = resolve_host_port()
BRIDGE_API_BASE = build_bridge_api_base(HOST, PORT)
DEFAULT_WORK_DIR = os.environ.get("WORK_DIR", "/home/jenkins")
DEFAULT_CODEX_HOME = Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex")).expanduser().resolve()
BRIDGE_TOKEN = os.environ.get("BRIDGE_TOKEN", "").strip()
BRIDGE_BASIC_AUTH = os.environ.get("BRIDGE_BASIC_AUTH", "").strip()
CODEX_EXEC_MODE = (os.environ.get("CODEX_EXEC_MODE", "sandboxed").strip().lower() or "sandboxed")
WECOM_WS = "wss://openws.work.weixin.qq.com"

MAX_JSON_BODY = int(os.environ.get("MAX_JSON_BODY", 1024 * 1024))
MAX_UPLOAD_SIZE = int(os.environ.get("MAX_UPLOAD_SIZE", 100 * 1024 * 1024))
MAX_INBOUND_IMAGE_SIZE = int(os.environ.get("MAX_INBOUND_IMAGE_SIZE", 30 * 1024 * 1024))
MAX_INBOUND_FILE_SIZE = int(os.environ.get("MAX_INBOUND_FILE_SIZE", 100 * 1024 * 1024))
MEDIA_CONNECT_TIMEOUT = int(os.environ.get("MEDIA_CONNECT_TIMEOUT", 8000))
MEDIA_TOTAL_TIMEOUT = int(os.environ.get("MEDIA_TOTAL_TIMEOUT", 20000))
MAX_CONCURRENT_CODEX_RUNS = max(1, int(os.environ.get("MAX_CONCURRENT_CODEX_RUNS", 20)))
SUBPROCESS_STREAM_LIMIT = max(64 * 1024, int(os.environ.get("SUBPROCESS_STREAM_LIMIT", 1024 * 1024)))
SUBPROCESS_STREAM_READ_SIZE = max(4096, int(os.environ.get("SUBPROCESS_STREAM_READ_SIZE", 16 * 1024)))
SUBPROCESS_STREAM_MAX_LINE = max(16 * 1024, int(os.environ.get("SUBPROCESS_STREAM_MAX_LINE", 256 * 1024)))
PROACTIVE_SEND_ACK_TIMEOUT_SEC = max(1, int(os.environ.get("PROACTIVE_SEND_ACK_TIMEOUT_SEC", 10)))
WEBSOCKET_SEND_TIMEOUT_SEC = max(1, int(os.environ.get("WEBSOCKET_SEND_TIMEOUT_SEC", 5)))
STATUS_STREAM_INTERVAL_SEC = max(1, int(os.environ.get("STATUS_STREAM_INTERVAL_SEC", 2)))
STATUS_SEND_TIMEOUT_SEC = max(1, int(os.environ.get("STATUS_SEND_TIMEOUT_SEC", 1)))
STATUS_SEND_LOCK_TIMEOUT_SEC = max(1, int(os.environ.get("STATUS_SEND_LOCK_TIMEOUT_SEC", 1)))
PROACTIVE_TEXT_MAX_CHARS = max(256, int(os.environ.get("PROACTIVE_TEXT_MAX_CHARS", 1800)))
REPLY_IDLE_FALLBACK_SEC = max(60, int(os.environ.get("REPLY_IDLE_FALLBACK_SEC", 240)))
REPLY_MAX_AGE_FALLBACK_SEC = max(60, int(os.environ.get("REPLY_MAX_AGE_FALLBACK_SEC", 540)))
PROACTIVE_STATUS_INTERVAL_SEC = max(30, int(os.environ.get("PROACTIVE_STATUS_INTERVAL_SEC", 120)))
SCHEDULE_PROCESSING_RETRY_MS = max(1000, int(os.environ.get("SCHEDULE_PROCESSING_RETRY_MS", 30000)))
SCHEDULE_ORPHAN_TTL_MS = max(60000, int(os.environ.get("SCHEDULE_ORPHAN_TTL_MS", 86400000)))

SESSION_TTL = 30 * 60
SESSION_LEASE_TTL_MS = int(os.environ.get("SESSION_LEASE_TTL", 30000))
SESSION_LEASE_RENEW_MS = max(3000, SESSION_LEASE_TTL_MS // 3)
RECENT_EVENT_TTL = 10 * 60
MAX_STREAM_CONTENT = 8000

DATA_FILE = Path(os.environ.get("BOTS_FILE", str(BASE_DIR / ".bots.json"))).expanduser().resolve()
SHARED_RUNTIME_ROOT = resolve_shared_runtime_root(BASE_DIR)
INSTANCE_RUNTIME_ROOT = resolve_runtime_root(BASE_DIR)
BOT_TOMBSTONE_ROOT = SHARED_RUNTIME_ROOT / ".bot-tombstones"
BOT_RUNTIME_LOCK_ROOT = SHARED_RUNTIME_ROOT / ".bot-runtime-locks"
SESSION_LOCK_ROOT = SHARED_RUNTIME_ROOT / ".session-locks"
SESSION_REGISTRY_ROOT = SHARED_RUNTIME_ROOT / ".session-registry"
CHATFILE_ROOT = INSTANCE_RUNTIME_ROOT / "chatfile"
WORKSPACE_ROOT = INSTANCE_RUNTIME_ROOT / "workspace"
BRIDGE_CODEX_HOME_ROOT = INSTANCE_RUNTIME_ROOT / ".bridge-codex-home"
BRIDGE_GLOBAL_SKILLS_ROOT = BRIDGE_CODEX_HOME_ROOT / "skills"
PROJECT_SHARED_SKILLS_ROOT = BASE_DIR / "relate-skills"
USER_ALIAS_ROOT = SHARED_RUNTIME_ROOT / ".user-aliases"
LOCAL_FILE_SEND_COMMAND = BASE_DIR / "send_file.py"
LOCAL_SCHEDULE_MESSAGE_COMMAND = BASE_DIR / "schedule_message.py"
LOCAL_FILE_SEND_QUEUE_ROOT = resolve_local_file_send_queue_root()
LOCAL_FILE_SEND_PENDING_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "pending"
LOCAL_FILE_SEND_PROCESSING_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "processing"
LOCAL_FILE_SEND_RESULT_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "results"
LOCAL_FILE_SEND_DONE_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "done"
LOCAL_FILE_SEND_FAILED_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "failed"
LOCAL_FILE_SEND_POLL_MS = int(os.environ.get("LOCAL_FILE_SEND_POLL_MS", 1000))
LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS = max(1000, int(os.environ.get("LOCAL_FILE_SEND_RESULT_TIMEOUT_MS", "120000")))
LOCAL_FILE_SEND_RESULT_RETENTION_MS = max(60000, int(os.environ.get("LOCAL_FILE_SEND_RESULT_RETENTION_MS", str(86400000))))
SCHEDULE_ROOT = SHARED_RUNTIME_ROOT / ".scheduled-messages"
SCHEDULE_PENDING_ROOT = SCHEDULE_ROOT / "pending"
SCHEDULE_PROCESSING_ROOT = SCHEDULE_ROOT / "processing"
SCHEDULE_DONE_ROOT = SCHEDULE_ROOT / "done"
SCHEDULE_FAILED_ROOT = SCHEDULE_ROOT / "failed"
SCHEDULE_DEFINITION_ROOT = SCHEDULE_ROOT / "definitions"
SCHEDULE_DEFINITION_LOCK_ROOT = SCHEDULE_ROOT / "definition-locks"
SCHEDULE_POLL_MS = int(os.environ.get("SCHEDULE_POLL_MS", 1000))
SCHEDULE_DEFINITION_POLL_MS = int(os.environ.get("SCHEDULE_DEFINITION_POLL_MS", 1000))
SCHEDULE_DEFINITION_LEASE_TTL_MS = max(1000, int(os.environ.get("SCHEDULE_DEFINITION_LEASE_TTL_MS", 30000)))
VALID_CODEX_EXEC_MODES = {"sandboxed", "host"}
VALID_GROUP_SESSION_MODES = {"shared", "per-user"}

EXTRA_FILE_ROOTS = [
    Path(item.strip()).resolve()
    for item in os.environ.get("FILE_SEND_ROOTS", "").split(",")
    if item.strip()
]

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\[\?[0-9]*[a-z]")
TEXT_MENTION_RE = re.compile(r"(?<!\S)@\S+(?:\s+|$)")
LEADING_MENTION_RE = re.compile(r"^\s*@\S+(?:\s+|$)")
MENTION_DELIMITER_CHARS = ",:;，。：；"
CRON_MONTH_NAMES = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
CRON_WEEKDAY_NAMES = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}
INSTANCE_ID = f"{os.getpid()}-{random.randint(1000, 9999)}-{int(time.time())}"

BOTS: dict[str, "BotState"] = {}
RECENT_EVENTS: dict[str, float] = {}
HTTP_SESSION: Optional[aiohttp.ClientSession] = None
SHUTDOWN_EVENT = asyncio.Event()
LOCAL_FILE_SEND_QUEUE_BUSY = False
CODEX_RUN_SEMAPHORE: Optional[asyncio.Semaphore] = None
SCHEDULE_DEFINITION_LOCK_HANDLES: dict[str, dict[str, Any]] = {}
PREPARED_PREVIOUS_BOT_CONFIGS: dict[str, dict[str, Any]] = {}
REPORTED_INVALID_PERSISTED_BOT_CONFIGS: set[str] = set()


class BridgeError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass
class SessionState:
    session_id: str
    work_dir: str
    lock_file: Path
    running: bool = False
    run_task: Optional[asyncio.Task] = None
    proc: Optional[asyncio.subprocess.Process] = None
    queue: list[dict[str, Any]] = field(default_factory=list)
    last_active: float = field(default_factory=time.time)
    chat: list[dict[str, Any]] = field(default_factory=list)
    pending_media: list[dict[str, Any]] = field(default_factory=list)
    pending_media_notes: list[str] = field(default_factory=list)
    pending_media_downloads: int = 0
    active_run_media: list[dict[str, Any]] = field(default_factory=list)
    active_run_media_notes: list[str] = field(default_factory=list)
    lease_owned: bool = False
    thread_id: Optional[str] = None
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    reply_started_at: dict[str, float] = field(default_factory=dict)
    reply_last_sent_at: dict[str, float] = field(default_factory=dict)
    reply_proactive_req_ids: set[str] = field(default_factory=set)
    proactive_status_sent_at: dict[str, float] = field(default_factory=dict)
    reply_mentions_sent: set[str] = field(default_factory=set)
    pending_stream_payload: Optional[dict[str, Any]] = None
    pending_final_payload: Optional[dict[str, Any]] = None
    run_generation: int = 0
    codex_runtime_status: Optional[str] = None
    active_scheduled_job_file: Optional[str] = None
    active_schedule_request_id: Optional[str] = None
    interrupt_requested: bool = False
    active_schedule_id: Optional[str] = None
    resume_candidates: list[dict[str, Any]] = field(default_factory=list)
    resume_selection_expires_at: int = 0


@dataclass
class BotState:
    config: dict[str, Any]
    status: str = "starting"
    started_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    logs: deque[str] = field(default_factory=lambda: deque(maxlen=200))
    sessions: dict[str, SessionState] = field(default_factory=dict)
    ws: Optional[aiohttp.ClientWebSocketResponse] = None
    runner_task: Optional[asyncio.Task] = None
    heartbeat_task: Optional[asyncio.Task] = None
    reader_task: Optional[asyncio.Task] = None
    upload_worker_task: Optional[asyncio.Task] = None
    upload_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    pending_requests: dict[str, asyncio.Future] = field(default_factory=dict)
    reply_sessions: dict[str, SessionState] = field(default_factory=dict)
    active_local_file_request_ids: set[str] = field(default_factory=set)
    cancelled_local_file_request_ids: set[str] = field(default_factory=set)
    active_upload_task: Optional[asyncio.Task] = None
    active_upload_job: Optional[dict[str, Any]] = None
    runtime_lock_handle: Any = None


def uid() -> str:
    return f"{int(time.time() * 1000):x}{random.randint(0, 0xFFFFFF):06x}"


def now_ms() -> int:
    return int(time.time() * 1000)


def codex_runs_in_sandbox() -> bool:
    return CODEX_EXEC_MODE == "sandboxed"


def build_codex_base_args(output_file: Path, image_paths: list[str], resume: bool) -> list[str]:
    args = ["codex", "exec"]
    if resume:
        args.append("resume")
    args.extend(["--skip-git-repo-check", "--json", "-o", str(output_file)])
    if codex_runs_in_sandbox():
        args.append("--full-auto")
    else:
        args.append("--dangerously-bypass-approvals-and-sandbox")
    for image_path in image_paths:
        args.extend(["-i", image_path])
    return args


async def write_process_prompt(process: asyncio.subprocess.Process, prompt: str) -> None:
    if process.stdin is None:
        raise BridgeError(500, "codex stdin is not available")
    try:
        process.stdin.write(prompt.encode("utf-8"))
        await process.stdin.drain()
    finally:
        process.stdin.close()
        wait_closed = getattr(process.stdin, "wait_closed", None)
        if wait_closed is not None:
            try:
                await wait_closed()
            except Exception:
                pass


def get_codex_run_semaphore() -> asyncio.Semaphore:
    if CODEX_RUN_SEMAPHORE is None:
        raise BridgeError(500, "codex run semaphore not ready")
    return CODEX_RUN_SEMAPHORE


async def iter_subprocess_lines(
    stream: asyncio.StreamReader,
    bot: BotState,
    key: str,
    stream_name: str,
) -> Any:
    buffer = bytearray()
    dropping = False
    while True:
        chunk = await stream.read(SUBPROCESS_STREAM_READ_SIZE)
        if not chunk:
            if buffer and not dropping:
                line = bytes(buffer)
                if line.endswith(b"\r"):
                    line = line[:-1]
                yield line.decode("utf-8", "ignore")
            elif dropping:
                add_log(bot, f"[{key}] codex {stream_name}: dropped oversized line at EOF")
            return

        if dropping:
            newline_idx = chunk.find(b"\n")
            if newline_idx == -1:
                continue
            dropping = False
            chunk = chunk[newline_idx + 1 :]
            if not chunk:
                continue

        buffer.extend(chunk)
        while True:
            newline_idx = buffer.find(b"\n")
            if newline_idx != -1:
                raw = bytes(buffer[:newline_idx])
                del buffer[: newline_idx + 1]
                if raw.endswith(b"\r"):
                    raw = raw[:-1]
                yield raw.decode("utf-8", "ignore")
                continue
            if len(buffer) > SUBPROCESS_STREAM_MAX_LINE:
                add_log(bot, f"[{key}] codex {stream_name}: dropped oversized line (> {SUBPROCESS_STREAM_MAX_LINE} bytes)")
                buffer.clear()
                dropping = True
            break


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def ensure_dir_for(path: Path) -> None:
    ensure_dir(path.parent)


def local_file_send_namespace(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    return quote(text, safe="._-")


def build_local_file_send_queue_paths(root: Path) -> dict[str, Path]:
    return {
        "root": root,
        "pending": root / "pending",
        "processing": root / "processing",
        "results": root / "results",
        "done": root / "done",
        "failed": root / "failed",
    }


def get_local_file_send_queue_paths(target_config_id: Optional[str] = None) -> dict[str, Path]:
    namespace = local_file_send_namespace(target_config_id)
    if namespace:
        return build_local_file_send_queue_paths(LOCAL_FILE_SEND_QUEUE_ROOT / "targets" / namespace)
    return build_local_file_send_queue_paths(LOCAL_FILE_SEND_QUEUE_ROOT)


def get_local_file_send_target_config_id_for_root(root: Path) -> Optional[str]:
    resolved = root.resolve()
    default_root = LOCAL_FILE_SEND_QUEUE_ROOT.resolve()
    if resolved == default_root:
        return None
    targets_root = (LOCAL_FILE_SEND_QUEUE_ROOT / "targets").resolve()
    if resolved.parent != targets_root:
        return None
    return unquote(resolved.name)


def get_local_file_send_queue_paths_for_job_file(job_file: Path) -> dict[str, Path]:
    resolved = job_file.resolve()
    queue_dir = resolved.parent
    if queue_dir.name not in {"pending", "processing", "results", "done", "failed"}:
        return get_local_file_send_queue_paths()
    return build_local_file_send_queue_paths(queue_dir.parent)


def get_local_file_send_queue_paths_for_request(data: dict[str, Any]) -> dict[str, Path]:
    target_config_id = str(data.get("targetConfigId") or data.get("target_config_id") or "").strip() or None
    return get_local_file_send_queue_paths(target_config_id)


def should_process_local_file_send_target_config(target_config_id: Optional[str]) -> bool:
    if not target_config_id:
        return True
    bot = BOTS.get(target_config_id)
    if bot:
        if bot_instance_is_stale(bot):
            return False
        if bot.config.get("enabled", True) is False:
            return True
        if bot.runtime_lock_handle is not None:
            return True
        wecom_bot_id = str(bot.config.get("botId") or "").strip()
        if wecom_bot_id and bot_runtime_lock_held_elsewhere(wecom_bot_id):
            return False
        return True
    persisted = read_persisted_bot_config(target_config_id)
    if persisted is None:
        return True
    if persisted.get("enabled", True) is False:
        return True
    wecom_bot_id = str(persisted.get("botId") or "").strip()
    if not wecom_bot_id:
        return True
    return not bot_runtime_lock_held_elsewhere(wecom_bot_id)


def list_local_file_send_queue_path_groups() -> list[dict[str, Path]]:
    queue_path_groups = [get_local_file_send_queue_paths()]
    seen_roots = {str(queue_path_groups[0]["root"])}
    targets_root = LOCAL_FILE_SEND_QUEUE_ROOT / "targets"
    if targets_root.exists():
        for root in sorted(targets_root.iterdir()):
            if not root.is_dir():
                continue
            root_str = str(root.resolve())
            if root_str in seen_roots:
                continue
            target_config_id = get_local_file_send_target_config_id_for_root(root)
            if not should_process_local_file_send_target_config(target_config_id):
                continue
            queue_path_groups.append(build_local_file_send_queue_paths(root.resolve()))
            seen_roots.add(root_str)
    for bot_id in sorted(BOTS.keys()):
        if not should_process_local_file_send_target_config(bot_id):
            continue
        target_paths = get_local_file_send_queue_paths(bot_id)
        target_root = str(target_paths["root"])
        if target_root in seen_roots:
            continue
        queue_path_groups.append(target_paths)
        seen_roots.add(target_root)
    return queue_path_groups


def list_all_local_file_send_queue_path_groups() -> list[dict[str, Path]]:
    queue_path_groups = [get_local_file_send_queue_paths()]
    seen_roots = {str(queue_path_groups[0]["root"])}
    targets_root = LOCAL_FILE_SEND_QUEUE_ROOT / "targets"
    if targets_root.exists():
        for root in sorted(targets_root.iterdir()):
            if not root.is_dir():
                continue
            root_str = str(root.resolve())
            if root_str in seen_roots:
                continue
            queue_path_groups.append(build_local_file_send_queue_paths(root.resolve()))
            seen_roots.add(root_str)
    return queue_path_groups


def local_file_send_request_expires_at_ms(request: dict[str, Any]) -> Optional[int]:
    expires_at = normalize_optional_int(request.get("expiresAt") or request.get("expires_at"))
    if expires_at is not None:
        return expires_at
    requested_at = normalize_optional_int(request.get("requestedAt") or request.get("requested_at"))
    if requested_at is None:
        return None
    timeout_ms = normalize_optional_int(request.get("timeoutMs") or request.get("timeout_ms"))
    return requested_at + max(1000, timeout_ms if timeout_ms is not None else LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS)


def ensure_local_file_send_request_deadline(request: dict[str, Any], job_file: Path, current_ms: Optional[int] = None) -> tuple[dict[str, Any], bool]:
    now_value = current_ms if current_ms is not None else now_ms()
    changed = False
    normalized = dict(request)

    requested_at = normalize_optional_int(normalized.get("requestedAt") or normalized.get("requested_at"))
    if requested_at is None:
        try:
            requested_at = int(job_file.stat().st_mtime * 1000)
        except FileNotFoundError:
            requested_at = now_value
        normalized["requestedAt"] = requested_at
        changed = True

    timeout_ms = normalize_optional_int(normalized.get("timeoutMs") or normalized.get("timeout_ms"))
    if timeout_ms is None:
        timeout_ms = LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS
        normalized["timeoutMs"] = timeout_ms
        changed = True

    expires_at = normalize_optional_int(normalized.get("expiresAt") or normalized.get("expires_at"))
    if expires_at is None:
        normalized["expiresAt"] = requested_at + max(1000, timeout_ms)
        changed = True

    return normalized, changed


def local_file_send_request_is_expired(request: dict[str, Any], current_ms: Optional[int] = None) -> bool:
    expires_at = local_file_send_request_expires_at_ms(request)
    if expires_at is None:
        return False
    return (current_ms if current_ms is not None else now_ms()) >= expires_at


def local_file_send_request_delivery_state(request: dict[str, Any]) -> str:
    return str(request.get("deliveryState") or request.get("delivery_state") or "").strip()


def local_file_send_request_delivery_started(request: dict[str, Any]) -> bool:
    return local_file_send_request_delivery_state(request) in {"uploading", "sent"}


def local_file_send_request_delivery_finished(request: dict[str, Any]) -> bool:
    return local_file_send_request_delivery_state(request) == "sent"


def local_file_send_request_delivery_is_ambiguous(request: dict[str, Any]) -> bool:
    return local_file_send_request_delivery_state(request) in {"sending", "sent"}


def local_file_send_request_has_active_delivery(request_id: str) -> bool:
    if not request_id:
        return False
    return any(request_id in bot.active_local_file_request_ids for bot in BOTS.values())


def local_file_send_request_timeout_payload(request: dict[str, Any], current_ms: Optional[int] = None) -> dict[str, Any]:
    expires_at = local_file_send_request_expires_at_ms(request)
    if expires_at is None:
        expires_at = current_ms if current_ms is not None else now_ms()
    return {
        "ok": False,
        "statusCode": 504,
        "error": f"local file-send request expired before delivery: {expires_at}",
    }


def local_file_send_request_ambiguous_payload() -> dict[str, Any]:
    return {
        "ok": False,
        "statusCode": 409,
        "error": "local file-send outcome unknown after bridge interruption; not retried",
    }


def local_file_send_request_delivery_timeout_payload(request: dict[str, Any], current_ms: Optional[int] = None) -> dict[str, Any]:
    expires_at = local_file_send_request_expires_at_ms(request)
    if expires_at is None:
        expires_at = current_ms if current_ms is not None else now_ms()
    return {
        "ok": False,
        "statusCode": 504,
        "error": f"local file-send request expired during delivery; outcome unknown: {expires_at}",
    }


def mark_local_file_send_processing_started(
    processing_file: Path,
    request: dict[str, Any],
    *,
    bot_id: str,
    chat_key: str,
) -> None:
    write_json_atomic(
        processing_file,
        {
            **request,
            "resolvedBotId": bot_id,
            "resolvedChatKey": chat_key,
            "deliveryState": "uploading",
            "deliveryStartedAt": now_ms(),
        },
    )


def mark_local_file_send_processing_sent(
    processing_file: Path,
    request: dict[str, Any],
    *,
    bot_id: str,
    chat_key: str,
) -> None:
    write_json_atomic(
        processing_file,
        {
            **request,
            "resolvedBotId": bot_id,
            "resolvedChatKey": chat_key,
            "deliveryState": "sent",
            "deliveryFinishedAt": now_ms(),
        },
    )


def mark_local_file_send_processing_sending(
    processing_file: Path,
    request: dict[str, Any],
    *,
    bot_id: str,
    chat_key: str,
) -> None:
    write_json_atomic(
        processing_file,
        {
            **request,
            "resolvedBotId": bot_id,
            "resolvedChatKey": chat_key,
            "deliveryState": "sending",
            "deliveryDispatchAt": now_ms(),
        },
    )


def local_file_send_request_retain_until_ms(request: dict[str, Any]) -> Optional[int]:
    retain_until = normalize_optional_int(request.get("retainUntil") or request.get("retain_until"))
    if retain_until is not None:
        return retain_until
    expires_at = local_file_send_request_expires_at_ms(request)
    if expires_at is None:
        return None
    return expires_at + LOCAL_FILE_SEND_RESULT_RETENTION_MS


def read_local_file_send_result(request_id: str, queue_paths: dict[str, Path]) -> Optional[dict[str, Any]]:
    result_file = queue_paths["results"] / f"{request_id}.json"
    payload = read_json_file(result_file, None)
    return payload if isinstance(payload, dict) else None


def cleanup_stale_local_file_send_result_files(current_ms: Optional[int] = None) -> None:
    now_value = current_ms if current_ms is not None else now_ms()
    for paths in list_all_local_file_send_queue_path_groups():
        result_root = paths["results"]
        if not result_root.exists():
            continue
        for result_file in result_root.glob("*.json"):
            payload = read_json_file(result_file, None)
            processed_at = normalize_optional_int((payload or {}).get("processedAt")) if isinstance(payload, dict) else None
            retain_until = normalize_optional_int((payload or {}).get("retainUntil") or (payload or {}).get("retain_until")) if isinstance(payload, dict) else None
            if processed_at is None:
                try:
                    processed_at = int(result_file.stat().st_mtime * 1000)
                except FileNotFoundError:
                    continue
            result_expires_at = max(processed_at + LOCAL_FILE_SEND_RESULT_RETENTION_MS, retain_until or 0)
            if now_value < result_expires_at:
                continue
            try:
                result_file.unlink()
            except FileNotFoundError:
                pass


def read_json_file(path: Path, fallback: Any = None) -> Any:
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return fallback


def write_json_atomic(path: Path, data: Any) -> None:
    ensure_dir_for(path)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
    tmp.replace(path)


def ensure_local_file_send_dirs(target_config_ids: Optional[list[str]] = None) -> None:
    path_groups = [get_local_file_send_queue_paths()]
    seen_roots = {str(path_groups[0]["root"])}
    for target_config_id in target_config_ids or []:
        target_paths = get_local_file_send_queue_paths(target_config_id)
        target_root = str(target_paths["root"])
        if target_root in seen_roots:
            continue
        path_groups.append(target_paths)
        seen_roots.add(target_root)
    for paths in path_groups:
        for item in paths.values():
            ensure_dir(item)


def ensure_schedule_dirs() -> None:
    for item in (
        SCHEDULE_ROOT,
        SCHEDULE_PENDING_ROOT,
        SCHEDULE_PROCESSING_ROOT,
        SCHEDULE_DONE_ROOT,
        SCHEDULE_FAILED_ROOT,
        SCHEDULE_DEFINITION_ROOT,
        SCHEDULE_DEFINITION_LOCK_ROOT,
    ):
        ensure_dir(item)


def legacy_shared_runtime_root() -> Path:
    return BASE_DIR.resolve()


def legacy_instance_runtime_root() -> Path:
    return BASE_DIR.resolve()


def get_shared_runtime_migration_marker() -> Path:
    return SHARED_RUNTIME_ROOT / ".legacy-shared-runtime-migrated.json"


def get_instance_runtime_migration_marker() -> Path:
    return INSTANCE_RUNTIME_ROOT / ".legacy-instance-runtime-migrated.json"


def maybe_migrate_legacy_shared_runtime_state() -> None:
    if SHARED_RUNTIME_ROOT == legacy_shared_runtime_root() or get_shared_runtime_migration_marker().exists():
        return
    legacy_root = legacy_shared_runtime_root()
    migrations = (
        (legacy_root / ".bot-tombstones", BOT_TOMBSTONE_ROOT),
        (legacy_root / ".bot-runtime-locks", BOT_RUNTIME_LOCK_ROOT),
        (legacy_root / ".session-locks", SESSION_LOCK_ROOT),
        (legacy_root / ".session-registry", SESSION_REGISTRY_ROOT),
        (legacy_root / ".scheduled-messages", SCHEDULE_ROOT),
        (legacy_root / ".user-aliases", USER_ALIAS_ROOT),
    )
    for source, target in migrations:
        if not source.exists():
            continue
        ensure_dir(target)
        copy_missing_tree_contents(source, target)
    write_json_atomic(
        get_shared_runtime_migration_marker(),
        {"source": str(legacy_root), "migratedAt": now_ms()},
    )


def maybe_migrate_legacy_instance_runtime_state() -> None:
    if INSTANCE_RUNTIME_ROOT == legacy_instance_runtime_root() or get_instance_runtime_migration_marker().exists():
        return
    legacy_root = legacy_instance_runtime_root()
    migrations = (
        (legacy_root / "workspace", WORKSPACE_ROOT),
        (legacy_root / ".bridge-codex-home", BRIDGE_CODEX_HOME_ROOT),
        (legacy_root / "chatfile", CHATFILE_ROOT),
    )
    for source, target in migrations:
        if not source.exists():
            continue
        ensure_dir(target)
        copy_missing_tree_contents(source, target)
    write_json_atomic(
        get_instance_runtime_migration_marker(),
        {"source": str(legacy_root), "migratedAt": now_ms()},
    )


def remove_session_codex_home(session_id: str) -> None:
    remove_tree_if_exists(get_session_codex_home_root(session_id))


def get_registry_key_file(bot_id: str, key: str) -> Path:
    return SESSION_REGISTRY_ROOT / "keys" / bot_id / f"{quote(key, safe='')}.json"


def get_registry_key_lock_file(bot_id: str, key: str) -> Path:
    return SESSION_REGISTRY_ROOT / "keys" / bot_id / f"{quote(key, safe='')}.lock"


def get_registry_session_file(session_id: str) -> Path:
    return SESSION_REGISTRY_ROOT / "sessions" / f"{session_id}.json"


def get_managed_session_lock_file(bot_id: str, session_id: str) -> Path:
    return SESSION_LOCK_ROOT / bot_id / f"{session_id}.lock.json"


def workspace_slug(value: Any, fallback: str = "unknown") -> str:
    slug = re.sub(r"[^\w.-]+", "_", str(value or "").strip())
    return slug or fallback


def chat_key_to_user_id(key: str) -> Optional[str]:
    text = str(key or "").strip()
    if text.startswith("single:"):
        return text.split(":", 1)[1] or None
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        return parts[2] if len(parts) == 3 and parts[2] else None
    return None


def chat_key_to_room_id(key: str) -> Optional[str]:
    text = str(key or "").strip()
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        return parts[1] if len(parts) >= 2 and parts[1] else None
    if text.startswith("group:"):
        return text.split(":", 1)[1] or None
    return None


def chat_key_to_workspace_slug(key: str) -> str:
    text = str(key or "").strip()
    if text.startswith("single:"):
        return f"single_{workspace_slug(text.split(':', 1)[1])}"
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        room_id = parts[1] if len(parts) >= 2 else ""
        user_id = parts[2] if len(parts) >= 3 else ""
        return f"group_user_{workspace_slug(room_id)}_{workspace_slug(user_id)}"
    if text.startswith("group:"):
        return f"group_{workspace_slug(text.split(':', 1)[1])}"
    return workspace_slug(text)


def get_bot_workspace_dir(bot_id: str) -> Path:
    return WORKSPACE_ROOT / workspace_slug(bot_id)


def get_exact_session_workspace_dir(bot_id: str, key: str) -> Path:
    return get_bot_workspace_dir(bot_id) / "sessions" / chat_key_to_workspace_slug(key)


def build_chat_key_with_user_id(key: str, user_id: str) -> str:
    text = str(key or "").strip()
    normalized_user_id = str(user_id or "").strip()
    if not text or not normalized_user_id:
        return text
    if text.startswith("single:"):
        return f"single:{normalized_user_id}"
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        if len(parts) == 3 and parts[1]:
            return f"group-user:{parts[1]}:{normalized_user_id}"
    return text


def alias_chat_key(bot_id: str, key: str) -> Optional[str]:
    raw_user_id = chat_key_to_user_id(key)
    if not raw_user_id:
        return None
    alias = read_user_alias(bot_id, raw_user_id)
    if not alias or alias == raw_user_id:
        return None
    return build_chat_key_with_user_id(key, alias)


def candidate_chat_keys_for_lookup(bot_id: str, key: str) -> list[str]:
    candidates = [key]
    alias_key = alias_chat_key(bot_id, key)
    if alias_key and alias_key not in candidates:
        candidates.append(alias_key)
    return candidates


def resolve_session_storage_key(bot_id: str, key: str) -> str:
    exact_dir = get_exact_session_workspace_dir(bot_id, key)
    if exact_dir.exists() or get_registry_key_file(bot_id, key).exists():
        return key
    alias_key = alias_chat_key(bot_id, key)
    if alias_key:
        alias_dir = get_exact_session_workspace_dir(bot_id, alias_key)
        if alias_dir.exists() or get_registry_key_file(bot_id, alias_key).exists():
            return alias_key
    return key


def get_session_workspace_dir(bot_id: str, key: str) -> Path:
    return get_exact_session_workspace_dir(bot_id, resolve_session_storage_key(bot_id, key))


def get_chatfile_dir(bot_id: str, key: str) -> Path:
    return get_session_workspace_dir(bot_id, key) / "chatfile"


def get_user_workspace_dir(bot_id: str, user_id: str) -> Path:
    return get_bot_workspace_dir(bot_id) / "users" / workspace_slug(user_id)


def get_workfile_dir(bot_id: str, user_id: str) -> Path:
    return get_user_workspace_dir(bot_id, user_id) / "workfile"


def get_room_workspace_dir(bot_id: str, room_id: str) -> Path:
    return get_bot_workspace_dir(bot_id) / "rooms" / workspace_slug(room_id)


def get_roomfile_dir(bot_id: str, room_id: str) -> Path:
    return get_room_workspace_dir(bot_id, room_id) / "roomfile"


def get_session_workspace_paths(bot: BotState, key: str) -> dict[str, Optional[Path]]:
    bot_id = str(bot.config["id"])
    user_id = resolve_workspace_user_id(bot_id, chat_key_to_user_id(key))
    room_id = chat_key_to_room_id(key)
    work_dir = Path(str(bot.config.get("workDir") or DEFAULT_WORK_DIR)).expanduser().resolve()
    return {
        "workDir": work_dir,
        "chatfile": get_chatfile_dir(bot_id, key),
        "workfile": get_workfile_dir(bot_id, user_id) if user_id else None,
        "roomfile": get_roomfile_dir(bot_id, room_id) if room_id else None,
    }


def get_session_runtime_cwd(bot: BotState, key: str) -> Path:
    paths = get_session_workspace_paths(bot, key)
    if paths["workfile"] is not None:
        return paths["workfile"].resolve()
    if paths["roomfile"] is not None:
        return paths["roomfile"].resolve()
    return paths["workDir"].resolve()


def path_contains(candidate: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(candidate.resolve())
        return True
    except Exception:
        return False


def workspace_bootstrap_skip_paths(target_root: Path) -> tuple[Path, ...]:
    return (
        target_root,
        DATA_FILE,
        BOT_TOMBSTONE_ROOT,
        BOT_RUNTIME_LOCK_ROOT,
        SESSION_LOCK_ROOT,
        SESSION_REGISTRY_ROOT,
        CHATFILE_ROOT,
        WORKSPACE_ROOT,
        BRIDGE_CODEX_HOME_ROOT,
        SCHEDULE_ROOT,
        USER_ALIAS_ROOT,
        LOCAL_FILE_SEND_QUEUE_ROOT,
        BASE_DIR / ".bridge.pid",
        BASE_DIR / ".bridge.guard.pid",
        BASE_DIR / ".bridge.watchdog.restarts",
        BASE_DIR / "bridge.log",
        BASE_DIR / "bridge.log.prev",
    )


def should_skip_workspace_bootstrap_path(path: Path, target_root: Path) -> bool:
    if path.name in {".git", "__pycache__", ".pytest_cache"}:
        return True
    for skip_path in workspace_bootstrap_skip_paths(target_root):
        if path_contains(path, skip_path):
            return True
    return False


def is_likely_workspace_source_root(source_root: Path, target_root: Path) -> bool:
    marker_files = {
        "README",
        "README.md",
        "README.en.md",
        "Makefile",
        "pyproject.toml",
        "setup.py",
        "setup.cfg",
        "requirements.txt",
        "package.json",
        "go.mod",
        "Cargo.toml",
        "pom.xml",
        "CMakeLists.txt",
    }
    marker_dirs = {"src", "lib", "app", "cmd", "pkg", "tests", "test", "docs", ".codex"}
    marker_suffixes = {
        ".py",
        ".js",
        ".jsx",
        ".ts",
        ".tsx",
        ".go",
        ".rs",
        ".java",
        ".kt",
        ".c",
        ".cc",
        ".cpp",
        ".h",
        ".hpp",
        ".rb",
        ".php",
        ".sh",
        ".json",
        ".yaml",
        ".yml",
        ".toml",
        ".ini",
        ".md",
    }
    if (source_root / ".git").exists():
        return True
    for child in source_root.iterdir():
        if should_skip_workspace_bootstrap_path(child, target_root):
            continue
        if child.name in marker_files:
            return True
        if child.is_dir() and child.name in marker_dirs:
            return True
        if child.is_file() and child.suffix.lower() in marker_suffixes:
            return True
    return False


def copy_missing_tree_contents_filtered(source: Path, target: Path, *, target_root: Path) -> None:
    ensure_dir(target)
    for child in sorted(source.iterdir(), key=lambda item: item.name):
        if should_skip_workspace_bootstrap_path(child, target_root):
            continue
        destination = target / child.name
        try:
            if child.is_symlink():
                if destination.exists():
                    continue
                ensure_dir_for(destination)
                os.symlink(os.readlink(child), destination)
                continue
            if child.is_dir():
                copy_missing_tree_contents_filtered(child, destination, target_root=target_root)
                continue
            if not child.is_file() or destination.exists():
                continue
            ensure_dir_for(destination)
            shutil.copy2(child, destination, follow_symlinks=False)
        except OSError:
            continue


def bootstrap_workspace_from_workdir(source_root: Path, target_root: Optional[Path]) -> None:
    if target_root is None:
        return
    if not source_root.exists() or not source_root.is_dir():
        return
    marker_file = target_root / ".workspace-bootstrap.json"
    if marker_file.exists():
        return
    source_root_resolved = source_root.resolve()
    if source_root_resolved == Path(DEFAULT_WORK_DIR).expanduser().resolve():
        return
    if source_root_resolved in {Path("/tmp").resolve(), Path("/var/tmp").resolve(), Path(tempfile.gettempdir()).resolve()}:
        return
    if not is_likely_workspace_source_root(source_root, target_root):
        return
    ensure_dir(target_root)
    copy_missing_tree_contents_filtered(source_root, target_root, target_root=target_root)
    write_json_atomic(
        marker_file,
        {
            "source": str(source_root_resolved),
            "bootstrappedAt": now_ms(),
        },
    )


def ensure_session_workspace_dirs(bot: BotState, key: str) -> dict[str, Optional[Path]]:
    paths = get_session_workspace_paths(bot, key)
    for path in (paths["chatfile"], paths["workfile"], paths["roomfile"]):
        if path is not None:
            ensure_dir(path)
    for path in (paths["workfile"], paths["roomfile"]):
        bootstrap_workspace_from_workdir(paths["workDir"], path)
    ensure_dir(get_workspace_codex_skills_dir(get_session_runtime_cwd(bot, key)))
    return paths


def get_workspace_codex_skills_dir(workspace_dir: Path) -> Path:
    return (workspace_dir / ".codex" / "skills").resolve()


def get_bridge_codex_home_root() -> Path:
    return BRIDGE_CODEX_HOME_ROOT.resolve()


def get_bridge_global_skills_root() -> Path:
    return BRIDGE_GLOBAL_SKILLS_ROOT.resolve()


def get_session_codex_home_root(session_id: str) -> Path:
    return (get_bridge_codex_home_root() / "sessions" / workspace_slug(session_id)).resolve()


def get_session_global_skills_root(session_id: str) -> Path:
    return (get_session_codex_home_root(session_id) / "skills").resolve()


def project_shared_skills_root() -> Path:
    return PROJECT_SHARED_SKILLS_ROOT.resolve()


def get_user_alias_dir(bot_id: str) -> Path:
    return USER_ALIAS_ROOT / workspace_slug(bot_id)


def get_user_alias_file(bot_id: str, raw_user_id: str) -> Path:
    return get_user_alias_dir(bot_id) / f"{quote(raw_user_id, safe='')}.json"


def read_user_alias(bot_id: str, raw_user_id: Optional[str]) -> Optional[str]:
    text = str(raw_user_id or "").strip()
    if not text:
        return None
    payload = read_json_file(get_user_alias_file(bot_id, text), None)
    if not isinstance(payload, dict):
        return None
    alias = str(payload.get("alias") or "").strip()
    return alias or None


def write_user_alias(bot_id: str, raw_user_id: str, alias: str) -> None:
    normalized_raw = str(raw_user_id or "").strip()
    normalized_alias = str(alias or "").strip()
    if not normalized_raw or not normalized_alias:
        return
    write_json_atomic(
        get_user_alias_file(bot_id, normalized_raw),
        {"rawUserId": normalized_raw, "alias": normalized_alias, "updatedAt": now_ms()},
    )


def should_prefer_user_alias(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text.startswith(("wo", "wm", "wp")) and len(text) >= 16:
        return True
    return False


def resolve_workspace_user_id(bot_id: str, raw_user_id: Optional[str]) -> Optional[str]:
    text = str(raw_user_id or "").strip()
    if not text:
        return None
    alias = read_user_alias(bot_id, text)
    if alias:
        return alias
    return text


def candidate_user_aliases(bot_id: str, raw_user_id: str) -> list[str]:
    candidates: list[str] = []
    historical = resolve_workspace_user_id(bot_id, raw_user_id)
    if historical:
        candidates.append(historical)
    text = str(raw_user_id or "").strip()
    if text and text not in candidates:
        candidates.append(text)
    return candidates


def remember_workspace_user_alias(bot_id: str, raw_user_id: Optional[str]) -> Optional[str]:
    text = str(raw_user_id or "").strip()
    if not text:
        return None
    alias = read_user_alias(bot_id, text)
    if alias:
        return alias
    if should_prefer_user_alias(text):
        return text
    write_user_alias(bot_id, text, text)
    return text


def get_bot_tombstone_file(wecom_bot_id: str) -> Path:
    return BOT_TOMBSTONE_ROOT / f"{quote(wecom_bot_id, safe='')}.json"


def get_bot_runtime_lock_file(wecom_bot_id: str) -> Path:
    return BOT_RUNTIME_LOCK_ROOT / f"{quote(wecom_bot_id, safe='')}.lock"


def get_persisted_bot_configs_lock_file() -> Path:
    return DATA_FILE.with_name(f"{DATA_FILE.name}.lock")


@contextmanager
def persisted_bot_configs_lock() -> Any:
    lock_file = get_persisted_bot_configs_lock_file()
    ensure_dir_for(lock_file)
    with open(lock_file, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def normalize_optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def bot_deleted_globally(wecom_bot_id: str) -> bool:
    return get_bot_tombstone_file(wecom_bot_id).exists()


def read_bot_tombstone(wecom_bot_id: str) -> Optional[dict[str, Any]]:
    payload = read_json_file(get_bot_tombstone_file(wecom_bot_id), None)
    return payload if isinstance(payload, dict) else None


def bot_tombstone_deleted_at(wecom_bot_id: str) -> int:
    tombstone = read_bot_tombstone(wecom_bot_id)
    if not tombstone:
        return 0
    try:
        return int(tombstone.get("deletedAt") or 0)
    except (TypeError, ValueError):
        return 0


def bot_instance_is_stale(bot: BotState) -> bool:
    deleted_at = bot_tombstone_deleted_at(str(bot.config.get("botId") or ""))
    return bool(deleted_at and bot.started_at_ms <= deleted_at)


def mark_bot_deleted_globally(bot_id: str, wecom_bot_id: Optional[str] = None) -> None:
    logical_bot_id = str(wecom_bot_id or "").strip() or bot_id
    write_json_atomic(
        get_bot_tombstone_file(logical_bot_id),
        {
            "botId": logical_bot_id,
            "configId": bot_id,
            "wecomBotId": logical_bot_id,
            "deletedAt": now_ms(),
            "instanceId": INSTANCE_ID,
        },
    )


def normalize_session_record(record: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not isinstance(record, dict):
        return None
    try:
        session_id = str(record["sessionId"]).strip()
        bot_id = str(record["botId"]).strip()
        chat_key = str(record["chatKey"]).strip()
        lock_file = str(record["lockFile"]).strip()
        created_at = int(record["createdAt"])
        updated_at = int(record.get("updatedAt") or created_at)
    except (KeyError, TypeError, ValueError):
        return None
    if not session_id or not bot_id or not chat_key or not lock_file:
        return None
    return {
        "sessionId": session_id,
        "botId": bot_id,
        "chatKey": chat_key,
        "threadId": str(record.get("threadId") or "").strip() or None,
        "lockFile": lock_file,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "lastRunAt": normalize_optional_int(record.get("lastRunAt")),
        "status": str(record.get("status") or "idle").strip() or "idle",
        "ownerInstance": str(record.get("ownerInstance") or "").strip() or None,
        "ownerPid": normalize_optional_int(record.get("ownerPid")),
        "leaseExpiresAt": normalize_optional_int(record.get("leaseExpiresAt")),
        "activeScheduleId": str(record.get("activeScheduleId") or "").strip() or None,
    }


def write_session_record(record: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_session_record(record)
    if not normalized:
        raise BridgeError(500, "invalid session record")
    normalized["updatedAt"] = now_ms()
    write_json_atomic(get_registry_session_file(normalized["sessionId"]), normalized)
    return normalized


def sanitize_all_session_records() -> None:
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if not sessions_root.exists():
        return
    for session_file in sessions_root.glob("*.json"):
        raw = read_json_file(session_file, None)
        normalized = normalize_session_record(raw)
        if not normalized:
            print_log(f"[INIT] ignore invalid session record: {session_file}")
            continue
        if raw != normalized:
            write_json_atomic(session_file, normalized)


def read_session_record_by_id(session_id: str) -> Optional[dict[str, Any]]:
    return normalize_session_record(read_json_file(get_registry_session_file(session_id), None))


def read_session_key_entry(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    key_entry = read_json_file(get_registry_key_file(bot_id, key), None)
    if not isinstance(key_entry, dict):
        return None
    session_id = str(key_entry.get("sessionId") or "").strip()
    entry_bot_id = str(key_entry.get("botId") or "").strip()
    chat_key = str(key_entry.get("chatKey") or "").strip()
    if not session_id or entry_bot_id != bot_id or chat_key != key:
        return None
    return {"sessionId": session_id, "botId": entry_bot_id, "chatKey": chat_key}


def build_session_key_entry(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "sessionId": record["sessionId"],
        "botId": record["botId"],
        "chatKey": record["chatKey"],
        "createdAt": record["createdAt"],
        "updatedAt": record["updatedAt"],
    }


def read_session_record_by_key_unlocked(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    for candidate_key in candidate_chat_keys_for_lookup(bot_id, key):
        key_entry = read_session_key_entry(bot_id, candidate_key)
        if not key_entry:
            continue
        record = read_session_record_by_id(key_entry["sessionId"])
        if not record:
            continue
        if str(record.get("botId") or "") != bot_id or str(record.get("chatKey") or "") != candidate_key:
            continue
        return record
    return None


def repair_session_record_by_key_unlocked(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    existing = read_session_record_by_key_unlocked(bot_id, key)
    if existing:
        return existing

    key_file = get_registry_key_file(bot_id, key)
    for candidate_key in candidate_chat_keys_for_lookup(bot_id, key):
        existing = find_session_record_by_bot_and_key(bot_id, candidate_key)
        if existing:
            write_json_atomic(key_file, build_session_key_entry(existing))
            return existing

    for candidate_key in candidate_chat_keys_for_lookup(bot_id, key):
        candidate_key_file = get_registry_key_file(bot_id, candidate_key)
        raw_key_entry = read_json_file(candidate_key_file, None)
        stale_session_id = str((raw_key_entry or {}).get("sessionId") or "").strip() if isinstance(raw_key_entry, dict) else ""
        if stale_session_id and read_session_record_by_id(stale_session_id) is None:
            remove_path_if_exists(candidate_key_file)
    return None


def read_session_record_by_key(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    record = read_session_record_by_key_unlocked(bot_id, key)
    if record:
        key_file = get_registry_key_file(bot_id, key)
        if not key_file.exists():
            write_json_atomic(key_file, build_session_key_entry(record))
        return record
    with session_key_lock(bot_id, key):
        return repair_session_record_by_key_unlocked(bot_id, key)


def find_session_record_by_bot_and_key(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if not sessions_root.exists():
        return None
    candidates: list[dict[str, Any]] = []
    for session_file in sessions_root.glob("*.json"):
        record = normalize_session_record(read_json_file(session_file, None))
        if not record:
            continue
        if str(record.get("botId") or "") != bot_id or str(record.get("chatKey") or "") != key:
            continue
        candidates.append(record)
    if not candidates:
        return None
    candidates.sort(key=lambda item: (int(item.get("updatedAt") or 0), int(item.get("createdAt") or 0), str(item["sessionId"])))
    return candidates[-1]


@contextmanager
def session_key_lock(bot_id: str, key: str) -> Any:
    lock_file = get_registry_key_lock_file(bot_id, key)
    ensure_dir_for(lock_file)
    with open(lock_file, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def update_session_record(session_id: str, updater: Any) -> Optional[dict[str, Any]]:
    current = read_session_record_by_id(session_id)
    if not current:
        return None
    next_record = updater(dict(current)) if updater else current
    if not next_record:
        next_record = current
    return write_session_record(next_record)


def create_session_record(bot: BotState, key: str) -> dict[str, Any]:
    key_file = get_registry_key_file(bot.config["id"], key)
    ensure_dir_for(key_file)
    with session_key_lock(bot.config["id"], key):
        existing = repair_session_record_by_key_unlocked(bot.config["id"], key)
        if existing:
            return existing

        session_id = uid()
        record = normalize_session_record(
            {
                "sessionId": session_id,
                "botId": bot.config["id"],
                "chatKey": key,
                "lockFile": str(get_managed_session_lock_file(bot.config["id"], session_id)),
                "createdAt": now_ms(),
                "updatedAt": now_ms(),
                "status": "idle",
            }
        )
        if not record:
            raise BridgeError(500, "failed to create session record")

        key_payload = build_session_key_entry(record)
        write_json_atomic(key_file, key_payload)
        return write_session_record(record)


def get_or_create_session_record(bot: BotState, key: str) -> dict[str, Any]:
    return read_session_record_by_key(bot.config["id"], key) or create_session_record(bot, key)


def read_lease(lock_file: Path) -> Any:
    return read_json_file(lock_file, None)


def write_lease(lock_file: Path, lease: dict[str, Any]) -> None:
    write_json_atomic(lock_file, lease)


def acquire_session_lease(bot: BotState, sess: SessionState, key: str) -> bool:
    lock_file = sess.lock_file
    now = now_ms()
    next_lease = {
        "instanceId": INSTANCE_ID,
        "pid": os.getpid(),
        "botId": bot.config["id"],
        "chatKey": key,
        "updatedAt": now,
        "expiresAt": now + SESSION_LEASE_TTL_MS,
    }

    for _ in range(2):
        try:
            ensure_dir_for(lock_file)
            fd = os.open(str(lock_file), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(next_lease, ensure_ascii=False, indent=2))
            sess.lease_owned = True
            update_session_record(
                sess.session_id,
                lambda record: {
                    **record,
                    "ownerInstance": INSTANCE_ID,
                    "ownerPid": os.getpid(),
                    "leaseExpiresAt": next_lease["expiresAt"],
                    "status": "leased" if record.get("status") == "idle" else record.get("status"),
                },
            )
            add_log(bot, f"[{key}] lease acquired")
            return True
        except FileExistsError:
            current = read_lease(lock_file)
            if current and current.get("instanceId") == INSTANCE_ID:
                write_lease(lock_file, next_lease)
                sess.lease_owned = True
                return True
            if not current or current.get("expiresAt", 0) <= now:
                try:
                    lock_file.unlink()
                except FileNotFoundError:
                    pass
                continue
            sess.lease_owned = False
            return False
    sess.lease_owned = False
    return False


def renew_session_lease(bot: BotState, key: str, sess: SessionState) -> bool:
    if not sess.lease_owned:
        return False
    current = read_lease(sess.lock_file)
    if not current or current.get("instanceId") != INSTANCE_ID:
        sess.lease_owned = False
        add_log(bot, f"[{key}] session lease lost")
        return False
    current["updatedAt"] = now_ms()
    current["expiresAt"] = current["updatedAt"] + SESSION_LEASE_TTL_MS
    write_lease(sess.lock_file, current)
    update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": INSTANCE_ID,
            "ownerPid": os.getpid(),
            "leaseExpiresAt": current["expiresAt"],
        },
    )
    return True


def release_session_lease(bot: BotState, key: str, sess: SessionState) -> None:
    current = read_lease(sess.lock_file)
    if current and current.get("instanceId") == INSTANCE_ID:
        try:
            sess.lock_file.unlink()
        except FileNotFoundError:
            pass
        add_log(bot, f"[{key}] lease released")
    sess.lease_owned = False
    update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": None,
            "ownerPid": None,
            "leaseExpiresAt": None,
            "activeScheduleId": None if record.get("status") != "running" else record.get("activeScheduleId"),
            "status": "idle" if record.get("status") == "leased" else record.get("status"),
        },
    )


def limit_stream_content(content: str) -> str:
    if len(content) <= MAX_STREAM_CONTENT:
        return content
    head = 5000
    tail = 2500
    omitted = len(content) - head - tail
    if omitted <= 0:
        return content[:MAX_STREAM_CONTENT]
    return f"{content[:head]}\n\n...[middle omitted {omitted} chars]...\n\n{content[-tail:]}"


def format_log_timestamp(ts: Optional[float] = None) -> str:
    target = time.time() if ts is None else ts
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(target))


def print_log(message: str) -> None:
    print(f"[{format_log_timestamp()}] {message}", flush=True)


def add_log(bot: BotState, message: str) -> None:
    timestamp = format_log_timestamp()
    entry = f"[{timestamp}] {message}"
    bot.logs.append(entry)
    print_log(f"[{bot.config['name']}] {message}")


def format_log_field_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if re.fullmatch(r"[A-Za-z0-9._:/@+-]+", text):
        return text
    return json.dumps(text, ensure_ascii=False)


def add_event_log(bot: BotState, event: str, **fields: Any) -> None:
    payload: dict[str, Any] = {"event": event, "botConfigId": str(bot.config.get("id") or "").strip() or None}
    for key, value in fields.items():
        if value is None or value == "":
            continue
        payload[key] = value
    parts = [f"{key}={format_log_field_value(value)}" for key, value in payload.items() if value is not None]
    add_log(bot, " ".join(parts))


def detect_codex_runtime_status(stderr_text: str) -> Optional[str]:
    text = str(stderr_text or "").strip().lower()
    if not text:
        return None
    if "reconnecting" in text or "retrying connection" in text or ("retry" in text and "connection" in text):
        return "reconnecting"
    if "reconnected" in text or "connection restored" in text:
        return "connected"
    if "network" in text and ("timeout" in text or "error" in text or "failed" in text):
        return "network_issue"
    return None


def update_codex_runtime_status(
    bot: BotState, sess: SessionState, key: str, status: Optional[str], detail: Optional[str] = None
) -> None:
    normalized = str(status or "").strip() or None
    if normalized == sess.codex_runtime_status:
        return
    sess.codex_runtime_status = normalized
    if not normalized:
        return
    add_event_log(bot, "codex.runtime_status", chatKey=key, sessionId=sess.session_id, status=normalized, detail=detail)


def build_session_status_text(status: str, detail: Optional[str] = None) -> str:
    text = f"运行状态：{status}"
    if detail:
        text = f"{text}，{detail}"
    return f"{text}。"


def build_queue_status_text(position: int) -> str:
    ahead = max(0, int(position) - 1)
    if ahead:
        return build_session_status_text("排队中", f"前方还有 {ahead} 个任务")
    return build_session_status_text("排队中", "即将开始处理")


def build_thinking_status_text(elapsed_sec: int, detail: Optional[str] = None) -> str:
    suffix = f"已运行 {max(0, int(elapsed_sec))}s"
    if detail:
        suffix = f"{suffix}，{detail}"
    return build_session_status_text("思考中", suffix)


def build_working_status_text(elapsed_sec: int, detail: Optional[str] = None) -> str:
    elapsed = max(0, int(elapsed_sec))
    dots = "." * ((elapsed // 5) % 3 + 1)
    text = f"运行状态：整理回复中{dots} 已运行 {elapsed}s"
    if detail:
        text = f"{text}，{detail}"
    return f"{text}。"


def build_status_stream_content(status_text: str, summary: Optional[str] = None) -> str:
    cleaned_summary = str(summary or "").strip()
    if not cleaned_summary:
        return status_text
    return limit_stream_content(f"{status_text}\n\n{cleaned_summary}")


def acquire_bot_runtime_lock(bot: BotState) -> bool:
    if bot.runtime_lock_handle is not None:
        return True
    lock_file = get_bot_runtime_lock_file(bot.config["botId"])
    ensure_dir_for(lock_file)
    handle = open(lock_file, "a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return False
    bot.runtime_lock_handle = handle
    return True


def bot_runtime_lock_held_elsewhere(wecom_bot_id: str) -> bool:
    lock_file = get_bot_runtime_lock_file(wecom_bot_id)
    ensure_dir_for(lock_file)
    handle = open(lock_file, "a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return False
    finally:
        handle.close()


def release_bot_runtime_lock(bot: BotState) -> None:
    handle = bot.runtime_lock_handle
    if handle is None:
        return
    bot.runtime_lock_handle = None
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def message_sender_userid(message: dict[str, Any]) -> str:
    body = message.get("body") or {}
    return str(((body.get("from") or {}).get("userid")) or "unknown")


def strip_text_mentions(content: str, bot_name: Optional[str] = None) -> str:
    text = content or ""
    normalized_bot_name = str(bot_name or "").strip()
    if not normalized_bot_name:
        return LEADING_MENTION_RE.sub("", text, count=1).strip()
    cursor = text.lstrip()
    bot_pattern = re.compile(rf"@{re.escape(normalized_bot_name)}(?P<suffix>\s+|[{re.escape(MENTION_DELIMITER_CHARS)}]|$)")
    leading_mentions_pattern = re.compile(r"^(?:@[^@\n]+?\s+)*$")
    for bot_match in bot_pattern.finditer(cursor):
        start = bot_match.start()
        if start > 0 and not cursor[start - 1].isspace():
            continue
        prefix = cursor[:start]
        if prefix and not leading_mentions_pattern.fullmatch(prefix):
            continue
        return cursor[bot_match.end() :].lstrip().strip()
    return text.strip()


def chat_key_for_bot(bot: BotState, message: dict[str, Any]) -> str:
    body = message.get("body") or {}
    raw_sender_user_id = message_sender_userid(message)
    remember_workspace_user_alias(str(bot.config["id"]), raw_sender_user_id)
    if body.get("chattype") == "group" and body.get("chatid"):
        if str(bot.config.get("groupSessionMode") or "per-user") == "per-user":
            return f"group-user:{body['chatid']}:{raw_sender_user_id}"
        return f"group:{body['chatid']}"
    return f"single:{raw_sender_user_id}"


def chat_key_is_group(key: str) -> bool:
    return key.startswith("group:") or key.startswith("group-user:")


def validate_chat_key(key: Any, *, label: str = "chatKey") -> str:
    text = str(key or "").strip()
    if not text:
        raise BridgeError(400, f"{label} required")
    if text.startswith("single:") and bool(text.split(":", 1)[1]):
        return text
    if text.startswith("group:") and bool(text.split(":", 1)[1]):
        return text
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        if len(parts) == 3 and parts[1] and parts[2]:
            return text
    raise BridgeError(400, f"invalid {label}: {text}")


def build_bridge_context(bot: BotState, sess: SessionState, key: str) -> str:
    chat_type = "group" if chat_key_is_group(key) else "single"
    workspace_paths = ensure_session_workspace_dirs(bot, key)
    runtime_cwd = get_session_runtime_cwd(bot, key)
    workspace_codex_skills_dir = get_workspace_codex_skills_dir(runtime_cwd)
    bridge_global_skills_dir = get_session_global_skills_root(sess.session_id)
    allowed_file_roots = ", ".join(str(root) for root in get_allowed_file_roots(bot, key))
    user_id = chat_key_to_user_id(key) or "-"
    room_id = chat_key_to_room_id(key) or "-"
    execution_mode_note = (
        "Local network access is blocked inside the Codex sandbox for this bridge. "
        "Never probe localhost ports or use curl/python sockets to send files."
        if codex_runs_in_sandbox()
        else "This bridge is running Codex in host mode without the built-in sandbox. "
        "Use shell and network access carefully, and still prefer the localSendFileCommand when sending files back."
    )
    return "\n".join(
        [
            "[BridgeContext]",
            f"botName: {bot.config['name']}",
            f"chatKey: {key}",
            f"chatType: {chat_type}",
            f"userId: {user_id}",
            f"roomId: {room_id}",
            f"sessionId: {sess.session_id}",
            f"executionMode: {CODEX_EXEC_MODE}",
            f"bridgeApiBase: {BRIDGE_API_BASE}",
            f"WORKDIR_DIR: {workspace_paths['workDir']}",
            f"CWD_DIR: {runtime_cwd}",
            f"CHATFILE_DIR: {workspace_paths['chatfile']}",
            f"EXPORT_DIR: {workspace_paths['chatfile']}",
            f"WORKFILE_DIR: {workspace_paths['workfile'] or '-'}",
            f"ROOMFILE_DIR: {workspace_paths['roomfile'] or '-'}",
            f"WORKSPACE_CODEX_SKILLS_DIR: {workspace_codex_skills_dir}",
            f"GLOBAL_CODEX_SKILLS_DIR: {bridge_global_skills_dir}",
            f"sendFileEndpoint: {BRIDGE_API_BASE}/api/send-file",
            f"scheduleMessageEndpoint: {BRIDGE_API_BASE}/api/schedule-message",
            (
                "localSendFileCommand: "
                f"python3 {LOCAL_FILE_SEND_COMMAND} --chat-key '{key}' --bot-config-id '{bot.config['id']}' --bot-name '{bot.config['name']}' --file-path ABSOLUTE_FILE_PATH "
                f"(fallback: --session-id {sess.session_id})"
            ),
            (
                "localScheduleMessageCommand: "
                f"python3 {LOCAL_SCHEDULE_MESSAGE_COMMAND} --chat-key '{key}' --bot-config-id '{bot.config['id']}' --bot-name '{bot.config['name']}' "
                f"(fallback: --session-id {sess.session_id}) "
                "--run-at RFC3339_OR_EPOCH_MS --message MESSAGE "
                'or --cron "0 9 * * *" --timezone TZ --message MESSAGE'
            ),
            f"localFileSendQueueRoot: {LOCAL_FILE_SEND_QUEUE_ROOT}",
            f"allowedFileSendRoots: {allowed_file_roots}",
            execution_mode_note,
            "When you need to send a file back, use the localSendFileCommand instead of HTTP.",
            "CHATFILE_DIR is the session exchange area for files received from and sent back to WeCom.",
            "EXPORT_DIR is the default directory for user-visible exported files in this session.",
            "WORKFILE_DIR is the user's long-lived private workspace when this chat has a user scope.",
            "ROOMFILE_DIR is the room-level shared workspace for group chats.",
            "WORKDIR_DIR is the shared project root for code context and is not automatically allowed for file send-back.",
            "CWD_DIR is the actual Codex working directory for this session.",
            "Create final exported files under EXPORT_DIR or CHATFILE_DIR, not under /tmp or other temp paths.",
            "Bridge sets TMPDIR/TMP/TEMP to CHATFILE_DIR for the Codex subprocess so temporary exports default there.",
            "Only files under allowedFileSendRoots can be sent back. Export files to CHATFILE_DIR first when needed.",
            "When you need to schedule a follow-up message in this chat, prefer the localScheduleMessageCommand instead of HTTP.",
            "If you need to send a file back to the user, prefer current chatKey; use sessionId only as a fallback. Never switch from group to single chat on your own.",
            "Personal Codex skills should live in CWD_DIR/.codex/skills.",
            "Project shared skills are injected into GLOBAL_CODEX_SKILLS_DIR by the bridge.",
            "Bridge runs Codex with cwd=CWD_DIR and a bridge-managed CODEX_HOME overlay.",
            "[/BridgeContext]",
        ]
    )


def build_pending_media_context(pending_media: list[dict[str, Any]]) -> str:
    lines = []
    for idx, media in enumerate(pending_media, start=1):
        if media.get("kind") == "image":
            lines.append(f"Attachment {idx}: image {media['path']}")
        else:
            lines.append(f"Attachment {idx}: file {media['fileName']} path {media['path']}")
    return "\n\n".join(lines)


def build_pending_media_notes(notes: list[str]) -> str:
    return "\n".join(f"Attachment note {idx}: {note}" for idx, note in enumerate(notes, start=1))


def build_active_media_download_note(pending_count: int) -> str:
    if not pending_count:
        return ""
    return f"There are {pending_count} attachments still downloading. They may not be available for this answer."


def restore_session_active_media(sess: SessionState) -> None:
    if sess.active_run_media:
        sess.pending_media = list(sess.active_run_media) + list(sess.pending_media)
        sess.active_run_media = []
    if sess.active_run_media_notes:
        sess.pending_media_notes = list(sess.active_run_media_notes) + list(sess.pending_media_notes)
        sess.active_run_media_notes = []


async def send_ws_payload(bot: BotState, payload: dict[str, Any]) -> None:
    if not bot.ws or bot.ws.closed:
        return
    await asyncio.wait_for(bot.ws.send_json(payload), WEBSOCKET_SEND_TIMEOUT_SEC)


def payload_req_id(payload: dict[str, Any]) -> str:
    return str(((payload.get("headers") or {}).get("req_id")) or "")


def is_reply_payload(payload: dict[str, Any]) -> bool:
    return payload.get("cmd") == "aibot_respond_msg" and bool(payload_req_id(payload))


def reply_payload_finished(payload: dict[str, Any]) -> bool:
    if not is_reply_payload(payload):
        return False
    stream = ((payload.get("body") or {}).get("stream") or {})
    return bool(stream.get("finish"))


def reply_payload_content(payload: dict[str, Any]) -> str:
    stream = ((payload.get("body") or {}).get("stream") or {})
    return str(stream.get("content") or "")


def register_reply_session(bot: BotState, req_id: Optional[str], sess: SessionState) -> None:
    if not req_id:
        return
    bot.reply_sessions[req_id] = sess
    sess.reply_started_at.setdefault(req_id, time.time())


def cleanup_reply_session(bot: BotState, req_id: Optional[str], sess: Optional[SessionState] = None) -> None:
    if not req_id:
        return
    context = sess or bot.reply_sessions.get(req_id)
    if context:
        context.reply_started_at.pop(req_id, None)
        context.reply_last_sent_at.pop(req_id, None)
        context.reply_proactive_req_ids.discard(req_id)
        context.proactive_status_sent_at.pop(req_id, None)
        context.reply_mentions_sent.discard(req_id)
    bot.reply_sessions.pop(req_id, None)


def cleanup_session_reply_contexts(bot: BotState, sess: SessionState) -> None:
    for req_id, context in list(bot.reply_sessions.items()):
        if context is sess:
            cleanup_reply_session(bot, req_id, sess)


def key_for_session(bot: BotState, sess: SessionState) -> str:
    for key, context in bot.sessions.items():
        if context is sess:
            return key
    return ""


def mark_session_reply_sent(bot: BotState, sess: Optional[SessionState], payload: dict[str, Any]) -> None:
    if not sess or not is_reply_payload(payload):
        return
    req_id = payload_req_id(payload)
    sess.reply_last_sent_at[req_id] = time.time()
    if req_id and key_for_session(bot, sess).startswith("group-user:"):
        mention = format_group_user_mention(chat_key_to_user_id(key_for_session(bot, sess)))
        if mention and reply_payload_content(payload).startswith(mention):
            sess.reply_mentions_sent.add(req_id)
    if reply_payload_finished(payload):
        cleanup_reply_session(bot, req_id, sess)


def reply_idle_too_long(sess: SessionState, req_id: str) -> bool:
    started_at = sess.reply_started_at.get(req_id)
    if not started_at:
        return False
    last_sent_at = sess.reply_last_sent_at.get(req_id, started_at)
    return (time.time() - last_sent_at) >= REPLY_IDLE_FALLBACK_SEC


def reply_age_too_long(sess: SessionState, req_id: str) -> bool:
    started_at = sess.reply_started_at.get(req_id)
    return bool(started_at and (time.time() - started_at) >= REPLY_MAX_AGE_FALLBACK_SEC)


def reply_should_use_proactive(sess: SessionState, req_id: str) -> bool:
    return bool(req_id and (req_id in sess.reply_proactive_req_ids or reply_age_too_long(sess, req_id)))


def mark_reply_proactive(sess: SessionState, req_id: Optional[str]) -> None:
    if req_id:
        sess.reply_proactive_req_ids.add(req_id)


def build_reply_window_expired_notice() -> str:
    return f"流式回复窗口即将到期，任务仍在后台运行，后续会约每 {PROACTIVE_STATUS_INTERVAL_SEC}s 主动发送阶段摘要和最终结果。"


def build_queued_proactive_notice(position: int) -> str:
    return f"{build_queue_status_text(position)} 任务完成后会主动发送结果。"


def proactive_status_due(sess: SessionState, req_id: str) -> bool:
    last_sent_at = sess.proactive_status_sent_at.get(req_id, 0)
    return (time.time() - last_sent_at) >= PROACTIVE_STATUS_INTERVAL_SEC


def mark_proactive_status_sent(sess: SessionState, req_id: str) -> None:
    sess.proactive_status_sent_at[req_id] = time.time()


async def close_reply_stream_for_proactive(bot: BotState, key: str, sess: SessionState, req_id: str) -> None:
    if not req_id or req_id in sess.reply_proactive_req_ids:
        return
    notice_payload = build_session_text_payload(key, sess, req_id, build_reply_window_expired_notice(), True)
    if notice_payload:
        try:
            await send_session_ws_payload(bot, sess, notice_payload)
        except Exception as exc:
            add_log(bot, f"[{key}] stream close notice skipped req_id={req_id}: {exc}")
    mark_reply_proactive(sess, req_id)


async def acquire_session_send_lock(sess: SessionState, timeout_sec: Optional[int] = None) -> bool:
    if timeout_sec is None:
        await sess.send_lock.acquire()
        return True
    try:
        await asyncio.wait_for(sess.send_lock.acquire(), timeout_sec)
        return True
    except asyncio.TimeoutError:
        return False


async def send_session_ws_payload(
    bot: BotState,
    sess: SessionState,
    payload: dict[str, Any],
    *,
    send_timeout_sec: Optional[int] = None,
    lock_timeout_sec: Optional[int] = None,
) -> None:
    acquired = await acquire_session_send_lock(sess, lock_timeout_sec)
    if not acquired:
        raise BridgeError(504, "websocket send lock timeout")
    try:
        if not bot.ws or bot.ws.closed:
            raise BridgeError(503, "bot websocket closed")
        try:
            await asyncio.wait_for(bot.ws.send_json(payload), send_timeout_sec or WEBSOCKET_SEND_TIMEOUT_SEC)
        except asyncio.TimeoutError as exc:
            raise BridgeError(504, "websocket send timeout") from exc
        mark_session_reply_sent(bot, sess, payload)
    finally:
        sess.send_lock.release()


async def send_session_ws_payload_with_ack(
    bot: BotState,
    sess: SessionState,
    payload: dict[str, Any],
    timeout_sec: int,
    *,
    send_timeout_sec: Optional[int] = None,
    lock_timeout_sec: Optional[int] = None,
) -> dict[str, Any]:
    acquired = await acquire_session_send_lock(sess, lock_timeout_sec)
    if not acquired:
        raise BridgeError(504, "websocket send lock timeout")
    try:
        response = await send_ws_payload_with_ack(bot, payload, timeout_sec, send_timeout_sec=send_timeout_sec)
        mark_session_reply_sent(bot, sess, payload)
        return response
    finally:
        sess.send_lock.release()


def chat_key_to_send_target(key: str) -> tuple[int, str]:
    if key.startswith("group-user:"):
        parts = key.split(":", 2)
        return 2, parts[1]
    chat_type_name, chat_id = key.split(":", 1)
    return (2 if chat_type_name == "group" else 1), chat_id


def limit_proactive_text(content: str) -> str:
    text = content.strip()
    if len(text) <= PROACTIVE_TEXT_MAX_CHARS:
        return text
    suffix = "...(truncated)"
    return text[: max(0, PROACTIVE_TEXT_MAX_CHARS - len(suffix))].rstrip() + suffix


def format_group_user_mention(user_id: Optional[str]) -> str:
    text = str(user_id or "").strip()
    if not text:
        return ""
    return f"<@{text}>"


def prepend_group_user_mention(content: str, user_id: Optional[str]) -> str:
    mention = format_group_user_mention(user_id)
    text = content.strip()
    if not mention:
        return text
    if text.startswith(mention):
        return text
    if not text:
        return mention
    return f"{mention}\n{text}"


def should_prepend_group_user_mention_to_reply(key: str, sess: SessionState, req_id: Optional[str]) -> bool:
    if not req_id or not key.startswith("group-user:"):
        return False
    if req_id in sess.reply_proactive_req_ids:
        return False
    if req_id in sess.reply_mentions_sent:
        return False
    return True


def proactive_reply_mention_user_id(key: str, sess: SessionState, req_id: Optional[str]) -> Optional[str]:
    if not req_id or not key.startswith("group-user:"):
        return None
    if not reply_age_too_long(sess, req_id):
        return None
    return chat_key_to_user_id(key)


def proactive_chat_mention_user_id(key: str, mention_user_id: Optional[str] = None) -> Optional[str]:
    explicit = str(mention_user_id or "").strip()
    if explicit:
        return explicit
    if key.startswith("group-user:"):
        return chat_key_to_user_id(key)
    return None


def build_proactive_chat_payload(key: str, content: str, mention_user_id: Optional[str] = None) -> dict[str, Any]:
    chat_type, chat_id = chat_key_to_send_target(key)
    resolved_mention_user_id = proactive_chat_mention_user_id(key, mention_user_id)
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {
            "chatid": chat_id,
            "chat_type": chat_type,
            "msgtype": "markdown",
            "markdown": {"content": limit_proactive_text(prepend_group_user_mention(content, resolved_mention_user_id))},
        },
    }


async def send_ws_payload_with_ack(
    bot: BotState, payload: dict[str, Any], timeout_sec: int, *, send_timeout_sec: Optional[int] = None
) -> dict[str, Any]:
    req_id = payload_req_id(payload)
    if not req_id:
        raise BridgeError(500, "payload req_id required for ack")
    if not bot.ws or bot.ws.closed:
        raise BridgeError(503, "bot websocket closed")
    future = create_request_future(bot, req_id)
    try:
        try:
            await asyncio.wait_for(bot.ws.send_json(payload), send_timeout_sec or WEBSOCKET_SEND_TIMEOUT_SEC)
        except asyncio.TimeoutError as exc:
            bot.pending_requests.pop(req_id, None)
            raise BridgeError(504, "websocket send timeout") from exc
        try:
            return await asyncio.wait_for(future, timeout_sec)
        except asyncio.TimeoutError as exc:
            bot.pending_requests.pop(req_id, None)
            raise BridgeError(504, "websocket ack timeout") from exc
    except Exception:
        bot.pending_requests.pop(req_id, None)
        raise


def build_session_text_payload(key: str, sess: SessionState, req_id: Optional[str], content: str, final: bool) -> Optional[dict[str, Any]]:
    if req_id:
        stream_content = content
        if should_prepend_group_user_mention_to_reply(key, sess, req_id):
            stream_content = prepend_group_user_mention(content, chat_key_to_user_id(key))
        return {
            "cmd": "aibot_respond_msg",
            "headers": {"req_id": req_id},
            "body": {"msgtype": "stream", "stream": {"id": sess.session_id, "finish": final, "content": stream_content}},
        }
    if not final:
        return None
    return build_proactive_chat_payload(key, content)


async def send_transient_session_status(
    bot: BotState,
    key: str,
    sess: SessionState,
    req_id: Optional[str],
    content: str,
) -> bool:
    payload = build_session_text_payload(key, sess, req_id, content, False)
    if not payload or not bot.ws or bot.ws.closed:
        return False
    try:
        await send_session_ws_payload(bot, sess, payload)
        return True
    except Exception as exc:
        add_log(bot, f"[{key}] transient status send skipped: {exc}")
        return False


async def send_session_status(
    bot: BotState,
    key: str,
    sess: SessionState,
    req_id: Optional[str],
    content: str,
) -> bool:
    if req_id and reply_age_too_long(sess, req_id):
        await close_reply_stream_for_proactive(bot, key, sess, req_id)
    if req_id and req_id in sess.reply_proactive_req_ids:
        if not proactive_status_due(sess, req_id):
            return False
        payload = build_proactive_chat_payload(key, content)
        delivered = await send_or_store_session_payload(bot, key, sess, payload, False)
        if delivered:
            mark_proactive_status_sent(sess, req_id)
        return delivered
    payload = build_session_text_payload(key, sess, req_id, content, False)
    if not payload:
        return False
    if bot.ws and not bot.ws.closed:
        try:
            await send_session_ws_payload(
                bot,
                sess,
                payload,
                send_timeout_sec=STATUS_SEND_TIMEOUT_SEC,
                lock_timeout_sec=STATUS_SEND_LOCK_TIMEOUT_SEC,
            )
            sess.pending_stream_payload = None
            return True
        except Exception as exc:
            add_log(bot, f"[{key}] stream status deferred req_id={payload_req_id(payload) or '-'}: {exc}")
            add_event_log(
                bot,
                "session.status_deferred",
                chatKey=key,
                sessionId=sess.session_id,
                reqId=payload_req_id(payload) or None,
                reason=str(exc),
            )
    sess.pending_stream_payload = dict(payload)
    return False


async def send_or_store_session_payload(
    bot: BotState, key: str, sess: SessionState, payload: dict[str, Any], final: bool
) -> bool:
    outgoing_payload = dict(payload)
    original_reply_req_id = payload_req_id(payload) if is_reply_payload(payload) else ""
    if payload.get("cmd") == "aibot_send_msg":
        headers = dict((payload.get("headers") or {}))
        headers["req_id"] = uid()
        outgoing_payload["headers"] = headers
    elif is_reply_payload(outgoing_payload):
        reply_req_id = payload_req_id(outgoing_payload)
        if reply_req_id and not final and reply_age_too_long(sess, reply_req_id):
            await close_reply_stream_for_proactive(bot, key, sess, reply_req_id)
            sess.pending_stream_payload = None
            return False
        if reply_req_id and final and (reply_idle_too_long(sess, reply_req_id) or reply_should_use_proactive(sess, reply_req_id)):
            mark_reply_proactive(sess, reply_req_id)
            outgoing_payload = build_proactive_chat_payload(
                key,
                reply_payload_content(outgoing_payload),
                proactive_reply_mention_user_id(key, sess, reply_req_id),
            )
    req_id = payload_req_id(outgoing_payload)
    payload_kind = "final" if final else "stream"
    requires_ack = outgoing_payload.get("cmd") == "aibot_send_msg" and bool(req_id)
    if bot.ws and not bot.ws.closed:
        try:
            if requires_ack:
                response = await send_session_ws_payload_with_ack(bot, sess, outgoing_payload, PROACTIVE_SEND_ACK_TIMEOUT_SEC)
                if response.get("errcode") not in (None, 0):
                    raise BridgeError(502, f"proactive send failed: {response.get('errcode')} {response.get('errmsg', '')}".strip())
            else:
                await send_session_ws_payload(bot, sess, outgoing_payload)
            if final:
                sess.pending_final_payload = None
                sess.pending_stream_payload = None
                cleanup_reply_session(bot, original_reply_req_id, sess)
            else:
                sess.pending_stream_payload = None
            return True
        except Exception as exc:
            add_log(bot, f"[{key}] {payload_kind} send deferred req_id={req_id or '-'}: {exc}")

    if final:
        sess.pending_final_payload = outgoing_payload
        sess.pending_stream_payload = None
    else:
        sess.pending_stream_payload = outgoing_payload
    add_log(bot, f"[{key}] cached {payload_kind} reply req_id={req_id or '-'}")
    return False


async def flush_session_pending_payloads(bot: BotState, key: str, sess: SessionState) -> None:
    if not bot.ws or bot.ws.closed:
        return
    if sess.pending_final_payload:
        payload = dict(sess.pending_final_payload)
        if await send_or_store_session_payload(bot, key, sess, payload, True):
            add_log(bot, f"[{key}] delivered cached final reply req_id={payload_req_id(payload) or '-'}")
            if sess.queue and can_continue_session_queue(sess):
                process_queue(bot, sess, key)
        return
    if sess.pending_stream_payload and sess.running:
        payload = dict(sess.pending_stream_payload)
        if await send_or_store_session_payload(bot, key, sess, payload, False):
            add_log(bot, f"[{key}] delivered cached running status req_id={payload_req_id(payload) or '-'}")


async def flush_all_pending_session_payloads(bot: BotState) -> None:
    for key, sess in list(bot.sessions.items()):
        await flush_session_pending_payloads(bot, key, sess)


async def respond_queued_error(bot: BotState, req_id: Optional[str], message: str) -> None:
    if not req_id:
        return
    sess = bot.reply_sessions.get(req_id)
    payload = {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": req_id},
        "body": {"msgtype": "stream", "stream": {"id": sess.session_id if sess else uid(), "finish": True, "content": f"Error: {message}"}},
    }
    try:
        if sess:
            await send_session_ws_payload(bot, sess, payload)
        else:
            await send_ws_payload(bot, payload)
    except Exception as exc:
        add_log(bot, f"queued error reply skipped req_id={req_id}: {exc}")
    else:
        cleanup_reply_session(bot, req_id, sess)


async def respond_info(bot: BotState, req_id: Optional[str], message: str, final: bool = True) -> None:
    if not req_id:
        return
    sess = bot.reply_sessions.get(req_id)
    payload = {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": req_id},
        "body": {"msgtype": "stream", "stream": {"id": sess.session_id if sess else uid(), "finish": final, "content": message}},
    }
    try:
        if sess:
            await send_session_ws_payload(bot, sess, payload)
        else:
            await send_ws_payload(bot, payload)
    except Exception as exc:
        add_log(bot, f"info reply skipped req_id={req_id}: {exc}")
        return
    if final:
        cleanup_reply_session(bot, req_id, sess)


async def respond_session_info(
    bot: BotState, key: str, sess: SessionState, req_id: Optional[str], message: str, final: bool = True
) -> bool:
    if not req_id:
        return False
    if not final and req_id in sess.reply_proactive_req_ids:
        if not proactive_status_due(sess, req_id):
            return False
        payload = build_proactive_chat_payload(key, message)
        delivered = await send_or_store_session_payload(bot, key, sess, payload, False)
        if delivered:
            mark_proactive_status_sent(sess, req_id)
        return delivered
    await respond_info(bot, req_id, message, final=final)
    return True


def sanitize_file_name(name: str, fallback: str = "attachment.bin") -> str:
    base = re.sub(r"[^\w.-]+", "_", str(name or fallback).strip())
    return base or fallback


def build_nonconflicting_file_path(target_dir: Path, file_name: str) -> Path:
    candidate = target_dir / file_name
    if not candidate.exists():
        return candidate
    path = Path(file_name)
    stem = path.stem or "attachment"
    suffix = path.suffix
    index = 1
    while True:
        candidate = target_dir / f"{stem}-{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def extension_from_url(raw_url: str) -> str:
    try:
        path = urlparse(raw_url).path
        ext = Path(path).suffix
        return ext if ext and len(ext) <= 10 else ""
    except Exception:
        return ""


def extension_from_content_type(content_type: str, fallback: str = ".bin") -> str:
    media_type = str(content_type or "").split(";", 1)[0].strip().lower()
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "application/pdf": ".pdf",
        "text/plain": ".txt",
        "application/zip": ".zip",
    }.get(media_type, fallback)


def decode_aes_key(aes_key: str) -> Optional[bytes]:
    if not aes_key:
        return None
    raw = str(aes_key).strip()
    try:
        normalized = raw if len(raw) % 4 == 0 else raw + ("=" * (4 - (len(raw) % 4)))
        decoded = base64.b64decode(normalized)
        if len(decoded) == 32:
            return decoded
    except Exception:
        pass
    utf8 = raw.encode("utf-8")
    return utf8 if len(utf8) == 32 else None


def decrypt_media_buffer(buffer: bytes, aes_key: str) -> bytes:
    key = decode_aes_key(aes_key)
    if not key:
        raise BridgeError(400, "invalid aes key")
    iv = key[:16]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(buffer)
    if not decrypted:
        return decrypted
    pad = decrypted[-1]
    if 1 <= pad <= 32 and decrypted.endswith(bytes([pad]) * pad):
        return decrypted[:-pad]
    return decrypted


async def download_buffer(url: str) -> dict[str, Any]:
    if HTTP_SESSION is None:
        raise BridgeError(500, "http session not ready")
    timeout = aiohttp.ClientTimeout(total=MEDIA_TOTAL_TIMEOUT / 1000, connect=MEDIA_CONNECT_TIMEOUT / 1000)
    async with HTTP_SESSION.get(url, timeout=timeout, allow_redirects=True) as response:
        if response.status != 200:
            raise BridgeError(502, f"media download failed: HTTP {response.status}")
        data = await response.read()
        return {
            "data": data,
            "contentType": response.headers.get("Content-Type", ""),
            "contentDisposition": response.headers.get("Content-Disposition", ""),
        }


async def download_buffer_via_curl(url: str) -> dict[str, Any]:
    tmp_dir = Path(tempfile.mkdtemp(prefix="wecom-codex-media-py-"))
    header_file = tmp_dir / "headers.txt"
    body_file = tmp_dir / "body.bin"
    process = await asyncio.create_subprocess_exec(
        "curl",
        "-fsSL",
        "--connect-timeout",
        str(max(1, MEDIA_CONNECT_TIMEOUT // 1000)),
        "--max-time",
        str(max(1, MEDIA_TOTAL_TIMEOUT // 1000)),
        "-D",
        str(header_file),
        "-o",
        str(body_file),
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    try:
        if process.returncode != 0:
            raise BridgeError(502, f"curl media download failed: {(stderr or stdout).decode('utf-8', 'ignore').strip()}")
        header_text = header_file.read_text("utf-8") if header_file.exists() else ""
        data = body_file.read_bytes()
        content_type = ""
        content_disposition = ""
        for line in header_text.splitlines():
            lower = line.lower()
            if lower.startswith("content-type:"):
                content_type = line.split(":", 1)[1].strip()
            if lower.startswith("content-disposition:"):
                content_disposition = line.split(":", 1)[1].strip()
        return {"data": data, "contentType": content_type, "contentDisposition": content_disposition}
    finally:
        for child in tmp_dir.glob("*"):
            try:
                child.unlink()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


def should_fallback_to_curl(exc: Exception) -> bool:
    message = str(exc)
    return bool(re.search(r"timed out|connection reset|socket|network|refused|unreachable", message, re.I))


def file_name_from_content_disposition(content_disposition: str) -> str:
    value = str(content_disposition or "")
    match = re.search(r'filename\*?=(?:UTF-8\'\'|")?([^";]+)', value, re.I)
    if not match:
        return ""
    try:
        return unquote(match.group(1).replace('"', ""))
    except Exception:
        return match.group(1).replace('"', "")


async def download_incoming_media(
    bot: BotState, sess: SessionState, key: str, kind: str, payload: dict[str, Any]
) -> dict[str, Any]:
    url = payload.get("url") or payload.get("download_url") or payload.get("downloadUrl")
    if not url:
        raise BridgeError(400, f"{kind} payload missing url")
    inbound_limit = MAX_INBOUND_IMAGE_SIZE if kind == "image" else MAX_INBOUND_FILE_SIZE

    try:
        response = await download_buffer(url)
    except Exception as exc:
        if not should_fallback_to_curl(exc):
            raise
        add_log(bot, f"[{key}] direct download failed, fallback to curl: {exc}")
        response = await download_buffer_via_curl(url)

    data = response["data"]
    if len(data) > inbound_limit:
        raise BridgeError(413, f"{kind} too large: {len(data)} bytes")
    aes_key = payload.get("aeskey") or payload.get("aes_key")
    if aes_key:
        data = decrypt_media_buffer(data, aes_key)
        if len(data) > inbound_limit:
            raise BridgeError(413, f"{kind} too large after decrypt: {len(data)} bytes")

    target_dir = ensure_session_workspace_dirs(bot, key)["chatfile"]
    assert target_dir is not None
    ext = extension_from_url(url) or extension_from_content_type(response.get("contentType", ""), ".jpg" if kind == "image" else ".bin")
    file_name = sanitize_file_name(
        payload.get("filename")
        or payload.get("name")
        or file_name_from_content_disposition(response.get("contentDisposition", ""))
        or f"{kind}-{int(time.time())}{ext}"
    )
    target = build_nonconflicting_file_path(target_dir, file_name)
    target.write_bytes(data)
    add_log(bot, f"[{key}] received {kind}: {target}")
    return {
        "kind": kind,
        "path": str(target),
        "size": len(data),
        "contentType": response.get("contentType", ""),
        "fileName": file_name,
    }


def extract_mixed_text(mixed: dict[str, Any]) -> str:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    parts = []
    for item in items:
        if item.get("msgtype") == "text":
            content = ((item.get("text") or {}).get("content") or "").strip()
            if content:
                parts.append(content)
    return "\n".join(parts)


def extract_mixed_images(mixed: dict[str, Any]) -> list[dict[str, Any]]:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    return [item.get("image") for item in items if item.get("msgtype") == "image" and (item.get("image") or {}).get("url")]


def get_or_create_session(bot: BotState, key: str) -> SessionState:
    if key not in bot.sessions:
        record = get_or_create_session_record(bot, key)
        sess = SessionState(
            session_id=record["sessionId"],
            work_dir=str(get_session_runtime_cwd(bot, key)),
            lock_file=Path(record["lockFile"]),
            thread_id=record.get("threadId"),
        )
        bot.sessions[key] = sess
        add_log(bot, f"[{key}] new session")
        add_event_log(bot, "session.created", chatKey=key, sessionId=sess.session_id)
    sess = bot.sessions[key]
    sess.work_dir = str(get_session_runtime_cwd(bot, key))
    sess.last_active = time.time()
    ensure_session_workspace_dirs(bot, key)
    return sess


def build_prompt(
    bot: BotState, sess: SessionState, key: str, text: str, pending_media: list[dict[str, Any]], pending_media_notes: list[str]
) -> str:
    media_context = build_pending_media_context(pending_media)
    media_notes = build_pending_media_notes(pending_media_notes)
    active_download_note = build_active_media_download_note(sess.pending_media_downloads)
    parts = [build_bridge_context(bot, sess, key), media_context, media_notes, active_download_note]
    parts = [part for part in parts if part]
    return f"{chr(10).join(parts)}\n\nUser request:\n{text}"


def session_run_is_current(sess: SessionState, current_task: Optional[asyncio.Task], run_generation: Optional[int]) -> bool:
    if run_generation is None:
        return sess.run_task is None or sess.run_task is current_task
    return sess.run_task is current_task and sess.run_generation == run_generation


def resume_session_queue(bot: BotState, sess: SessionState, key: str, reason: str) -> None:
    if not sess.queue:
        return
    if can_continue_session_queue(sess):
        add_log(bot, f"[{key}] resume queue after {reason}")
        process_queue(bot, sess, key)
    else:
        add_log(bot, f"[{key}] queue paused after {reason} because session lease is not owned")


def process_queue(bot: BotState, sess: SessionState, key: str) -> None:
    if sess.running or sess.pending_final_payload or not sess.queue:
        return
    item = sess.queue.pop(0)
    sess.running = True
    sess.interrupt_requested = False
    sess.run_generation += 1
    run_generation = sess.run_generation
    register_reply_session(bot, item.get("reqId"), sess)
    sess.active_schedule_id = str(item.get("scheduleId") or "").strip() or None
    sess.active_scheduled_job_file = str(item.get("scheduledJobFile") or "").strip() or None
    sess.active_schedule_request_id = str(item.get("scheduleRequestId") or "").strip() or None
    pending_media = list(sess.pending_media)
    pending_media_notes = list(sess.pending_media_notes)
    prompt = build_prompt(bot, sess, key, item["text"], pending_media, pending_media_notes)
    image_paths = [media["path"] for media in pending_media if media.get("kind") == "image"]
    sess.active_run_media = list(pending_media)
    sess.active_run_media_notes = list(pending_media_notes)
    sess.pending_media = []
    sess.pending_media_notes = []
    task = asyncio.create_task(
        run_codex(
            bot,
            sess,
            key,
            prompt,
            item["reqId"],
            image_paths,
            scheduled_job_file=item.get("scheduledJobFile"),
            run_generation=run_generation,
        )
    )
    sess.run_task = task

    def on_done(done_task: asyncio.Task) -> None:
        if sess.run_task is done_task:
            sess.run_task = None
        if sess.run_generation != run_generation:
            return
        if not sess.running and sess.queue:
            resume_session_queue(bot, sess, key, "run completion")

    task.add_done_callback(on_done)


async def run_codex(
    bot: BotState,
    sess: SessionState,
    key: str,
    prompt: str,
    req_id: Optional[str],
    image_paths: list[str],
    allow_fresh_fallback: bool = True,
    scheduled_job_file: Optional[str] = None,
    run_generation: Optional[int] = None,
) -> None:
    semaphore = get_codex_run_semaphore()
    current_task = asyncio.current_task()
    final = "(no output)"
    return_code = 1
    output_file: Optional[Path] = None
    active_process: Optional[asyncio.subprocess.Process] = None
    process_started = False
    active_schedule_id = str(sess.active_schedule_id or "").strip() or None
    final_delivery_pending = False

    def owns_context() -> bool:
        return session_run_is_current(sess, current_task, run_generation)

    try:
        await send_session_status(bot, key, sess, req_id, build_session_status_text("正在启动处理"))
        async with semaphore:
            while True:
                if not owns_context():
                    return
                if sess.interrupt_requested:
                    sess.interrupt_requested = False
                    sess.running = False
                    sess.proc = None
                    sess.active_schedule_id = None
                    sess.active_scheduled_job_file = None
                    sess.active_schedule_request_id = None
                    sess.last_active = time.time()
                    update_session_record(
                        sess.session_id,
                        lambda record: {**record, "status": "idle", "activeScheduleId": None},
                    )
                    return
                sess.last_active = time.time()
                update_session_record(
                    sess.session_id,
                    lambda record: {**record, "status": "running", "activeScheduleId": active_schedule_id},
                )

                output_file = Path(tempfile.gettempdir()) / f"wecom-codex-py-{sess.session_id}-{int(time.time() * 1000)}.txt"
                attempted_resume = bool(sess.thread_id)
                if attempted_resume:
                    args = build_codex_base_args(output_file, image_paths, resume=True)
                    args.extend([sess.thread_id, "-"])
                else:
                    args = build_codex_base_args(output_file, image_paths, resume=False)
                    args.append("-")

                workspace_paths = ensure_session_workspace_dirs(bot, key)
                codex_home = build_codex_home_for_subprocess(sess.session_id)
                env = dict(os.environ)
                env.update(
                    {
                        "TERM": "xterm-256color",
                        "CODEX_HOME": str(codex_home),
                        "WECOM_LOCAL_FILE_SEND_COMMAND": str(LOCAL_FILE_SEND_COMMAND),
                        "LOCAL_FILE_SEND_QUEUE_ROOT": str(LOCAL_FILE_SEND_QUEUE_ROOT),
                        "WECOM_BRIDGE_API_BASE": BRIDGE_API_BASE,
                        "WECOM_BRIDGE_BOT_NAME": str(bot.config["name"]),
                        "WECOM_BRIDGE_BOT_CONFIG_ID": str(bot.config["id"]),
                        "WECOM_BRIDGE_SESSION_ID": sess.session_id,
                        "WECOM_BRIDGE_CHAT_KEY": key,
                        "WECOM_BRIDGE_WORKDIR_DIR": str(workspace_paths["workDir"]),
                        "WECOM_BRIDGE_CWD_DIR": str(sess.work_dir),
                        "WECOM_BRIDGE_GLOBAL_SKILL_DIR": str(get_session_global_skills_root(sess.session_id)),
                        "WECOM_BRIDGE_WORKSPACE_SKILL_DIR": str(get_workspace_codex_skills_dir(Path(sess.work_dir))),
                        "WECOM_BRIDGE_CHATFILE_DIR": str(workspace_paths["chatfile"]),
                        "WECOM_BRIDGE_EXPORT_DIR": str(workspace_paths["chatfile"]),
                        "WECOM_BRIDGE_WORKFILE_DIR": str(workspace_paths["workfile"] or ""),
                        "WECOM_BRIDGE_ROOMFILE_DIR": str(workspace_paths["roomfile"] or ""),
                        "WECOM_BRIDGE_USER_ID": chat_key_to_user_id(key) or "",
                        "WECOM_BRIDGE_ROOM_ID": chat_key_to_room_id(key) or "",
                        "TMPDIR": str(workspace_paths["chatfile"]),
                        "TMP": str(workspace_paths["chatfile"]),
                        "TEMP": str(workspace_paths["chatfile"]),
                    }
                )

                process = await asyncio.create_subprocess_exec(
                    *args,
                    cwd=sess.work_dir,
                    env=env,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    limit=SUBPROCESS_STREAM_LIMIT,
                )
                active_process = process
                if not owns_context():
                    await terminate_process(process)
                    return
                process_started = True
                sess.proc = process
                sess.codex_runtime_status = None
                add_log(bot, f"[{key}] codex {'resume' if sess.thread_id else 'exec'} started")

                latest_message = ""
                stderr_parts: list[str] = []
                run_started_at = time.monotonic()

                async def ticker() -> None:
                    nonlocal latest_message
                    if not req_id:
                        return
                    next_tick_at = time.monotonic() + STATUS_STREAM_INTERVAL_SEC
                    while sess.running:
                        await asyncio.sleep(max(0, next_tick_at - time.monotonic()))
                        next_tick_at += STATUS_STREAM_INTERVAL_SEC
                        if not sess.running:
                            return
                        elapsed = int(time.monotonic() - run_started_at)
                        if latest_message:
                            content = build_status_stream_content(
                                build_working_status_text(elapsed),
                                latest_message,
                            )
                        else:
                            content = build_thinking_status_text(elapsed)
                        await send_session_status(bot, key, sess, req_id, content)

                async def read_stdout() -> None:
                    nonlocal latest_message
                    assert process.stdout is not None
                    async for line in iter_subprocess_lines(process.stdout, bot, key, "stdout"):
                        try:
                            event = json.loads(line.strip())
                        except Exception:
                            continue
                        if event.get("type") == "thread.started" and event.get("thread_id"):
                            sess.thread_id = event["thread_id"]
                            update_session_record(sess.session_id, lambda record: {**record, "threadId": event["thread_id"]})
                        if event.get("type") == "item.completed" and ((event.get("item") or {}).get("type") == "agent_message"):
                            text = ((event.get("item") or {}).get("text") or "").strip()
                            if text:
                                latest_message = ANSI_RE.sub("", text)

                async def read_stderr() -> None:
                    assert process.stderr is not None
                    async for line in iter_subprocess_lines(process.stderr, bot, key, "stderr"):
                        text = line.strip()
                        if not text or text == "Reading additional input from stdin...":
                            continue
                        runtime_status = detect_codex_runtime_status(text)
                        if runtime_status:
                            update_codex_runtime_status(bot, sess, key, runtime_status, text)
                        stderr_parts.append(text)
                        add_log(bot, f"[{key}] codex stderr: {text}")

                ticker_task = asyncio.create_task(ticker())
                stdout_task = asyncio.create_task(read_stdout())
                stderr_task = asyncio.create_task(read_stderr())
                prompt_write_error: Optional[Exception] = None
                try:
                    await send_session_status(bot, key, sess, req_id, build_thinking_status_text(0))
                    try:
                        await write_process_prompt(process, prompt)
                    except Exception as exc:
                        prompt_write_error = exc
                    return_code = await process.wait()
                    await stdout_task
                    await stderr_task
                finally:
                    ticker_task.cancel()
                    try:
                        await ticker_task
                    except asyncio.CancelledError:
                        pass
                    for stream_task in (stdout_task, stderr_task):
                        if not stream_task.done():
                            stream_task.cancel()
                        try:
                            await stream_task
                        except asyncio.CancelledError:
                            pass

                stderr_text = "\n".join(stderr_parts)
                if prompt_write_error is not None and return_code == 0:
                    raise prompt_write_error
                if (
                    return_code != 0
                    and allow_fresh_fallback
                    and attempted_resume
                    and re.search(
                        r"no rollout found|no prompt provid\w* via stdin",
                        "\n".join(part for part in (stderr_text, str(prompt_write_error or "")) if part),
                        re.I,
                    )
                ):
                    add_log(bot, f"[{key}] codex resume state missing, fallback to fresh exec")
                    sess.proc = None
                    sess.thread_id = None
                    update_session_record(
                        sess.session_id,
                        lambda record: {
                            **record,
                            "threadId": None,
                            "status": "starting",
                            "activeScheduleId": active_schedule_id,
                        },
                    )
                    try:
                        output_file.unlink()
                    except FileNotFoundError:
                        pass
                    output_file = None
                    allow_fresh_fallback = False
                    process_started = False
                    continue
                if prompt_write_error is not None:
                    raise prompt_write_error

                final = latest_message
                if not final and output_file.exists():
                    final = output_file.read_text("utf-8", errors="ignore").strip()
                try:
                    output_file.unlink()
                except FileNotFoundError:
                    pass
                output_file = None
                if not final:
                    final = stderr_text or "(no output)"

                final = limit_stream_content(final)
                break

        if not owns_context():
            return
        if sess.interrupt_requested:
            sess.interrupt_requested = False
            sess.running = False
            sess.proc = None
            sess.active_schedule_id = None
            sess.active_scheduled_job_file = None
            sess.active_schedule_request_id = None
            sess.codex_runtime_status = None
            sess.last_active = time.time()
            restore_session_active_media(sess)
            update_session_record(
                sess.session_id,
                lambda record: {**record, "status": "idle", "activeScheduleId": None},
            )
            resume_session_queue(bot, sess, key, "interrupt")
            return

        if not owns_context():
            return
        sess.running = False
        sess.proc = None
        sess.active_schedule_id = None
        sess.active_scheduled_job_file = None
        sess.active_schedule_request_id = None
        sess.codex_runtime_status = None
        sess.last_active = time.time()

        final_payload = build_session_text_payload(key, sess, req_id, final, True)
        if final_payload:
            final_delivery_pending = not await send_or_store_session_payload(bot, key, sess, final_payload, True)
        add_log(bot, f"[{key}] reply completed ({len(final)} chars, exit={return_code})")
        sess.chat.append({"role": "bot", "text": final, "time": int(time.time())})
        if len(sess.chat) > 200:
            del sess.chat[: len(sess.chat) - 200]
        update_session_record(
            sess.session_id,
            lambda record: {
                **record,
                "threadId": sess.thread_id or record.get("threadId"),
                "status": "idle" if return_code == 0 else "error",
                "activeScheduleId": None,
                "lastRunAt": now_ms(),
            },
        )
        if return_code == 0:
            sess.active_run_media = []
            sess.active_run_media_notes = []
        else:
            restore_session_active_media(sess)
        if return_code == 0 and process_started and scheduled_job_file:
            finalize_scheduled_message_job(Path(scheduled_job_file), True)
            maybe_cleanup_schedule_definition(active_schedule_id)
    except Exception as exc:
        if sess.interrupt_requested:
            sess.interrupt_requested = False
            if output_file is not None:
                try:
                    output_file.unlink()
                except FileNotFoundError:
                    pass
            await terminate_process(active_process)
            if not owns_context():
                return
            sess.running = False
            sess.proc = None
            sess.active_schedule_id = None
            sess.active_scheduled_job_file = None
            sess.active_schedule_request_id = None
            sess.codex_runtime_status = None
            sess.pending_stream_payload = None
            sess.pending_final_payload = None
            sess.last_active = time.time()
            restore_session_active_media(sess)
            update_session_record(
                sess.session_id,
                lambda record: {**record, "status": "idle", "activeScheduleId": None},
            )
            resume_session_queue(bot, sess, key, "interrupt")
            return
        if output_file is not None:
            try:
                output_file.unlink()
            except FileNotFoundError:
                pass
        await terminate_process(active_process)
        if not owns_context():
            return
        sess.running = False
        sess.proc = None
        sess.active_schedule_id = None
        sess.active_scheduled_job_file = None
        sess.active_schedule_request_id = None
        sess.codex_runtime_status = None
        sess.last_active = time.time()
        sess.pending_stream_payload = None
        restore_session_active_media(sess)
        error_text = f"Error: {'failed to start Codex' if not process_started else 'Codex run failed'}: {exc}"
        add_log(bot, f"[{key}] codex task failed: {exc}")
        try:
            error_payload = build_session_text_payload(key, sess, req_id, error_text, True)
            if error_payload:
                final_delivery_pending = not await send_or_store_session_payload(bot, key, sess, error_payload, True)
        except Exception as send_exc:
            add_log(bot, f"[{key}] failed to report codex error: {send_exc}")
        sess.chat.append({"role": "bot", "text": error_text, "time": int(time.time())})
        if len(sess.chat) > 200:
            del sess.chat[: len(sess.chat) - 200]
        try:
            update_session_record(
                sess.session_id,
                lambda record: {
                    **record,
                    "threadId": sess.thread_id or record.get("threadId"),
                    "status": "error",
                    "activeScheduleId": None,
                    "lastRunAt": now_ms(),
                },
            )
        except Exception as update_exc:
            add_log(bot, f"[{key}] failed to persist codex error status: {update_exc}")
    except asyncio.CancelledError:
        if output_file is not None:
            try:
                output_file.unlink()
            except FileNotFoundError:
                pass
        await terminate_process(active_process)
        if not owns_context():
            raise
        sess.running = False
        sess.proc = None
        sess.active_schedule_id = None
        sess.active_scheduled_job_file = None
        sess.active_schedule_request_id = None
        sess.codex_runtime_status = None
        sess.last_active = time.time()
        restore_session_active_media(sess)
        update_session_record(
            sess.session_id,
            lambda record: {**record, "status": "idle", "activeScheduleId": None},
        )
        if sess.interrupt_requested:
            sess.interrupt_requested = False
            return
        raise
    finally:
        if sess.run_task is current_task:
            sess.run_task = None

    if final_delivery_pending:
        add_log(bot, f"[{key}] queue paused until cached final reply is delivered")
        return
    resume_session_queue(bot, sess, key, "run completion")


def recycle_session(bot: BotState, sess: SessionState, key: str) -> None:
    add_log(bot, f"[{key}] idle recycle")
    if sess.proc and sess.proc.returncode is None:
        sess.proc.kill()
    sess.running = False
    sess.interrupt_requested = False
    sess.active_schedule_id = None
    sess.active_scheduled_job_file = None
    sess.active_schedule_request_id = None
    sess.codex_runtime_status = None
    recover_session_scheduled_jobs(sess)
    sess.queue.clear()
    sess.pending_stream_payload = None
    sess.pending_final_payload = None
    sess.active_run_media = []
    sess.active_run_media_notes = []
    cleanup_session_reply_contexts(bot, sess)
    release_session_lease(bot, key, sess)
    update_session_record(sess.session_id, lambda record: {**record, "status": "idle", "activeScheduleId": None})
    bot.sessions.pop(key, None)


def interrupt_session(
    bot: BotState,
    key: str,
    sess: SessionState,
    clear_thread: bool,
    clear_chat: bool,
    clear_queue: bool,
) -> None:
    add_log(bot, f"[{key}] session interrupted")
    active_task = sess.run_task
    active_proc = sess.proc
    had_active_run = bool(sess.running or (active_task and not active_task.done()) or (active_proc and active_proc.returncode is None))
    sess.interrupt_requested = had_active_run
    if had_active_run:
        sess.run_generation += 1
    if active_task and not active_task.done():
        active_task.cancel()
    if active_proc and active_proc.returncode is None:
        active_proc.kill()
    sess.running = False
    sess.run_task = None
    sess.proc = None
    sess.active_schedule_id = None
    sess.active_scheduled_job_file = None
    sess.active_schedule_request_id = None
    sess.codex_runtime_status = None
    if had_active_run and not clear_queue:
        restore_session_active_media(sess)
    else:
        sess.active_run_media = []
        sess.active_run_media_notes = []
    if clear_queue:
        recover_session_scheduled_jobs(sess)
        sess.queue.clear()
        cancelled_file_jobs = cancel_pending_file_send_requests(
            bot,
            key,
            sess.session_id,
            "session reset before file send",
        )
        if cancelled_file_jobs:
            add_log(bot, f"[{key}] cancelled pending file sends: {cancelled_file_jobs}")
    if clear_chat:
        sess.chat.clear()
    if clear_queue:
        sess.pending_media.clear()
        sess.pending_media_notes.clear()
        sess.pending_media_downloads = 0
    sess.pending_stream_payload = None
    sess.pending_final_payload = None
    cleanup_session_reply_contexts(bot, sess)
    if clear_thread:
        sess.thread_id = None
    sess.last_active = time.time()
    keep_lease = sess.lease_owned and not clear_queue and bool(sess.queue)
    if keep_lease:
        current_lease = read_lease(sess.lock_file) if sess.lock_file else None
        lease_expires_at = current_lease.get("expiresAt") if isinstance(current_lease, dict) else None
        update_session_record(
            sess.session_id,
            lambda record: {
                **record,
                "threadId": None if clear_thread else (sess.thread_id or record.get("threadId")),
                "ownerInstance": INSTANCE_ID,
                "ownerPid": os.getpid(),
                "leaseExpiresAt": lease_expires_at or record.get("leaseExpiresAt"),
                "status": "leased",
                "activeScheduleId": None,
                "lastRunAt": now_ms(),
            },
        )
    else:
        release_session_lease(bot, key, sess)
        update_session_record(
            sess.session_id,
            lambda record: {
                **record,
                "threadId": None if clear_thread else (sess.thread_id or record.get("threadId")),
                "ownerInstance": None,
                "ownerPid": None,
                "leaseExpiresAt": None,
                "status": "idle",
                "activeScheduleId": None,
                "lastRunAt": now_ms(),
            },
        )


async def reset_session_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    sess = bot.sessions.get(key)
    control_session(bot, key, clear_thread=True, clear_chat=True)
    if sess:
        clear_resume_selection(sess)
        register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, "Session reset.")


async def interrupt_session_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    sess = bot.sessions.get(key)
    control_session(bot, key, clear_thread=False, clear_chat=False)
    if sess:
        clear_resume_selection(sess)
        register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, "Current task interrupted.")
    if sess:
        resume_session_queue(bot, sess, key, "interrupt command")


def count_scheduled_messages_for_key(bot_id: str, key: str) -> int:
    total = 0
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT):
        for pending_file in root.glob("*.json"):
            job = read_json_file(pending_file, None)
            if (
                isinstance(job, dict)
                and str(job.get("botId") or "") == bot_id
                and str(job.get("chatKey") or "") == key
            ):
                total += 1
    return total


RESUME_SELECTION_TTL_MS = 5 * 60 * 1000


def session_records_for_bot(bot_id: str) -> list[dict[str, Any]]:
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if not sessions_root.exists():
        return []
    records: list[dict[str, Any]] = []
    for session_file in sessions_root.glob("*.json"):
        record = normalize_session_record(read_json_file(session_file, None))
        if not record:
            continue
        if str(record.get("botId") or "") != bot_id:
            continue
        records.append(record)
    records.sort(
        key=lambda item: (
            int(item.get("lastRunAt") or 0),
            int(item.get("updatedAt") or 0),
            int(item.get("createdAt") or 0),
            str(item["sessionId"]),
        ),
        reverse=True,
    )
    return records


def resume_record_is_visible_to_chat(target_key: str, current_key: str) -> bool:
    if target_key == current_key:
        return True
    target_user_id = chat_key_to_user_id(target_key)
    current_user_id = chat_key_to_user_id(current_key)
    if target_user_id and current_user_id:
        return target_user_id == current_user_id
    if target_key.startswith("group:") and current_key.startswith("group:"):
        return chat_key_to_room_id(target_key) == chat_key_to_room_id(current_key)
    return False


def build_resume_candidates(bot: BotState, key: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen_session_ids: set[str] = set()
    for record in session_records_for_bot(bot.config["id"]):
        session_id = str(record["sessionId"])
        thread_id = str(record.get("threadId") or "").strip()
        candidate_key = str(record.get("chatKey") or "").strip()
        if not thread_id or not candidate_key or session_id in seen_session_ids:
            continue
        if not resume_record_is_visible_to_chat(candidate_key, key):
            continue
        seen_session_ids.add(session_id)
        candidates.append(
            {
                "sessionId": session_id,
                "threadId": thread_id,
                "chatKey": candidate_key,
                "updatedAt": int(record.get("updatedAt") or record.get("createdAt") or 0),
                "lastRunAt": int(record.get("lastRunAt") or 0),
                "status": str(record.get("status") or "idle"),
            }
        )
    return candidates


def format_resume_candidate_line(index: int, candidate: dict[str, Any]) -> str:
    ts = int(candidate.get("lastRunAt") or candidate.get("updatedAt") or 0)
    ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts / 1000)) if ts > 0 else "-"
    status = str(candidate.get("status") or "idle")
    return f"{index}. {candidate['sessionId']}  {ts_text}  status={status}  chatKey={candidate['chatKey']}"


def build_resume_candidates_message(candidates: list[dict[str, Any]]) -> str:
    lines = ["检测到以下可恢复会话，请回复编号或直接回复 /bridge-resume <sessionId>："]
    for index, candidate in enumerate(candidates, start=1):
        lines.append(format_resume_candidate_line(index, candidate))
    lines.append("回复“取消”可退出恢复选择。")
    return "\n".join(lines)


def clear_resume_selection(sess: SessionState) -> None:
    sess.resume_candidates.clear()
    sess.resume_selection_expires_at = 0


def resume_selection_active(sess: SessionState) -> bool:
    if not sess.resume_candidates:
        return False
    if sess.resume_selection_expires_at <= now_ms():
        clear_resume_selection(sess)
        return False
    return True


def select_resume_candidate(sess: SessionState, token: str) -> Optional[dict[str, Any]]:
    text = str(token or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d+", text):
        index = int(text)
        if 1 <= index <= len(sess.resume_candidates):
            return sess.resume_candidates[index - 1]
        return None
    for candidate in sess.resume_candidates:
        if text == str(candidate.get("sessionId") or "").strip():
            return candidate
    return None


def bind_resume_candidate_to_session(
    bot: BotState,
    sess: SessionState,
    key: str,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    thread_id = str(candidate.get("threadId") or "").strip()
    if not thread_id:
        raise BridgeError(404, "selected session is no longer resumable")
    target_record = read_session_record_by_id(str(candidate["sessionId"]))
    if not target_record:
        raise BridgeError(404, f"session not found: {candidate['sessionId']}")
    target_thread_id = str(target_record.get("threadId") or "").strip()
    if not target_thread_id:
        raise BridgeError(404, "selected session no longer has a resumable thread")
    sess.thread_id = target_thread_id
    update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "threadId": target_thread_id,
            "lastRunAt": now_ms(),
        },
    )
    clear_resume_selection(sess)
    add_log(
        bot,
        f"[{key}] bound resume thread from session={target_record['sessionId']} chatKey={target_record['chatKey']}",
    )
    return {
        "sourceSessionId": target_record["sessionId"],
        "sourceChatKey": target_record["chatKey"],
        "threadId": target_thread_id,
    }


async def start_resume_selection_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    sess = get_or_create_session(bot, key)
    candidates = build_resume_candidates(bot, key)
    if not candidates:
        clear_resume_selection(sess)
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, "没有可恢复的会话。")
        return
    sess.resume_candidates = candidates
    sess.resume_selection_expires_at = now_ms() + RESUME_SELECTION_TTL_MS
    register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, build_resume_candidates_message(candidates))


async def apply_resume_selection_command(
    bot: BotState,
    key: str,
    req_id: Optional[str],
    token: str,
) -> bool:
    sess = get_or_create_session(bot, key)
    if not resume_selection_active(sess):
        clear_resume_selection(sess)
        return False
    text = str(token or "").strip()
    if text in {"取消", "cancel", "Cancel", "CANCEL"}:
        clear_resume_selection(sess)
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, "已取消恢复选择。")
        return True
    candidate = select_resume_candidate(sess, text)
    if not candidate:
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, "无效选择，请回复列表编号、sessionId，或回复“取消”。")
        return True
    bound = bind_resume_candidate_to_session(bot, sess, key, candidate)
    register_reply_session(bot, req_id, sess)
    await respond_info(
        bot,
        req_id,
        f"已选择会话 {bound['sourceSessionId']}，接下来会继续该上下文。",
    )
    return True


async def resume_session_command(bot: BotState, key: str, req_id: Optional[str], session_id: str) -> None:
    sess = get_or_create_session(bot, key)
    record = read_session_record_by_id(session_id)
    if not record:
        raise BridgeError(404, f"session not found: {session_id}")
    candidate_key = str(record.get("chatKey") or "").strip()
    if not resume_record_is_visible_to_chat(candidate_key, key):
        raise BridgeError(403, "session is not resumable from current chat")
    candidate = {
        "sessionId": record["sessionId"],
        "chatKey": candidate_key,
        "threadId": str(record.get("threadId") or "").strip(),
        "updatedAt": int(record.get("updatedAt") or record.get("createdAt") or 0),
        "lastRunAt": int(record.get("lastRunAt") or 0),
        "status": str(record.get("status") or "idle"),
    }
    bound = bind_resume_candidate_to_session(bot, sess, key, candidate)
    register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, f"已选择会话 {bound['sourceSessionId']}，接下来会继续该上下文。")


async def status_session_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    sess = bot.sessions.get(key)
    record = read_session_record_by_key(bot.config["id"], key)
    scheduled_count = count_scheduled_messages_for_key(bot.config["id"], key)
    if sess:
        status_text = (
            f"status={('running' if sess.running else 'idle')}; "
            f"queue={len(sess.queue)}; "
            f"scheduled={scheduled_count}; "
            f"sessionId={sess.session_id}; "
            f"threadId={sess.thread_id or '-'}"
        )
    elif record:
        status_text = (
            f"status={record.get('status', 'idle')}; "
            f"queue=0; "
            f"scheduled={scheduled_count}; "
            f"sessionId={record['sessionId']}; "
            f"threadId={record.get('threadId') or '-'}"
        )
    else:
        status_text = f"status=empty; queue=0; scheduled={scheduled_count}; sessionId=-; threadId=-"
    if sess:
        register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, status_text)


def ensure_session_control_safe(record: dict[str, Any]) -> None:
    lease_expires_at = int(record.get("leaseExpiresAt") or 0)
    owner_instance = str(record.get("ownerInstance") or "").strip()
    lock_file = Path(record["lockFile"])
    lease = read_lease(lock_file)
    now = now_ms()

    if isinstance(lease, dict):
        lease_owner = str(lease.get("instanceId") or "").strip()
        lease_expires = int(lease.get("expiresAt") or 0)
        if lease_owner and lease_owner != INSTANCE_ID and lease_expires > now:
            raise BridgeError(409, f"session is owned by another instance: {record['chatKey']}")

    if owner_instance and owner_instance != INSTANCE_ID and lease_expires_at > now:
        raise BridgeError(409, f"session is owned by another instance: {record['chatKey']}")


def control_session(bot: BotState, key: str, clear_thread: bool, clear_chat: bool) -> dict[str, Any]:
    sess = bot.sessions.get(key)
    if sess:
        interrupt_session(
            bot,
            key,
            sess,
            clear_thread=clear_thread,
            clear_chat=clear_chat,
            clear_queue=clear_thread or clear_chat,
        )
        return {
            "key": key,
            "sessionId": sess.session_id,
            "threadId": sess.thread_id,
            "status": "idle",
            "clearedThread": clear_thread,
            "clearedChat": clear_chat,
            "live": True,
        }

    record = read_session_record_by_key(bot.config["id"], key)
    if not record:
        raise BridgeError(404, f"session not found: {key}")
    ensure_session_control_safe(record)

    update_session_record(
        record["sessionId"],
        lambda current: {
            **current,
            "threadId": None if clear_thread else current.get("threadId"),
            "ownerInstance": None,
            "ownerPid": None,
            "leaseExpiresAt": None,
            "status": "idle",
            "activeScheduleId": None,
            "lastRunAt": now_ms(),
        },
    )
    lock_file = Path(record["lockFile"])
    try:
        lock_file.unlink()
    except FileNotFoundError:
        pass
    return {
        "key": key,
        "sessionId": record["sessionId"],
        "threadId": None if clear_thread else record.get("threadId"),
        "status": "idle",
        "clearedThread": clear_thread,
        "clearedChat": clear_chat,
        "live": False,
    }


def can_continue_session_queue(sess: SessionState) -> bool:
    return bool(sess.lease_owned or not sess.queue)


def is_path_inside(file_path: Path, root_path: Path) -> bool:
    try:
        file_path.resolve().relative_to(root_path.resolve())
        return True
    except Exception:
        return False


def get_allowed_file_roots(bot: BotState, key: str) -> list[Path]:
    workspace_paths = ensure_session_workspace_dirs(bot, key)
    roots = [workspace_paths["chatfile"].resolve(), *EXTRA_FILE_ROOTS]
    deduped = []
    for root in roots:
        if root not in deduped:
            deduped.append(root)
    return deduped


def validate_file_for_upload(bot: BotState, key: str, file_path: str) -> Path:
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.exists():
        raise BridgeError(404, f"file not found: {resolved}")
    if not resolved.is_file():
        raise BridgeError(400, f"not a regular file: {resolved}")
    size = resolved.stat().st_size
    if size > MAX_UPLOAD_SIZE:
        raise BridgeError(413, f"file too large: {size} bytes (max {MAX_UPLOAD_SIZE})")
    allowed_roots = get_allowed_file_roots(bot, key)
    if not any(is_path_inside(resolved, root) for root in allowed_roots):
        allowed = ", ".join(str(root) for root in allowed_roots)
        raise BridgeError(403, f"filePath is outside allowed roots: {allowed}")
    return resolved


def require_unique_bot_for_chat_key(chat_key_value: str, bot_name: Optional[str]) -> BotState:
    matches = [
        bot
        for bot in BOTS.values()
        if not bot_instance_is_stale(bot)
        and bot.config.get("enabled", True) is not False
        and bot_has_chat_key(bot, chat_key_value)
    ]
    if bot_name:
        for bot in matches:
            if bot.config["name"] == bot_name:
                return bot
        raise BridgeError(404, f"bot not found or chatKey not in bot sessions: {bot_name}")
    if not matches:
        raise BridgeError(404, f"chatKey not found: {chat_key_value}")
    if len(matches) > 1:
        names = ", ".join(sorted(bot.config["name"] for bot in matches))
        raise BridgeError(409, f"chatKey matches multiple bots; provide botName or sessionId: {chat_key_value} ({names})")
    return matches[0]


def resolve_loaded_target_bot(target_config_id: Optional[str], bot_name: Optional[str]) -> Optional[BotState]:
    if not target_config_id:
        return None
    bot = BOTS.get(target_config_id)
    if bot:
        if bot_instance_is_stale(bot):
            raise BridgeError(404, f"bot not found: {target_config_id}")
        if bot.config.get("enabled", True) is False:
            raise BridgeError(503, f"bot disabled: {target_config_id}")
    else:
        persisted = read_persisted_bot_config(target_config_id)
        if persisted:
            if persisted.get("enabled", True) is False:
                raise BridgeError(503, f"bot disabled: {target_config_id}")
            raise BridgeError(503, f"bot not running: {target_config_id}")
        raise BridgeError(404, f"bot not found: {target_config_id}")
    if bot_name and bot.config["name"] != bot_name:
        raise BridgeError(409, f"bot name mismatch for target bot: {target_config_id} ({bot.config['name']} != {bot_name})")
    return bot


def resolve_schedule_target_bot_metadata(target_config_id: Optional[str], bot_name: Optional[str]) -> dict[str, str] | None:
    if not target_config_id:
        return None
    config = get_authoritative_bot_config(target_config_id)
    if not config:
        raise BridgeError(404, f"bot not found: {target_config_id}")
    if config.get("enabled", True) is False:
        raise BridgeError(503, f"bot disabled: {target_config_id}")
    actual_name = str(config.get("name") or "").strip()
    if bot_name and actual_name != bot_name:
        raise BridgeError(409, f"bot name mismatch for target bot: {target_config_id} ({actual_name} != {bot_name})")
    return {"botId": str(config["id"]), "botName": actual_name}


def resolve_file_send_request(data: dict[str, Any]) -> tuple[BotState, str, Path]:
    file_path = data.get("filePath") or data.get("file_path")
    chat_key_value = data.get("chatKey") or data.get("chat_key")
    bot_name = data.get("botName") or data.get("bot_name")
    target_config_id = str(data.get("targetConfigId") or data.get("target_config_id") or "").strip() or None
    session_id = data.get("sessionId") or data.get("session_id")
    if not file_path:
        raise BridgeError(400, "filePath required")
    last_error: Optional[BridgeError] = None

    if chat_key_value:
        try:
            validated_chat_key = validate_chat_key(chat_key_value)
            if target_config_id:
                bot = resolve_loaded_target_bot(target_config_id, bot_name)
            else:
                bot = require_unique_bot_for_chat_key(validated_chat_key, bot_name)
            if not bot.ws or bot.ws.closed:
                raise BridgeError(503, "bot not connected")
            resolved_file = validate_file_for_upload(bot, validated_chat_key, file_path)
            return bot, validated_chat_key, resolved_file
        except BridgeError as exc:
            last_error = exc
            if not session_id:
                raise

    if session_id:
        try:
            record = read_session_record_by_id(session_id)
            if not record:
                raise BridgeError(404, f"session not found: {session_id}")
            if target_config_id and str(record["botId"]) != target_config_id:
                raise BridgeError(409, f"bot config mismatch for file send target: {target_config_id}")
            resolved_target_config_id = target_config_id or str(record["botId"])
            bot = resolve_loaded_target_bot(resolved_target_config_id, bot_name)
            resolved_chat_key = record["chatKey"]
            if not bot.ws or bot.ws.closed:
                raise BridgeError(503, "bot not connected")
            resolved_file = validate_file_for_upload(bot, resolved_chat_key, file_path)
            return bot, resolved_chat_key, resolved_file
        except BridgeError as exc:
            if last_error is None:
                raise
            raise exc

    if last_error is not None:
        raise last_error

    raise BridgeError(400, "chatKey or sessionId required")


def submit_file_send_request(data: dict[str, Any], source: str = "api") -> dict[str, Any]:
    bot, resolved_chat_key, resolved_file = resolve_file_send_request(data)
    local_request_id = str(data.get("localRequestId") or data.get("local_request_id") or "").strip() or None
    local_processing_file = str(data.get("localProcessingFile") or data.get("local_processing_file") or "").strip() or None
    target_config_id = str(data.get("targetConfigId") or data.get("target_config_id") or "").strip() or None
    if local_request_id:
        bot.active_local_file_request_ids.add(local_request_id)
    bot.upload_queue.put_nowait(
        {
            "id": uid(),
            "chatKey": resolved_chat_key,
            "filePath": str(resolved_file),
            "targetConfigId": target_config_id,
            "localRequestId": local_request_id,
            "localProcessingFile": local_processing_file,
        }
    )
    queue_depth = bot.upload_queue.qsize()
    add_log(bot, f"[{resolved_chat_key}] file queued: {resolved_file.name} (queue={queue_depth})")
    add_log(bot, f"[{resolved_chat_key}] file request accepted via {source}: {resolved_file.name}")
    add_event_log(
        bot,
        "file.queue_accepted",
        chatKey=resolved_chat_key,
        fileName=resolved_file.name,
        source=source,
        queueDepth=queue_depth,
    )
    return {"ok": True, "message": f"queued {resolved_file.name}", "queueDepth": queue_depth}


def write_local_file_send_result(request_id: str, payload: dict[str, Any], queue_paths: Optional[dict[str, Path]] = None) -> None:
    paths = queue_paths or get_local_file_send_queue_paths()
    ensure_dir(paths["results"])
    retain_until = normalize_optional_int(payload.get("retainUntil") or payload.get("retain_until"))
    write_json_atomic(
        paths["results"] / f"{request_id}.json",
        {
            **payload,
            "processedAt": now_ms(),
            "retainUntil": retain_until,
        },
    )


def finalize_local_file_send_job(processing_file: Path, ok: bool) -> None:
    queue_paths = get_local_file_send_queue_paths_for_job_file(processing_file)
    target_dir = queue_paths["done"] if ok else queue_paths["failed"]
    ensure_dir(target_dir)
    try:
        processing_file.replace(target_dir / processing_file.name)
    except Exception:
        try:
            processing_file.unlink()
        except FileNotFoundError:
            pass


def parse_timestamp_ms(value: Any, label: str) -> int:
    if value in (None, ""):
        raise BridgeError(400, f"{label} required")
    if isinstance(value, (int, float)):
        raw = float(value)
        return int(raw * 1000) if raw < 10_000_000_000 else int(raw)
    text = str(value).strip()
    if not text:
        raise BridgeError(400, f"{label} required")
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        raw = float(text)
        return int(raw * 1000) if raw < 10_000_000_000 else int(raw)
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise BridgeError(400, f"invalid {label} format") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def parse_optional_timestamp_ms(value: Any, label: str) -> Optional[int]:
    if value in (None, ""):
        return None
    return parse_timestamp_ms(value, label)


def parse_schedule_run_at(run_at: Any, delay_seconds: Any) -> int:
    if delay_seconds not in (None, ""):
        try:
            seconds = float(delay_seconds)
        except (TypeError, ValueError) as exc:
            raise BridgeError(400, "invalid delaySeconds") from exc
        if seconds < 0:
            raise BridgeError(400, "delaySeconds must be >= 0")
        run_at_ms = now_ms() + int(seconds * 1000)
    else:
        run_at_ms = parse_timestamp_ms(run_at, "runAt")
    if run_at_ms <= now_ms():
        raise BridgeError(400, "runAt must be in the future")
    return run_at_ms


def get_schedule_definition_file(schedule_id: str) -> Path:
    return SCHEDULE_DEFINITION_ROOT / f"{quote(schedule_id, safe='')}.json"


def get_schedule_definition_lock_file(schedule_id: str) -> Path:
    return SCHEDULE_DEFINITION_LOCK_ROOT / f"{quote(schedule_id, safe='')}.lock.json"


def normalize_schedule_definition(record: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not record:
        return None

    def as_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        return int(value)

    mode = str(record.get("mode") or "").strip().lower()
    if mode != "cron":
        return None
    return {
        "scheduleId": str(record["scheduleId"]),
        "botId": str(record["botId"]),
        "botName": str(record.get("botName") or "").strip() or None,
        "sessionId": str(record.get("sessionId") or "").strip() or None,
        "chatKey": str(record["chatKey"]),
        "message": str(record["message"]),
        "mode": mode,
        "cron": str(record.get("cron") or "").strip() or None,
        "timezone": str(record.get("timezone") or "UTC").strip() or "UTC",
        "startAt": as_int(record.get("startAt")),
        "endAt": as_int(record.get("endAt")),
        "maxRuns": as_int(record.get("maxRuns")),
        "runCount": int(record.get("runCount") or 0),
        "enabled": record.get("enabled", True) is not False,
        "nextRunAt": as_int(record.get("nextRunAt")),
        "lastPlannedAt": as_int(record.get("lastPlannedAt")),
        "lastTriggeredAt": as_int(record.get("lastTriggeredAt")),
        "lastFinishedAt": as_int(record.get("lastFinishedAt")),
        "misfirePolicy": str(record.get("misfirePolicy") or "fire_once_now").strip() or "fire_once_now",
        "concurrencyPolicy": str(record.get("concurrencyPolicy") or "skip_if_running").strip() or "skip_if_running",
        "autoDeleteOnDone": record.get("autoDeleteOnDone", False) is True,
        "createdAt": int(record["createdAt"]),
        "updatedAt": int(record.get("updatedAt") or record["createdAt"]),
    }


def write_schedule_definition(record: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_schedule_definition(record)
    if not normalized:
        raise BridgeError(500, "invalid schedule definition")
    normalized["updatedAt"] = now_ms()
    write_json_atomic(get_schedule_definition_file(normalized["scheduleId"]), normalized)
    return normalized


def create_schedule_definition(record: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_schedule_definition(record)
    if not normalized:
        raise BridgeError(500, "invalid schedule definition")
    schedule_id = normalized["scheduleId"]
    if not acquire_schedule_definition_lock(schedule_id):
        raise BridgeError(409, f"schedule is being modified by another instance: {schedule_id}")
    try:
        if read_schedule_definition(schedule_id):
            raise BridgeError(409, f"scheduleId already exists: {schedule_id}")
        normalized["updatedAt"] = now_ms()
        write_json_atomic(get_schedule_definition_file(schedule_id), normalized)
        return normalized
    finally:
        release_schedule_definition_lock(schedule_id)


def read_schedule_definition(schedule_id: str) -> Optional[dict[str, Any]]:
    return normalize_schedule_definition(read_json_file(get_schedule_definition_file(schedule_id), None))


def update_schedule_definition(schedule_id: str, updater: Any) -> Optional[dict[str, Any]]:
    current = read_schedule_definition(schedule_id)
    if not current:
        return None
    next_record = updater(dict(current)) if updater else current
    if not next_record:
        next_record = current
    return write_schedule_definition(next_record)


def list_schedule_definitions() -> list[dict[str, Any]]:
    ensure_schedule_dirs()
    payload: list[dict[str, Any]] = []
    for definition_file in sorted(SCHEDULE_DEFINITION_ROOT.glob("*.json")):
        definition = normalize_schedule_definition(read_json_file(definition_file, None))
        if definition:
            payload.append(definition)
    return payload


def resolve_schedule_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise BridgeError(400, f"invalid timezone: {name}") from exc


def parse_cron_atom(token: str, names: dict[str, int], minimum: int, maximum: int, label: str) -> int:
    upper = token.strip().upper()
    if upper in names:
        value = names[upper]
    else:
        try:
            value = int(token)
        except ValueError as exc:
            raise BridgeError(400, f"invalid {label} cron field: {token}") from exc
    if label == "day-of-week" and value == 7:
        value = 0
    if value < minimum or value > maximum:
        raise BridgeError(400, f"{label} cron field out of range: {token}")
    return value


def parse_cron_field(
    expr: str,
    minimum: int,
    maximum: int,
    label: str,
    names: Optional[dict[str, int]] = None,
    allow_question: bool = False,
) -> tuple[set[int], bool]:
    text = expr.strip()
    if allow_question and text == "?":
        text = "*"
    if not text:
        raise BridgeError(400, f"empty {label} cron field")
    wildcard = text == "*"
    values: set[int] = set()
    name_map = names or {}

    for part in text.split(","):
        item = part.strip()
        if not item:
            raise BridgeError(400, f"invalid {label} cron field")
        step = 1
        if "/" in item:
            base, step_text = item.split("/", 1)
            try:
                step = int(step_text)
            except ValueError as exc:
                raise BridgeError(400, f"invalid {label} cron step: {item}") from exc
            if step <= 0:
                raise BridgeError(400, f"invalid {label} cron step: {item}")
        else:
            base = item

        if base == "*":
            start = minimum
            end = maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start = parse_cron_atom(start_text, name_map, minimum, maximum, label)
            end = parse_cron_atom(end_text, name_map, minimum, maximum, label)
            if end < start:
                raise BridgeError(400, f"invalid {label} cron range: {item}")
        else:
            start = parse_cron_atom(base, name_map, minimum, maximum, label)
            end = start

        for value in range(start, end + 1, step):
            if label == "day-of-week" and value == 7:
                value = 0
            values.add(value)

    return values, wildcard


def parse_cron_expression(expr: str) -> dict[str, Any]:
    parts = [part for part in str(expr or "").split() if part]
    if len(parts) != 5:
        raise BridgeError(400, "cron must contain exactly 5 fields")
    minutes, _ = parse_cron_field(parts[0], 0, 59, "minute")
    hours, _ = parse_cron_field(parts[1], 0, 23, "hour")
    month_days, month_days_any = parse_cron_field(parts[2], 1, 31, "day-of-month", allow_question=True)
    months, _ = parse_cron_field(parts[3], 1, 12, "month", names=CRON_MONTH_NAMES)
    weekdays, weekdays_any = parse_cron_field(parts[4], 0, 7, "day-of-week", names=CRON_WEEKDAY_NAMES, allow_question=True)
    return {
        "minutes": minutes,
        "hours": hours,
        "monthDays": month_days,
        "monthDaysAny": month_days_any,
        "months": months,
        "weekdays": weekdays,
        "weekdaysAny": weekdays_any,
    }


def cron_datetime_matches(dt: datetime, spec: dict[str, Any]) -> bool:
    cron_weekday = (dt.weekday() + 1) % 7
    month_day_match = dt.day in spec["monthDays"]
    weekday_match = cron_weekday in spec["weekdays"]
    if spec["monthDaysAny"] and spec["weekdaysAny"]:
        day_match = True
    elif spec["monthDaysAny"]:
        day_match = weekday_match
    elif spec["weekdaysAny"]:
        day_match = month_day_match
    else:
        day_match = month_day_match or weekday_match
    return (
        dt.minute in spec["minutes"]
        and dt.hour in spec["hours"]
        and dt.month in spec["months"]
        and day_match
    )


def compute_next_cron_run_on_or_after(expr: str, timezone_name: str, earliest_ms: int) -> int:
    spec = parse_cron_expression(expr)
    tzinfo = resolve_schedule_timezone(timezone_name)
    earliest = datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc).astimezone(tzinfo)
    candidate = earliest.replace(second=0, microsecond=0)
    if earliest.second or earliest.microsecond:
        candidate += timedelta(minutes=1)
    limit = candidate + timedelta(days=366 * 5)
    while candidate <= limit:
        if cron_datetime_matches(candidate, spec):
            return int(candidate.astimezone(timezone.utc).timestamp() * 1000)
        candidate += timedelta(minutes=1)
    raise BridgeError(400, "cron has no matching run time within 5 years")


def ceil_timestamp_to_minute_ms(timestamp_ms: int) -> int:
    remainder = timestamp_ms % 60000
    if remainder == 0:
        return timestamp_ms
    return timestamp_ms + (60000 - remainder)


def cron_expression_for_timestamp_ms(timestamp_ms: int, timezone_name: str = "UTC") -> str:
    tzinfo = resolve_schedule_timezone(timezone_name)
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(tzinfo)
    return f"{dt.minute} {dt.hour} {dt.day} {dt.month} *"


def clamp_schedule_next_run(next_run_at: Optional[int], end_at: Optional[int]) -> Optional[int]:
    if next_run_at is None:
        return None
    if end_at is not None and next_run_at > end_at:
        return None
    return next_run_at


def compute_schedule_next_run_on_or_after(definition: dict[str, Any], earliest_ms: int) -> Optional[int]:
    next_run_at = compute_next_cron_run_on_or_after(definition["cron"] or "", definition["timezone"], earliest_ms)
    return clamp_schedule_next_run(next_run_at, definition.get("endAt"))


def validate_schedule_concurrency_policy(policy: str) -> str:
    value = (policy or "skip_if_running").strip() or "skip_if_running"
    if value not in {"skip_if_running", "enqueue"}:
        raise BridgeError(400, f"unsupported concurrencyPolicy: {value}")
    return value


def validate_schedule_misfire_policy(policy: str) -> str:
    value = (policy or "fire_once_now").strip() or "fire_once_now"
    if value not in {"fire_once_now", "skip_missed"}:
        raise BridgeError(400, f"unsupported misfirePolicy: {value}")
    return value


def resolve_schedule_target(data: dict[str, Any]) -> dict[str, Any]:
    session_id = str(data.get("sessionId") or data.get("session_id") or "").strip()
    chat_key_value = str(data.get("chatKey") or data.get("chat_key") or "").strip()
    bot_name = str(data.get("botName") or data.get("bot_name") or "").strip() or None
    target_config_id = str(data.get("targetConfigId") or data.get("target_config_id") or "").strip() or None
    last_error: Optional[BridgeError] = None

    if chat_key_value:
        try:
            validated_chat_key = validate_chat_key(chat_key_value)
            if target_config_id:
                target_meta = resolve_schedule_target_bot_metadata(target_config_id, bot_name)
                session_record = read_session_record_by_key(target_config_id, validated_chat_key)
            else:
                bot = require_unique_bot_for_chat_key(validated_chat_key, bot_name)
                target_meta = {"botId": bot.config["id"], "botName": bot.config["name"]}
                session_record = read_session_record_by_key(bot.config["id"], validated_chat_key)
            return {
                "botId": target_meta["botId"],
                "botName": target_meta["botName"],
                "sessionId": session_record["sessionId"] if session_record else None,
                "chatKey": validated_chat_key,
            }
        except BridgeError as exc:
            last_error = exc
            if not session_id:
                raise

    if session_id:
        try:
            record = read_session_record_by_id(session_id)
            if not record:
                raise BridgeError(404, f"session not found: {session_id}")
            if target_config_id and str(record["botId"]) != target_config_id:
                raise BridgeError(409, f"bot config mismatch for schedule target: {target_config_id}")
            bot_id = str(record["botId"])
            resolved_chat_key = record["chatKey"]
            target_meta = resolve_schedule_target_bot_metadata(target_config_id or bot_id, bot_name)
            return {
                "botId": target_meta["botId"],
                "botName": target_meta["botName"],
                "sessionId": session_id,
                "chatKey": resolved_chat_key,
            }
        except BridgeError as exc:
            if last_error is None:
                raise
            raise exc

    if last_error is not None:
        raise last_error

    raise BridgeError(400, "sessionId or chatKey required")


def build_scheduled_message_job(definition: dict[str, Any], run_at_ms: int) -> dict[str, Any]:
    return {
        "requestId": uid(),
        "scheduleId": definition["scheduleId"],
        "botId": definition["botId"],
        "botName": definition.get("botName"),
        "sessionId": definition.get("sessionId"),
        "chatKey": definition["chatKey"],
        "message": definition["message"],
        "runAt": run_at_ms,
        "createdAt": now_ms(),
    }


def create_schedule_definition_record(data: dict[str, Any], created_at_ms: Optional[int] = None) -> dict[str, Any]:
    message = str(data.get("message") or data.get("text") or "").strip()
    if not message:
        raise BridgeError(400, "message required")

    target = resolve_schedule_target(data)
    created_at = created_at_ms if created_at_ms is not None else now_ms()
    mode = str(data.get("mode") or "").strip().lower()
    cron_expr = str(data.get("cron") or "").strip()
    interval_value = data.get("intervalSeconds") or data.get("interval_seconds") or data.get("everySeconds") or data.get("every_seconds")
    has_cron = bool(cron_expr)
    if interval_value not in (None, ""):
        raise BridgeError(400, "intervalSeconds is no longer supported; use cron")
    if mode not in {"", "cron"}:
        raise BridgeError(400, "mode must be cron")
    if not has_cron:
        raise BridgeError(400, "cron required")

    timezone_name = str(data.get("timezone") or "UTC").strip() or "UTC"
    start_at = parse_optional_timestamp_ms(data.get("startAt") or data.get("start_at"), "startAt")
    end_at = parse_optional_timestamp_ms(data.get("endAt") or data.get("end_at"), "endAt")
    if end_at is not None and end_at <= created_at:
        raise BridgeError(400, "endAt must be in the future")

    max_runs_raw = data.get("maxRuns") or data.get("max_runs")
    max_runs = None
    if max_runs_raw not in (None, ""):
        try:
            max_runs = int(max_runs_raw)
        except (TypeError, ValueError) as exc:
            raise BridgeError(400, "invalid maxRuns") from exc
        if max_runs <= 0:
            raise BridgeError(400, "maxRuns must be > 0")

    misfire_policy = validate_schedule_misfire_policy(str(data.get("misfirePolicy") or data.get("misfire_policy") or "fire_once_now"))
    concurrency_policy = validate_schedule_concurrency_policy(
        str(data.get("concurrencyPolicy") or data.get("concurrency_policy") or "skip_if_running")
    )

    parse_cron_expression(cron_expr)
    resolve_schedule_timezone(timezone_name)
    earliest_ms = max(created_at, start_at or created_at)
    next_run_at = compute_next_cron_run_on_or_after(cron_expr, timezone_name, earliest_ms)

    next_run_at = clamp_schedule_next_run(next_run_at, end_at)
    if next_run_at is None:
        raise BridgeError(400, "schedule has no future run time")

    return normalize_schedule_definition(
        {
            "scheduleId": data.get("scheduleId") or uid(),
            "botId": target["botId"],
            "botName": target.get("botName"),
            "sessionId": target.get("sessionId"),
            "chatKey": target["chatKey"],
            "message": message,
            "mode": "cron",
            "cron": cron_expr or None,
            "timezone": timezone_name,
            "startAt": start_at,
            "endAt": end_at,
            "maxRuns": max_runs,
            "runCount": 0,
            "enabled": True,
            "nextRunAt": next_run_at,
            "lastPlannedAt": None,
            "lastTriggeredAt": None,
            "lastFinishedAt": None,
            "misfirePolicy": misfire_policy,
            "concurrencyPolicy": concurrency_policy,
            "autoDeleteOnDone": data.get("autoDeleteOnDone") is True,
            "createdAt": created_at,
            "updatedAt": created_at,
        }
    ) or {}


def create_one_shot_schedule_definition_record(data: dict[str, Any], created_at_ms: Optional[int] = None) -> tuple[dict[str, Any], int]:
    created_at = created_at_ms if created_at_ms is not None else now_ms()
    requested_run_at = parse_schedule_run_at(data.get("runAt") or data.get("run_at"), data.get("delaySeconds") or data.get("delay_seconds"))
    normalized_run_at = ceil_timestamp_to_minute_ms(requested_run_at)
    schedule_data = {
        **data,
        "mode": "cron",
        "cron": cron_expression_for_timestamp_ms(normalized_run_at, "UTC"),
        "timezone": "UTC",
        "startAt": normalized_run_at,
        "endAt": normalized_run_at + 59999,
        "maxRuns": 1,
        "misfirePolicy": data.get("misfirePolicy") or data.get("misfire_policy") or "fire_once_now",
        "concurrencyPolicy": data.get("concurrencyPolicy") or data.get("concurrency_policy") or "skip_if_running",
        "autoDeleteOnDone": True,
    }
    definition = create_schedule_definition_record(schedule_data, created_at_ms=created_at)
    return definition, requested_run_at


def resolve_schedule_message_request(data: dict[str, Any]) -> dict[str, Any]:
    definition, requested_run_at = create_one_shot_schedule_definition_record(data)
    return {
        "requestId": definition["scheduleId"],
        "scheduleId": definition["scheduleId"],
        "botId": definition["botId"],
        "botName": definition.get("botName"),
        "sessionId": definition.get("sessionId"),
        "chatKey": definition["chatKey"],
        "message": definition["message"],
        "requestedRunAt": requested_run_at,
        "runAt": definition["nextRunAt"],
        "createdAt": definition["createdAt"],
    }


def submit_schedule_message_request(data: dict[str, Any], source: str = "api") -> dict[str, Any]:
    definition, requested_run_at = create_one_shot_schedule_definition_record(data)
    ensure_schedule_dirs()
    stored = create_schedule_definition(definition)
    bot = BOTS.get(stored["botId"])
    if bot:
        add_log(
            bot,
            (
                f"[{stored['chatKey']}] one-shot cron schedule accepted via {source}: "
                f"scheduleId={stored['scheduleId']} runAt={stored['nextRunAt']}"
            ),
        )
    return {
        "ok": True,
        "requestId": stored["scheduleId"],
        "scheduleId": stored["scheduleId"],
        "requestedRunAt": requested_run_at,
        "runAt": stored["nextRunAt"],
        "chatKey": stored["chatKey"],
        "mode": "cron",
        "message": "scheduled",
    }


def submit_schedule_definition_request(data: dict[str, Any], source: str = "api") -> dict[str, Any]:
    definition = create_schedule_definition_record(data)
    ensure_schedule_dirs()
    stored = create_schedule_definition(definition)
    bot = BOTS.get(stored["botId"])
    if bot:
        add_log(
            bot,
            (
                f"[{stored['chatKey']}] recurring schedule accepted via {source}: "
                f"scheduleId={stored['scheduleId']} mode={stored['mode']} nextRunAt={stored['nextRunAt']}"
            ),
        )
    return {"ok": True, **stored}


def finalize_scheduled_message_job(pending_file: Path, ok: bool) -> None:
    target_dir = SCHEDULE_DONE_ROOT if ok else SCHEDULE_FAILED_ROOT
    ensure_dir(target_dir)
    try:
        pending_file.replace(target_dir / pending_file.name)
    except Exception:
        try:
            pending_file.unlink()
        except FileNotFoundError:
            pass


def local_file_request_matches_target(request: dict[str, Any], chat_key: str, session_id: str) -> bool:
    request_chat_key = str(request.get("chatKey") or request.get("chat_key") or "").strip()
    request_session_id = str(request.get("sessionId") or request.get("session_id") or "").strip()
    if request_chat_key and request_chat_key == chat_key:
        return True
    if session_id and request_session_id and request_session_id == session_id:
        return True
    return False


def cancel_pending_file_send_requests(bot: BotState, chat_key: str, session_id: Optional[str], reason: str) -> int:
    cancelled = 0
    target_session_id = str(session_id or "").strip()
    active_upload_job = bot.active_upload_job or {}
    active_request_id = str(active_upload_job.get("localRequestId") or "").strip()
    active_processing_file = str(active_upload_job.get("localProcessingFile") or "").strip()
    active_upload_task = bot.active_upload_task
    bot_queue_paths = [get_local_file_send_queue_paths(), get_local_file_send_queue_paths(bot.config["id"])]
    seen_roots = set()
    relevant_queue_paths = []
    for paths in bot_queue_paths:
        root_str = str(paths["root"])
        if root_str in seen_roots:
            continue
        relevant_queue_paths.append(paths)
        seen_roots.add(root_str)

    queue_items = list(bot.upload_queue._queue)
    bot.upload_queue._queue.clear()
    for job in queue_items:
        job_chat_key = str(job.get("chatKey") or "").strip()
        if job_chat_key != chat_key:
            bot.upload_queue._queue.append(job)
            continue
        local_request_id = str(job.get("localRequestId") or "").strip()
        local_processing_file = str(job.get("localProcessingFile") or "").strip()
        queue_paths = (
            get_local_file_send_queue_paths_for_job_file(Path(local_processing_file))
            if local_processing_file
            else get_local_file_send_queue_paths_for_request(job)
        )
        if local_request_id:
            bot.active_local_file_request_ids.discard(local_request_id)
            write_local_file_send_result(
                local_request_id,
                {"ok": False, "statusCode": 409, "error": reason},
                queue_paths,
            )
        if local_processing_file:
            finalize_local_file_send_job(Path(local_processing_file), False)
        try:
            bot.upload_queue.task_done()
        except ValueError:
            pass
        cancelled += 1

    ensure_local_file_send_dirs([bot.config["id"]])
    for queue_paths in relevant_queue_paths:
        for root in (queue_paths["pending"], queue_paths["processing"]):
            for request_file in root.glob("*.json"):
                request = read_json_file(request_file, None)
                if not isinstance(request, dict):
                    continue
                if not local_file_request_matches_target(request, chat_key, target_session_id):
                    continue
                request_id = str(request.get("requestId") or request_file.stem).strip() or request_file.stem
                is_active_request = bool(request_id and request_id == active_request_id)
                is_active_processing_file = bool(active_processing_file and str(request_file) == active_processing_file)
                if is_active_request or is_active_processing_file:
                    if request_id:
                        bot.cancelled_local_file_request_ids.add(request_id)
                    if active_upload_task is not None and not active_upload_task.done():
                        active_upload_task.cancel()
                bot.active_local_file_request_ids.discard(request_id)
                write_local_file_send_result(
                    request_id,
                    {"ok": False, "statusCode": 409, "error": reason},
                    get_local_file_send_queue_paths_for_job_file(request_file),
                )
                finalize_local_file_send_job(request_file, False)
                cancelled += 1
    return cancelled


def reset_scheduled_message_job(pending_file: Path) -> None:
    job = read_json_file(pending_file, None)
    if not isinstance(job, dict):
        finalize_scheduled_message_job(pending_file, False)
        return
    job["enqueuedAt"] = None
    job["enqueuedByInstance"] = None
    target = SCHEDULE_PENDING_ROOT / pending_file.name
    write_json_atomic(target, job)
    if pending_file != target:
        try:
            pending_file.unlink()
        except FileNotFoundError:
            pass


def recover_session_scheduled_jobs(sess: SessionState) -> None:
    for item in list(sess.queue):
        scheduled_job_file = str(item.get("scheduledJobFile") or "").strip()
        if not scheduled_job_file:
            continue
        reset_scheduled_message_job(Path(scheduled_job_file))


def iter_due_scheduled_job_files(now: int) -> list[Path]:
    due: list[Path] = []
    for root in (SCHEDULE_PROCESSING_ROOT, SCHEDULE_PENDING_ROOT):
        for pending_file in sorted(root.glob("*.json")):
            job = read_json_file(pending_file, None)
            if not isinstance(job, dict):
                due.append(pending_file)
                continue
            if root == SCHEDULE_PROCESSING_ROOT:
                due.append(pending_file)
                continue
            if int(job.get("runAt") or 0) <= now:
                due.append(pending_file)
    return due


def schedule_definition_has_pending_work(schedule_id: str) -> bool:
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT):
        for pending_file in root.glob("*.json"):
            job = read_json_file(pending_file, None)
            if isinstance(job, dict) and str(job.get("scheduleId") or "") == schedule_id:
                return True
    for bot in BOTS.values():
        for sess in bot.sessions.values():
            if sess.active_schedule_id == schedule_id:
                return True
            for item in sess.queue:
                if str(item.get("scheduleId") or "") == schedule_id:
                    return True
    return False


def acquire_schedule_definition_lock(schedule_id: str) -> bool:
    try:
        owner_token = ("task", id(asyncio.current_task()))
    except RuntimeError:
        owner_token = ("thread", threading.get_ident())
    lock_file = get_schedule_definition_lock_file(schedule_id)
    lock_payload = {"instanceId": INSTANCE_ID, "expiresAt": now_ms() + SCHEDULE_DEFINITION_LEASE_TTL_MS}
    existing = SCHEDULE_DEFINITION_LOCK_HANDLES.get(schedule_id)
    if existing is not None:
        if existing.get("owner") == owner_token:
            existing["count"] = int(existing.get("count") or 0) + 1
            return True
        return False
    ensure_dir_for(lock_file)
    handle = open(lock_file, "a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return False
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps(lock_payload, ensure_ascii=False, indent=2))
    handle.flush()
    try:
        os.fsync(handle.fileno())
    except OSError:
        pass
    SCHEDULE_DEFINITION_LOCK_HANDLES[schedule_id] = {"handle": handle, "owner": owner_token, "count": 1}
    return True


def release_schedule_definition_lock(schedule_id: str) -> None:
    try:
        owner_token = ("task", id(asyncio.current_task()))
    except RuntimeError:
        owner_token = ("thread", threading.get_ident())
    lock_state = SCHEDULE_DEFINITION_LOCK_HANDLES.get(schedule_id)
    if lock_state is None:
        return
    if lock_state.get("owner") != owner_token:
        return
    remaining = int(lock_state.get("count") or 0) - 1
    if remaining > 0:
        lock_state["count"] = remaining
        return
    SCHEDULE_DEFINITION_LOCK_HANDLES.pop(schedule_id, None)
    handle = lock_state["handle"]
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def require_schedule_definition_lock(schedule_id: str) -> None:
    if not acquire_schedule_definition_lock(schedule_id):
        raise BridgeError(409, f"schedule is being modified by another instance: {schedule_id}")


def should_skip_schedule_due_to_misfire(definition: dict[str, Any], now: int) -> bool:
    if definition["misfirePolicy"] != "skip_missed":
        return False
    grace_ms = max(1000, SCHEDULE_DEFINITION_POLL_MS * 2)
    next_run_at = int(definition.get("nextRunAt") or 0)
    return next_run_at and now - next_run_at > grace_ms


def is_schedule_job_running(job: dict[str, Any], now: int) -> bool:
    session_id = str(job.get("sessionId") or "").strip()
    schedule_id = str(job.get("scheduleId") or "").strip()
    if not session_id or not schedule_id:
        return False
    record = read_session_record_by_id(session_id)
    if not record:
        return False
    if str(record.get("activeScheduleId") or "") != schedule_id:
        return False
    if str(record.get("status") or "") != "running":
        return False
    lease_expires_at = int(record.get("leaseExpiresAt") or 0)
    return lease_expires_at > now


def session_has_scheduled_job(
    sess: SessionState, scheduled_job_file: Optional[str], schedule_request_id: Optional[str]
) -> bool:
    job_file = str(scheduled_job_file or "").strip()
    request_id = str(schedule_request_id or "").strip()
    if job_file and str(sess.active_scheduled_job_file or "").strip() == job_file:
        return True
    if request_id and str(sess.active_schedule_request_id or "").strip() == request_id:
        return True
    for item in sess.queue:
        if job_file and str(item.get("scheduledJobFile") or "").strip() == job_file:
            return True
        if request_id and str(item.get("scheduleRequestId") or "").strip() == request_id:
            return True
    return False


def advance_schedule_definition(definition: dict[str, Any], now: int) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
    next_run_at = int(definition.get("nextRunAt") or 0)
    if not definition.get("enabled", True) or not next_run_at or next_run_at > now:
        return None, definition

    should_trigger = True
    if should_skip_schedule_due_to_misfire(definition, now):
        should_trigger = False
    if definition["concurrencyPolicy"] == "skip_if_running" and schedule_definition_has_pending_work(definition["scheduleId"]):
        should_trigger = False

    next_definition = dict(definition)
    trigger_job = None

    if should_trigger:
        trigger_job = build_scheduled_message_job(definition, next_run_at)
        next_definition["runCount"] = int(next_definition.get("runCount") or 0) + 1
        next_definition["lastTriggeredAt"] = next_run_at

    if next_definition.get("maxRuns") is not None and int(next_definition["runCount"]) >= int(next_definition["maxRuns"] or 0):
        next_definition["enabled"] = False
        next_definition["nextRunAt"] = None
    else:
        future_earliest = max(now + 1, next_run_at + 1)
        next_definition["nextRunAt"] = compute_schedule_next_run_on_or_after(next_definition, future_earliest)
        if next_definition["nextRunAt"] is None:
            next_definition["enabled"] = False
    next_definition["lastPlannedAt"] = now
    return trigger_job, next_definition


async def process_schedule_definitions_once() -> None:
    ensure_schedule_dirs()
    now = now_ms()
    for definition in list_schedule_definitions():
        if read_persisted_bot_config(definition["botId"]) is None and definition["botId"] not in BOTS:
            schedule_id = definition["scheduleId"]
            if not acquire_schedule_definition_lock(schedule_id):
                continue
            try:
                current = read_schedule_definition(schedule_id)
                if current:
                    purge_schedule_definition_jobs(schedule_id, stop_running=True)
                    remove_path_if_exists(get_schedule_definition_file(schedule_id))
            finally:
                release_schedule_definition_lock(schedule_id)
            continue
        if not definition.get("enabled", True):
            continue
        next_run_at = int(definition.get("nextRunAt") or 0)
        if not next_run_at or next_run_at > now:
            continue
        schedule_id = definition["scheduleId"]
        if not acquire_schedule_definition_lock(schedule_id):
            continue
        try:
            current = read_schedule_definition(schedule_id)
            if not current or not current.get("enabled", True):
                continue
            current_next_run_at = int(current.get("nextRunAt") or 0)
            if not current_next_run_at or current_next_run_at > now:
                continue
            trigger_job, next_definition = advance_schedule_definition(current, now)
            write_schedule_definition(next_definition)
            if trigger_job:
                pending_file = SCHEDULE_PENDING_ROOT / f"{trigger_job['runAt']:013d}-{trigger_job['requestId']}.json"
                write_json_atomic(pending_file, trigger_job)
        finally:
            release_schedule_definition_lock(schedule_id)


def pause_schedule_definition(schedule_id: str) -> dict[str, Any]:
    require_schedule_definition_lock(schedule_id)
    try:
        updated = update_schedule_definition(schedule_id, lambda current: {**current, "enabled": False})
        if not updated:
            raise BridgeError(404, f"schedule not found: {schedule_id}")
        purge_schedule_definition_jobs(schedule_id, stop_running=False)
        return updated
    finally:
        release_schedule_definition_lock(schedule_id)


def resume_schedule_definition(schedule_id: str) -> dict[str, Any]:
    require_schedule_definition_lock(schedule_id)
    try:
        current = read_schedule_definition(schedule_id)
        if not current:
            raise BridgeError(404, f"schedule not found: {schedule_id}")
        if current.get("maxRuns") is not None and int(current.get("runCount") or 0) >= int(current.get("maxRuns") or 0):
            raise BridgeError(400, "schedule already reached maxRuns")
        next_run_at = compute_schedule_next_run_on_or_after(current, now_ms())
        if next_run_at is None:
            raise BridgeError(400, "schedule has no future run time")
        updated = write_schedule_definition({**current, "enabled": True, "nextRunAt": next_run_at})
        return updated
    finally:
        release_schedule_definition_lock(schedule_id)


def purge_schedule_definition_jobs(schedule_id: str, stop_running: bool = False) -> None:
    now = now_ms()
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT):
        for job_file in root.glob("*.json"):
            job = read_json_file(job_file, None)
            if isinstance(job, dict) and str(job.get("scheduleId") or "") == schedule_id:
                if root == SCHEDULE_PROCESSING_ROOT and not stop_running and is_schedule_job_running(job, now):
                    continue
                finalize_scheduled_message_job(job_file, False)

    for bot in BOTS.values():
        for key, sess in bot.sessions.items():
            interrupted = False
            if stop_running and sess.active_schedule_id == schedule_id:
                interrupt_session(bot, key, sess, clear_thread=False, clear_chat=False, clear_queue=False)
                interrupted = True
            kept_queue: list[dict[str, Any]] = []
            for item in sess.queue:
                if str(item.get("scheduleId") or "") != schedule_id:
                    kept_queue.append(item)
                    continue
                scheduled_job_file = str(item.get("scheduledJobFile") or "").strip()
                if scheduled_job_file:
                    finalize_scheduled_message_job(Path(scheduled_job_file), False)
            if len(kept_queue) != len(sess.queue):
                sess.queue = kept_queue
            if interrupted:
                resume_session_queue(bot, sess, key, "schedule purge")


def delete_schedule_definition(schedule_id: str) -> None:
    require_schedule_definition_lock(schedule_id)
    try:
        definition_file = get_schedule_definition_file(schedule_id)
        if not definition_file.exists():
            raise BridgeError(404, f"schedule not found: {schedule_id}")
        purge_schedule_definition_jobs(schedule_id, stop_running=True)
        try:
            definition_file.unlink()
        except FileNotFoundError:
            pass
    finally:
        release_schedule_definition_lock(schedule_id)


def bot_has_chat_key(bot: BotState, chat_key_value: str) -> bool:
    return chat_key_value in bot.sessions or read_session_record_by_key(bot.config["id"], chat_key_value) is not None


def maybe_cleanup_schedule_definition(schedule_id: Optional[str]) -> None:
    schedule_id = str(schedule_id or "").strip()
    if not schedule_id:
        return
    definition = read_schedule_definition(schedule_id)
    if not definition:
        return
    if not definition.get("autoDeleteOnDone", False):
        return
    if definition.get("enabled", True):
        return
    max_runs = definition.get("maxRuns")
    if max_runs is not None and int(definition.get("runCount") or 0) < int(max_runs):
        return
    if schedule_definition_has_pending_work(schedule_id):
        return
    try:
        get_schedule_definition_file(schedule_id).unlink()
    except FileNotFoundError:
        pass


async def process_scheduled_messages_once() -> None:
    ensure_schedule_dirs()
    now = now_ms()
    for pending_file in iter_due_scheduled_job_files(now):
        job = read_json_file(pending_file, None)
        if not isinstance(job, dict):
            finalize_scheduled_message_job(pending_file, False)
            continue
        if pending_file.parent == SCHEDULE_PENDING_ROOT and int(job.get("runAt") or 0) > now:
            continue
        bot = BOTS.get(str(job.get("botId") or ""))
        if not bot or not bot.config.get("enabled", True):
            run_at = int(job.get("runAt") or 0)
            if run_at and now - run_at > SCHEDULE_ORPHAN_TTL_MS:
                finalize_scheduled_message_job(pending_file, False)
            continue
        if bot.status != "running" or not bot.ws or bot.ws.closed:
            continue
        chat_key_value = str(job.get("chatKey") or "").strip()
        message = str(job.get("message") or "").strip()
        if not chat_key_value or not message:
            finalize_scheduled_message_job(pending_file, False)
            continue
        processing_file = pending_file
        if pending_file.parent == SCHEDULE_PENDING_ROOT:
            processing_file = SCHEDULE_PROCESSING_ROOT / pending_file.name
            try:
                pending_file.replace(processing_file)
            except FileNotFoundError:
                continue
            job["enqueuedAt"] = None
            job["enqueuedByInstance"] = None
            write_json_atomic(processing_file, job)
        enqueued_at = int(job.get("enqueuedAt") or 0)
        if is_schedule_job_running(job, now):
            continue
        if enqueued_at and now - enqueued_at < SCHEDULE_PROCESSING_RETRY_MS:
            continue
        sess = bot.sessions.get(chat_key_value)
        processing_file_str = str(processing_file)
        schedule_request_id = str(job.get("requestId") or "").strip()
        if sess and session_has_scheduled_job(sess, processing_file_str, schedule_request_id):
            job["enqueuedAt"] = now_ms()
            job["enqueuedByInstance"] = INSTANCE_ID
            write_json_atomic(processing_file, job)
            add_log(bot, f"[{chat_key_value}] scheduled message already queued requestId={job.get('requestId')}")
            add_event_log(
                bot,
                "schedule.already_queued",
                chatKey=chat_key_value,
                sessionId=job.get("sessionId"),
                scheduleId=job.get("scheduleId"),
                requestId=job.get("requestId"),
            )
            continue
        accepted = await enqueue_message(
            bot,
            chat_key_value,
            message,
            None,
            silent_lease_failure=True,
            scheduled_job_file=str(processing_file),
            schedule_id=str(job.get("scheduleId") or "").strip() or None,
            schedule_request_id=schedule_request_id or None,
        )
        if not accepted:
            continue
        job["enqueuedAt"] = now_ms()
        job["enqueuedByInstance"] = INSTANCE_ID
        write_json_atomic(processing_file, job)
        add_log(bot, f"[{chat_key_value}] scheduled message dispatched requestId={job.get('requestId')}")
        add_event_log(
            bot,
            "schedule.dispatched",
            chatKey=chat_key_value,
            sessionId=job.get("sessionId"),
            scheduleId=job.get("scheduleId"),
            requestId=job.get("requestId"),
        )


async def process_local_file_send_queue_once() -> None:
    global LOCAL_FILE_SEND_QUEUE_BUSY
    if LOCAL_FILE_SEND_QUEUE_BUSY:
        return
    LOCAL_FILE_SEND_QUEUE_BUSY = True
    try:
        cleanup_stale_local_file_send_result_files()
        ensure_local_file_send_dirs(sorted(BOTS.keys()))
        for queue_paths in list_local_file_send_queue_path_groups():
            for pending_file in [*sorted(queue_paths["pending"].glob("*.json")), *sorted(queue_paths["processing"].glob("*.json"))]:
                processing_file = pending_file
                if pending_file.parent == queue_paths["pending"]:
                    processing_file = queue_paths["processing"] / pending_file.name
                    try:
                        pending_file.replace(processing_file)
                    except FileNotFoundError:
                        continue
                request = read_json_file(processing_file, None)
                if not isinstance(request, dict):
                    write_local_file_send_result(
                        processing_file.stem,
                        {"ok": False, "statusCode": 400, "error": "invalid local file-send request"},
                        queue_paths,
                    )
                    finalize_local_file_send_job(processing_file, False)
                    continue
                request, request_changed = ensure_local_file_send_request_deadline(request, processing_file)
                if request_changed:
                    write_json_atomic(processing_file, request)
                request_id = str(request.get("requestId") or processing_file.stem).strip() or processing_file.stem
                existing_result = read_local_file_send_result(request_id, queue_paths)
                if existing_result is not None and isinstance(existing_result.get("ok"), bool):
                    finalize_local_file_send_job(processing_file, bool(existing_result.get("ok")))
                    continue
                if local_file_send_request_is_expired(request):
                    write_local_file_send_result(
                        request_id,
                        {
                            **local_file_send_request_timeout_payload(request),
                            "retainUntil": local_file_send_request_retain_until_ms(request),
                        },
                        queue_paths,
                    )
                    finalize_local_file_send_job(processing_file, False)
                    continue
                if local_file_send_request_delivery_is_ambiguous(request) and not local_file_send_request_has_active_delivery(request_id):
                    write_local_file_send_result(
                        request_id,
                        {
                            **local_file_send_request_ambiguous_payload(),
                            "retainUntil": local_file_send_request_retain_until_ms(request),
                        },
                        queue_paths,
                    )
                    finalize_local_file_send_job(processing_file, False)
                    continue
                try:
                    bot, _, _ = resolve_file_send_request(request)
                    if request_id in bot.active_local_file_request_ids:
                        continue
                    submit_file_send_request(
                        {
                            **request,
                            "localRequestId": request_id,
                            "localProcessingFile": str(processing_file),
                        },
                            "local-command",
                        )
                except BridgeError as exc:
                    if exc.status_code == 503 and (exc.message == "bot not connected" or exc.message.startswith("bot not running:")):
                        continue
                    write_local_file_send_result(
                        request_id,
                        {
                            "ok": False,
                            "statusCode": exc.status_code,
                            "error": exc.message,
                            "retainUntil": local_file_send_request_retain_until_ms(request),
                        },
                        queue_paths,
                    )
                    finalize_local_file_send_job(processing_file, False)
    finally:
        LOCAL_FILE_SEND_QUEUE_BUSY = False


def create_request_future(bot: BotState, req_id: str) -> asyncio.Future:
    future = asyncio.get_running_loop().create_future()
    bot.pending_requests[req_id] = future
    return future


def resolve_request_future(bot: BotState, req_id: str, payload: dict[str, Any]) -> bool:
    future = bot.pending_requests.pop(req_id, None)
    if not future:
        return False
    if not future.done():
        future.set_result(payload)
    return True


def reject_pending_requests(bot: BotState, message: str) -> None:
    for req_id, future in list(bot.pending_requests.items()):
        bot.pending_requests.pop(req_id, None)
        if not future.done():
            future.set_exception(BridgeError(503, message))


async def upload_and_send_file(
    bot: BotState,
    chat_key_value: str,
    file_path: str,
    *,
    before_delivery: Optional[Any] = None,
    after_delivery: Optional[Any] = None,
) -> None:
    if not bot.ws or bot.ws.closed:
        raise BridgeError(503, "bot not connected")
    file_bytes = Path(file_path).read_bytes()
    file_name = Path(file_path).name
    file_size = len(file_bytes)
    chunk_size = 400 * 1024
    total_chunks = max(1, (file_size + chunk_size - 1) // chunk_size)
    md5 = hashlib.md5(file_bytes).hexdigest()
    chat_type, chat_id = chat_key_to_send_target(chat_key_value)

    add_log(bot, f"upload file: {file_name} ({round(file_size / 1024)}KB, {total_chunks} chunks)")
    add_event_log(bot, "file.upload_start", chatKey=chat_key_value, fileName=file_name, sizeBytes=file_size, chunks=total_chunks)
    init_payload = {
        "cmd": "aibot_upload_media_init",
        "headers": {"req_id": uid()},
        "body": {
            "type": "file",
            "filename": file_name,
            "total_size": file_size,
            "total_chunks": total_chunks,
            "md5": md5,
        },
    }
    init_response = await send_ws_payload_with_ack(bot, init_payload, 30)
    if init_response.get("errcode") != 0 or not ((init_response.get("body") or {}).get("upload_id")):
        raise BridgeError(502, f"upload init failed: {init_response.get('errcode')} {init_response.get('errmsg', '')}".strip())
    upload_id = init_response["body"]["upload_id"]

    chunk_futures: list[asyncio.Future] = []
    chunk_req_ids: list[str] = []
    for idx in range(total_chunks):
        chunk = file_bytes[idx * chunk_size : (idx + 1) * chunk_size]
        req_id = uid()
        future = create_request_future(bot, req_id)
        payload = {
            "cmd": "aibot_upload_media_chunk",
            "headers": {"req_id": req_id},
            "body": {"upload_id": upload_id, "chunk_index": idx, "base64_data": base64.b64encode(chunk).decode("ascii")},
        }
        try:
            await send_ws_payload(bot, payload)
        except Exception:
            bot.pending_requests.pop(req_id, None)
            raise
        chunk_req_ids.append(req_id)
        chunk_futures.append(future)

    try:
        chunk_responses = await asyncio.wait_for(asyncio.gather(*chunk_futures), max(5, total_chunks * 2))
    except asyncio.TimeoutError as exc:
        for req_id in chunk_req_ids:
            bot.pending_requests.pop(req_id, None)
        raise BridgeError(504, "upload chunk ack timeout") from exc
    for req_id, response in zip(chunk_req_ids, chunk_responses):
        if response.get("errcode") not in (None, 0):
            raise BridgeError(502, f"upload chunk failed: {req_id} {response.get('errcode')} {response.get('errmsg', '')}".strip())

    finish_payload = {
        "cmd": "aibot_upload_media_finish",
        "headers": {"req_id": uid()},
        "body": {"upload_id": upload_id},
    }
    finish_response = await send_ws_payload_with_ack(bot, finish_payload, 60)

    media_id = ((finish_response.get("body") or {}).get("media_id"))
    if finish_response.get("errcode") != 0 or not media_id:
        raise BridgeError(502, f"upload finish failed: {finish_response.get('errcode')} {finish_response.get('errmsg', '')}".strip())

    send_payload = {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {"chatid": chat_id, "chat_type": chat_type, "msgtype": "file", "file": {"media_id": media_id}},
    }
    if before_delivery:
        before_delivery()
    response = await send_ws_payload_with_ack(bot, send_payload, 30)
    if response.get("errcode") not in (None, 0):
        raise BridgeError(502, f"file send failed: {response.get('errcode')} {response.get('errmsg', '')}".strip())
    if after_delivery:
        after_delivery()
    add_log(bot, f"file sent: {file_name}")
    add_event_log(bot, "file.sent", chatKey=chat_key_value, fileName=file_name, mediaId=media_id)


async def upload_worker(bot: BotState) -> None:
    while bot.config.get("enabled", True):
        job = await bot.upload_queue.get()
        local_request_id = str(job.get("localRequestId") or "").strip()
        local_processing_file = str(job.get("localProcessingFile") or "").strip()
        queue_paths = (
            get_local_file_send_queue_paths_for_job_file(Path(local_processing_file))
            if local_processing_file
            else get_local_file_send_queue_paths()
        )
        upload_task: Optional[asyncio.Task] = None
        processing_request: Optional[dict[str, Any]] = None
        processing_path = Path(local_processing_file) if local_processing_file else None
        try:
            if processing_path is not None:
                processing_request = read_json_file(processing_path, None)
                if not isinstance(processing_request, dict):
                    if local_request_id:
                        write_local_file_send_result(
                            local_request_id,
                            {"ok": False, "statusCode": 400, "error": "invalid local file-send request"},
                            queue_paths,
                        )
                        finalize_local_file_send_job(processing_path, False)
                    continue
                if local_file_send_request_is_expired(processing_request):
                    if local_request_id:
                        write_local_file_send_result(
                            local_request_id,
                            local_file_send_request_timeout_payload(processing_request),
                            queue_paths,
                        )
                        finalize_local_file_send_job(processing_path, False)
                    continue
                mark_local_file_send_processing_started(
                    processing_path,
                    processing_request,
                    bot_id=str(bot.config["id"]),
                    chat_key=str(job["chatKey"]),
                )
            bot.active_upload_job = dict(job)
            if local_request_id and local_request_id in bot.cancelled_local_file_request_ids:
                continue
            existing_result = read_local_file_send_result(local_request_id, queue_paths) if local_request_id else None
            if existing_result is not None and isinstance(existing_result.get("ok"), bool):
                if processing_path is not None:
                    finalize_local_file_send_job(processing_path, bool(existing_result.get("ok")))
                continue
            def before_delivery() -> None:
                if processing_path is None or not isinstance(processing_request, dict):
                    return
                mark_local_file_send_processing_sending(
                    processing_path,
                    processing_request,
                    bot_id=str(bot.config["id"]),
                    chat_key=str(job["chatKey"]),
                )
                processing_request.update(
                    {
                        "resolvedBotId": str(bot.config["id"]),
                        "resolvedChatKey": str(job["chatKey"]),
                        "deliveryState": "sending",
                        "deliveryDispatchAt": now_ms(),
                    }
                )
            bot.active_upload_task = asyncio.create_task(
                upload_and_send_file(
                    bot,
                    job["chatKey"],
                    job["filePath"],
                    before_delivery=before_delivery,
                )
            )
            upload_task = bot.active_upload_task
            if local_request_id and local_request_id in bot.cancelled_local_file_request_ids:
                upload_task.cancel()
                with suppress(asyncio.CancelledError):
                    await upload_task
                continue
            expires_at = local_file_send_request_expires_at_ms(processing_request) if isinstance(processing_request, dict) else None
            if expires_at is None:
                await upload_task
            else:
                remaining_ms = expires_at - now_ms()
                if remaining_ms <= 0:
                    raise asyncio.TimeoutError
                await asyncio.wait_for(upload_task, remaining_ms / 1000)
            if local_request_id and local_request_id in bot.cancelled_local_file_request_ids:
                continue
            if local_request_id:
                if processing_path is not None and isinstance(processing_request, dict):
                    mark_local_file_send_processing_sent(
                        processing_path,
                        processing_request,
                        bot_id=str(bot.config["id"]),
                        chat_key=str(job["chatKey"]),
                    )
                write_local_file_send_result(
                    local_request_id,
                    {"ok": True, "message": f"sent {Path(job['filePath']).name}"},
                    queue_paths,
                )
                if local_processing_file:
                    finalize_local_file_send_job(Path(local_processing_file), True)
        except asyncio.CancelledError:
            if upload_task is not None and not upload_task.done():
                upload_task.cancel()
                with suppress(asyncio.CancelledError):
                    await upload_task
            if local_request_id and local_request_id in bot.cancelled_local_file_request_ids:
                continue
            raise
        except asyncio.TimeoutError:
            if upload_task is not None and not upload_task.done():
                upload_task.cancel()
                with suppress(asyncio.CancelledError):
                    await upload_task
            if local_request_id and local_request_id in bot.cancelled_local_file_request_ids:
                continue
            if local_request_id:
                write_local_file_send_result(
                    local_request_id,
                    local_file_send_request_delivery_timeout_payload(processing_request or {}),
                    queue_paths,
                )
                if local_processing_file:
                    finalize_local_file_send_job(Path(local_processing_file), False)
        except Exception as exc:
            if local_request_id and local_request_id in bot.cancelled_local_file_request_ids:
                continue
            add_log(bot, f"[{job['chatKey']}] file send failed: {exc}")
            if local_request_id:
                status_code = exc.status_code if isinstance(exc, BridgeError) else 500
                error_message = exc.message if isinstance(exc, BridgeError) else str(exc) or "file send failed"
                result_payload = {"ok": False, "statusCode": status_code, "error": error_message}
                if (
                    isinstance(exc, BridgeError)
                    and exc.status_code in {503, 504}
                    and exc.message in {"bot websocket closed", "websocket send timeout", "websocket ack timeout"}
                    and isinstance(processing_request, dict)
                    and local_file_send_request_delivery_is_ambiguous(processing_request)
                ):
                    result_payload = local_file_send_request_ambiguous_payload()
                write_local_file_send_result(
                    local_request_id,
                    result_payload,
                    queue_paths,
                )
                if local_processing_file:
                    finalize_local_file_send_job(Path(local_processing_file), False)
        finally:
            bot.active_upload_task = None
            bot.active_upload_job = None
            if local_request_id:
                bot.active_local_file_request_ids.discard(local_request_id)
                bot.cancelled_local_file_request_ids.discard(local_request_id)
            bot.upload_queue.task_done()


def find_bot_by_chat_key(chat_key_value: str, bot_name: Optional[str]) -> BotState:
    return require_unique_bot_for_chat_key(chat_key_value, bot_name)


def normalize_work_dir(work_dir: Optional[str]) -> str:
    resolved = Path(work_dir or DEFAULT_WORK_DIR).expanduser().resolve()
    if not resolved.exists():
        raise BridgeError(400, f"workDir not found: {resolved}")
    if not resolved.is_dir():
        raise BridgeError(400, f"workDir is not a directory: {resolved}")
    return str(resolved)


def normalize_group_session_mode(value: Any) -> str:
    mode = (str(value or "per-user").strip().lower() or "per-user").replace("_", "-")
    if mode not in VALID_GROUP_SESSION_MODES:
        raise BridgeError(400, f"groupSessionMode must be one of: {', '.join(sorted(VALID_GROUP_SESSION_MODES))}")
    return mode


def normalize_optional_path(value: Any, *, base_dir: Optional[Path] = None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = ((base_dir or BASE_DIR) / path).resolve()
    else:
        path = path.resolve()
    return str(path)


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise BridgeError(500, f"invalid boolean env {name}: {raw}")


def default_bot_config_id(bot_id: str) -> str:
    digest = hashlib.sha1(bot_id.encode("utf-8")).hexdigest()[:16]
    return f"env-bot-{digest}"


def read_text_file(path_value: Any, *, label: str, status_code: int = 400) -> str:
    normalized = normalize_optional_path(path_value)
    if not normalized:
        raise BridgeError(status_code, f"{label} path required")
    path = Path(normalized)
    if not path.exists():
        raise BridgeError(status_code, f"{label} not found: {path}")
    if not path.is_file():
        raise BridgeError(status_code, f"{label} is not a file: {path}")
    try:
        content = path.read_text("utf-8").strip()
    except BridgeError:
        raise
    except Exception as exc:
        raise BridgeError(status_code, f"read {label} failed: {exc}") from exc
    if not content:
        raise BridgeError(status_code, f"{label} is empty: {path}")
    return content


def resolve_bot_secret(data: dict[str, Any], *, status_code: int, allow_inline_secret: bool = False) -> tuple[str, str]:
    secret = str(data.get("secret") or "").strip()
    secret_file = normalize_optional_path(data.get("secretFile") or data.get("secret_file"))
    if secret and secret_file:
        file_secret = read_text_file(secret_file, label="secretFile", status_code=status_code)
        if file_secret != secret:
            raise BridgeError(status_code, "secret and secretFile contents do not match")
        return file_secret, secret_file
    if secret_file:
        secret = read_text_file(secret_file, label="secretFile", status_code=status_code)
    if not secret:
        raise BridgeError(status_code, "secretFile required")
    if not allow_inline_secret and not secret_file:
        raise BridgeError(status_code, "plaintext secret is no longer supported; use secretFile")
    return secret, secret_file


def normalize_bot_config(data: dict[str, Any], *, allow_inline_secret: bool = False) -> dict[str, Any]:
    bot_id = str(data.get("botId") or "").strip()
    if not bot_id:
        raise BridgeError(400, "botId required")
    secret, secret_file = resolve_bot_secret(data, status_code=400, allow_inline_secret=allow_inline_secret)
    normalized = {
        "id": data.get("id") or uid(),
        "name": (str(data.get("name") or "unnamed").strip() or "unnamed"),
        "botId": bot_id,
        "secret": secret,
        "workDir": normalize_work_dir(data.get("workDir")),
        "welcome": str(data.get("welcome") or ""),
        "groupSessionMode": normalize_group_session_mode(data.get("groupSessionMode") or data.get("group_session_mode")),
        "enabled": data.get("enabled", True) is not False,
        "restartToken": str(data.get("restartToken") or data.get("restart_token") or "").strip() or None,
    }
    if secret_file:
        normalized["secretFile"] = secret_file
    return normalized


def normalize_persisted_bot_config(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    if str(normalized.get("secret") or "").strip():
        raise BridgeError(
            500,
            "legacy plaintext bot secrets in .bots.json are no longer supported; rewrite the bot to use secretFile or env bootstrap",
        )
    legacy_secret_file = normalized.pop("secret_file", None)
    if legacy_secret_file and not normalized.get("secretFile"):
        normalized["secretFile"] = legacy_secret_file
    created_at = normalize_optional_int(normalized.get("createdAt") or normalized.get("created_at"))
    updated_at = normalize_optional_int(normalized.get("updatedAt") or normalized.get("updated_at"))
    if created_at is not None:
        normalized["createdAt"] = created_at
    else:
        normalized.pop("createdAt", None)
    if updated_at is not None:
        normalized["updatedAt"] = updated_at
    else:
        normalized.pop("updatedAt", None)
    return normalized


def load_env_bootstrap_bot_configs() -> list[dict[str, Any]]:
    configs: list[dict[str, Any]] = []

    raw_json = str(os.environ.get("WECOM_BOOTSTRAP_BOTS_JSON") or "").strip()
    raw_json_file = str(os.environ.get("WECOM_BOOTSTRAP_BOTS_JSON_FILE") or "").strip()
    if raw_json and raw_json_file:
        raise BridgeError(500, "WECOM_BOOTSTRAP_BOTS_JSON and WECOM_BOOTSTRAP_BOTS_JSON_FILE cannot be set together")
    if raw_json_file:
        raw_json = read_text_file(raw_json_file, label="WECOM_BOOTSTRAP_BOTS_JSON_FILE", status_code=500)
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except Exception as exc:
            source = "WECOM_BOOTSTRAP_BOTS_JSON_FILE" if raw_json_file else "WECOM_BOOTSTRAP_BOTS_JSON"
            raise BridgeError(500, f"invalid {source}: {exc}") from exc
        if not isinstance(parsed, list):
            source = "WECOM_BOOTSTRAP_BOTS_JSON_FILE" if raw_json_file else "WECOM_BOOTSTRAP_BOTS_JSON"
            raise BridgeError(500, f"{source} must be a JSON array")
        for index, item in enumerate(parsed):
            if not isinstance(item, dict):
                source = "WECOM_BOOTSTRAP_BOTS_JSON_FILE" if raw_json_file else "WECOM_BOOTSTRAP_BOTS_JSON"
                raise BridgeError(500, f"{source}[{index}] must be an object")
            candidate = dict(item)
            candidate["__secretSourceExplicit__"] = any(key in candidate for key in ("secret", "secretFile", "secret_file"))
            bot_id = str(candidate.get("botId") or "").strip()
            if bot_id and not str(candidate.get("id") or "").strip():
                candidate["id"] = default_bot_config_id(bot_id)
                candidate["__idExplicit__"] = False
            else:
                candidate["__idExplicit__"] = True
            configs.append(candidate)

    bot_id = str(os.environ.get("WECOM_BOT_ID") or "").strip()
    secret_file = str(os.environ.get("WECOM_BOT_SECRET_FILE") or "").strip()
    if os.environ.get("WECOM_BOT_SECRET"):
        raise BridgeError(500, "WECOM_BOT_SECRET is no longer supported; use WECOM_BOT_SECRET_FILE")
    if bot_id or secret_file:
        if not bot_id or not secret_file:
            raise BridgeError(500, "WECOM_BOT_ID must be set together with WECOM_BOT_SECRET_FILE")
        explicit_id = str(os.environ.get("WECOM_BOT_CONFIG_ID") or "").strip()
        candidate = {
            "id": explicit_id or default_bot_config_id(bot_id),
            "name": (str(os.environ.get("WECOM_BOT_NAME") or "default").strip() or "default"),
            "botId": bot_id,
            "workDir": str(os.environ.get("WECOM_BOT_WORK_DIR") or DEFAULT_WORK_DIR).strip() or DEFAULT_WORK_DIR,
            "welcome": str(os.environ.get("WECOM_BOT_WELCOME") or ""),
            "groupSessionMode": str(os.environ.get("WECOM_BOT_GROUP_SESSION_MODE") or "per-user").strip()
            or "per-user",
            "enabled": env_bool("WECOM_BOT_ENABLED", True),
            "__idExplicit__": bool(explicit_id),
            "__secretSourceExplicit__": True,
        }
        if secret_file:
            candidate["secretFile"] = secret_file
        configs.append(candidate)

    return configs


def merge_bot_configs(
    stored_configs: list[dict[str, Any]],
    bootstrap_configs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged = [dict(item) for item in stored_configs if isinstance(item, dict)]
    index_by_id = {
        str(item.get("id") or "").strip(): idx
        for idx, item in enumerate(merged)
        if str(item.get("id") or "").strip()
    }
    index_by_bot_id = {
        str(item.get("botId") or "").strip(): idx
        for idx, item in enumerate(merged)
        if str(item.get("botId") or "").strip()
    }

    for raw_config in bootstrap_configs:
        candidate = dict(raw_config)
        candidate_id_explicit = candidate.pop("__idExplicit__", True)
        candidate_secret_source_explicit = candidate.pop("__secretSourceExplicit__", False)
        candidate_id = str(candidate.get("id") or "").strip()
        candidate_bot_id = str(candidate.get("botId") or "").strip()
        match_index = None
        if candidate_bot_id and not candidate_id_explicit and candidate_bot_id in index_by_bot_id:
            match_index = index_by_bot_id[candidate_bot_id]
            existing_id = str(merged[match_index].get("id") or "").strip()
            if existing_id:
                candidate["id"] = existing_id
        elif candidate_id and candidate_id in index_by_id:
            match_index = index_by_id[candidate_id]
        elif candidate_bot_id and candidate_bot_id in index_by_bot_id:
            match_index = index_by_bot_id[candidate_bot_id]
            existing_id = str(merged[match_index].get("id") or "").strip()
            if existing_id and not candidate_id:
                candidate["id"] = existing_id
        if match_index is not None:
            base = dict(merged[match_index])
            if candidate_secret_source_explicit:
                base.pop("secret", None)
                base.pop("secretFile", None)
                base.pop("secret_file", None)
            base.update(candidate)
            candidate = base
        normalized = normalize_bot_config(candidate)
        if match_index is None:
            merged.append(normalized)
            match_index = len(merged) - 1
        else:
            merged[match_index] = normalized
        index_by_id[normalized["id"]] = match_index
        index_by_bot_id[normalized["botId"]] = match_index

    return merged


def serialize_bot_config_for_disk(config: dict[str, Any]) -> dict[str, Any]:
    persisted = dict(config)
    persisted.pop("secret", None)
    if not str(persisted.get("secretFile") or "").strip():
        persisted.pop("secretFile", None)
    return persisted


def stamp_persisted_bot_config(config: dict[str, Any], previous: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    stamped = serialize_bot_config_for_disk(config)
    previous_created_at = normalize_optional_int((previous or {}).get("createdAt") or (previous or {}).get("created_at"))
    now_value = now_ms()
    stamped["createdAt"] = previous_created_at if previous_created_at is not None else now_value
    stamped["updatedAt"] = now_value
    return stamped


def serialize_bot_configs_for_disk(configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [serialize_bot_config_for_disk(item) for item in configs if isinstance(item, dict)]


def read_persisted_bot_config_entries_unlocked() -> list[Any]:
    stored_raw = read_json_file(DATA_FILE, [])
    if not isinstance(stored_raw, list):
        print_log(f"[INIT] invalid bot config file, ignore: {DATA_FILE}")
        return []
    return stored_raw


def log_invalid_persisted_bot_config_once(index: int, item: Any, message: str) -> None:
    config_id = f"index={index}"
    if isinstance(item, dict):
        config_id = str(item.get("id") or item.get("name") or config_id).strip() or config_id
    fingerprint = f"{index}:{config_id}:{message}"
    if fingerprint in REPORTED_INVALID_PERSISTED_BOT_CONFIGS:
        return
    REPORTED_INVALID_PERSISTED_BOT_CONFIGS.add(fingerprint)
    if isinstance(item, dict):
        print_log(f"[INIT] ignore invalid bot config {config_id}: {message}")
    else:
        print_log(f"[INIT] ignore invalid bot config at index {index}: {message}")


def is_valid_persisted_bot_config_entry(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    try:
        return normalize_persisted_bot_config(item) is not None
    except BridgeError:
        return False


def normalize_persisted_bot_config_entries(entries: list[Any]) -> list[dict[str, Any]]:
    normalized_items: list[dict[str, Any]] = []
    for index, item in enumerate(entries):
        if not isinstance(item, dict):
            log_invalid_persisted_bot_config_once(index, item, "expected object")
            continue
        try:
            normalized_items.append(normalize_persisted_bot_config(item))
        except BridgeError as exc:
            log_invalid_persisted_bot_config_once(index, item, exc.message)
    return normalized_items


def replace_persisted_bot_config_entries(
    existing_entries: list[Any],
    configs: list[dict[str, Any]],
    *,
    drop_ids: Optional[set[str]] = None,
) -> list[Any]:
    desired_entries = serialize_bot_configs_for_disk(configs)
    desired_by_id = {
        str(item.get("id") or "").strip(): item
        for item in desired_entries
        if str(item.get("id") or "").strip()
    }
    desired_ids = set(desired_by_id)
    removed_ids = {str(item).strip() for item in (drop_ids or set()) if str(item).strip()}
    replaced: list[Any] = []
    seen_ids: set[str] = set()

    for item in existing_entries:
        item_id = str(item.get("id") or "").strip() if isinstance(item, dict) else ""
        if item_id in removed_ids:
            continue
        if item_id and item_id in desired_ids:
            if item_id in seen_ids:
                continue
            replaced.append(desired_by_id[item_id])
            seen_ids.add(item_id)
            continue
        if is_valid_persisted_bot_config_entry(item):
            continue
        replaced.append(item)

    for item in desired_entries:
        config_id = str(item.get("id") or "").strip()
        if config_id and config_id in seen_ids:
            continue
        replaced.append(item)
    return replaced


def read_persisted_bot_configs_unlocked() -> list[dict[str, Any]]:
    return normalize_persisted_bot_config_entries(read_persisted_bot_config_entries_unlocked())


def write_persisted_bot_configs_unlocked(configs: list[dict[str, Any]]) -> None:
    current_entries = read_persisted_bot_config_entries_unlocked()
    next_entries = replace_persisted_bot_config_entries(current_entries, configs)
    if next_entries != current_entries or not DATA_FILE.exists():
        write_json_atomic(DATA_FILE, next_entries)


def read_persisted_bot_configs() -> list[dict[str, Any]]:
    with persisted_bot_configs_lock():
        return filter_deleted_persisted_bot_configs(read_persisted_bot_configs_unlocked())


def write_persisted_bot_configs(configs: list[dict[str, Any]]) -> None:
    with persisted_bot_configs_lock():
        write_persisted_bot_configs_unlocked(configs)


def upsert_persisted_bot_config(config: dict[str, Any]) -> None:
    with persisted_bot_configs_lock():
        stored = read_persisted_bot_configs_unlocked()
        config_id = str(config.get("id") or "").strip()
        updated = False
        for index, current in enumerate(stored):
            if str(current.get("id") or "").strip() == config_id:
                stored[index] = stamp_persisted_bot_config(config, current)
                updated = True
                break
        if not updated:
            stored.append(stamp_persisted_bot_config(config))
        write_persisted_bot_configs_unlocked(stored)


def remove_persisted_bot_config(bot_id: str) -> None:
    with persisted_bot_configs_lock():
        stored = read_persisted_bot_configs_unlocked()
        filtered = [item for item in stored if str(item.get("id") or "").strip() != bot_id]
        if len(filtered) != len(stored) or not DATA_FILE.exists():
            current_entries = read_persisted_bot_config_entries_unlocked()
            next_entries = replace_persisted_bot_config_entries(current_entries, filtered, drop_ids={bot_id})
            if next_entries != current_entries or not DATA_FILE.exists():
                write_json_atomic(DATA_FILE, next_entries)


def read_persisted_bot_config(bot_id: str) -> Optional[dict[str, Any]]:
    for item in read_persisted_bot_configs():
        if str(item.get("id") or "").strip() == bot_id:
            return item
    return None


def find_config_id_by_wecom_bot_id(wecom_bot_id: str) -> Optional[str]:
    target = str(wecom_bot_id or "").strip()
    if not target:
        return None
    for bot in BOTS.values():
        if bot_instance_is_stale(bot):
            continue
        if str(bot.config.get("botId") or "").strip() == target:
            return str(bot.config.get("id") or "").strip() or None
    for item in read_persisted_bot_configs():
        if str(item.get("botId") or "").strip() == target:
            return str(item.get("id") or "").strip() or None
    payload = read_bot_tombstone(target)
    if isinstance(payload, dict):
        config_id = str(payload.get("configId") or "").strip()
        if config_id:
            return config_id
    return None


def find_conflicting_config_id_by_wecom_bot_id(config_id: str, wecom_bot_id: str) -> Optional[str]:
    target_config_id = str(config_id or "").strip()
    existing_config_id = find_config_id_by_wecom_bot_id(wecom_bot_id)
    if existing_config_id and existing_config_id != target_config_id:
        return existing_config_id
    return None


def get_existing_bot_config(bot_id: str) -> Optional[dict[str, Any]]:
    bot = BOTS.get(bot_id)
    if bot and not bot_instance_is_stale(bot):
        return dict(bot.config)
    current = read_persisted_bot_config(bot_id)
    return dict(current) if current else None


def get_authoritative_bot_config(bot_id: str) -> Optional[dict[str, Any]]:
    current = read_persisted_bot_config(bot_id)
    if current:
        return dict(current)
    bot = BOTS.get(bot_id)
    if bot and not bot_instance_is_stale(bot):
        return dict(bot.config)
    return None


def invalidate_bot_session_threads(bot_id: str) -> int:
    invalidated = 0
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    ensure_dir(sessions_root)
    for session_file in sessions_root.glob("*.json"):
        record = read_json_file(session_file, None)
        if not isinstance(record, dict):
            continue
        if str(record.get("botId") or "") != bot_id:
            continue
        if record.get("threadId") is None:
            continue
        write_json_atomic(session_file, {**record, "threadId": None})
        invalidated += 1
    bot = BOTS.get(bot_id)
    if bot:
        for sess in bot.sessions.values():
            sess.thread_id = None
    return invalidated


def remove_path_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def remove_tree_if_exists(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)


def clear_directory_contents(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            remove_path_if_exists(child)


def sync_tree_contents(source: Path, target: Path) -> None:
    ensure_dir(target)
    for child in sorted(source.iterdir(), key=lambda item: item.name):
        destination = target / child.name
        if child.is_dir():
            shutil.copytree(child, destination, dirs_exist_ok=True)
        else:
            ensure_dir_for(destination)
            shutil.copy2(child, destination)


def copy_missing_tree_contents(source: Path, target: Path) -> None:
    ensure_dir(target)
    for child in sorted(source.iterdir(), key=lambda item: item.name):
        destination = target / child.name
        if destination.exists():
            if child.is_dir() and destination.is_dir():
                copy_missing_tree_contents(child, destination)
            continue
        if child.is_dir():
            shutil.copytree(child, destination)
        else:
            ensure_dir_for(destination)
            shutil.copy2(child, destination)


def sync_project_shared_skills_to_bridge_global() -> list[str]:
    source_root = project_shared_skills_root()
    target_root = get_bridge_global_skills_root()
    ensure_dir(get_bridge_codex_home_root())
    ensure_dir(target_root)
    clear_directory_contents(target_root)
    if not source_root.exists():
        return []
    synced: list[str] = []
    for child in sorted(source_root.iterdir(), key=lambda item: item.name):
        skill_file = child / "SKILL.md"
        if not child.is_dir() or not skill_file.is_file():
            continue
        sync_tree_contents(child, target_root / child.name)
        synced.append(child.name)
    return synced


def merge_source_skills_to_target(source_root: Path, target_root: Path) -> None:
    if not source_root.exists():
        return
    for child in sorted(source_root.iterdir(), key=lambda item: item.name):
        skill_file = child / "SKILL.md"
        if not child.is_dir() or not skill_file.is_file():
            continue
        if (target_root / child.name).exists():
            continue
        sync_tree_contents(child, target_root / child.name)


def build_codex_home_for_subprocess(session_id: str) -> Path:
    base_home = DEFAULT_CODEX_HOME
    bridge_home = get_session_codex_home_root(session_id)
    ensure_dir(bridge_home)

    if base_home.exists():
        for child in sorted(base_home.iterdir(), key=lambda item: item.name):
            # Skip volatile runtime trees. They are recreated per session and may
            # contain short-lived sandbox binaries that disappear while copying.
            if child.name in {"skills", "sessions", "tmp"}:
                continue
            destination = bridge_home / child.name
            if child.is_dir():
                shutil.copytree(child, destination, dirs_exist_ok=True)
            else:
                ensure_dir_for(destination)
                shutil.copy2(child, destination)

    ensure_dir(bridge_home / "sessions")
    ensure_dir(bridge_home / "tmp")
    ensure_dir(get_session_global_skills_root(session_id))
    clear_directory_contents(get_session_global_skills_root(session_id))
    merge_source_skills_to_target(base_home / "skills", get_session_global_skills_root(session_id))
    merge_source_skills_to_target(project_shared_skills_root(), get_session_global_skills_root(session_id))
    return bridge_home


def cleanup_bot_session_storage(bot_id: str) -> None:
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if sessions_root.exists():
        for session_file in sessions_root.glob("*.json"):
            record = normalize_session_record(read_json_file(session_file, None))
            if not record or str(record.get("botId") or "") != bot_id:
                continue
            remove_path_if_exists(Path(record["lockFile"]))
            remove_session_codex_home(record["sessionId"])
            remove_path_if_exists(session_file)
    remove_tree_if_exists(SESSION_REGISTRY_ROOT / "keys" / bot_id)
    remove_tree_if_exists(SESSION_LOCK_ROOT / bot_id)
    remove_tree_if_exists(get_bot_workspace_dir(bot_id))


def cleanup_bot_schedule_jobs(bot_id: str) -> None:
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT, SCHEDULE_DONE_ROOT, SCHEDULE_FAILED_ROOT):
        if not root.exists():
            continue
        for job_file in root.glob("*.json"):
            job = read_json_file(job_file, None)
            if isinstance(job, dict) and str(job.get("botId") or "") == bot_id:
                remove_path_if_exists(job_file)


def cleanup_bot_schedule_definitions(bot_id: str) -> None:
    schedule_ids = [item["scheduleId"] for item in list_schedule_definitions() if str(item.get("botId") or "") == bot_id]
    for schedule_id in schedule_ids:
        delete_schedule_definition(schedule_id)
        remove_path_if_exists(get_schedule_definition_lock_file(schedule_id))
    cleanup_bot_schedule_jobs(bot_id)


async def remove_deleted_bots_from_memory_once() -> None:
    for bot_id, bot in list(BOTS.items()):
        if not bot_instance_is_stale(bot):
            continue
        bot.config["enabled"] = False
        await stop_bot(bot_id, persist_disable=False)
        BOTS.pop(bot_id, None)
        PREPARED_PREVIOUS_BOT_CONFIGS.pop(bot_id, None)


def bot_secret_source(config: dict[str, Any]) -> str:
    if str(config.get("secretFile") or "").strip():
        return "file"
    if str(config.get("secret") or "").strip():
        return "memory"
    return "missing"


def bot_secret_persistence_warning(config: dict[str, Any]) -> Optional[str]:
    if str(config.get("secretFile") or "").strip():
        return None
    if str(config.get("secret") or "").strip():
        return "secret is only kept in memory and will not be written to .bots.json; use secretFile or env bootstrap for restart survival"
    return "secret is not configured"


def filter_deleted_persisted_bot_configs(configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    changed = False
    for item in configs:
        wecom_bot_id = str(item.get("botId") or "").strip()
        if wecom_bot_id:
            deleted_at = bot_tombstone_deleted_at(wecom_bot_id)
            if deleted_at:
                updated_at = normalize_optional_int(item.get("updatedAt") or item.get("updated_at"))
                created_at = normalize_optional_int(item.get("createdAt") or item.get("created_at"))
                config_generation = updated_at if updated_at is not None else created_at
                if config_generation is None or config_generation <= deleted_at:
                    changed = True
                    continue
        filtered.append(item)
    if changed:
        write_persisted_bot_configs_unlocked(filtered)
    return filtered


def filter_deleted_bot_configs(configs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in configs:
        wecom_bot_id = str(item.get("botId") or "").strip()
        if not wecom_bot_id:
            filtered.append(item)
            continue
        deleted_at = bot_tombstone_deleted_at(wecom_bot_id)
        if not deleted_at:
            filtered.append(item)
            continue
        updated_at = normalize_optional_int(item.get("updatedAt") or item.get("updated_at"))
        created_at = normalize_optional_int(item.get("createdAt") or item.get("created_at"))
        config_generation = updated_at if updated_at is not None else created_at
        if config_generation is None or config_generation <= deleted_at:
            continue
        filtered.append(item)
    return filtered


def prepare_bot_configs() -> list[dict[str, Any]]:
    PREPARED_PREVIOUS_BOT_CONFIGS.clear()
    raw_bootstrap = load_env_bootstrap_bot_configs()
    with persisted_bot_configs_lock():
        stored = filter_deleted_persisted_bot_configs(read_persisted_bot_configs_unlocked())
        bootstrap = filter_deleted_bot_configs(raw_bootstrap)
        runtime_payload = stored if not bootstrap else merge_bot_configs(stored, bootstrap)
        seen_bot_ids: dict[str, str] = {}
        for item in runtime_payload:
            config_id = str(item.get("id") or "").strip()
            wecom_bot_id = str(item.get("botId") or "").strip()
            if not config_id or not wecom_bot_id:
                continue
            existing_config_id = seen_bot_ids.get(wecom_bot_id)
            if existing_config_id and existing_config_id != config_id:
                raise BridgeError(500, f"duplicate botId in persisted/bootstrap configs: {wecom_bot_id} ({existing_config_id}, {config_id})")
            seen_bot_ids[wecom_bot_id] = config_id
        previous_by_id = {
            str(item.get("id") or "").strip(): dict(item)
            for item in stored
            if str(item.get("id") or "").strip()
        }
        for item in runtime_payload:
            config_id = str(item.get("id") or "").strip()
            if config_id and config_id in previous_by_id:
                PREPARED_PREVIOUS_BOT_CONFIGS[config_id] = previous_by_id[config_id]
        stamped_runtime_payload = [
            stamp_persisted_bot_config(item, previous_by_id.get(str(item.get("id") or "").strip()))
            for item in runtime_payload
            if isinstance(item, dict)
        ]
        persisted_payload = serialize_bot_configs_for_disk(stamped_runtime_payload)
        current_persisted = serialize_bot_configs_for_disk(stored)
        if persisted_payload != current_persisted or not DATA_FILE.exists():
            write_persisted_bot_configs_unlocked(stamped_runtime_payload)
    if bootstrap:
        print_log(f"[INIT] bootstrapped {len(bootstrap)} bot config(s) from env")
    return runtime_payload


def save_bots() -> None:
    with persisted_bot_configs_lock():
        stored = read_persisted_bot_configs_unlocked()
        index_by_id = {
            str(item.get("id") or "").strip(): idx
            for idx, item in enumerate(stored)
            if str(item.get("id") or "").strip()
        }
        for bot in BOTS.values():
            config_id = str(bot.config.get("id") or "").strip()
            if bot_instance_is_stale(bot):
                continue
            if config_id in index_by_id:
                stored[index_by_id[config_id]] = stamp_persisted_bot_config(bot.config, stored[index_by_id[config_id]])
            else:
                index_by_id[config_id] = len(stored)
                stored.append(stamp_persisted_bot_config(bot.config))
        write_persisted_bot_configs_unlocked(stored)


async def cancel_task(task: Optional[asyncio.Task]) -> None:
    if not task:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def terminate_process(process: Optional[asyncio.subprocess.Process], timeout_sec: int = 5) -> None:
    if not process:
        return
    if process.returncode is None:
        process.kill()
    try:
        await asyncio.wait_for(process.wait(), timeout_sec)
    except Exception:
        pass


async def stop_bot(bot_id: str, persist_disable: bool = True, persist_state: bool = True) -> None:
    bot = BOTS.get(bot_id)
    if not bot:
        return
    add_log(bot, "stopping")
    if persist_disable:
        bot.config["enabled"] = False
    if bot.ws and not bot.ws.closed:
        await bot.ws.close()
    await cancel_task(bot.heartbeat_task)
    await cancel_task(bot.reader_task)
    await cancel_task(bot.runner_task)
    await cancel_task(bot.upload_worker_task)
    release_bot_runtime_lock(bot)
    reject_pending_requests(bot, "bot websocket closed")
    session_run_tasks: list[asyncio.Task] = []
    session_processes: list[asyncio.subprocess.Process] = []
    for key, sess in list(bot.sessions.items()):
        if sess.run_task and not sess.run_task.done():
            session_run_tasks.append(sess.run_task)
        if sess.proc and sess.proc.returncode is None:
            session_processes.append(sess.proc)
        interrupt_session(bot, key, sess, clear_thread=False, clear_chat=False, clear_queue=True)
    for task in session_run_tasks:
        await cancel_task(task)
    for process in session_processes:
        await terminate_process(process)
    bot.sessions.clear()
    bot.status = "stopped"
    if persist_state and not bot_instance_is_stale(bot):
        upsert_persisted_bot_config(bot.config)


async def remove_bot(bot_id: str) -> None:
    existing_config = get_authoritative_bot_config(bot_id)
    if existing_config is None:
        raise BridgeError(404, f"bot not found: {bot_id}")
    wecom_bot_id = str((existing_config or {}).get("botId") or "").strip() or None
    mark_bot_deleted_globally(bot_id, wecom_bot_id)
    remove_persisted_bot_config(bot_id)
    try:
        await stop_bot(bot_id, persist_disable=False)
        cleanup_bot_schedule_definitions(bot_id)
        cleanup_bot_session_storage(bot_id)
    finally:
        BOTS.pop(bot_id, None)
        PREPARED_PREVIOUS_BOT_CONFIGS.pop(bot_id, None)


async def handle_wecom_message(bot: BotState, message: dict[str, Any]) -> None:
    req_id = ((message.get("headers") or {}).get("req_id"))
    if req_id and resolve_request_future(bot, req_id, message):
        return

    if message.get("errcode") is not None and not message.get("cmd"):
        body = message.get("body") or {}
        if message.get("errcode") != 0:
            add_log(bot, f"WeCom response error: {message.get('errcode')} {message.get('errmsg', '')}")
        return

    event_key = (
        (message.get("body") or {}).get("msgid")
        or f"{((message.get('headers') or {}).get('req_id') or '')}:{((message.get('body') or {}).get('msgtype') or '')}:{chat_key_for_bot(bot, message)}"
    )
    prune_recent_events()
    if event_key in RECENT_EVENTS:
        add_log(bot, f"ignore duplicate event: {event_key}")
        return
    RECENT_EVENTS[event_key] = time.time() + RECENT_EVENT_TTL

    command = message.get("cmd")
    body = message.get("body") or {}
    msg_type = body.get("msgtype")

    if command == "aibot_msg_callback" and msg_type == "text":
        content = strip_text_mentions(((body.get("text") or {}).get("content") or ""), bot.config.get("name"))
        key = chat_key_for_bot(bot, message)
        add_log(bot, f'recv: "{content}" {key}')
        try:
            if content == "/bridge-resume":
                await start_resume_selection_command(bot, key, req_id)
                return
            if content.startswith("/bridge-resume "):
                await resume_session_command(bot, key, req_id, content.split(None, 1)[1].strip())
                return
            if content == "/bridge-interrupt":
                await interrupt_session_command(bot, key, req_id)
                return
            if content == "/bridge-reset":
                await reset_session_command(bot, key, req_id)
                return
            if content == "/bridge-status":
                await status_session_command(bot, key, req_id)
                return
            current_sess = bot.sessions.get(key)
            if current_sess and await apply_resume_selection_command(bot, key, req_id, content):
                return
        except BridgeError as exc:
            await respond_info(bot, req_id, f"Bridge command failed: {exc.message}")
            return
        if content:
            await enqueue_message(bot, key, content, req_id)
        return

    if command == "aibot_msg_callback" and msg_type == "image":
        key = chat_key_for_bot(bot, message)
        add_log(bot, f"recv image {key}")
        await enqueue_media_message(bot, message, "image")
        return

    if command == "aibot_msg_callback" and msg_type == "file":
        key = chat_key_for_bot(bot, message)
        add_log(bot, f"recv file {key}")
        await enqueue_media_message(bot, message, "file")
        return

    if command == "aibot_msg_callback" and msg_type == "mixed":
        key = chat_key_for_bot(bot, message)
        add_log(bot, f"recv mixed {key}")
        await enqueue_mixed_message(bot, message)
        return

    if command == "aibot_event_callback" and ((body.get("event") or {}).get("eventtype") == "enter_chat"):
        welcome = bot.config.get("welcome") or "Send me a message and I will route it to Codex."
        await send_ws_payload(
            bot,
            {
                "cmd": "aibot_respond_welcome_msg",
                "headers": {"req_id": req_id},
                "body": {"msgtype": "text", "text": {"content": welcome}},
            },
        )


async def ws_reader_loop(bot: BotState, ws: aiohttp.ClientWebSocketResponse) -> None:
    async for raw_msg in ws:
        if raw_msg.type == WSMsgType.TEXT:
            try:
                payload = json.loads(raw_msg.data)
            except Exception:
                continue
            try:
                await handle_wecom_message(bot, payload)
            except BridgeError as exc:
                add_log(bot, f"message handling error: {exc.message}")
                req_id = ((payload.get("headers") or {}).get("req_id"))
                await respond_info(bot, req_id, f"Bridge error: {exc.message}")
            except Exception as exc:
                add_log(bot, f"message handling crash: {exc}")
        elif raw_msg.type == WSMsgType.ERROR:
            raise BridgeError(503, f"websocket error: {ws.exception()}")
        elif raw_msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
            break


async def heartbeat_loop(bot: BotState) -> None:
    while bot.ws and not bot.ws.closed and bot.config.get("enabled", True):
        await asyncio.sleep(30)
        if not bot.ws or bot.ws.closed:
            return
        await send_ws_payload(bot, {"cmd": "ping", "headers": {"req_id": uid()}})


async def bot_runner(bot: BotState) -> None:
    while bot.config.get("enabled", True) and not SHUTDOWN_EVENT.is_set():
        if not acquire_bot_runtime_lock(bot):
            if bot.status != "standby":
                bot.status = "standby"
                add_log(bot, "WeCom standby: bot runtime is owned by another instance")
                add_event_log(bot, "wecom.standby")
            await asyncio.sleep(5)
            continue
        if bot.status == "standby":
            add_log(bot, "WeCom runtime lock acquired")
            add_event_log(bot, "wecom.runtime_lock_acquired")
        add_log(bot, "connecting WeCom...")
        add_event_log(bot, "wecom.connecting")
        try:
            assert HTTP_SESSION is not None
            async with HTTP_SESSION.ws_connect(WECOM_WS, heartbeat=None, autoping=True) as ws:
                bot.ws = ws
                bot.status = "connecting"
                bot.reader_task = asyncio.create_task(ws_reader_loop(bot, ws))
                sub_req_id = uid()
                sub_future = create_request_future(bot, sub_req_id)
                add_log(
                    bot,
                    f"subscribe request: req_id={sub_req_id} bot_id={bot.config['botId']} secret_len={len(bot.config['secret'])}",
                )
                add_event_log(bot, "wecom.subscribe_request", reqId=sub_req_id, wecomBotId=bot.config["botId"])
                await send_ws_payload(
                    bot,
                    {
                        "cmd": "aibot_subscribe",
                        "headers": {"req_id": sub_req_id},
                        "body": {"bot_id": bot.config["botId"], "secret": bot.config["secret"]},
                    },
                )
                try:
                    response = await asyncio.wait_for(sub_future, 30)
                    add_log(bot, f"subscribe response: {json.dumps(response, ensure_ascii=False)}")
                    if response.get("errcode") == 0:
                        bot.status = "running"
                        add_log(bot, "subscribed")
                        add_event_log(bot, "wecom.subscribed", reqId=sub_req_id)
                        await flush_all_pending_session_payloads(bot)
                    else:
                        bot.status = "error"
                        add_log(bot, f"subscribe failed: {response.get('errcode')} {response.get('errmsg', '')}")
                        add_event_log(
                            bot,
                            "wecom.subscribe_failed",
                            reqId=sub_req_id,
                            errcode=response.get("errcode"),
                            errmsg=response.get("errmsg"),
                        )
                except asyncio.TimeoutError:
                    bot.status = "error"
                    add_log(bot, "subscribe timeout")
                    add_event_log(bot, "wecom.subscribe_timeout", reqId=sub_req_id)
                bot.heartbeat_task = asyncio.create_task(heartbeat_loop(bot))
                if bot.reader_task:
                    await bot.reader_task
        except asyncio.CancelledError:
            break
        except Exception as exc:
            add_log(bot, f"WeCom error: {exc}")
        finally:
            await cancel_task(bot.heartbeat_task)
            bot.heartbeat_task = None
            await cancel_task(bot.reader_task)
            bot.reader_task = None
            reject_pending_requests(bot, "bot websocket closed")
            if bot.ws and not bot.ws.closed:
                await bot.ws.close()
            bot.ws = None
            release_bot_runtime_lock(bot)
            if bot.config.get("enabled", True):
                bot.status = "disconnected"
                add_log(bot, "WeCom disconnected")
                add_event_log(bot, "wecom.disconnected")

        if bot.config.get("enabled", True) and not SHUTDOWN_EVENT.is_set():
            await asyncio.sleep(5)


async def start_bot(config: dict[str, Any]) -> BotState:
    candidate = {**config, "enabled": True}
    if str(candidate.get("secretFile") or "").strip():
        candidate.pop("secret", None)
    normalized = normalize_bot_config(candidate, allow_inline_secret=True)
    conflicting_config_id = find_conflicting_config_id_by_wecom_bot_id(normalized["id"], normalized["botId"])
    if conflicting_config_id:
        raise BridgeError(409, f"botId already managed by another config: {normalized['botId']} ({conflicting_config_id})")
    previous_config = BOTS.get(normalized["id"]).config if normalized["id"] in BOTS else None
    if previous_config is None:
        previous_config = PREPARED_PREVIOUS_BOT_CONFIGS.pop(normalized["id"], None)
    if previous_config is None:
        previous_config = read_persisted_bot_config(normalized["id"])
    if previous_config and str(previous_config.get("workDir") or "") != normalized["workDir"]:
        invalidate_bot_session_threads(normalized["id"])
    if normalized["id"] in BOTS:
        await stop_bot(normalized["id"], persist_disable=False)
    bot = BotState(config=normalized)
    BOTS[normalized["id"]] = bot
    add_log(bot, "starting...")
    bot.upload_worker_task = asyncio.create_task(upload_worker(bot))
    bot.runner_task = asyncio.create_task(bot_runner(bot))
    upsert_persisted_bot_config(bot.config)
    return bot


async def load_bots() -> None:
    payload = prepare_bot_configs()
    try:
        for config in payload:
            if not isinstance(config, dict) or config.get("enabled") is False:
                continue
            try:
                await start_bot(config)
            except Exception as exc:
                print_log(f"[INIT] skip invalid bot {config.get('name') or config.get('id')}: {exc}")
    finally:
        PREPARED_PREVIOUS_BOT_CONFIGS.clear()
    print_log(f"[INIT] loaded {len(payload)} bot configs")


def prune_recent_events() -> None:
    now = time.time()
    expired = [key for key, expires_at in RECENT_EVENTS.items() if expires_at <= now]
    for key in expired:
        RECENT_EVENTS.pop(key, None)


async def enqueue_message(
    bot: BotState,
    key: str,
    text: str,
    req_id: Optional[str],
    silent_lease_failure: bool = False,
    scheduled_job_file: Optional[str] = None,
    schedule_id: Optional[str] = None,
    schedule_request_id: Optional[str] = None,
) -> bool:
    try:
        sess = get_or_create_session(bot, key)
    except Exception as exc:
        if not silent_lease_failure:
            add_log(bot, f"[{key}] init failed: {exc}")
            await respond_queued_error(bot, req_id, f"init failed: {exc}")
        return False
    if not acquire_session_lease(bot, sess, key):
        if not silent_lease_failure:
            add_log(bot, f"[{key}] lease owned by another process, ignore message")
        return False
    register_reply_session(bot, req_id, sess)
    queue_position = len(sess.queue) + (1 if sess.running else 0) + 1
    sess.queue.append(
        {
            "text": text,
            "reqId": req_id,
            "scheduledJobFile": scheduled_job_file,
            "scheduleId": schedule_id,
            "scheduleRequestId": schedule_request_id,
        }
    )
    sess.chat.append({"role": "user", "text": text, "from": key, "time": int(time.time())})
    if len(sess.chat) > 200:
        del sess.chat[: len(sess.chat) - 200]
    if queue_position > 1:
        if req_id:
            await respond_info(bot, req_id, build_queued_proactive_notice(queue_position))
            mark_reply_proactive(sess, req_id)
        else:
            await send_transient_session_status(bot, key, sess, req_id, build_queue_status_text(queue_position))
    if sess.pending_media_downloads > 0:
        await respond_session_info(
            bot,
            key,
            sess,
            req_id,
            "Attachments are still downloading. The question will continue immediately, but some media may not yet be available.",
            final=False,
        )
    process_queue(bot, sess, key)
    return True


async def enqueue_media_message(bot: BotState, message: dict[str, Any], kind: str) -> None:
    key = chat_key_for_bot(bot, message)
    req_id = ((message.get("headers") or {}).get("req_id"))
    try:
        sess = get_or_create_session(bot, key)
    except Exception as exc:
        add_log(bot, f"[{key}] media session init failed: {exc}")
        await respond_queued_error(bot, req_id, f"init failed: {exc}")
        return
    if not acquire_session_lease(bot, sess, key):
        add_log(bot, f"[{key}] lease owned by another process, ignore media message")
        return
    register_reply_session(bot, req_id, sess)
    sess.pending_media_downloads += 1
    await respond_info(bot, req_id, f"Receiving {kind}...", final=False)
    try:
        media = await download_incoming_media(bot, sess, key, kind, (message.get("body") or {}).get(kind) or {})
        sess.pending_media.append(media)
        add_log(bot, f"[{key}] {kind} added to session context")
        await respond_info(bot, req_id, f"Received {kind}. You can continue asking.")
    except Exception as exc:
        add_log(bot, f"[{key}] receive {kind} failed: {exc}")
        sess.pending_media_notes.append(
            f"{kind} receive failed: {exc}. If the user asks about this attachment, say it was not received successfully."
        )
        await respond_info(bot, req_id, f"Failed to receive {kind}.")
    finally:
        sess.pending_media_downloads = max(0, sess.pending_media_downloads - 1)


async def enqueue_mixed_message(bot: BotState, message: dict[str, Any]) -> None:
    key = chat_key_for_bot(bot, message)
    req_id = ((message.get("headers") or {}).get("req_id"))
    try:
        sess = get_or_create_session(bot, key)
    except Exception as exc:
        add_log(bot, f"[{key}] mixed session init failed: {exc}")
        await respond_queued_error(bot, req_id, f"init failed: {exc}")
        return
    if not acquire_session_lease(bot, sess, key):
        add_log(bot, f"[{key}] lease owned by another process, ignore mixed message")
        return
    register_reply_session(bot, req_id, sess)

    mixed = (message.get("body") or {}).get("mixed") or {}
    mixed_text = extract_mixed_text(mixed)
    images = extract_mixed_images(mixed)

    for image in images:
        sess.pending_media_downloads += 1
        try:
            media = await download_incoming_media(bot, sess, key, "image", image or {})
            sess.pending_media.append(media)
            add_log(bot, f"[{key}] mixed image added to session context")
        except Exception as exc:
            add_log(bot, f"[{key}] receive mixed image failed: {exc}")
            sess.pending_media_notes.append(f"mixed image receive failed: {exc}.")
        finally:
            sess.pending_media_downloads = max(0, sess.pending_media_downloads - 1)

    if images:
        await respond_info(bot, req_id, "Mixed message images processed.", final=not bool(mixed_text))
    if mixed_text:
        await enqueue_message(bot, key, mixed_text, req_id)


async def session_recycler_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await asyncio.sleep(60)
            now = time.time()
            for bot in list(BOTS.values()):
                for key, sess in list(bot.sessions.items()):
                    if not sess.running and now - sess.last_active > SESSION_TTL:
                        recycle_session(bot, sess, key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] session_recycler_loop error: {exc}")


async def lease_renew_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await asyncio.sleep(SESSION_LEASE_RENEW_MS / 1000)
            for bot in list(BOTS.values()):
                for key, sess in list(bot.sessions.items()):
                    if sess.running or sess.queue or sess.pending_media_downloads > 0:
                        renew_session_lease(bot, key, sess)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] lease_renew_loop error: {exc}")


async def local_file_send_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await process_local_file_send_queue_once()
            await asyncio.sleep(LOCAL_FILE_SEND_POLL_MS / 1000)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] local_file_send_loop error: {exc}")
            if not SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(1)


async def scheduled_message_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await process_scheduled_messages_once()
            await asyncio.sleep(SCHEDULE_POLL_MS / 1000)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] scheduled_message_loop error: {exc}")
            if not SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(1)


async def schedule_definition_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await process_schedule_definitions_once()
            await asyncio.sleep(SCHEDULE_DEFINITION_POLL_MS / 1000)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] schedule_definition_loop error: {exc}")
            if not SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(1)


async def paused_session_recovery_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await asyncio.sleep(5)
            for bot in list(BOTS.values()):
                for key, sess in list(bot.sessions.items()):
                    if sess.running or not sess.queue or sess.lease_owned:
                        continue
                    if acquire_session_lease(bot, sess, key):
                        add_log(bot, f"[{key}] resumed paused queue after reacquiring lease")
                        process_queue(bot, sess, key)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] paused_session_recovery_loop error: {exc}")


async def reconcile_bots_once() -> None:
    with persisted_bot_configs_lock():
        persisted_configs = filter_deleted_persisted_bot_configs(read_persisted_bot_configs_unlocked())
    desired_by_id: dict[str, dict[str, Any]] = {}
    for stored in persisted_configs:
        try:
            runtime = normalize_bot_config(stored, allow_inline_secret=True)
        except BridgeError as exc:
            print_log(f"[SYNC] skip invalid persisted bot {stored.get('name') or stored.get('id')}: {exc.message}")
            continue
        desired_by_id[str(runtime.get("id") or "").strip()] = runtime

    for bot_id, bot in list(BOTS.items()):
        if bot_instance_is_stale(bot):
            continue
        desired = desired_by_id.get(bot_id)
        if not desired:
            await stop_bot(bot_id, persist_disable=False, persist_state=False)
            BOTS.pop(bot_id, None)
            PREPARED_PREVIOUS_BOT_CONFIGS.pop(bot_id, None)
            continue
        if desired.get("enabled") is False:
            if bot.config.get("enabled", True):
                bot.config = {**bot.config, **desired, "enabled": False}
                await stop_bot(bot_id, persist_disable=False)
            continue
        if bot.config != desired:
            await start_bot(dict(desired))

    for bot_id, desired in desired_by_id.items():
        if desired.get("enabled") is False:
            continue
        if bot_id in BOTS:
            continue
        await start_bot(dict(desired))


async def bot_config_reconciler_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await asyncio.sleep(5)
            await reconcile_bots_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] bot_config_reconciler_loop error: {exc}")


async def deleted_bot_reaper_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await asyncio.sleep(5)
            await remove_deleted_bots_from_memory_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] deleted_bot_reaper_loop error: {exc}")


def extract_request_token(request: web.Request) -> str:
    auth = request.headers.get("Authorization", "").strip()
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return request.headers.get("X-Bridge-Token", "").strip()


def extract_basic_auth(request: web.Request) -> str:
    auth = request.headers.get("Authorization", "").strip()
    if not auth.startswith("Basic "):
        return ""
    encoded = auth[6:].strip()
    if not encoded:
        return ""
    try:
        return base64.b64decode(encoded, validate=True).decode("utf-8")
    except Exception:
        return ""


def api_auth_enabled() -> bool:
    return bool(BRIDGE_TOKEN or BRIDGE_BASIC_AUTH)


def api_auth_mode_label() -> str:
    if BRIDGE_TOKEN and BRIDGE_BASIC_AUTH:
        return "token or basic auth required"
    if BRIDGE_BASIC_AUTH:
        return "basic auth required"
    if BRIDGE_TOKEN:
        return "token required"
    return "localhost only"


def build_api_auth_error(message: str, status: int) -> web.Response:
    headers = {}
    if BRIDGE_BASIC_AUTH:
        headers["WWW-Authenticate"] = 'Basic realm="WeCom Codex Bridge"'
    return web.json_response({"ok": False, "error": message}, status=status, headers=headers)


def request_is_loopback(request: web.Request) -> bool:
    remote = request.remote or ""
    return remote in {"127.0.0.1", "::1", "::ffff:127.0.0.1", "localhost"}


async def require_api_access(request: web.Request) -> Optional[web.Response]:
    if api_auth_enabled():
        if BRIDGE_TOKEN and extract_request_token(request) == BRIDGE_TOKEN:
            return None
        if BRIDGE_BASIC_AUTH and extract_basic_auth(request) == BRIDGE_BASIC_AUTH:
            return None
        return build_api_auth_error(f"API auth required ({api_auth_mode_label()})", 401)
    if request_is_loopback(request):
        return None
    return web.json_response({"ok": False, "error": "API is limited to localhost unless BRIDGE_TOKEN or BRIDGE_BASIC_AUTH is configured"}, status=403)


async def read_json_body(request: web.Request) -> dict[str, Any]:
    body = await request.read()
    if len(body) > MAX_JSON_BODY:
        raise BridgeError(413, f"request body too large (max {MAX_JSON_BODY} bytes)")
    if not body:
        return {}
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise BridgeError(400, f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise BridgeError(400, "JSON body must be an object")
    return payload


def bot_to_payload(bot: BotState) -> dict[str, Any]:
    sessions = []
    for key, sess in bot.sessions.items():
        sessions.append(
            {
                "key": key,
                "sessionId": sess.session_id,
                "threadId": sess.thread_id,
                "status": "running" if sess.running else "idle",
                "alive": sess.running,
                "busy": sess.running,
                "queueLen": len(sess.queue),
                "lastActive": int(sess.last_active * 1000),
                "chatLen": len(sess.chat),
            }
        )
    return {
        "id": bot.config["id"],
        "name": bot.config["name"],
        "botId": bot.config["botId"],
        "workDir": bot.config["workDir"],
        "groupSessionMode": bot.config.get("groupSessionMode", "per-user"),
        "secretSource": bot_secret_source(bot.config),
        "status": bot.status,
        "enabled": bot.config["enabled"],
        "logs": list(bot.logs)[-30:],
        "sessions": sessions,
    }


def persisted_bot_to_payload(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": config["id"],
        "name": config["name"],
        "botId": config["botId"],
        "workDir": config["workDir"],
        "groupSessionMode": config.get("groupSessionMode", "per-user"),
        "secretSource": bot_secret_source(config),
        "status": "disabled" if config.get("enabled") is False else "not_loaded",
        "enabled": config.get("enabled", True) is not False,
        "logs": [],
        "sessions": [],
    }


async def api_get_bots(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    payload_by_id = {
        bot.config["id"]: bot_to_payload(bot)
        for bot in BOTS.values()
        if not bot_instance_is_stale(bot)
    }
    for config in read_persisted_bot_configs():
        config_id = str(config.get("id") or "").strip()
        if not config_id or config_id in payload_by_id:
            continue
        payload_by_id[config_id] = persisted_bot_to_payload(config)
    return web.json_response(list(payload_by_id.values()))


async def api_add_bot(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        data = await read_json_body(request)
        wecom_bot_id = str(data.get("botId") or "").strip()
        config_id = find_config_id_by_wecom_bot_id(wecom_bot_id) or default_bot_config_id(wecom_bot_id)
        config = normalize_bot_config({**data, "id": config_id, "enabled": True})
        await start_bot(config)
        payload = {"ok": True, "id": config["id"], "secretSource": bot_secret_source(config)}
        warning = bot_secret_persistence_warning(config)
        if warning:
            payload["warning"] = warning
        return web.json_response(payload)
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_delete_bot(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        await remove_bot(request.match_info["bot_id"])
        return web.json_response({"ok": True})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_restart_bot(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        bot_id = request.match_info["bot_id"]
        current = get_authoritative_bot_config(bot_id)
        if not current:
            return web.json_response({"ok": False, "error": "bot not found"}, status=404)
        updated = {**current, "enabled": True, "restartToken": uid()}
        upsert_persisted_bot_config(updated)
        if bot_id in BOTS:
            BOTS[bot_id].config = dict(updated)
            await start_bot(dict(updated))
        return web.json_response({"ok": True})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_stop_bot(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        bot_id = request.match_info["bot_id"]
        current = get_authoritative_bot_config(bot_id)
        if not current:
            return web.json_response({"ok": False, "error": "bot not found"}, status=404)
        updated = {**current, "enabled": False}
        upsert_persisted_bot_config(updated)
        if bot_id in BOTS:
            BOTS[bot_id].config = dict(updated)
            await stop_bot(bot_id, persist_disable=False)
        return web.json_response({"ok": True})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_get_chat(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    bot = BOTS.get(request.match_info["bot_id"])
    key = unquote(request.match_info["chat_key"])
    sess = bot.sessions.get(key) if bot else None
    return web.json_response(sess.chat if sess else [])


async def api_interrupt_session(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    bot = BOTS.get(request.match_info["bot_id"])
    if not bot:
        return web.json_response({"ok": False, "error": "bot not found"}, status=404)
    key = unquote(request.match_info["chat_key"])
    try:
        result = control_session(bot, key, clear_thread=False, clear_chat=False)
        sess = bot.sessions.get(key)
        if sess:
            resume_session_queue(bot, sess, key, "interrupt api")
        return web.json_response({"ok": True, "action": "interrupt", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_reset_session(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    bot = BOTS.get(request.match_info["bot_id"])
    if not bot:
        return web.json_response({"ok": False, "error": "bot not found"}, status=404)
    key = unquote(request.match_info["chat_key"])
    try:
        result = control_session(bot, key, clear_thread=True, clear_chat=True)
        return web.json_response({"ok": True, "action": "reset", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_send_file(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        data = await read_json_body(request)
        return web.json_response(submit_file_send_request(data))
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_get_schedules(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    return web.json_response(list_schedule_definitions())


async def api_add_schedule(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        data = await read_json_body(request)
        return web.json_response(submit_schedule_definition_request(data))
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_get_schedule(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    schedule = read_schedule_definition(request.match_info["schedule_id"])
    if not schedule:
        return web.json_response({"ok": False, "error": "schedule not found"}, status=404)
    return web.json_response(schedule)


async def api_pause_schedule(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        result = pause_schedule_definition(request.match_info["schedule_id"])
        return web.json_response({"ok": True, "action": "pause", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_resume_schedule(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        result = resume_schedule_definition(request.match_info["schedule_id"])
        return web.json_response({"ok": True, "action": "resume", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_delete_schedule(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        delete_schedule_definition(request.match_info["schedule_id"])
        return web.json_response({"ok": True})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_schedule_message(request: web.Request) -> web.Response:
    denied = await require_api_access(request)
    if denied:
        return denied
    try:
        data = await read_json_body(request)
        return web.json_response(submit_schedule_message_request(data))
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_root(_: web.Request) -> web.Response:
    return web.json_response(
        {
            "ok": True,
            "name": "WeCom Codex Bridge Python",
            "ui": False,
            "host": HOST,
            "port": PORT,
            "codexExecMode": CODEX_EXEC_MODE,
            "maxConcurrentCodexRuns": MAX_CONCURRENT_CODEX_RUNS,
        }
    )


async def app_cleanup(_: web.Application) -> None:
    SHUTDOWN_EVENT.set()
    for bot_id in list(BOTS.keys()):
        await stop_bot(bot_id, persist_disable=False)
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", api_root)
    app.router.add_get("/api/bots", api_get_bots)
    app.router.add_post("/api/bots", api_add_bot)
    app.router.add_delete("/api/bots/{bot_id}", api_delete_bot)
    app.router.add_post("/api/bots/{bot_id}/restart", api_restart_bot)
    app.router.add_post("/api/bots/{bot_id}/stop", api_stop_bot)
    app.router.add_get("/api/bots/{bot_id}/sessions/{chat_key}/chat", api_get_chat)
    app.router.add_post("/api/bots/{bot_id}/sessions/{chat_key}/interrupt", api_interrupt_session)
    app.router.add_post("/api/bots/{bot_id}/sessions/{chat_key}/reset", api_reset_session)
    app.router.add_post("/api/send-file", api_send_file)
    app.router.add_get("/api/schedules", api_get_schedules)
    app.router.add_post("/api/schedules", api_add_schedule)
    app.router.add_get("/api/schedules/{schedule_id}", api_get_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/pause", api_pause_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/resume", api_resume_schedule)
    app.router.add_delete("/api/schedules/{schedule_id}", api_delete_schedule)
    app.router.add_post("/api/schedule-message", api_schedule_message)
    app.on_cleanup.append(app_cleanup)
    return app


async def main() -> None:
    global HTTP_SESSION, CODEX_RUN_SEMAPHORE

    if HOST not in {"127.0.0.1", "::1", "localhost"} and not api_auth_enabled():
        print_log("[SECURITY] HOST is not loopback but API auth is not configured. Refusing to start.")
        raise SystemExit(1)
    if CODEX_EXEC_MODE not in VALID_CODEX_EXEC_MODES:
        print_log(f"[CONFIG] invalid CODEX_EXEC_MODE={CODEX_EXEC_MODE!r}; expected one of {sorted(VALID_CODEX_EXEC_MODES)}")
        raise SystemExit(1)

    ensure_local_file_send_dirs()
    ensure_schedule_dirs()
    ensure_dir(BOT_TOMBSTONE_ROOT)
    ensure_dir(BOT_RUNTIME_LOCK_ROOT)
    ensure_dir(SESSION_LOCK_ROOT)
    ensure_dir(SESSION_REGISTRY_ROOT / "keys")
    ensure_dir(SESSION_REGISTRY_ROOT / "sessions")
    ensure_dir(CHATFILE_ROOT)
    ensure_dir(WORKSPACE_ROOT)
    ensure_dir(USER_ALIAS_ROOT)
    ensure_dir(get_bridge_codex_home_root())
    ensure_dir(get_bridge_global_skills_root())
    if not DATA_FILE.exists():
        write_json_atomic(DATA_FILE, [])

    HTTP_SESSION = aiohttp.ClientSession(trust_env=True)
    CODEX_RUN_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_CODEX_RUNS)

    app = build_app()
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    print_log(f"WeCom Codex Bridge Python: {BRIDGE_API_BASE}")
    print_log(f"[SECURITY] API access: {api_auth_mode_label()}")
    print_log(f"[CODEX] exec mode: {CODEX_EXEC_MODE}")
    print_log(f"[CODEX] max concurrent runs: {MAX_CONCURRENT_CODEX_RUNS}")

    maybe_migrate_legacy_shared_runtime_state()
    maybe_migrate_legacy_instance_runtime_state()
    sync_project_shared_skills_to_bridge_global()
    sanitize_all_session_records()

    await load_bots()

    recycler_task = asyncio.create_task(session_recycler_loop())
    renew_task = asyncio.create_task(lease_renew_loop())
    local_send_task = asyncio.create_task(local_file_send_loop())
    schedule_definition_task = asyncio.create_task(schedule_definition_loop())
    scheduled_message_task = asyncio.create_task(scheduled_message_loop())
    paused_recovery_task = asyncio.create_task(paused_session_recovery_loop())
    bot_config_reconciler_task = asyncio.create_task(bot_config_reconciler_loop())
    deleted_bot_reaper_task = asyncio.create_task(deleted_bot_reaper_loop())

    await process_schedule_definitions_once()
    await process_scheduled_messages_once()
    await remove_deleted_bots_from_memory_once()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, SHUTDOWN_EVENT.set)
        except NotImplementedError:
            pass

    await SHUTDOWN_EVENT.wait()

    recycler_task.cancel()
    renew_task.cancel()
    local_send_task.cancel()
    schedule_definition_task.cancel()
    scheduled_message_task.cancel()
    paused_recovery_task.cancel()
    bot_config_reconciler_task.cancel()
    deleted_bot_reaper_task.cancel()
    for task in (
        recycler_task,
        renew_task,
        local_send_task,
        schedule_definition_task,
        scheduled_message_task,
        paused_recovery_task,
        bot_config_reconciler_task,
        deleted_bot_reaper_task,
    ):
        try:
            await task
        except asyncio.CancelledError:
            pass
    await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
