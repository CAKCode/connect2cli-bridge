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
import shlex
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
from workspace_bridge.agent_backends import (
    AgentProgress,
    build_agent_argv,
    extract_agent_reply,
    extract_agent_thread_id,
    extract_agent_progress,
    normalize_agent_backend,
)
from workspace_bridge.agent_runtime import normalize_optional_text
from workspace_bridge.api_security import (
    api_auth_enabled as shared_api_auth_enabled,
    api_auth_mode_label as shared_api_auth_mode_label,
    authorize_api_access,
    extract_basic_credentials,
    extract_bearer_token,
    is_loopback_address,
    secure_text_equal as shared_secure_text_equal,
)
from workspace_bridge.atomic_io import (
    atomic_write_json,
    ensure_state_directory,
    open_state_lock_file,
    state_file_mode,
)
from workspace_bridge.async_utils import run_blocking
from workspace_bridge import codex_runtime
from workspace_bridge.codex_status import collect_codex_status, format_codex_status
from workspace_bridge.wecom_context import (
    AssistantCallAssembler, callback_context, decorate_context_replies,
    feedback_event, message_source, message_text, normalize_assistant_reply, quoted_media, text_with_quote,
)
from workspace_bridge.wecom_media import MediaUploadRejected, native_media_body, upload_media
from workspace_bridge.wecom_limits import (
    WeComRateLimitError, WeComRateLimiter, validate_media_upload, wait_for_send_slot,
)
from workspace_bridge.wecom_protocol import (
    build_text_response_payloads as build_wecom_text_response_payloads,
    stream_text_preview,
)
from workspace_bridge.wecom_text import (
    WECOM_CONTENT_MAX_BYTES,
    split_markdown_chunks,
    split_text_chunks as split_wecom_text_chunks,
    truncate_markdown,
)
from workspace_bridge.process_control import (
    isolated_subprocess_kwargs,
    remember_isolated_process,
    terminate_process_tree,
)
from workspace_bridge.template_card_validation import (
    enrich_template_card_for_delivery,
    extract_template_card_owner_user_id,
    require_non_empty_string,
    validate_feedback_id,
    validate_template_card_payload,
    validate_template_card_update_payload,
)


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
    if path.exists() and not path.is_dir():
        raise RuntimeError(f"BRIDGE_SHARED_RUNTIME_ROOT is not a directory: {path}")
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
    if path.exists() and not path.is_dir():
        raise RuntimeError(f"BRIDGE_RUNTIME_ROOT is not a directory: {path}")
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


def resolve_local_send_message_queue_root(base_dir: Optional[Path] = None) -> Path:
    runtime_root = resolve_runtime_root(base_dir)
    raw = str(os.environ.get("LOCAL_SEND_MESSAGE_QUEUE_ROOT") or "").strip()
    if not raw:
        return (runtime_root / ".local-send-message-queue").resolve()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (runtime_root / path).resolve()
    else:
        path = path.resolve()
    return path


def require_directory_path(path: Path, *, label: str) -> Path:
    if path.exists() and not path.is_dir():
        raise RuntimeError(f"{label} is not a directory: {path}")
    return path


HOST, PORT = resolve_host_port()
BRIDGE_API_BASE = build_bridge_api_base(HOST, PORT)
DEFAULT_WORK_DIR = os.environ.get("WORK_DIR", "/home/jenkins")
DEFAULT_CODEX_HOME = Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex")).expanduser().resolve()
BRIDGE_TOKEN = os.environ.get("BRIDGE_TOKEN", "").strip()
BRIDGE_BASIC_AUTH = os.environ.get("BRIDGE_BASIC_AUTH", "").strip()
_RAW_CODEX_EXEC_MODE = os.environ.get("CODEX_EXEC_MODE", "host").strip().lower()
CODEX_EXEC_MODE = "host" if _RAW_CODEX_EXEC_MODE in {"", "null", "none"} else _RAW_CODEX_EXEC_MODE
WECOM_WS = "wss://openws.work.weixin.qq.com"

MAX_JSON_BODY = int(os.environ.get("MAX_JSON_BODY", 1024 * 1024))
MAX_UPLOAD_SIZE = int(os.environ.get("MAX_UPLOAD_SIZE", 20 * 1024 * 1024))
MAX_INBOUND_IMAGE_SIZE = int(os.environ.get("MAX_INBOUND_IMAGE_SIZE", 30 * 1024 * 1024))
MAX_INBOUND_FILE_SIZE = int(os.environ.get("MAX_INBOUND_FILE_SIZE", 100 * 1024 * 1024))
MEDIA_CONNECT_TIMEOUT = int(os.environ.get("MEDIA_CONNECT_TIMEOUT", 8000))
MEDIA_TOTAL_TIMEOUT = int(os.environ.get("MEDIA_TOTAL_TIMEOUT", 20000))
MAX_CONCURRENT_CODEX_RUNS = max(1, int(os.environ.get("MAX_CONCURRENT_CODEX_RUNS", 20)))
SUBPROCESS_STREAM_LIMIT = max(64 * 1024, int(os.environ.get("SUBPROCESS_STREAM_LIMIT", 1024 * 1024)))
SUBPROCESS_STREAM_READ_SIZE = max(4096, int(os.environ.get("SUBPROCESS_STREAM_READ_SIZE", 16 * 1024)))
SUBPROCESS_STREAM_MAX_LINE = max(16 * 1024, int(os.environ.get("SUBPROCESS_STREAM_MAX_LINE", 256 * 1024)))
PROACTIVE_SEND_ACK_TIMEOUT_SEC = max(1, int(os.environ.get("PROACTIVE_SEND_ACK_TIMEOUT_SEC", 10)))
REPLY_SEND_ACK_TIMEOUT_SEC = max(1, int(os.environ.get("REPLY_SEND_ACK_TIMEOUT_SEC", 10)))
UNCERTAIN_DELIVERY_RETRY_SEC = max(1, int(os.environ.get("UNCERTAIN_DELIVERY_RETRY_SEC", 90)))
WEBSOCKET_SEND_TIMEOUT_SEC = max(1, int(os.environ.get("WEBSOCKET_SEND_TIMEOUT_SEC", 5)))
STATUS_STREAM_INTERVAL_SEC = max(5, int(os.environ.get("STATUS_STREAM_INTERVAL_SEC", 5)))
STATUS_STREAM_PROGRESS_ENABLED = str(os.environ.get("STATUS_STREAM_PROGRESS_ENABLED", "true")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
CALLBACK_CONCURRENCY = max(1, int(os.environ.get("CALLBACK_CONCURRENCY", 16)))
ASSISTANT_CALL_TIMEOUT_SEC = max(30, int(os.environ.get("ASSISTANT_CALL_TIMEOUT_SEC", 300)))
STATUS_SEND_TIMEOUT_SEC = max(1, int(os.environ.get("STATUS_SEND_TIMEOUT_SEC", 1)))
STATUS_SEND_LOCK_TIMEOUT_SEC = max(1, int(os.environ.get("STATUS_SEND_LOCK_TIMEOUT_SEC", 1)))
PROACTIVE_TEXT_MAX_CHARS = max(256, int(os.environ.get("PROACTIVE_TEXT_MAX_CHARS", 1800)))
STREAM_TEXT_MAX_CHARS = max(256, int(os.environ.get("STREAM_TEXT_MAX_CHARS", 3500)))
REPLY_MAX_AGE_FALLBACK_SEC = max(60, int(os.environ.get("REPLY_MAX_AGE_FALLBACK_SEC", 540)))
REPLY_IDLE_FALLBACK_SEC = max(60, int(os.environ.get("REPLY_IDLE_FALLBACK_SEC", REPLY_MAX_AGE_FALLBACK_SEC)))
PROACTIVE_STATUS_INTERVAL_SEC = max(30, int(os.environ.get("PROACTIVE_STATUS_INTERVAL_SEC", 120)))
SCHEDULE_PROCESSING_RETRY_MS = max(1000, int(os.environ.get("SCHEDULE_PROCESSING_RETRY_MS", 30000)))
SCHEDULE_ORPHAN_TTL_MS = max(60000, int(os.environ.get("SCHEDULE_ORPHAN_TTL_MS", 86400000)))
WECOM_INBOUND_IDLE_TIMEOUT_MS = max(60000, int(os.environ.get("WECOM_INBOUND_IDLE_TIMEOUT_MS", str(10 * 60 * 1000))))
WECOM_HEARTBEAT_ACK_TIMEOUT_MS = max(30000, int(os.environ.get("WECOM_HEARTBEAT_ACK_TIMEOUT_MS", str(2 * 60 * 1000))))
WECOM_DISCONNECT_GRACE_MS = max(1000, int(os.environ.get("WECOM_DISCONNECT_GRACE_MS", "15000")))
WECOM_RAW_INBOUND_LOG_ENABLED = str(os.environ.get("WECOM_RAW_INBOUND_LOG_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
WECOM_REPLY_URL_TTL_MS = 60 * 60 * 1000
REPEATABLE_TEMPLATE_CARD_EVENT_KEYS = {"refresh_progress"}

MIN_SESSION_TTL_SECONDS = 30 * 60
DEFAULT_SESSION_TTL_SECONDS = 30 * 24 * 60 * 60
SESSION_TTL = max(
    MIN_SESSION_TTL_SECONDS,
    int(os.environ.get("SESSION_TTL", DEFAULT_SESSION_TTL_SECONDS)),
)
SESSION_LEASE_TTL_MS = int(os.environ.get("SESSION_LEASE_TTL", 30000))
SESSION_LEASE_RENEW_MS = max(3000, SESSION_LEASE_TTL_MS // 3)
SESSION_CONTROL_EXIT_TIMEOUT_SEC = 5.0
RECENT_EVENT_TTL = 10 * 60
MAX_STREAM_CONTENT = 8000
MAX_SESSION_CHAT_ITEMS = 200
MAX_PERSISTED_SESSION_CHAT_ITEMS = 50
MAX_COMPAT_HISTORY_ITEMS = 12

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
USER_ALIAS_ROOT = SHARED_RUNTIME_ROOT / ".user-aliases"
TEMPLATE_CARD_STATE_ROOT = SHARED_RUNTIME_ROOT / ".template-card-state"
LOCAL_FILE_SEND_COMMAND = BASE_DIR / "send_file.py"
LOCAL_SEND_MESSAGE_COMMAND = BASE_DIR / "send_message.py"
LOCAL_SCHEDULE_MESSAGE_COMMAND = BASE_DIR / "schedule_message.py"
LOCAL_FILE_SEND_QUEUE_ROOT = require_directory_path(
    resolve_local_file_send_queue_root(),
    label="LOCAL_FILE_SEND_QUEUE_ROOT",
)
LOCAL_FILE_SEND_PENDING_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "pending"
LOCAL_FILE_SEND_PROCESSING_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "processing"
LOCAL_FILE_SEND_RESULT_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "results"
LOCAL_FILE_SEND_DONE_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "done"
LOCAL_FILE_SEND_FAILED_ROOT = LOCAL_FILE_SEND_QUEUE_ROOT / "failed"
LOCAL_FILE_SEND_POLL_MS = int(os.environ.get("LOCAL_FILE_SEND_POLL_MS", 1000))
LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS = max(1000, int(os.environ.get("LOCAL_FILE_SEND_RESULT_TIMEOUT_MS", "120000")))
LOCAL_FILE_SEND_RESULT_RETENTION_MS = max(60000, int(os.environ.get("LOCAL_FILE_SEND_RESULT_RETENTION_MS", str(86400000))))
LOCAL_SEND_MESSAGE_QUEUE_ROOT = require_directory_path(
    resolve_local_send_message_queue_root(),
    label="LOCAL_SEND_MESSAGE_QUEUE_ROOT",
)
LOCAL_SEND_MESSAGE_PENDING_ROOT = LOCAL_SEND_MESSAGE_QUEUE_ROOT / "pending"
LOCAL_SEND_MESSAGE_PROCESSING_ROOT = LOCAL_SEND_MESSAGE_QUEUE_ROOT / "processing"
LOCAL_SEND_MESSAGE_RESULT_ROOT = LOCAL_SEND_MESSAGE_QUEUE_ROOT / "results"
LOCAL_SEND_MESSAGE_DONE_ROOT = LOCAL_SEND_MESSAGE_QUEUE_ROOT / "done"
LOCAL_SEND_MESSAGE_FAILED_ROOT = LOCAL_SEND_MESSAGE_QUEUE_ROOT / "failed"
LOCAL_SEND_MESSAGE_POLL_MS = int(os.environ.get("LOCAL_SEND_MESSAGE_POLL_MS", str(LOCAL_FILE_SEND_POLL_MS)))
LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS = max(1000, int(os.environ.get("LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS", "120000")))
LOCAL_SEND_MESSAGE_RESULT_RETENTION_MS = max(60000, int(os.environ.get("LOCAL_SEND_MESSAGE_RESULT_RETENTION_MS", str(86400000))))
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
VALID_WORKSPACE_MODES = {"personal", "team"}
VALID_AGENT_BACKENDS = {"codex", "claude"}

EXTRA_FILE_ROOTS = [
    Path(item.strip()).expanduser().resolve()
    for item in os.environ.get("FILE_SEND_ROOTS", "").split(",")
    if item.strip()
]
for _file_send_root in EXTRA_FILE_ROOTS:
    if _file_send_root.exists() and not _file_send_root.is_dir():
        raise RuntimeError(f"FILE_SEND_ROOTS entry is not a directory: {_file_send_root}")

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b\[\?[0-9]*[a-z]")
TEXT_MENTION_RE = re.compile(r"(?<!\S)@\S+(?:\s+|$)")
LEADING_MENTION_RE = re.compile(r"^\s*@\S+(?:\s+|$)")
MENTION_DELIMITER_CHARS = ",:;，。：；"
PROGRESS_EVENT_KEY_RE = re.compile(r"^progress_(\d{1,3})$")
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
TASK_ID_ALLOWED_RE = re.compile(r"^[A-Za-z0-9_@-]+$")
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
    active_run_thread_checkpoint: Optional[str] = None
    active_run_thread_checkpoint_set: bool = False
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
    pending_stream_payloads: Optional[list[dict[str, Any]]] = None
    pending_final_payloads: Optional[list[dict[str, Any]]] = None
    late_ack_payloads: list[dict[str, Any]] = field(default_factory=list)
    pending_payload_retry_after: float = 0.0
    run_generation: int = 0
    codex_runtime_status: Optional[str] = None
    active_scheduled_job_file: Optional[str] = None
    active_schedule_request_id: Optional[str] = None
    interrupt_requested: bool = False
    interrupt_queue_resume_deferred: bool = False
    active_schedule_id: Optional[str] = None
    active_message_context: dict[str, Any] = field(default_factory=dict)
    reply_contexts: dict[str, dict[str, Any]] = field(default_factory=dict)
    resume_candidates: list[dict[str, Any]] = field(default_factory=list)
    resume_selection_expires_at: int = 0
    use_global_codex_sessions: bool = False


@dataclass
class BotState:
    config: dict[str, Any]
    status: str = "starting"
    started_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    logs: deque[str] = field(default_factory=lambda: deque(maxlen=200))
    sessions: dict[str, SessionState] = field(default_factory=dict)
    session_lifecycle_locks: dict[str, asyncio.Lock] = field(default_factory=dict)
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
    template_card_button_texts: dict[str, dict[str, str]] = field(default_factory=dict)
    template_card_payloads: dict[str, dict[str, Any]] = field(default_factory=dict)
    template_card_delivery_meta: dict[str, dict[str, Any]] = field(default_factory=dict)
    reply_urls: dict[str, dict[str, Any]] = field(default_factory=dict)
    consumed_template_card_actions: set[str] = field(default_factory=set)
    runtime_lock_handle: Any = None
    stop_reason: Optional[str] = None
    last_subscribed_at_ms: Optional[int] = None
    last_disconnect_at_ms: Optional[int] = None
    last_inbound_at_ms: Optional[int] = None
    last_heartbeat_ack_at_ms: Optional[int] = None
    task_restart_counts: dict[str, int] = field(default_factory=dict)
    pending_payload_flush_in_progress: bool = False
    callback_contexts: dict[str, dict[str, Any]] = field(default_factory=dict)
    callback_tasks: set[asyncio.Task] = field(default_factory=set)
    callback_semaphore: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(CALLBACK_CONCURRENCY))
    assistant_calls: AssistantCallAssembler = field(default_factory=AssistantCallAssembler)
    assistant_call_timeout_tasks: dict[str, asyncio.Task] = field(default_factory=dict)
    rate_limiter: WeComRateLimiter = field(default_factory=WeComRateLimiter)
    feedback_records: dict[str, dict[str, Any]] = field(default_factory=dict)


def uid() -> str:
    return f"{int(time.time() * 1000):x}{random.randint(0, 0xFFFFFF):06x}"


def get_session_lifecycle_lock(bot: BotState, key: str) -> asyncio.Lock:
    lock = bot.session_lifecycle_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        bot.session_lifecycle_locks[key] = lock
    return lock


def now_ms() -> int:
    return int(time.time() * 1000)


def codex_runs_in_sandbox() -> bool:
    return CODEX_EXEC_MODE == "sandboxed"


def parse_command_to_argv(command: str) -> list[str]:
    try:
        return shlex.split(command)
    except ValueError:
        return [command]


def resolve_codex_command() -> list[str]:
    raw = str(os.environ.get("CODEX_COMMAND") or "").strip()
    if raw:
        return parse_command_to_argv(raw)

    resolved = shutil.which("codex")
    if resolved:
        return [resolved]
    return ["codex"]


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def normalize_agent_command(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if text.lower() in {"none", "null"}:
        return None
    return text or None


def bot_agent_backend(config: dict[str, Any]) -> str:
    raw = config.get("agentBackend") or config.get("agent_backend") or "codex"
    return normalize_agent_backend(str(raw))


def bot_agent_command(config: dict[str, Any]) -> Optional[str]:
    return normalize_agent_command(config.get("agentCommand") or config.get("agent_command"))


def build_codex_base_args(
    output_file: Path,
    image_paths: list[str],
    resume: bool,
    *,
    backend: str = "codex",
    command: Optional[str] = None,
    resume_thread_id: Optional[str] = None,
    exec_mode: Optional[str] = None,
) -> list[str]:
    resolved_command = command
    if normalize_agent_backend(backend) == "codex" and not normalize_agent_command(command):
        env_command = str(os.environ.get("CODEX_COMMAND") or "").strip()
        if env_command:
            resolved_command = env_command
        else:
            resolved = shutil.which("codex")
            if resolved:
                resolved_command = resolved
    argv = build_agent_argv(
        normalize_agent_backend(backend),
        resolved_command,
        output_file,
        resume=resume,
        resume_thread_id=resume_thread_id,
        image_paths=image_paths,
        exec_mode=exec_mode or CODEX_EXEC_MODE,
    )
    return list(argv)


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


def ensure_secure_state_root(path: Path, label: str) -> Path:
    if path.is_symlink():
        raise RuntimeError(f"{label} must not be a symlink: {path}")
    path.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise RuntimeError(f"{label} is not a directory: {path}")
    if path.stat().st_mode & 0o002:
        raise RuntimeError(f"{label} must not be world-writable: {path}")
    if not os.access(path, os.W_OK | os.X_OK):
        raise RuntimeError(f"{label} is not writable: {path}")
    return path


def local_file_send_namespace(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    return quote(text, safe="._-")


def local_queue_request_file_component(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "request"
    return quote(text, safe="._-")


def local_queue_request_file_name(request_id: Any) -> str:
    return f"{local_queue_request_file_component(request_id)}.json"


def local_queue_request_file_path(root: Path, request_id: Any) -> Path:
    return root / local_queue_request_file_name(request_id)


def local_queue_request_id_from_job_file(job_file: Path) -> str:
    stem = str(job_file.stem).strip()
    return unquote(stem) if stem else ""


def local_queue_request_path_candidates(root: Path, request_id: Any) -> list[Path]:
    text = str(request_id or "").strip()
    candidates = [local_queue_request_file_path(root, text)]
    if text and "/" not in text and "\\" not in text:
        legacy = root / f"{text}.json"
        if legacy not in candidates:
            candidates.append(legacy)
    return candidates


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
    if root.is_symlink():
        return None
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
    target_config_id = normalize_optional_string(data.get("targetConfigId") or data.get("target_config_id"))
    return get_local_file_send_queue_paths(target_config_id)


def build_local_send_message_queue_paths(root: Path) -> dict[str, Path]:
    return {
        "root": root,
        "pending": root / "pending",
        "processing": root / "processing",
        "results": root / "results",
        "done": root / "done",
        "failed": root / "failed",
    }


def get_local_send_message_queue_paths(target_config_id: Optional[str] = None) -> dict[str, Path]:
    namespace = local_file_send_namespace(target_config_id)
    if namespace:
        return build_local_send_message_queue_paths(LOCAL_SEND_MESSAGE_QUEUE_ROOT / "targets" / namespace)
    return build_local_send_message_queue_paths(LOCAL_SEND_MESSAGE_QUEUE_ROOT)


def get_local_send_message_target_config_id_for_root(root: Path) -> Optional[str]:
    if root.is_symlink():
        return None
    resolved = root.resolve()
    default_root = LOCAL_SEND_MESSAGE_QUEUE_ROOT.resolve()
    if resolved == default_root:
        return None
    targets_root = (LOCAL_SEND_MESSAGE_QUEUE_ROOT / "targets").resolve()
    if resolved.parent != targets_root:
        return None
    return unquote(resolved.name)


def get_template_card_state_file(bot_config_id: str) -> Path:
    safe_bot_id = quote(str(bot_config_id or "").strip(), safe="._-") or "default"
    return TEMPLATE_CARD_STATE_ROOT / f"{safe_bot_id}.json"


def get_reply_url_state_file(bot_config_id: str) -> Path:
    safe_bot_id = quote(str(bot_config_id or "").strip(), safe="._-") or "default"
    return TEMPLATE_CARD_STATE_ROOT / f"{safe_bot_id}.reply-urls.json"


def read_template_card_state(bot_config_id: str) -> dict[str, dict[str, Any]]:
    payload = read_json_file(get_template_card_state_file(bot_config_id), {})
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for task_id, item in payload.items():
        task_text = str(task_id or "").strip()
        if not task_text or not isinstance(item, dict):
            continue
        normalized[task_text] = dict(item)
    return prune_template_card_state_items(normalized)


def read_reply_url_state(bot_config_id: str) -> dict[str, dict[str, Any]]:
    payload = read_json_file(get_reply_url_state_file(bot_config_id), {})
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for req_id, item in payload.items():
        req_text = str(req_id or "").strip()
        if not req_text or not isinstance(item, dict):
            continue
        normalized[req_text] = dict(item)
    return prune_reply_url_state_items(normalized)


def prune_template_card_state_items(items: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for task_id, item in items.items():
        task_text = str(task_id).strip()
        if not task_text or not isinstance(item, dict):
            continue
        current = dict(item)
        current.pop("responseCode", None)
        current.pop("responseCodeCapturedAtMs", None)
        current.pop("responseCodeConsumed", None)
        current.pop("responseCodeConsumedAtMs", None)
        normalized[task_text] = current
    return normalized


def prune_reply_url_state_items(items: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    now_value = now_ms()
    for req_id, item in items.items():
        req_text = str(req_id).strip()
        if not req_text or not isinstance(item, dict):
            continue
        response_url = str(item.get("responseUrl") or "").strip()
        if not response_url:
            continue
        captured_at = normalize_optional_int(item.get("capturedAtMs")) or 0
        if captured_at <= 0 or now_value - captured_at >= WECOM_REPLY_URL_TTL_MS:
            continue
        normalized[req_text] = {**dict(item), "responseUrl": response_url}
    return normalized


def write_template_card_state(bot: BotState) -> None:
    config_id = str(bot.config.get("id") or "").strip()
    if not config_id:
        return
    state_file = get_template_card_state_file(config_id)
    ensure_dir_for(state_file)
    bot.template_card_delivery_meta = prune_template_card_state_items(bot.template_card_delivery_meta)
    write_json_atomic(state_file, bot.template_card_delivery_meta)


def write_reply_url_state(bot: BotState) -> None:
    config_id = str(bot.config.get("id") or "").strip()
    if not config_id:
        return
    state_file = get_reply_url_state_file(config_id)
    ensure_dir_for(state_file)
    bot.reply_urls = prune_reply_url_state_items(bot.reply_urls)
    write_json_atomic(state_file, bot.reply_urls)


def delete_template_card_state(bot_config_id: str) -> None:
    with suppress(FileNotFoundError):
        get_template_card_state_file(bot_config_id).unlink()
    with suppress(FileNotFoundError):
        get_reply_url_state_file(bot_config_id).unlink()


def restore_template_card_runtime_state(bot: BotState) -> None:
    for item in bot.template_card_delivery_meta.values():
        if not isinstance(item, dict):
            continue
        template_card = item.get("templateCard")
        if isinstance(template_card, dict):
            register_template_card_button_texts(bot, template_card)


def list_local_send_message_queue_path_groups() -> list[dict[str, Path]]:
    queue_path_groups = [get_local_send_message_queue_paths()]
    seen_roots = {str(queue_path_groups[0]["root"])}
    targets_root = LOCAL_SEND_MESSAGE_QUEUE_ROOT / "targets"
    if targets_root.exists():
        for root in sorted(targets_root.iterdir()):
            if not root.is_dir() or root.is_symlink():
                continue
            root_str = str(root.resolve())
            if root_str in seen_roots:
                continue
            target_config_id = get_local_send_message_target_config_id_for_root(root)
            if target_config_id and not should_process_local_file_send_target_config(target_config_id):
                continue
            queue_path_groups.append(build_local_send_message_queue_paths(root.resolve()))
            seen_roots.add(root_str)
    for bot_id in sorted(BOTS.keys()):
        if not should_process_local_file_send_target_config(bot_id):
            continue
        target_paths = get_local_send_message_queue_paths(bot_id)
        target_root = str(target_paths["root"])
        if target_root in seen_roots:
            continue
        queue_path_groups.append(target_paths)
        seen_roots.add(target_root)
    return queue_path_groups


def list_all_local_send_message_queue_path_groups() -> list[dict[str, Path]]:
    queue_path_groups = [get_local_send_message_queue_paths()]
    seen_roots = {str(queue_path_groups[0]["root"])}
    targets_root = LOCAL_SEND_MESSAGE_QUEUE_ROOT / "targets"
    if targets_root.exists():
        for root in sorted(targets_root.iterdir()):
            if not root.is_dir() or root.is_symlink():
                continue
            root_str = str(root.resolve())
            if root_str in seen_roots:
                continue
            queue_path_groups.append(build_local_send_message_queue_paths(root.resolve()))
            seen_roots.add(root_str)
    return queue_path_groups


def ensure_local_send_message_dirs(target_config_ids: Optional[list[str]] = None) -> None:
    for path_group in [get_local_send_message_queue_paths(), *(get_local_send_message_queue_paths(item) for item in (target_config_ids or []))]:
        for path in path_group.values():
            ensure_dir(path)


def write_local_send_message_result(request_id: str, payload: dict[str, Any], queue_paths: Optional[dict[str, Path]] = None) -> None:
    queue_paths = queue_paths or get_local_send_message_queue_paths()
    ensure_dir(queue_paths["results"])
    retain_until = normalize_optional_int(payload.get("retainUntil") or payload.get("retain_until"))
    write_json_atomic(
        local_queue_request_file_path(queue_paths["results"], request_id),
        {
            **payload,
            "processedAt": now_ms(),
            "retainUntil": retain_until,
        },
    )


def finalize_local_send_message_job(processing_file: Path, ok: bool) -> None:
    queue_paths = build_local_send_message_queue_paths(processing_file.parent.parent.resolve())
    target_dir = queue_paths["done"] if ok else queue_paths["failed"]
    ensure_dir(target_dir)
    try:
        processing_file.replace(target_dir / processing_file.name)
    except Exception:
        try:
            processing_file.unlink()
        except FileNotFoundError:
            pass


def cleanup_stale_local_send_message_result_files(current_ms: Optional[int] = None) -> None:
    now_value = current_ms if current_ms is not None else now_ms()
    for paths in list_all_local_send_message_queue_path_groups():
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
            result_expires_at = max(processed_at + LOCAL_SEND_MESSAGE_RESULT_RETENTION_MS, retain_until or 0)
            if now_value < result_expires_at:
                continue
            try:
                result_file.unlink()
            except FileNotFoundError:
                pass


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
            if not root.is_dir() or root.is_symlink():
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
            if not root.is_dir() or root.is_symlink():
                continue
            root_str = str(root.resolve())
            if root_str in seen_roots:
                continue
            queue_path_groups.append(build_local_file_send_queue_paths(root.resolve()))
            seen_roots.add(root_str)
    return queue_path_groups


def local_file_send_request_expires_at_ms(request: dict[str, Any]) -> Optional[int]:
    expires_at_value = request.get("expiresAt") if "expiresAt" in request else request.get("expires_at")
    expires_at = normalize_optional_int(expires_at_value)
    if expires_at is not None:
        return expires_at
    requested_at_value = request.get("requestedAt") if "requestedAt" in request else request.get("requested_at")
    requested_at = normalize_optional_int(requested_at_value)
    if requested_at is None:
        return None
    timeout_ms_value = request.get("timeoutMs") if "timeoutMs" in request else request.get("timeout_ms")
    timeout_ms = normalize_optional_int(timeout_ms_value)
    return requested_at + max(1000, timeout_ms if timeout_ms is not None else LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS)


def ensure_local_file_send_request_deadline(request: dict[str, Any], job_file: Path, current_ms: Optional[int] = None) -> tuple[dict[str, Any], bool]:
    now_value = current_ms if current_ms is not None else now_ms()
    changed = False
    normalized = dict(request)

    requested_at_value = normalized.get("requestedAt") if "requestedAt" in normalized else normalized.get("requested_at")
    requested_at = normalize_optional_int(requested_at_value)
    if requested_at is None:
        try:
            requested_at = int(job_file.stat().st_mtime * 1000)
        except FileNotFoundError:
            requested_at = now_value
        normalized["requestedAt"] = requested_at
        changed = True

    timeout_ms_value = normalized.get("timeoutMs") if "timeoutMs" in normalized else normalized.get("timeout_ms")
    timeout_ms = normalize_optional_int(timeout_ms_value)
    if timeout_ms is None:
        timeout_ms = LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS
        normalized["timeoutMs"] = timeout_ms
        changed = True

    expires_at_value = normalized.get("expiresAt") if "expiresAt" in normalized else normalized.get("expires_at")
    expires_at = normalize_optional_int(expires_at_value)
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
    retain_until_value = request.get("retainUntil") if "retainUntil" in request else request.get("retain_until")
    retain_until = normalize_optional_int(retain_until_value)
    if retain_until is not None:
        return retain_until
    expires_at = local_file_send_request_expires_at_ms(request)
    if expires_at is None:
        return None
    return expires_at + LOCAL_FILE_SEND_RESULT_RETENTION_MS


def read_local_file_send_result(request_id: str, queue_paths: dict[str, Path]) -> Optional[dict[str, Any]]:
    for result_file in local_queue_request_path_candidates(queue_paths["results"], request_id):
        payload = read_json_file(result_file, None)
        if isinstance(payload, dict):
            return payload
    return None


def cleanup_stale_local_file_send_result_files(current_ms: Optional[int] = None) -> None:
    now_value = current_ms if current_ms is not None else now_ms()
    for paths in list_all_local_file_send_queue_path_groups():
        result_root = paths["results"]
        if not result_root.exists():
            continue
        for result_file in result_root.glob("*.json"):
            payload = read_json_file(result_file, None)
            processed_at = normalize_optional_int((payload or {}).get("processedAt")) if isinstance(payload, dict) else None
            retain_until = (
                normalize_optional_int((payload or {}).get("retainUntil") if "retainUntil" in (payload or {}) else (payload or {}).get("retain_until"))
                if isinstance(payload, dict)
                else None
            )
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
    if path.is_symlink():
        return fallback
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return fallback


def write_json_atomic(path: Path, data: Any) -> None:
    atomic_write_json(path, data)


def get_shutdown_reason_file() -> Path:
    return INSTANCE_RUNTIME_ROOT / ".bridge.shutdown-reason.json"


def write_shutdown_reason(reason: str, detail: Optional[str] = None) -> None:
    payload = {"reason": str(reason or "").strip() or "unknown", "at": now_ms()}
    if detail:
        payload["detail"] = str(detail)
    try:
        write_json_atomic(get_shutdown_reason_file(), payload)
    except Exception:
        pass


def consume_shutdown_reason() -> Optional[dict[str, Any]]:
    shutdown_reason_file = get_shutdown_reason_file()
    payload = read_json_file(shutdown_reason_file, None)
    try:
        shutdown_reason_file.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return payload if isinstance(payload, dict) else None


def set_shutdown_signal_reason(reason: str, detail: Optional[str] = None) -> None:
    write_shutdown_reason(reason, detail)
    SHUTDOWN_EVENT.set()


def bot_health_snapshot(bot: BotState) -> dict[str, Any]:
    now = now_ms()
    runner_done = bool(bot.runner_task and bot.runner_task.done())
    upload_done = bool(bot.upload_worker_task and bot.upload_worker_task.done())
    ws_connected = bool(bot.ws and not bot.ws.closed)
    inbound_idle_ms = None
    if bot.last_inbound_at_ms is not None:
        inbound_idle_ms = max(0, now - bot.last_inbound_at_ms)
    heartbeat_ack_idle_ms = None
    if bot.last_heartbeat_ack_at_ms is not None:
        heartbeat_ack_idle_ms = max(0, now - bot.last_heartbeat_ack_at_ms)
    inbound_stale = bool(
        bot.status == "running"
        and bot.last_subscribed_at_ms is not None
        and (
            (
                bot.last_inbound_at_ms is None
                and now - bot.last_subscribed_at_ms >= WECOM_INBOUND_IDLE_TIMEOUT_MS
            )
            or (
                inbound_idle_ms is not None
                and inbound_idle_ms >= WECOM_INBOUND_IDLE_TIMEOUT_MS
            )
        )
    )
    heartbeat_ack_stale = bool(
        bot.status == "running"
        and bot.last_heartbeat_ack_at_ms is not None
        and heartbeat_ack_idle_ms is not None
        and heartbeat_ack_idle_ms >= WECOM_HEARTBEAT_ACK_TIMEOUT_MS
    )
    disconnect_grace_active = bool(
        bot.status == "disconnected"
        and bot.last_disconnect_at_ms is not None
        and now - bot.last_disconnect_at_ms < WECOM_DISCONNECT_GRACE_MS
    )
    healthy = (
        bool(bot.config.get("enabled", True))
        and bot.status in {"running", "connecting", "standby", "disconnected"}
        and not runner_done
        and not upload_done
        and (
            bot.status == "standby"
            or ws_connected
            or bot.status == "connecting"
            or disconnect_grace_active
        )
        and not inbound_stale
        and not heartbeat_ack_stale
    )
    return {
        "botConfigId": bot.config["id"],
        "botId": bot.config["botId"],
        "name": bot.config["name"],
        "enabled": bot.config.get("enabled", True),
        "status": bot.status,
        "healthy": healthy,
        "wsConnected": ws_connected,
        "runnerTaskDone": runner_done,
        "uploadWorkerDone": upload_done,
        "taskRestartCounts": dict(bot.task_restart_counts),
        "sessionCount": len(bot.sessions),
        "lastSubscribedAt": bot.last_subscribed_at_ms,
        "lastDisconnectedAt": bot.last_disconnect_at_ms,
        "lastInboundAt": bot.last_inbound_at_ms,
        "lastHeartbeatAckAt": bot.last_heartbeat_ack_at_ms,
        "inboundIdleMs": inbound_idle_ms,
        "heartbeatAckIdleMs": heartbeat_ack_idle_ms,
        "inboundStale": inbound_stale,
        "heartbeatAckStale": heartbeat_ack_stale,
        "disconnectGraceActive": disconnect_grace_active,
        "stopReason": bot.stop_reason,
    }


def summarize_inbound_payload(payload: dict[str, Any]) -> str:
    headers = _as_dict(payload.get("headers"))
    body = _as_dict(payload.get("body"))
    event = _as_dict(body.get("event"))
    parts = [
        f"cmd={str(payload.get('cmd') or '-').strip() or '-'}",
        f"reqId={str(headers.get('req_id') or '-').strip() or '-'}",
        f"msgtype={str(body.get('msgtype') or '-').strip() or '-'}",
        f"chattype={str(body.get('chattype') or '-').strip() or '-'}",
        f"chatid={str(body.get('chatid') or '-').strip() or '-'}",
        f"from={message_sender_userid(payload)}",
        f"msgid={str(body.get('msgid') or '-').strip() or '-'}",
        f"eventtype={str(event.get('eventtype') or '-').strip() or '-'}",
        f"errcode={payload.get('errcode') if payload.get('errcode') is not None else '-'}",
    ]
    return " ".join(parts)


def extract_response_url(message: dict[str, Any]) -> Optional[str]:
    body = _as_dict(message.get("body"))
    headers = _as_dict(message.get("headers"))
    for candidate in (
        body.get("response_url"),
        body.get("responseUrl"),
        headers.get("response_url"),
        headers.get("responseUrl"),
        message.get("response_url"),
        message.get("responseUrl"),
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return None


def save_reply_url(bot: BotState, req_id: str, chat_key: str, response_url: str) -> None:
    req_text = str(req_id or "").strip()
    url_text = str(response_url or "").strip()
    if not req_text or not url_text:
        return
    bot.reply_urls[req_text] = {"responseUrl": url_text, "chatKey": chat_key, "capturedAtMs": now_ms(), "consumed": False}
    write_reply_url_state(bot)


def get_reply_url_state(bot: BotState, req_id: str) -> Optional[dict[str, Any]]:
    req_text = str(req_id or "").strip()
    state = dict(bot.reply_urls.get(req_text) or {})
    if not state:
        return None
    response_url = str(state.get("responseUrl") or "").strip()
    if not response_url:
        return None
    captured_at = normalize_optional_int(state.get("capturedAtMs")) or 0
    if captured_at <= 0 or now_ms() - captured_at >= WECOM_REPLY_URL_TTL_MS:
        return None
    if state.get("consumed") is True:
        return None
    return {**state, "responseUrl": response_url}


def supervise_bot_task(bot: BotState, attr_name: str, task_name: str, factory: Any) -> asyncio.Task:
    task = asyncio.create_task(factory())
    setattr(bot, attr_name, task)

    def on_done(done_task: asyncio.Task) -> None:
        current_task = getattr(bot, attr_name, None)
        if current_task is not done_task:
            return
        if done_task.cancelled():
            return
        try:
            exc = done_task.exception()
        except asyncio.CancelledError:
            return
        except Exception as exc:  # pragma: no cover
            restart_reason = f"inspection_failed:{exc}"
        else:
            if exc is None:
                return
            restart_reason = f"{task_name}_crashed:{exc}"
        if not bot.config.get("enabled", True) or SHUTDOWN_EVENT.is_set():
            return
        bot.task_restart_counts[task_name] = bot.task_restart_counts.get(task_name, 0) + 1
        add_log(bot, f"{task_name} exited unexpectedly; restarting ({restart_reason})")
        add_event_log(
            bot,
            "bot.task_restarted",
            task=task_name,
            count=bot.task_restart_counts[task_name],
            reason=restart_reason,
        )
        supervise_bot_task(bot, attr_name, task_name, factory)

    task.add_done_callback(on_done)
    return task


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
        ensure_state_directory(item)


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


def cleanup_orphan_session_codex_homes() -> int:
    sessions_root = get_bridge_codex_home_root() / "sessions"
    if not sessions_root.exists():
        return 0
    persisted_session_ids: set[str] = set()
    registry_root = SESSION_REGISTRY_ROOT / "sessions"
    if registry_root.exists():
        for session_file in registry_root.glob("*.json"):
            raw = read_json_file(session_file, None)
            record = normalize_session_record(raw) if isinstance(raw, dict) else None
            candidate_session_ids = {
                session_id
                for session_id in {
                    str((record or {}).get("sessionId") or ""),
                    str((raw or {}).get("sessionId") or "") if isinstance(raw, dict) else "",
                    str(session_file.stem or ""),
                }
                if safe_session_id(session_id)
            }
            persisted_session_ids.update(candidate_session_ids)
    removed = 0
    for child in sessions_root.iterdir():
        if not child.is_dir() and not child.is_symlink():
            continue
        if child.name in persisted_session_ids:
            continue
        if remove_tree_if_exists(child):
            removed += 1
    return removed


def cleanup_stale_session_codex_homes(current_ms: Optional[int] = None, active_session_ids: Optional[set[str]] = None) -> int:
    now = now_ms() if current_ms is None else int(current_ms)
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if not sessions_root.exists():
        return 0
    active_ids = {str(item).strip() for item in (active_session_ids or set()) if str(item).strip()}
    removed = 0
    for session_file in sessions_root.glob("*.json"):
        raw = read_json_file(session_file, None)
        if not isinstance(raw, dict):
            continue
        record = normalize_session_record(raw)
        current = record if record is not None else raw
        lease_expires_at = int(normalize_optional_int(current.get("leaseExpiresAt")) or 0)
        if lease_expires_at > now:
            continue
        last_seen_ms = int(
            normalize_optional_int(current.get("lastRunAt"))
            or normalize_optional_int(current.get("updatedAt"))
            or normalize_optional_int(current.get("createdAt"))
            or 0
        )
        if (now - last_seen_ms) <= SESSION_TTL * 1000:
            continue
        session_id = str((record or {}).get("sessionId") or raw.get("sessionId") or session_file.stem).strip()
        if not session_id:
            continue
        if session_id in active_ids:
            continue
        home_dir = get_session_codex_home_root(session_id)
        if not home_dir.exists() and not home_dir.is_symlink():
            continue
        if remove_tree_if_exists(home_dir):
            removed += 1
    return removed


def get_registry_key_file(bot_id: str, key: str) -> Path:
    return SESSION_REGISTRY_ROOT / "keys" / bot_id / f"{quote(key, safe='')}.json"


def get_registry_key_lock_file(bot_id: str, key: str) -> Path:
    return SESSION_REGISTRY_ROOT / "keys" / bot_id / f"{quote(key, safe='')}.lock"


def get_local_resume_thread_binding_lock_file(thread_id: str) -> Path:
    digest = hashlib.sha256(str(thread_id or "").encode("utf-8")).hexdigest()
    return SESSION_REGISTRY_ROOT / "local-thread-locks" / f"{digest}.lock"


def get_registry_session_file(session_id: str) -> Path:
    return SESSION_REGISTRY_ROOT / "sessions" / f"{session_id}.json"


def get_managed_session_lock_file(bot_id: str, session_id: str) -> Path:
    return SESSION_LOCK_ROOT / bot_id / f"{session_id}.lock.json"


def legacy_managed_session_lock_file(bot_id: str, session_id: str) -> Path:
    return legacy_shared_runtime_root() / ".session-locks" / bot_id / f"{session_id}.lock.json"


def safe_session_id(session_id: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9._:-]+", str(session_id or "").strip()))


def bot_id_from_managed_session_lock_file(lock_file: str, session_id: str) -> Optional[str]:
    raw = str(lock_file or "").strip()
    if not raw or not safe_session_id(session_id):
        return None
    try:
        resolved = Path(raw).expanduser().resolve()
    except Exception:
        return None
    candidate_roots = [
        SESSION_LOCK_ROOT.resolve(),
        (legacy_shared_runtime_root() / ".session-locks").resolve(),
    ]
    expected_name = f"{session_id}.lock.json"
    for root in candidate_roots:
        try:
            relative = resolved.relative_to(root)
        except Exception:
            continue
        if len(relative.parts) == 2 and relative.parts[1] == expected_name:
            return relative.parts[0]
    return None


def managed_session_lock_file_matches(bot_id: str, session_id: str, lock_file: str) -> bool:
    raw = str(lock_file or "").strip()
    if not raw:
        return False
    try:
        resolved = Path(raw).expanduser().resolve()
    except Exception:
        return False
    candidates = [
        get_managed_session_lock_file(bot_id, session_id).resolve(),
        legacy_managed_session_lock_file(bot_id, session_id).resolve(),
    ]
    return any(resolved == candidate for candidate in candidates)


def raw_session_record_matches_bot(bot_id: str, raw: Any, *, session_file: Optional[Path] = None) -> bool:
    if not isinstance(raw, dict):
        return False
    raw_bot_id = str(raw.get("botId") or "").strip()
    if raw_bot_id:
        return raw_bot_id == bot_id
    session_id = str(raw.get("sessionId") or "").strip()
    if not session_id and session_file is not None:
        session_id = str(session_file.stem or "").strip()
    if not safe_session_id(session_id):
        return False
    return managed_session_lock_file_matches(bot_id, session_id, str(raw.get("lockFile") or "").strip())


def workspace_slug(value: Any, fallback: str = "unknown") -> str:
    slug = re.sub(r"[^\w.-]+", "_", str(value or "").strip())
    return slug or fallback


def workspace_namespace_for_bot(config: dict[str, Any]) -> str:
    return workspace_slug(config.get("workspaceNamespace") or config.get("workspace_namespace") or config.get("id"))


def workspace_namespace_for_bot_id(bot_id: str) -> str:
    for bot in BOTS.values():
        if str(bot.config.get("id") or "").strip() == str(bot_id or "").strip():
            return workspace_namespace_for_bot(bot.config)
    persisted = next(
        (
            item
            for item in read_persisted_bot_configs()
            if str(item.get("id") or "").strip() == str(bot_id or "").strip()
        ),
        None,
    )
    if persisted:
        return workspace_namespace_for_bot(persisted)
    return workspace_slug(bot_id)


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


def get_workspace_namespace_dir(namespace: str) -> Path:
    return WORKSPACE_ROOT / workspace_slug(namespace)


def get_chatfile_dir(*args: str) -> Path:
    if len(args) == 1:
        return CHATFILE_ROOT / workspace_slug(args[0])
    if len(args) == 2:
        bot_id, key = args
        record = read_session_record_by_key(str(bot_id), str(key))
        if record is None:
            bot = BOTS.get(str(bot_id))
            if bot is not None:
                record = get_or_create_session_record(bot, str(key))
        if record is None:
            raise BridgeError(404, f"session not found for chat key: {key}")
        return CHATFILE_ROOT / workspace_slug(str(record["sessionId"]))
    raise TypeError("get_chatfile_dir expects session_id or (bot_id, chat_key)")


def get_chatfile_dir_for_key(bot: BotState, key: str) -> Path:
    record = get_or_create_session_record(bot, key)
    return get_chatfile_dir(str(record["sessionId"]))


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


def alias_chat_key(alias_scope: str, key: str) -> Optional[str]:
    raw_user_id = chat_key_to_user_id(key)
    if not raw_user_id:
        return None
    alias = read_user_alias(alias_scope, raw_user_id)
    if not alias or alias == raw_user_id:
        return None
    return build_chat_key_with_user_id(key, alias)


def candidate_chat_keys_for_lookup(alias_scope: str, key: str) -> list[str]:
    candidates = [key]
    alias_key = alias_chat_key(alias_scope, key)
    if alias_key and alias_key not in candidates:
        candidates.append(alias_key)
    return candidates


def get_user_workspace_dir(namespace: str, user_id: str) -> Path:
    return get_workspace_namespace_dir(namespace) / "users" / workspace_slug(user_id)


def get_workfile_dir(namespace: str, user_id: str) -> Path:
    return get_user_workspace_dir(namespace, user_id) / "workfile"


def get_room_workspace_dir(namespace: str, room_id: str) -> Path:
    return get_workspace_namespace_dir(namespace) / "rooms" / workspace_slug(room_id)


def get_roomfile_dir(namespace: str, room_id: str) -> Path:
    return get_room_workspace_dir(namespace, room_id) / "roomfile"


def get_legacy_bot_workspace_dir(bot_id: str) -> Path:
    return WORKSPACE_ROOT / workspace_slug(bot_id)


def get_legacy_user_workspace_dir(bot_id: str, user_id: str) -> Path:
    return get_legacy_bot_workspace_dir(bot_id) / "users" / workspace_slug(user_id)


def get_legacy_workfile_dir(bot_id: str, user_id: str) -> Path:
    return get_legacy_user_workspace_dir(bot_id, user_id) / "workfile"


def get_legacy_room_workspace_dir(bot_id: str, room_id: str) -> Path:
    return get_legacy_bot_workspace_dir(bot_id) / "rooms" / workspace_slug(room_id)


def get_legacy_roomfile_dir(bot_id: str, room_id: str) -> Path:
    return get_legacy_room_workspace_dir(bot_id, room_id) / "roomfile"


def get_session_workspace_paths(bot: BotState, key: str, *, session_id: Optional[str] = None) -> dict[str, Optional[Path]]:
    namespace = workspace_namespace_for_bot(bot.config)
    user_id = resolve_workspace_user_id(namespace, chat_key_to_user_id(key))
    room_id = chat_key_to_room_id(key)
    work_dir = Path(str(bot.config.get("workDir") or DEFAULT_WORK_DIR)).expanduser().resolve()
    resolved_session_id = str(session_id or "").strip()
    if not resolved_session_id:
        record = read_session_record_by_key(str(bot.config["id"]), key) or get_or_create_session_record(bot, key)
        resolved_session_id = str((record or {}).get("sessionId") or "").strip()
    return {
        "workDir": work_dir,
        "chatfile": get_chatfile_dir(resolved_session_id) if resolved_session_id else None,
        "workfile": get_workfile_dir(namespace, user_id) if user_id else None,
        "roomfile": get_roomfile_dir(namespace, room_id) if room_id else None,
    }


def default_workspace_mode_for_backend(backend: str) -> str:
    return "personal" if normalize_agent_backend(backend) == "claude" else "team"


def normalize_workspace_mode(value: Any, *, default_backend: str = "codex", fallback_for_missing: Optional[str] = None) -> str:
    mode = str(value or "").strip().lower().replace("_", "-")
    if mode in {"none", "null"}:
        mode = ""
    if not mode:
        mode = fallback_for_missing or default_workspace_mode_for_backend(default_backend)
    aliases = {
        "workspace": "team",
        "workfile": "team",
        "project": "personal",
        "workdir": "personal",
        "work-dir": "personal",
    }
    mode = aliases.get(mode, mode)
    if mode not in VALID_WORKSPACE_MODES:
        raise BridgeError(400, f"workspaceMode must be one of: {', '.join(sorted(VALID_WORKSPACE_MODES))}")
    return mode


def bot_workspace_mode(config: dict[str, Any]) -> str:
    raw = config.get("workspaceMode") or config.get("workspace_mode")
    return normalize_workspace_mode(raw, default_backend=bot_agent_backend(config), fallback_for_missing="team")


def get_session_runtime_cwd(bot: BotState, key: str, *, session_id: Optional[str] = None) -> Path:
    paths = get_session_workspace_paths(bot, key, session_id=session_id)
    if bot_workspace_mode(bot.config) == "team":
        if paths["workfile"] is not None:
            return paths["workfile"].resolve()
        if paths["roomfile"] is not None:
            return paths["roomfile"].resolve()
    if session_id:
        record = read_session_record_by_id(session_id)
        if record:
            override = str(record.get("cwdDir") or "").strip()
            if override:
                return resolve_personal_cwd_for_bot(bot, override)
    return paths["workDir"].resolve()


def resolve_personal_cwd_for_bot(bot: BotState, cwd: Any) -> Path:
    base_dir = Path(str(bot.config.get("workDir") or DEFAULT_WORK_DIR)).expanduser().resolve()
    candidate = Path(str(cwd or "")).expanduser()
    resolved = (base_dir / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    if not resolved.exists():
        raise BridgeError(400, f"cwd does not exist: {resolved}")
    if not resolved.is_dir():
        raise BridgeError(400, f"cwd is not a directory: {resolved}")
    if not path_contains(base_dir, resolved):
        raise BridgeError(400, f"cwd must stay within workDir: {base_dir}")
    return resolved


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
            if destination.is_symlink() and not destination.exists():
                destination.unlink(missing_ok=True)
            if child.is_symlink():
                if destination.exists() or destination.is_symlink():
                    continue
                ensure_dir_for(destination)
                os.symlink(os.readlink(child), destination)
                continue
            if child.is_dir():
                copy_missing_tree_contents_filtered(child, destination, target_root=target_root)
                continue
            if not child.is_file() or destination.exists() or destination.is_symlink():
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


def ensure_session_workspace_dirs(bot: BotState, key: str, *, session_id: Optional[str] = None) -> dict[str, Optional[Path]]:
    paths = get_session_workspace_paths(bot, key, session_id=session_id)
    for path in (paths["chatfile"], paths["workfile"], paths["roomfile"]):
        if path is not None:
            ensure_dir(path)
    if bot_workspace_mode(bot.config) == "team":
        migrate_legacy_workspace_paths(bot, key, paths)
        for path in (paths["workfile"], paths["roomfile"]):
            bootstrap_workspace_from_workdir(paths["workDir"], path)
    ensure_dir(get_workspace_codex_skills_dir(get_session_runtime_cwd(bot, key, session_id=session_id)))
    return paths


def ensure_session_workspace_dirs_light(bot: BotState, key: str, *, session_id: Optional[str] = None) -> dict[str, Optional[Path]]:
    paths = get_session_workspace_paths(bot, key, session_id=session_id)
    for path in (paths["chatfile"], paths["workfile"], paths["roomfile"]):
        if path is not None:
            ensure_dir(path)
    if bot_workspace_mode(bot.config) == "team":
        migrate_legacy_workspace_paths(bot, key, paths)
    ensure_dir(get_workspace_codex_skills_dir(get_session_runtime_cwd(bot, key, session_id=session_id)))
    return paths


def ensure_session_workspace_dirs_light_for_session(bot: BotState, key: str, session_id: str) -> dict[str, Optional[Path]]:
    try:
        paths = ensure_session_workspace_dirs_light(bot, key, session_id=session_id)
    except TypeError as exc:
        if "session_id" not in str(exc):
            raise
        paths = ensure_session_workspace_dirs_light(bot, key)
    if paths.get("chatfile") is None:
        chatfile_dir = get_chatfile_dir(session_id)
        ensure_dir(chatfile_dir)
        paths = {**paths, "chatfile": chatfile_dir}
    return paths


def workspace_migration_marker_file(target_root: Path) -> Path:
    return target_root / ".workspace-layout-migration.json"


def migrate_legacy_workspace_dir(source_root: Optional[Path], target_root: Optional[Path]) -> None:
    if source_root is None or target_root is None:
        return
    if not source_root.exists() or not source_root.is_dir():
        return
    marker_file = workspace_migration_marker_file(target_root)
    if marker_file.exists():
        return
    ensure_dir(target_root)
    copy_missing_tree_contents(source_root, target_root)
    write_json_atomic(
        marker_file,
        {
            "source": str(source_root.resolve()),
            "target": str(target_root.resolve()),
            "migratedAt": now_ms(),
        },
    )


def migrate_legacy_workspace_paths(bot: BotState, key: str, paths: dict[str, Optional[Path]]) -> None:
    bot_id = str(bot.config["id"])
    user_id = chat_key_to_user_id(key)
    room_id = chat_key_to_room_id(key)
    if paths.get("workfile") is not None and user_id:
        for candidate_user_id in candidate_user_aliases(workspace_namespace_for_bot(bot.config), user_id):
            migrate_legacy_workspace_dir(get_legacy_workfile_dir(bot_id, candidate_user_id), paths["workfile"])
    if paths.get("roomfile") is not None and room_id:
        migrate_legacy_workspace_dir(get_legacy_roomfile_dir(bot_id, room_id), paths["roomfile"])


def get_workspace_codex_skills_dir(workspace_dir: Path) -> Path:
    return (workspace_dir / ".codex" / "skills").resolve()


def get_bridge_codex_home_root() -> Path:
    return BRIDGE_CODEX_HOME_ROOT.resolve()


def get_session_codex_home_root(session_id: str) -> Path:
    return get_bridge_codex_home_root() / "sessions" / workspace_slug(session_id)


def get_session_codex_home_local_sessions_backup_root(session_id: str) -> Path:
    return get_session_codex_home_root(session_id) / "sessions.bridge-local"


def get_user_alias_dir(alias_scope: str) -> Path:
    return USER_ALIAS_ROOT / workspace_slug(alias_scope)


def get_user_alias_file(alias_scope: str, raw_user_id: str) -> Path:
    return get_user_alias_dir(alias_scope) / f"{quote(raw_user_id, safe='')}.json"


def read_user_alias(alias_scope: str, raw_user_id: Optional[str]) -> Optional[str]:
    text = str(raw_user_id or "").strip()
    if not text:
        return None
    payload = read_json_file(get_user_alias_file(alias_scope, text), None)
    if not isinstance(payload, dict):
        return None
    alias = str(payload.get("alias") or "").strip()
    return alias or None


def write_user_alias(alias_scope: str, raw_user_id: str, alias: str) -> None:
    normalized_raw = str(raw_user_id or "").strip()
    normalized_alias = str(alias or "").strip()
    if not normalized_raw or not normalized_alias:
        return
    write_json_atomic(
        get_user_alias_file(alias_scope, normalized_raw),
        {"rawUserId": normalized_raw, "alias": normalized_alias, "updatedAt": now_ms()},
    )


def should_prefer_user_alias(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text.startswith(("wo", "wm", "wp")) and len(text) >= 16:
        return True
    return False


def resolve_workspace_user_id(alias_scope: str, raw_user_id: Optional[str]) -> Optional[str]:
    text = str(raw_user_id or "").strip()
    if not text:
        return None
    alias = read_user_alias(alias_scope, text)
    if alias:
        return alias
    return text


def candidate_user_aliases(alias_scope: str, raw_user_id: str) -> list[str]:
    candidates: list[str] = []
    historical = resolve_workspace_user_id(alias_scope, raw_user_id)
    if historical:
        candidates.append(historical)
    text = str(raw_user_id or "").strip()
    if text and text not in candidates:
        candidates.append(text)
    return candidates


def remember_workspace_user_alias(alias_scope: str, raw_user_id: Optional[str]) -> Optional[str]:
    text = str(raw_user_id or "").strip()
    if not text:
        return None
    alias = read_user_alias(alias_scope, text)
    if alias:
        return alias
    if should_prefer_user_alias(text):
        return text
    write_user_alias(alias_scope, text, text)
    return text


def get_bot_tombstone_file(wecom_bot_id: str) -> Path:
    return BOT_TOMBSTONE_ROOT / f"{quote(wecom_bot_id, safe='')}.json"


def get_bot_runtime_lock_file(wecom_bot_id: str) -> Path:
    return BOT_RUNTIME_LOCK_ROOT / f"{quote(wecom_bot_id, safe='')}.lock"


def get_persisted_bot_configs_lock_file() -> Path:
    return DATA_FILE.with_name(f"{DATA_FILE.name}.lock")


def _open_flock_lock_file(lock_file: Path) -> Any:
    ensure_dir_for(lock_file)
    return open_state_lock_file(lock_file)


@contextmanager
def persisted_bot_configs_lock() -> Any:
    with _open_flock_lock_file(get_persisted_bot_configs_lock_file()) as handle:
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


def normalize_session_chat_items(items: Any, *, limit: int = MAX_PERSISTED_SESSION_CHAT_ITEMS) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        text = str(item.get("text") or item.get("content") or "").strip()
        if role not in {"user", "bot"} or not text:
            continue
        current: dict[str, Any] = {"role": role, "text": text}
        source = str(item.get("from") or "").strip()
        if source:
            current["from"] = source
        ts = normalize_optional_int(item.get("time"))
        if ts is not None:
            current["time"] = ts
        normalized.append(current)
    if len(normalized) > limit:
        normalized = normalized[-limit:]
    return normalized


def trim_session_chat_in_memory(sess: SessionState, *, limit: int = MAX_SESSION_CHAT_ITEMS) -> None:
    if len(sess.chat) > limit:
        del sess.chat[: len(sess.chat) - limit]


def session_recent_chat_snapshot(sess: SessionState) -> list[dict[str, Any]]:
    return normalize_session_chat_items(sess.chat)


def persist_session_recent_chat(sess: SessionState) -> None:
    update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "recentChat": session_recent_chat_snapshot(sess),
        },
    )


def append_session_chat_message(sess: SessionState, item: dict[str, Any], *, persist: bool = False) -> None:
    sess.chat.append(dict(item))
    trim_session_chat_in_memory(sess)
    if persist:
        persist_session_recent_chat(sess)


def normalize_local_resume_backup(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    thread_id = str(value.get("threadId") or "").strip() or None
    recent_chat = normalize_session_chat_items(value.get("recentChat"))
    saved_at = normalize_optional_int(value.get("savedAt"))
    if thread_id is None and not recent_chat:
        return None
    backup = {
        "threadId": thread_id,
        "recentChat": recent_chat,
        "useGlobalCodexSessions": False,
    }
    if saved_at is not None:
        backup["savedAt"] = saved_at
    return backup


def normalize_session_record(record: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not isinstance(record, dict):
        return None
    try:
        session_id = str(record["sessionId"]).strip()
        bot_id = str(record.get("botId") or "").strip()
        chat_key = str(record["chatKey"]).strip()
        lock_file = str(record["lockFile"]).strip()
        created_at = int(record["createdAt"])
        updated_at = int(record.get("updatedAt") or created_at)
    except (KeyError, TypeError, ValueError):
        return None
    if not bot_id:
        bot_id = str(bot_id_from_managed_session_lock_file(lock_file, session_id) or "").strip()
    if not session_id or not bot_id or not chat_key or not lock_file:
        return None
    if not safe_session_id(session_id):
        return None
    if not managed_session_lock_file_matches(bot_id, session_id, lock_file):
        return None
    return {
        "sessionId": session_id,
        "botId": bot_id,
        "chatKey": chat_key,
        "lockFile": lock_file,
        "cwdDir": str(record.get("cwdDir") or "").strip() or None,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "lastRunAt": normalize_optional_int(record.get("lastRunAt")),
        "status": str(record.get("status") or "idle").strip() or "idle",
        "ownerInstance": str(record.get("ownerInstance") or "").strip() or None,
        "ownerPid": normalize_optional_int(record.get("ownerPid")),
        "leaseExpiresAt": normalize_optional_int(record.get("leaseExpiresAt")),
        "activeScheduleId": str(record.get("activeScheduleId") or "").strip() or None,
        "threadId": str(record.get("threadId") or "").strip() or None,
        "useGlobalCodexSessions": normalize_internal_bool_flag(record.get("useGlobalCodexSessions"), default=False),
        "localResumeBackup": normalize_local_resume_backup(record.get("localResumeBackup")),
        "recentChat": normalize_session_chat_items(record.get("recentChat")),
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
    session_file = get_registry_session_file(session_id)
    raw = read_json_file(session_file, None)
    record = normalize_session_record(raw)
    if record is not None:
        return record
    if not isinstance(raw, dict):
        return None
    session_id_text = str(raw.get("sessionId") or session_id).strip()
    if not safe_session_id(session_id_text):
        return None
    inferred_bot_id = str(raw.get("botId") or "").strip()
    if not inferred_bot_id:
        inferred_bot_id = str(bot_id_from_managed_session_lock_file(str(raw.get("lockFile") or "").strip(), session_id_text) or "").strip()
    if not inferred_bot_id:
        return None
    inferred_chat_key = chat_key_for_session_id_from_key_root(inferred_bot_id, session_id_text)
    if not inferred_chat_key:
        return None
    record = normalize_session_record(
        {
            **raw,
            "sessionId": session_id_text,
            "botId": inferred_bot_id,
            "chatKey": inferred_chat_key,
        }
    )
    if record is not None:
        write_session_record(record)
    return record


def read_session_key_entry(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    key_entry = read_json_file(get_registry_key_file(bot_id, key), None)
    if not isinstance(key_entry, dict):
        return None
    session_id = str(key_entry.get("sessionId") or "").strip()
    entry_bot_id = str(key_entry.get("botId") or bot_id).strip()
    chat_key = str(key_entry.get("chatKey") or key).strip()
    if not session_id or entry_bot_id != bot_id or chat_key != key:
        return None
    return {"sessionId": session_id, "botId": entry_bot_id, "chatKey": chat_key}


def chat_key_for_session_id_from_key_root(bot_id: str, session_id: str) -> str:
    text = str(session_id or "").strip()
    if not safe_session_id(text):
        return ""
    key_root = SESSION_REGISTRY_ROOT / "keys" / bot_id
    if not key_root.exists():
        return ""
    best_key = ""
    best_sort_key: tuple[int, int, str] | None = None
    for key_file in key_root.glob("*.json"):
        key_entry = read_json_file(key_file, None)
        if not isinstance(key_entry, dict):
            continue
        if str(key_entry.get("sessionId") or "").strip() != text:
            continue
        candidate_key = str(key_entry.get("chatKey") or unquote(key_file.stem)).strip()
        if not candidate_key:
            continue
        sort_key = (
            int(normalize_optional_int(key_entry.get("updatedAt")) or 0),
            int(normalize_optional_int(key_entry.get("createdAt")) or 0),
            candidate_key,
        )
        if best_sort_key is None or sort_key > best_sort_key:
            best_sort_key = sort_key
            best_key = candidate_key
    return best_key


def build_session_key_entry(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "sessionId": record["sessionId"],
        "botId": record["botId"],
        "chatKey": record["chatKey"],
        "createdAt": record["createdAt"],
        "updatedAt": record["updatedAt"],
    }


def read_session_record_by_key_unlocked(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    alias_scope = workspace_namespace_for_bot_id(bot_id)
    for candidate_key in candidate_chat_keys_for_lookup(alias_scope, key):
        key_entry = read_session_key_entry(bot_id, candidate_key)
        if not key_entry:
            continue
        record = read_session_record_by_id(key_entry["sessionId"])
        if record is None:
            raw_record = read_json_file(get_registry_session_file(key_entry["sessionId"]), None)
            if raw_session_record_matches_bot(bot_id, raw_record, session_file=get_registry_session_file(key_entry["sessionId"])):
                record = normalize_session_record(
                    {
                        **dict(raw_record or {}),
                        "sessionId": key_entry["sessionId"],
                        "botId": bot_id,
                        "chatKey": candidate_key,
                    }
                )
                if record is not None:
                    write_session_record(record)
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
    alias_scope = workspace_namespace_for_bot_id(bot_id)
    for candidate_key in candidate_chat_keys_for_lookup(alias_scope, key):
        existing = find_session_record_by_bot_and_key(bot_id, candidate_key)
        if existing:
            write_json_atomic(key_file, build_session_key_entry(existing))
            return existing

    for candidate_key in candidate_chat_keys_for_lookup(alias_scope, key):
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
        desired_key = build_session_key_entry(record)
        current_key = read_json_file(key_file, None)
        if current_key != desired_key:
            write_json_atomic(key_file, desired_key)
        return record
    with session_key_lock(bot_id, key):
        return repair_session_record_by_key_unlocked(bot_id, key)


def find_session_record_by_bot_and_key(bot_id: str, key: str) -> Optional[dict[str, Any]]:
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if not sessions_root.exists():
        return None
    candidates: list[dict[str, Any]] = []
    for session_file in sessions_root.glob("*.json"):
        raw = read_json_file(session_file, None)
        record = normalize_session_record(raw)
        if record is None:
            if not raw_session_record_matches_bot(bot_id, raw, session_file=session_file):
                continue
            if not isinstance(raw, dict) or str(raw.get("chatKey") or "").strip() != key:
                continue
            record = normalize_session_record(
                {
                    **raw,
                    "sessionId": str(raw.get("sessionId") or session_file.stem).strip(),
                    "botId": bot_id,
                }
            )
        if not record or str(record.get("botId") or "") != bot_id or str(record.get("chatKey") or "") != key:
            continue
        candidates.append(record)
    if not candidates:
        return None
    candidates.sort(key=lambda item: (int(item.get("updatedAt") or 0), int(item.get("createdAt") or 0), str(item["sessionId"])))
    return candidates[-1]


def session_ids_referenced_by_key_root(bot_id: str) -> set[str]:
    session_ids: set[str] = set()
    key_root = SESSION_REGISTRY_ROOT / "keys" / bot_id
    if not key_root.exists():
        return session_ids
    for key_file in key_root.glob("*.json"):
        raw = read_json_file(key_file, None)
        if not isinstance(raw, dict):
            continue
        session_id = str(raw.get("sessionId") or "").strip()
        if safe_session_id(session_id):
            session_file = get_registry_session_file(session_id)
            if session_file.exists():
                raw_session = read_json_file(session_file, None)
                if not isinstance(raw_session, dict):
                    continue
                session_owner = str(raw_session.get("botId") or "").strip()
                if session_owner:
                    if session_owner != bot_id:
                        continue
                else:
                    lock_file = str(raw_session.get("lockFile") or "").strip()
                    if not managed_session_lock_file_matches(bot_id, session_id, lock_file):
                        continue
            else:
                foreign_live_owner = next(
                    (
                        runtime_bot_id
                        for runtime_bot_id, runtime_bot in BOTS.items()
                        if runtime_bot_id != bot_id
                        and not bot_instance_is_stale(runtime_bot)
                        and any(sess.session_id == session_id for sess in runtime_bot.sessions.values())
                    ),
                    None,
                )
                if foreign_live_owner is not None:
                    continue
            session_ids.add(session_id)
    return session_ids


@contextmanager
def session_key_lock(bot_id: str, key: str) -> Any:
    with _open_flock_lock_file(get_registry_key_lock_file(bot_id, key)) as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def local_resume_thread_binding_lock(thread_id: str) -> Any:
    with _open_flock_lock_file(get_local_resume_thread_binding_lock_file(thread_id)) as handle:
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


def update_owned_session_record(
    bot: BotState,
    key: str,
    sess: SessionState,
    updater: Any,
) -> Optional[dict[str, Any]]:
    with session_key_lock(str(bot.config["id"]), key):
        current_lease = read_lease(sess.lock_file)
        current_owner = (
            str(current_lease.get("instanceId") or "").strip()
            if isinstance(current_lease, dict)
            else ""
        )
        if not sess.lease_owned or current_owner != INSTANCE_ID:
            sess.lease_owned = False
            return None
        return update_session_record(sess.session_id, updater)


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
                "cwdDir": (
                    str(Path(str(bot.config.get("workDir") or DEFAULT_WORK_DIR)).expanduser().resolve())
                    if bot_workspace_mode(bot.config) == "personal"
                    else None
                ),
                "createdAt": now_ms(),
                "updatedAt": now_ms(),
                "status": "idle",
                "useGlobalCodexSessions": False,
                "recentChat": [],
            }
        )
        if not record:
            raise BridgeError(500, "failed to create session record")

        key_payload = build_session_key_entry(record)
        write_json_atomic(key_file, key_payload)
        return write_session_record(record)


def create_fresh_session_record(
    bot: BotState,
    key: str,
    *,
    expected_session_id: str,
    cwd_dir: Path,
) -> dict[str, Any]:
    bot_id = str(bot.config["id"])
    key_file = get_registry_key_file(bot_id, key)
    ensure_dir_for(key_file)
    with session_key_lock(bot_id, key):
        current = repair_session_record_by_key_unlocked(bot_id, key)
        if current is None or str(current["sessionId"]) != expected_session_id:
            raise BridgeError(409, f"session binding changed while starting a new session: {key}")

        session_id = uid()
        while get_registry_session_file(session_id).exists():
            session_id = uid()
        record = normalize_session_record(
            {
                "sessionId": session_id,
                "botId": bot_id,
                "chatKey": key,
                "lockFile": str(get_managed_session_lock_file(bot_id, session_id)),
                "cwdDir": str(cwd_dir.expanduser().resolve()),
                "createdAt": now_ms(),
                "updatedAt": now_ms(),
                "status": "idle",
                "useGlobalCodexSessions": False,
                "recentChat": [],
            }
        )
        if record is None:
            raise BridgeError(500, "failed to create session record")

        stored = write_session_record(record)
        try:
            write_json_atomic(key_file, build_session_key_entry(stored))
        except Exception:
            remove_path_if_exists(get_registry_session_file(session_id))
            raise
        return stored


def get_or_create_session_record(bot: BotState, key: str) -> dict[str, Any]:
    return read_session_record_by_key(bot.config["id"], key) or create_session_record(bot, key)


def read_lease(lock_file: Path) -> Any:
    return read_json_file(lock_file, None)


def write_lease(lock_file: Path, lease: dict[str, Any]) -> None:
    write_json_atomic(lock_file, lease)


def _acquire_session_lease_unlocked(bot: BotState, sess: SessionState, key: str) -> bool:
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
            ensure_state_directory(lock_file.parent)
            if lock_file.is_symlink():
                lock_file.unlink(missing_ok=True)
            fd = os.open(
                str(lock_file),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                state_file_mode(lock_file.parent),
            )
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
            current = current if isinstance(current, dict) else None
            if current and current.get("instanceId") == INSTANCE_ID:
                write_lease(lock_file, next_lease)
                sess.lease_owned = True
                return True
            expires_at = normalize_optional_int(current.get("expiresAt")) if current else None
            if not current or expires_at is None or expires_at <= now:
                try:
                    lock_file.unlink()
                except FileNotFoundError:
                    pass
                continue
            sess.lease_owned = False
            return False
    sess.lease_owned = False
    return False


def acquire_session_lease(bot: BotState, sess: SessionState, key: str) -> bool:
    with session_key_lock(str(bot.config["id"]), key):
        return _acquire_session_lease_unlocked(bot, sess, key)


def _renew_session_lease_unlocked(bot: BotState, key: str, sess: SessionState) -> bool:
    if not sess.lease_owned:
        return False
    current = read_lease(sess.lock_file)
    current = current if isinstance(current, dict) else None
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


def renew_session_lease(bot: BotState, key: str, sess: SessionState) -> bool:
    with session_key_lock(str(bot.config["id"]), key):
        return _renew_session_lease_unlocked(bot, key, sess)


def session_lease_is_owned_by_current_instance(sess: SessionState) -> bool:
    if not sess.lease_owned:
        return False
    current = read_lease(sess.lock_file)
    if not isinstance(current, dict) or str(current.get("instanceId") or "").strip() != INSTANCE_ID:
        sess.lease_owned = False
        return False
    return True


def _release_session_lease_unlocked(bot: BotState, key: str, sess: SessionState) -> bool:
    current = read_lease(sess.lock_file)
    current = current if isinstance(current, dict) else None
    record = read_session_record_by_id(sess.session_id)
    lock_owner = str(current.get("instanceId") or "").strip() if current else ""
    record_owner = str(record.get("ownerInstance") or "").strip() if record else ""
    sess.lease_owned = False
    if (lock_owner and lock_owner != INSTANCE_ID) or (record_owner and record_owner != INSTANCE_ID):
        add_log(bot, f"[{key}] lease release skipped after ownership changed")
        return False
    if current and lock_owner == INSTANCE_ID:
        try:
            sess.lock_file.unlink()
        except FileNotFoundError:
            pass
        add_log(bot, f"[{key}] lease released")
    update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": None,
            "ownerPid": None,
            "leaseExpiresAt": None,
            "activeScheduleId": None,
            "status": "idle",
        },
    )
    return True


def release_session_lease(bot: BotState, key: str, sess: SessionState) -> bool:
    with session_key_lock(str(bot.config["id"]), key):
        return _release_session_lease_unlocked(bot, key, sess)


def release_session_lease_and_update(
    bot: BotState,
    key: str,
    sess: SessionState,
    updater: Any,
) -> bool:
    with session_key_lock(str(bot.config["id"]), key):
        if not _release_session_lease_unlocked(bot, key, sess):
            return False
        update_session_record(sess.session_id, updater)
        return True


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


def build_status_stream_content(status_text: str, summary: Optional[str] = None) -> str:
    cleaned_summary = str(summary or "").strip()
    if not cleaned_summary:
        return status_text
    content = limit_stream_content(f"{status_text}\n\n{cleaned_summary}")
    return stream_text_preview(content, max_chars=STREAM_TEXT_MAX_CHARS)


def acquire_bot_runtime_lock(bot: BotState) -> bool:
    if bot.runtime_lock_handle is not None:
        return True
    handle = _open_flock_lock_file(get_bot_runtime_lock_file(bot.config["botId"]))
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return False
    bot.runtime_lock_handle = handle
    return True


def bot_runtime_lock_held_elsewhere(wecom_bot_id: str) -> bool:
    handle = _open_flock_lock_file(get_bot_runtime_lock_file(wecom_bot_id))
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
    body = _as_dict(message.get("body"))
    return str((_as_dict(body.get("from")).get("userid")) or "unknown")


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


def normalize_bridge_command_text(content: str, bot_name: Optional[str] = None) -> str:
    text = strip_text_mentions(content, bot_name)
    if text == "/new" or text.startswith(("/bridge-", "/cwd", "/local-")):
        return text
    fallback = LEADING_MENTION_RE.sub("", content or "", count=1).strip()
    if fallback == "/new" or fallback.startswith(("/bridge-", "/cwd", "/local-")):
        return fallback
    return (content or "").strip()


def dedupe_content_fingerprint(message: dict[str, Any]) -> str:
    body = _as_dict(message.get("body"))
    msg_type = str(body.get("msgtype") or "").strip()
    if msg_type in {"text", "voice"}:
        return message_text(body)
    if msg_type == "image":
        return str((_as_dict(body.get("image")).get("url") or "")).strip()
    if msg_type == "file":
        file_body = _as_dict(body.get("file"))
        return str(file_body.get("file_id") or file_body.get("fileId") or file_body.get("filename") or "").strip()
    if msg_type == "video":
        return str(_as_dict(body.get("video")).get("url") or "").strip()
    if msg_type == "mixed":
        return json.dumps(_as_dict(body.get("mixed")), ensure_ascii=False, sort_keys=True)
    return ""


def chat_key_for_bot(bot: BotState, message: dict[str, Any]) -> str:
    body = _as_dict(message.get("body"))
    raw_sender_user_id = message_sender_userid(message)
    if raw_sender_user_id == "unknown":
        raise BridgeError(400, "message sender userid required")
    remember_workspace_user_alias(workspace_namespace_for_bot(bot.config), raw_sender_user_id)
    if body.get("chattype") == "group":
        chat_id = str(body.get("chatid") or "").strip()
        if not chat_id:
            raise BridgeError(400, "group message chatid required")
        if str(bot.config.get("groupSessionMode") or "per-user") == "per-user":
            return f"group-user:{chat_id}:{raw_sender_user_id}"
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
    workspace_paths = ensure_session_workspace_dirs_light_for_session(bot, key, sess.session_id)
    runtime_cwd = get_session_runtime_cwd(bot, key, session_id=sess.session_id)
    workspace_codex_skills_dir = get_workspace_codex_skills_dir(runtime_cwd)
    allowed_file_roots = ", ".join(str(root) for root in get_allowed_file_roots(bot, key, session_id=sess.session_id))
    user_id = chat_key_to_user_id(key) or "-"
    room_id = chat_key_to_room_id(key) or "-"
    agent_backend = bot_agent_backend(bot.config)
    agent_command = bot_agent_command(bot.config) or "-"
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
            f"messageSource: {sess.active_message_context.get('source') or 'user'}",
            f"assistantTaskId: {sess.active_message_context.get('assistant_task_id') or '-'}",
            "nativeMediaTypes: file,image,voice,video (assistant calls: file,image,video)",
            "nativeMediaLimits: file=20MB,image=10MB,voice=2MB(amr),video=10MB(mp4)",
            f"agentBackend: {agent_backend}",
            f"agentCommand: {agent_command}",
            f"workspaceMode: {bot_workspace_mode(bot.config)}",
            f"executionMode: {CODEX_EXEC_MODE}",
            f"CWD_DIR: {runtime_cwd}",
            f"WORKDIR_DIR: {workspace_paths['workDir']}",
            f"CHATFILE_DIR: {workspace_paths['chatfile']}",
            f"WORKFILE_DIR: {workspace_paths['workfile'] or '-'}",
            f"ROOMFILE_DIR: {workspace_paths['roomfile'] or '-'}",
            f"WORKSPACE_CODEX_SKILLS_DIR: {workspace_codex_skills_dir}",
            "HOME_CODEX_SKILLS_DIR: ~/.codex/skills",
            (
                "localSendFileCommand: "
                f"python3 {LOCAL_FILE_SEND_COMMAND} --chat-key '{key}' --bot-config-id '{bot.config['id']}' --bot-name '{bot.config['name']}' --file-path ABSOLUTE_FILE_PATH "
                f"(fallback: --session-id {sess.session_id})"
            ),
            (
                "localSendMessageCommand: "
                f"python3 {LOCAL_SEND_MESSAGE_COMMAND} --chat-key '{key}' --bot-config-id '{bot.config['id']}' --bot-name '{bot.config['name']}' "
                f"(fallback: --session-id {sess.session_id}) --msgtype template_card --template-card-file ABSOLUTE_JSON_FILE"
            ),
            (
                "localScheduleMessageCommand: "
                f"python3 {LOCAL_SCHEDULE_MESSAGE_COMMAND} --chat-key '{key}' --bot-config-id '{bot.config['id']}' --bot-name '{bot.config['name']}' "
                f"(fallback: --session-id {sess.session_id}) "
                "--run-at RFC3339_OR_EPOCH_MS --message MESSAGE "
                'or --cron "0 9 * * *" --timezone TZ --message MESSAGE'
            ),
            f"allowedFileSendRoots: {allowed_file_roots}",
            execution_mode_note,
            "Use localSendFileCommand for file replies.",
            "Use localSendMessageCommand for proactive template-card sends.",
            "Use localScheduleMessageCommand for follow-up messages.",
            "Export send-back files under CHATFILE_DIR.",
            "User-global Codex skills should live in ~/.codex/skills.",
            "Personal Codex skills should live in CWD_DIR/.codex/skills.",
            (
                "If the selected backend lacks native bridge resume support, preserve continuity by using the recent bridge-local chat history included in the prompt."
                if agent_backend != "codex"
                else "Native Codex resume is enabled when a live bridge thread is available."
            ),
            "For native media previews, append --media-type image, video or voice to localSendFileCommand; file is the default.",
            "Assistant calls support text, images and files, not voice, video or template cards; file/message commands keep additions waiting until the final response.",
            "[/BridgeContext]",
        ]
    )


def build_pending_media_context(pending_media: list[dict[str, Any]]) -> str:
    lines = []
    for idx, media in enumerate(pending_media, start=1):
        if media.get("kind") == "image":
            lines.append(
                f"Attachment {idx}: image file available at {media['path']}. "
                "Read the local file directly if image analysis is needed."
            )
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


async def prepare_wecom_send(bot: BotState, payload: dict[str, Any], send_budget: Optional[float] = None) -> float:
    context = bot.callback_contexts.get(str(_as_dict(payload.get("headers")).get("req_id") or ""), {})
    target = context.get("target")
    if not target:
        sess = bot.reply_sessions.get(str(_as_dict(payload.get("headers")).get("req_id") or ""))
        if sess:
            key = key_for_session(bot, sess)
            if key:
                target = chat_key_to_send_target(key)
    now = bot.rate_limiter.clock()
    deadline = now + send_budget if send_budget is not None else None
    cmd = payload.get("cmd")
    if cmd in {"aibot_respond_update_msg", "aibot_respond_welcome_msg"} and context:
        callback_deadline = context["received_at"] + 5
        deadline = min(deadline, callback_deadline) if deadline is not None else callback_deadline
    stream_id = str(_as_dict(_as_dict(payload.get("body")).get("stream")).get("id") or "")
    started = context.get("streams", {}).get(stream_id)
    if started is not None:
        stream_deadline = started + 600
        deadline = min(deadline, stream_deadline) if deadline is not None else stream_deadline
    try:
        await wait_for_send_slot(bot.rate_limiter, payload, callback_target=target, received_at=context.get("received_at"), deadline=deadline)
    except WeComRateLimitError as exc:
        raise BridgeError(429, str(exc)) from exc
    timeout = WEBSOCKET_SEND_TIMEOUT_SEC
    if deadline is not None:
        timeout = min(timeout, deadline - bot.rate_limiter.clock())
        if timeout <= 0:
            raise BridgeError(429, "callback reply deadline exceeded")
    return timeout


def note_wecom_send(bot: BotState, payload: dict[str, Any]) -> None:
    context = bot.callback_contexts.get(str(_as_dict(payload.get("headers")).get("req_id") or ""))
    stream_id = str(_as_dict(_as_dict(payload.get("body")).get("stream")).get("id") or "")
    if context is not None and stream_id:
        context["streams"].setdefault(stream_id, bot.rate_limiter.clock())


async def send_ws_payload(bot: BotState, payload: dict[str, Any]) -> None:
    if not bot.ws or bot.ws.closed:
        return
    timeout = await prepare_wecom_send(bot, payload)
    await asyncio.wait_for(bot.ws.send_json(payload), timeout)
    note_wecom_send(bot, payload)


def payload_req_id(payload: dict[str, Any]) -> str:
    return str(((payload.get("headers") or {}).get("req_id")) or "")


def same_payload_delivery(left: dict[str, Any], right: dict[str, Any]) -> bool:
    left_headers = {key: value for key, value in (left.get("headers") or {}).items() if key != "req_id"}
    right_headers = {key: value for key, value in (right.get("headers") or {}).items() if key != "req_id"}
    return {**left, "headers": left_headers} == {**right, "headers": right_headers}


def is_reply_payload(payload: dict[str, Any]) -> bool:
    return payload.get("cmd") == "aibot_respond_msg" and bool(payload_req_id(payload))


def reply_payload_finished(payload: dict[str, Any]) -> bool:
    if not is_reply_payload(payload):
        return False
    assistant = _as_dict(_as_dict(payload.get("body")).get("to_ai_assistant"))
    if assistant.get("reply_status") == "waiting":
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
    if req_id in bot.callback_contexts:
        sess.reply_contexts[req_id] = bot.callback_contexts[req_id]
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
        context.late_ack_payloads = [
            payload for payload in context.late_ack_payloads if payload_req_id(payload) != req_id
        ]
    bot.reply_sessions.pop(req_id, None)


def cleanup_session_reply_contexts(bot: BotState, sess: SessionState) -> None:
    for req_id, context in list(bot.reply_sessions.items()):
        if context is sess:
            cleanup_reply_session(bot, req_id, sess)


def active_reply_request_ids(bot: BotState, sess: SessionState) -> list[str]:
    """Return reply streams that must be closed before a session is interrupted."""
    request_ids = set(sess.reply_started_at)
    request_ids.update(req_id for req_id, context in bot.reply_sessions.items() if context is sess)
    for payload in (sess.pending_stream_payload, sess.pending_final_payload):
        if payload_req_id(payload or {}):
            request_ids.add(payload_req_id(payload))
    return sorted(request_id for request_id in request_ids if str(request_id).strip())


async def finish_interrupted_reply_streams(
    bot: BotState,
    key: str,
    sess: SessionState,
    request_ids: list[str],
) -> None:
    """Finish WeCom streams left open when a running task is cancelled."""
    if not request_ids:
        return
    for req_id in request_ids:
        if req_id in sess.reply_proactive_req_ids:
            continue
        payload = build_session_text_payload(key, sess, req_id, "任务已中断。", True)
        if payload is None:
            continue
        try:
            # Cache a failed close and retry it after reconnect/next inbound ACK;
            # dropping it leaves the client stream permanently loading.
            await send_or_store_session_payload(bot, key, sess, payload, True)
        except Exception as exc:
            add_log(bot, f"[{key}] interrupted reply stream close skipped req_id={req_id}: {exc}")


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
    return (
        "流式回复窗口即将到期，任务仍在后台运行，"
        f"后续每 {PROACTIVE_STATUS_INTERVAL_SEC} 秒主动发送运行状态，完成后发送最终结果。"
    )


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
    notice_delivered = False
    if notice_payload:
        notice_payload = decorate_context_replies([notice_payload], sess.reply_contexts.get(req_id), final=False)[0]
        try:
            await send_session_payload(bot, sess, notice_payload)
            notice_delivered = True
        except Exception as exc:
            add_log(bot, f"[{key}] stream close notice skipped req_id={req_id}: {exc}")
    mark_reply_proactive(sess, req_id)
    if notice_delivered:
        mark_proactive_status_sent(sess, req_id)


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
            timeout = await prepare_wecom_send(bot, payload, send_timeout_sec)
            await asyncio.wait_for(bot.ws.send_json(payload), timeout)
            note_wecom_send(bot, payload)
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
        try:
            response = await send_ws_payload_with_ack(bot, payload, timeout_sec, send_timeout_sec=send_timeout_sec)
        except BridgeError as exc:
            if exc.status_code == 504 and exc.message in {"websocket ack timeout", "websocket send timeout"}:
                sess.late_ack_payloads.append(clone_payload_sequence([payload])[0])
                if len(sess.late_ack_payloads) > 20:
                    del sess.late_ack_payloads[:-20]
                sess.pending_payload_retry_after = max(
                    sess.pending_payload_retry_after,
                    time.monotonic() + UNCERTAIN_DELIVERY_RETRY_SEC,
                )
            raise
        mark_session_reply_sent(bot, sess, payload)
        return response
    finally:
        sess.send_lock.release()


def session_payload_ack_timeout(payload: dict[str, Any]) -> Optional[int]:
    req_id = payload_req_id(payload)
    if not req_id:
        return None
    cmd = str(payload.get("cmd") or "").strip()
    if cmd == "aibot_send_msg":
        return PROACTIVE_SEND_ACK_TIMEOUT_SEC
    if cmd == "aibot_respond_msg":
        return REPLY_SEND_ACK_TIMEOUT_SEC
    return None


def require_ack_success(response: dict[str, Any], stage: str, *, required_body_key: Optional[str] = None) -> dict[str, Any]:
    raw_errcode = response.get("errcode", 0)
    try:
        errcode = int(raw_errcode if raw_errcode not in (None, "") else 0)
    except (TypeError, ValueError) as exc:
        raise BridgeError(502, f"{stage} failed: invalid errcode") from exc
    if errcode != 0:
        errmsg = str(response.get("errmsg") or "").strip()
        raise BridgeError(502, f"{stage} failed: {errcode} {errmsg}".strip())
    body = response.get("body")
    if not isinstance(body, dict):
        body = {}
    if required_body_key and not body.get(required_body_key):
        raise BridgeError(502, f"{stage} failed: missing {required_body_key}")
    return body


def has_explicit_zero_errcode(payload: dict[str, Any]) -> bool:
    raw_errcode = payload.get("errcode")
    if raw_errcode in (None, ""):
        return False
    try:
        return int(raw_errcode) == 0
    except (TypeError, ValueError):
        return False


def session_payload_ack_error(payload: dict[str, Any], response: dict[str, Any]) -> Optional[BridgeError]:
    cmd = str(payload.get("cmd") or "").strip()
    stage = "proactive send" if cmd == "aibot_send_msg" else "reply send"
    try:
        require_ack_success(response, stage)
    except BridgeError as exc:
        return exc
    return None


async def send_session_payload(
    bot: BotState,
    sess: SessionState,
    payload: dict[str, Any],
    *,
    send_timeout_sec: Optional[int] = None,
    lock_timeout_sec: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    ack_timeout_sec = session_payload_ack_timeout(payload)
    if ack_timeout_sec:
        response = await send_session_ws_payload_with_ack(
            bot,
            sess,
            payload,
            ack_timeout_sec,
            send_timeout_sec=send_timeout_sec,
            lock_timeout_sec=lock_timeout_sec,
        )
        error = session_payload_ack_error(payload, response)
        if error is not None:
            raise error
        return response
    await send_session_ws_payload(
        bot,
        sess,
        payload,
        send_timeout_sec=send_timeout_sec,
        lock_timeout_sec=lock_timeout_sec,
    )
    return None


def chat_key_to_send_target(key: str) -> tuple[int, str]:
    text = str(key or "").strip()
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        if len(parts) == 3 and parts[1] and parts[2]:
            return 2, parts[1]
        raise BridgeError(400, f"invalid chat key: {key}")
    if text.startswith("group:"):
        room_id = text.split(":", 1)[1]
        if room_id:
            return 2, room_id
        raise BridgeError(400, f"invalid chat key: {key}")
    if text.startswith("single:"):
        user_id = text.split(":", 1)[1]
        if user_id:
            return 1, user_id
        raise BridgeError(400, f"invalid chat key: {key}")
    raise BridgeError(400, f"invalid chat key: {key}")


def limit_proactive_text(content: str) -> str:
    return truncate_markdown(content, max_chars=PROACTIVE_TEXT_MAX_CHARS, suffix="...(truncated)")


def split_text_chunks(content: str, *, max_chars: int) -> list[str]:
    return split_wecom_text_chunks(content, max_chars=max_chars)


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
    explicit = normalize_optional_string(mention_user_id) or ""
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


def build_proactive_chat_payloads(
    key: str, content: str, mention_user_id: Optional[str] = None
) -> list[dict[str, Any]]:
    resolved_mention = proactive_chat_mention_user_id(key, mention_user_id)
    mention = format_group_user_mention(resolved_mention)
    prefix = mention + "\n" if mention else ""
    chunks = split_markdown_chunks(
        content,
        max_chars=PROACTIVE_TEXT_MAX_CHARS - len(prefix),
        max_bytes=WECOM_CONTENT_MAX_BYTES - len(prefix.encode("utf-8")),
    )
    return [build_proactive_chat_payload(key, chunk, resolved_mention) for chunk in chunks]


def attach_message_feedback(message_body: dict[str, Any], feedback_id: Optional[str]) -> dict[str, Any]:
    resolved_feedback_id = str(feedback_id or "").strip()
    body = dict(message_body or {})
    if not resolved_feedback_id:
        return body
    resolved_feedback_id = validate_feedback_id(resolved_feedback_id)
    if body.get("msgtype") == "markdown":
        markdown = dict(body.get("markdown") or {})
        markdown["feedback"] = {"id": resolved_feedback_id}
        body["markdown"] = markdown
    elif body.get("msgtype") == "template_card":
        template_card = dict(body.get("template_card") or {})
        feedback = dict(template_card.get("feedback") or {})
        feedback["id"] = resolved_feedback_id
        template_card["feedback"] = feedback
        body["template_card"] = template_card
    return body


async def send_ws_payload_with_ack(
    bot: BotState, payload: dict[str, Any], timeout_sec: int, *, send_timeout_sec: Optional[int] = None
) -> dict[str, Any]:
    req_id = payload_req_id(payload)
    if not req_id:
        raise BridgeError(500, "payload req_id required for ack")
    if not bot.ws or bot.ws.closed:
        raise BridgeError(503, "bot websocket closed")
    timeout = await prepare_wecom_send(bot, payload, send_timeout_sec)
    future = create_request_future(bot, req_id)
    try:
        try:
            await asyncio.wait_for(bot.ws.send_json(payload), timeout)
            note_wecom_send(bot, payload)
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
    finally:
        if bot.pending_requests.get(req_id) is future:
            bot.pending_requests.pop(req_id, None)
        if not future.done():
            future.cancel()


def build_session_text_payload(key: str, sess: SessionState, req_id: Optional[str], content: str, final: bool) -> Optional[dict[str, Any]]:
    if req_id:
        stream_content = content
        if should_prepend_group_user_mention_to_reply(key, sess, req_id):
            stream_content = prepend_group_user_mention(content, chat_key_to_user_id(key))
        payload = {
            "cmd": "aibot_respond_msg",
            "headers": {"req_id": req_id},
            "body": {"msgtype": "stream", "stream": {"id": sess.session_id, "finish": final, "content": stream_content}},
        }
        return decorate_context_replies([payload], sess.reply_contexts.get(req_id), final=final)[0]
    if not final:
        return None
    return decorate_context_replies([build_proactive_chat_payload(key, content)], sess.active_message_context, final=final)[0]


def build_session_text_payloads(
    key: str, sess: SessionState, req_id: Optional[str], content: str, final: bool
) -> list[dict[str, Any]]:
    if req_id:
        stream_content = content
        if should_prepend_group_user_mention_to_reply(key, sess, req_id):
            stream_content = prepend_group_user_mention(content, chat_key_to_user_id(key))
        payloads = build_wecom_text_response_payloads(
            req_id, sess.session_id, stream_content, final=final, max_chars=STREAM_TEXT_MAX_CHARS
        )
        return decorate_context_replies(payloads, sess.reply_contexts.get(req_id), final=final)
    if not final:
        return []
    return decorate_context_replies(build_proactive_chat_payloads(key, content), sess.active_message_context, final=final)


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
        await send_session_payload(bot, sess, payload)
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
    payloads = build_session_text_payloads(key, sess, req_id, content, False)
    if not payloads:
        return False
    if bot.ws and not bot.ws.closed:
        failed_index: Optional[int] = None
        try:
            for index, payload in enumerate(payloads):
                await send_session_payload(
                    bot,
                    sess,
                    payload,
                    send_timeout_sec=STATUS_SEND_TIMEOUT_SEC,
                    lock_timeout_sec=STATUS_SEND_LOCK_TIMEOUT_SEC,
                )
            sess.pending_stream_payload = None
            sess.pending_stream_payloads = None
            return True
        except Exception as exc:
            if isinstance(exc, BridgeError) and exc.status_code == 429:
                return False
            failed_index = index if 'index' in locals() else 0
            add_log(bot, f"[{key}] stream status deferred req_id={payload_req_id(payloads[-1]) or '-'}: {exc}")
            add_event_log(
                bot,
                "session.status_deferred",
                chatKey=key,
                sessionId=sess.session_id,
                reqId=payload_req_id(payloads[-1]) or None,
                reason=str(exc),
            )
    remaining_payloads = payloads[failed_index:] if 'failed_index' in locals() and failed_index is not None else payloads
    sess.pending_stream_payload = dict(remaining_payloads[-1])
    sess.pending_stream_payloads = clone_payload_sequence(remaining_payloads)
    return False


async def send_or_store_session_payloads(
    bot: BotState, key: str, sess: SessionState, payloads: list[dict[str, Any]], final: bool
) -> bool:
    if not payloads:
        return False
    if len(payloads) == 1:
        return await send_or_store_session_payload(bot, key, sess, payloads[0], final)

    outgoing_payloads = clone_payload_sequence(payloads)
    original_reply_req_id = payload_req_id(payloads[0]) if is_reply_payload(payloads[0]) else ""
    first_payload = outgoing_payloads[0]
    if first_payload.get("cmd") == "aibot_send_msg":
        for outgoing_payload in outgoing_payloads:
            headers = dict((outgoing_payload.get("headers") or {}))
            headers["req_id"] = uid()
            outgoing_payload["headers"] = headers
    elif is_reply_payload(first_payload):
        reply_req_id = payload_req_id(first_payload)
        if reply_req_id and not final and reply_age_too_long(sess, reply_req_id):
            await close_reply_stream_for_proactive(bot, key, sess, reply_req_id)
            sess.pending_stream_payload = None
            sess.pending_stream_payloads = None
            return False
        uncertain_stream_delivery = bool(sess.pending_stream_payload)
        if reply_req_id and final and (
            uncertain_stream_delivery
            or reply_idle_too_long(sess, reply_req_id)
            or reply_should_use_proactive(sess, reply_req_id)
        ):
            mark_reply_proactive(sess, reply_req_id)
            mention_user_id = (
                chat_key_to_user_id(key)
                if uncertain_stream_delivery and key.startswith("group-user:")
                else proactive_reply_mention_user_id(key, sess, reply_req_id)
            )
            proactive_payloads = [
                part
                for payload in outgoing_payloads
                for part in build_proactive_chat_payloads(key, reply_payload_content(payload), mention_user_id)
            ]
            sess.pending_stream_payload = None
            sess.pending_stream_payloads = None
            cleanup_reply_session(bot, reply_req_id, sess)
            proactive_payloads = decorate_context_replies(proactive_payloads, sess.reply_contexts.get(reply_req_id), final=final)
            return await send_or_store_session_payloads(bot, key, sess, proactive_payloads, final)
    req_id = payload_req_id(outgoing_payloads[-1])
    payload_kind = "final" if final else "stream"
    if bot.ws and not bot.ws.closed:
        failed_index: Optional[int] = None
        try:
            for index, outgoing_payload in enumerate(outgoing_payloads):
                await send_session_payload(bot, sess, outgoing_payload)
            if final:
                sess.pending_final_payload = None
                sess.pending_final_payloads = None
                sess.pending_stream_payload = None
                sess.pending_stream_payloads = None
                sess.pending_payload_retry_after = 0.0
                cleanup_reply_session(bot, original_reply_req_id, sess)
            else:
                sess.pending_stream_payload = None
                sess.pending_stream_payloads = None
                sess.pending_payload_retry_after = 0.0
            return True
        except Exception as exc:
            failed_index = index if 'index' in locals() else 0
            add_log(bot, f"[{key}] {payload_kind} send deferred req_id={req_id or '-'}: {exc}")

    remaining_payloads = outgoing_payloads[failed_index:] if 'failed_index' in locals() and failed_index is not None else outgoing_payloads
    if final:
        sess.pending_final_payload = dict(remaining_payloads[-1])
        sess.pending_final_payloads = clone_payload_sequence(remaining_payloads)
        sess.pending_stream_payload = None
        sess.pending_stream_payloads = None
    else:
        sess.pending_stream_payload = dict(remaining_payloads[-1])
        sess.pending_stream_payloads = clone_payload_sequence(remaining_payloads)
    add_log(bot, f"[{key}] cached {payload_kind} reply req_id={req_id or '-'}")
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
            sess.pending_stream_payloads = None
            return False
        uncertain_stream_delivery = bool(sess.pending_stream_payload)
        if reply_req_id and final and (
            uncertain_stream_delivery
            or reply_idle_too_long(sess, reply_req_id)
            or reply_should_use_proactive(sess, reply_req_id)
        ):
            mark_reply_proactive(sess, reply_req_id)
            proactive_payloads = build_proactive_chat_payloads(
                key,
                reply_payload_content(outgoing_payload),
                (
                    chat_key_to_user_id(key)
                    if uncertain_stream_delivery and key.startswith("group-user:")
                    else proactive_reply_mention_user_id(key, sess, reply_req_id)
                ),
            )
            sess.pending_stream_payload = None
            sess.pending_stream_payloads = None
            cleanup_reply_session(bot, reply_req_id, sess)
            proactive_payloads = decorate_context_replies(proactive_payloads, sess.reply_contexts.get(reply_req_id), final=final)
            return await send_or_store_session_payloads(bot, key, sess, proactive_payloads, final)
    req_id = payload_req_id(outgoing_payload)
    payload_kind = "final" if final else "stream"
    if bot.ws and not bot.ws.closed:
        try:
            await send_session_payload(bot, sess, outgoing_payload)
            if final:
                sess.pending_final_payload = None
                sess.pending_final_payloads = None
                sess.pending_stream_payload = None
                sess.pending_stream_payloads = None
                sess.pending_payload_retry_after = 0.0
                cleanup_reply_session(bot, original_reply_req_id, sess)
            else:
                sess.pending_stream_payload = None
                sess.pending_stream_payloads = None
                sess.pending_payload_retry_after = 0.0
            return True
        except Exception as exc:
            add_log(bot, f"[{key}] {payload_kind} send deferred req_id={req_id or '-'}: {exc}")

    if final:
        sess.pending_final_payload = outgoing_payload
        sess.pending_final_payloads = [dict(outgoing_payload)]
        sess.pending_stream_payload = None
        sess.pending_stream_payloads = None
    else:
        sess.pending_stream_payload = outgoing_payload
        sess.pending_stream_payloads = [dict(outgoing_payload)]
    add_log(bot, f"[{key}] cached {payload_kind} reply req_id={req_id or '-'}")
    return False


async def flush_session_pending_payloads(bot: BotState, key: str, sess: SessionState) -> None:
    if not bot.ws or bot.ws.closed:
        return
    pending_payload = sess.pending_final_payload or sess.pending_stream_payload
    if (
        pending_payload
        and time.monotonic() < sess.pending_payload_retry_after
        and any(same_payload_delivery(pending_payload, payload) for payload in sess.late_ack_payloads)
    ):
        return
    if sess.pending_final_payload:
        payloads = clone_payload_sequence(sess.pending_final_payloads or [sess.pending_final_payload])
        if await send_or_store_session_payloads(bot, key, sess, payloads, True):
            add_log(bot, f"[{key}] delivered cached final reply req_id={payload_req_id(payloads[-1]) or '-'}")
            if sess.queue and can_continue_session_queue(sess):
                process_queue(bot, sess, key)
        return
    if sess.pending_stream_payload and sess.running:
        payloads = clone_payload_sequence(sess.pending_stream_payloads or [sess.pending_stream_payload])
        if await send_or_store_session_payloads(bot, key, sess, payloads, False):
            add_log(bot, f"[{key}] delivered cached running status req_id={payload_req_id(payloads[-1]) or '-'}")


async def flush_all_pending_session_payloads(bot: BotState) -> None:
    if bot.pending_payload_flush_in_progress:
        return
    bot.pending_payload_flush_in_progress = True
    try:
        for key, sess in list(bot.sessions.items()):
            await flush_session_pending_payloads(bot, key, sess)
    finally:
        bot.pending_payload_flush_in_progress = False


def has_pending_session_payloads(bot: BotState) -> bool:
    for sess in bot.sessions.values():
        if sess.pending_final_payload or sess.pending_stream_payload:
            return True
    return False


def find_chat_key_for_session(bot: BotState, target: SessionState) -> Optional[str]:
    for key, sess in bot.sessions.items():
        if sess is target:
            return key
    return None


def consume_late_cached_session_payload_ack(bot: BotState, req_id: Optional[str]) -> bool:
    req_text = str(req_id or "").strip()
    if not req_text:
        return False
    for key, sess in list(bot.sessions.items()):
        tracked_index = next(
            (
                index
                for index, payload in enumerate(sess.late_ack_payloads)
                if payload_req_id(payload) == req_text
            ),
            None,
        )
        if tracked_index is None:
            continue
        tracked_payload = sess.late_ack_payloads.pop(tracked_index)
        cached_payloads: Optional[list[dict[str, Any]]] = None
        payload_kind = ""
        if sess.pending_final_payload:
            cached_payloads = clone_payload_sequence(sess.pending_final_payloads or [sess.pending_final_payload])
            payload_kind = "final"
        elif sess.pending_stream_payload:
            cached_payloads = clone_payload_sequence(sess.pending_stream_payloads or [sess.pending_stream_payload])
            payload_kind = "stream"
        if not cached_payloads or not same_payload_delivery(cached_payloads[0], tracked_payload):
            add_log(bot, f"[{key}] accepted late ack for superseded payload req_id={req_text}")
            return True
        cached_payload = cached_payloads[0]
        mark_session_reply_sent(bot, sess, cached_payload)
        sess.pending_payload_retry_after = 0.0
        remaining_payloads = cached_payloads[1:]
        if payload_kind == "final":
            if remaining_payloads:
                sess.pending_final_payload = dict(remaining_payloads[-1])
                sess.pending_final_payloads = clone_payload_sequence(remaining_payloads)
                add_log(bot, f"[{key}] accepted late ack for cached final reply fragment req_id={req_text}")
            else:
                sess.pending_final_payload = None
                sess.pending_final_payloads = None
                sess.pending_stream_payload = None
                sess.pending_stream_payloads = None
                add_log(bot, f"[{key}] accepted late ack for cached final reply req_id={req_text}")
                if sess.queue and can_continue_session_queue(sess):
                    process_queue(bot, sess, key)
        else:
            if remaining_payloads:
                sess.pending_stream_payload = dict(remaining_payloads[-1])
                sess.pending_stream_payloads = clone_payload_sequence(remaining_payloads)
                add_log(bot, f"[{key}] accepted late ack for cached stream reply fragment req_id={req_text}")
            else:
                sess.pending_stream_payload = None
                sess.pending_stream_payloads = None
                add_log(bot, f"[{key}] accepted late ack for cached stream reply req_id={req_text}")
        return True
    return False


async def flush_pending_session_payloads_from_inbound(bot: BotState, source: str) -> None:
    if not bot.ws or bot.ws.closed or not has_pending_session_payloads(bot):
        return
    try:
        await flush_all_pending_session_payloads(bot)
    except Exception as exc:
        add_log(bot, f"inbound-triggered pending payload flush skipped ({source}): {exc}")


async def respond_queued_error(bot: BotState, req_id: Optional[str], message: str) -> None:
    if not req_id:
        return
    sess = bot.reply_sessions.get(req_id)
    key = find_chat_key_for_session(bot, sess) if sess else None
    payload = {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": req_id},
        "body": {"msgtype": "stream", "stream": {"id": sess.session_id if sess else uid(), "finish": True, "content": f"Error: {message}"}},
    }
    try:
        if sess and key:
            delivered = await send_or_store_session_payload(bot, key, sess, payload, True)
            if delivered:
                cleanup_reply_session(bot, req_id, sess)
            return
        if sess:
            await send_session_payload(bot, sess, payload)
        else:
            response = await send_ws_payload_with_ack(bot, payload, REPLY_SEND_ACK_TIMEOUT_SEC)
            error = session_payload_ack_error(payload, response)
            if error is not None:
                raise error
    except Exception as exc:
        add_log(bot, f"queued error reply skipped req_id={req_id}: {exc}")
    else:
        cleanup_reply_session(bot, req_id, sess)


async def respond_info(
    bot: BotState,
    req_id: Optional[str],
    message: str,
    final: bool = True,
    *,
    cache_on_failure: bool = True,
) -> None:
    if not req_id:
        return
    sess = bot.reply_sessions.get(req_id)
    key = find_chat_key_for_session(bot, sess) if sess else None
    stream_id = sess.session_id if sess else uid()
    payloads = build_wecom_text_response_payloads(
        req_id, stream_id, message, final=final, max_chars=STREAM_TEXT_MAX_CHARS
    )
    payloads = decorate_context_replies(payloads, bot.callback_contexts.get(req_id), final=final)
    try:
        if sess and key and cache_on_failure:
            delivered = await send_or_store_session_payloads(bot, key, sess, payloads, final)
            if final and delivered:
                cleanup_reply_session(bot, req_id, sess)
            return
        if sess:
            for payload in payloads:
                await send_session_payload(bot, sess, payload)
        else:
            for payload in payloads:
                response = await send_ws_payload_with_ack(bot, payload, REPLY_SEND_ACK_TIMEOUT_SEC)
                error = session_payload_ack_error(payload, response)
                if error is not None:
                    raise error
    except Exception as exc:
        add_log(bot, f"info reply skipped req_id={req_id}: {exc}")
        return
    if final:
        cleanup_reply_session(bot, req_id, sess)


async def handle_template_card_event(bot: BotState, message: dict[str, Any]) -> None:
    req_id = str(((message.get("headers") or {}).get("req_id")) or "").strip()
    event = (((message.get("body") or {}).get("event") or {}).get("template_card_event") or {})
    if not req_id or not isinstance(event, dict):
        return
    card_type = str(event.get("card_type") or "").strip()
    event_key = str(event.get("event_key") or "").strip()
    task_id = str(event.get("task_id") or "").strip()
    click_user_id = message_sender_userid(message)
    owner_user_id = extract_template_card_owner_user_id(task_id)
    key = chat_key_for_bot(bot, message)
    action_token = f"{task_id}\n{event_key}\n{click_user_id}"
    repeatable_event = is_repeatable_template_card_event_key(event_key)
    if not card_type:
        return
    if owner_user_id and click_user_id and owner_user_id != click_user_id:
        add_event_log(
            bot,
            "template_card.click_rejected_non_owner",
            reqId=req_id,
            cardType=card_type,
            taskId=task_id or None,
            ownerUserId=owner_user_id,
            clickUserId=click_user_id or None,
            eventKey=event_key or None,
        )
        notice_payload = build_proactive_chat_payload(key, f"这是 {owner_user_id} 的卡片，你点击了但不会改动它。")
        try:
            await send_ws_payload_with_ack(bot, notice_payload, 5)
        except Exception as exc:
            add_log(bot, f"template card non-owner notice skipped req_id={req_id}: {exc}")
        return
    if card_type != "button_interaction":
        add_event_log(
            bot,
            "template_card.click_no_auto_update",
            reqId=req_id,
            cardType=card_type,
            taskId=task_id or None,
            ownerUserId=owner_user_id,
            clickUserId=click_user_id or None,
            eventKey=event_key or None,
        )
        notice_payload = build_proactive_chat_payload(key, f"已收到卡片点击：{event_key or card_type}。当前类型暂不自动更新卡片。")
        try:
            await send_ws_payload_with_ack(bot, notice_payload, 5)
        except Exception as exc:
            add_log(bot, f"template card informational notice skipped req_id={req_id}: {exc}")
        return
    if not repeatable_event and action_token in bot.consumed_template_card_actions:
        add_event_log(
            bot,
            "template_card.click_duplicate_ignored",
            reqId=req_id,
            cardType=card_type,
            taskId=task_id or None,
            ownerUserId=owner_user_id,
            clickUserId=click_user_id or None,
            eventKey=event_key or None,
        )
        notice_payload = build_proactive_chat_payload(key, "该操作已处理，请勿重复点击。")
        try:
            await send_ws_payload_with_ack(bot, notice_payload, 5)
        except Exception as exc:
            add_log(bot, f"template card duplicate notice skipped req_id={req_id}: {exc}")
        return
    event_text = (
        bot.template_card_button_texts.get(task_id, {}).get(event_key)
        or event_key
        or "确认"
    )
    original_card = dict(bot.template_card_payloads.get(task_id) or {})
    if repeatable_event:
        update_card = build_refresh_progress_template_card(original_card, task_id)
    else:
        update_card = dict(original_card)
        update_card["card_type"] = "button_interaction"
        update_card["task_id"] = task_id or f"processed-{uid()}"
        main_title = dict(update_card.get("main_title") or {})
        original_title = str(main_title.get("title") or "").strip()
        main_title["title"] = original_title or "已确认"
        if not str(main_title.get("desc") or "").strip():
            main_title["desc"] = event_text
        update_card["main_title"] = main_title
        rebuilt_buttons = []
        for item in original_card.get("button_list") or []:
            if not isinstance(item, dict):
                continue
            current = dict(item)
            if str(current.get("key") or "").strip() == event_key:
                current["style"] = 1
                current["text"] = str(current.get("text") or "").strip() or event_text
            rebuilt_buttons.append(current)
        if not rebuilt_buttons:
            rebuilt_buttons = [{"text": event_text, "style": 1, "key": event_key or "processed"}]
        update_card["button_list"] = rebuilt_buttons
        bot.consumed_template_card_actions.add(action_token)
    save_template_card_delivery_meta(bot, update_card["task_id"], chat_key=key, template_card=update_card)
    try:
        response = await send_ws_payload_with_ack(
            bot,
            build_template_card_update_payload(req_id, update_card),
            5,
        )
        require_ack_success(response, "template card update")
        add_event_log(
            bot,
            "template_card.updated",
            reqId=req_id,
            cardType=card_type,
            taskId=task_id or None,
            ownerUserId=owner_user_id,
            clickUserId=click_user_id or None,
            eventKey=event_key or None,
        )
    except Exception as exc:
        add_log(bot, f"template card update skipped req_id={req_id}: {exc}")
    if not repeatable_event:
        await enqueue_message(bot, key, event_text, None, silent_lease_failure=True)


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
    if base in {".", ".."}:
        return fallback
    return base or fallback


def build_nonconflicting_file_path(target_dir: Path, file_name: str) -> Path:
    normalized_name = Path(str(file_name or "")).name or "attachment.bin"
    if normalized_name in {".", ".."}:
        normalized_name = "attachment.bin"
    candidate = target_dir / normalized_name
    if not candidate.exists() and not candidate.is_symlink():
        return candidate
    path = Path(normalized_name)
    stem = path.stem or "attachment"
    suffix = path.suffix
    index = 1
    while True:
        candidate = target_dir / f"{stem}-{index}{suffix}"
        if not candidate.exists() and not candidate.is_symlink():
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
    if len(raw) == 32:
        return raw.encode("utf-8")
    try:
        normalized = raw if len(raw) % 4 == 0 else raw + ("=" * (4 - (len(raw) % 4)))
        decoded = base64.b64decode(normalized)
        if len(decoded) in {16, 24, 32}:
            return decoded
    except Exception:
        pass
    return None


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


async def download_buffer(url: str, max_bytes: int) -> dict[str, Any]:
    if HTTP_SESSION is None:
        raise BridgeError(500, "http session not ready")
    timeout = aiohttp.ClientTimeout(total=MEDIA_TOTAL_TIMEOUT / 1000, connect=MEDIA_CONNECT_TIMEOUT / 1000)
    async with HTTP_SESSION.get(url, timeout=timeout, allow_redirects=True) as response:
        if response.status != 200:
            raise BridgeError(502, f"media download failed: HTTP {response.status}")
        content_length = normalize_optional_int(response.headers.get("Content-Length"))
        if content_length is not None and content_length > max_bytes:
            raise BridgeError(413, f"media too large: {content_length} bytes (max {max_bytes})")
        data = bytearray()
        async for chunk in response.content.iter_chunked(64 * 1024):
            if len(data) + len(chunk) > max_bytes:
                raise BridgeError(413, f"media too large while downloading (max {max_bytes} bytes)")
            data.extend(chunk)
        return {
            "data": bytes(data),
            "contentType": response.headers.get("Content-Type", ""),
            "contentDisposition": response.headers.get("Content-Disposition", ""),
        }


async def download_buffer_via_curl(url: str, max_bytes: int) -> dict[str, Any]:
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
        "--max-filesize",
        str(max_bytes),
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
        body_size = body_file.stat().st_size if body_file.exists() else 0
        if process.returncode == 63 or body_size > max_bytes:
            raise BridgeError(413, f"media too large while downloading (max {max_bytes} bytes)")
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
    aes_key = payload.get("aeskey") or payload.get("aes_key")
    download_limit = inbound_limit + 32 if aes_key else inbound_limit

    try:
        response = await download_buffer(url, download_limit)
    except Exception as exc:
        if not should_fallback_to_curl(exc):
            raise
        add_log(bot, f"[{key}] direct download failed, fallback to curl: {exc}")
        response = await download_buffer_via_curl(url, download_limit)

    data = response["data"]
    if aes_key:
        data = decrypt_media_buffer(data, aes_key)
        if len(data) > inbound_limit:
            raise BridgeError(413, f"{kind} too large after decrypt: {len(data)} bytes")
    elif len(data) > inbound_limit:
        raise BridgeError(413, f"{kind} too large: {len(data)} bytes")

    target_dir = ensure_session_workspace_dirs_light_for_session(bot, key, sess.session_id)["chatfile"]
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
        if not isinstance(item, dict):
            continue
        if item.get("msgtype") == "text":
            content = ((item.get("text") or {}).get("content") or "").strip()
            if content:
                parts.append(content)
    return "\n".join(parts)


def extract_mixed_images(mixed: dict[str, Any]) -> list[dict[str, Any]]:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    return [
        item.get("image")
        for item in items
        if (
            isinstance(item, dict)
            and item.get("msgtype") == "image"
            and isinstance(item.get("image"), dict)
            and (
                item["image"].get("url")
                or item["image"].get("download_url")
                or item["image"].get("downloadUrl")
            )
        )
    ]


def restore_bridge_state_from_local_resume_backup(bot: BotState, sess: SessionState, key: str) -> None:
    record = read_session_record_by_id(sess.session_id) or {}
    backup = normalize_local_resume_backup(record.get("localResumeBackup"))
    if backup is not None:
        sess.thread_id = str(backup.get("threadId") or "").strip() or None
        sess.chat = [dict(item) for item in normalize_session_chat_items(backup.get("recentChat"))]
    else:
        sess.thread_id = None
        sess.chat = []
    sess.use_global_codex_sessions = False
    clear_resume_selection(sess)
    update_owned_session_record(
        bot,
        key,
        sess,
        lambda current: {
            **current,
            "threadId": sess.thread_id,
            "useGlobalCodexSessions": False,
            "localResumeBackup": None,
            "recentChat": session_recent_chat_snapshot(sess),
        },
    )


def get_or_create_session(bot: BotState, key: str) -> SessionState:
    if key not in bot.sessions:
        record = get_or_create_session_record(bot, key)
        sess = SessionState(
            session_id=record["sessionId"],
            work_dir=str(get_session_runtime_cwd(bot, key, session_id=record["sessionId"])),
            lock_file=Path(record["lockFile"]),
            thread_id=str(record.get("threadId") or "").strip() or None,
            chat=list(record.get("recentChat") or []),
            use_global_codex_sessions=normalize_internal_bool_flag(record.get("useGlobalCodexSessions"), default=False),
        )
        bot.sessions[key] = sess
        add_log(bot, f"[{key}] new session")
        add_event_log(bot, "session.created", chatKey=key, sessionId=sess.session_id)
    sess = bot.sessions[key]
    if sess.use_global_codex_sessions:
        revoke_reason = None
        if not local_resume_binding_allowed_for_chat(bot.config, key):
            revoke_reason = "unauthorized"
        elif sess.thread_id:
            with local_resume_thread_binding_lock(sess.thread_id):
                if find_local_resume_thread_binding(sess.thread_id, excluding_session_id=sess.session_id) is not None:
                    revoke_reason = "duplicate"
        if revoke_reason:
            if acquire_session_lease(bot, sess, key):
                restore_bridge_state_from_local_resume_backup(bot, sess, key)
                add_log(bot, f"[{key}] revoked {revoke_reason} local session binding")
            else:
                add_log(bot, f"[{key}] deferred {revoke_reason} local session binding revocation; lease not owned")
    sess.work_dir = str(get_session_runtime_cwd(bot, key, session_id=sess.session_id))
    sess.last_active = time.time()
    return sess


async def get_or_create_session_ready(bot: BotState, key: str) -> SessionState:
    sess = get_or_create_session(bot, key)
    await run_blocking(ensure_session_workspace_dirs, bot, key, session_id=sess.session_id)
    sess.work_dir = str(get_session_runtime_cwd(bot, key, session_id=sess.session_id))
    return sess


def build_prompt(
    bot: BotState,
    sess: SessionState,
    key: str,
    text: str,
    pending_media: list[dict[str, Any]],
    pending_media_notes: list[str],
    *,
    compat_history: Optional[list[dict[str, Any]]] = None,
) -> str:
    media_context = build_pending_media_context(pending_media)
    media_notes = build_pending_media_notes(pending_media_notes)
    active_download_note = build_active_media_download_note(sess.pending_media_downloads)
    parts = [build_bridge_context(bot, sess, key), media_context, media_notes, active_download_note]
    parts = [part for part in parts if part]
    prompt = f"{chr(10).join(parts)}\n\nUser request:\n{text}"
    if bot_agent_backend(bot.config) in {"codex", "claude"}:
        return prompt
    history_lines: list[str] = []
    history_items = sess.chat if compat_history is None else compat_history
    for item in history_items[-12:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        body = str(item.get("text") or "").strip()
        if role not in {"user", "bot"} or not body:
            continue
        history_lines.append(f"{'User' if role == 'user' else 'Assistant'}: {body}")
    if not history_lines:
        return prompt
    return (
        f"{prompt}\n\n[RecentConversation]\n"
        "Use this recent bridge-local chat history for continuity when native resume is unavailable.\n"
        f"{chr(10).join(history_lines)}\n[/RecentConversation]"
    )


def build_compat_history_prompt(
    bot: BotState,
    sess: SessionState,
    prompt: str,
    *,
    compat_history: Optional[list[dict[str, Any]]] = None,
) -> str:
    history_lines: list[str] = []
    history_items = sess.chat if compat_history is None else compat_history
    for item in history_items[-MAX_COMPAT_HISTORY_ITEMS:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        body = str(item.get("text") or "").strip()
        if role not in {"user", "bot"} or not body:
            continue
        history_lines.append(f"{'User' if role == 'user' else 'Assistant'}: {body}")
    if not history_lines:
        return prompt
    return (
        f"{prompt}\n\n[RecentConversation]\n"
        "Use this recent bridge-local chat history for continuity when native resume is unavailable.\n"
        f"{chr(10).join(history_lines)}\n[/RecentConversation]"
    )


def session_compat_history_snapshot(
    sess: SessionState,
    key: str,
    current_text: str,
    pending_queue: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    pending_texts = [str(current_text or "").strip()]
    pending_texts.extend(str(item.get("text") or "").strip() for item in pending_queue if isinstance(item, dict))
    pending_texts = [text for text in pending_texts if text]
    pending_index = len(pending_texts) - 1
    excluded_indices: set[int] = set()
    for index in range(len(sess.chat) - 1, -1, -1):
        if pending_index < 0:
            break
        item = sess.chat[index]
        if not isinstance(item, dict) or str(item.get("role") or "").strip() != "user":
            continue
        source = str(item.get("from") or "").strip()
        text = str(item.get("text") or item.get("content") or "").strip()
        if (not source or source == key) and text == pending_texts[pending_index]:
            excluded_indices.add(index)
            pending_index -= 1
    history = [dict(item) for index, item in enumerate(sess.chat) if index not in excluded_indices]
    return normalize_session_chat_items(history, limit=MAX_COMPAT_HISTORY_ITEMS)


def local_codex_sessions_root() -> Path:
    return (DEFAULT_CODEX_HOME / "sessions").resolve()


def parse_jsonl_timestamp_ms(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except ValueError:
        return 0


def iter_local_codex_rollout_files() -> list[Path]:
    root = local_codex_sessions_root()
    if not root.exists() or not root.is_dir():
        return []
    files: list[Path] = []
    try:
        for candidate in root.rglob("rollout-*.jsonl"):
            if candidate.is_file():
                files.append(candidate)
    except Exception:
        return []

    def sort_key(item: Path) -> tuple[int, str]:
        try:
            modified_at = int(item.stat().st_mtime_ns)
        except OSError:
            modified_at = 0
        return modified_at, str(item)

    files.sort(
        key=sort_key,
        reverse=True,
    )
    return files


def read_local_codex_session_meta(rollout_file: Path) -> Optional[dict[str, Any]]:
    try:
        with rollout_file.open("r", encoding="utf-8") as handle:
            first_line = handle.readline()
    except Exception:
        return None
    if not first_line:
        return None
    try:
        payload = json.loads(first_line)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("type") or "").strip() != "session_meta":
        return None
    meta = payload.get("payload")
    if not isinstance(meta, dict):
        return None
    session_id = str(meta.get("session_id") or meta.get("id") or "").strip()
    cwd_text = str(meta.get("cwd") or "").strip()
    if not session_id or not cwd_text:
        return None
    try:
        cwd = Path(cwd_text).expanduser().resolve()
    except Exception:
        return None
    rollout_mtime_ns = 0
    rollout_size = -1
    try:
        rollout_stat = rollout_file.stat()
        rollout_mtime_ns = int(rollout_stat.st_mtime_ns)
        rollout_size = int(rollout_stat.st_size)
        timestamp_ms = int(rollout_mtime_ns / 1_000_000)
    except OSError:
        timestamp_ms = parse_jsonl_timestamp_ms(meta.get("timestamp"))
    return {
        "sessionId": session_id,
        "threadId": session_id,
        "cwd": cwd,
        "rolloutFile": rollout_file,
        "rolloutMtimeNs": rollout_mtime_ns,
        "rolloutSize": rollout_size,
        "updatedAt": timestamp_ms,
        "lastRunAt": timestamp_ms,
        "status": "local",
        "source": "local",
    }


def extract_local_codex_message_text(content: Any) -> str:
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if text:
            parts.append(text)
    return "\n".join(parts).strip()


def sanitize_imported_local_user_text(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"^(?:(?:<image\b[^>]*>|</image>)\s*)+", "", cleaned, flags=re.IGNORECASE)
    if not cleaned:
        return ""
    if cleaned.startswith("<environment_context>") or cleaned.startswith("<turn_aborted>"):
        return ""
    if cleaned.startswith("<codex_internal_context"):
        return ""
    if cleaned.startswith("<goal_context>") or cleaned.startswith("<user_action>"):
        return ""
    bubblewrap_warning_tail = "Codex will use the bundled bubblewrap in the meantime."
    if cleaned.startswith("⚠ Codex could not find bubblewrap on PATH."):
        if bubblewrap_warning_tail in cleaned:
            cleaned = cleaned.split(bubblewrap_warning_tail, 1)[1].lstrip()
        else:
            return ""
        if not cleaned:
            return ""
    had_bridge_wrapper = False
    if cleaned.startswith("[BridgeContext]"):
        had_bridge_wrapper = True
        marker = "\n\nUser request:\n"
        if marker in cleaned:
            cleaned = cleaned.split(marker, 1)[1].strip()
        elif "[/BridgeContext]" in cleaned:
            cleaned = cleaned.split("[/BridgeContext]", 1)[1].strip()
    compat_marker = "\n\n[RecentConversation]\n"
    if (had_bridge_wrapper or cleaned.startswith("[RecentConversation]")) and compat_marker in cleaned:
        cleaned = cleaned.split(compat_marker, 1)[0].rstrip()
    return cleaned


def load_local_codex_recent_chat(rollout_file: Path, *, limit: int = MAX_PERSISTED_SESSION_CHAT_ITEMS) -> list[dict[str, Any]]:
    effective_limit = max(0, int(limit))
    history: deque[dict[str, Any]] = deque(maxlen=effective_limit)
    try:
        with rollout_file.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if not isinstance(payload, dict):
                    continue
                if str(payload.get("type") or "").strip() != "response_item":
                    continue
                item = payload.get("payload")
                if not isinstance(item, dict):
                    continue
                if str(item.get("type") or "").strip() != "message":
                    continue
                role = str(item.get("role") or "").strip()
                if role not in {"user", "assistant"}:
                    continue
                text = extract_local_codex_message_text(item.get("content"))
                if not text:
                    continue
                if role == "user":
                    text = sanitize_imported_local_user_text(text)
                    if not text:
                        continue
                else:
                    if str(item.get("phase") or "").strip() == "commentary":
                        continue
                message = {"role": "user" if role == "user" else "bot", "text": text}
                timestamp_sec = int(parse_jsonl_timestamp_ms(payload.get("timestamp")) / 1000)
                if timestamp_sec > 0:
                    message["time"] = timestamp_sec
                history.append(message)
    except Exception:
        return []
    return normalize_session_chat_items(list(history), limit=effective_limit)


def list_local_codex_resume_candidates(current_cwd: Path) -> list[dict[str, Any]]:
    resolved_cwd = current_cwd.expanduser().resolve()
    latest_by_session: dict[str, dict[str, Any]] = {}
    for rollout_file in iter_local_codex_rollout_files():
        meta = read_local_codex_session_meta(rollout_file)
        if not meta:
            continue
        session_id = str(meta["sessionId"])
        if session_id in latest_by_session:
            continue
        latest_by_session[session_id] = dict(meta)
    candidates = [candidate for candidate in latest_by_session.values() if candidate["cwd"] == resolved_cwd]
    candidates.sort(
        key=lambda item: (
            int(item.get("lastRunAt") or 0),
            int(item.get("updatedAt") or 0),
            str(item.get("sessionId") or ""),
        ),
        reverse=True,
    )
    return candidates


def find_local_codex_resume_candidate(session_id: str, current_cwd: Path) -> Optional[dict[str, Any]]:
    target_id = str(session_id or "").strip()
    if not target_id:
        return None
    for candidate in list_local_codex_resume_candidates(current_cwd):
        if target_id == str(candidate.get("sessionId") or "").strip():
            return candidate
    return None


def session_run_is_current(sess: SessionState, current_task: Optional[asyncio.Task], run_generation: Optional[int]) -> bool:
    if run_generation is None:
        return sess.run_task is None or sess.run_task is current_task
    return sess.run_task is current_task and sess.run_generation == run_generation


def create_agent_output_file(session_id: str) -> Path:
    session_digest = hashlib.sha256(str(session_id or "").encode("utf-8")).hexdigest()[:16]
    fd, path_text = tempfile.mkstemp(prefix=f"wecom-codex-py-{session_digest}-", suffix=".txt")
    try:
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)
    return Path(path_text)


def checkpoint_session_run_thread(sess: SessionState) -> None:
    sess.active_run_thread_checkpoint = sess.thread_id
    sess.active_run_thread_checkpoint_set = True


def clear_session_run_thread_checkpoint(sess: SessionState) -> None:
    sess.active_run_thread_checkpoint = None
    sess.active_run_thread_checkpoint_set = False


def restore_session_run_thread_checkpoint(sess: SessionState) -> None:
    if sess.active_run_thread_checkpoint_set:
        sess.thread_id = sess.active_run_thread_checkpoint
    clear_session_run_thread_checkpoint(sess)


def session_context_switch_is_quiescent(sess: SessionState) -> bool:
    active_task = sess.run_task
    active_proc = sess.proc
    return not (
        sess.running
        or sess.queue
        or sess.pending_final_payload
        or sess.pending_media_downloads > 0
        or sess.pending_media
        or sess.pending_media_notes
        or (active_task is not None and not active_task.done())
        or (active_proc is not None and active_proc.returncode is None)
    )


def ensure_session_context_switch_quiescent(sess: SessionState) -> None:
    if not session_context_switch_is_quiescent(sess):
        raise BridgeError(409, "当前仍有任务或待发送结果，不能切换会话上下文。请先等待完成或使用 /bridge-interrupt。")
    record = read_session_record_by_id(sess.session_id)
    if record is not None:
        ensure_session_control_safe(record)


def acquire_session_context_switch_lease(bot: BotState, sess: SessionState, key: str) -> None:
    if not acquire_session_lease(bot, sess, key):
        raise BridgeError(409, f"session is owned by another instance: {key}")


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
    sess.interrupt_queue_resume_deferred = False
    sess.run_generation += 1
    run_generation = sess.run_generation
    register_reply_session(bot, item.get("reqId"), sess)
    sess.active_message_context = dict(bot.callback_contexts.get(str(item.get("reqId") or ""), item.get("messageContext") or {}))
    sess.active_schedule_id = str(item.get("scheduleId") or "").strip() or None
    sess.active_scheduled_job_file = str(item.get("scheduledJobFile") or "").strip() or None
    sess.active_schedule_request_id = str(item.get("scheduleRequestId") or "").strip() or None
    pending_media = list(sess.pending_media)
    pending_media_notes = list(sess.pending_media_notes)
    compat_history = session_compat_history_snapshot(sess, key, item["text"], sess.queue)
    prompt = build_prompt(
        bot,
        sess,
        key,
        item["text"],
        pending_media,
        pending_media_notes,
        compat_history=compat_history,
    )
    image_paths = [media["path"] for media in pending_media if media.get("kind") == "image"]
    sess.active_run_media = list(pending_media)
    sess.active_run_media_notes = list(pending_media_notes)
    checkpoint_session_run_thread(sess)
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
            compat_history=compat_history,
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
    compat_history: Optional[list[dict[str, Any]]] = None,
) -> None:
    semaphore = get_codex_run_semaphore()
    current_task = asyncio.current_task()
    backend = bot_agent_backend(bot.config)
    agent_command = bot_agent_command(bot.config)
    final = "(no output)"
    return_code = 1
    output_file: Optional[Path] = None
    active_process: Optional[asyncio.subprocess.Process] = None
    process_started = False
    active_schedule_id = str(sess.active_schedule_id or "").strip() or None
    final_delivery_pending = False
    local_resume_fallback = False

    def owns_context() -> bool:
        return session_run_is_current(sess, current_task, run_generation) and session_lease_is_owned_by_current_instance(sess)

    if owns_context():
        checkpoint_session_run_thread(sess)

    try:
        if not owns_context():
            return
        await send_session_status(bot, key, sess, req_id, "**运行状态：启动中**")
        async with semaphore:
            while True:
                if not owns_context():
                    return
                if sess.interrupt_requested:
                    restore_session_run_thread_checkpoint(sess)
                    sess.interrupt_requested = False
                    sess.running = False
                    sess.proc = None
                    sess.active_schedule_id = None
                    sess.active_scheduled_job_file = None
                    sess.active_schedule_request_id = None
                    sess.last_active = time.time()
                    update_owned_session_record(
                        bot,
                        key,
                        sess,
                        lambda record: {**record, "status": "idle", "activeScheduleId": None},
                    )
                    return
                sess.last_active = time.time()
                if update_owned_session_record(
                    bot,
                    key,
                    sess,
                    lambda record: {**record, "status": "running", "activeScheduleId": active_schedule_id},
                ) is None:
                    return

                output_file = create_agent_output_file(sess.session_id)
                checkpoint_session_run_thread(sess)
                attempted_resume = backend in {"codex", "claude"} and bool(sess.thread_id)
                effective_prompt = (
                    prompt
                    if attempted_resume
                    else build_compat_history_prompt(bot, sess, prompt, compat_history=compat_history)
                )
                args = build_codex_base_args(
                    output_file,
                    image_paths,
                    resume=attempted_resume,
                    backend=backend,
                    command=agent_command,
                    resume_thread_id=sess.thread_id if attempted_resume else None,
                )

                workspace_paths = await run_blocking(ensure_session_workspace_dirs, bot, key)
                codex_home = await run_blocking(
                    build_codex_home_for_subprocess,
                    sess.session_id,
                    use_global_sessions=sess.use_global_codex_sessions,
                )
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
                        "WECOM_BRIDGE_AGENT_BACKEND": backend,
                        "WECOM_BRIDGE_ASSISTANT_TASK_ID": str(sess.active_message_context.get("assistant_task_id") or ""),
                        "WECOM_BRIDGE_WORKSPACE_MODE": bot_workspace_mode(bot.config),
                        "WECOM_BRIDGE_WORKDIR_DIR": str(workspace_paths["workDir"]),
                        "WECOM_BRIDGE_CWD_DIR": str(sess.work_dir),
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
                if agent_command:
                    env["WECOM_BRIDGE_AGENT_COMMAND"] = agent_command
                env = codex_runtime.prepare_codex_subprocess_env(env, command=args[0] if args else None)
                process_cwd = Path(sess.work_dir)

                process = await asyncio.create_subprocess_exec(
                    *args,
                    cwd=process_cwd,
                    env=env,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    limit=SUBPROCESS_STREAM_LIMIT,
                    **isolated_subprocess_kwargs(),
                )
                remember_isolated_process(process)
                active_process = process
                if not owns_context():
                    await terminate_process(process)
                    return
                process_started = True
                sess.proc = process
                sess.codex_runtime_status = None
                add_log(bot, f"[{key}] {backend} {'resume' if attempted_resume else 'exec'} started")

                latest_message = ""
                latest_progress = AgentProgress("分析中")
                stderr_parts: list[str] = []

                async def ticker() -> None:
                    if not req_id:
                        return
                    last_sent_content = "**运行状态：分析中**"
                    while sess.running:
                        await asyncio.sleep(STATUS_STREAM_INTERVAL_SEC)
                        if not sess.running or not owns_context():
                            return
                        if req_id not in sess.reply_proactive_req_ids and reply_age_too_long(sess, req_id):
                            await close_reply_stream_for_proactive(bot, key, sess, req_id)
                        proactive_mode = req_id in sess.reply_proactive_req_ids
                        if proactive_mode and not proactive_status_due(sess, req_id):
                            continue
                        if not proactive_mode and not STATUS_STREAM_PROGRESS_ENABLED:
                            continue
                        content = (
                            build_status_stream_content(
                                f"**运行状态：{latest_progress.status}**", latest_progress.summary
                            )
                            if STATUS_STREAM_PROGRESS_ENABLED
                            else "**运行状态：后台运行中**\n\n任务仍在运行，完成后会主动发送最终结果。"
                        )
                        if not proactive_mode and content == last_sent_content:
                            continue
                        if await send_session_status(bot, key, sess, req_id, content):
                            last_sent_content = content

                async def read_stdout() -> None:
                    nonlocal latest_message, latest_progress
                    assert process.stdout is not None
                    async for line in iter_subprocess_lines(process.stdout, bot, key, "stdout"):
                        extracted_thread = extract_agent_thread_id(normalize_agent_backend(backend), line)
                        if extracted_thread and owns_context():
                            sess.thread_id = extracted_thread
                        extracted_reply = extract_agent_reply(normalize_agent_backend(backend), line)
                        if extracted_reply:
                            latest_message = ANSI_RE.sub("", extracted_reply)
                        extracted_progress = extract_agent_progress(normalize_agent_backend(backend), line)
                        if extracted_progress is not None:
                            latest_progress = latest_progress.updated(AgentProgress(
                                extracted_progress.status, ANSI_RE.sub("", extracted_progress.summary)
                            ))

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
                        add_log(bot, f"[{key}] {backend} stderr: {text}")

                ticker_task = asyncio.create_task(ticker())
                stdout_task = asyncio.create_task(read_stdout())
                stderr_task = asyncio.create_task(read_stderr())
                prompt_write_error: Optional[Exception] = None
                try:
                    await send_session_status(bot, key, sess, req_id, "**运行状态：分析中**")
                    try:
                        await write_process_prompt(process, effective_prompt)
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

                if not owns_context():
                    return

                stderr_text = "\n".join(stderr_parts)
                resume_error_text = "\n".join(part for part in (stderr_text, str(prompt_write_error or "")) if part)
                resume_state_missing = (
                    allow_fresh_fallback
                    and attempted_resume
                    and re.search(
                        r"no rollout found|no prompt provid\w* via stdin|thread [0-9a-f-]+ not found|"
                        r"failed to record rollout items: thread [0-9a-f-]+ not found|"
                        r"failed to resolve rollout path .*file does not exist|No conversation found with session ID:",
                        resume_error_text,
                        re.I,
                    )
                )
                if resume_state_missing:
                    add_log(bot, f"[{key}] {backend} resume state missing, fallback to isolated fresh exec")
                    sess.proc = None
                    sess.thread_id = None
                    local_resume_fallback = sess.use_global_codex_sessions
                    sess.use_global_codex_sessions = False
                    checkpoint_session_run_thread(sess)
                    if update_owned_session_record(
                        bot,
                        key,
                        sess,
                        lambda record: {
                            **record,
                            "status": "starting",
                            "activeScheduleId": active_schedule_id,
                            "threadId": None,
                            "useGlobalCodexSessions": False,
                        },
                    ) is None:
                        return
                    try:
                        output_file.unlink()
                    except FileNotFoundError:
                        pass
                    output_file = None
                    allow_fresh_fallback = False
                    process_started = False
                    continue
                if prompt_write_error is not None and return_code == 0:
                    raise prompt_write_error
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
                    final = extract_agent_reply(normalize_agent_backend(backend), "") or stderr_text or "(no output)"

                if local_resume_fallback:
                    final = f"提示：原本地 Codex 会话已不可用，已基于近期聊天创建新会话。\n\n{final}"
                final = limit_stream_content(final)
                clear_session_run_thread_checkpoint(sess)
                break

        if not owns_context():
            return
        if sess.interrupt_requested:
            restore_session_run_thread_checkpoint(sess)
            sess.interrupt_requested = False
            sess.running = False
            sess.proc = None
            sess.active_schedule_id = None
            sess.active_scheduled_job_file = None
            sess.active_schedule_request_id = None
            sess.codex_runtime_status = None
            sess.last_active = time.time()
            restore_session_active_media(sess)
            update_owned_session_record(
                bot,
                key,
                sess,
                lambda record: {
                    **record,
                    "status": "idle",
                    "activeScheduleId": None,
                    "threadId": sess.thread_id,
                },
            )
            if not sess.interrupt_queue_resume_deferred:
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

        final_payloads = build_session_text_payloads(key, sess, req_id, final, True)
        if final_payloads:
            final_delivery_pending = not await send_or_store_session_payloads(bot, key, sess, final_payloads, True)
        add_log(bot, f"[{key}] reply completed ({len(final)} chars, exit={return_code})")
        append_session_chat_message(sess, {"role": "bot", "text": final, "time": int(time.time())})
        persisted_result = update_owned_session_record(
            bot,
            key,
            sess,
            lambda record: {
                **record,
                "status": "idle" if return_code == 0 else "error",
                "activeScheduleId": None,
                "lastRunAt": now_ms(),
                "threadId": sess.thread_id,
                "recentChat": session_recent_chat_snapshot(sess),
            },
        )
        if persisted_result is None:
            return
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
            restore_session_run_thread_checkpoint(sess)
            sess.running = False
            sess.proc = None
            sess.active_schedule_id = None
            sess.active_scheduled_job_file = None
            sess.active_schedule_request_id = None
            sess.codex_runtime_status = None
            sess.pending_stream_payload = None
            sess.pending_stream_payloads = None
            sess.pending_final_payload = None
            sess.pending_final_payloads = None
            sess.last_active = time.time()
            restore_session_active_media(sess)
            update_owned_session_record(
                bot,
                key,
                sess,
                lambda record: {
                    **record,
                    "status": "idle",
                    "activeScheduleId": None,
                    "threadId": sess.thread_id,
                },
            )
            if not sess.interrupt_queue_resume_deferred:
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
        restore_session_run_thread_checkpoint(sess)
        sess.running = False
        sess.proc = None
        sess.active_schedule_id = None
        sess.active_scheduled_job_file = None
        sess.active_schedule_request_id = None
        sess.codex_runtime_status = None
        sess.last_active = time.time()
        sess.pending_stream_payload = None
        sess.pending_stream_payloads = None
        sess.pending_final_payload = None
        sess.pending_final_payloads = None
        restore_session_active_media(sess)
        error_text = f"Error: {'failed to start agent' if not process_started else 'agent run failed'}: {exc}"
        add_log(bot, f"[{key}] {backend} task failed: {exc}")
        try:
            error_payloads = build_session_text_payloads(key, sess, req_id, error_text, True)
            if error_payloads:
                final_delivery_pending = not await send_or_store_session_payloads(bot, key, sess, error_payloads, True)
        except Exception as send_exc:
            add_log(bot, f"[{key}] failed to report codex error: {send_exc}")
        append_session_chat_message(sess, {"role": "bot", "text": error_text, "time": int(time.time())})
        try:
            update_owned_session_record(
                bot,
                key,
                sess,
                lambda record: {
                    **record,
                    "status": "error",
                    "activeScheduleId": None,
                    "lastRunAt": now_ms(),
                    "threadId": sess.thread_id,
                    "recentChat": session_recent_chat_snapshot(sess),
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
        restore_session_run_thread_checkpoint(sess)
        sess.running = False
        sess.proc = None
        sess.active_schedule_id = None
        sess.active_scheduled_job_file = None
        sess.active_schedule_request_id = None
        sess.codex_runtime_status = None
        sess.last_active = time.time()
        sess.pending_stream_payload = None
        sess.pending_stream_payloads = None
        sess.pending_final_payload = None
        sess.pending_final_payloads = None
        restore_session_active_media(sess)
        update_owned_session_record(
            bot,
            key,
            sess,
            lambda record: {
                **record,
                "status": "idle",
                "activeScheduleId": None,
                "threadId": sess.thread_id,
            },
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
    sess.pending_stream_payloads = None
    sess.pending_final_payload = None
    sess.pending_final_payloads = None
    sess.late_ack_payloads.clear()
    sess.pending_payload_retry_after = 0.0
    sess.active_run_media = []
    sess.active_run_media_notes = []
    clear_session_run_thread_checkpoint(sess)
    cleanup_session_reply_contexts(bot, sess)
    release_session_lease(bot, key, sess)
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
    sess.interrupt_queue_resume_deferred = False
    if had_active_run:
        restore_session_run_thread_checkpoint(sess)
        sess.run_generation += 1
    else:
        clear_session_run_thread_checkpoint(sess)
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
    clear_session_run_thread_checkpoint(sess)
    if clear_queue:
        recover_session_scheduled_jobs(sess)
        sess.queue.clear()
        cancelled_file_jobs = cancel_pending_file_send_requests(
            bot,
            key,
            sess.session_id,
            "new session before file send",
        )
        if cancelled_file_jobs:
            add_log(bot, f"[{key}] cancelled pending file sends: {cancelled_file_jobs}")
    if clear_chat:
        sess.chat.clear()
    if clear_thread:
        sess.pending_media.clear()
        sess.pending_media_notes.clear()
        sess.pending_media_downloads = 0
        remove_session_codex_home(sess.session_id)
    elif clear_queue:
        sess.pending_media.clear()
        sess.pending_media_notes.clear()
        sess.pending_media_downloads = 0
    sess.pending_stream_payload = None
    sess.pending_stream_payloads = None
    sess.pending_final_payload = None
    sess.pending_final_payloads = None
    sess.late_ack_payloads.clear()
    sess.pending_payload_retry_after = 0.0
    cleanup_session_reply_contexts(bot, sess)
    if clear_thread:
        sess.thread_id = None
        sess.use_global_codex_sessions = False
    sess.last_active = time.time()
    keep_lease = sess.lease_owned and not clear_queue and bool(sess.queue)
    if keep_lease:
        current_lease = read_lease(sess.lock_file) if sess.lock_file else None
        lease_expires_at = current_lease.get("expiresAt") if isinstance(current_lease, dict) else None
        update_owned_session_record(
            bot,
            key,
            sess,
            lambda record: {
                **record,
                "ownerInstance": INSTANCE_ID,
                "ownerPid": os.getpid(),
                "leaseExpiresAt": lease_expires_at or record.get("leaseExpiresAt"),
                "status": "leased",
                "activeScheduleId": None,
                "lastRunAt": now_ms(),
                "threadId": sess.thread_id,
                "useGlobalCodexSessions": sess.use_global_codex_sessions,
                "localResumeBackup": None if clear_thread or clear_chat else record.get("localResumeBackup"),
                "recentChat": session_recent_chat_snapshot(sess),
            },
        )
    else:
        release_session_lease_and_update(
            bot,
            key,
            sess,
            lambda record: {
                **record,
                "ownerInstance": None,
                "ownerPid": None,
                "leaseExpiresAt": None,
                "status": "idle",
                "activeScheduleId": None,
                "lastRunAt": now_ms(),
                "threadId": sess.thread_id,
                "useGlobalCodexSessions": sess.use_global_codex_sessions,
                "localResumeBackup": None if clear_thread or clear_chat else record.get("localResumeBackup"),
                "recentChat": session_recent_chat_snapshot(sess),
            },
        )


async def interrupt_session_and_wait(bot: BotState, key: str) -> tuple[dict[str, Any], Optional[SessionState]]:
    """Interrupt a live run only after its process and task have quiesced."""
    async with get_session_lifecycle_lock(bot, key):
        sess = bot.sessions.get(key)
        if sess is None:
            return control_session(bot, key, clear_thread=False, clear_chat=False), None

        acquire_session_control_lease(bot, key, sess)
        active_task = sess.run_task
        active_proc = sess.proc
        active_reply_req_ids = active_reply_request_ids(bot, sess)
        had_active_run = bool(
            sess.running
            or (active_task is not None and not active_task.done())
            or (active_proc is not None and active_proc.returncode is None)
        )
        if had_active_run:
            sess.interrupt_requested = True
            sess.interrupt_queue_resume_deferred = True
            restore_session_run_thread_checkpoint(sess)
            sess.run_generation += 1

        await terminate_process(
            active_proc,
            timeout_sec=SESSION_CONTROL_EXIT_TIMEOUT_SEC,
            require_exit=True,
            skip_wait_if_exited=True,
        )
        task_stopped = await cancel_task(active_task, timeout_sec=SESSION_CONTROL_EXIT_TIMEOUT_SEC)
        if not task_stopped:
            raise BridgeError(409, "agent task did not exit; current session is still stopping")
        if not session_lease_is_owned_by_current_instance(sess):
            abandon_session_after_lease_loss(bot, key, sess)
            raise BridgeError(409, f"session ownership changed while interrupting: {key}")

        if sess.run_task is active_task:
            sess.run_task = None
        if sess.proc is active_proc:
            sess.proc = None
        sess.running = False
        result = control_session(bot, key, clear_thread=False, clear_chat=False)
        # control_session clears cached payload state.  Close the original reply
        # streams afterwards so an unavailable websocket leaves a retryable final
        # packet instead of a client-side loading indicator.
        await finish_interrupted_reply_streams(bot, key, sess, active_reply_req_ids)
        sess.interrupt_queue_resume_deferred = False
        return result, sess


def abandon_session_after_lease_loss(bot: BotState, key: str, sess: SessionState) -> None:
    active_task = sess.run_task
    active_proc = sess.proc
    had_active_run = bool(
        sess.running
        or (active_task is not None and not active_task.done())
        or (active_proc is not None and active_proc.returncode is None)
    )
    add_log(bot, f"[{key}] abandoned local session state after lease loss")
    add_event_log(
        bot,
        "session.lease_lost",
        chatKey=key,
        sessionId=sess.session_id,
        activeRun=had_active_run,
    )
    if had_active_run:
        sess.run_generation += 1
    if active_task is not None and not active_task.done():
        active_task.cancel()
    if active_proc is not None and active_proc.returncode is None:
        try:
            active_proc.kill()
        except ProcessLookupError:
            pass
    sess.running = False
    sess.interrupt_requested = False
    sess.interrupt_queue_resume_deferred = False
    sess.proc = None
    sess.active_schedule_id = None
    sess.active_scheduled_job_file = None
    sess.active_schedule_request_id = None
    sess.codex_runtime_status = None
    sess.pending_stream_payload = None
    sess.pending_stream_payloads = None
    sess.pending_final_payload = None
    sess.pending_final_payloads = None
    sess.late_ack_payloads.clear()
    sess.pending_payload_retry_after = 0.0
    sess.active_run_media = []
    sess.active_run_media_notes = []
    clear_session_run_thread_checkpoint(sess)
    cleanup_session_reply_contexts(bot, sess)


async def quiesce_session_for_context_change(bot: BotState, key: str, action: str) -> SessionState:
    sess = await get_or_create_session_ready(bot, key)
    acquire_session_control_lease(bot, key, sess)
    active_task = sess.run_task
    active_proc = sess.proc
    if sess.running or (active_task is not None and not active_task.done()) or (
        active_proc is not None and active_proc.returncode is None
    ):
        sess.run_generation += 1
    await terminate_process(
        active_proc,
        timeout_sec=SESSION_CONTROL_EXIT_TIMEOUT_SEC,
        require_exit=True,
    )
    task_stopped = await cancel_task(active_task, timeout_sec=SESSION_CONTROL_EXIT_TIMEOUT_SEC)
    if not task_stopped:
        raise BridgeError(409, f"agent task did not exit; session was not {action}")
    if not session_lease_is_owned_by_current_instance(sess):
        abandon_session_after_lease_loss(bot, key, sess)
        raise BridgeError(409, f"session ownership changed while {action}: {key}")
    if sess.run_task is active_task:
        sess.run_task = None
    if sess.proc is active_proc:
        sess.proc = None
    sess.running = False
    sess.interrupt_requested = False
    sess.interrupt_queue_resume_deferred = False
    return sess


async def reset_session_context(bot: BotState, key: str) -> dict[str, Any]:
    async with get_session_lifecycle_lock(bot, key):
        sess = await quiesce_session_for_context_change(bot, key, "reset")
        result = control_session(bot, key, clear_thread=True, clear_chat=True)
        clear_resume_selection(sess)
        return result


async def new_session_context(bot: BotState, key: str) -> dict[str, Any]:
    async with get_session_lifecycle_lock(bot, key):
        previous = await quiesce_session_for_context_change(bot, key, "replaced")
        previous_session_id = previous.session_id
        previous_cwd = Path(previous.work_dir)

        interrupt_session(
            bot,
            key,
            previous,
            clear_thread=False,
            clear_chat=False,
            clear_queue=True,
        )
        clear_resume_selection(previous)
        record = create_fresh_session_record(
            bot,
            key,
            expected_session_id=previous_session_id,
            cwd_dir=previous_cwd,
        )
        current = SessionState(
            session_id=str(record["sessionId"]),
            work_dir=str(get_session_runtime_cwd(bot, key, session_id=record["sessionId"])),
            lock_file=Path(record["lockFile"]),
        )
        bot.sessions[key] = current
        add_log(bot, f"[{key}] switched to new session {current.session_id}; previous={previous_session_id}")
        add_event_log(
            bot,
            "session.new",
            chatKey=key,
            sessionId=current.session_id,
            previousSessionId=previous_session_id,
        )
        try:
            rebuild_schedule_definition_session_bindings(bot)
            rebuild_scheduled_job_session_bindings(bot)
        except Exception as exc:
            add_log(bot, f"[{key}] deferred schedule session rebinding after /new: {exc}")
        return {
            "key": key,
            "sessionId": current.session_id,
            "previousSessionId": previous_session_id,
            "threadId": None,
            "status": "idle",
            "clearedThread": False,
            "clearedChat": False,
            "live": True,
        }


async def clear_session_command(
    bot: BotState,
    key: str,
    req_id: Optional[str],
    confirmation: str,
) -> None:
    await reset_session_context(bot, key)
    sess = bot.sessions.get(key)
    if sess:
        clear_resume_selection(sess)
        register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, confirmation)


async def new_session_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    await new_session_context(bot, key)
    sess = bot.sessions.get(key)
    if sess:
        register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, "New session ready. Your next message will start a new thread.")


async def reset_session_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    await clear_session_command(
        bot,
        key,
        req_id,
        "Session reset. Your next message will start a new thread.",
    )


async def interrupt_session_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    _result, sess = await interrupt_session_and_wait(bot, key)
    if sess:
        clear_resume_selection(sess)
        register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, "Current task interrupted.")
    if sess:
        resume_session_queue(bot, sess, key, "interrupt command")


async def cwd_session_command(bot: BotState, key: str, req_id: Optional[str], raw_argument: Optional[str]) -> None:
    sess = get_or_create_session(bot, key)
    if bot_workspace_mode(bot.config) != "personal":
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, "仅 personal 模式支持 /cwd；team 模式不允许切换。")
        return
    current_cwd = get_session_runtime_cwd(bot, key, session_id=sess.session_id)
    argument = str(raw_argument or "").strip()
    if not argument:
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, f"Current cwd: {current_cwd}")
        return
    if not session_context_switch_is_quiescent(sess):
        register_reply_session(bot, req_id, sess)
        if sess.running:
            message = "当前有任务在运行，不能切换 cwd。请先等待完成或使用 /bridge-interrupt。"
        else:
            message = "当前仍有任务或待发送结果，不能切换 cwd。请先等待完成或使用 /bridge-interrupt。"
        await respond_info(bot, req_id, message)
        return
    acquire_session_context_switch_lease(bot, sess, key)
    requested_path = Path(argument).expanduser()
    resolved_argument = (current_cwd / requested_path).resolve() if not requested_path.is_absolute() else requested_path.resolve()
    next_cwd = resolve_personal_cwd_for_bot(bot, resolved_argument)
    changed = next_cwd != current_cwd
    if changed:
        sess.thread_id = None
        sess.use_global_codex_sessions = False
        clear_resume_selection(sess)
    update_owned_session_record(
        bot,
        key,
        sess,
        lambda record: {
            **record,
            "cwdDir": str(next_cwd),
            "updatedAt": now_ms(),
            "threadId": sess.thread_id,
            "useGlobalCodexSessions": sess.use_global_codex_sessions,
            "localResumeBackup": None if changed else record.get("localResumeBackup"),
        },
    )
    sess.work_dir = str(next_cwd)
    register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, f"cwd changed to {next_cwd}")


def count_scheduled_messages_for_key(bot_id: str, key: str) -> int:
    total = 0
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT):
        for pending_file in root.glob("*.json"):
            job = read_json_file(pending_file, None)
            if not isinstance(job, dict):
                continue
            if str(job.get("chatKey") or "") != key:
                continue
            job_bot_id = str(job.get("botId") or "").strip()
            if job_bot_id == bot_id:
                total += 1
                continue
            if not job_bot_id:
                schedule_id = str(job.get("scheduleId") or "").strip()
                definition = read_schedule_definition(schedule_id) if schedule_id else None
                if definition and str(definition.get("botId") or "").strip() == bot_id:
                    total += 1
    return total


RESUME_SELECTION_TTL_MS = 5 * 60 * 1000


def session_records_for_bot(bot_id: str) -> list[dict[str, Any]]:
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if not sessions_root.exists():
        return []
    records: list[dict[str, Any]] = []
    for session_file in sessions_root.glob("*.json"):
        raw = read_json_file(session_file, None)
        record = normalize_session_record(raw)
        if record is None:
            if raw_session_record_matches_bot(bot_id, raw, session_file=session_file):
                session_id = str((raw or {}).get("sessionId") or session_file.stem).strip()
                record = normalize_session_record(
                    {
                        **dict(raw or {}),
                        "sessionId": session_id,
                        "botId": bot_id,
                        "chatKey": str((raw or {}).get("chatKey") or chat_key_for_session_id_from_key_root(bot_id, session_id)).strip(),
                    }
                )
                if record is not None:
                    write_session_record(record)
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
        runtime_session = next(
            (sess for sess in bot.sessions.values() if sess.session_id == session_id and str(sess.thread_id or "").strip()),
            None,
        )
        thread_id = str(runtime_session.thread_id or "").strip() if runtime_session else ""
        if not thread_id:
            thread_id = str(record.get("threadId") or "").strip()
        candidate_key = str(record.get("chatKey") or "").strip()
        if not thread_id or not candidate_key or session_id in seen_session_ids:
            continue
        if normalize_internal_bool_flag(record.get("useGlobalCodexSessions"), default=False):
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


def resolve_bridge_resume_candidate(bot: BotState, key: str, session_id: str) -> dict[str, Any]:
    target_id = str(session_id or "").strip()
    record = read_session_record_by_id(target_id)
    if not record:
        raise BridgeError(404, f"session not found: {target_id}")
    if str(record.get("botId") or "").strip() != str(bot.config.get("id") or "").strip():
        raise BridgeError(403, "session is not resumable from current bot")
    candidate_key = str(record.get("chatKey") or "").strip()
    if not resume_record_is_visible_to_chat(candidate_key, key):
        raise BridgeError(403, "session is not resumable from current chat")
    if normalize_internal_bool_flag(record.get("useGlobalCodexSessions"), default=False):
        if not local_resume_binding_allowed_for_chat(bot.config, key):
            raise BridgeError(403, "selected local session binding is no longer authorized")
        raise BridgeError(409, "本地 Codex 会话不能通过 /bridge-resume 继承，请使用 /local-resume。")
    runtime_thread_id = next(
        (
            str(runtime_sess.thread_id or "").strip()
            for runtime_sess in bot.sessions.values()
            if runtime_sess.session_id == target_id and str(runtime_sess.thread_id or "").strip()
        ),
        "",
    )
    thread_id = runtime_thread_id or str(record.get("threadId") or "").strip()
    if not thread_id:
        raise BridgeError(404, "selected session is no longer resumable")
    return {
        "sessionId": record["sessionId"],
        "chatKey": candidate_key,
        "threadId": thread_id,
        "updatedAt": int(record.get("updatedAt") or record.get("createdAt") or 0),
        "lastRunAt": int(record.get("lastRunAt") or 0),
        "status": str(record.get("status") or "idle"),
    }


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


def format_local_resume_candidate_line(index: int, candidate: dict[str, Any]) -> str:
    ts = int(candidate.get("lastRunAt") or candidate.get("updatedAt") or 0)
    ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts / 1000)) if ts > 0 else "-"
    return f"{index}. {candidate['sessionId']}  {ts_text}  cwd={candidate['cwd']}"


def build_local_resume_candidates_message(candidates: list[dict[str, Any]]) -> str:
    lines = ["检测到以下当前目录下的本地会话，请回复编号或直接回复 /local-resume <sessionId>："]
    for index, candidate in enumerate(candidates, start=1):
        lines.append(format_local_resume_candidate_line(index, candidate))
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


def find_local_resume_thread_binding(
    thread_id: str,
    *,
    excluding_session_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    target_thread_id = str(thread_id or "").strip()
    excluded_session_id = str(excluding_session_id or "").strip()
    if not target_thread_id:
        return None
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if sessions_root.exists():
        for session_file in sessions_root.glob("*.json"):
            record = normalize_session_record(read_json_file(session_file, None))
            if not record or str(record.get("sessionId") or "").strip() == excluded_session_id:
                continue
            if not normalize_internal_bool_flag(record.get("useGlobalCodexSessions"), default=False):
                continue
            if str(record.get("threadId") or "").strip() != target_thread_id:
                continue
            record_bot_config = get_authoritative_bot_config(str(record.get("botId") or ""))
            if not record_bot_config or not local_resume_binding_allowed_for_chat(
                record_bot_config,
                str(record.get("chatKey") or ""),
            ):
                continue
            return record
    for runtime_bot in BOTS.values():
        for runtime_key, runtime_sess in runtime_bot.sessions.items():
            if runtime_sess.session_id == excluded_session_id or not runtime_sess.use_global_codex_sessions:
                continue
            if str(runtime_sess.thread_id or "").strip() != target_thread_id:
                continue
            if not local_resume_binding_allowed_for_chat(runtime_bot.config, runtime_key):
                continue
            return {
                "sessionId": runtime_sess.session_id,
                "botId": str(runtime_bot.config.get("id") or ""),
                "chatKey": runtime_key,
            }
    return None


def bind_resume_candidate_to_session(
    bot: BotState,
    sess: SessionState,
    key: str,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    ensure_session_context_switch_quiescent(sess)
    acquire_session_context_switch_lease(bot, sess, key)
    thread_id = str(candidate.get("threadId") or "").strip()
    if not thread_id:
        raise BridgeError(404, "selected session is no longer resumable")
    source = str(candidate.get("source") or "bridge").strip().lower() or "bridge"
    if source == "local":
        with local_resume_thread_binding_lock(thread_id):
            conflict = find_local_resume_thread_binding(thread_id, excluding_session_id=sess.session_id)
            if conflict is not None:
                raise BridgeError(409, "该本地 Codex 会话已绑定到其他企微会话，请先在原会话执行 /local-detach。")
            copied_recent_chat = [dict(item) for item in normalize_session_chat_items(candidate.get("recentChat"))]
            was_using_global_codex_sessions = sess.use_global_codex_sessions
            local_resume_backup = None
            if not was_using_global_codex_sessions:
                current_recent_chat = session_recent_chat_snapshot(sess)
                if sess.thread_id or current_recent_chat:
                    local_resume_backup = {
                        "threadId": sess.thread_id,
                        "recentChat": current_recent_chat,
                        "useGlobalCodexSessions": False,
                        "savedAt": now_ms(),
                    }
            sess.thread_id = thread_id
            sess.chat = copied_recent_chat
            sess.use_global_codex_sessions = True
            update_owned_session_record(
                bot,
                key,
                sess,
                lambda record: {
                    **record,
                    "lastRunAt": now_ms(),
                    "threadId": thread_id,
                    "useGlobalCodexSessions": True,
                    "localResumeBackup": (
                        normalize_local_resume_backup(record.get("localResumeBackup"))
                        if was_using_global_codex_sessions
                        else local_resume_backup
                    ),
                    "recentChat": copied_recent_chat,
                },
            )
        clear_resume_selection(sess)
        add_log(bot, f"[{key}] bound local codex session thread={thread_id} cwd={candidate.get('cwd')}")
        return {
            "sourceSessionId": thread_id,
            "sourceChatKey": None,
            "threadId": thread_id,
            "source": "local",
        }
    target_record = read_session_record_by_id(str(candidate["sessionId"]))
    if not target_record:
        raise BridgeError(404, f"session not found: {candidate['sessionId']}")
    if str(target_record.get("botId") or "").strip() != str(bot.config.get("id") or "").strip():
        raise BridgeError(403, "session is not resumable from current bot")
    copied_recent_chat = [dict(item) for item in normalize_session_chat_items(target_record.get("recentChat"))]
    use_global_codex_sessions = normalize_internal_bool_flag(target_record.get("useGlobalCodexSessions"), default=False)
    if use_global_codex_sessions:
        if not local_resume_binding_allowed_for_chat(bot.config, key):
            raise BridgeError(403, "selected local session binding is no longer authorized")
        raise BridgeError(409, "本地 Codex 会话不能通过 /bridge-resume 继承，请使用 /local-resume。")
    copy_bridge_session_resume_state(str(target_record["sessionId"]), sess.session_id)
    sess.thread_id = thread_id
    sess.chat = copied_recent_chat
    sess.use_global_codex_sessions = False
    update_owned_session_record(
        bot,
        key,
        sess,
        lambda record: {
            **record,
            "lastRunAt": now_ms(),
            "threadId": thread_id,
            "useGlobalCodexSessions": False,
            "localResumeBackup": None,
            "recentChat": copied_recent_chat,
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
        "threadId": thread_id,
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


def local_resume_owner_user_id_for_bot(config: dict[str, Any]) -> Optional[str]:
    return normalize_optional_user_id(config.get("localResumeOwnerUserId") or config.get("local_resume_owner_user_id"))


def local_resume_binding_allowed_for_chat(config: dict[str, Any], key: str) -> bool:
    if bot_workspace_mode(config) != "personal" or bot_agent_backend(config) != "codex" or key.startswith("group:"):
        return False
    owner_user_id = local_resume_owner_user_id_for_bot(config)
    return owner_user_id is not None and chat_key_to_user_id(key) == owner_user_id


async def hydrate_local_resume_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    source = str(candidate.get("source") or "").strip().lower()
    if source != "local" or candidate.get("recentChat") is not None:
        return candidate
    rollout_file = candidate.get("rolloutFile")
    if not isinstance(rollout_file, Path):
        return candidate
    recent_chat = await run_blocking(load_local_codex_recent_chat, rollout_file)
    return {**candidate, "recentChat": recent_chat}


def local_resume_candidate_revision(candidate: dict[str, Any]) -> tuple[str, str, str, int, int]:
    return (
        str(candidate.get("sessionId") or "").strip(),
        str(candidate.get("rolloutFile") or ""),
        str(candidate.get("cwd") or ""),
        int(candidate.get("rolloutMtimeNs") or 0),
        int(candidate.get("rolloutSize") if candidate.get("rolloutSize") is not None else -1),
    )


async def resolve_hydrated_local_resume_candidate(session_id: str, current_cwd: Path) -> dict[str, Any]:
    target_id = str(session_id or "").strip()
    for _ in range(3):
        candidate = await run_blocking(find_local_codex_resume_candidate, target_id, current_cwd)
        if not candidate:
            raise BridgeError(404, f"local session not found for current cwd: {target_id}")
        hydrated = await hydrate_local_resume_candidate(candidate)
        latest = await run_blocking(find_local_codex_resume_candidate, target_id, current_cwd)
        if not latest:
            raise BridgeError(404, f"local session not found for current cwd: {target_id}")
        if local_resume_candidate_revision(candidate) == local_resume_candidate_revision(latest):
            return hydrated
    raise BridgeError(409, f"local session changed while loading; retry: {target_id}")


def ensure_local_resume_allowed(bot: BotState, sess: SessionState, key: str, *, sender_user_id: Optional[str]) -> Path:
    if bot_workspace_mode(bot.config) != "personal":
        raise BridgeError(400, "仅 personal 模式支持本地会话命令。")
    if bot_agent_backend(bot.config) != "codex":
        raise BridgeError(400, "仅 codex backend 支持本地会话命令。")
    if key.startswith("group:"):
        raise BridgeError(403, "shared 群会话不支持本地会话命令。")
    owner_user_id = local_resume_owner_user_id_for_bot(bot.config)
    if not owner_user_id:
        raise BridgeError(403, "当前 bot 未启用本地会话命令；请先配置 localResumeOwnerUserId。")
    current_user_id = normalize_optional_user_id(sender_user_id)
    if current_user_id != owner_user_id:
        raise BridgeError(403, "当前用户无权使用本地会话命令。")
    ensure_session_context_switch_quiescent(sess)
    acquire_session_context_switch_lease(bot, sess, key)
    return Path(sess.work_dir).expanduser().resolve()


async def start_local_resume_selection_command(
    bot: BotState,
    key: str,
    req_id: Optional[str],
    *,
    sender_user_id: Optional[str],
) -> None:
    sess = get_or_create_session(bot, key)
    current_cwd = ensure_local_resume_allowed(bot, sess, key, sender_user_id=sender_user_id)
    candidates = await run_blocking(list_local_codex_resume_candidates, current_cwd)
    if not candidates:
        clear_resume_selection(sess)
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, "当前目录下没有可恢复的本机会话。")
        return
    sess.resume_candidates = candidates
    sess.resume_selection_expires_at = now_ms() + RESUME_SELECTION_TTL_MS
    register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, build_local_resume_candidates_message(candidates))


async def local_resume_session_command(
    bot: BotState,
    key: str,
    req_id: Optional[str],
    session_id: str,
    *,
    sender_user_id: Optional[str],
) -> None:
    sess = get_or_create_session(bot, key)
    current_cwd = ensure_local_resume_allowed(bot, sess, key, sender_user_id=sender_user_id)
    candidate = await resolve_hydrated_local_resume_candidate(session_id, current_cwd)
    ensure_local_resume_allowed(bot, sess, key, sender_user_id=sender_user_id)
    bound = bind_resume_candidate_to_session(bot, sess, key, candidate)
    register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, f"已切换到本地会话 {bound['sourceSessionId']}，接下来会继续该上下文。")


async def apply_resume_selection_command(
    bot: BotState,
    key: str,
    req_id: Optional[str],
    token: str,
    *,
    sender_user_id: Optional[str],
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
    if str(candidate.get("source") or "").strip().lower() == "local":
        current_cwd = ensure_local_resume_allowed(bot, sess, key, sender_user_id=sender_user_id)
        session_id = str(candidate.get("sessionId") or "").strip()
        try:
            candidate = await resolve_hydrated_local_resume_candidate(session_id, current_cwd)
        except BridgeError as exc:
            if exc.status_code == 404:
                clear_resume_selection(sess)
            raise
    else:
        candidate = resolve_bridge_resume_candidate(bot, key, str(candidate.get("sessionId") or ""))
    if str(candidate.get("source") or "").strip().lower() == "local":
        ensure_local_resume_allowed(bot, sess, key, sender_user_id=sender_user_id)
    bound = bind_resume_candidate_to_session(bot, sess, key, candidate)
    register_reply_session(bot, req_id, sess)
    if str(bound.get("source") or "").strip() == "local":
        await respond_info(bot, req_id, f"已切换到本地会话 {bound['sourceSessionId']}，接下来会继续该上下文。")
    else:
        await respond_info(bot, req_id, f"已选择会话 {bound['sourceSessionId']}，接下来会继续该上下文。")
    return True


async def resume_session_command(bot: BotState, key: str, req_id: Optional[str], session_id: str) -> None:
    sess = get_or_create_session(bot, key)
    candidate = resolve_bridge_resume_candidate(bot, key, session_id)
    bound = bind_resume_candidate_to_session(bot, sess, key, candidate)
    register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, f"已选择会话 {bound['sourceSessionId']}，接下来会继续该上下文。")


async def local_detach_session_command(
    bot: BotState,
    key: str,
    req_id: Optional[str],
    *,
    sender_user_id: Optional[str],
) -> None:
    sess = get_or_create_session(bot, key)
    try:
        ensure_local_resume_allowed(bot, sess, key, sender_user_id=sender_user_id)
    except BridgeError as exc:
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, exc.message)
        return
    record = read_session_record_by_id(sess.session_id) or {}
    backup = normalize_local_resume_backup(record.get("localResumeBackup"))
    if not sess.use_global_codex_sessions and backup is None:
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, "当前未绑定本地会话。")
        return
    if backup is not None:
        sess.thread_id = str(backup.get("threadId") or "").strip() or None
        sess.use_global_codex_sessions = False
        sess.chat = [dict(item) for item in normalize_session_chat_items(backup.get("recentChat"))]
    else:
        sess.thread_id = None
        sess.use_global_codex_sessions = False
        sess.chat = []
    clear_resume_selection(sess)
    update_owned_session_record(
        bot,
        key,
        sess,
        lambda record: {
            **record,
            "updatedAt": now_ms(),
            "threadId": sess.thread_id,
            "useGlobalCodexSessions": sess.use_global_codex_sessions,
            "localResumeBackup": None,
            "recentChat": session_recent_chat_snapshot(sess),
        },
    )
    register_reply_session(bot, req_id, sess)
    await respond_info(bot, req_id, "已解除本地会话绑定。")


async def status_session_command(bot: BotState, key: str, req_id: Optional[str]) -> None:
    sess = bot.sessions.get(key)
    record = read_session_record_by_key(bot.config["id"], key)
    scheduled_count = count_scheduled_messages_for_key(bot.config["id"], key)
    if sess:
        bridge_status_text = (
            f"status={('running' if sess.running else 'idle')}; "
            f"queue={len(sess.queue)}; "
            f"scheduled={scheduled_count}; "
            f"sessionId={sess.session_id}; "
            f"threadId={sess.thread_id or '-'}; "
            f"cwd={sess.work_dir}"
        )
        session_id = sess.session_id
        thread_id = sess.thread_id
        current_cwd = Path(sess.work_dir)
    elif record:
        thread_id = str(record.get("threadId") or "").strip() or None
        current_cwd = Path(record.get("cwdDir") or get_session_runtime_cwd(bot, key, session_id=record["sessionId"]))
        bridge_status_text = (
            f"status={record.get('status', 'idle')}; "
            f"queue=0; "
            f"scheduled={scheduled_count}; "
            f"sessionId={record['sessionId']}; "
            f"threadId={thread_id or '-'}; "
            f"cwd={current_cwd}"
        )
        session_id = str(record["sessionId"])
    else:
        bridge_status_text = f"status=empty; queue=0; scheduled={scheduled_count}; sessionId=-; threadId=-; cwd=-"
        session_id = None
        thread_id = None
        current_cwd = Path(str(bot.config.get("workDir") or DEFAULT_WORK_DIR)).expanduser().resolve()
    native_status = await run_blocking(
        collect_codex_status,
        default_codex_home=DEFAULT_CODEX_HOME,
        session_codex_home=(
            get_session_codex_home_root(session_id)
            if session_id
            else get_bridge_codex_home_root() / "sessions" / "unbound"
        ),
        directory=current_cwd,
        thread_id=thread_id,
        exec_mode=CODEX_EXEC_MODE,
        agent_backend=bot_agent_backend(bot.config),
    )
    status_text = f"{bridge_status_text}\n\n{format_codex_status(native_status)}"
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


def acquire_session_control_lease(bot: BotState, key: str, sess: SessionState) -> None:
    with session_key_lock(str(bot.config["id"]), key):
        record = read_session_record_by_id(sess.session_id)
        if record is None:
            raise BridgeError(404, f"session not found: {key}")
        ensure_session_control_safe(record)
        if not _acquire_session_lease_unlocked(bot, sess, key):
            raise BridgeError(409, f"session is owned by another instance: {key}")


def control_session(bot: BotState, key: str, clear_thread: bool, clear_chat: bool) -> dict[str, Any]:
    sess = bot.sessions.get(key)
    if sess:
        acquire_session_control_lease(bot, key, sess)
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
        record = find_session_record_by_bot_and_key(bot.config["id"], key)
        if record:
            write_json_atomic(get_registry_key_file(bot.config["id"], key), build_session_key_entry(record))
    if not record:
        raise BridgeError(404, f"session not found: {key}")
    with session_key_lock(str(bot.config["id"]), key):
        current_record = read_session_record_by_id(str(record["sessionId"]))
        if not current_record:
            raise BridgeError(404, f"session not found: {key}")
        ensure_session_control_safe(current_record)
        update_session_record(
            current_record["sessionId"],
            lambda current: {
                **current,
                "ownerInstance": None,
                "ownerPid": None,
                "leaseExpiresAt": None,
                "status": "idle",
                "activeScheduleId": None,
                "useGlobalCodexSessions": False if clear_thread else normalize_internal_bool_flag(current.get("useGlobalCodexSessions"), default=False),
                "localResumeBackup": None if clear_thread or clear_chat else current.get("localResumeBackup"),
                "lastRunAt": now_ms(),
                "threadId": None if clear_thread else current.get("threadId"),
                "recentChat": [] if clear_chat else current.get("recentChat"),
            },
        )
        lock_file = Path(current_record["lockFile"])
        try:
            lock_file.unlink()
        except FileNotFoundError:
            pass
    if clear_thread:
        remove_session_codex_home(record["sessionId"])
    return {
        "key": key,
        "sessionId": record["sessionId"],
        "threadId": None,
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


def get_allowed_file_roots(bot: BotState, key: str, *, session_id: Optional[str] = None) -> list[Path]:
    if session_id:
        workspace_paths = ensure_session_workspace_dirs_light_for_session(bot, key, session_id)
    else:
        workspace_paths = ensure_session_workspace_dirs_light(bot, key)
    roots = [workspace_paths["chatfile"].resolve(), *EXTRA_FILE_ROOTS]
    deduped = []
    for root in roots:
        if root not in deduped:
            deduped.append(root)
    return deduped


def validate_file_for_upload(bot: BotState, key: str, file_path: str, *, session_id: Optional[str] = None, media_type: str = "file") -> Path:
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.exists():
        raise BridgeError(404, f"file not found: {resolved}")
    if not resolved.is_file():
        raise BridgeError(400, f"not a regular file: {resolved}")
    allowed_roots = get_allowed_file_roots(bot, key, session_id=session_id)
    if not any(is_path_inside(resolved, root) for root in allowed_roots):
        allowed = ", ".join(str(root) for root in allowed_roots)
        raise BridgeError(403, f"filePath is outside allowed roots: {allowed}")
    try:
        validate_media_upload(resolved.name, resolved.stat().st_size, media_type, max_bytes=MAX_UPLOAD_SIZE)
    except ValueError as exc:
        raise BridgeError(413 if str(exc).startswith("file too large:") else 400, str(exc)) from exc
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
    media_type = str(data.get("mediaType") or data.get("media_type") or "file").strip()
    chat_key_value = data.get("chatKey") or data.get("chat_key")
    bot_name = normalize_optional_string(data.get("botName") or data.get("bot_name"))
    target_config_id = normalize_optional_string(data.get("targetConfigId") or data.get("target_config_id"))
    session_id = normalize_optional_string(data.get("sessionId") or data.get("session_id"))
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
            existing = read_session_record_by_key(str(bot.config["id"]), validated_chat_key)
            resolved_file = validate_file_for_upload(
                bot,
                validated_chat_key,
                file_path,
                media_type=media_type,
                session_id=str((existing or {}).get("sessionId") or ""),
            )
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
            resolved_file = validate_file_for_upload(
                bot,
                resolved_chat_key,
                file_path,
                media_type=media_type,
                session_id=str(record["sessionId"]),
            )
            return bot, resolved_chat_key, resolved_file
        except BridgeError as exc:
            if last_error is None:
                raise
            raise exc

    if last_error is not None:
        raise last_error

    raise BridgeError(400, "chatKey or sessionId required")


def build_proactive_template_card_payload(key: str, template_card: dict[str, Any]) -> dict[str, Any]:
    chat_type, chat_id = chat_key_to_send_target(key)
    card = enrich_template_card_for_delivery(key, template_card, unique_token=uid())
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": attach_message_feedback(
            {
                "chatid": chat_id,
                "chat_type": chat_type,
                "msgtype": "template_card",
                "template_card": card,
            },
            None,
        ),
    }


def build_response_url_message_body(chat_key: str, payload_data: dict[str, Any]) -> dict[str, Any]:
    msgtype = str(payload_data.get("msgtype") or "").strip()
    if msgtype == "markdown":
        resolved_mention_user_id = proactive_chat_mention_user_id(chat_key, payload_data.get("mentionUserId"))
        markdown = {
            "content": limit_proactive_text(
                prepend_group_user_mention(str(payload_data.get("content") or ""), resolved_mention_user_id)
            )
        }
        feedback_id = str(payload_data.get("feedbackId") or "").strip()
        if feedback_id:
            markdown["feedback"] = {"id": validate_feedback_id(feedback_id)}
        return {"msgtype": "markdown", "markdown": markdown}
    if msgtype == "template_card":
        card = enrich_template_card_for_delivery(chat_key, dict(payload_data["templateCard"] or {}), unique_token=uid())
        feedback_id = str(payload_data.get("feedbackId") or "").strip()
        if feedback_id:
            feedback = dict(card.get("feedback") or {})
            feedback["id"] = validate_feedback_id(feedback_id)
            card["feedback"] = feedback
        return {"msgtype": "template_card", "template_card": card}
    raise BridgeError(400, f"unsupported msgtype for response_url send: {msgtype}")


async def send_reply_url_message(bot: BotState, req_id: str, payload_data: dict[str, Any]) -> dict[str, Any]:
    state = get_reply_url_state(bot, req_id)
    if not state:
        raise BridgeError(404, f"reply response_url not found or expired: {req_id}")
    if HTTP_SESSION is None:
        raise BridgeError(500, "http session not ready")
    response_url = str(state["responseUrl"])
    chat_key = str(state.get("chatKey") or "").strip()
    body = build_response_url_message_body(chat_key, payload_data)
    timeout = aiohttp.ClientTimeout(total=15, connect=5)
    async with HTTP_SESSION.post(response_url, json=body, timeout=timeout) as response:
        if response.status != 200:
            raise BridgeError(502, f"response_url send failed: HTTP {response.status}")
        result = await response.json(content_type=None)
    raw_errcode = result.get("errcode", 0)
    try:
        errcode = int(raw_errcode if raw_errcode not in (None, "") else 0)
    except (TypeError, ValueError) as exc:
        raise BridgeError(502, "response_url send failed: invalid errcode") from exc
    if errcode != 0:
        raise BridgeError(502, f"response_url send failed: {errcode} {result.get('errmsg', '')}".strip())
    current_state = bot.reply_urls.get(req_id)
    if isinstance(current_state, dict):
        current_state["consumed"] = True
        write_reply_url_state(bot)
    if str(payload_data.get("msgtype") or "").strip() == "template_card":
        delivered_card = dict(body.get("template_card") or {})
        task_id = str(delivered_card.get("task_id") or "").strip()
        if task_id:
            save_template_card_delivery_meta(bot, task_id, chat_key=chat_key, template_card=delivered_card)
        return {"ok": True, "chatKey": chat_key, "msgtype": "template_card", "cardType": str(delivered_card.get("card_type") or ""), "taskId": task_id or None}
    return {"ok": True, "chatKey": chat_key, "msgtype": "markdown"}


def build_template_card_update_payload(req_id: str, template_card: dict[str, Any]) -> dict[str, Any]:
    req_id = require_non_empty_string(req_id, "reqId")
    template_card = validate_template_card_update_payload(template_card)
    return {
        "cmd": "aibot_respond_update_msg",
        "headers": {"req_id": req_id},
        "body": {
            "response_type": "update_template_card",
            "template_card": dict(template_card or {}),
        },
    }


def template_card_event_text(event: dict[str, Any], fallback: str) -> str:
    event_key = str(event.get("event_key") or "").strip()
    button_list = event.get("button_list")
    if isinstance(button_list, list):
        for item in button_list:
            if not isinstance(item, dict):
                continue
            if str(item.get("key") or "").strip() != event_key:
                continue
            text = str(item.get("text") or "").strip()
            if text:
                return text
    return fallback


def progress_percent_from_event_key(event_key: str) -> Optional[int]:
    match = PROGRESS_EVENT_KEY_RE.fullmatch(str(event_key or "").strip())
    if not match:
        return None
    return max(0, min(100, int(match.group(1))))


def apply_progress_template_card_update(update_card: dict[str, Any], percent: int, event_text: str) -> None:
    title = f"进度 {percent}/100"
    status = "done" if percent >= 100 else "running"
    main_title = dict(update_card.get("main_title") or {})
    main_title["title"] = title
    main_title["desc"] = f"已更新到 {percent}%"
    update_card["main_title"] = main_title
    update_card["sub_title_text"] = f"最近一次操作：{event_text}。当前状态：{status}。"

    rebuilt_contents = []
    matched_progress = False
    matched_status = False
    for item in update_card.get("horizontal_content_list") or []:
        if not isinstance(item, dict):
            continue
        current = dict(item)
        keyname = str(current.get("keyname") or "").strip()
        if keyname in {"当前进度", "进度", "Current Progress"}:
            current["value"] = f"{percent}%"
            matched_progress = True
        elif keyname in {"状态", "Status"}:
            current["value"] = status
            matched_status = True
        rebuilt_contents.append(current)
    if not matched_progress:
        rebuilt_contents.insert(0, {"keyname": "当前进度", "value": f"{percent}%"})
    if not matched_status:
        rebuilt_contents.append({"keyname": "状态", "value": status})
    update_card["horizontal_content_list"] = rebuilt_contents


def is_repeatable_template_card_event_key(event_key: str) -> bool:
    return str(event_key or "").strip() in REPEATABLE_TEMPLATE_CARD_EVENT_KEYS


def extract_progress_percent_from_card(template_card: dict[str, Any]) -> int:
    candidates = (
        str(((template_card.get("main_title") or {}).get("desc")) or "").strip(),
        str(template_card.get("sub_title_text") or "").strip(),
    )
    for text in candidates:
        match = re.search(r"(\d{1,3})%", text)
        if not match:
            continue
        try:
            return max(0, min(int(match.group(1)), 100))
        except ValueError:
            continue
    return 0


def render_template_card_progress_bar(progress: int) -> str:
    clamped = max(0, min(int(progress), 100))
    filled = clamped // 10
    return "[" + ("#" * filled) + ("." * (10 - filled)) + "]"


def build_refresh_progress_template_card(original_card: dict[str, Any], task_id: str, *, step: int = 10) -> dict[str, Any]:
    current_progress = extract_progress_percent_from_card(original_card)
    next_progress = min(100, current_progress + max(1, int(step)))
    status_text = "已完成" if next_progress >= 100 else "进行中"
    now_text = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    update_card = dict(original_card or {})
    update_card["card_type"] = "button_interaction"
    update_card["task_id"] = task_id or str(update_card.get("task_id") or "").strip() or f"progress-{uid()}"

    main_title = dict(update_card.get("main_title") or {})
    main_title["title"] = str(main_title.get("title") or "").strip() or "进度刷新卡片演示"
    main_title["desc"] = f"当前进度: {next_progress}%"
    update_card["main_title"] = main_title
    update_card["sub_title_text"] = (
        f"进度条: {render_template_card_progress_bar(next_progress)} {next_progress}%\n"
        f"状态: {status_text}\n"
        f"最近更新时间: {now_text}"
    )

    rebuilt_buttons = []
    for item in original_card.get("button_list") or []:
        if not isinstance(item, dict):
            continue
        current = dict(item)
        key = str(current.get("key") or "").strip()
        if key == "refresh_progress":
            current["text"] = "刷新进度"
            current["style"] = 1
        elif key == "view_status":
            current["text"] = "查看状态"
            current["style"] = 0
        rebuilt_buttons.append(current)
    if not rebuilt_buttons:
        rebuilt_buttons = [
            {"text": "刷新进度", "style": 1, "key": "refresh_progress"},
            {"text": "查看状态", "style": 0, "key": "view_status"},
        ]
    update_card["button_list"] = rebuilt_buttons
    return update_card


def register_template_card_button_texts(bot: BotState, template_card: dict[str, Any]) -> None:
    if not isinstance(template_card, dict):
        return
    task_id = str(template_card.get("task_id") or "").strip()
    if not task_id:
        return
    bot.template_card_payloads[task_id] = dict(template_card)
    button_texts: dict[str, str] = {}
    for item in template_card.get("button_list") or []:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        text = str(item.get("text") or "").strip()
        if key and text:
            button_texts[key] = text
    if button_texts:
        bot.template_card_button_texts[task_id] = button_texts
    else:
        bot.template_card_button_texts.pop(task_id, None)


def save_template_card_delivery_meta(
    bot: BotState,
    task_id: str,
    *,
    chat_key: str,
    template_card: dict[str, Any],
    sent_at_ms: Optional[int] = None,
) -> None:
    task_text = str(task_id or "").strip()
    if not task_text:
        return
    current = dict(bot.template_card_delivery_meta.get(task_text) or {})
    current.update(
        {
            "taskId": task_text,
            "chatKey": chat_key,
            "templateCard": dict(template_card or {}),
            "sentAtMs": int(sent_at_ms if sent_at_ms is not None else now_ms()),
        }
    )
    bot.template_card_delivery_meta[task_text] = current
    register_template_card_button_texts(bot, {**dict(template_card or {}), "task_id": task_text})
    write_template_card_state(bot)



def resolve_message_send_request(data: dict[str, Any]) -> tuple[BotState, str, dict[str, Any], Optional[str]]:
    chat_key_value = data.get("chatKey") or data.get("chat_key")
    bot_name = normalize_optional_string(data.get("botName") or data.get("bot_name"))
    target_config_id = normalize_optional_string(data.get("targetConfigId") or data.get("target_config_id"))
    session_id = normalize_optional_string(data.get("sessionId") or data.get("session_id"))
    reply_req_id = normalize_optional_string(data.get("replyReqId") or data.get("reply_req_id"))
    msgtype = str(data.get("msgtype") or "").strip()
    if not msgtype:
        raise BridgeError(400, "msgtype required")
    last_error: Optional[BridgeError] = None

    if msgtype == "template_card":
        try:
            template_card = validate_template_card_payload(data.get("templateCard"), require_interaction_task_id=False)
        except ValueError as exc:
            raise BridgeError(400, str(exc)) from exc
        feedback_id = normalize_optional_string(data.get("feedbackId") or data.get("feedback_id"))
        if feedback_id is not None:
            try:
                feedback_id = validate_feedback_id(feedback_id)
            except ValueError as exc:
                raise BridgeError(400, str(exc)) from exc
        payload_data = {"msgtype": "template_card", "templateCard": template_card, "feedbackId": feedback_id}
    elif msgtype == "markdown":
        content = str(data.get("content") or "").strip()
        if not content:
            raise BridgeError(400, "content required")
        feedback_id = normalize_optional_string(data.get("feedbackId") or data.get("feedback_id"))
        if feedback_id is not None:
            try:
                feedback_id = validate_feedback_id(feedback_id)
            except ValueError as exc:
                raise BridgeError(400, str(exc)) from exc
        payload_data = {
            "msgtype": "markdown",
            "content": content,
            "mentionUserId": normalize_optional_string(data.get("mentionUserId") or data.get("mention_user_id")),
            "feedbackId": feedback_id,
        }
    elif msgtype in {"file", "image", "voice", "video"}:
        media_id = data.get("mediaId") or data.get("media_id")
        if not isinstance(media_id, str) or not media_id.strip():
            raise BridgeError(400, "mediaId required")
        payload_data = {"msgtype": msgtype, "mediaId": media_id, "title": str(data.get("title") or ""),
                        "description": str(data.get("description") or "")}
    else:
        raise BridgeError(400, f"unsupported msgtype: {msgtype}")
    try:
        assistant = normalize_assistant_reply(data.get("toAiAssistant") or data.get("to_ai_assistant"), proactive=True)
    except ValueError as exc:
        raise BridgeError(400, str(exc)) from exc
    if assistant:
        if msgtype not in {"markdown", "file", "image", "video"}:
            raise BridgeError(400, "assistant replies do not support this message type")
        payload_data["toAiAssistant"] = assistant

    if reply_req_id:
        matching_bots = []
        if target_config_id:
            bot = resolve_loaded_target_bot(target_config_id, bot_name)
            if get_reply_url_state(bot, reply_req_id):
                matching_bots.append(bot)
        else:
            for candidate in BOTS.values():
                if bot_instance_is_stale(candidate):
                    continue
                if get_reply_url_state(candidate, reply_req_id):
                    if bot_name and candidate.config["name"] != bot_name:
                        continue
                    matching_bots.append(candidate)
            if len(matching_bots) > 1:
                raise BridgeError(409, f"replyReqId matches multiple bots; provide targetConfigId: {reply_req_id}")
        if not matching_bots:
            raise BridgeError(404, f"reply response_url not found or expired: {reply_req_id}")
        bot = matching_bots[0]
        state = get_reply_url_state(bot, reply_req_id)
        if not state:
            raise BridgeError(404, f"reply response_url not found or expired: {reply_req_id}")
        return bot, str(state.get("chatKey") or ""), payload_data, reply_req_id

    if chat_key_value:
        try:
            validated_chat_key = validate_chat_key(chat_key_value)
            if target_config_id:
                bot = resolve_loaded_target_bot(target_config_id, bot_name)
            else:
                bot = require_unique_bot_for_chat_key(validated_chat_key, bot_name)
            if not bot.ws or bot.ws.closed:
                raise BridgeError(503, "bot not connected")
            return bot, validated_chat_key, payload_data, reply_req_id
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
                raise BridgeError(409, f"bot config mismatch for message send target: {target_config_id}")
            resolved_target_config_id = target_config_id or str(record["botId"])
            bot = resolve_loaded_target_bot(resolved_target_config_id, bot_name)
            resolved_chat_key = str(record["chatKey"])
            if not bot.ws or bot.ws.closed:
                raise BridgeError(503, "bot not connected")
            return bot, resolved_chat_key, payload_data, reply_req_id
        except BridgeError as exc:
            if last_error is None:
                raise
            raise exc

    if last_error is not None:
        raise last_error

    raise BridgeError(400, "chatKey or sessionId or replyReqId required")


async def submit_send_message_request(data: dict[str, Any], source: str = "api") -> dict[str, Any]:
    bot, resolved_chat_key, payload_data, reply_req_id = resolve_message_send_request(data)
    if reply_req_id:
        return await send_reply_url_message(bot, reply_req_id, payload_data)
    sent_template_card: Optional[dict[str, Any]] = None
    if payload_data["msgtype"] == "template_card":
        payload = build_proactive_template_card_payload(resolved_chat_key, payload_data["templateCard"])
        payload["body"] = attach_message_feedback(payload["body"], payload_data.get("feedbackId"))
        sent_template_card = dict(((payload.get("body") or {}).get("template_card") or {}))
        register_template_card_button_texts(bot, sent_template_card)
        payloads = [payload]
    elif payload_data["msgtype"] == "markdown":
        payloads = build_proactive_chat_payloads(resolved_chat_key, payload_data["content"], payload_data.get("mentionUserId"))
        for payload in payloads:
            payload["body"] = attach_message_feedback(payload["body"], payload_data.get("feedbackId"))
    else:
        chat_type, chat_id = chat_key_to_send_target(resolved_chat_key)
        body = {"chatid": chat_id, "chat_type": chat_type,
                **native_media_body(payload_data["msgtype"], payload_data["mediaId"], title=payload_data["title"], description=payload_data["description"])}
        payloads = [{"cmd": "aibot_send_msg", "headers": {"req_id": uid()}, "body": body}]
    assistant = payload_data.get("toAiAssistant")
    for index, payload in enumerate(payloads):
        if assistant:
            payload["body"]["to_ai_assistant"] = {**assistant, "reply_status": "waiting" if index < len(payloads) - 1 else assistant["reply_status"]}
        response = await send_ws_payload_with_ack(bot, payload, PROACTIVE_SEND_ACK_TIMEOUT_SEC)
        require_ack_success(response, "message send")
    if sent_template_card is not None:
        task_id = str(sent_template_card.get("task_id") or "").strip()
        if task_id:
            save_template_card_delivery_meta(
                bot,
                task_id,
                chat_key=resolved_chat_key,
                template_card=sent_template_card,
            )
    add_log(bot, f"[{resolved_chat_key}] message request accepted via {source}: {payload_data['msgtype']}")
    add_event_log(
        bot,
        "message.send_accepted",
        chatKey=resolved_chat_key,
        source=source,
        msgtype=payload_data["msgtype"],
    )
    result = {"ok": True, "chatKey": resolved_chat_key, "msgtype": payload_data["msgtype"]}
    if payload_data["msgtype"] == "template_card":
        result["cardType"] = str(payload_data["templateCard"].get("card_type") or "")
        if sent_template_card is not None:
            task_id = str(sent_template_card.get("task_id") or "").strip()
            if task_id:
                result["taskId"] = task_id
    return result


def submit_local_send_message_request(data: dict[str, Any], source: str = "local-command") -> dict[str, Any]:
    bot, resolved_chat_key, payload_data, reply_req_id = resolve_message_send_request(data)
    queue_paths = get_local_send_message_queue_paths(bot.config["id"])
    ensure_local_send_message_dirs([bot.config["id"]])
    request_id = str(data.get("requestId") or data.get("request_id") or "").strip() or uid()
    requested_at_value = data.get("requestedAt") if "requestedAt" in data else data.get("requested_at")
    timeout_ms_value = data.get("timeoutMs") if "timeoutMs" in data else data.get("timeout_ms")
    expires_at_value = data.get("expiresAt") if "expiresAt" in data else data.get("expires_at")
    requested_at = normalize_optional_int(requested_at_value) or now_ms()
    timeout_ms = normalize_optional_int(timeout_ms_value)
    if timeout_ms is None:
        timeout_ms = LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS
    expires_at = normalize_optional_int(expires_at_value)
    if expires_at is None:
        expires_at = requested_at + timeout_ms
    request = {
        "requestId": request_id,
        "chatKey": resolved_chat_key,
        "botName": bot.config["name"],
        "targetConfigId": bot.config["id"],
        "replyReqId": reply_req_id,
        "msgtype": payload_data["msgtype"],
        "content": payload_data.get("content"),
        "mentionUserId": payload_data.get("mentionUserId"),
        "feedbackId": payload_data.get("feedbackId"),
        "templateCard": payload_data.get("templateCard"),
        "requestedAt": requested_at,
        "timeoutMs": timeout_ms,
        "expiresAt": expires_at,
        "source": source,
    }
    for name in ("mediaId", "title", "description", "toAiAssistant"):
        if name in payload_data:
            request[name] = payload_data[name]
    pending_file = local_queue_request_file_path(queue_paths["pending"], request_id)
    write_json_atomic(pending_file, request)
    add_log(bot, f"[{resolved_chat_key}] local message request accepted via {source}: {payload_data['msgtype']}")
    return {"ok": True, "requestId": request_id, "chatKey": resolved_chat_key, "msgtype": payload_data["msgtype"]}


def submit_file_send_request(data: dict[str, Any], source: str = "api") -> dict[str, Any]:
    bot, resolved_chat_key, resolved_file = resolve_file_send_request(data)
    try:
        assistant = normalize_assistant_reply(data.get("toAiAssistant") or data.get("to_ai_assistant"), proactive=True)
    except ValueError as exc:
        raise BridgeError(400, str(exc)) from exc
    media_type = str(data.get("mediaType") or data.get("media_type") or "file").strip()
    if assistant and media_type == "voice":
        raise BridgeError(400, "assistant replies do not support voice")
    local_request_id = str(data.get("localRequestId") or data.get("local_request_id") or "").strip() or None
    local_processing_file = str(data.get("localProcessingFile") or data.get("local_processing_file") or "").strip() or None
    target_config_id = normalize_optional_string(data.get("targetConfigId") or data.get("target_config_id"))
    if local_request_id:
        bot.active_local_file_request_ids.add(local_request_id)
    bot.upload_queue.put_nowait(
        {
            "id": uid(),
            "chatKey": resolved_chat_key,
            "filePath": str(resolved_file),
            "mediaType": media_type,
            "title": str(data.get("title") or ""),
            "description": str(data.get("description") or ""),
            "toAiAssistant": assistant,
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
        local_queue_request_file_path(paths["results"], request_id),
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
    using_delay_seconds = delay_seconds not in (None, "")
    if using_delay_seconds:
        try:
            seconds = float(delay_seconds)
        except (TypeError, ValueError) as exc:
            raise BridgeError(400, "invalid delaySeconds") from exc
        if seconds < 0:
            raise BridgeError(400, "delaySeconds must be >= 0")
        run_at_ms = now_ms() + int(seconds * 1000)
    else:
        run_at_ms = parse_timestamp_ms(run_at, "runAt")
    if run_at_ms < now_ms() or (not using_delay_seconds and run_at_ms <= now_ms()):
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
        "botConfigId": str(record.get("botConfigId") or record["botId"]),
        "botName": str(record.get("botName") or "").strip() or None,
        "sessionId": str(record.get("sessionId") or "").strip() or None,
        "chatKey": str(record["chatKey"]),
        "replyReqId": str(record.get("replyReqId") or "").strip() or None,
        "message": str(record["message"]),
        "msgtype": str(record.get("msgtype") or "markdown").strip() or "markdown",
        "content": str(record.get("content") or "").strip() or None,
        "templateCard": validate_template_card_payload(record.get("templateCard"), require_interaction_task_id=False)
        if (record.get("msgtype") or "markdown").strip() == "template_card"
        else None,
        "mode": mode,
        "cron": str(record.get("cron") or "").strip() or None,
        "timezone": str(record.get("timezone") or "UTC").strip() or "UTC",
        "startAt": as_int(record.get("startAt")),
        "endAt": as_int(record.get("endAt")),
        "maxRuns": as_int(record.get("maxRuns")),
        "runCount": int(record.get("runCount") or 0),
        "enabled": normalize_enabled_flag(record.get("enabled", True), status_code=500),
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
    session_id = normalize_optional_string(data.get("sessionId") or data.get("session_id")) or ""
    chat_key_value = str(data.get("chatKey") or data.get("chat_key") or "").strip()
    reply_req_id = normalize_optional_string(data.get("replyReqId") or data.get("reply_req_id")) or ""
    bot_name = normalize_optional_string(data.get("botName") or data.get("bot_name"))
    target_config_id = normalize_optional_string(data.get("targetConfigId") or data.get("target_config_id"))
    last_error: Optional[BridgeError] = None

    if reply_req_id:
        matching_bots = []
        if target_config_id:
            bot = resolve_loaded_target_bot(target_config_id, bot_name)
            if get_reply_url_state(bot, reply_req_id):
                matching_bots.append(bot)
        else:
            for candidate in BOTS.values():
                if bot_instance_is_stale(candidate):
                    continue
                if get_reply_url_state(candidate, reply_req_id):
                    if bot_name and candidate.config["name"] != bot_name:
                        continue
                    matching_bots.append(candidate)
            if len(matching_bots) > 1:
                raise BridgeError(409, f"replyReqId matches multiple bots; provide targetConfigId: {reply_req_id}")
        if not matching_bots:
            raise BridgeError(404, f"reply response_url not found or expired: {reply_req_id}")
        bot = matching_bots[0]
        state = get_reply_url_state(bot, reply_req_id)
        if not state:
            raise BridgeError(404, f"reply response_url not found or expired: {reply_req_id}")
        return {
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "sessionId": None,
            "chatKey": str(state.get("chatKey") or ""),
            "replyReqId": reply_req_id,
        }

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

    raise BridgeError(400, "sessionId or chatKey or replyReqId required")


def build_scheduled_message_job(definition: dict[str, Any], run_at_ms: int) -> dict[str, Any]:
    return {
        "requestId": uid(),
        "scheduleId": definition["scheduleId"],
        "botId": definition["botId"],
        "botName": definition.get("botName"),
        "sessionId": definition.get("sessionId"),
        "chatKey": definition["chatKey"],
        "message": definition["message"],
        "replyReqId": definition.get("replyReqId"),
        "msgtype": definition.get("msgtype") or "markdown",
        "content": definition.get("content"),
        "templateCard": definition.get("templateCard"),
        "runAt": run_at_ms,
        "createdAt": now_ms(),
    }


def create_schedule_definition_record(data: dict[str, Any], created_at_ms: Optional[int] = None) -> dict[str, Any]:
    msgtype = str(data.get("msgtype") or "markdown").strip() or "markdown"
    message = str(data.get("message") or data.get("text") or "").strip()
    content = str(data.get("content") or "").strip() or None
    template_card = data.get("templateCard") or data.get("template_card")
    if msgtype == "markdown":
        if not message:
            raise BridgeError(400, "message required")
        content = content or message
    elif msgtype == "template_card":
        try:
            template_card = validate_template_card_payload(template_card, require_interaction_task_id=False)
        except ValueError as exc:
            raise BridgeError(400, str(exc)) from exc
        if not message:
            message = "[template_card scheduled follow-up]"
    else:
        raise BridgeError(400, f"unsupported msgtype: {msgtype}")

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

    timezone_name = normalize_optional_string(data.get("timezone")) or "UTC"
    start_at = parse_optional_timestamp_ms(data.get("startAt") or data.get("start_at"), "startAt")
    end_at = parse_optional_timestamp_ms(data.get("endAt") or data.get("end_at"), "endAt")
    if end_at is not None and end_at <= created_at:
        raise BridgeError(400, "endAt must be in the future")

    max_runs_raw = data.get("maxRuns") if "maxRuns" in data else data.get("max_runs")
    max_runs = None
    if max_runs_raw not in (None, ""):
        try:
            max_runs = int(max_runs_raw)
        except (TypeError, ValueError) as exc:
            raise BridgeError(400, "invalid maxRuns") from exc
        if max_runs <= 0:
            raise BridgeError(400, "maxRuns must be > 0")

    misfire_policy = validate_schedule_misfire_policy(
        normalize_optional_string(data.get("misfirePolicy") or data.get("misfire_policy")) or "fire_once_now"
    )
    concurrency_policy = validate_schedule_concurrency_policy(
        normalize_optional_string(data.get("concurrencyPolicy") or data.get("concurrency_policy")) or "skip_if_running"
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
            "scheduleId": normalize_optional_string(data.get("scheduleId") or data.get("schedule_id")) or uid(),
            "botId": target["botId"],
            "botConfigId": target["botId"],
            "botName": target.get("botName"),
            "sessionId": target.get("sessionId"),
            "chatKey": target["chatKey"],
            "replyReqId": target.get("replyReqId"),
            "message": message,
            "msgtype": msgtype,
            "content": content,
            "templateCard": template_card,
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
    run_at_value = data.get("runAt") if "runAt" in data else data.get("run_at")
    delay_seconds_value = data.get("delaySeconds") if "delaySeconds" in data else data.get("delay_seconds")
    requested_run_at = parse_schedule_run_at(run_at_value, delay_seconds_value)
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
        "botConfigId": definition["botId"],
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
        "botId": stored["botId"],
        "botConfigId": stored["botId"],
        "botName": stored.get("botName"),
        "sessionId": stored.get("sessionId"),
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
    return {"ok": True, **stored, "botConfigId": stored["botId"]}


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
                request_id = str(request.get("requestId") or local_queue_request_id_from_job_file(request_file)).strip() or local_queue_request_id_from_job_file(request_file)
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
            run_at = normalize_optional_int(job.get("runAt"))
            if run_at is None or run_at <= now:
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
    handle = _open_flock_lock_file(lock_file)
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
        run_at = normalize_optional_int(job.get("runAt"))
        if run_at is None:
            finalize_scheduled_message_job(pending_file, False)
            continue
        if pending_file.parent == SCHEDULE_PENDING_ROOT and run_at > now:
            continue
        job_bot_id = str(job.get("botId") or "").strip()
        if not job_bot_id:
            definition = read_schedule_definition(str(job.get("scheduleId") or "").strip())
            if definition:
                job_bot_id = str(definition.get("botId") or "").strip()
        bot = BOTS.get(job_bot_id)
        if not bot or not bot.config.get("enabled", True):
            if run_at and now - run_at > SCHEDULE_ORPHAN_TTL_MS:
                finalize_scheduled_message_job(pending_file, False)
            continue
        if bot.status != "running" or not bot.ws or bot.ws.closed:
            continue
        chat_key_value = str(job.get("chatKey") or "").strip()
        reply_req_id = str(job.get("replyReqId") or "").strip()
        message = str(job.get("message") or "").strip()
        msgtype = str(job.get("msgtype") or "markdown").strip() or "markdown"
        if (not chat_key_value and not reply_req_id) or (msgtype == "markdown" and not message):
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
        enqueued_at = normalize_optional_int(job.get("enqueuedAt")) or 0
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
        if msgtype == "template_card":
            try:
                reply_req_id = reply_req_id or None
                result = await submit_send_message_request(
                    {
                        "replyReqId": reply_req_id,
                        "chatKey": None if reply_req_id else chat_key_value,
                        "targetConfigId": job_bot_id or None,
                        "botName": str(bot.config.get("name") or "").strip() or None,
                        "msgtype": "template_card",
                        "templateCard": job.get("templateCard"),
                    },
                    "scheduled-message",
                )
                accepted = bool(result.get("ok"))
            except BridgeError as exc:
                if 400 <= exc.status_code < 500:
                    add_log(bot, f"[{chat_key_value}] scheduled template card failed permanently requestId={job.get('requestId')}: {exc.message}")
                    finalize_scheduled_message_job(processing_file, False)
                    continue
                accepted = False
            if accepted:
                finalize_scheduled_message_job(processing_file, True)
                maybe_cleanup_schedule_definition(str(job.get("scheduleId") or "").strip() or None)
                add_log(bot, f"[{chat_key_value}] scheduled message dispatched requestId={job.get('requestId')}")
                add_event_log(
                    bot,
                    "schedule.dispatched",
                    chatKey=chat_key_value,
                    sessionId=job.get("sessionId"),
                    scheduleId=job.get("scheduleId"),
                    requestId=job.get("requestId"),
                )
                continue
        else:
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
                        local_queue_request_id_from_job_file(processing_file),
                        {"ok": False, "statusCode": 400, "error": "invalid local file-send request"},
                        queue_paths,
                    )
                    finalize_local_file_send_job(processing_file, False)
                    continue
                request, request_changed = ensure_local_file_send_request_deadline(request, processing_file)
                if request_changed:
                    write_json_atomic(processing_file, request)
                request_id = str(request.get("requestId") or local_queue_request_id_from_job_file(processing_file)).strip() or local_queue_request_id_from_job_file(processing_file)
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


async def process_local_send_message_queue_once() -> None:
    cleanup_stale_local_send_message_result_files()
    ensure_local_send_message_dirs(sorted(BOTS.keys()))
    for queue_paths in list_local_send_message_queue_path_groups():
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
                write_local_send_message_result(
                    local_queue_request_id_from_job_file(processing_file),
                    {"ok": False, "statusCode": 400, "error": "invalid local message request"},
                    queue_paths,
                )
                finalize_local_send_message_job(processing_file, False)
                continue
            request_id = str(request.get("requestId") or local_queue_request_id_from_job_file(processing_file)).strip() or local_queue_request_id_from_job_file(processing_file)
            expires_at_value = request.get("expiresAt") if "expiresAt" in request else request.get("expires_at")
            expires_at = normalize_optional_int(expires_at_value)
            if expires_at is not None and now_ms() > expires_at:
                write_local_send_message_result(
                    request_id,
                    {"ok": False, "statusCode": 408, "error": "timeout waiting for bridge result"},
                    queue_paths,
                )
                finalize_local_send_message_job(processing_file, False)
                continue
            try:
                result = await submit_send_message_request(request, "local-command")
                write_local_send_message_result(request_id, result, queue_paths)
                finalize_local_send_message_job(processing_file, True)
            except BridgeError as exc:
                if exc.status_code == 503 and (exc.message == "bot not connected" or exc.message.startswith("bot not running:")):
                    write_json_atomic(queue_paths["pending"] / processing_file.name, request)
                    with suppress(FileNotFoundError):
                        processing_file.unlink()
                    continue
                write_local_send_message_result(
                    request_id,
                    {"ok": False, "statusCode": exc.status_code, "error": exc.message},
                    queue_paths,
                )
                finalize_local_send_message_job(processing_file, False)


def create_request_future(bot: BotState, req_id: str) -> asyncio.Future:
    req_text = str(req_id or "").strip()
    if not req_text:
        raise BridgeError(400, "req_id required")
    existing = bot.pending_requests.get(req_text)
    if existing is not None and not existing.done():
        raise BridgeError(409, f"duplicate pending request: {req_text}")
    future = asyncio.get_running_loop().create_future()
    bot.pending_requests[req_text] = future
    return future


def resolve_request_future(bot: BotState, req_id: str, payload: dict[str, Any]) -> bool:
    future = bot.pending_requests.pop(str(req_id or "").strip(), None)
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
    media_type: str = "file",
    title: str = "",
    description: str = "",
    to_ai_assistant: Optional[dict[str, Any]] = None,
) -> None:
    if not bot.ws or bot.ws.closed:
        raise BridgeError(503, "bot not connected")
    path = Path(file_path)
    try:
        assistant = normalize_assistant_reply(to_ai_assistant, proactive=True)
        if assistant and media_type == "voice":
            raise ValueError("assistant replies do not support voice")
        validate_media_upload(path.name, path.stat().st_size, media_type, max_bytes=MAX_UPLOAD_SIZE)
    except ValueError as exc:
        raise BridgeError(413 if str(exc).startswith("file too large:") else 400, str(exc)) from exc
    cache_root = SHARED_RUNTIME_ROOT / ".wecom-uploads" / quote(str(bot.config["id"]), safe="")
    try:
        media_id = await upload_media(
            path, filename=path.name, media_type=media_type, owner=str(bot.config["botId"]), destination=chat_key_value,
            cache_root=cache_root, send=lambda payload, timeout: send_ws_payload_with_ack(bot, payload, timeout),
            uid=uid, max_bytes=MAX_UPLOAD_SIZE,
        )
    except MediaUploadRejected as exc:
        raise BridgeError(502, str(exc)) from exc
    chat_type, chat_id = chat_key_to_send_target(chat_key_value)
    body = {"chatid": chat_id, "chat_type": chat_type, **native_media_body(media_type, media_id, title=title, description=description)}
    if assistant:
        body["to_ai_assistant"] = assistant
    send_payload = {"cmd": "aibot_send_msg", "headers": {"req_id": uid()}, "body": body}
    if before_delivery:
        before_delivery()
    response = await send_ws_payload_with_ack(bot, send_payload, 30)
    require_ack_success(response, "file send")
    if after_delivery:
        after_delivery()
    add_log(bot, f"{media_type} sent: {path.name}")
    add_event_log(bot, "file.sent", chatKey=chat_key_value, fileName=path.name, mediaId=media_id, mediaType=media_type)


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
                    media_type=str(job.get("mediaType") or "file"),
                    title=str(job.get("title") or ""),
                    description=str(job.get("description") or ""),
                    to_ai_assistant=job.get("toAiAssistant"),
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
    normalized = normalize_optional_string(work_dir) or DEFAULT_WORK_DIR
    resolved = Path(normalized).expanduser().resolve()
    if not resolved.exists():
        raise BridgeError(400, f"workDir not found: {resolved}")
    if not resolved.is_dir():
        raise BridgeError(400, f"workDir is not a directory: {resolved}")
    return str(resolved)


def normalize_group_session_mode(value: Any) -> str:
    mode = (str(value or "per-user").strip().lower() or "per-user").replace("_", "-")
    if mode in {"none", "null"}:
        mode = "per-user"
    if mode not in VALID_GROUP_SESSION_MODES:
        raise BridgeError(400, f"groupSessionMode must be one of: {', '.join(sorted(VALID_GROUP_SESSION_MODES))}")
    return mode


def normalize_optional_path(value: Any, *, base_dir: Optional[Path] = None) -> str:
    normalized_text = normalize_optional_string(value)
    raw = normalized_text or ""
    if not raw:
        return ""
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = ((base_dir or BASE_DIR) / path).resolve()
    else:
        path = path.resolve()
    return str(path)


def normalize_optional_string(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null"}:
        return None
    return text


def normalize_optional_user_id(value: Any) -> Optional[str]:
    return normalize_optional_string(value)


def normalize_enabled_flag(value: Any, *, status_code: int) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return True
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise BridgeError(status_code, f"enabled must be a boolean, got: {value}")


def normalize_internal_bool_flag(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in {"none", "null"}:
        return default
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
    secret = normalize_optional_string(data.get("secret")) or ""
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


def resolve_optional_secret_file(data: dict[str, Any], *keys: str, status_code: int = 400) -> tuple[Optional[str], Optional[str]]:
    path_value = None
    for key in keys:
        candidate = data.get(key)
        if str(candidate or "").strip():
            path_value = candidate
            break
    normalized_path = normalize_optional_path(path_value)
    if not normalized_path:
        return None, None
    secret = read_text_file(normalized_path, label=keys[0], status_code=status_code)
    return secret, normalized_path


def normalize_bot_config(data: dict[str, Any], *, allow_inline_secret: bool = False) -> dict[str, Any]:
    bot_id = normalize_optional_string(data.get("botId")) or ""
    if not bot_id:
        raise BridgeError(400, "botId required")
    secret, secret_file = resolve_bot_secret(data, status_code=400, allow_inline_secret=allow_inline_secret)
    agent_backend = normalize_agent_backend(data.get("agentBackend") or data.get("agent_backend") or "codex")
    workspace_mode = normalize_workspace_mode(
        data.get("workspaceMode") or data.get("workspace_mode"),
        default_backend=agent_backend,
        fallback_for_missing="team" if any(key in data for key in ("createdAt", "created_at", "updatedAt", "updated_at")) else None,
    )
    config_id = normalize_optional_string(data.get("id")) or uid()
    workspace_namespace = normalize_optional_string(data.get("workspaceNamespace") or data.get("workspace_namespace")) or config_id
    normalized = {
        "id": config_id,
        "name": (normalize_optional_string(data.get("name")) or "unnamed"),
        "botId": bot_id,
        "secret": secret,
        "workDir": normalize_work_dir(data.get("workDir")),
        "workspaceNamespace": workspace_namespace,
        "welcome": normalize_optional_string(data.get("welcome")) or "",
        "groupSessionMode": normalize_group_session_mode(data.get("groupSessionMode") or data.get("group_session_mode")),
        "workspaceMode": workspace_mode,
        "agentBackend": agent_backend,
        "agentCommand": normalize_agent_command(data.get("agentCommand") or data.get("agent_command")),
        "agentRunAsUser": None,
        "agentRunAsGroup": None,
        "agentRuntimeRoot": None,
        "enabled": normalize_enabled_flag(data.get("enabled", True), status_code=400),
        "restartToken": normalize_optional_string(data.get("restartToken") or data.get("restart_token")),
        "localResumeOwnerUserId": normalize_optional_user_id(
            data.get("localResumeOwnerUserId") or data.get("local_resume_owner_user_id")
        ),
    }
    if secret_file:
        normalized["secretFile"] = secret_file
    if normalize_internal_bool_flag(data.get("bootstrapManaged"), default=False):
        normalized["bootstrapManaged"] = True
    return normalized


def normalize_persisted_bot_config(record: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(record)
    if str(normalized.get("secret") or "").strip():
        raise BridgeError(
            500,
            "legacy plaintext bot secrets in .bots.json are no longer supported; rewrite the bot to use secretFile or env bootstrap",
        )
    if "botId" in normalized:
        normalized_bot_id = normalize_optional_string(normalized.get("botId")) or ""
        if not normalized_bot_id:
            raise BridgeError(500, "botId required")
        normalized["botId"] = normalized_bot_id
    if "name" in normalized:
        normalized["name"] = normalize_optional_string(normalized.get("name")) or "unnamed"
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
    workspace_namespace = normalize_optional_string(
        normalized.get("workspaceNamespace") or normalized.get("workspace_namespace") or normalized.get("id")
    )
    if workspace_namespace:
        normalized["workspaceNamespace"] = workspace_namespace
    else:
        normalized.pop("workspaceNamespace", None)
    if "welcome" in normalized:
        normalized["welcome"] = normalize_optional_string(normalized.get("welcome")) or ""
    workspace_mode = str(normalized.get("workspaceMode") or normalized.get("workspace_mode") or "").strip()
    normalized["workspaceMode"] = normalize_workspace_mode(
        workspace_mode,
        default_backend=normalized.get("agentBackend") or "codex",
        fallback_for_missing="team",
    )
    try:
        group_session_mode = normalized.get("groupSessionMode") or normalized.get("group_session_mode")
        if group_session_mode not in (None, ""):
            normalized["groupSessionMode"] = normalize_group_session_mode(group_session_mode)
        agent_backend = normalized.get("agentBackend") or normalized.get("agent_backend")
        if agent_backend not in (None, ""):
            normalized["agentBackend"] = normalize_agent_backend(agent_backend)
    except ValueError as exc:
        raise BridgeError(500, str(exc)) from exc
    normalized["agentCommand"] = normalize_agent_command(normalized.get("agentCommand") or normalized.get("agent_command"))
    normalized["agentRunAsUser"] = None
    normalized["agentRunAsGroup"] = None
    normalized["agentRuntimeRoot"] = None
    normalized["localResumeOwnerUserId"] = normalize_optional_user_id(
        normalized.get("localResumeOwnerUserId") or normalized.get("local_resume_owner_user_id")
    )
    if "enabled" in normalized:
        normalized["enabled"] = normalize_enabled_flag(normalized.get("enabled"), status_code=500)
    if normalize_internal_bool_flag(normalized.get("bootstrapManaged"), default=False):
        normalized["bootstrapManaged"] = True
    else:
        normalized.pop("bootstrapManaged", None)
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
            candidate["bootstrapManaged"] = True
            configs.append(candidate)

    bot_id = normalize_optional_string(os.environ.get("WECOM_BOT_ID")) or ""
    secret_file = normalize_optional_string(os.environ.get("WECOM_BOT_SECRET_FILE")) or ""
    if bot_id and bot_tombstone_deleted_at(bot_id):
        return configs
    if os.environ.get("WECOM_BOT_SECRET"):
        raise BridgeError(500, "WECOM_BOT_SECRET is no longer supported; use WECOM_BOT_SECRET_FILE")
    if bot_id or secret_file:
        if not bot_id or not secret_file:
            raise BridgeError(500, "WECOM_BOT_ID must be set together with WECOM_BOT_SECRET_FILE")
        explicit_id = normalize_optional_string(os.environ.get("WECOM_BOT_CONFIG_ID")) or ""
        candidate = {
            "id": explicit_id or default_bot_config_id(bot_id),
            "name": (normalize_optional_string(os.environ.get("WECOM_BOT_NAME")) or "default"),
            "botId": bot_id,
            "workDir": str(os.environ.get("WECOM_BOT_WORK_DIR") or DEFAULT_WORK_DIR).strip() or DEFAULT_WORK_DIR,
            "workspaceNamespace": str(
                os.environ.get("WECOM_BOT_WORKSPACE_NAMESPACE")
                or os.environ.get("WECOM_WORKSPACE_NAMESPACE")
                or explicit_id
                or default_bot_config_id(bot_id)
            ).strip()
            or (explicit_id or default_bot_config_id(bot_id)),
            "welcome": str(os.environ.get("WECOM_BOT_WELCOME") or ""),
            "groupSessionMode": str(os.environ.get("WECOM_BOT_GROUP_SESSION_MODE") or "per-user").strip()
            or "per-user",
            "agentBackend": str(os.environ.get("WECOM_BOT_AGENT_BACKEND") or os.environ.get("AGENT_BACKEND") or "codex").strip()
            or "codex",
            "workspaceMode": str(
                os.environ.get("WECOM_BOT_WORKSPACE_MODE") or os.environ.get("WECOM_WORKSPACE_MODE") or ""
            ).strip()
            or None,
            "agentCommand": str(os.environ.get("WECOM_BOT_AGENT_COMMAND") or os.environ.get("AGENT_COMMAND") or "").strip() or None,
            "agentRunAsUser": None,
            "agentRunAsGroup": None,
            "agentRuntimeRoot": None,
            "localResumeOwnerUserId": str(
                os.environ.get("WECOM_BOT_LOCAL_RESUME_OWNER_USER_ID")
                or os.environ.get("LOCAL_RESUME_OWNER_USER_ID")
                or ""
            ).strip()
            or None,
            "enabled": env_bool("WECOM_BOT_ENABLED", True),
            "__idExplicit__": bool(explicit_id),
            "__secretSourceExplicit__": True,
            "bootstrapManaged": True,
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
        candidate_bootstrap_managed = normalize_internal_bool_flag(candidate.get("bootstrapManaged"), default=False)
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
        if candidate_bootstrap_managed:
            normalized["bootstrapManaged"] = True
        if match_index is None:
            merged.append(normalized)
            match_index = len(merged) - 1
        else:
            merged[match_index] = normalized
        index_by_id[normalized["id"]] = match_index
        index_by_bot_id[normalized["botId"]] = match_index

    return merged


def is_bootstrap_managed_persisted_bot_config(config: dict[str, Any]) -> bool:
    if normalize_internal_bool_flag(config.get("bootstrapManaged"), default=False):
        return True
    bot_id = normalize_optional_string(config.get("botId")) or ""
    config_id = normalize_optional_string(config.get("id")) or ""
    return bool(bot_id and config_id and config_id == default_bot_config_id(bot_id))


def prune_stale_bootstrap_managed_persisted_bot_configs(
    stored_configs: list[dict[str, Any]],
    bootstrap_configs: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], set[str]]:
    if not bootstrap_configs:
        return list(stored_configs), set()

    active_bootstrap_ids = {
        str(item.get("id") or "").strip()
        for item in bootstrap_configs
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    active_bootstrap_bot_ids = {
        str(item.get("botId") or "").strip()
        for item in bootstrap_configs
        if isinstance(item, dict) and str(item.get("botId") or "").strip()
    }

    filtered: list[dict[str, Any]] = []
    dropped_ids: set[str] = set()
    for item in stored_configs:
        config_id = str(item.get("id") or "").strip()
        bot_id = str(item.get("botId") or "").strip()
        if not config_id or not bot_id:
            filtered.append(item)
            continue
        if config_id in active_bootstrap_ids or bot_id in active_bootstrap_bot_ids:
            filtered.append(item)
            continue
        if is_bootstrap_managed_persisted_bot_config(item):
            dropped_ids.add(config_id)
            continue
        filtered.append(item)
    return filtered, dropped_ids


def serialize_bot_config_for_disk(config: dict[str, Any]) -> dict[str, Any]:
    persisted = dict(config)
    persisted.pop("secret", None)
    persisted.pop("appSecret", None)
    if not str(persisted.get("secretFile") or "").strip():
        persisted.pop("secretFile", None)
    if not str(persisted.get("appSecretFile") or "").strip():
        persisted.pop("appSecretFile", None)
    for key in (
        "agentCommand",
        "agentRunAsUser",
        "agentRunAsGroup",
        "agentRuntimeRoot",
        "localResumeOwnerUserId",
    ):
        if not str(persisted.get(key) or "").strip():
            persisted.pop(key, None)
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


def persisted_bot_config_generation(record: dict[str, Any]) -> Optional[int]:
    updated_at = normalize_optional_int(record.get("updatedAt") or record.get("updated_at"))
    created_at = normalize_optional_int(record.get("createdAt") or record.get("created_at"))
    return updated_at if updated_at is not None else created_at


def filter_deleted_persisted_bot_config_entries(entries: list[Any]) -> list[Any]:
    filtered: list[Any] = []
    for item in entries:
        if not isinstance(item, dict):
            filtered.append(item)
            continue
        wecom_bot_id = str(item.get("botId") or "").strip()
        if wecom_bot_id:
            deleted_at = bot_tombstone_deleted_at(wecom_bot_id)
            if deleted_at:
                config_generation = persisted_bot_config_generation(item)
                if config_generation is None or config_generation <= deleted_at:
                    continue
        filtered.append(item)
    return filtered


def has_newer_persisted_bot_config_than_tombstone(bot_id: str, wecom_bot_id: str, deleted_at: int) -> bool:
    with persisted_bot_configs_lock():
        for item in read_persisted_bot_configs_unlocked():
            config_id = str(item.get("id") or "").strip()
            current_wecom_bot_id = str(item.get("botId") or "").strip()
            if config_id != bot_id and current_wecom_bot_id != wecom_bot_id:
                continue
            generation = persisted_bot_config_generation(item)
            if generation is not None and generation > deleted_at:
                return True
    return False


def newer_persisted_config_uses_different_id(bot_id: str, wecom_bot_id: str, deleted_at: int) -> bool:
    with persisted_bot_configs_lock():
        for item in read_persisted_bot_configs_unlocked():
            current_wecom_bot_id = str(item.get("botId") or "").strip()
            if current_wecom_bot_id != wecom_bot_id:
                continue
            generation = persisted_bot_config_generation(item)
            if generation is None or generation <= deleted_at:
                continue
            config_id = str(item.get("id") or "").strip()
            if config_id and config_id != bot_id:
                return True
    return False


def read_persisted_bot_config_entries_unlocked() -> list[Any]:
    stored_raw = read_json_file(DATA_FILE, [])
    if not isinstance(stored_raw, list):
        print_log(f"[INIT] invalid bot config file, ignore: {DATA_FILE}")
        return []
    filtered_raw = filter_deleted_persisted_bot_config_entries(stored_raw)
    if filtered_raw != stored_raw or not DATA_FILE.exists():
        normalized_valid = normalize_persisted_bot_config_entries(filtered_raw)
        next_entries = replace_persisted_bot_config_entries(filtered_raw, normalized_valid)
        write_json_atomic(DATA_FILE, next_entries)
        return next_entries
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
        target_wecom_bot_id = ""
        for item in stored:
            if str(item.get("id") or "").strip() == bot_id:
                target_wecom_bot_id = str(item.get("botId") or "").strip()
                break
        if not target_wecom_bot_id:
            runtime_bot = BOTS.get(bot_id)
            if runtime_bot:
                target_wecom_bot_id = str(runtime_bot.config.get("botId") or "").strip()
        current_entries = read_persisted_bot_config_entries_unlocked()
        next_entries = [
            item
            for item in current_entries
            if not (
                isinstance(item, dict)
                and (
                    str(item.get("id") or "").strip() == bot_id
                    or (target_wecom_bot_id and str(item.get("botId") or "").strip() == target_wecom_bot_id)
                )
            )
        ]
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


def workspace_namespace_in_use(namespace: str, *, excluding_bot_id: Optional[str] = None) -> bool:
    target_namespace = workspace_slug(namespace)
    excluded = str(excluding_bot_id or "").strip()
    for bot_id, bot in BOTS.items():
        if excluded and bot_id == excluded:
            continue
        if bot_instance_is_stale(bot):
            continue
        if workspace_namespace_for_bot(bot.config) == target_namespace:
            return True
    for config in read_persisted_bot_configs():
        config_id = str(config.get("id") or "").strip()
        if excluded and config_id == excluded:
            continue
        if workspace_namespace_for_bot(config) == target_namespace:
            return True
    return False


def cleanup_workspace_namespace_if_unused(namespace: str, *, excluding_bot_id: Optional[str] = None) -> None:
    if workspace_namespace_in_use(namespace, excluding_bot_id=excluding_bot_id):
        return
    remove_tree_if_exists(get_workspace_namespace_dir(namespace))
    remove_tree_if_exists(get_user_alias_dir(namespace))


def clear_persisted_session_threads(bot_id: str) -> int:
    cleared = 0
    for record in session_records_for_bot(bot_id):
        if not (
            str(record.get("threadId") or "").strip()
            or str(record.get("activeScheduleId") or "").strip()
            or str(record.get("status") or "") in {"leased", "running"}
            or record.get("ownerInstance") is not None
            or record.get("ownerPid") is not None
            or record.get("leaseExpiresAt") is not None
        ):
            continue
        update_session_record(
            str(record["sessionId"]),
            lambda current: {
                **current,
                "threadId": None,
                "useGlobalCodexSessions": False,
                "activeScheduleId": None,
                "status": "idle",
                "ownerInstance": None,
                "ownerPid": None,
                "leaseExpiresAt": None,
            },
        )
        cleared += 1
    return cleared


def invalidate_bot_session_mappings(bot_id: str) -> int:
    removed = 0
    candidate_session_ids = set(session_ids_referenced_by_key_root(bot_id))
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if sessions_root.exists():
        for session_file in sessions_root.glob("*.json"):
            raw = read_json_file(session_file, None)
            if not raw_session_record_matches_bot(bot_id, raw, session_file=session_file):
                continue
            candidate_session_ids.update(
                session_id
                for session_id in {
                    str(raw.get("sessionId") or "").strip(),
                    str(session_file.stem or "").strip(),
                }
                if safe_session_id(session_id)
            )
            for session_id in {
                session_id
                for session_id in {
                    str(raw.get("sessionId") or "").strip(),
                    str(session_file.stem or "").strip(),
                }
                if safe_session_id(session_id)
            }:
                remove_tree_if_exists(get_chatfile_dir(session_id))
                remove_session_codex_home(session_id)
                remove_path_if_exists(get_managed_session_lock_file(bot_id, session_id))
                remove_path_if_exists(legacy_managed_session_lock_file(bot_id, session_id))
            remove_path_if_exists(session_file)
            removed += 1
    for session_id in candidate_session_ids:
        remove_tree_if_exists(get_chatfile_dir(session_id))
        remove_session_codex_home(session_id)
        remove_path_if_exists(get_managed_session_lock_file(bot_id, session_id))
        remove_path_if_exists(legacy_managed_session_lock_file(bot_id, session_id))
        if get_registry_session_file(session_id).exists():
            remove_path_if_exists(get_registry_session_file(session_id))
            removed += 1
    key_root = SESSION_REGISTRY_ROOT / "keys" / bot_id
    if key_root.exists():
        for key_file in key_root.glob("*.json"):
            remove_path_if_exists(key_file)
    return removed


def rebuild_schedule_definition_session_bindings(bot: BotState) -> int:
    updated = 0
    for definition in list_schedule_definitions():
        if str(definition.get("botId") or "").strip() != str(bot.config.get("id") or "").strip():
            continue
        chat_key = str(definition.get("chatKey") or "").strip()
        if not chat_key:
            continue
        record = get_or_create_session_record(bot, chat_key)
        next_session_id = str(record["sessionId"])
        if str(definition.get("sessionId") or "").strip() == next_session_id:
            continue
        write_schedule_definition({**definition, "sessionId": next_session_id})
        updated += 1
    return updated


def rebuild_scheduled_job_session_bindings(bot: BotState) -> int:
    updated = 0
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT):
        if not root.exists():
            continue
        for job_file in root.glob("*.json"):
            job = read_json_file(job_file, None)
            if not isinstance(job, dict):
                continue
            job_bot_id = str(job.get("botId") or "").strip()
            if job_bot_id and job_bot_id != str(bot.config.get("id") or "").strip():
                continue
            if not job_bot_id:
                schedule_id = str(job.get("scheduleId") or "").strip()
                definition = read_schedule_definition(schedule_id) if schedule_id else None
                if not definition or str(definition.get("botId") or "").strip() != str(bot.config.get("id") or "").strip():
                    continue
            chat_key = str(job.get("chatKey") or "").strip()
            if not chat_key:
                continue
            record = get_or_create_session_record(bot, chat_key)
            next_session_id = str(record["sessionId"])
            if str(job.get("sessionId") or "").strip() == next_session_id:
                continue
            write_json_atomic(job_file, {**job, "sessionId": next_session_id})
            updated += 1
    return updated


def invalidate_bot_session_threads(bot_id: str) -> int:
    invalidated = 0
    bot = BOTS.get(bot_id)
    if bot:
        for sess in bot.sessions.values():
            if sess.thread_id is None:
                continue
            sess.thread_id = None
            sess.use_global_codex_sessions = False
            invalidated += 1
    invalidated += clear_persisted_session_threads(bot_id)
    return invalidated


def reset_bot_session_runtime_state(bot_id: str, replacement_config: Optional[dict[str, Any]] = None) -> int:
    bot = BOTS.get(bot_id)
    if not bot:
        cleared = clear_persisted_session_threads(bot_id)
        invalidate_bot_session_mappings(bot_id)
        if replacement_config:
            rebuild_bot = BotState(config=dict(replacement_config))
            rebuild_schedule_definition_session_bindings(rebuild_bot)
            rebuild_scheduled_job_session_bindings(rebuild_bot)
        return cleared
    cleared = 0
    for sess in bot.sessions.values():
        had_state = bool(
            sess.thread_id
            or sess.chat
            or sess.queue
            or sess.pending_media
            or sess.pending_media_notes
            or sess.active_run_media
            or sess.active_run_media_notes
            or sess.pending_stream_payload
            or sess.pending_final_payload
            or sess.reply_started_at
            or sess.reply_last_sent_at
            or sess.reply_proactive_req_ids
            or sess.proactive_status_sent_at
            or sess.reply_mentions_sent
        )
        sess.thread_id = None
        clear_session_run_thread_checkpoint(sess)
        sess.use_global_codex_sessions = False
        sess.chat = []
        sess.queue = []
        sess.pending_media = []
        sess.pending_media_notes = []
        sess.pending_media_downloads = 0
        sess.active_run_media = []
        sess.active_run_media_notes = []
        sess.pending_stream_payload = None
        sess.pending_final_payload = None
        sess.pending_stream_payloads = None
        sess.pending_final_payloads = None
        sess.late_ack_payloads = []
        sess.pending_payload_retry_after = 0.0
        sess.reply_started_at = {}
        sess.reply_last_sent_at = {}
        sess.reply_proactive_req_ids = set()
        sess.proactive_status_sent_at = {}
        sess.reply_mentions_sent = set()
        sess.active_schedule_id = None
        sess.active_scheduled_job_file = None
        sess.active_schedule_request_id = None
        if had_state:
            cleared += 1
    if bot.reply_sessions:
        bot.reply_sessions = {}
    cleared += clear_persisted_session_threads(bot_id)
    invalidate_bot_session_mappings(bot_id)
    rebuild_target = BotState(config=dict(replacement_config)) if replacement_config else bot
    rebuild_schedule_definition_session_bindings(rebuild_target)
    rebuild_scheduled_job_session_bindings(rebuild_target)
    return cleared


def bot_runtime_workspace_identity(config: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(config.get("workDir") or "").strip(),
        workspace_namespace_for_bot(config),
        bot_workspace_mode(config),
        normalize_group_session_mode(config.get("groupSessionMode") or config.get("group_session_mode") or "per-user"),
    )


def remove_path_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def remove_tree_if_exists(path: Path) -> bool:
    if path.is_symlink():
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        return True
    if not path.exists():
        return False
    shutil.rmtree(path, ignore_errors=True)
    return not path.exists()


def clear_directory_contents(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir() or child.is_symlink():
            remove_tree_if_exists(child)
        else:
            remove_path_if_exists(child)


def replace_path_if_needed(path: Path) -> None:
    if path.is_symlink():
        remove_path_if_exists(path)
        return
    if path.is_dir():
        remove_tree_if_exists(path)


def sync_tree_contents(source: Path, target: Path) -> None:
    ensure_dir(target)
    for child in sorted(source.iterdir(), key=lambda item: item.name):
        destination = target / child.name
        if child.is_symlink():
            if destination.exists() or destination.is_symlink():
                continue
            ensure_dir_for(destination)
            os.symlink(os.readlink(child), destination, target_is_directory=child.is_dir())
        elif child.is_dir():
            shutil.copytree(child, destination, dirs_exist_ok=True, symlinks=True)
        else:
            ensure_dir_for(destination)
            shutil.copy2(child, destination, follow_symlinks=False)


def copy_missing_tree_contents(source: Path, target: Path) -> None:
    ensure_dir(target)
    for child in sorted(source.iterdir(), key=lambda item: item.name):
        destination = target / child.name
        if child.is_symlink():
            if destination.exists() or destination.is_symlink():
                continue
            ensure_dir_for(destination)
            os.symlink(os.readlink(child), destination, target_is_directory=child.is_dir())
            continue
        if destination.exists() or destination.is_symlink():
            if child.is_dir() and destination.is_dir() and not destination.is_symlink():
                copy_missing_tree_contents(child, destination)
            continue
        if child.is_dir():
            shutil.copytree(child, destination, symlinks=True)
        else:
            ensure_dir_for(destination)
            shutil.copy2(child, destination, follow_symlinks=False)


def copy_bridge_session_resume_state(source_session_id: str, target_session_id: str) -> None:
    if source_session_id == target_session_id:
        return
    source = get_session_codex_home_root(source_session_id) / "sessions"
    if not source.exists() or not source.is_dir():
        return
    target = get_session_codex_home_root(target_session_id) / "sessions"
    copy_missing_tree_contents(source, target)


def clone_payload_sequence(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [dict(item) for item in payloads]


def build_codex_home_for_subprocess(session_id: str, *, use_global_sessions: bool = False) -> Path:
    base_home = DEFAULT_CODEX_HOME
    bridge_home = get_session_codex_home_root(session_id)
    backup_sessions_root = get_session_codex_home_local_sessions_backup_root(session_id)
    ensure_dir(bridge_home)

    allowed_root_files = {"auth.json", "config.toml", "version.json", "installation_id"}
    if base_home.exists() and base_home.is_dir():
        for child in sorted(base_home.iterdir(), key=lambda item: item.name):
            # Skip volatile runtime trees. They are recreated per session and may
            # contain short-lived sandbox binaries that disappear while copying.
            if child.name in {"skills", "sessions", "tmp"}:
                continue
            if child.is_file() and child.name not in allowed_root_files:
                continue
            if child.is_dir():
                continue
            destination = bridge_home / child.name
            try:
                if child.resolve() == destination.resolve():
                    continue
            except FileNotFoundError:
                pass
            replace_path_if_needed(destination)
            ensure_dir_for(destination)
            shutil.copy2(child, destination)

    sessions_root = bridge_home / "sessions"
    if use_global_sessions and base_home.exists() and base_home.is_dir():
        global_sessions_root = base_home / "sessions"
        same_sessions_root = False
        try:
            same_sessions_root = base_home.resolve() == bridge_home.resolve()
        except Exception:
            same_sessions_root = base_home == bridge_home
        if same_sessions_root:
            if sessions_root.is_symlink():
                remove_path_if_exists(sessions_root)
            ensure_dir(sessions_root)
        else:
            ensure_dir(global_sessions_root)
            sessions_link_is_current = False
            if sessions_root.is_symlink():
                try:
                    sessions_link_is_current = sessions_root.resolve() == global_sessions_root.resolve()
                except Exception:
                    sessions_link_is_current = False
            if not sessions_link_is_current:
                if sessions_root.exists() and not sessions_root.is_symlink():
                    replace_path_if_needed(backup_sessions_root)
                    sessions_root.replace(backup_sessions_root)
                elif sessions_root.is_symlink():
                    remove_path_if_exists(sessions_root)
            if not sessions_root.exists():
                os.symlink(global_sessions_root, sessions_root, target_is_directory=True)
    else:
        if sessions_root.is_symlink():
            remove_path_if_exists(sessions_root)
        if backup_sessions_root.exists():
            replace_path_if_needed(sessions_root)
            backup_sessions_root.replace(sessions_root)
        else:
            ensure_dir(sessions_root)
    ensure_dir(bridge_home / "tmp")
    skills_link = bridge_home / "skills"
    if skills_link.exists() or skills_link.is_symlink():
        if skills_link.is_symlink() or skills_link.is_file():
            remove_path_if_exists(skills_link)
        else:
            remove_tree_if_exists(skills_link)
    if (base_home / "skills").exists() and (base_home / "skills").is_dir():
        os.symlink((base_home / "skills"), skills_link, target_is_directory=True)
    else:
        ensure_dir(skills_link)
    return bridge_home


def cleanup_bot_session_storage(bot_id: str) -> None:
    candidate_session_ids = set(session_ids_referenced_by_key_root(bot_id))
    sessions_root = SESSION_REGISTRY_ROOT / "sessions"
    if sessions_root.exists():
        for session_file in sessions_root.glob("*.json"):
            raw = read_json_file(session_file, None)
            if not raw_session_record_matches_bot(bot_id, raw, session_file=session_file):
                continue
            candidate_session_ids.update(
                session_id
                for session_id in {
                    str(raw.get("sessionId") or "").strip(),
                    str(session_file.stem or "").strip(),
                }
                if safe_session_id(session_id)
            )
            for session_id in {
                session_id
                for session_id in {
                    str(raw.get("sessionId") or "").strip(),
                    str(session_file.stem or "").strip(),
                }
                if safe_session_id(session_id)
            }:
                remove_path_if_exists(get_managed_session_lock_file(bot_id, session_id))
                remove_path_if_exists(legacy_managed_session_lock_file(bot_id, session_id))
                remove_session_codex_home(session_id)
                remove_tree_if_exists(get_chatfile_dir(session_id))
            remove_path_if_exists(session_file)
    for session_id in candidate_session_ids:
        remove_path_if_exists(get_managed_session_lock_file(bot_id, session_id))
        remove_path_if_exists(legacy_managed_session_lock_file(bot_id, session_id))
        remove_session_codex_home(session_id)
        remove_tree_if_exists(get_chatfile_dir(session_id))
        remove_path_if_exists(get_registry_session_file(session_id))
    remove_tree_if_exists(SESSION_REGISTRY_ROOT / "keys" / bot_id)
    remove_tree_if_exists(SESSION_LOCK_ROOT / bot_id)


def cleanup_bot_schedule_jobs(bot_id: str, *, schedule_ids: Optional[set[str]] = None) -> None:
    target_schedule_ids = {str(item).strip() for item in (schedule_ids or set()) if str(item).strip()}
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT, SCHEDULE_DONE_ROOT, SCHEDULE_FAILED_ROOT):
        if not root.exists():
            continue
        for job_file in root.glob("*.json"):
            job = read_json_file(job_file, None)
            if not isinstance(job, dict):
                continue
            job_bot_id = str(job.get("botId") or "").strip()
            schedule_id = str(job.get("scheduleId") or "").strip()
            if job_bot_id == bot_id or (not job_bot_id and schedule_id in target_schedule_ids):
                remove_path_if_exists(job_file)


def cleanup_bot_schedule_definitions(bot_id: str) -> None:
    schedule_ids = [item["scheduleId"] for item in list_schedule_definitions() if str(item.get("botId") or "") == bot_id]
    removed_schedule_ids: set[str] = {str(item).strip() for item in schedule_ids if str(item).strip()}
    referenced_schedule_ids: set[str] = set(removed_schedule_ids)
    for root in (SCHEDULE_PENDING_ROOT, SCHEDULE_PROCESSING_ROOT, SCHEDULE_DONE_ROOT, SCHEDULE_FAILED_ROOT):
        if not root.exists():
            continue
        for job_file in root.glob("*.json"):
            raw_job = read_json_file(job_file, None)
            if not isinstance(raw_job, dict) or str(raw_job.get("botId") or "").strip() != bot_id:
                continue
            schedule_id = str(raw_job.get("scheduleId") or "").strip()
            if schedule_id:
                referenced_schedule_ids.add(schedule_id)
    for schedule_id in schedule_ids:
        delete_schedule_definition(schedule_id)
        remove_path_if_exists(get_schedule_definition_lock_file(schedule_id))
    for definition_file in SCHEDULE_DEFINITION_ROOT.glob("*.json"):
        raw = read_json_file(definition_file, None)
        if isinstance(raw, dict):
            schedule_id = str(raw.get("scheduleId") or "").strip()
            if not schedule_id:
                schedule_id = unquote(definition_file.stem).strip()
            raw_bot_id = str(raw.get("botId") or "").strip()
            owner_matches = raw_bot_id == bot_id
            linked_from_jobs = bool(schedule_id and schedule_id in referenced_schedule_ids)
            if not owner_matches and not (not raw_bot_id and linked_from_jobs):
                continue
        else:
            schedule_id = unquote(definition_file.stem).strip()
            if not schedule_id or schedule_id not in referenced_schedule_ids:
                continue
        if not schedule_id:
            continue
        if schedule_id and schedule_id in schedule_ids:
            continue
        if schedule_id:
            removed_schedule_ids.add(schedule_id)
        remove_path_if_exists(definition_file)
        if schedule_id:
            remove_path_if_exists(get_schedule_definition_lock_file(schedule_id))
    cleanup_bot_schedule_jobs(bot_id, schedule_ids=removed_schedule_ids)


async def remove_deleted_bots_from_memory_once() -> None:
    for bot_id, bot in list(BOTS.items()):
        if not bot_instance_is_stale(bot):
            continue
        wecom_bot_id = str(bot.config.get("botId") or "").strip()
        deleted_at = bot_tombstone_deleted_at(wecom_bot_id) if wecom_bot_id else 0
        preserve_recreated_state = bool(
            wecom_bot_id
            and deleted_at > 0
            and has_newer_persisted_bot_config_than_tombstone(bot_id, wecom_bot_id, deleted_at)
        )
        preserve_with_new_config_id = bool(
            preserve_recreated_state
            and wecom_bot_id
            and deleted_at > 0
            and newer_persisted_config_uses_different_id(bot_id, wecom_bot_id, deleted_at)
        )
        workspace_namespace = workspace_namespace_for_bot(bot.config)
        bot.config["enabled"] = False
        await stop_bot(bot_id, persist_disable=False, reason="deleted_bot_reaper")
        if not preserve_recreated_state:
            remove_persisted_bot_config(bot_id)
            cleanup_bot_schedule_definitions(bot_id)
            cleanup_bot_session_storage(bot_id)
            cleanup_workspace_namespace_if_unused(workspace_namespace, excluding_bot_id=bot_id)
        elif preserve_with_new_config_id:
            cleanup_bot_schedule_definitions(bot_id)
            cleanup_bot_session_storage(bot_id)
            cleanup_workspace_namespace_if_unused(workspace_namespace, excluding_bot_id=bot_id)
        BOTS.pop(bot_id, None)
        PREPARED_PREVIOUS_BOT_CONFIGS.pop(bot_id, None)
        if not preserve_recreated_state or preserve_with_new_config_id:
            delete_template_card_state(bot_id)


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
        stored, dropped_bootstrap_managed_ids = prune_stale_bootstrap_managed_persisted_bot_configs(stored, bootstrap)
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
        current_entries = read_persisted_bot_config_entries_unlocked()
        next_entries = replace_persisted_bot_config_entries(
            current_entries,
            stamped_runtime_payload,
            drop_ids=dropped_bootstrap_managed_ids,
        )
        if next_entries != current_entries or not DATA_FILE.exists():
            write_json_atomic(DATA_FILE, next_entries)
        if dropped_bootstrap_managed_ids:
            print_log(f"[INIT] pruned {len(dropped_bootstrap_managed_ids)} stale bootstrap bot config(s)")
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


async def cancel_task(task: Optional[asyncio.Task], timeout_sec: Optional[float] = None) -> bool:
    if not task:
        return True
    task.cancel()
    try:
        if timeout_sec is None:
            await task
        else:
            await asyncio.wait_for(asyncio.shield(task), timeout_sec)
    except asyncio.CancelledError:
        return True
    except asyncio.TimeoutError:
        return False
    except Exception:
        return True
    return True


async def terminate_process(
    process: Optional[asyncio.subprocess.Process],
    timeout_sec: float = 5,
    *,
    require_exit: bool = False,
    skip_wait_if_exited: bool = False,
) -> bool:
    if not process:
        return True
    if skip_wait_if_exited and process.returncode is not None:
        session_id = getattr(process, "_bridge_process_session_id", None)
        if session_id is None:
            return True
    exited = await terminate_process_tree(process, timeout_sec=timeout_sec)
    if exited:
        return True
    if require_exit:
        raise BridgeError(409, "agent process did not exit; session context was not cleared")
    return False


async def stop_bot(
    bot_id: str,
    persist_disable: bool = True,
    persist_state: bool = True,
    reason: str = "internal",
) -> None:
    bot = BOTS.get(bot_id)
    if not bot:
        return
    normalized_reason = str(reason or "internal").strip() or "internal"
    bot.stop_reason = normalized_reason
    add_log(bot, f"stopping (reason={normalized_reason})")
    add_event_log(bot, "bot.stopping", reason=normalized_reason)
    if persist_disable:
        bot.config["enabled"] = False
    if bot.ws and not bot.ws.closed:
        await bot.ws.close()
    await cancel_task(bot.heartbeat_task)
    await cancel_task(bot.reader_task)
    for task in list(bot.callback_tasks):
        await cancel_task(task)
    bot.callback_tasks.clear()
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
    write_template_card_state(bot)
    if persist_state and not bot_instance_is_stale(bot):
        upsert_persisted_bot_config(bot.config)


async def remove_bot(bot_id: str) -> None:
    existing_config = get_authoritative_bot_config(bot_id)
    if existing_config is None:
        raise BridgeError(404, f"bot not found: {bot_id}")
    wecom_bot_id = str((existing_config or {}).get("botId") or "").strip() or None
    workspace_namespace = workspace_namespace_for_bot(existing_config)
    mark_bot_deleted_globally(bot_id, wecom_bot_id)
    remove_persisted_bot_config(bot_id)
    try:
        await stop_bot(bot_id, persist_disable=False, reason="remove_bot")
        cleanup_bot_schedule_definitions(bot_id)
        cleanup_bot_session_storage(bot_id)
        cleanup_workspace_namespace_if_unused(workspace_namespace, excluding_bot_id=bot_id)
    finally:
        BOTS.pop(bot_id, None)
        PREPARED_PREVIOUS_BOT_CONFIGS.pop(bot_id, None)
        delete_template_card_state(bot_id)


async def dispatch_assistant_call(bot: BotState, frames: list[dict]) -> None:
    first = frames[0]
    assistant_info = _as_dict(
        _as_dict(_as_dict(first.get("body")).get("compat_info")).get("from_ai_assistant")
    )
    task_id = str(assistant_info.get("task_id") or "")
    timeout_task = bot.assistant_call_timeout_tasks.pop(task_id, None)
    if timeout_task is not None and timeout_task is not asyncio.current_task():
        timeout_task.cancel()
    key = chat_key_for_bot(bot, first)
    req_id = str(_as_dict(first.get("headers")).get("req_id") or "")
    texts = [text_with_quote(message_text(_as_dict(frame.get("body"))), _as_dict(frame.get("body")))
             for frame in frames if _as_dict(frame.get("body")).get("msgtype") == "text"]
    content = "\n\n".join(text for text in texts if text)
    if not content:
        content = "本次智能助理调用只提供了附件，未提供文字指令。请询问需要执行的处理目标。"
    async with get_session_lifecycle_lock(bot, key):
        sess = await get_or_create_session_ready(bot, key)
        if not acquire_session_lease(bot, sess, key):
            await respond_queued_error(bot, req_id, "session is temporarily owned by another instance")
            return
        register_reply_session(bot, req_id, sess)
        media = [(body.get("msgtype"), _as_dict(body.get(body.get("msgtype"))))
                 for frame in frames if (body := _as_dict(frame.get("body"))).get("msgtype") in {"image", "file"}]
        media.extend(item for frame in frames for item in quoted_media(_as_dict(frame.get("body"))))
        if media:
            await respond_info(bot, req_id, "正在接收本次调用的附件。", final=False)
        for kind, payload in media:
            sess.pending_media_downloads += 1
            try:
                sess.pending_media.append(await download_incoming_media(bot, sess, key, kind, payload))
            except Exception as exc:
                add_log(bot, f"[{key}] assistant attachment receive failed: {type(exc).__name__}")
                sess.pending_media_notes.append("本次调用的附件接收失败；请说明需要重新提供附件，不能声称已读取成功。")
            finally:
                sess.pending_media_downloads = max(0, sess.pending_media_downloads - 1)
        await enqueue_message_for_session(bot, sess, key, content, req_id, silent_lease_failure=False,
                                          scheduled_job_file=None, schedule_id=None, schedule_request_id=None)


async def expire_assistant_call(bot: BotState, task_id: str) -> None:
    try:
        await asyncio.sleep(ASSISTANT_CALL_TIMEOUT_SEC)
        frames = bot.assistant_calls.expire(task_id, timeout_sec=ASSISTANT_CALL_TIMEOUT_SEC)
        if not frames:
            return
        first = frames[0]
        req_id = str(_as_dict(first.get("headers")).get("req_id") or "")
        if req_id:
            await respond_info(bot, req_id, "本次智能助理调用未在规定时间内收齐全部内容，已结束。")
        add_log(bot, f"assistant call timed out task_id={task_id}")
    finally:
        bot.assistant_call_timeout_tasks.pop(task_id, None)


async def enqueue_text_with_quoted_media(bot: BotState, message: dict[str, Any], content: str) -> None:
    key = chat_key_for_bot(bot, message)
    req_id = str(_as_dict(message.get("headers")).get("req_id") or "")
    async with get_session_lifecycle_lock(bot, key):
        sess = await get_or_create_session_ready(bot, key)
        if not acquire_session_lease(bot, sess, key):
            await respond_queued_error(bot, req_id, "session is temporarily owned by another instance")
            return
        register_reply_session(bot, req_id, sess)
        await respond_info(bot, req_id, "正在接收引用消息的附件。", final=False)
        for kind, payload in quoted_media(_as_dict(message.get("body"))):
            try:
                sess.pending_media.append(await download_incoming_media(bot, sess, key, kind, payload))
            except Exception as exc:
                add_log(bot, f"[{key}] quoted attachment receive failed: {type(exc).__name__}")
                sess.pending_media_notes.append("引用附件无法下载，可能已过期；请说明需要重新发送，不能声称已读取成功。")
        await enqueue_message_for_session(bot, sess, key, content, req_id, silent_lease_failure=False,
                                          scheduled_job_file=None, schedule_id=None, schedule_request_id=None)


def load_wecom_feedback(bot: BotState) -> dict[str, dict[str, Any]]:
    if bot.feedback_records:
        return bot.feedback_records
    path = SHARED_RUNTIME_ROOT / ".wecom-feedback" / (quote(str(bot.config["id"]), safe="") + ".json")
    try:
        payload = json.loads(path.read_text("utf-8"))
        records = payload.get("records") if isinstance(payload, dict) else []
        if isinstance(records, list):
            bot.feedback_records = {record["id"]: record for record in records if isinstance(record, dict) and isinstance(record.get("id"), str)}
    except (OSError, ValueError):
        pass
    return bot.feedback_records


def record_wecom_feedback(bot: BotState, body: dict[str, Any]) -> None:
    load_wecom_feedback(bot)
    record = feedback_event(body)
    if record is None:
        return
    record = {**record, "user_id": str(_as_dict(body.get("from")).get("userid") or ""),
              "source": message_source(body), "received_at": now_ms()}
    for context in bot.callback_contexts.values():
        if context.get("feedback_id") == record["id"]:
            record["chat_key"] = context["chat_key"]
            record["assistant_task_id"] = context.get("assistant_task_id")
            break
    # The latest verdict replaces the previous one; type 3 explicitly cancels it.
    bot.feedback_records[record["id"]] = record
    if len(bot.feedback_records) > 1000:
        oldest = min(bot.feedback_records, key=lambda key: bot.feedback_records[key]["received_at"])
        bot.feedback_records.pop(oldest, None)
    path = SHARED_RUNTIME_ROOT / ".wecom-feedback" / (quote(str(bot.config["id"]), safe="") + ".json")
    write_json_atomic(path, {"records": list(bot.feedback_records.values())})
    add_event_log(bot, "wecom.feedback", feedbackId=record["id"], feedbackType=record["type"], source=record["source"])


async def handle_wecom_message(bot: BotState, message: dict[str, Any], *, received_at: Optional[float] = None) -> None:
    if not isinstance(message, dict):
        return
    bot.last_inbound_at_ms = now_ms()
    if WECOM_RAW_INBOUND_LOG_ENABLED:
        add_log(bot, f"raw inbound: {summarize_inbound_payload(message)}")
    headers = _as_dict(message.get("headers"))
    body = _as_dict(message.get("body"))
    req_id = headers.get("req_id")
    inbound_chat_key: Optional[str] = None
    try:
        inbound_chat_key = chat_key_for_bot(bot, message)
    except BridgeError:
        inbound_chat_key = None
    if req_id and inbound_chat_key and message.get("cmd") in {"aibot_msg_callback", "aibot_event_callback"}:
        now = bot.rate_limiter.clock()
        bot.callback_contexts = {key: value for key, value in bot.callback_contexts.items() if now - value["received_at"] < 86400}
        try:
            bot.callback_contexts[str(req_id)] = callback_context(message, inbound_chat_key, chat_key_to_send_target(inbound_chat_key), received_at=received_at if received_at is not None else now)
        except ValueError as exc:
            add_log(bot, f"invalid callback context: {exc}")
            return
    if req_id:
        response_url = extract_response_url(message)
        if response_url and inbound_chat_key:
            save_reply_url(bot, str(req_id), inbound_chat_key, response_url)
    if req_id and resolve_request_future(bot, req_id, message):
        if str(message.get("cmd") or "").strip().lower() == "ping":
            bot.last_heartbeat_ack_at_ms = bot.last_inbound_at_ms
        await flush_pending_session_payloads_from_inbound(bot, "resolved_request")
        return

    if message.get("errcode") is not None and not message.get("cmd"):
        if not has_explicit_zero_errcode(message):
            add_log(bot, f"WeCom response error: {message.get('errcode')} {message.get('errmsg', '')}")
        else:
            if req_id and consume_late_cached_session_payload_ack(bot, req_id):
                await flush_pending_session_payloads_from_inbound(bot, "late_cached_ack")
                return
            await flush_pending_session_payloads_from_inbound(bot, "ack_only")
        return

    command = message.get("cmd")
    msg_type = body.get("msgtype")
    if command == "aibot_event_callback" and _as_dict(body.get("event")).get("eventtype") == "feedback_event":
        try:
            record_wecom_feedback(bot, body)
        except ValueError as exc:
            add_log(bot, f"invalid feedback event: {exc}")
        return
    if command == "aibot_event_callback" and msg_type == "event":
        event = _as_dict(body.get("event"))
        if str(event.get("eventtype") or "").strip() == "disconnected_event":
            bot.status = "disconnected"
            bot.last_disconnect_at_ms = now_ms()
            add_log(bot, "recv disconnected event from WeCom, closing websocket")
            add_event_log(bot, "wecom.disconnected_event")
            try:
                if bot.ws and not bot.ws.closed:
                    await bot.ws.close()
            except Exception as exc:
                add_log(bot, f"close websocket after disconnected event failed: {exc}")
            return

    if inbound_chat_key is None:
        return

    event_key = (
        body.get("msgid")
        or (
            f"{(headers.get('req_id') or '')}:"
            f"{(body.get('msgtype') or '')}:"
            f"{inbound_chat_key}:"
            f"{message_sender_userid(message)}:"
            f"{dedupe_content_fingerprint(message)}"
        )
    )
    prune_recent_events()
    if event_key in RECENT_EVENTS:
        add_log(bot, f"ignore duplicate event: {event_key}")
        return
    RECENT_EVENTS[event_key] = time.time() + RECENT_EVENT_TTL
    if command == "aibot_msg_callback" and message_source(body) == "ai_assistant":
        try:
            frames = bot.assistant_calls.add(message)
        except ValueError as exc:
            await respond_info(bot, req_id, f"本次调用的输入无法聚合：{exc}")
            return
        if frames is not None:
            await dispatch_assistant_call(bot, frames)
        else:
            task_id = str(_as_dict(_as_dict(body.get("compat_info")).get("from_ai_assistant")).get("task_id") or "")
            if task_id and task_id not in bot.assistant_call_timeout_tasks:
                timeout_task = asyncio.create_task(expire_assistant_call(bot, task_id))
                bot.assistant_call_timeout_tasks[task_id] = timeout_task
                bot.callback_tasks.add(timeout_task)
                timeout_task.add_done_callback(bot.callback_tasks.discard)
        return

    if command == "aibot_msg_callback" and msg_type in {"text", "voice"}:
        inbound_text = message_text(body)
        content = strip_text_mentions(inbound_text, bot.config.get("name"))
        command_text = normalize_bridge_command_text(inbound_text, bot.config.get("name"))
        key = inbound_chat_key
        add_log(bot, f'recv: "{content}" {key}')
        try:
            if command_text == "/bridge-resume":
                await start_resume_selection_command(bot, key, req_id)
                return
            if command_text.startswith("/bridge-resume "):
                await resume_session_command(bot, key, req_id, command_text.split(None, 1)[1].strip())
                return
            if command_text == "/local-resume":
                await start_local_resume_selection_command(
                    bot,
                    key,
                    req_id,
                    sender_user_id=message_sender_userid(message),
                )
                return
            if command_text.startswith("/local-resume "):
                await local_resume_session_command(
                    bot,
                    key,
                    req_id,
                    command_text.split(None, 1)[1].strip(),
                    sender_user_id=message_sender_userid(message),
                )
                return
            if command_text == "/local-detach":
                await local_detach_session_command(
                    bot,
                    key,
                    req_id,
                    sender_user_id=message_sender_userid(message),
                )
                return
            if command_text == "/cwd":
                await cwd_session_command(bot, key, req_id, None)
                return
            if command_text.startswith("/cwd "):
                await cwd_session_command(bot, key, req_id, command_text.split(None, 1)[1])
                return
            if command_text == "/bridge-interrupt":
                await interrupt_session_command(bot, key, req_id)
                return
            if command_text == "/new":
                await new_session_command(bot, key, req_id)
                return
            if command_text == "/bridge-reset":
                await reset_session_command(bot, key, req_id)
                return
            if command_text == "/bridge-status":
                await status_session_command(bot, key, req_id)
                return
            current_sess = bot.sessions.get(key)
            if current_sess and await apply_resume_selection_command(
                bot,
                key,
                req_id,
                content,
                sender_user_id=message_sender_userid(message),
            ):
                return
        except BridgeError as exc:
            await respond_info(bot, req_id, f"Bridge command failed: {exc.message}")
            return
        if content:
            business_text = text_with_quote(content, body)
            if quoted_media(body):
                await enqueue_text_with_quoted_media(bot, message, business_text)
            else:
                await enqueue_message(bot, key, business_text, req_id)
        return

    if command == "aibot_msg_callback" and msg_type == "image":
        key = chat_key_for_bot(bot, message)
        add_log(bot, f"recv image {key}")
        await enqueue_media_message(bot, message, "image")
        return

    if command == "aibot_msg_callback" and msg_type in {"file", "video"}:
        key = chat_key_for_bot(bot, message)
        add_log(bot, f"recv file {key}")
        await enqueue_media_message(bot, message, msg_type)
        return

    if command == "aibot_msg_callback" and msg_type == "mixed":
        key = chat_key_for_bot(bot, message)
        add_log(bot, f"recv mixed {key}")
        await enqueue_mixed_message(bot, message)
        return

    event = _as_dict(body.get("event"))
    if command == "aibot_event_callback" and (event.get("eventtype") == "enter_chat"):
        welcome = bot.config.get("welcome") or "Send me a message and I will route it to Codex."
        await send_ws_payload(
            bot,
            {
                "cmd": "aibot_respond_welcome_msg",
                "headers": {"req_id": req_id},
                "body": {"msgtype": "text", "text": {"content": welcome}},
            },
        )
        return

    if command == "aibot_event_callback" and (event.get("eventtype") == "template_card_event"):
        await handle_template_card_event(bot, message)
        return

    if command == "aibot_event_callback" and (event.get("eventtype") == "disconnected_event"):
        bot.status = "disconnected"
        bot.last_disconnect_at_ms = now_ms()
        add_log(bot, "WeCom disconnected event received")
        add_event_log(bot, "wecom.disconnected_event")
        if bot.ws and not bot.ws.closed:
            await bot.ws.close()
        return


async def handle_wecom_frame_safely(bot: BotState, payload: Any, received_at: Optional[float] = None) -> None:
    try:
        await handle_wecom_message(bot, payload, received_at=received_at)
    except BridgeError as exc:
        add_log(bot, f"message handling error: {exc.message}")
        if isinstance(payload, dict) and payload.get("cmd") == "aibot_msg_callback":
            req_id = _as_dict(payload.get("headers")).get("req_id")
            await respond_info(bot, req_id, f"请求处理失败：{exc.message}")
    except Exception as exc:
        add_log(bot, f"message handling crash: {exc}")


async def run_bounded_callback_task(bot: BotState, payload: Any, received_at: float) -> None:
    try:
        await handle_wecom_frame_safely(bot, payload, received_at)
    finally:
        bot.callback_semaphore.release()


async def ws_reader_loop(bot: BotState, ws: aiohttp.ClientWebSocketResponse) -> None:
    async for raw_msg in ws:
        if raw_msg.type == WSMsgType.TEXT:
            try:
                payload = json.loads(raw_msg.data)
            except Exception:
                continue
            # Apply backpressure before creating a task. A semaphore inside the
            # task would still allow an unbounded number of waiting tasks.
            await bot.callback_semaphore.acquire()
            task = asyncio.create_task(run_bounded_callback_task(bot, payload, bot.rate_limiter.clock()))
            bot.callback_tasks.add(task)
            task.add_done_callback(bot.callback_tasks.discard)
        elif raw_msg.type == WSMsgType.ERROR:
            raise BridgeError(503, f"websocket error: {ws.exception()}")
        elif raw_msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
            break


async def heartbeat_loop(bot: BotState) -> None:
    while bot.ws and not bot.ws.closed and bot.config.get("enabled", True):
        await asyncio.sleep(30)
        if not bot.ws or bot.ws.closed:
            return
        try:
            await send_ws_payload_with_ack(
                bot,
                {"cmd": "ping", "headers": {"req_id": uid()}},
                max(1, int(WECOM_HEARTBEAT_ACK_TIMEOUT_MS / 1000)),
            )
        except BridgeError as exc:
            add_log(bot, f"WeCom heartbeat failed: {exc.message}")
            raise


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
                    if has_explicit_zero_errcode(response):
                        bot.status = "running"
                        bot.last_subscribed_at_ms = now_ms()
                        bot.stop_reason = None
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
                bot.last_disconnect_at_ms = now_ms()
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
    if previous_config and bot_runtime_workspace_identity(previous_config) != bot_runtime_workspace_identity(normalized):
        reset_bot_session_runtime_state(normalized["id"], normalized)
    if normalized["id"] in BOTS:
        await stop_bot(normalized["id"], persist_disable=False, reason="start_bot_replace")
    bot = BotState(config=normalized)
    bot.template_card_delivery_meta = read_template_card_state(normalized["id"])
    bot.reply_urls = read_reply_url_state(normalized["id"])
    restore_template_card_runtime_state(bot)
    BOTS[normalized["id"]] = bot
    bot.stop_reason = None
    add_log(bot, "starting...")
    supervise_bot_task(bot, "upload_worker_task", "upload_worker", lambda: upload_worker(bot))
    supervise_bot_task(bot, "runner_task", "bot_runner", lambda: bot_runner(bot))
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
    async with get_session_lifecycle_lock(bot, key):
        try:
            sess = await get_or_create_session_ready(bot, key)
        except Exception as exc:
            if not silent_lease_failure:
                add_log(bot, f"[{key}] init failed: {exc}")
                await respond_queued_error(bot, req_id, f"init failed: {exc}")
            return False
        return await enqueue_message_for_session(
            bot,
            sess,
            key,
            text,
            req_id,
            silent_lease_failure=silent_lease_failure,
            scheduled_job_file=scheduled_job_file,
            schedule_id=schedule_id,
            schedule_request_id=schedule_request_id,
        )


async def enqueue_message_for_session(
    bot: BotState,
    sess: SessionState,
    key: str,
    text: str,
    req_id: Optional[str],
    *,
    silent_lease_failure: bool,
    scheduled_job_file: Optional[str],
    schedule_id: Optional[str],
    schedule_request_id: Optional[str],
) -> bool:
    if not acquire_session_lease(bot, sess, key):
        if not silent_lease_failure:
            add_log(bot, f"[{key}] lease owned by another process, ignore message")
            add_event_log(bot, "session.lease_conflict", chatKey=key, sessionId=sess.session_id, action="drop_user_message")
            await respond_queued_error(bot, req_id, "session is temporarily owned by another bridge instance; please retry")
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
    append_session_chat_message(
        sess,
        {"role": "user", "text": text, "from": key, "time": int(time.time())},
        persist=True,
    )
    if queue_position > 1:
        if req_id:
            await respond_info(bot, req_id, build_queued_proactive_notice(queue_position), cache_on_failure=False)
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
    async with get_session_lifecycle_lock(bot, key):
        try:
            sess = await get_or_create_session_ready(bot, key)
        except Exception as exc:
            add_log(bot, f"[{key}] media session init failed: {exc}")
            await respond_queued_error(bot, req_id, f"init failed: {exc}")
            return
        await enqueue_media_message_for_session(bot, sess, key, req_id, message, kind)


async def enqueue_media_message_for_session(
    bot: BotState,
    sess: SessionState,
    key: str,
    req_id: Optional[str],
    message: dict[str, Any],
    kind: str,
) -> None:
    if not acquire_session_lease(bot, sess, key):
        add_log(bot, f"[{key}] lease owned by another process, ignore media message")
        add_event_log(bot, "session.lease_conflict", chatKey=key, sessionId=sess.session_id, action="drop_media_message")
        await respond_queued_error(bot, req_id, "session is temporarily owned by another bridge instance; please retry sending the attachment")
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
    async with get_session_lifecycle_lock(bot, key):
        try:
            sess = await get_or_create_session_ready(bot, key)
        except Exception as exc:
            add_log(bot, f"[{key}] mixed session init failed: {exc}")
            await respond_queued_error(bot, req_id, f"init failed: {exc}")
            return
        await enqueue_mixed_message_for_session(bot, sess, key, req_id, message)


async def enqueue_mixed_message_for_session(
    bot: BotState,
    sess: SessionState,
    key: str,
    req_id: Optional[str],
    message: dict[str, Any],
) -> None:
    if not acquire_session_lease(bot, sess, key):
        add_log(bot, f"[{key}] lease owned by another process, ignore mixed message")
        add_event_log(bot, "session.lease_conflict", chatKey=key, sessionId=sess.session_id, action="drop_mixed_message")
        await respond_queued_error(bot, req_id, "session is temporarily owned by another bridge instance; please retry")
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
        await enqueue_message_for_session(
            bot,
            sess,
            key,
            mixed_text,
            req_id,
            silent_lease_failure=False,
            scheduled_job_file=None,
            schedule_id=None,
            schedule_request_id=None,
        )


async def session_recycler_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await asyncio.sleep(60)
            now = time.time()
            active_session_ids = {
                sess.session_id
                for bot in list(BOTS.values())
                for sess in bot.sessions.values()
                if str(sess.session_id or "").strip()
            }
            cleanup_stale_session_codex_homes(int(now * 1000), active_session_ids=active_session_ids)
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
                    pending_delivery = bool(sess.pending_final_payload or sess.pending_stream_payload)
                    needs_renewal = bool(
                        sess.running
                        or sess.queue
                        or sess.pending_media_downloads > 0
                        or (sess.lease_owned and (pending_delivery or sess.send_lock.locked()))
                    )
                    if not needs_renewal:
                        continue
                    if renew_session_lease(bot, key, sess):
                        continue
                    active_task = sess.run_task
                    active_proc = sess.proc
                    if (
                        sess.running
                        or pending_delivery
                        or (active_task is not None and not active_task.done())
                        or (active_proc is not None and active_proc.returncode is None)
                    ):
                        abandon_session_after_lease_loss(bot, key, sess)
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


async def local_send_message_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            await process_local_send_message_queue_once()
            await asyncio.sleep(LOCAL_SEND_MESSAGE_POLL_MS / 1000)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print_log(f"[LOOP] local_send_message_loop error: {exc}")
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
    raw_bootstrap = load_env_bootstrap_bot_configs()
    with persisted_bot_configs_lock():
        persisted_configs = filter_deleted_persisted_bot_configs(read_persisted_bot_configs_unlocked())
        bootstrap_configs = filter_deleted_bot_configs(raw_bootstrap)
        persisted_configs, dropped_bootstrap_managed_ids = prune_stale_bootstrap_managed_persisted_bot_configs(
            persisted_configs,
            bootstrap_configs,
        )
        if dropped_bootstrap_managed_ids:
            current_entries = read_persisted_bot_config_entries_unlocked()
            next_entries = [
                item
                for item in current_entries
                if not (
                    isinstance(item, dict)
                    and str(item.get("id") or "").strip() in dropped_bootstrap_managed_ids
                )
            ]
            if next_entries != current_entries or not DATA_FILE.exists():
                write_json_atomic(DATA_FILE, next_entries)
            print_log(f"[SYNC] pruned {len(dropped_bootstrap_managed_ids)} stale bootstrap bot config(s)")
        if bootstrap_configs:
            persisted_configs = merge_bot_configs(persisted_configs, bootstrap_configs)
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
            await stop_bot(bot_id, persist_disable=False, persist_state=False, reason="config_reconcile_missing")
            BOTS.pop(bot_id, None)
            PREPARED_PREVIOUS_BOT_CONFIGS.pop(bot_id, None)
            continue
        if desired.get("enabled") is False:
            if bot.config.get("enabled", True):
                bot.config = {**bot.config, **desired, "enabled": False}
                await stop_bot(bot_id, persist_disable=False, reason="config_reconcile_disabled")
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
    return extract_bearer_token(request.headers)


def extract_basic_auth(request: web.Request) -> str:
    return extract_basic_credentials(request.headers)


def api_auth_enabled() -> bool:
    return shared_api_auth_enabled(BRIDGE_TOKEN, BRIDGE_BASIC_AUTH)


def api_auth_mode_label() -> str:
    return shared_api_auth_mode_label(BRIDGE_TOKEN, BRIDGE_BASIC_AUTH)


def build_api_auth_error(message: str, status: int) -> web.Response:
    headers = {}
    if BRIDGE_BASIC_AUTH:
        headers["WWW-Authenticate"] = 'Basic realm="WeCom Codex Bridge"'
    return web.json_response({"ok": False, "error": message}, status=status, headers=headers)


def request_is_loopback(request: web.Request) -> bool:
    return is_loopback_address(request.remote)


def secure_text_equal(candidate: str, expected: str) -> bool:
    return shared_secure_text_equal(candidate, expected)


async def require_api_access(request: web.Request) -> Optional[web.Response]:
    decision = authorize_api_access(
        request.headers,
        request.remote,
        token=BRIDGE_TOKEN,
        basic_auth=BRIDGE_BASIC_AUTH,
    )
    if decision.allowed:
        return None
    return build_api_auth_error(decision.message, decision.status)


@web.middleware
async def api_access_middleware(request: web.Request, handler: Any) -> web.StreamResponse:
    if request.path == "/healthz" or request.path.startswith("/api/"):
        denied = await require_api_access(request)
        if denied is not None:
            return denied
    return await handler(request)


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
    health = bot_health_snapshot(bot)
    sessions = []
    for key, sess in bot.sessions.items():
        session_status = "running" if sess.running else ("queued" if sess.queue else "idle")
        sessions.append(
            {
                "key": key,
                "sessionId": sess.session_id,
                "threadId": sess.thread_id,
                "status": session_status,
                "alive": True,
                "busy": bool(sess.running or sess.queue),
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
        "workspaceMode": bot.config.get("workspaceMode", "team"),
        "workspaceNamespace": bot.config.get("workspaceNamespace"),
        "groupSessionMode": bot.config.get("groupSessionMode", "per-user"),
        "agentBackend": bot.config.get("agentBackend", "codex"),
        "agentCommand": bot.config.get("agentCommand"),
        "agentRunAsUser": bot.config.get("agentRunAsUser"),
        "agentRunAsGroup": bot.config.get("agentRunAsGroup"),
        "agentRuntimeRoot": bot.config.get("agentRuntimeRoot"),
        "localResumeOwnerUserId": bot.config.get("localResumeOwnerUserId"),
        "secretSource": bot_secret_source(bot.config),
        "status": bot.status,
        "healthy": health["healthy"],
        "capabilities": {"assistantAggregation": True, "assistantReplyState": True,
                         "channelGroupCompatibility": True, "nativeChannelLongConnection": False,
                         "nativeMediaTypes": ["file", "image", "voice", "video"],
                         "resumableUploads": True, "feedbackCollection": True, "conversationRateLimits": True},
        "callbackBusy": any(not task.done() for task in bot.callback_tasks),
        "wsConnected": health["wsConnected"],
        "runnerTaskDone": health["runnerTaskDone"],
        "uploadWorkerDone": health["uploadWorkerDone"],
        "taskRestartCounts": health["taskRestartCounts"],
        "lastSubscribedAt": health["lastSubscribedAt"],
        "lastDisconnectedAt": health["lastDisconnectedAt"],
        "lastInboundAt": health["lastInboundAt"],
        "lastHeartbeatAckAt": health["lastHeartbeatAckAt"],
        "inboundIdleMs": health["inboundIdleMs"],
        "heartbeatAckIdleMs": health["heartbeatAckIdleMs"],
        "inboundStale": health["inboundStale"],
        "heartbeatAckStale": health["heartbeatAckStale"],
        "disconnectGraceActive": health["disconnectGraceActive"],
        "stopReason": health["stopReason"],
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
        "workspaceMode": config.get("workspaceMode", "team"),
        "workspaceNamespace": config.get("workspaceNamespace"),
        "groupSessionMode": config.get("groupSessionMode", "per-user"),
        "agentBackend": config.get("agentBackend", "codex"),
        "agentCommand": config.get("agentCommand"),
        "agentRunAsUser": config.get("agentRunAsUser"),
        "agentRunAsGroup": config.get("agentRunAsGroup"),
        "agentRuntimeRoot": config.get("agentRuntimeRoot"),
        "localResumeOwnerUserId": config.get("localResumeOwnerUserId"),
        "secretSource": bot_secret_source(config),
        "status": "disabled" if config.get("enabled") is False else "not_loaded",
        "healthy": False,
        "wsConnected": False,
        "runnerTaskDone": None,
        "uploadWorkerDone": None,
        "taskRestartCounts": {},
        "lastSubscribedAt": None,
        "lastDisconnectedAt": None,
        "lastInboundAt": None,
        "lastHeartbeatAckAt": None,
        "inboundIdleMs": None,
        "heartbeatAckIdleMs": None,
        "inboundStale": False,
        "heartbeatAckStale": False,
        "disconnectGraceActive": False,
        "stopReason": None,
        "enabled": config.get("enabled", True) is not False,
        "logs": [],
        "sessions": [],
    }


async def api_get_wecom_feedback(request: web.Request) -> web.Response:
    bot = BOTS.get(request.match_info["bot_id"])
    if bot is None:
        raise web.HTTPNotFound(text="bot not found")
    records = list(load_wecom_feedback(bot).values())
    counts = {"accurate": sum(record.get("type") == 1 for record in records),
              "inaccurate": sum(record.get("type") == 2 for record in records),
              "cancelled": sum(record.get("type") == 3 for record in records)}
    return web.json_response({"counts": counts, "records": sorted(records, key=lambda record: record.get("received_at", 0), reverse=True)[:100]})


async def api_get_bots(request: web.Request) -> web.Response:
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
    try:
        data = await read_json_body(request)
        wecom_bot_id = str(data.get("botId") or "").strip()
        config_id = find_config_id_by_wecom_bot_id(wecom_bot_id) or default_bot_config_id(wecom_bot_id)
        base = get_authoritative_bot_config(config_id) or {}
        candidate = {**base, **data, "id": config_id, "enabled": True}
        config = normalize_bot_config(candidate)
        await start_bot(config)
        payload = {"ok": True, "id": config["id"], "secretSource": bot_secret_source(config)}
        warning = bot_secret_persistence_warning(config)
        if warning:
            payload["warning"] = warning
        return web.json_response(payload)
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_delete_bot(request: web.Request) -> web.Response:
    try:
        await remove_bot(request.match_info["bot_id"])
        return web.json_response({"ok": True})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_restart_bot(request: web.Request) -> web.Response:
    try:
        bot_id = request.match_info["bot_id"]
        current = get_authoritative_bot_config(bot_id)
        if not current:
            return web.json_response({"ok": False, "error": "bot not found"}, status=404)
        updated = {**current, "enabled": True, "restartToken": uid()}
        upsert_persisted_bot_config(updated)
        if bot_id in BOTS:
            add_log(BOTS[bot_id], "restart requested via API")
            add_event_log(BOTS[bot_id], "bot.restart_requested", reason="api")
            BOTS[bot_id].config = dict(updated)
            await start_bot(dict(updated))
        return web.json_response({"ok": True, "reason": "api"})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_stop_bot(request: web.Request) -> web.Response:
    try:
        bot_id = request.match_info["bot_id"]
        current = get_authoritative_bot_config(bot_id)
        if not current:
            return web.json_response({"ok": False, "error": "bot not found"}, status=404)
        updated = {**current, "enabled": False}
        upsert_persisted_bot_config(updated)
        if bot_id in BOTS:
            BOTS[bot_id].config = dict(updated)
            await stop_bot(bot_id, persist_disable=False, reason="api_stop")
        return web.json_response({"ok": True, "reason": "api_stop"})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_get_chat(request: web.Request) -> web.Response:
    bot = BOTS.get(request.match_info["bot_id"])
    if not bot:
        return web.json_response({"ok": False, "error": "bot not found"}, status=404)
    key = unquote(request.match_info["chat_key"])
    sess = bot.sessions.get(key)
    if not sess:
        return web.json_response({"ok": False, "error": "session not found"}, status=404)
    return web.json_response(sess.chat)


async def api_interrupt_session(request: web.Request) -> web.Response:
    bot = BOTS.get(request.match_info["bot_id"])
    if not bot:
        return web.json_response({"ok": False, "error": "bot not found"}, status=404)
    key = unquote(request.match_info["chat_key"])
    try:
        result, sess = await interrupt_session_and_wait(bot, key)
        if sess:
            resume_session_queue(bot, sess, key, "interrupt api")
        return web.json_response({"ok": True, "action": "interrupt", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_new_session(request: web.Request) -> web.Response:
    bot = BOTS.get(request.match_info["bot_id"])
    if not bot:
        return web.json_response({"ok": False, "error": "bot not found"}, status=404)
    key = unquote(request.match_info["chat_key"])
    try:
        result = await new_session_context(bot, key)
        return web.json_response({"ok": True, "action": "new", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_reset_session(request: web.Request) -> web.Response:
    bot = BOTS.get(request.match_info["bot_id"])
    if not bot:
        return web.json_response({"ok": False, "error": "bot not found"}, status=404)
    key = unquote(request.match_info["chat_key"])
    try:
        result = await reset_session_context(bot, key)
        return web.json_response({"ok": True, "action": "reset", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_send_file(request: web.Request) -> web.Response:
    try:
        data = await read_json_body(request)
        return web.json_response(submit_file_send_request(data))
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_send_message(request: web.Request) -> web.Response:
    try:
        data = await read_json_body(request)
        return web.json_response(await submit_send_message_request(data))
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_get_schedules(request: web.Request) -> web.Response:
    schedules = [{**item, "botConfigId": item["botId"]} for item in list_schedule_definitions()]
    return web.json_response(schedules)


async def api_add_schedule(request: web.Request) -> web.Response:
    try:
        data = await read_json_body(request)
        return web.json_response(submit_schedule_definition_request(data))
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_get_schedule(request: web.Request) -> web.Response:
    schedule = read_schedule_definition(request.match_info["schedule_id"])
    if not schedule:
        return web.json_response({"ok": False, "error": "schedule not found"}, status=404)
    return web.json_response({**schedule, "botConfigId": schedule["botId"]})


async def api_pause_schedule(request: web.Request) -> web.Response:
    try:
        result = pause_schedule_definition(request.match_info["schedule_id"])
        return web.json_response({"ok": True, "action": "pause", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_resume_schedule(request: web.Request) -> web.Response:
    try:
        result = resume_schedule_definition(request.match_info["schedule_id"])
        return web.json_response({"ok": True, "action": "resume", **result})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_delete_schedule(request: web.Request) -> web.Response:
    try:
        delete_schedule_definition(request.match_info["schedule_id"])
        return web.json_response({"ok": True})
    except BridgeError as exc:
        return web.json_response({"ok": False, "error": exc.message}, status=exc.status_code)


async def api_schedule_message(request: web.Request) -> web.Response:
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


async def api_healthz(_: web.Request) -> web.Response:
    bot_snapshots = [bot_health_snapshot(bot) for bot in BOTS.values() if not bot_instance_is_stale(bot)]
    loaded_bot_ids = {snapshot["botConfigId"] for snapshot in bot_snapshots}
    missing_enabled_bot_ids = []
    missing_enabled_bots = []
    for config in read_persisted_bot_configs():
        if config.get("enabled") is False:
            continue
        config_id = str(config.get("id") or "").strip()
        if config_id and config_id not in loaded_bot_ids:
            missing_enabled_bot_ids.append(config_id)
            missing_enabled_bots.append(persisted_bot_to_payload(config))
    unhealthy = [snapshot for snapshot in bot_snapshots if not snapshot["healthy"]]
    status = 200 if not unhealthy else 503
    if missing_enabled_bot_ids:
        status = 503
    return web.json_response(
        {
            "ok": not unhealthy and not missing_enabled_bot_ids,
            "name": "WeCom Codex Bridge Python",
            "instanceId": INSTANCE_ID,
            "shutdown": SHUTDOWN_EVENT.is_set(),
            "botCount": len(bot_snapshots),
            "bots": bot_snapshots,
            "missingEnabledBotIds": missing_enabled_bot_ids,
            "missingEnabledBots": missing_enabled_bots,
        },
        status=status,
    )


async def api_livez(_: web.Request) -> web.Response:
    return web.json_response(
        {
            "ok": not SHUTDOWN_EVENT.is_set(),
            "name": "WeCom Codex Bridge Python",
            "instanceId": INSTANCE_ID,
            "shutdown": SHUTDOWN_EVENT.is_set(),
            "botCount": len([bot for bot in BOTS.values() if not bot_instance_is_stale(bot)]),
        },
        status=200 if not SHUTDOWN_EVENT.is_set() else 503,
    )


async def app_cleanup(_: web.Application) -> None:
    SHUTDOWN_EVENT.set()
    for bot_id in list(BOTS.keys()):
        await stop_bot(bot_id, persist_disable=False, reason="app_cleanup")
    if HTTP_SESSION and not HTTP_SESSION.closed:
        await HTTP_SESSION.close()


def build_app() -> web.Application:
    app = web.Application(middlewares=[api_access_middleware])
    app.router.add_get("/", api_root)
    app.router.add_get("/livez", api_livez)
    app.router.add_get("/healthz", api_healthz)
    app.router.add_get("/api/bots", api_get_bots)
    app.router.add_get("/api/bots/{bot_id}/feedback", api_get_wecom_feedback)
    app.router.add_post("/api/bots", api_add_bot)
    app.router.add_delete("/api/bots/{bot_id}", api_delete_bot)
    app.router.add_post("/api/bots/{bot_id}/restart", api_restart_bot)
    app.router.add_post("/api/bots/{bot_id}/stop", api_stop_bot)
    app.router.add_get("/api/bots/{bot_id}/sessions/{chat_key}/chat", api_get_chat)
    app.router.add_post("/api/bots/{bot_id}/sessions/{chat_key}/interrupt", api_interrupt_session)
    app.router.add_post("/api/bots/{bot_id}/sessions/{chat_key}/new", api_new_session)
    app.router.add_post("/api/bots/{bot_id}/sessions/{chat_key}/reset", api_reset_session)
    app.router.add_post("/api/send-message", api_send_message)
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

    ensure_secure_state_root(SHARED_RUNTIME_ROOT, "BRIDGE_SHARED_RUNTIME_ROOT")
    ensure_secure_state_root(INSTANCE_RUNTIME_ROOT, "BRIDGE_RUNTIME_ROOT")
    ensure_secure_state_root(DATA_FILE.parent, "BOTS_FILE parent")

    if not is_loopback_address(HOST) and not api_auth_enabled():
        print_log("[SECURITY] HOST is not loopback but API auth is not configured. Refusing to start.")
        raise SystemExit(1)
    if CODEX_EXEC_MODE not in VALID_CODEX_EXEC_MODES:
        print_log(f"[CONFIG] invalid CODEX_EXEC_MODE={CODEX_EXEC_MODE!r}; expected one of {sorted(VALID_CODEX_EXEC_MODES)}")
        raise SystemExit(1)

    ensure_local_file_send_dirs()
    ensure_schedule_dirs()
    ensure_state_directory(BOT_TOMBSTONE_ROOT)
    ensure_state_directory(BOT_RUNTIME_LOCK_ROOT)
    ensure_state_directory(SESSION_LOCK_ROOT)
    ensure_state_directory(SESSION_REGISTRY_ROOT)
    ensure_state_directory(SESSION_REGISTRY_ROOT / "keys")
    ensure_state_directory(SESSION_REGISTRY_ROOT / "sessions")
    ensure_state_directory(USER_ALIAS_ROOT)
    ensure_state_directory(TEMPLATE_CARD_STATE_ROOT)
    ensure_dir(CHATFILE_ROOT)
    ensure_dir(WORKSPACE_ROOT)
    ensure_state_directory(get_bridge_codex_home_root())
    if DATA_FILE.is_symlink():
        raise RuntimeError(f"BOTS_FILE must not be a symlink: {DATA_FILE}")
    if not DATA_FILE.exists():
        write_json_atomic(DATA_FILE, [])
    elif DATA_FILE.is_file():
        os.chmod(DATA_FILE, state_file_mode(DATA_FILE.parent))

    HTTP_SESSION = aiohttp.ClientSession(trust_env=True)
    CODEX_RUN_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_CODEX_RUNS)

    app = build_app()
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()

    previous_shutdown = consume_shutdown_reason()

    print_log(f"WeCom Codex Bridge Python: {BRIDGE_API_BASE}")
    print_log(f"[SECURITY] API access: {api_auth_mode_label()}")
    print_log(f"[CODEX] exec mode: {CODEX_EXEC_MODE}")
    print_log(f"[CODEX] max concurrent runs: {MAX_CONCURRENT_CODEX_RUNS}")
    if previous_shutdown:
        print_log(
            f"[INIT] previous shutdown reason={previous_shutdown.get('reason')} detail={previous_shutdown.get('detail') or '-'} at={previous_shutdown.get('at')}"
        )

    maybe_migrate_legacy_shared_runtime_state()
    maybe_migrate_legacy_instance_runtime_state()
    sanitize_all_session_records()
    cleanup_orphan_session_codex_homes()
    cleanup_stale_session_codex_homes()

    await load_bots()

    recycler_task = asyncio.create_task(session_recycler_loop())
    renew_task = asyncio.create_task(lease_renew_loop())
    local_send_task = asyncio.create_task(local_file_send_loop())
    local_send_message_task = asyncio.create_task(local_send_message_loop())
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
            loop.add_signal_handler(sig, lambda current_sig=sig: set_shutdown_signal_reason(f"signal:{current_sig.name.lower()}"))
        except NotImplementedError:
            pass

    await SHUTDOWN_EVENT.wait()

    recycler_task.cancel()
    renew_task.cancel()
    local_send_task.cancel()
    local_send_message_task.cancel()
    schedule_definition_task.cancel()
    scheduled_message_task.cancel()
    paused_recovery_task.cancel()
    bot_config_reconciler_task.cancel()
    deleted_bot_reaper_task.cancel()
    for task in (
        recycler_task,
        renew_task,
        local_send_task,
        local_send_message_task,
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
