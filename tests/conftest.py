from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
BRIDGE_PATH = REPO_ROOT / "bridge.py"
ISOLATED_ENV_VARS = (
    "AGENT_BACKEND",
    "AGENT_COMMAND",
    "AGENT_RUNTIME_ROOT",
    "AGENT_RUN_AS_GROUP",
    "AGENT_RUN_AS_USER",
    "BOTS_FILE",
    "BRIDGE_BASIC_AUTH",
    "BRIDGE_BIND",
    "BRIDGE_HOST",
    "BRIDGE_PORT",
    "BRIDGE_RUNTIME_ROOT",
    "BRIDGE_SHARED_RUNTIME_ROOT",
    "BRIDGE_TOKEN",
    "CODEX_COMMAND",
    "CODEX_EXEC_MODE",
    "CODEX_HOME",
    "FILE_SEND_ROOTS",
    "HOST",
    "LOCAL_FILE_SEND_QUEUE_ROOT",
    "LOCAL_FILE_SEND_POLL_MS",
    "LOCAL_FILE_SEND_RESULT_RETENTION_MS",
    "LOCAL_FILE_SEND_RESULT_TIMEOUT_MS",
    "LOCAL_RESUME_OWNER_USER_ID",
    "LOCAL_SEND_MESSAGE_QUEUE_ROOT",
    "LOCAL_SEND_MESSAGE_POLL_MS",
    "LOCAL_SEND_MESSAGE_RESULT_RETENTION_MS",
    "LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS",
    "MAX_CONCURRENT_CODEX_RUNS",
    "MAX_INBOUND_FILE_SIZE",
    "MAX_INBOUND_IMAGE_SIZE",
    "MAX_JSON_BODY",
    "MAX_UPLOAD_SIZE",
    "MEDIA_CONNECT_TIMEOUT",
    "MEDIA_TOTAL_TIMEOUT",
    "PORT",
    "PROACTIVE_SEND_ACK_TIMEOUT_SEC",
    "PROACTIVE_STATUS_INTERVAL_SEC",
    "PROACTIVE_TEXT_MAX_CHARS",
    "REPLY_IDLE_FALLBACK_SEC",
    "REPLY_MAX_AGE_FALLBACK_SEC",
    "REPLY_SEND_ACK_TIMEOUT_SEC",
    "SCHEDULE_DEFINITION_LEASE_TTL_MS",
    "SCHEDULE_DEFINITION_POLL_MS",
    "SCHEDULE_ORPHAN_TTL_MS",
    "SCHEDULE_POLL_MS",
    "SCHEDULE_PROCESSING_RETRY_MS",
    "SESSION_LEASE_TTL",
    "STATUS_SEND_LOCK_TIMEOUT_SEC",
    "STATUS_SEND_TIMEOUT_SEC",
    "STATUS_STREAM_INTERVAL_SEC",
    "STREAM_TEXT_MAX_CHARS",
    "SUBPROCESS_STREAM_LIMIT",
    "SUBPROCESS_STREAM_MAX_LINE",
    "SUBPROCESS_STREAM_READ_SIZE",
    "UNCERTAIN_DELIVERY_RETRY_SEC",
    "WEBSOCKET_SEND_TIMEOUT_SEC",
    "WECOM_BOOTSTRAP_BOTS_JSON",
    "WECOM_BOOTSTRAP_BOTS_JSON_FILE",
    "WECOM_BOT_AGENT_BACKEND",
    "WECOM_BOT_AGENT_COMMAND",
    "WECOM_BOT_AGENT_RUNTIME_ROOT",
    "WECOM_BOT_AGENT_RUN_AS_GROUP",
    "WECOM_BOT_AGENT_RUN_AS_USER",
    "WECOM_BOT_CONFIG_ID",
    "WECOM_BOT_ENABLED",
    "WECOM_BOT_GROUP_SESSION_MODE",
    "WECOM_BOT_ID",
    "WECOM_BOT_LOCAL_RESUME_OWNER_USER_ID",
    "WECOM_BOT_NAME",
    "WECOM_BOT_SECRET",
    "WECOM_BOT_SECRET_FILE",
    "WECOM_BOT_WELCOME",
    "WECOM_BOT_WORK_DIR",
    "WECOM_BOT_WORKSPACE_MODE",
    "WECOM_BOT_WORKSPACE_NAMESPACE",
    "WECOM_DISCONNECT_GRACE_MS",
    "WECOM_HEARTBEAT_ACK_TIMEOUT_MS",
    "WECOM_INBOUND_IDLE_TIMEOUT_MS",
    "WECOM_RAW_INBOUND_LOG_ENABLED",
    "WECOM_WORKSPACE_MODE",
    "WECOM_WORKSPACE_NAMESPACE",
    "WORK_DIR",
)

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def bridge_module(tmp_path, monkeypatch):
    for env_name in ISOLATED_ENV_VARS:
        monkeypatch.delenv(env_name, raising=False)

    shared_runtime_root = tmp_path / "shared-runtime"
    instance_runtime_root = tmp_path / "instance-runtime"
    codex_home = tmp_path / ".real-codex-home"
    local_file_send_root = tmp_path / "local-file-send"
    local_send_message_root = tmp_path / "local-send-message"
    monkeypatch.setenv("BOTS_FILE", str(tmp_path / ".bots.json"))
    monkeypatch.setenv("BRIDGE_SHARED_RUNTIME_ROOT", str(shared_runtime_root))
    monkeypatch.setenv("BRIDGE_RUNTIME_ROOT", str(instance_runtime_root))
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("LOCAL_FILE_SEND_QUEUE_ROOT", str(local_file_send_root))
    monkeypatch.setenv("LOCAL_SEND_MESSAGE_QUEUE_ROOT", str(local_send_message_root))

    spec = importlib.util.spec_from_file_location(f"bridge_test_{id(tmp_path)}", BRIDGE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    for env_name in (
        "BOTS_FILE",
        "BRIDGE_SHARED_RUNTIME_ROOT",
        "BRIDGE_RUNTIME_ROOT",
        "CODEX_HOME",
        "LOCAL_FILE_SEND_QUEUE_ROOT",
        "LOCAL_SEND_MESSAGE_QUEUE_ROOT",
    ):
        monkeypatch.delenv(env_name, raising=False)

    module.BASE_DIR = tmp_path
    module.SHARED_RUNTIME_ROOT = shared_runtime_root
    module.INSTANCE_RUNTIME_ROOT = instance_runtime_root
    module.DATA_FILE = tmp_path / ".bots.json"
    module.BOT_TOMBSTONE_ROOT = module.SHARED_RUNTIME_ROOT / ".bot-tombstones"
    module.BOT_RUNTIME_LOCK_ROOT = module.SHARED_RUNTIME_ROOT / ".bot-runtime-locks"
    module.SESSION_LOCK_ROOT = module.SHARED_RUNTIME_ROOT / ".session-locks"
    module.SESSION_REGISTRY_ROOT = module.SHARED_RUNTIME_ROOT / ".session-registry"
    module.CHATFILE_ROOT = module.INSTANCE_RUNTIME_ROOT / "chatfile"
    module.WORKSPACE_ROOT = module.INSTANCE_RUNTIME_ROOT / "workspace"
    module.BRIDGE_CODEX_HOME_ROOT = module.INSTANCE_RUNTIME_ROOT / ".bridge-codex-home"
    module.DEFAULT_CODEX_HOME = codex_home
    module.LOCAL_FILE_SEND_QUEUE_ROOT = local_file_send_root
    module.LOCAL_FILE_SEND_PENDING_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "pending"
    module.LOCAL_FILE_SEND_PROCESSING_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "processing"
    module.LOCAL_FILE_SEND_RESULT_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "results"
    module.LOCAL_FILE_SEND_DONE_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "done"
    module.LOCAL_FILE_SEND_FAILED_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "failed"
    module.LOCAL_SEND_MESSAGE_QUEUE_ROOT = local_send_message_root
    module.LOCAL_SEND_MESSAGE_PENDING_ROOT = module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "pending"
    module.LOCAL_SEND_MESSAGE_PROCESSING_ROOT = module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "processing"
    module.LOCAL_SEND_MESSAGE_RESULT_ROOT = module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "results"
    module.LOCAL_SEND_MESSAGE_DONE_ROOT = module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "done"
    module.LOCAL_SEND_MESSAGE_FAILED_ROOT = module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "failed"
    module.SCHEDULE_ROOT = module.SHARED_RUNTIME_ROOT / ".scheduled-messages"
    module.SCHEDULE_PENDING_ROOT = module.SCHEDULE_ROOT / "pending"
    module.SCHEDULE_PROCESSING_ROOT = module.SCHEDULE_ROOT / "processing"
    module.SCHEDULE_DONE_ROOT = module.SCHEDULE_ROOT / "done"
    module.SCHEDULE_FAILED_ROOT = module.SCHEDULE_ROOT / "failed"
    module.SCHEDULE_DEFINITION_ROOT = module.SCHEDULE_ROOT / "definitions"
    module.SCHEDULE_DEFINITION_LOCK_ROOT = module.SCHEDULE_ROOT / "definition-locks"
    module.USER_ALIAS_ROOT = module.SHARED_RUNTIME_ROOT / ".user-aliases"
    module.TEMPLATE_CARD_STATE_ROOT = module.SHARED_RUNTIME_ROOT / ".template-card-state"
    module.BOTS.clear()
    module.RECENT_EVENTS.clear()
    module.HTTP_SESSION = None
    module.SHUTDOWN_EVENT = asyncio.Event()
    module.LOCAL_FILE_SEND_QUEUE_BUSY = False
    module.CODEX_RUN_SEMAPHORE = asyncio.Semaphore(2)
    module.SCHEDULE_DEFINITION_LOCK_HANDLES.clear()
    module.PREPARED_PREVIOUS_BOT_CONFIGS.clear()
    module.EXTRA_FILE_ROOTS = []

    module.ensure_local_file_send_dirs()
    module.ensure_local_send_message_dirs()
    module.ensure_schedule_dirs()
    module.ensure_dir(module.BOT_TOMBSTONE_ROOT)
    module.ensure_dir(module.BOT_RUNTIME_LOCK_ROOT)
    module.ensure_dir(module.SESSION_LOCK_ROOT)
    module.ensure_dir(module.SESSION_REGISTRY_ROOT / "keys")
    module.ensure_dir(module.SESSION_REGISTRY_ROOT / "sessions")
    module.ensure_dir(module.CHATFILE_ROOT)
    module.ensure_dir(module.WORKSPACE_ROOT)
    module.ensure_dir(module.DEFAULT_CODEX_HOME)
    module.ensure_dir(module.USER_ALIAS_ROOT)
    module.ensure_dir(module.TEMPLATE_CARD_STATE_ROOT)
    module.write_json_atomic(module.DATA_FILE, [])
    return module
