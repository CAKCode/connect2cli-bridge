from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
BRIDGE_PATH = REPO_ROOT / "bridge.py"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def bridge_module(tmp_path):
    spec = importlib.util.spec_from_file_location(f"bridge_test_{id(tmp_path)}", BRIDGE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    module.BASE_DIR = tmp_path
    module.SHARED_RUNTIME_ROOT = tmp_path / "shared-runtime"
    module.INSTANCE_RUNTIME_ROOT = tmp_path / "instance-runtime"
    module.DATA_FILE = tmp_path / ".bots.json"
    module.BOT_TOMBSTONE_ROOT = module.SHARED_RUNTIME_ROOT / ".bot-tombstones"
    module.BOT_RUNTIME_LOCK_ROOT = module.SHARED_RUNTIME_ROOT / ".bot-runtime-locks"
    module.SESSION_LOCK_ROOT = module.SHARED_RUNTIME_ROOT / ".session-locks"
    module.SESSION_REGISTRY_ROOT = module.SHARED_RUNTIME_ROOT / ".session-registry"
    module.CHATFILE_ROOT = module.INSTANCE_RUNTIME_ROOT / "chatfile"
    module.WORKSPACE_ROOT = module.INSTANCE_RUNTIME_ROOT / "workspace"
    module.BRIDGE_CODEX_HOME_ROOT = module.INSTANCE_RUNTIME_ROOT / ".bridge-codex-home"
    module.BRIDGE_GLOBAL_SKILLS_ROOT = module.BRIDGE_CODEX_HOME_ROOT / "skills"
    module.PROJECT_SHARED_SKILLS_ROOT = tmp_path / "relate-skills"
    module.DEFAULT_CODEX_HOME = tmp_path / ".real-codex-home"
    module.LOCAL_FILE_SEND_QUEUE_ROOT = tmp_path / "local-file-send"
    module.LOCAL_FILE_SEND_PENDING_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "pending"
    module.LOCAL_FILE_SEND_PROCESSING_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "processing"
    module.LOCAL_FILE_SEND_RESULT_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "results"
    module.LOCAL_FILE_SEND_DONE_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "done"
    module.LOCAL_FILE_SEND_FAILED_ROOT = module.LOCAL_FILE_SEND_QUEUE_ROOT / "failed"
    module.SCHEDULE_ROOT = module.SHARED_RUNTIME_ROOT / ".scheduled-messages"
    module.SCHEDULE_PENDING_ROOT = module.SCHEDULE_ROOT / "pending"
    module.SCHEDULE_PROCESSING_ROOT = module.SCHEDULE_ROOT / "processing"
    module.SCHEDULE_DONE_ROOT = module.SCHEDULE_ROOT / "done"
    module.SCHEDULE_FAILED_ROOT = module.SCHEDULE_ROOT / "failed"
    module.SCHEDULE_DEFINITION_ROOT = module.SCHEDULE_ROOT / "definitions"
    module.SCHEDULE_DEFINITION_LOCK_ROOT = module.SCHEDULE_ROOT / "definition-locks"
    module.USER_ALIAS_ROOT = module.SHARED_RUNTIME_ROOT / ".user-aliases"
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
    module.ensure_schedule_dirs()
    module.ensure_dir(module.BOT_TOMBSTONE_ROOT)
    module.ensure_dir(module.BOT_RUNTIME_LOCK_ROOT)
    module.ensure_dir(module.SESSION_LOCK_ROOT)
    module.ensure_dir(module.SESSION_REGISTRY_ROOT / "keys")
    module.ensure_dir(module.SESSION_REGISTRY_ROOT / "sessions")
    module.ensure_dir(module.CHATFILE_ROOT)
    module.ensure_dir(module.WORKSPACE_ROOT)
    module.ensure_dir(module.DEFAULT_CODEX_HOME)
    module.ensure_dir(module.PROJECT_SHARED_SKILLS_ROOT)
    module.ensure_dir(module.USER_ALIAS_ROOT)
    module.write_json_atomic(module.DATA_FILE, [])
    return module
