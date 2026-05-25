from __future__ import annotations

import asyncio
import base64
import contextlib
import fcntl
import importlib.util
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
START_SH_PATH = REPO_ROOT / "start.sh"
WATCHDOG_SH_PATH = REPO_ROOT / "bridge_watchdog.sh"
BRIDGE_RUNTIME_CONFIG_PATH = REPO_ROOT / "bridge_runtime_config.py"
BRIDGE_ENV_SH_PATH = REPO_ROOT / "bridge_env.sh"


def load_runtime_config_module():
    spec = importlib.util.spec_from_file_location(
        f"bridge_runtime_config_test_{time.time_ns()}",
        BRIDGE_RUNTIME_CONFIG_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def make_bot(bridge_module, *, config_id="bot-1", name="codex1", remote_bot_id="bot-id", work_dir="/tmp"):
    bot = bridge_module.BotState(
        config={
            "id": config_id,
            "name": name,
            "botId": remote_bot_id,
            "secret": "secret",
            "workDir": work_dir,
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )
    bridge_module.BOTS[bot.config["id"]] = bot
    return bot


def make_session(bridge_module, bot, key="single:test-user"):
    record = bridge_module.create_session_record(bot, key)
    sess = bridge_module.SessionState(
        session_id=record["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, key)),
        lock_file=Path(record["lockFile"]),
        thread_id=record.get("threadId"),
    )
    bot.sessions[key] = sess
    bridge_module.acquire_session_lease(bot, sess, key)
    return sess


def write_secret_file(path: Path, value: str) -> Path:
    path.write_text(value, encoding="utf-8")
    return path


class FakeStdin:
    def __init__(self) -> None:
        self.buffer = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def test_create_session_record_does_not_persist_workdir(bridge_module):
    bot = make_bot(bridge_module)
    record = bridge_module.create_session_record(bot, "single:test-user")
    persisted = bridge_module.read_json_file(bridge_module.get_registry_session_file(record["sessionId"]), None)
    assert persisted is not None
    assert "workDir" not in persisted


def test_recycle_session_removes_idle_session_from_memory(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot, "single:test-user")

    bridge_module.recycle_session(bot, sess, "single:test-user")

    assert "single:test-user" not in bot.sessions


def test_watchdog_script_no_longer_uses_chunk_ack_queue():
    content = (REPO_ROOT / "bridge.py").read_text(encoding="utf-8")

    assert "chunk_ack_queue" not in content


def test_sanitize_all_session_records_ignores_invalid_records(bridge_module, capsys):
    invalid = bridge_module.SESSION_REGISTRY_ROOT / "sessions" / "bad.json"
    invalid.write_text('{"sessionId":"bad"}', encoding="utf-8")

    bridge_module.sanitize_all_session_records()

    captured = capsys.readouterr()
    assert "ignore invalid session record" in captured.out
    assert bridge_module.read_session_record_by_id("bad") is None


def test_create_session_record_recovers_from_stale_key_mapping(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": "broken", "botId": bot.config["id"], "chatKey": key}, ensure_ascii=False),
        encoding="utf-8",
    )
    bridge_module.get_registry_session_file("broken").write_text('{"sessionId":"broken"}', encoding="utf-8")

    record = bridge_module.create_session_record(bot, key)

    stored_key = bridge_module.read_json_file(key_file, None)
    assert record["sessionId"] != "broken"
    assert stored_key is not None
    assert stored_key["sessionId"] == record["sessionId"]


def test_create_session_record_repairs_invalid_key_file_without_recreating_existing_session(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    existing = bridge_module.create_session_record(bot, key)
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    key_file.write_text("{}", encoding="utf-8")

    repaired = bridge_module.create_session_record(bot, key)

    assert repaired["sessionId"] == existing["sessionId"]
    stored_key = bridge_module.read_json_file(key_file, None)
    assert stored_key is not None
    assert stored_key["sessionId"] == existing["sessionId"]


def test_read_session_record_by_key_repairs_invalid_key_file_without_recreating_existing_session(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    existing = bridge_module.create_session_record(bot, key)
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    key_file.write_text("{}", encoding="utf-8")

    repaired = bridge_module.read_session_record_by_key(bot.config["id"], key)

    assert repaired is not None
    assert repaired["sessionId"] == existing["sessionId"]
    stored_key = bridge_module.read_json_file(key_file, None)
    assert stored_key is not None
    assert stored_key["sessionId"] == existing["sessionId"]


def test_read_session_record_by_key_falls_back_to_alias_key(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_user_alias(bot.config["id"], "wo-user", "friendly-user")
    existing = bridge_module.create_session_record(bot, "single:friendly-user")

    repaired = bridge_module.read_session_record_by_key(bot.config["id"], "single:wo-user")

    assert repaired is not None
    assert repaired["sessionId"] == existing["sessionId"]
    raw_key_file = bridge_module.get_registry_key_file(bot.config["id"], "single:wo-user")
    stored_key = bridge_module.read_json_file(raw_key_file, None)
    assert stored_key is not None
    assert stored_key["sessionId"] == existing["sessionId"]


def test_get_or_create_session_uses_current_bot_workdir(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.CODEX_EXEC_MODE = "host"
    new_work_dir = str(bridge_module.BASE_DIR / "new-workdir")
    Path(new_work_dir).mkdir(parents=True, exist_ok=True)
    bot.config["workDir"] = new_work_dir
    sess = make_session(bridge_module, bot)

    reused = bridge_module.get_or_create_session(bot, "single:test-user")

    assert reused is sess
    assert reused.work_dir == str(bridge_module.get_workfile_dir(bot.config["id"], "test-user"))


def test_build_bridge_context_mentions_project_skill_dir(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo"
    work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bridge_module.CODEX_EXEC_MODE = "host"
    workspace_skills = bridge_module.get_workfile_dir(bot.config["id"], "test-user") / ".codex" / "skills" / "workspace-skill"
    workspace_skills.mkdir(parents=True, exist_ok=True)
    (workspace_skills / "SKILL.md").write_text("workspace", encoding="utf-8")
    shared_skill = bridge_module.PROJECT_SHARED_SKILLS_ROOT / "wecom-schedule-message"
    shared_skill.mkdir(parents=True, exist_ok=True)
    (shared_skill / "SKILL.md").write_text("shared", encoding="utf-8")
    sess = make_session(bridge_module, bot)

    context = bridge_module.build_bridge_context(bot, sess, "single:test-user")

    assert f"CWD_DIR: {bridge_module.get_workfile_dir(bot.config['id'], 'test-user')}" in context
    assert f"WORKSPACE_CODEX_SKILLS_DIR: {bridge_module.get_workfile_dir(bot.config['id'], 'test-user') / '.codex' / 'skills'}" in context
    assert "EXPORT_DIR:" in context
    assert "Create final exported files under EXPORT_DIR or CHATFILE_DIR" in context
    assert "Bridge sets TMPDIR/TMP/TEMP to CHATFILE_DIR" in context
    assert "Personal Codex skills should live in CWD_DIR/.codex/skills." in context
    assert "Project shared skills are injected into GLOBAL_CODEX_SKILLS_DIR by the bridge." in context


def test_start_sh_exports_runtime_tuning_envs():
    content = START_SH_PATH.read_text(encoding="utf-8")

    assert "load_bridge_runtime_env" in content
    assert "export_bridge_runtime_env" in content
    assert ".bridge.guard.pid" in content


def test_start_sh_cleans_watchdog_on_failed_start():
    content = START_SH_PATH.read_text(encoding="utf-8")

    assert 'if is_truthy "$BRIDGE_WATCHDOG_ENABLED" && [ -n "$GUARD_PID" ] && kill -0 "$GUARD_PID" 2>/dev/null; then' in content
    assert 'kill "$GUARD_PID" 2>/dev/null || true' in content
    assert 'rm -f "$GUARD_PID_FILE"' in content


def test_env_example_declares_watchdog_controls():
    content = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")

    assert "BRIDGE_WATCHDOG_ENABLED=true" in content
    assert "BRIDGE_WATCHDOG_POLL_SEC=5" in content
    assert "BRIDGE_WATCHDOG_FAIL_THRESHOLD=3" in content
    assert "BRIDGE_WATCHDOG_COOLDOWN_SEC=60" in content


def test_watchdog_script_exists_and_uses_health_probe():
    content = WATCHDOG_SH_PATH.read_text(encoding="utf-8")

    assert "export_bridge_runtime_env" in content
    assert "GET /" not in content
    assert "bridge_health_ok()" in content
    assert ".bridge.guard.pid" in content
    assert "restart triggered" in content
    assert "bridge recovered before restart; skip restart" in content
    assert 'load_bridge_runtime_env "$SCRIPT_DIR" || exit 1' in content.split("start_bridge_process() {", 1)[1]


async def test_main_starts_http_before_runtime_migration_and_bot_loading(bridge_module, monkeypatch):
    order = []

    class FakeClientSession:
        def __init__(self, **_kwargs) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    class FakeRunner:
        def __init__(self, app, access_log=None) -> None:
            self.app = app
            self.access_log = access_log

        async def setup(self) -> None:
            order.append("runner.setup")

        async def cleanup(self) -> None:
            order.append("runner.cleanup")
            await bridge_module.app_cleanup(self.app)

    class FakeSite:
        def __init__(self, runner, host, port) -> None:
            self.runner = runner
            self.host = host
            self.port = port

        async def start(self) -> None:
            order.append("site.start")

    async def wait_for_shutdown() -> None:
        await bridge_module.SHUTDOWN_EVENT.wait()

    def mark_migration(name: str):
        def inner() -> None:
            order.append(name)
        return inner

    async def fake_load_bots() -> None:
        order.append("load_bots")
        bridge_module.SHUTDOWN_EVENT.set()

    async def fake_process_schedule_definitions_once() -> None:
        order.append("process_schedule_definitions_once")

    async def fake_process_scheduled_messages_once() -> None:
        order.append("process_scheduled_messages_once")

    async def fake_remove_deleted_bots_from_memory_once() -> None:
        order.append("remove_deleted_bots_from_memory_once")

    monkeypatch.setattr(bridge_module.aiohttp, "ClientSession", FakeClientSession)
    monkeypatch.setattr(bridge_module.web, "AppRunner", FakeRunner)
    monkeypatch.setattr(bridge_module.web, "TCPSite", FakeSite)
    monkeypatch.setattr(bridge_module, "maybe_migrate_legacy_shared_runtime_state", mark_migration("migrate_shared"))
    monkeypatch.setattr(bridge_module, "maybe_migrate_legacy_instance_runtime_state", mark_migration("migrate_instance"))
    monkeypatch.setattr(bridge_module, "sync_project_shared_skills_to_bridge_global", mark_migration("sync_skills"))
    monkeypatch.setattr(bridge_module, "load_bots", fake_load_bots)
    monkeypatch.setattr(bridge_module, "process_schedule_definitions_once", fake_process_schedule_definitions_once)
    monkeypatch.setattr(bridge_module, "process_scheduled_messages_once", fake_process_scheduled_messages_once)
    monkeypatch.setattr(bridge_module, "remove_deleted_bots_from_memory_once", fake_remove_deleted_bots_from_memory_once)
    monkeypatch.setattr(bridge_module, "session_recycler_loop", wait_for_shutdown)
    monkeypatch.setattr(bridge_module, "lease_renew_loop", wait_for_shutdown)
    monkeypatch.setattr(bridge_module, "local_file_send_loop", wait_for_shutdown)
    monkeypatch.setattr(bridge_module, "schedule_definition_loop", wait_for_shutdown)
    monkeypatch.setattr(bridge_module, "scheduled_message_loop", wait_for_shutdown)
    monkeypatch.setattr(bridge_module, "paused_session_recovery_loop", wait_for_shutdown)
    monkeypatch.setattr(bridge_module, "bot_config_reconciler_loop", wait_for_shutdown)
    monkeypatch.setattr(bridge_module, "deleted_bot_reaper_loop", wait_for_shutdown)

    await bridge_module.main()

    assert order.index("site.start") < order.index("migrate_shared")
    assert order.index("site.start") < order.index("migrate_instance")
    assert order.index("site.start") < order.index("sync_skills")
    assert order.index("site.start") < order.index("load_bots")


def test_runtime_config_prefers_bridge_bind():
    module = load_runtime_config_module()

    host, port = module.resolve_host_port(
        {
            "BRIDGE_BIND": "0.0.0.0:19300",
            "BRIDGE_HOST": "127.0.0.1",
            "BRIDGE_PORT": "9299",
            "HOST": "localhost",
            "PORT": "9300",
        }
    )

    assert host == "0.0.0.0"
    assert port == 19300


def test_runtime_config_supports_ipv6_bridge_bind():
    module = load_runtime_config_module()

    host, port = module.resolve_host_port({"BRIDGE_BIND": "[::1]:19301"})

    assert host == "::1"
    assert port == 19301
    assert module.build_bridge_api_base(host, port) == "http://[::1]:19301"


def test_runtime_config_rejects_unbracketed_ipv6_bind():
    module = load_runtime_config_module()

    with pytest.raises(ValueError, match="IPv6"):
        module.resolve_host_port({"BRIDGE_BIND": "::1:19301"})


def test_bridge_env_load_uses_bridge_bind_from_dotenv(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / ".env").write_text("BRIDGE_BIND=127.0.0.1:19399\n", encoding="utf-8")

    result = subprocess.run(
        [
            "sh",
            "-c",
            'unset HOST PORT BRIDGE_BIND BRIDGE_HOST BRIDGE_PORT; . "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; printf "%s %s\\n" "$HOST" "$PORT"',
            "sh",
            str(script_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "127.0.0.1 19399"


def test_bridge_env_export_propagates_runtime_variables_to_child(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / ".env").write_text(
        "BRIDGE_BIND=127.0.0.1:19399\n"
        "WORK_DIR=/tmp/from-dotenv\n"
        "BRIDGE_TOKEN=token-from-dotenv\n"
        "LOCAL_FILE_SEND_POLL_MS=4321\n"
        "LOCAL_FILE_SEND_RESULT_RETENTION_MS=654321\n"
        "PROACTIVE_TEXT_MAX_CHARS=987\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; export_bridge_runtime_env; '
            'python3 -c "import os; print(os.getenv(\'WORK_DIR\')); print(os.getenv(\'BRIDGE_TOKEN\')); print(os.getenv(\'LOCAL_FILE_SEND_POLL_MS\')); print(os.getenv(\'LOCAL_FILE_SEND_RESULT_RETENTION_MS\')); print(os.getenv(\'PROACTIVE_TEXT_MAX_CHARS\'))"',
            "sh",
            str(script_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip().splitlines() == [
        "/tmp/from-dotenv",
        "token-from-dotenv",
        "4321",
        "654321",
        "987",
    ]


def test_start_bot_invalidates_persisted_threads_when_workdir_changes(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR / "old-workdir"),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "sess-1",
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "threadId": "thread-old",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", "sess-1")),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        }
    )
    bridge_module.write_session_record(record)

    asyncio.run(
        bridge_module.start_bot(
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        )
    )

    persisted = bridge_module.read_session_record_by_id("sess-1")
    assert persisted is not None
    assert persisted["threadId"] is None


def test_prepare_and_start_bot_invalidates_threads_when_bootstrap_workdir_changes(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    old_work_dir = bridge_module.BASE_DIR / "old-workdir"
    new_work_dir = bridge_module.BASE_DIR / "new-workdir"
    old_work_dir.mkdir(parents=True, exist_ok=True)
    new_work_dir.mkdir(parents=True, exist_ok=True)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(old_work_dir),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    bridge_module.write_session_record(
        {
            "sessionId": "sess-1",
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "threadId": "thread-old",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", "sess-1")),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        }
    )
    monkeypatch.setenv("WECOM_BOT_NAME", "codex1")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(new_work_dir))

    configs = bridge_module.prepare_bot_configs()
    asyncio.run(bridge_module.start_bot(configs[0]))

    persisted = bridge_module.read_session_record_by_id("sess-1")
    assert persisted is not None
    assert persisted["threadId"] is None


def test_prepare_bot_configs_rejects_duplicate_wecom_bot_ids(bridge_module):
    secret_a = write_secret_file(bridge_module.BASE_DIR / "a.secret", "secret-a\n")
    secret_b = write_secret_file(bridge_module.BASE_DIR / "b.secret", "secret-b\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "bot-a",
                "botId": "same-bot",
                "secretFile": str(secret_a),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            },
            {
                "id": "bot-2",
                "name": "bot-b",
                "botId": "same-bot",
                "secretFile": str(secret_b),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            },
        ],
    )

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.prepare_bot_configs()

    assert "duplicate botId" in excinfo.value.message


def test_require_api_access_accepts_basic_auth(bridge_module):
    bridge_module.BRIDGE_BASIC_AUTH = "czh:test"
    bridge_module.BRIDGE_TOKEN = ""
    header = "Basic " + base64.b64encode(b"czh:test").decode("ascii")
    request = SimpleNamespace(headers={"Authorization": header}, remote="114.236.137.79")

    result = asyncio.run(bridge_module.require_api_access(request))

    assert result is None


def test_require_api_access_rejects_missing_basic_auth(bridge_module):
    bridge_module.BRIDGE_BASIC_AUTH = "czh:test"
    bridge_module.BRIDGE_TOKEN = ""
    request = SimpleNamespace(headers={}, remote="114.236.137.79")

    result = asyncio.run(bridge_module.require_api_access(request))

    assert result is not None
    assert result.status == 401
    assert result.headers["WWW-Authenticate"] == 'Basic realm="WeCom Codex Bridge"'


def test_prepare_bot_configs_bootstraps_single_bot_from_env(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_GROUP_SESSION_MODE", "shared")

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == bridge_module.default_bot_config_id("bot-id")
    assert configs[0]["name"] == "env-bot"
    assert configs[0]["botId"] == "bot-id"
    assert configs[0]["secret"] == "secret"
    assert configs[0]["workDir"] == str(bridge_module.BASE_DIR)
    assert configs[0]["groupSessionMode"] == "shared"
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    expected = bridge_module.serialize_bot_config_for_disk(configs[0])
    for field in ("createdAt", "updatedAt"):
        assert isinstance(stored[0][field], int)
        expected[field] = stored[0][field]
    assert stored == [expected]
    assert "secret" not in stored[0]


def test_prepare_bot_configs_bootstraps_single_bot_from_secret_file(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret-from-file\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["secret"] == "secret-from-file"
    assert configs[0]["secretFile"] == str(secret_file)
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored[0]["secretFile"] == str(secret_file)
    assert "secret" not in stored[0]


def test_prepare_bot_configs_preserves_existing_id_for_same_bot_id(bridge_module, monkeypatch):
    old_secret_file = write_secret_file(bridge_module.BASE_DIR / "old.secret", "old-secret\n")
    new_secret_file = write_secret_file(bridge_module.BASE_DIR / "new.secret", "new-secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "existing-id",
                "name": "old-name",
                "botId": "bot-id",
                "secretFile": str(old_secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "shared",
                "enabled": False,
            }
        ],
    )
    monkeypatch.setenv("WECOM_BOT_NAME", "new-name")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(new_secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == "existing-id"
    assert configs[0]["name"] == "new-name"
    assert configs[0]["secret"] == "new-secret"
    assert configs[0]["secretFile"] == str(new_secret_file)
    assert configs[0]["enabled"] is True
    assert configs[0]["groupSessionMode"] == "per-user"
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored[0]["id"] == "existing-id"
    assert "secret" not in stored[0]


def test_prepare_bot_configs_switches_secret_source_without_stale_conflict(bridge_module, monkeypatch):
    old_secret_file = write_secret_file(bridge_module.BASE_DIR / "old.secret", "old-secret\n")
    new_secret_file = write_secret_file(bridge_module.BASE_DIR / "new.secret", "new-secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "existing-id",
                "name": "old-name",
                "botId": "bot-id",
                "secretFile": str(old_secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "shared",
                "enabled": True,
            }
        ],
    )
    monkeypatch.setenv("WECOM_BOT_NAME", "new-name")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(new_secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == "existing-id"
    assert configs[0]["secret"] == "new-secret"
    assert configs[0]["secretFile"] == str(new_secret_file)


def test_prepare_bot_configs_filters_tombstoned_persisted_bot(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")

    configs = bridge_module.prepare_bot_configs()

    assert configs == []
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_prepare_bot_configs_filters_tombstoned_env_bootstrap_bot(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_NAME", "default")
    monkeypatch.setenv("WECOM_BOT_CONFIG_ID", "bot-1")

    configs = bridge_module.prepare_bot_configs()

    assert configs == []
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_filter_deleted_bot_configs_does_not_persist_changes(bridge_module):
    payload = [
        {
            "id": "bot-1",
            "name": "default",
            "botId": "bot-id",
            "secretFile": str(write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")),
            "enabled": True,
        }
    ]
    bridge_module.write_json_atomic(bridge_module.DATA_FILE, [{"id": "keep-bot", "name": "keep"}])
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")

    filtered = bridge_module.filter_deleted_bot_configs(payload)

    assert filtered == []
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == [{"id": "keep-bot", "name": "keep"}]


def test_prepare_bot_configs_keeps_recreated_persisted_bot_newer_than_tombstone(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")
    deleted_at = bridge_module.bot_tombstone_deleted_at("bot-id")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
                "createdAt": deleted_at + 1,
                "updatedAt": deleted_at + 1,
            }
        ],
    )

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == "bot-1"


def test_prepare_bot_configs_rejects_plaintext_env_secret(bridge_module, monkeypatch):
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET", "secret")
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.prepare_bot_configs()

    assert "WECOM_BOT_SECRET is no longer supported" in excinfo.value.message


def test_prepare_bot_configs_filters_legacy_plaintext_secret(bridge_module, capsys):
    payload = [
        {
            "id": "legacy-id",
            "name": "legacy",
            "botId": "legacy-bot",
            "secret": "legacy-secret",
            "workDir": str(bridge_module.BASE_DIR),
            "welcome": "",
            "groupSessionMode": "per-user",
            "enabled": True,
        }
    ]
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        payload,
    )

    configs = bridge_module.prepare_bot_configs()

    assert configs == []
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == payload
    captured = capsys.readouterr()
    assert "ignore invalid bot config legacy-id" in captured.out


def test_prepare_bot_configs_skips_invalid_persisted_entries(bridge_module, capsys):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    payload = [
        {
            "id": "legacy-id",
            "name": "legacy",
            "botId": "legacy-bot",
            "secret": "legacy-secret",
            "workDir": str(bridge_module.BASE_DIR),
            "welcome": "",
            "groupSessionMode": "per-user",
            "enabled": True,
        },
        {
            "id": "bot-1",
            "name": "valid",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "welcome": "",
            "groupSessionMode": "per-user",
            "enabled": True,
        },
        "bad-entry",
    ]
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        payload,
    )

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == "bot-1"
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert isinstance(stored, list)
    assert len(stored) == 3
    assert stored[0] == payload[0]
    assert stored[2] == payload[2]
    assert stored[1]["id"] == "bot-1"
    assert stored[1]["secretFile"] == str(secret_file)
    assert isinstance(stored[1]["createdAt"], int)
    assert isinstance(stored[1]["updatedAt"], int)
    captured = capsys.readouterr()
    assert "ignore invalid bot config legacy-id" in captured.out
    assert "ignore invalid bot config at index 2" in captured.out


def test_save_bots_redacts_secret_but_keeps_secret_file(bridge_module):
    bot = bridge_module.BotState(
        config={
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secret": "secret",
            "secretFile": str(bridge_module.BASE_DIR / "bot.secret"),
            "workDir": "/tmp",
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )
    bridge_module.BOTS[bot.config["id"]] = bot

    bridge_module.save_bots()

    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    expected = {
        "id": "bot-1",
        "name": "codex1",
        "botId": "bot-id",
        "secretFile": str(bridge_module.BASE_DIR / "bot.secret"),
        "workDir": "/tmp",
        "enabled": True,
        "welcome": "",
        "groupSessionMode": "per-user",
    }
    assert isinstance(stored[0]["createdAt"], int)
    assert isinstance(stored[0]["updatedAt"], int)
    expected["createdAt"] = stored[0]["createdAt"]
    expected["updatedAt"] = stored[0]["updatedAt"]
    assert stored == [expected]
    assert bridge_module.get_persisted_bot_configs_lock_file().exists()


def test_save_bots_preserves_disabled_persisted_configs(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "disabled.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "disabled-bot",
                "name": "disabled",
                "botId": "disabled-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": False,
            }
        ],
    )

    bot = bridge_module.BotState(
        config={
            "id": "running-bot",
            "name": "running",
            "botId": "running-id",
            "secret": "secret",
            "secretFile": str(bridge_module.BASE_DIR / "running.secret"),
            "workDir": "/tmp",
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )
    bridge_module.BOTS[bot.config["id"]] = bot

    bridge_module.save_bots()

    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    ids = sorted(item["id"] for item in stored)
    assert ids == ["disabled-bot", "running-bot"]


def test_save_bots_does_not_restore_tombstoned_bot(bridge_module):
    bot = make_bot(bridge_module)
    bot.started_at_ms = bridge_module.now_ms() - 1000
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    bridge_module.save_bots()

    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored == []


def test_save_bots_preserves_generation_metadata_for_recreated_bot(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")
    deleted_at = bridge_module.bot_tombstone_deleted_at("bot-id")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
                "createdAt": deleted_at + 1,
                "updatedAt": deleted_at + 1,
            }
        ],
    )
    bot = bridge_module.BotState(
        config={
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secret": "secret",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "enabled": True,
            "welcome": "newer",
            "groupSessionMode": "per-user",
        }
    )
    bridge_module.BOTS[bot.config["id"]] = bot

    bridge_module.save_bots()

    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert isinstance(stored[0]["createdAt"], int)
    assert isinstance(stored[0]["updatedAt"], int)
    assert stored[0]["createdAt"] == deleted_at + 1
    assert stored[0]["updatedAt"] >= stored[0]["createdAt"]


def test_allowed_file_roots_do_not_include_workdir_by_default(bridge_module):
    bot = make_bot(bridge_module)
    bot.config["workDir"] = str(bridge_module.BASE_DIR)
    roots = bridge_module.get_allowed_file_roots(bot, "single:test-user")
    assert Path(bot.config["workDir"]).resolve() not in roots
    assert bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user").resolve() in roots


def test_acquire_and_release_bot_runtime_lock(bridge_module):
    bot = make_bot(bridge_module)

    assert bridge_module.acquire_bot_runtime_lock(bot) is True
    assert bot.runtime_lock_handle is not None

    bridge_module.release_bot_runtime_lock(bot)

    assert bot.runtime_lock_handle is None


def test_build_bridge_context_describes_file_send_roots(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    context = bridge_module.build_bridge_context(bot, sess, "single:test-user")

    assert "allowedFileSendRoots:" in context
    assert str(bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")) in context
    assert "WORKFILE_DIR:" in context
    assert "WORKDIR_DIR is the shared project root for code context" in context
    assert f"--bot-name '{bot.config['name']}'" in context


def test_get_session_workspace_paths_for_single_chat(bridge_module):
    bot = make_bot(bridge_module)

    paths = bridge_module.get_session_workspace_paths(bot, "single:test-user")

    assert paths["workDir"] == Path(bot.config["workDir"]).resolve()
    assert paths["chatfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "sessions" / "single_test-user" / "chatfile"
    assert paths["workfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "users" / "test-user" / "workfile"
    assert paths["roomfile"] is None


def test_get_session_workspace_paths_prefers_remembered_user_alias(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_user_alias(bot.config["id"], "woa8RJEQAAFXDjymuF_RYIhCS3k1hhnQ", "chenzihang5149")

    paths = bridge_module.get_session_workspace_paths(bot, "single:woa8RJEQAAFXDjymuF_RYIhCS3k1hhnQ")

    assert paths["workfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "users" / "chenzihang5149" / "workfile"


def test_get_session_workspace_paths_for_group_user_chat(bridge_module):
    bot = make_bot(bridge_module)

    paths = bridge_module.get_session_workspace_paths(bot, "group-user:group-1:user-a")

    assert paths["chatfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "sessions" / "group_user_group-1_user-a" / "chatfile"
    assert paths["workfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "users" / "user-a" / "workfile"
    assert paths["roomfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "rooms" / "group-1" / "roomfile"


def test_get_session_workspace_paths_for_group_shared_chat(bridge_module):
    bot = make_bot(bridge_module)

    paths = bridge_module.get_session_workspace_paths(bot, "group:group-1")

    assert paths["chatfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "sessions" / "group_group-1" / "chatfile"
    assert paths["workfile"] is None
    assert paths["roomfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "rooms" / "group-1" / "roomfile"


def test_get_session_runtime_cwd_uses_workfile_for_single_chat(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))

    runtime_cwd = bridge_module.get_session_runtime_cwd(bot, "single:test-user")

    assert runtime_cwd == bridge_module.get_workfile_dir(bot.config["id"], "test-user").resolve()


def test_ensure_session_workspace_dirs_bootstraps_workfile_from_workdir(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "tracked.txt").write_text("hello", encoding="utf-8")
    (work_dir / ".git").mkdir()
    (work_dir / "__pycache__").mkdir()
    (work_dir / "__pycache__" / "ignored.pyc").write_text("x", encoding="utf-8")
    bot = make_bot(bridge_module, work_dir=str(work_dir))

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    assert (paths["workfile"] / "tracked.txt").read_text(encoding="utf-8") == "hello"
    assert not (paths["workfile"] / ".git").exists()
    assert not (paths["workfile"] / "__pycache__").exists()


def test_ensure_session_workspace_dirs_bootstraps_non_git_project_tree(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "app.py").write_text("print('ok')\n", encoding="utf-8")
    (work_dir / "tests").mkdir()
    bot = make_bot(bridge_module, work_dir=str(work_dir))

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    assert (paths["workfile"] / "app.py").read_text(encoding="utf-8") == "print('ok')\n"
    assert (paths["workfile"] / "tests").is_dir()


def test_ensure_session_workspace_dirs_does_not_overwrite_existing_workfile(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "tracked.txt").write_text("source", encoding="utf-8")
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    workfile = bridge_module.get_workfile_dir(bot.config["id"], "test-user")
    workfile.mkdir(parents=True, exist_ok=True)
    (workfile / "tracked.txt").write_text("user", encoding="utf-8")

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    assert (paths["workfile"] / "tracked.txt").read_text(encoding="utf-8") == "user"


def test_ensure_session_workspace_dirs_writes_bootstrap_marker(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "app.py").write_text("print('source')\n", encoding="utf-8")
    bot = make_bot(bridge_module, work_dir=str(work_dir))

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    marker = paths["workfile"] / ".workspace-bootstrap.json"
    payload = json.loads(marker.read_text(encoding="utf-8"))
    assert payload["source"] == str(work_dir.resolve())


def test_get_session_runtime_cwd_uses_workfile_for_group_user_chat(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))

    runtime_cwd = bridge_module.get_session_runtime_cwd(bot, "group-user:group-1:user-a")

    assert runtime_cwd == bridge_module.get_workfile_dir(bot.config["id"], "user-a").resolve()


def test_get_session_runtime_cwd_uses_roomfile_for_group_shared_chat(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))

    runtime_cwd = bridge_module.get_session_runtime_cwd(bot, "group:group-1")

    assert runtime_cwd == bridge_module.get_roomfile_dir(bot.config["id"], "group-1").resolve()


def test_validate_file_for_upload_only_allows_chatfile_by_default(bridge_module):
    bot = make_bot(bridge_module)
    work_file = bridge_module.BASE_DIR / "artifact.txt"
    work_file.write_text("hello", encoding="utf-8")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.validate_file_for_upload(bot, "single:test-user", str(work_file))

    assert excinfo.value.status_code == 403
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    resolved = bridge_module.validate_file_for_upload(bot, "single:test-user", str(chat_file))

    assert resolved == chat_file.resolve()


def test_resolve_file_send_request_prefers_chat_key_over_stale_session(bridge_module):
    bot = make_bot(bridge_module)
    make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    resolved_bot, resolved_chat_key, resolved_file = bridge_module.resolve_file_send_request(
        {
            "filePath": str(chat_file),
            "chatKey": "single:test-user",
            "sessionId": "missing-session",
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert resolved_file == chat_file.resolve()


def test_resolve_file_send_request_falls_back_to_session_when_chat_key_invalid(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    resolved_bot, resolved_chat_key, resolved_file = bridge_module.resolve_file_send_request(
        {
            "filePath": str(chat_file),
            "chatKey": "single:missing",
            "sessionId": sess.session_id,
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert resolved_file == chat_file.resolve()


def test_resolve_file_send_request_honors_target_config_id(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_file_send_request(
            {
                "filePath": str(chat_file),
                "chatKey": "single:test-user",
                "targetConfigId": "bot-2",
            }
        )

    assert excinfo.value.status_code == 404
    assert excinfo.value.message == "bot not found: bot-2"


def test_resolve_file_send_request_allows_explicit_target_config_without_session_record(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    bot.ws = SimpleNamespace(closed=False)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    resolved_bot, resolved_chat_key, resolved_file = bridge_module.resolve_file_send_request(
        {
            "filePath": str(chat_file),
            "chatKey": "single:test-user",
            "targetConfigId": "bot-1",
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert resolved_file == chat_file.resolve()


def test_resolve_file_send_request_rejects_invalid_chat_key_for_explicit_target(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    bot.ws = SimpleNamespace(closed=False)
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_file_send_request(
            {
                "filePath": str(file_path.resolve()),
                "chatKey": "invalid-chat-key",
                "targetConfigId": "bot-1",
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "invalid chatKey: invalid-chat-key"


def test_resolve_file_send_request_preserves_session_error_after_chat_key_failure(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=True)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_file_send_request(
            {
                "filePath": str(chat_file),
                "chatKey": "single:missing",
                "sessionId": sess.session_id,
            }
        )

    assert excinfo.value.status_code == 503
    assert excinfo.value.message == "bot not connected"


def test_resolve_file_send_request_rejects_unloaded_target_bot_for_session(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [{"id": "bot-1", "name": bot.config["name"], "botId": bot.config["botId"], "enabled": True}],
    )
    bridge_module.BOTS.clear()

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_file_send_request(
            {
                "sessionId": sess.session_id,
                "filePath": str(file_path.resolve()),
            }
        )

    assert excinfo.value.status_code == 503
    assert excinfo.value.message == "bot not running: bot-1"


def test_resolve_loaded_target_bot_rejects_stale_bot(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="remote-bot-1")
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_loaded_target_bot(bot.config["id"], None)

    assert excinfo.value.status_code == 404
    assert excinfo.value.message == "bot not found: bot-1"


def test_require_unique_bot_for_chat_key_ignores_stale_bot(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="remote-bot-1")
    make_session(bridge_module, bot)
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.require_unique_bot_for_chat_key("single:test-user", None)

    assert excinfo.value.status_code == 404
    assert excinfo.value.message == "chatKey not found: single:test-user"


def test_resolve_schedule_target_prefers_chat_key_over_stale_session(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "sessionId": "missing-session",
        }
    )

    assert target["botId"] == bot.config["id"]
    assert target["botName"] == bot.config["name"]
    assert target["chatKey"] == "single:test-user"
    assert target["sessionId"] == sess.session_id


def test_resolve_schedule_target_falls_back_to_session_when_chat_key_invalid(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:missing",
            "sessionId": sess.session_id,
        }
    )

    assert target["botId"] == bot.config["id"]
    assert target["chatKey"] == "single:test-user"
    assert target["sessionId"] == sess.session_id


def test_resolve_schedule_target_honors_target_config_id(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "targetConfigId": "bot-1",
            "sessionId": "missing-session",
        }
    )

    assert target["botId"] == "bot-1"
    assert target["sessionId"] == sess.session_id

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_schedule_target(
            {
                "chatKey": "single:test-user",
                "targetConfigId": "bot-2",
            }
        )

    assert excinfo.value.status_code == 404
    assert excinfo.value.message == "bot not found: bot-2"


def test_resolve_schedule_target_allows_explicit_target_config_without_session_record(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "targetConfigId": "bot-1",
        }
    )

    assert target["botId"] == "bot-1"
    assert target["botName"] == bot.config["name"]
    assert target["chatKey"] == "single:test-user"
    assert target["sessionId"] is None


def test_resolve_schedule_target_rejects_invalid_chat_key_for_explicit_target(bridge_module):
    make_bot(bridge_module, config_id="bot-1")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_schedule_target(
            {
                "chatKey": "invalid-chat-key",
                "targetConfigId": "bot-1",
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "invalid chatKey: invalid-chat-key"


def test_resolve_schedule_target_preserves_session_error_after_chat_key_failure(bridge_module):
    make_bot(bridge_module)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_schedule_target(
            {
                "chatKey": "single:missing",
                "sessionId": "missing-session",
            }
        )

    assert excinfo.value.status_code == 404
    assert excinfo.value.message == "session not found: missing-session"


def test_submit_schedule_message_request_rejects_disabled_bot_for_session(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [{"id": "bot-1", "name": bot.config["name"], "botId": bot.config["botId"], "enabled": False}],
    )
    bridge_module.BOTS.clear()

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.submit_schedule_message_request(
            {
                "sessionId": sess.session_id,
                "delaySeconds": 60,
                "message": "check later",
            }
        )

    assert excinfo.value.status_code == 503
    assert excinfo.value.message == "bot disabled: bot-1"


def test_submit_schedule_message_request_rejects_unloaded_bot_for_session(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [{"id": "bot-1", "name": bot.config["name"], "botId": bot.config["botId"], "enabled": True}],
    )
    bridge_module.BOTS.clear()

    result = bridge_module.submit_schedule_message_request(
        {
            "sessionId": sess.session_id,
            "delaySeconds": 60,
            "message": "check later",
        }
    )

    stored = bridge_module.read_schedule_definition(result["scheduleId"])
    assert stored is not None
    assert stored["botId"] == "bot-1"
    assert stored["sessionId"] == sess.session_id
    assert stored["chatKey"] == bridge_module.read_session_record_by_id(sess.session_id)["chatKey"]


def test_normalize_bot_config_accepts_matching_secret_and_secret_file(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secret": "secret",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "enabled": True,
        }
    )

    assert config["secret"] == "secret"
    assert config["secretFile"] == str(secret_file)


def test_start_bot_does_not_stop_existing_bot_when_new_config_is_invalid(bridge_module):
    existing = make_bot(bridge_module)
    stop_calls = []

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True):
        stop_calls.append((bot_id, persist_disable))
        bridge_module.BOTS.pop(bot_id, None)

    bridge_module.stop_bot = fake_stop_bot

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        asyncio.run(
            bridge_module.start_bot(
                {
                    **existing.config,
                    "secret": "different-secret",
                    "secretFile": str(bridge_module.BASE_DIR / "missing.secret"),
                }
            )
        )

    assert "secretFile" in excinfo.value.message
    assert stop_calls == []
    assert bridge_module.BOTS[existing.config["id"]] is existing


def test_start_bot_rejects_conflicting_wecom_bot_id(bridge_module):
    make_bot(bridge_module, config_id="bot-1", remote_bot_id="same-bot")
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        asyncio.run(
            bridge_module.start_bot(
                {
                    "id": "bot-2",
                    "name": "codex2",
                    "botId": "same-bot",
                    "secretFile": str(secret_file),
                    "workDir": str(bridge_module.BASE_DIR),
                    "welcome": "",
                    "groupSessionMode": "per-user",
                    "enabled": True,
                }
            )
        )

    assert excinfo.value.status_code == 409
    assert "already managed by another config" in excinfo.value.message


def test_start_bot_allows_recreate_after_tombstone(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    recreated = asyncio.run(bridge_module.start_bot(bot.config))

    assert recreated.config["id"] == bot.config["id"]
    assert recreated.started_at_ms >= bridge_module.bot_tombstone_deleted_at(bot.config["botId"])


def test_interrupt_preserves_queue_but_keeps_lease_when_queue_exists(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.queue.append({"text": "next", "reqId": None})

    bridge_module.interrupt_session(bot, "single:test-user", sess, clear_thread=False, clear_chat=False, clear_queue=False)

    assert sess.queue == [{"text": "next", "reqId": None}]
    assert sess.lease_owned is True
    record = bridge_module.read_session_record_by_id(sess.session_id)
    assert record is not None
    assert record["status"] == "leased"
    assert record["ownerInstance"] == bridge_module.INSTANCE_ID


def test_reset_clears_queue_and_requeues_scheduled_jobs(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    scheduled = bridge_module.SCHEDULE_PROCESSING_ROOT / "job.json"
    bridge_module.write_json_atomic(
        scheduled,
        {
            "requestId": "job-1",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "do later",
            "runAt": bridge_module.now_ms() + 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms(),
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )
    sess.queue.append({"text": "do later", "reqId": None, "scheduledJobFile": str(scheduled)})

    bridge_module.interrupt_session(bot, "single:test-user", sess, clear_thread=True, clear_chat=True, clear_queue=True)

    assert sess.queue == []
    reset_job = bridge_module.read_json_file(bridge_module.SCHEDULE_PENDING_ROOT / "job.json", None)
    assert reset_job is not None
    assert reset_job["enqueuedAt"] is None
    assert reset_job["enqueuedByInstance"] is None


def test_reset_cancels_pending_file_sends_for_session(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    request_id = "file-job-1"
    processing_file = bridge_module.LOCAL_FILE_SEND_PROCESSING_ROOT / f"{request_id}.json"
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "filePath": "/tmp/a.txt",
            "requestedAt": bridge_module.now_ms(),
        },
    )
    bot.upload_queue.put_nowait(
        {
            "id": "queued-1",
            "chatKey": "single:test-user",
            "filePath": "/tmp/b.txt",
            "localRequestId": "queued-request",
            "localProcessingFile": str(bridge_module.LOCAL_FILE_SEND_PROCESSING_ROOT / "queued-request.json"),
        }
    )
    bridge_module.write_json_atomic(
        bridge_module.LOCAL_FILE_SEND_PROCESSING_ROOT / "queued-request.json",
        {
            "requestId": "queued-request",
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "filePath": "/tmp/b.txt",
            "requestedAt": bridge_module.now_ms(),
        },
    )
    bot.active_local_file_request_ids.add("queued-request")

    bridge_module.interrupt_session(bot, "single:test-user", sess, clear_thread=True, clear_chat=True, clear_queue=True)

    assert bot.upload_queue.qsize() == 0
    result_1 = bridge_module.read_json_file(bridge_module.LOCAL_FILE_SEND_RESULT_ROOT / f"{request_id}.json", None)
    result_2 = bridge_module.read_json_file(bridge_module.LOCAL_FILE_SEND_RESULT_ROOT / "queued-request.json", None)
    assert result_1 is not None and result_1["statusCode"] == 409
    assert result_2 is not None and result_2["statusCode"] == 409


@pytest.mark.asyncio
async def test_process_scheduled_messages_does_not_reenqueue_same_instance_processing_job(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-1",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms(),
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )

    called = 0

    async def fake_enqueue_message(*args, **kwargs):
        nonlocal called
        called += 1
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert called == 0
    assert processing.exists()


@pytest.mark.asyncio
async def test_process_scheduled_messages_retries_stale_processing_from_other_instance(bridge_module):
    bot = make_bot(bridge_module)
    make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-2",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms() - bridge_module.SCHEDULE_PROCESSING_RETRY_MS - 1000,
            "enqueuedByInstance": "other-instance",
        },
    )

    called = 0

    async def fake_enqueue_message(*args, **kwargs):
        nonlocal called
        called += 1
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert called == 1
    updated = bridge_module.read_json_file(processing, None)
    assert updated["enqueuedByInstance"] == bridge_module.INSTANCE_ID
    assert updated["enqueuedAt"] is not None


@pytest.mark.asyncio
async def test_process_scheduled_messages_retries_stale_processing_from_same_instance(bridge_module):
    bot = make_bot(bridge_module)
    make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-same.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-same",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms() - bridge_module.SCHEDULE_PROCESSING_RETRY_MS - 1000,
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )

    called = 0

    async def fake_enqueue_message(*args, **kwargs):
        nonlocal called
        called += 1
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert called == 1
    updated = bridge_module.read_json_file(processing, None)
    assert updated["enqueuedByInstance"] == bridge_module.INSTANCE_ID
    assert updated["enqueuedAt"] is not None


@pytest.mark.asyncio
async def test_process_scheduled_messages_does_not_reenqueue_job_already_in_session_queue(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-queued.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-queued",
            "scheduleId": "sched-queued",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms() - bridge_module.SCHEDULE_PROCESSING_RETRY_MS - 1000,
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )
    sess.queue.append(
        {
            "text": "scheduled text",
            "reqId": None,
            "scheduledJobFile": str(processing),
            "scheduleId": "sched-queued",
            "scheduleRequestId": "job-queued",
        }
    )

    called = 0

    async def fake_enqueue_message(*args, **kwargs):
        nonlocal called
        called += 1
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert called == 0
    updated = bridge_module.read_json_file(processing, None)
    assert updated["enqueuedByInstance"] == bridge_module.INSTANCE_ID
    assert updated["enqueuedAt"] > 0


@pytest.mark.asyncio
async def test_process_scheduled_messages_does_not_reenqueue_job_already_running_locally(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-running-local.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-running-local",
            "scheduleId": "sched-running-local",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms() - bridge_module.SCHEDULE_PROCESSING_RETRY_MS - 1000,
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )
    sess.running = True
    sess.active_schedule_id = "sched-running-local"
    sess.active_scheduled_job_file = str(processing)
    sess.active_schedule_request_id = "job-running-local"

    called = 0

    async def fake_enqueue_message(*args, **kwargs):
        nonlocal called
        called += 1
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert called == 0
    updated = bridge_module.read_json_file(processing, None)
    assert updated["enqueuedByInstance"] == bridge_module.INSTANCE_ID
    assert updated["enqueuedAt"] > 0


@pytest.mark.asyncio
async def test_process_scheduled_messages_skips_dispatch_until_bot_running(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "connecting"
    bot.ws = SimpleNamespace(closed=True)
    sess = make_session(bridge_module, bot)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-wait-bot.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-wait-bot",
            "scheduleId": "sched-wait-bot",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms() - bridge_module.SCHEDULE_PROCESSING_RETRY_MS - 1000,
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )

    called = 0

    async def fake_enqueue_message(*args, **kwargs):
        nonlocal called
        called += 1
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert called == 0
    updated = bridge_module.read_json_file(processing, None)
    assert updated["enqueuedAt"] < bridge_module.now_ms()


@pytest.mark.asyncio
async def test_bridge_interrupt_command_is_intercepted(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.queue.append({"text": "queued", "reqId": None})

    replies = []
    resumed = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    def fake_process_queue(_bot, _sess, key):
        resumed.append(key)

    bridge_module.respond_info = fake_respond_info
    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgtype": "text",
            "text": {"content": "/bridge-interrupt"},
            "from": {"userid": "test-user"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert replies == [("req-1", "Current task interrupted.")]
    assert resumed == ["single:test-user"]
    assert sess.queue == [{"text": "queued", "reqId": None}]


@pytest.mark.asyncio
async def test_resume_command_lists_candidates_for_current_chat_scope(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    bridge_module.update_session_record(current.session_id, lambda record: {**record, "threadId": "thread-current", "lastRunAt": bridge_module.now_ms()})
    other_visible = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        other_visible["sessionId"],
        lambda record: {**record, "threadId": "thread-old", "lastRunAt": bridge_module.now_ms() - 1000},
    )
    other_user = bridge_module.create_session_record(bot, "single:someone-else")
    bridge_module.update_session_record(
        other_user["sessionId"],
        lambda record: {**record, "threadId": "thread-hidden", "lastRunAt": bridge_module.now_ms() - 2000},
    )

    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-resume"},
        "body": {
            "msgtype": "text",
            "text": {"content": "/bridge-resume"},
            "from": {"userid": "test-user"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert replies
    content = replies[0][1]
    assert "可恢复会话" in content
    assert current.session_id in content
    assert other_visible["sessionId"] in content
    assert other_user["sessionId"] not in content
    assert len(current.resume_candidates) == 2


@pytest.mark.asyncio
async def test_resume_command_binds_selected_thread_to_current_chat(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {**record, "threadId": "thread-target", "lastRunAt": bridge_module.now_ms()},
    )

    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-resume"},
            "body": {"msgtype": "text", "text": {"content": "/bridge-resume"}, "from": {"userid": "test-user"}},
        },
    )
    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-select"},
            "body": {"msgtype": "text", "text": {"content": "1"}, "from": {"userid": "test-user"}},
        },
    )

    assert current.thread_id == "thread-target"
    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert persisted["threadId"] == "thread-target"
    assert current.resume_candidates == []
    assert "已选择会话" in replies[-1][1]


@pytest.mark.asyncio
async def test_resume_selection_invalid_choice_is_reported(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    bridge_module.update_session_record(current.session_id, lambda record: {**record, "threadId": "thread-current", "lastRunAt": bridge_module.now_ms()})

    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-resume"},
            "body": {"msgtype": "text", "text": {"content": "/bridge-resume"}, "from": {"userid": "test-user"}},
        },
    )
    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-select"},
            "body": {"msgtype": "text", "text": {"content": "99"}, "from": {"userid": "test-user"}},
        },
    )

    assert "无效选择" in replies[-1][1]
    assert current.resume_candidates


@pytest.mark.asyncio
async def test_bridge_status_counts_processing_and_pending_jobs(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.queue.append({"text": "queued", "reqId": None})
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-pending.json"
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-processing.json"
    for path in (pending, processing):
        bridge_module.write_json_atomic(
            path,
            {
                "requestId": path.stem,
                "botId": bot.config["id"],
                "chatKey": "single:test-user",
                "message": "scheduled text",
                "runAt": bridge_module.now_ms() + 1000,
                "createdAt": bridge_module.now_ms(),
            },
        )

    replies = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    bridge_module.respond_info = fake_respond_info
    await bridge_module.status_session_command(bot, "single:test-user", "req-2")

    assert replies
    assert "scheduled=2" in replies[0][1]


@pytest.mark.asyncio
async def test_bridge_status_ignores_jobs_from_other_bots_with_same_chat_key(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", name="bot-a", remote_bot_id="bot-a")
    other_bot = make_bot(bridge_module, config_id="bot-2", name="bot-b", remote_bot_id="bot-b")
    make_session(bridge_module, bot)
    make_session(bridge_module, other_bot)
    bridge_module.write_json_atomic(
        bridge_module.SCHEDULE_PENDING_ROOT / "job-a.json",
        {
            "requestId": "job-a",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() + 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    bridge_module.write_json_atomic(
        bridge_module.SCHEDULE_PENDING_ROOT / "job-b.json",
        {
            "requestId": "job-b",
            "botId": other_bot.config["id"],
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() + 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    replies = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    bridge_module.respond_info = fake_respond_info
    await bridge_module.status_session_command(bot, "single:test-user", "req-2")

    assert replies
    assert "scheduled=1" in replies[0][1]


@pytest.mark.asyncio
async def test_send_or_store_session_payload_refreshes_req_id_on_retry(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    payload = {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": "original-req"},
        "body": {"chatid": "test-user", "chat_type": 1, "msgtype": "markdown", "markdown": {"content": "hello"}},
    }

    await bridge_module.send_or_store_session_payload(bot, "single:test-user", sess, payload, True)
    first_req_id = sess.pending_final_payload["headers"]["req_id"]
    assert first_req_id != "original-req"

    await bridge_module.send_or_store_session_payload(bot, "single:test-user", sess, payload, True)
    second_req_id = sess.pending_final_payload["headers"]["req_id"]
    assert second_req_id != first_req_id


@pytest.mark.asyncio
async def test_send_ws_payload_with_ack_cleans_future_on_send_error(bridge_module):
    class BrokenWS:
        closed = False

        async def send_json(self, payload):
            raise RuntimeError("boom")

    bot = make_bot(bridge_module)
    bot.ws = BrokenWS()

    with pytest.raises(RuntimeError):
        await bridge_module.send_ws_payload_with_ack(
            bot,
            {"cmd": "aibot_send_msg", "headers": {"req_id": "req-ack"}, "body": {}},
            1,
        )

    assert "req-ack" not in bot.pending_requests


@pytest.mark.asyncio
async def test_process_scheduled_messages_moves_due_job_to_processing_without_done(bridge_module):
    bot = make_bot(bridge_module)
    make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-3",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    async def fake_enqueue_message(*args, **kwargs):
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job.json"
    done = bridge_module.SCHEDULE_DONE_ROOT / "job.json"
    assert processing.exists()
    assert not done.exists()
    job = bridge_module.read_json_file(processing, None)
    assert job["enqueuedByInstance"] == bridge_module.INSTANCE_ID
    assert job["enqueuedAt"] is not None


@pytest.mark.asyncio
async def test_process_scheduled_messages_fails_orphaned_job_for_missing_bot(bridge_module):
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-4",
            "botId": "missing-bot",
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - bridge_module.SCHEDULE_ORPHAN_TTL_MS - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms() - bridge_module.SCHEDULE_PROCESSING_RETRY_MS - 1000,
            "enqueuedByInstance": "other-instance",
        },
    )

    await bridge_module.process_scheduled_messages_once()

    failed = bridge_module.SCHEDULE_FAILED_ROOT / "job.json"
    assert failed.exists()
    assert not processing.exists()


def test_process_scheduled_reminder_message_is_reenqueued_into_session(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-5",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "提醒我：scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    calls = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        calls.append((key, text, kwargs))
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    asyncio.run(bridge_module.process_scheduled_messages_once())

    assert calls == [
        (
            "single:test-user",
            "提醒我：scheduled text",
                {
                    "silent_lease_failure": True,
                    "scheduled_job_file": str(processing),
                    "schedule_id": None,
                    "schedule_request_id": "job-5",
                },
            )
        ]
    job = bridge_module.read_json_file(processing, None)
    assert job["enqueuedByInstance"] == bridge_module.INSTANCE_ID
    assert job["enqueuedAt"] is not None


def test_process_scheduled_reminder_message_marks_done_after_enqueue(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-reminder-retry.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-5b",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "提醒我：scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms() - bridge_module.SCHEDULE_PROCESSING_RETRY_MS - 1000,
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )

    called = 0

    async def fake_enqueue_message(*args, **kwargs):
        nonlocal called
        called += 1
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    asyncio.run(bridge_module.process_scheduled_messages_once())

    assert called == 1
    assert processing.exists()
    assert not (bridge_module.SCHEDULE_DONE_ROOT / "job-reminder-retry.json").exists()


@pytest.mark.asyncio
async def test_bridge_command_bridge_error_is_reported_to_user(bridge_module):
    bot = make_bot(bridge_module)
    make_session(bridge_module, bot)
    replies = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    def raise_error(*args, **kwargs):
        raise bridge_module.BridgeError(409, "session is owned by another instance: single:test-user")

    bridge_module.respond_info = fake_respond_info
    bridge_module.interrupt_session_command = raise_error

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-3"},
        "body": {
            "msgtype": "text",
            "text": {"content": "/bridge-interrupt"},
            "from": {"userid": "test-user"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert replies == [("req-3", "Bridge command failed: session is owned by another instance: single:test-user")]


def test_create_cron_schedule_definition_uses_timezone(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    created_at = int(datetime(2026, 4, 17, 0, 30, tzinfo=timezone.utc).timestamp() * 1000)

    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "0 9 * * *",
            "timezone": "Asia/Shanghai",
        },
        created_at_ms=created_at,
    )

    expected = int(datetime(2026, 4, 17, 1, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert definition["mode"] == "cron"
    assert definition["nextRunAt"] == expected


def test_chat_key_for_bot_group_mode_per_user_by_default(bridge_module):
    bot = make_bot(bridge_module)
    key_a = bridge_module.chat_key_for_bot(
        bot,
        {
            "body": {
                "chattype": "group",
                "chatid": "group-1",
                "from": {"userid": "user-a"},
            }
        },
    )
    key_b = bridge_module.chat_key_for_bot(
        bot,
        {
            "body": {
                "chattype": "group",
                "chatid": "group-1",
                "from": {"userid": "user-b"},
            }
        },
    )
    assert key_a == "group-user:group-1:user-a"
    assert key_b == "group-user:group-1:user-b"


def test_chat_key_for_bot_prefers_existing_user_alias(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_user_alias(bot.config["id"], "woa8RJEQAAFXDjymuF_RYIhCS3k1hhnQ", "chenzihang5149")

    key = bridge_module.chat_key_for_bot(
        bot,
        {
            "body": {
                "from": {"userid": "woa8RJEQAAFXDjymuF_RYIhCS3k1hhnQ"},
            }
        },
    )

    assert key == "single:woa8RJEQAAFXDjymuF_RYIhCS3k1hhnQ"
    assert bridge_module.read_user_alias(bot.config["id"], "woa8RJEQAAFXDjymuF_RYIhCS3k1hhnQ") == "chenzihang5149"


def test_chat_key_for_bot_group_mode_per_user_isolates_sender(bridge_module):
    bot = make_bot(bridge_module)
    bot.config["groupSessionMode"] = "per-user"

    key_a = bridge_module.chat_key_for_bot(
        bot,
        {
            "body": {
                "chattype": "group",
                "chatid": "group-1",
                "from": {"userid": "user-a"},
            }
        },
    )
    key_b = bridge_module.chat_key_for_bot(
        bot,
        {
            "body": {
                "chattype": "group",
                "chatid": "group-1",
                "from": {"userid": "user-b"},
            }
        },
    )

    assert key_a == "group-user:group-1:user-a"
    assert key_b == "group-user:group-1:user-b"


def test_chat_key_for_bot_group_mode_shared_preserves_legacy_behavior(bridge_module):
    bot = make_bot(bridge_module)
    bot.config["groupSessionMode"] = "shared"
    key = bridge_module.chat_key_for_bot(
        bot,
        {
            "body": {
                "chattype": "group",
                "chatid": "group-1",
                "from": {"userid": "user-a"},
            }
        },
    )
    assert key == "group:group-1"


@pytest.mark.asyncio
async def test_handle_group_mentions_uses_per_user_session_keys(bridge_module):
    bot = make_bot(bridge_module, name="robot")
    bot.config["groupSessionMode"] = "per-user"
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload_a = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-a"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@robot hello from a"},
            "from": {"userid": "user-a"},
        },
    }
    payload_b = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-b"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@robot hello from b"},
            "from": {"userid": "user-b"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload_a)
    await bridge_module.handle_wecom_message(bot, payload_b)

    assert captured == [
        ("group-user:group-1:user-a", "hello from a", "req-a"),
        ("group-user:group-1:user-b", "hello from b", "req-b"),
    ]


@pytest.mark.asyncio
async def test_handle_group_mentions_supports_bot_name_with_spaces(bridge_module):
    bot = make_bot(bridge_module, name="Leo C")
    bot.config["groupSessionMode"] = "per-user"
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-space-name"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@Leo C /bridge-interrupt"},
            "from": {"userid": "user-a"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert captured == []


@pytest.mark.asyncio
async def test_handle_group_message_preserves_bot_name_mention_in_body_text(bridge_module):
    bot = make_bot(bridge_module, name="Leo C")
    bot.config["groupSessionMode"] = "per-user"
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-body-mention"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@Leo C 请分析这句话里的 @Leo C 是否会被保留"},
            "from": {"userid": "user-a"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert captured == [
        ("group-user:group-1:user-a", "请分析这句话里的 @Leo C 是否会被保留", "req-body-mention"),
    ]


@pytest.mark.asyncio
async def test_handle_group_message_does_not_strip_similar_prefix_mention(bridge_module):
    bot = make_bot(bridge_module, name="bot")
    bot.config["groupSessionMode"] = "per-user"
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-similar-mention"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@bot2 hello"},
            "from": {"userid": "user-a"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert captured == [
        ("group-user:group-1:user-a", "@bot2 hello", "req-similar-mention"),
    ]


@pytest.mark.asyncio
async def test_handle_group_message_strips_bot_mention_after_other_mentions(bridge_module):
    bot = make_bot(bridge_module, name="Leo C")
    bot.config["groupSessionMode"] = "per-user"
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-other-mentions"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@alice @Leo C hello"},
            "from": {"userid": "user-a"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert captured == [
        ("group-user:group-1:user-a", "hello", "req-other-mentions"),
    ]


@pytest.mark.asyncio
async def test_handle_group_message_strips_bot_mention_after_spaced_name_mentions(bridge_module):
    bot = make_bot(bridge_module, name="Leo C")
    bot.config["groupSessionMode"] = "per-user"
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-spaced-name-mentions"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@Alice Bob @Leo C hello"},
            "from": {"userid": "user-a"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert captured == [
        ("group-user:group-1:user-a", "hello", "req-spaced-name-mentions"),
    ]


@pytest.mark.asyncio
async def test_handle_group_message_strips_bot_mention_with_punctuation(bridge_module):
    bot = make_bot(bridge_module, name="robot")
    bot.config["groupSessionMode"] = "per-user"
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payloads = [
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-comma"},
            "body": {
                "msgtype": "text",
                "chattype": "group",
                "chatid": "group-1",
                "text": {"content": "@robot, hello"},
                "from": {"userid": "user-a"},
            },
        },
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-colon"},
            "body": {
                "msgtype": "text",
                "chattype": "group",
                "chatid": "group-1",
                "text": {"content": "@robot: hello"},
                "from": {"userid": "user-a"},
            },
        },
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-cn-colon"},
            "body": {
                "msgtype": "text",
                "chattype": "group",
                "chatid": "group-1",
                "text": {"content": "@robot：hello"},
                "from": {"userid": "user-a"},
            },
        },
    ]

    for payload in payloads:
        await bridge_module.handle_wecom_message(bot, payload)

    assert captured == [
        ("group-user:group-1:user-a", "hello", "req-comma"),
        ("group-user:group-1:user-a", "hello", "req-colon"),
        ("group-user:group-1:user-a", "hello", "req-cn-colon"),
    ]


@pytest.mark.asyncio
async def test_handle_text_message_keeps_ssh_url_with_at_sign(bridge_module):
    bot = make_bot(bridge_module)
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-ssh"},
        "body": {
            "msgtype": "text",
            "text": {"content": "ssh://git@git.agoralab.co/qa/streaming-test.git"},
            "from": {"userid": "test-user"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert captured == [
        ("single:test-user", "ssh://git@git.agoralab.co/qa/streaming-test.git", "req-ssh"),
    ]


@pytest.mark.asyncio
async def test_handle_text_message_keeps_space_after_at_sign(bridge_module):
    bot = make_bot(bridge_module)
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-space"},
        "body": {
            "msgtype": "text",
            "text": {"content": "ssh://git@ git.agoralab.co/qa/streaming-test.git"},
            "from": {"userid": "test-user"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert captured == [
        ("single:test-user", "ssh://git@ git.agoralab.co/qa/streaming-test.git", "req-space"),
    ]


def test_chat_key_to_send_target_preserves_group_for_per_user_mode(bridge_module):
    chat_type, chat_id = bridge_module.chat_key_to_send_target("group-user:group-1:user-a")
    assert chat_type == 2
    assert chat_id == "group-1"


def test_build_proactive_chat_payload_mentions_group_user_by_default(bridge_module):
    payload = bridge_module.build_proactive_chat_payload("group-user:group-1:user-a", "hello")

    assert payload["body"]["chat_type"] == 2
    assert payload["body"]["chatid"] == "group-1"
    assert payload["body"]["markdown"]["content"] == "<@user-a>\nhello"


def test_build_proactive_chat_payload_prefers_explicit_mention_user(bridge_module):
    payload = bridge_module.build_proactive_chat_payload("group-user:group-1:user-a", "hello")

    assert payload["body"]["markdown"]["content"] == "<@user-a>\nhello"


def test_build_session_text_payload_mentions_group_user_once_for_direct_reply(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot, "group-user:group-1:user-a")
    bridge_module.register_reply_session(bot, "req-1", sess)

    first = bridge_module.build_session_text_payload("group-user:group-1:user-a", sess, "req-1", "hello", False)
    bridge_module.mark_session_reply_sent(bot, sess, first)
    second = bridge_module.build_session_text_payload("group-user:group-1:user-a", sess, "req-1", "world", False)

    assert first["body"]["stream"]["content"] == "<@user-a>\nhello"
    assert second["body"]["stream"]["content"] == "world"


@pytest.mark.asyncio
async def test_process_schedule_definitions_creates_pending_job_and_advances_cron(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_000_000
    created_at = current_now - 70_000

    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "poll metrics",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=created_at,
    )
    bridge_module.write_schedule_definition(definition)
    bridge_module.now_ms = lambda: current_now

    await bridge_module.process_schedule_definitions_once()

    pending_files = sorted(bridge_module.SCHEDULE_PENDING_ROOT.glob("*.json"))
    assert len(pending_files) == 1
    job = bridge_module.read_json_file(pending_files[0], None)
    assert job["scheduleId"] == definition["scheduleId"]
    stored = bridge_module.read_schedule_definition(definition["scheduleId"])
    assert stored is not None
    assert stored["runCount"] == 1
    assert stored["nextRunAt"] == current_now + 60_000


@pytest.mark.asyncio
async def test_process_schedule_definitions_skips_overlap_for_default_policy(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_100_000
    created_at = current_now - 70_000

    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "poll metrics",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=created_at,
    )
    bridge_module.write_schedule_definition(definition)
    bridge_module.now_ms = lambda: current_now
    bridge_module.write_json_atomic(
        bridge_module.SCHEDULE_PENDING_ROOT / "existing.json",
        {
            "requestId": "existing",
            "scheduleId": definition["scheduleId"],
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "poll metrics",
            "runAt": current_now - 5_000,
            "createdAt": current_now - 5_000,
        },
    )

    await bridge_module.process_schedule_definitions_once()

    pending_files = sorted(bridge_module.SCHEDULE_PENDING_ROOT.glob("*.json"))
    assert len(pending_files) == 1
    stored = bridge_module.read_schedule_definition(definition["scheduleId"])
    assert stored is not None
    assert stored["runCount"] == 0
    assert stored["nextRunAt"] == bridge_module.compute_next_cron_run_on_or_after("* * * * *", "UTC", current_now + 1)


def test_pause_resume_delete_schedule_definition(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_200_000
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=current_now - 70_000,
    )
    bridge_module.write_schedule_definition(definition)
    bridge_module.now_ms = lambda: current_now

    paused = bridge_module.pause_schedule_definition(definition["scheduleId"])
    assert paused["enabled"] is False

    resumed = bridge_module.resume_schedule_definition(definition["scheduleId"])
    assert resumed["enabled"] is True
    assert resumed["nextRunAt"] > current_now

    bridge_module.delete_schedule_definition(definition["scheduleId"])
    assert bridge_module.read_schedule_definition(definition["scheduleId"]) is None


def test_schedule_message_request_creates_one_shot_cron_definition(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_300_000
    requested_run_at = current_now + 30_000
    bridge_module.now_ms = lambda: current_now

    result = bridge_module.submit_schedule_message_request(
        {
            "sessionId": sess.session_id,
            "runAt": requested_run_at,
            "message": "one-shot",
        },
        "test",
    )

    schedule_id = result["scheduleId"]
    stored = bridge_module.read_schedule_definition(schedule_id)
    assert stored is not None
    assert stored["mode"] == "cron"
    assert stored["maxRuns"] == 1
    assert stored["runCount"] == 0
    assert stored["nextRunAt"] == current_now + 60_000
    assert result["requestedRunAt"] == requested_run_at
    assert result["runAt"] == current_now + 60_000


def test_submit_schedule_definition_request_rejects_duplicate_schedule_id(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_300_000
    bridge_module.now_ms = lambda: current_now
    payload = {
        "sessionId": sess.session_id,
        "scheduleId": "sched-1",
        "cron": "* * * * *",
        "timezone": "UTC",
        "message": "daily summary",
    }

    first = bridge_module.submit_schedule_definition_request(payload, "test")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.submit_schedule_definition_request(payload, "test")

    assert first["scheduleId"] == "sched-1"
    assert excinfo.value.status_code == 409
    assert "scheduleId already exists" in excinfo.value.message


def test_maybe_cleanup_schedule_definition_removes_finished_one_shot(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_360_000
    bridge_module.now_ms = lambda: current_now

    result = bridge_module.submit_schedule_message_request(
        {
            "sessionId": sess.session_id,
            "runAt": current_now + 30_000,
            "message": "one-shot",
        },
        "test",
    )
    schedule_id = result["scheduleId"]
    bridge_module.update_schedule_definition(
        schedule_id,
        lambda current: {**current, "enabled": False, "runCount": 1, "nextRunAt": None},
    )

    bridge_module.maybe_cleanup_schedule_definition(schedule_id)

    assert bridge_module.read_schedule_definition(schedule_id) is None


def test_is_schedule_job_running_uses_session_record(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_420_000
    bridge_module.now_ms = lambda: current_now
    bridge_module.update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "status": "running",
            "activeScheduleId": "sch-1",
            "leaseExpiresAt": current_now + 30_000,
        },
    )

    assert bridge_module.is_schedule_job_running(
        {
            "sessionId": sess.session_id,
            "scheduleId": "sch-1",
        },
        current_now,
    )


def test_pause_schedule_definition_requires_lock(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    bridge_module.acquire_schedule_definition_lock = lambda schedule_id: False

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.pause_schedule_definition(definition["scheduleId"])

    assert excinfo.value.status_code == 409


@pytest.mark.asyncio
async def test_schedule_definition_lock_blocks_other_task_in_same_process(bridge_module):
    assert bridge_module.acquire_schedule_definition_lock("sch-lock")
    assert bridge_module.acquire_schedule_definition_lock("sch-lock")

    result = {}

    async def contender():
        result["ok"] = bridge_module.acquire_schedule_definition_lock("sch-lock")

    await asyncio.create_task(contender())

    assert result["ok"] is False
    bridge_module.release_schedule_definition_lock("sch-lock")
    bridge_module.release_schedule_definition_lock("sch-lock")
    assert "sch-lock" not in bridge_module.SCHEDULE_DEFINITION_LOCK_HANDLES


def test_pause_schedule_definition_clears_pending_jobs(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "pause-job.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "pause-job",
            "scheduleId": definition["scheduleId"],
            "botId": bot.config["id"],
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    bridge_module.pause_schedule_definition(definition["scheduleId"])

    assert not pending.exists()
    assert (bridge_module.SCHEDULE_FAILED_ROOT / "pause-job.json").exists()


def test_delete_schedule_definition_interrupts_running_schedule(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    sess.active_schedule_id = definition["scheduleId"]

    calls = []

    def fake_interrupt_session(_bot, key, _sess, clear_thread, clear_chat, clear_queue):
        calls.append((key, clear_thread, clear_chat, clear_queue))
        _sess.active_schedule_id = None

    bridge_module.interrupt_session = fake_interrupt_session
    bridge_module.delete_schedule_definition(definition["scheduleId"])

    assert calls == [("single:test-user", False, False, False)]
    assert bridge_module.read_schedule_definition(definition["scheduleId"]) is None


def test_find_bot_by_chat_key_uses_registry_fallback(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.create_session_record(bot, "group-user:group-1:user-a")

    assert bridge_module.find_bot_by_chat_key("group-user:group-1:user-a", None) is bot


def test_find_bot_by_chat_key_requires_bot_name_for_ambiguous_match(bridge_module):
    bot_a = make_bot(bridge_module, config_id="bot-1", name="codex1", remote_bot_id="bot-a")
    bot_b = make_bot(bridge_module, config_id="bot-2", name="codex2", remote_bot_id="bot-b")
    bridge_module.create_session_record(bot_a, "single:test-user")
    bridge_module.create_session_record(bot_b, "single:test-user")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.find_bot_by_chat_key("single:test-user", None)

    assert excinfo.value.status_code == 409
    assert "provide botName or sessionId" in excinfo.value.message
    assert bridge_module.find_bot_by_chat_key("single:test-user", "codex2") is bot_b


def test_remove_bot_cleans_persisted_state_and_artifacts(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(sess.session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "pending-job.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "pending-job",
            "scheduleId": definition["scheduleId"],
            "botId": bot.config["id"],
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    orphan_done = bridge_module.SCHEDULE_DONE_ROOT / "orphan-job.json"
    bridge_module.write_json_atomic(
        orphan_done,
        {
            "requestId": "orphan-job",
            "scheduleId": "missing-schedule",
            "botId": bot.config["id"],
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "message": "old scheduled text",
            "runAt": bridge_module.now_ms() - 5000,
            "createdAt": bridge_module.now_ms() - 6000,
        },
    )
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": bot.config["name"],
                "botId": bot.config["botId"],
                "secretFile": str(write_secret_file(bridge_module.BASE_DIR / "persisted.secret", "secret\n")),
                "workDir": bot.config["workDir"],
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            },
            {"id": "keep-bot", "name": "keep"},
        ],
    )

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert bot.config["id"] not in bridge_module.BOTS
    assert bridge_module.read_session_record_by_id(sess.session_id) is None
    assert not bridge_module.get_registry_key_file(bot.config["id"], "single:test-user").exists()
    assert not sess.lock_file.exists()
    assert not (bridge_module.SESSION_REGISTRY_ROOT / "keys" / bot.config["id"]).exists()
    assert not (bridge_module.SESSION_LOCK_ROOT / bot.config["id"]).exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()
    assert bridge_module.read_schedule_definition(definition["scheduleId"]) is None
    assert not bridge_module.get_schedule_definition_lock_file(definition["scheduleId"]).exists()
    assert not pending.exists()
    assert not orphan_done.exists()
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored == [{"id": "keep-bot", "name": "keep"}]


def test_migrate_legacy_runtime_state_only_copies_missing_files(bridge_module):
    bridge_module.SHARED_RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    bridge_module.INSTANCE_RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    legacy_registry = bridge_module.BASE_DIR / ".session-registry" / "sessions"
    new_registry = bridge_module.SESSION_REGISTRY_ROOT / "sessions"
    legacy_registry.mkdir(parents=True, exist_ok=True)
    new_registry.mkdir(parents=True, exist_ok=True)
    (legacy_registry / "old.json").write_text('{"sessionId":"old"}', encoding="utf-8")
    (new_registry / "old.json").write_text('{"sessionId":"new"}', encoding="utf-8")
    legacy_workspace = bridge_module.BASE_DIR / "workspace" / "bot-1"
    new_workspace = bridge_module.WORKSPACE_ROOT / "bot-1"
    legacy_workspace.mkdir(parents=True, exist_ok=True)
    new_workspace.mkdir(parents=True, exist_ok=True)
    (legacy_workspace / "from-legacy.txt").write_text("legacy", encoding="utf-8")
    (new_workspace / "from-legacy.txt").write_text("current", encoding="utf-8")
    (legacy_workspace / "new-only.txt").write_text("legacy-new", encoding="utf-8")

    bridge_module.maybe_migrate_legacy_shared_runtime_state()
    bridge_module.maybe_migrate_legacy_instance_runtime_state()

    assert (new_registry / "old.json").read_text(encoding="utf-8") == '{"sessionId":"new"}'
    assert (new_workspace / "from-legacy.txt").read_text(encoding="utf-8") == "current"
    assert (new_workspace / "new-only.txt").read_text(encoding="utf-8") == "legacy-new"


def test_migrate_legacy_runtime_state_runs_only_once(bridge_module):
    bridge_module.SHARED_RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    legacy_registry = bridge_module.BASE_DIR / ".session-registry" / "sessions"
    new_registry = bridge_module.SESSION_REGISTRY_ROOT / "sessions"
    legacy_registry.mkdir(parents=True, exist_ok=True)
    new_registry.mkdir(parents=True, exist_ok=True)
    (legacy_registry / "session.json").write_text('{"sessionId":"legacy"}', encoding="utf-8")

    bridge_module.maybe_migrate_legacy_shared_runtime_state()
    assert (new_registry / "session.json").read_text(encoding="utf-8") == '{"sessionId":"legacy"}'
    assert bridge_module.get_shared_runtime_migration_marker().exists()

    (new_registry / "session.json").unlink()
    bridge_module.maybe_migrate_legacy_shared_runtime_state()

    assert not (new_registry / "session.json").exists()


def test_remove_bot_removes_persisted_config_before_cleanup_finishes(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [{"id": bot.config["id"], "botId": bot.config["botId"], "name": bot.config["name"]}],
    )

    def fake_cleanup_bot_schedule_definitions(_bot_id):
        raise bridge_module.BridgeError(409, "schedule busy")

    bridge_module.cleanup_bot_schedule_definitions = fake_cleanup_bot_schedule_definitions

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert excinfo.value.status_code == 409
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []
    assert bot.config["id"] not in bridge_module.BOTS


def test_remove_deleted_bots_from_memory_once_stops_tombstoned_bot(bridge_module):
    bot = make_bot(bridge_module)
    bot.started_at_ms = bridge_module.now_ms() - 1000
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])
    calls = []

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True):
        calls.append((bot_id, persist_disable))
        target = bridge_module.BOTS[bot_id]
        target.status = "stopped"

    bridge_module.stop_bot = fake_stop_bot

    asyncio.run(bridge_module.remove_deleted_bots_from_memory_once())

    assert calls == [(bot.config["id"], False)]
    assert bot.config["id"] not in bridge_module.BOTS


def test_remove_bot_rejects_unknown_bot_id(bridge_module):
    with pytest.raises(bridge_module.BridgeError) as excinfo:
        asyncio.run(bridge_module.remove_bot("missing-bot"))

    assert excinfo.value.status_code == 404


def test_find_config_id_by_wecom_bot_id_prefers_existing_config(bridge_module):
    bot = make_bot(bridge_module, config_id="custom-id", remote_bot_id="bot-id")

    assert bridge_module.find_config_id_by_wecom_bot_id("bot-id") == "custom-id"


def test_find_config_id_by_wecom_bot_id_falls_back_to_tombstone(bridge_module):
    bridge_module.mark_bot_deleted_globally("custom-id", "bot-id")

    assert bridge_module.find_config_id_by_wecom_bot_id("bot-id") == "custom-id"


def test_find_config_id_by_wecom_bot_id_ignores_stale_runtime_bot(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="bot-id")
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    assert bridge_module.find_config_id_by_wecom_bot_id("bot-id") == "bot-1"


def test_get_authoritative_bot_config_prefers_persisted_over_memory(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": bot.config["name"],
                "botId": bot.config["botId"],
                "secretFile": str(write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "persisted",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    bot.config["welcome"] = "memory"

    current = bridge_module.get_authoritative_bot_config(bot.config["id"])

    assert current is not None
    assert current["welcome"] == "persisted"


def test_get_authoritative_bot_config_ignores_stale_runtime_bot(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="bot-id")
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    assert bridge_module.get_authoritative_bot_config(bot.config["id"]) is None


def test_reconcile_bots_once_stops_bot_when_persisted_disabled(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bot = make_bot(bridge_module)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": bot.config["name"],
                "botId": bot.config["botId"],
                "secretFile": str(secret_file),
                "workDir": bot.config["workDir"],
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": False,
            }
        ],
    )
    calls = []

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True):
        calls.append((bot_id, persist_disable))
        bridge_module.BOTS[bot_id].status = "stopped"

    bridge_module.stop_bot = fake_stop_bot

    asyncio.run(bridge_module.reconcile_bots_once())

    assert calls == [(bot.config["id"], False)]
    assert bridge_module.BOTS[bot.config["id"]].config["enabled"] is False


def test_reconcile_bots_once_restarts_bot_when_persisted_config_changes(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bot = bridge_module.BotState(
        config={
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secret": "secret",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "enabled": True,
            "welcome": "old",
            "groupSessionMode": "per-user",
        }
    )
    bridge_module.BOTS[bot.config["id"]] = bot
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "new",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    calls = []

    async def fake_start_bot(config):
        calls.append(config)
        return bridge_module.BotState(config=config)

    bridge_module.start_bot = fake_start_bot

    asyncio.run(bridge_module.reconcile_bots_once())

    assert len(calls) == 1
    assert calls[0]["welcome"] == "new"


def test_reconcile_bots_once_starts_missing_enabled_bot(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    calls = []

    async def fake_start_bot(config):
        calls.append(config)
        return bridge_module.BotState(config=config)

    bridge_module.start_bot = fake_start_bot

    asyncio.run(bridge_module.reconcile_bots_once())

    assert len(calls) == 1
    assert calls[0]["id"] == "bot-1"


def test_reconcile_bots_once_skips_tombstoned_persisted_bot(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")
    calls = []

    async def fake_start_bot(config):
        calls.append(config)
        return bridge_module.BotState(config=config)

    bridge_module.start_bot = fake_start_bot

    asyncio.run(bridge_module.reconcile_bots_once())

    assert calls == []
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_reconcile_bots_once_keeps_recreated_persisted_bot_newer_than_tombstone(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")
    deleted_at = bridge_module.bot_tombstone_deleted_at("bot-id")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
                "createdAt": deleted_at + 1,
                "updatedAt": deleted_at + 1,
            }
        ],
    )
    calls = []

    async def fake_start_bot(config):
        calls.append(config)
        return bridge_module.BotState(config=config)

    bridge_module.start_bot = fake_start_bot

    asyncio.run(bridge_module.reconcile_bots_once())

    assert len(calls) == 1
    assert calls[0]["id"] == "bot-1"


def test_start_bot_persists_created_and_updated_at(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="bot-id")

    bridge_module.upsert_persisted_bot_config(bot.config)

    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert isinstance(stored, list) and len(stored) == 1
    assert isinstance(stored[0]["createdAt"], int)
    assert isinstance(stored[0]["updatedAt"], int)
    assert stored[0]["updatedAt"] >= stored[0]["createdAt"]


def test_reconcile_bots_once_stops_bot_missing_from_persisted_configs(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_json_atomic(bridge_module.DATA_FILE, [])

    asyncio.run(bridge_module.reconcile_bots_once())

    assert bot.config["id"] not in bridge_module.BOTS
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_resolve_local_file_send_queue_root_uses_bridge_base_dir_for_relative_env(bridge_module, monkeypatch):
    monkeypatch.setenv("LOCAL_FILE_SEND_QUEUE_ROOT", "relative-queue")

    resolved = bridge_module.resolve_local_file_send_queue_root(bridge_module.BASE_DIR)

    assert resolved == (bridge_module.BASE_DIR / "relative-queue").resolve()


def test_main_loads_bots_before_starting_local_file_send_loop(bridge_module, monkeypatch):
    calls: list[str] = []

    async def fake_load_bots():
        calls.append("load_bots")
        bridge_module.SHUTDOWN_EVENT.set()

    async def fake_local_file_send_loop():
        calls.append("local_file_send_loop")
        await bridge_module.SHUTDOWN_EVENT.wait()

    async def fake_wait_loop():
        await bridge_module.SHUTDOWN_EVENT.wait()

    async def fake_noop():
        return None

    class FakeClientSession:
        def __init__(self, *args, **kwargs):
            self.closed = False

        async def close(self):
            self.closed = True

    class FakeRunner:
        def __init__(self, app, access_log=None):
            self.app = app

        async def setup(self):
            return None

        async def cleanup(self):
            return None

    class FakeSite:
        def __init__(self, runner, host, port):
            self.runner = runner
            self.host = host
            self.port = port

        async def start(self):
            return None

    monkeypatch.setattr(bridge_module, "load_bots", fake_load_bots)
    monkeypatch.setattr(bridge_module, "local_file_send_loop", fake_local_file_send_loop)
    monkeypatch.setattr(bridge_module, "session_recycler_loop", fake_wait_loop)
    monkeypatch.setattr(bridge_module, "lease_renew_loop", fake_wait_loop)
    monkeypatch.setattr(bridge_module, "schedule_definition_loop", fake_wait_loop)
    monkeypatch.setattr(bridge_module, "scheduled_message_loop", fake_wait_loop)
    monkeypatch.setattr(bridge_module, "paused_session_recovery_loop", fake_wait_loop)
    monkeypatch.setattr(bridge_module, "bot_config_reconciler_loop", fake_wait_loop)
    monkeypatch.setattr(bridge_module, "deleted_bot_reaper_loop", fake_wait_loop)
    monkeypatch.setattr(bridge_module, "process_schedule_definitions_once", fake_noop)
    monkeypatch.setattr(bridge_module, "process_scheduled_messages_once", fake_noop)
    monkeypatch.setattr(bridge_module, "remove_deleted_bots_from_memory_once", fake_noop)
    monkeypatch.setattr(bridge_module.aiohttp, "ClientSession", FakeClientSession)
    monkeypatch.setattr(bridge_module.web, "AppRunner", FakeRunner)
    monkeypatch.setattr(bridge_module.web, "TCPSite", FakeSite)
    monkeypatch.setattr(bridge_module, "build_app", lambda: object())

    asyncio.run(bridge_module.main())

    assert calls[0] == "load_bots"
    if "local_file_send_loop" in calls:
        assert calls.index("load_bots") < calls.index("local_file_send_loop")


def test_local_file_send_loop_recovers_after_iteration_error(bridge_module, monkeypatch, capsys):
    calls = {"count": 0}

    async def fake_process_local_file_send_queue_once():
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("boom")
        bridge_module.SHUTDOWN_EVENT.set()

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr(bridge_module, "process_local_file_send_queue_once", fake_process_local_file_send_queue_once)
    monkeypatch.setattr(bridge_module.asyncio, "sleep", fake_sleep)

    asyncio.run(bridge_module.local_file_send_loop())

    captured = capsys.readouterr()
    assert calls["count"] == 2
    assert "[LOOP] local_file_send_loop error: boom" in captured.out


def test_bot_config_reconciler_loop_recovers_after_iteration_error(bridge_module, monkeypatch, capsys):
    calls = {"count": 0}

    async def fake_reconcile_bots_once():
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("boom")
        bridge_module.SHUTDOWN_EVENT.set()

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr(bridge_module, "reconcile_bots_once", fake_reconcile_bots_once)
    monkeypatch.setattr(bridge_module.asyncio, "sleep", fake_sleep)

    asyncio.run(bridge_module.bot_config_reconciler_loop())

    captured = capsys.readouterr()
    assert calls["count"] == 2
    assert "[LOOP] bot_config_reconciler_loop error: boom" in captured.out


@pytest.mark.asyncio
async def test_process_schedule_definitions_once_removes_orphaned_bot_schedule(bridge_module):
    definition = {
        "scheduleId": "sched-1",
        "botId": "missing-bot",
        "botName": "missing",
        "sessionId": None,
        "chatKey": "single:test-user",
        "message": "daily summary",
        "mode": "cron",
        "cron": "* * * * *",
        "timezone": "UTC",
        "startAt": None,
        "endAt": None,
        "maxRuns": None,
        "runCount": 0,
        "enabled": True,
        "nextRunAt": bridge_module.now_ms() - 1000,
        "lastPlannedAt": None,
        "lastTriggeredAt": None,
        "lastFinishedAt": None,
        "misfirePolicy": "fire_once_now",
        "concurrencyPolicy": "skip_if_running",
        "autoDeleteOnDone": False,
        "createdAt": bridge_module.now_ms(),
        "updatedAt": bridge_module.now_ms(),
    }
    bridge_module.write_schedule_definition(definition)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "orphan-job.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "orphan-job",
            "scheduleId": definition["scheduleId"],
            "botId": "missing-bot",
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    await bridge_module.process_schedule_definitions_once()

    assert bridge_module.read_schedule_definition(definition["scheduleId"]) is None
    assert not pending.exists()


def test_api_stop_bot_updates_persisted_config_without_local_bot(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    request = SimpleNamespace(match_info={"bot_id": "bot-1"})
    response = asyncio.run(bridge_module.api_stop_bot(request))
    body = json.loads(response.text)

    assert response.status == 200
    assert body["ok"] is True
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored[0]["enabled"] is False


def test_api_stop_bot_does_not_overwrite_newer_persisted_fields(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bot = make_bot(bridge_module)
    bot.config["welcome"] = "stale-memory"
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": bot.config["name"],
                "botId": bot.config["botId"],
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "persisted-new",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )

    async def fake_require_api_access(_request):
        return None

    async def fake_stop_bot(_bot_id: str, persist_disable: bool = True):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "stop_bot", fake_stop_bot)

    request = SimpleNamespace(match_info={"bot_id": bot.config["id"]})
    response = asyncio.run(bridge_module.api_stop_bot(request))
    body = json.loads(response.text)

    assert response.status == 200
    assert body["ok"] is True
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored[0]["enabled"] is False
    assert stored[0]["welcome"] == "persisted-new"


def test_api_restart_bot_updates_persisted_restart_token_without_local_bot(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": False,
            }
        ],
    )

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    request = SimpleNamespace(match_info={"bot_id": "bot-1"})
    response = asyncio.run(bridge_module.api_restart_bot(request))
    body = json.loads(response.text)

    assert response.status == 200
    assert body["ok"] is True
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored[0]["enabled"] is True
    assert stored[0]["restartToken"]


def test_api_get_bots_includes_disabled_persisted_bot(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": False,
            }
        ],
    )

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(bridge_module.api_get_bots(SimpleNamespace()))
    body = json.loads(response.text)

    assert len(body) == 1
    assert body[0]["id"] == "bot-1"
    assert body[0]["enabled"] is False
    assert body[0]["status"] == "disabled"


def test_api_get_bots_prefers_runtime_payload_for_loaded_bot(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bot = make_bot(bridge_module)
    bot.status = "standby"
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": bot.config["name"],
                "botId": bot.config["botId"],
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": False,
            }
        ],
    )

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(bridge_module.api_get_bots(SimpleNamespace()))
    body = json.loads(response.text)

    assert len(body) == 1
    assert body[0]["id"] == bot.config["id"]
    assert body[0]["status"] == "standby"
    assert body[0]["enabled"] is True


def test_api_get_bots_ignores_stale_runtime_bot(monkeypatch, bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="bot-id")
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(bridge_module.api_get_bots(SimpleNamespace()))
    body = json.loads(response.text)

    assert body == []


def test_api_get_bots_ignores_tombstoned_persisted_bot(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(bridge_module.api_get_bots(SimpleNamespace()))
    body = json.loads(response.text)

    assert body == []
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_api_restart_bot_does_not_overwrite_newer_persisted_fields(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bot = make_bot(bridge_module)
    bot.config["welcome"] = "stale-memory"
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": bot.config["name"],
                "botId": bot.config["botId"],
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "persisted-new",
                "groupSessionMode": "per-user",
                "enabled": False,
            }
        ],
    )

    async def fake_require_api_access(_request):
        return None

    async def fake_start_bot(config):
        return bridge_module.BotState(config=config)

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "start_bot", fake_start_bot)

    request = SimpleNamespace(match_info={"bot_id": bot.config["id"]})
    response = asyncio.run(bridge_module.api_restart_bot(request))
    body = json.loads(response.text)

    assert response.status == 200
    assert body["ok"] is True
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored[0]["enabled"] is True
    assert stored[0]["welcome"] == "persisted-new"
    assert stored[0]["restartToken"]


def test_api_restart_bot_rejects_stale_runtime_bot_without_persisted_config(monkeypatch, bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="bot-id")
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])
    started = False

    async def fake_require_api_access(_request):
        return None

    async def fake_start_bot(_config):
        nonlocal started
        started = True
        return bridge_module.BotState(config=_config)

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "start_bot", fake_start_bot)

    response = asyncio.run(bridge_module.api_restart_bot(SimpleNamespace(match_info={"bot_id": "bot-1"})))
    body = json.loads(response.text)

    assert response.status == 404
    assert body["ok"] is False
    assert body["error"] == "bot not found"
    assert started is False
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_api_restart_bot_rejects_tombstoned_persisted_bot(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")
    started = False

    async def fake_require_api_access(_request):
        return None

    async def fake_start_bot(_config):
        nonlocal started
        started = True
        return bridge_module.BotState(config=_config)

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "start_bot", fake_start_bot)

    response = asyncio.run(bridge_module.api_restart_bot(SimpleNamespace(match_info={"bot_id": "bot-1"})))
    body = json.loads(response.text)

    assert response.status == 404
    assert body["ok"] is False
    assert body["error"] == "bot not found"
    assert started is False
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_bot_runner_enters_standby_when_runtime_lock_unavailable(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)

    def fake_acquire_bot_runtime_lock(_bot):
        return False

    async def fake_sleep(_seconds):
        bridge_module.SHUTDOWN_EVENT.set()

    monkeypatch.setattr(bridge_module, "acquire_bot_runtime_lock", fake_acquire_bot_runtime_lock)
    monkeypatch.setattr(bridge_module.asyncio, "sleep", fake_sleep)

    asyncio.run(bridge_module.bot_runner(bot))

    assert bot.status == "standby"
    assert any("runtime is owned by another instance" in entry for entry in bot.logs)


def test_api_add_bot_uses_stable_default_id(monkeypatch, bridge_module):
    payload = {
        "botId": "bot-id",
        "name": "codex1",
        "secretFile": str(write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")),
        "workDir": str(bridge_module.BASE_DIR),
        "groupSessionMode": "per-user",
    }

    async def fake_read_json_body(_request):
        return payload

    async def fake_require_api_access(_request):
        return None

    async def fake_start_bot(config):
        return bridge_module.BotState(config=config)

    monkeypatch.setattr(bridge_module, "read_json_body", fake_read_json_body)
    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "start_bot", fake_start_bot)

    response = asyncio.run(bridge_module.api_add_bot(SimpleNamespace()))
    body = json.loads(response.text)

    assert body["ok"] is True
    assert body["id"] == bridge_module.default_bot_config_id("bot-id")


@pytest.mark.asyncio
async def test_read_json_body_rejects_non_object_payload(bridge_module):
    class FakeRequest:
        async def read(self):
            return b"[]"

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        await bridge_module.read_json_body(FakeRequest())

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "JSON body must be an object"


def test_resolve_schedule_target_uses_bot_name_to_disambiguate_chat_key(bridge_module):
    bot_a = make_bot(bridge_module, config_id="bot-1", name="codex1", remote_bot_id="bot-a")
    bot_b = make_bot(bridge_module, config_id="bot-2", name="codex2", remote_bot_id="bot-b")
    bridge_module.create_session_record(bot_a, "single:test-user")
    session_b = bridge_module.create_session_record(bot_b, "single:test-user")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_schedule_target({"chatKey": "single:test-user"})

    assert excinfo.value.status_code == 409
    resolved = bridge_module.resolve_schedule_target({"chatKey": "single:test-user", "botName": "codex2"})
    assert resolved["botId"] == bot_b.config["id"]
    assert resolved["sessionId"] == session_b["sessionId"]


def test_resolve_schedule_target_prefers_target_config_id_over_duplicate_bot_name(bridge_module):
    bot_a = make_bot(bridge_module, config_id="bot-a", name="default", remote_bot_id="remote-a")
    bot_b = make_bot(bridge_module, config_id="bot-b", name="default", remote_bot_id="remote-b")
    bridge_module.create_session_record(bot_a, "single:test-user")
    session_b = bridge_module.create_session_record(bot_b, "single:test-user")
    bot_b.ws = SimpleNamespace(closed=False)

    resolved = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "botName": "default",
            "targetConfigId": "bot-b",
        }
    )

    assert resolved["botId"] == "bot-b"
    assert resolved["sessionId"] == session_b["sessionId"]


@pytest.mark.asyncio
async def test_upload_and_send_file_routes_group_user_key_to_group_chat(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    ack_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        ack_payloads.append(payload)
        if payload["cmd"] == "aibot_upload_media_init":
            return {"errcode": 0, "body": {"upload_id": "upload-1"}}
        if payload["cmd"] == "aibot_upload_media_finish":
            return {"errcode": 0, "body": {"media_id": "media-1"}}
        if payload["cmd"] == "aibot_send_msg":
            return {"errcode": 0, "body": {}}
        raise AssertionError(payload["cmd"])

    async def fake_send_ws_payload(_bot, payload):
        future = _bot.pending_requests.pop(payload["headers"]["req_id"], None)
        if future and not future.done():
            future.set_result({"errcode": 0})

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "send_ws_payload", fake_send_ws_payload)

    await bridge_module.upload_and_send_file(bot, "group-user:group-1:user-a", str(file_path))

    send_payload = next(payload for payload in ack_payloads if payload["cmd"] == "aibot_send_msg")
    assert send_payload["body"]["chat_type"] == 2
    assert send_payload["body"]["chatid"] == "group-1"


@pytest.mark.asyncio
async def test_download_incoming_media_keeps_same_named_files(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    async def fake_download_buffer(url: str):
        payload = b"first" if url.endswith("/1") else b"second"
        return {"data": payload, "contentType": "text/plain", "contentDisposition": ""}

    monkeypatch.setattr(bridge_module, "download_buffer", fake_download_buffer)

    first = await bridge_module.download_incoming_media(
        bot,
        sess,
        "single:test-user",
        "file",
        {"url": "https://example.test/file/1", "filename": "report.txt"},
    )
    second = await bridge_module.download_incoming_media(
        bot,
        sess,
        "single:test-user",
        "file",
        {"url": "https://example.test/file/2", "filename": "report.txt"},
    )

    assert first["path"] != second["path"]
    assert Path(first["path"]).read_bytes() == b"first"
    assert Path(second["path"]).read_bytes() == b"second"


@pytest.mark.asyncio
async def test_process_local_file_send_queue_writes_success_only_after_upload_finishes(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    make_session(bridge_module, bot)
    chatfile_dir = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")["chatfile"]
    assert chatfile_dir is not None
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_id = "local-send-1"
    result_file = bridge_module.LOCAL_FILE_SEND_RESULT_ROOT / f"{request_id}.json"
    done_file = bridge_module.LOCAL_FILE_SEND_DONE_ROOT / f"{request_id}.json"
    bridge_module.write_json_atomic(
        bridge_module.LOCAL_FILE_SEND_PENDING_ROOT / f"{request_id}.json",
        {"requestId": request_id, "chatKey": "single:test-user", "filePath": str(file_path)},
    )
    started = asyncio.Event()
    finish = asyncio.Event()

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        assert result_file.exists() is False
        started.set()
        await finish.wait()

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await bridge_module.process_local_file_send_queue_once()
        assert result_file.exists() is False
        await asyncio.wait_for(started.wait(), 1)
        assert request_id in bot.active_local_file_request_ids
        finish.set()
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        result = json.loads(result_file.read_text("utf-8"))
        assert result["ok"] is True
        assert result["message"] == "sent reply.txt"
        assert done_file.exists()
        assert request_id not in bot.active_local_file_request_ids
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_process_local_file_send_queue_reads_namespaced_bot_queue(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-1")
    bot.ws = SimpleNamespace(closed=False)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_id = "namespaced-send-1"
    result_file = queue_paths["results"] / f"{request_id}.json"
    done_file = queue_paths["done"] / f"{request_id}.json"
    bridge_module.write_json_atomic(
        queue_paths["pending"] / f"{request_id}.json",
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path),
        },
    )
    started = asyncio.Event()
    finish = asyncio.Event()

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        assert result_file.exists() is False
        started.set()
        await finish.wait()

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await bridge_module.process_local_file_send_queue_once()
        assert result_file.exists() is False
        await asyncio.wait_for(started.wait(), 1)
        assert request_id in bot.active_local_file_request_ids
        finish.set()
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        result = json.loads(result_file.read_text("utf-8"))
        assert result["ok"] is True
        assert result["message"] == "sent reply.txt"
        assert done_file.exists()
        assert request_id not in bot.active_local_file_request_ids
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_process_local_file_send_queue_ignores_foreign_namespaced_queue(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-1")
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [{"id": "bot-2", "name": "codex2", "botId": "remote-bot-2", "enabled": True}],
    )
    foreign_paths = bridge_module.get_local_file_send_queue_paths("bot-2")
    bridge_module.ensure_local_file_send_dirs(["bot-2"])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_file = foreign_paths["pending"] / "foreign-send.json"
    bridge_module.write_json_atomic(
        request_file,
        {
            "requestId": "foreign-send",
            "chatKey": "single:test-user",
            "targetConfigId": "bot-2",
            "filePath": str(file_path),
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    foreign_lock = open(bridge_module.get_bot_runtime_lock_file("remote-bot-2"), "a+", encoding="utf-8")
    fcntl.flock(foreign_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        await bridge_module.process_local_file_send_queue_once()
        await asyncio.sleep(0)

        assert called is False
        assert request_file.exists()
        assert not (foreign_paths["processing"] / request_file.name).exists()
        assert not (foreign_paths["done"] / request_file.name).exists()
        assert not (foreign_paths["failed"] / request_file.name).exists()
    finally:
        fcntl.flock(foreign_lock.fileno(), fcntl.LOCK_UN)
        foreign_lock.close()
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_process_local_file_send_queue_skips_namespaced_queue_for_loaded_standby_bot(bridge_module, monkeypatch):
    standby_bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    standby_bot.status = "standby"
    queue_paths = bridge_module.get_local_file_send_queue_paths(standby_bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([standby_bot.config["id"]])
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_file = queue_paths["pending"] / "standby-send.json"
    bridge_module.write_json_atomic(
        request_file,
        {
            "requestId": "standby-send",
            "chatKey": "single:test-user",
            "targetConfigId": standby_bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    foreign_lock = open(bridge_module.get_bot_runtime_lock_file("remote-bot-2"), "a+", encoding="utf-8")
    fcntl.flock(foreign_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        await bridge_module.process_local_file_send_queue_once()
        await asyncio.sleep(0)

        assert called is False
        assert request_file.exists()
        assert not (queue_paths["processing"] / request_file.name).exists()
        assert not (queue_paths["done"] / request_file.name).exists()
        assert not (queue_paths["failed"] / request_file.name).exists()
    finally:
        fcntl.flock(foreign_lock.fileno(), fcntl.LOCK_UN)
        foreign_lock.close()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_fails_missing_target_bot_namespace(bridge_module, monkeypatch):
    foreign_paths = bridge_module.get_local_file_send_queue_paths("missing:bot")
    bridge_module.ensure_local_file_send_dirs(["missing:bot"])
    bridge_module.write_json_atomic(
        foreign_paths["pending"] / "missing-bot.json",
        {
            "requestId": "missing-bot",
            "chatKey": "single:test-user",
            "targetConfigId": "missing:bot",
            "filePath": str((bridge_module.BASE_DIR / "reply.txt").resolve()),
        },
    )
    (bridge_module.BASE_DIR / "reply.txt").write_text("hello", encoding="utf-8")
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    result = json.loads((foreign_paths["results"] / "missing-bot.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 404
    assert result["error"] == "bot not found: missing:bot"
    assert (foreign_paths["failed"] / "missing-bot.json").exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_fails_disabled_target_bot_namespace(bridge_module, monkeypatch):
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [{"id": "bot-2", "name": "codex2", "botId": "remote-bot-2", "enabled": False}],
    )
    foreign_paths = bridge_module.get_local_file_send_queue_paths("bot-2")
    bridge_module.ensure_local_file_send_dirs(["bot-2"])
    bridge_module.write_json_atomic(
        foreign_paths["pending"] / "disabled-bot.json",
        {
            "requestId": "disabled-bot",
            "chatKey": "single:test-user",
            "targetConfigId": "bot-2",
            "filePath": str((bridge_module.BASE_DIR / "reply.txt").resolve()),
        },
    )
    (bridge_module.BASE_DIR / "reply.txt").write_text("hello", encoding="utf-8")
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    result = json.loads((foreign_paths["results"] / "disabled-bot.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 503
    assert result["error"] == "bot disabled: bot-2"
    assert (foreign_paths["failed"] / "disabled-bot.json").exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_retries_unloaded_target_bot_without_runtime_lock(bridge_module, monkeypatch):
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [{"id": "bot-2", "name": "codex2", "botId": "remote-bot-2", "enabled": True}],
    )
    foreign_paths = bridge_module.get_local_file_send_queue_paths("bot-2")
    bridge_module.ensure_local_file_send_dirs(["bot-2"])
    bridge_module.write_json_atomic(
        foreign_paths["pending"] / "stopped-bot.json",
        {
            "requestId": "stopped-bot",
            "chatKey": "single:test-user",
            "targetConfigId": "bot-2",
            "filePath": str((bridge_module.BASE_DIR / "reply.txt").resolve()),
        },
    )
    (bridge_module.BASE_DIR / "reply.txt").write_text("hello", encoding="utf-8")
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    assert not (foreign_paths["pending"] / "stopped-bot.json").exists()
    assert (foreign_paths["processing"] / "stopped-bot.json").exists()
    assert not (foreign_paths["results"] / "stopped-bot.json").exists()
    assert not (foreign_paths["failed"] / "stopped-bot.json").exists()
    assert not (foreign_paths["done"] / "stopped-bot.json").exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_retries_loaded_disconnected_target_bot(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    bot.ws = SimpleNamespace(closed=True)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_file = queue_paths["pending"] / "disconnected-bot.json"
    bridge_module.write_json_atomic(
        request_file,
        {
            "requestId": "disconnected-bot",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    assert not request_file.exists()
    assert (queue_paths["processing"] / "disconnected-bot.json").exists()
    assert not (queue_paths["results"] / "disconnected-bot.json").exists()
    assert not (queue_paths["failed"] / "disconnected-bot.json").exists()
    assert not (queue_paths["done"] / "disconnected-bot.json").exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_fails_expired_request_before_retry(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    bot.ws = SimpleNamespace(closed=True)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_file = queue_paths["pending"] / "expired-bot.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        request_file,
        {
            "requestId": "expired-bot",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": now_ms - 10000,
            "timeoutMs": 1000,
            "expiresAt": now_ms - 1000,
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    result = json.loads((queue_paths["results"] / "expired-bot.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 504
    assert "expired before delivery" in result["error"]
    assert (queue_paths["failed"] / "expired-bot.json").exists()
    assert not (queue_paths["processing"] / "expired-bot.json").exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_backfills_deadline_for_legacy_request(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    bot.ws = SimpleNamespace(closed=True)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_file = queue_paths["pending"] / "legacy-bot.json"
    bridge_module.write_json_atomic(
        request_file,
        {
            "requestId": "legacy-bot",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    processing_file = queue_paths["processing"] / "legacy-bot.json"
    assert processing_file.exists()
    stored = json.loads(processing_file.read_text("utf-8"))
    assert isinstance(stored["requestedAt"], int)
    assert stored["timeoutMs"] == bridge_module.LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS
    assert stored["expiresAt"] >= stored["requestedAt"] + bridge_module.LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS - 1000


@pytest.mark.asyncio
async def test_process_local_file_send_queue_expires_legacy_request_after_backfill(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    bot.ws = SimpleNamespace(closed=True)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / "legacy-expired.json"
    old_requested_at = bridge_module.now_ms() - bridge_module.LOCAL_FILE_SEND_DEFAULT_TIMEOUT_MS - 5000
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": "legacy-expired",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": old_requested_at,
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    result = json.loads((queue_paths["results"] / "legacy-expired.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 504
    assert "expired before delivery" in result["error"]
    assert (queue_paths["failed"] / "legacy-expired.json").exists()
    assert not processing_file.exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_fails_ambiguous_started_request_after_restart(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / "ambiguous-bot.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": "ambiguous-bot",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": now_ms,
            "timeoutMs": 120000,
            "expiresAt": now_ms + 120000,
            "deliveryState": "sent",
            "deliveryStartedAt": now_ms - 5000,
            "deliveryFinishedAt": now_ms - 4000,
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    result = json.loads((queue_paths["results"] / "ambiguous-bot.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 409
    assert "outcome unknown" in result["error"]
    assert (queue_paths["failed"] / "ambiguous-bot.json").exists()
    assert not processing_file.exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_retries_uploading_request_after_restart(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    bot.ws = SimpleNamespace(closed=True)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / "uploading-bot.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": "uploading-bot",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": now_ms,
            "timeoutMs": 120000,
            "expiresAt": now_ms + 120000,
            "deliveryState": "uploading",
            "deliveryStartedAt": now_ms - 5000,
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    assert processing_file.exists()
    assert not (queue_paths["results"] / "uploading-bot.json").exists()
    assert not (queue_paths["failed"] / "uploading-bot.json").exists()


@pytest.mark.asyncio
async def test_process_local_file_send_queue_retries_uploading_request_after_restart_once_connected(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    bot.ws = SimpleNamespace(closed=False)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / "uploading-connected.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": "uploading-connected",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": now_ms,
            "timeoutMs": 120000,
            "expiresAt": now_ms + 120000,
            "deliveryState": "uploading",
            "deliveryStartedAt": now_ms - 5000,
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await bridge_module.process_local_file_send_queue_once()
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        assert called is True
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_upload_worker_marks_request_sending_before_final_dispatch(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-1")
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "state-before-dispatch"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": bridge_module.now_ms(),
            "timeoutMs": 120000,
            "expiresAt": bridge_module.now_ms() + 120000,
        },
    )
    captured_state = {}

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, before_delivery=None, **_kwargs):
        assert before_delivery is not None
        before_delivery()
        captured_state.update(json.loads(processing_file.read_text("utf-8")))

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    bot.active_local_file_request_ids.add(request_id)
    bot.upload_queue.put_nowait(
        {
            "id": "state-before-dispatch-job",
            "chatKey": "single:test-user",
            "filePath": str(file_path.resolve()),
            "targetConfigId": bot.config["id"],
            "localRequestId": request_id,
            "localProcessingFile": str(processing_file),
        }
    )
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        assert captured_state["deliveryState"] == "sending"
        assert "deliveryDispatchAt" in captured_state
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_process_local_file_send_queue_finalizes_existing_success_result_before_ambiguous_fallback(bridge_module):
    queue_paths = bridge_module.get_local_file_send_queue_paths("bot-2")
    bridge_module.ensure_local_file_send_dirs(["bot-2"])
    processing_file = queue_paths["processing"] / "completed-bot.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": "completed-bot",
            "chatKey": "single:test-user",
            "targetConfigId": "bot-2",
            "filePath": str((bridge_module.BASE_DIR / "reply.txt").resolve()),
            "requestedAt": now_ms,
            "timeoutMs": 120000,
            "expiresAt": now_ms + 120000,
            "deliveryState": "sent",
            "deliveryStartedAt": now_ms - 5000,
            "deliveryFinishedAt": now_ms - 4000,
        },
    )
    bridge_module.write_json_atomic(
        queue_paths["results"] / "completed-bot.json",
        {
            "ok": True,
            "message": "sent reply.txt",
            "processedAt": now_ms - 1000,
        },
    )

    await bridge_module.process_local_file_send_queue_once()

    assert not processing_file.exists()
    assert (queue_paths["done"] / "completed-bot.json").exists()
    result = json.loads((queue_paths["results"] / "completed-bot.json").read_text("utf-8"))
    assert result["ok"] is True
    assert result["message"] == "sent reply.txt"


@pytest.mark.asyncio
async def test_process_local_file_send_queue_cleans_up_stale_result_files(bridge_module):
    bridge_module.ensure_local_file_send_dirs()
    old_result = bridge_module.LOCAL_FILE_SEND_RESULT_ROOT / "old.json"
    fresh_result = bridge_module.LOCAL_FILE_SEND_RESULT_ROOT / "fresh.json"
    retained_result = bridge_module.LOCAL_FILE_SEND_RESULT_ROOT / "retained.json"
    bridge_module.write_json_atomic(
        old_result,
        {
            "ok": False,
            "statusCode": 504,
            "error": "expired",
            "processedAt": bridge_module.now_ms() - bridge_module.LOCAL_FILE_SEND_RESULT_RETENTION_MS - 1000,
        },
    )
    bridge_module.write_json_atomic(
        fresh_result,
        {
            "ok": True,
            "message": "sent",
            "processedAt": bridge_module.now_ms(),
        },
    )
    bridge_module.write_json_atomic(
        retained_result,
        {
            "ok": True,
            "message": "sent after restart",
            "processedAt": bridge_module.now_ms() - bridge_module.LOCAL_FILE_SEND_RESULT_RETENTION_MS - 1000,
            "retainUntil": bridge_module.now_ms() + 60_000,
        },
    )

    await bridge_module.process_local_file_send_queue_once()

    assert not old_result.exists()
    assert fresh_result.exists()
    assert retained_result.exists()


def test_cancel_pending_file_send_requests_scans_namespaced_pending_queue(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "pending-reset"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    bridge_module.write_json_atomic(
        queue_paths["pending"] / f"{request_id}.json",
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "session reset before file send")

    assert cancelled == 1
    result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 409
    assert result["error"] == "session reset before file send"
    assert (queue_paths["failed"] / f"{request_id}.json").exists()


def test_cancel_pending_file_send_requests_writes_namespaced_result_for_queued_job(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "queued-reset"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )
    bot.active_local_file_request_ids.add(request_id)
    bot.upload_queue.put_nowait(
        {
            "id": "queued-job",
            "chatKey": "single:test-user",
            "filePath": str(file_path.resolve()),
            "targetConfigId": bot.config["id"],
            "localRequestId": request_id,
            "localProcessingFile": str(processing_file),
        }
    )

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "session reset before file send")

    assert cancelled == 1
    assert bot.upload_queue.qsize() == 0
    assert request_id not in bot.active_local_file_request_ids
    result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 409
    assert result["error"] == "session reset before file send"
    assert (queue_paths["failed"] / f"{request_id}.json").exists()


@pytest.mark.asyncio
async def test_cancel_pending_file_send_requests_marks_removed_queue_items_done(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "join-reset"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )
    bot.upload_queue.put_nowait(
        {
            "id": "queued-job",
            "chatKey": "single:test-user",
            "filePath": str(file_path.resolve()),
            "targetConfigId": bot.config["id"],
            "localRequestId": request_id,
            "localProcessingFile": str(processing_file),
        }
    )

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "session reset before file send")

    assert cancelled == 1
    await asyncio.wait_for(bot.upload_queue.join(), 1)


@pytest.mark.asyncio
async def test_cancel_pending_file_send_requests_cancels_active_upload_without_overwriting_result(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "active-reset"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )
    started = asyncio.Event()
    cancelled_upload = asyncio.Event()

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled_upload.set()
            raise

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    bot.active_local_file_request_ids.add(request_id)
    bot.upload_queue.put_nowait(
        {
            "id": "active-job",
            "chatKey": "single:test-user",
            "filePath": str(file_path.resolve()),
            "targetConfigId": bot.config["id"],
            "localRequestId": request_id,
            "localProcessingFile": str(processing_file),
        }
    )
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await asyncio.wait_for(started.wait(), 1)

        cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "session reset before file send")

        assert cancelled == 1
        await asyncio.wait_for(cancelled_upload.wait(), 1)
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
        assert result["ok"] is False
        assert result["statusCode"] == 409
        assert result["error"] == "session reset before file send"
        assert (queue_paths["failed"] / f"{request_id}.json").exists()
        assert not (queue_paths["done"] / f"{request_id}.json").exists()
        assert request_id not in bot.active_local_file_request_ids
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_upload_worker_stops_delivery_after_request_expiry(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-1")
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "delivery-expired"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": now_ms,
            "timeoutMs": 50,
            "expiresAt": now_ms + 50,
        },
    )
    cancelled_upload = asyncio.Event()

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled_upload.set()
            raise

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    bot.active_local_file_request_ids.add(request_id)
    bot.upload_queue.put_nowait(
        {
            "id": "delivery-expired-job",
            "chatKey": "single:test-user",
            "filePath": str(file_path.resolve()),
            "targetConfigId": bot.config["id"],
            "localRequestId": request_id,
            "localProcessingFile": str(processing_file),
        }
    )
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await asyncio.wait_for(cancelled_upload.wait(), 1)
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
        assert result["ok"] is False
        assert result["statusCode"] == 504
        assert "expired during delivery; outcome unknown" in result["error"]
        assert (queue_paths["failed"] / f"{request_id}.json").exists()
        assert not (queue_paths["done"] / f"{request_id}.json").exists()
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_upload_worker_marks_websocket_closed_during_sending_as_ambiguous(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-1")
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "delivery-websocket-closed"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": now_ms,
            "timeoutMs": 120000,
            "expiresAt": now_ms + 120000,
        },
    )

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, before_delivery=None, **_kwargs):
        assert before_delivery is not None
        before_delivery()
        raise bridge_module.BridgeError(503, "bot websocket closed")

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    bot.active_local_file_request_ids.add(request_id)
    bot.upload_queue.put_nowait(
        {
            "id": "delivery-websocket-closed-job",
            "chatKey": "single:test-user",
            "filePath": str(file_path.resolve()),
            "targetConfigId": bot.config["id"],
            "localRequestId": request_id,
            "localProcessingFile": str(processing_file),
        }
    )
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
        assert result["ok"] is False
        assert result["statusCode"] == 409
        assert "outcome unknown" in result["error"]
        assert (queue_paths["failed"] / f"{request_id}.json").exists()
        assert not (queue_paths["done"] / f"{request_id}.json").exists()
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
@pytest.mark.parametrize("message", ["websocket send timeout", "websocket ack timeout"])
async def test_upload_worker_marks_final_send_timeouts_as_ambiguous(bridge_module, monkeypatch, message):
    bot = make_bot(bridge_module, config_id="bot-1")
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = f"delivery-timeout-{message.replace(' ', '-')}"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    now_ms = bridge_module.now_ms()
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": now_ms,
            "timeoutMs": 120000,
            "expiresAt": now_ms + 120000,
        },
    )

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, before_delivery=None, **_kwargs):
        assert before_delivery is not None
        before_delivery()
        raise bridge_module.BridgeError(504, message)

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)
    bot.active_local_file_request_ids.add(request_id)
    bot.upload_queue.put_nowait(
        {
            "id": f"{request_id}-job",
            "chatKey": "single:test-user",
            "filePath": str(file_path.resolve()),
            "targetConfigId": bot.config["id"],
            "localRequestId": request_id,
            "localProcessingFile": str(processing_file),
        }
    )
    worker = asyncio.create_task(bridge_module.upload_worker(bot))
    try:
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
        assert result["ok"] is False
        assert result["statusCode"] == 409
        assert "outcome unknown" in result["error"]
    finally:
        bot.config["enabled"] = False
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker


@pytest.mark.asyncio
async def test_cancel_pending_file_send_requests_catches_active_job_before_task_published(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    request_id = "pre-task-cancel"
    processing_file = queue_paths["processing"] / f"{request_id}.json"
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    bridge_module.write_json_atomic(
        processing_file,
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "sessionId": sess.session_id,
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
        },
    )
    bot.active_upload_job = {
        "localRequestId": request_id,
        "localProcessingFile": str(processing_file),
    }
    bot.active_upload_task = None
    bot.active_local_file_request_ids.add(request_id)

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "session reset before file send")

    assert cancelled == 1
    result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 409
    assert result["error"] == "session reset before file send"
    assert (queue_paths["failed"] / f"{request_id}.json").exists()


@pytest.mark.asyncio
async def test_run_codex_keeps_processing_job_on_failure(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sess.active_schedule_id = "sch-1"
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-keep",
            "scheduleId": "sch-1",
            "botId": bot.config["id"],
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": bridge_module.now_ms(),
            "enqueuedByInstance": bridge_module.INSTANCE_ID,
        },
    )

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = 1
            return 1

    async def fake_create_subprocess_exec(*args, **kwargs):
        return FakeProcess()

    async def fake_send_or_store_session_payload(*args, **kwargs):
        return True

    bridge_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    bridge_module.send_or_store_session_payload = fake_send_or_store_session_payload

    await bridge_module.run_codex(
        bot,
        sess,
        "single:test-user",
        "prompt",
        "req-1",
        [],
        scheduled_job_file=str(processing),
    )

    assert processing.exists()
    assert not (bridge_module.SCHEDULE_DONE_ROOT / "job.json").exists()


def test_add_log_prints_timestamped_entry(bridge_module, monkeypatch, capsys):
    bot = make_bot(bridge_module)
    monkeypatch.setattr(bridge_module, "format_log_timestamp", lambda ts=None: "2026-04-21 12:34:56")

    bridge_module.add_log(bot, "hello")

    captured = capsys.readouterr()
    assert bot.logs[-1] == "[2026-04-21 12:34:56] hello"
    assert captured.out == "[2026-04-21 12:34:56] [codex1] hello\n"


@pytest.mark.asyncio
async def test_terminate_process_kills_and_waits(bridge_module):
    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.killed = False
            self.waited = False

        def kill(self):
            self.killed = True
            self.returncode = -9

        async def wait(self):
            self.waited = True
            return self.returncode

    process = FakeProcess()
    await bridge_module.terminate_process(process)

    assert process.killed is True
    assert process.waited is True


def test_detect_codex_runtime_status_recognizes_reconnect_patterns(bridge_module):
    assert bridge_module.detect_codex_runtime_status("Reconnecting to backend...") == "reconnecting"
    assert bridge_module.detect_codex_runtime_status("network timeout while contacting server") == "network_issue"
    assert bridge_module.detect_codex_runtime_status("Connection restored") == "connected"
    assert bridge_module.detect_codex_runtime_status("plain stderr line") is None


def test_update_codex_runtime_status_logs_only_on_change(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    bridge_module.update_codex_runtime_status(bot, sess, "single:test-user", "reconnecting", "Reconnecting to backend...")
    bridge_module.update_codex_runtime_status(bot, sess, "single:test-user", "reconnecting", "Reconnecting to backend...")
    bridge_module.update_codex_runtime_status(bot, sess, "single:test-user", "connected", "Connection restored")

    status_logs = [entry for entry in bot.logs if "event=codex.runtime_status" in entry]
    assert len(status_logs) == 2
    assert "reconnecting" in status_logs[0]
    assert "connected" in status_logs[1]


def test_build_queue_status_text_includes_queue_position(bridge_module):
    assert bridge_module.build_queue_status_text(3) == "运行状态：排队中，前方还有 2 个任务。"
    assert bridge_module.build_queue_status_text(1) == "运行状态：排队中，即将开始处理。"


def test_build_thinking_status_text_includes_optional_detail(bridge_module):
    assert bridge_module.build_thinking_status_text(7) == "运行状态：思考中，已运行 7s。"
    assert bridge_module.build_thinking_status_text(7, "正在读取上下文") == "运行状态：思考中，已运行 7s，正在读取上下文。"


def test_build_working_status_text_cycles_dots(bridge_module):
    assert bridge_module.build_working_status_text(0) == "运行状态：整理回复中. 已运行 0s。"
    assert bridge_module.build_working_status_text(5) == "运行状态：整理回复中.. 已运行 5s。"
    assert bridge_module.build_working_status_text(10) == "运行状态：整理回复中... 已运行 10s。"


def test_build_status_stream_content_keeps_summary_visible(bridge_module):
    result = bridge_module.build_status_stream_content("运行状态：整理回复中. 已运行 5s。", "阶段性摘要")
    assert result == "运行状态：整理回复中. 已运行 5s。\n\n阶段性摘要"


@pytest.mark.asyncio
async def test_enqueue_message_sends_queue_status_when_session_busy(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    payloads = []

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            payloads.append(payload)

    bot.ws = FakeWS()

    result = await bridge_module.enqueue_message(bot, "single:test-user", "queued", "req-1")

    assert result is True
    assert payloads[0]["body"]["stream"]["finish"] is True
    assert payloads[0]["body"]["stream"]["content"] == "运行状态：排队中，前方还有 1 个任务。 任务完成后会主动发送结果。"
    assert "req-1" in sess.reply_proactive_req_ids


@pytest.mark.asyncio
async def test_enqueue_message_uses_proactive_for_attachment_notice_after_queue_finish(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sess.pending_media_downloads = 1
    stream_payloads = []
    proactive_payloads = []

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            stream_payloads.append(payload)

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        proactive_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    bot.ws = FakeWS()
    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    result = await bridge_module.enqueue_message(bot, "single:test-user", "queued", "req-1")

    assert result is True
    assert stream_payloads[0]["body"]["stream"]["finish"] is True
    assert len(proactive_payloads) == 1
    assert proactive_payloads[0]["cmd"] == "aibot_send_msg"
    assert "Attachments are still downloading" in proactive_payloads[0]["body"]["markdown"]["content"]


@pytest.mark.asyncio
async def test_process_queue_restores_pending_media_when_run_fails_to_start(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    media = {"kind": "file", "path": "/tmp/a.txt", "fileName": "a.txt"}
    sess.pending_media = [media]
    sess.pending_media_notes = ["attachment-note"]
    sess.queue.append({"text": "hello", "reqId": "req-1"})

    async def fake_create_subprocess_exec(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    bridge_module.process_queue(bot, sess, "single:test-user")
    assert sess.run_task is not None
    await sess.run_task

    assert sess.pending_media == [media]
    assert sess.pending_media_notes == ["attachment-note"]
    assert sess.active_run_media == []
    assert sess.active_run_media_notes == []


def test_interrupt_session_restores_active_run_media_without_dropping_pending_media(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    active_media = {"kind": "file", "path": "/tmp/a.txt", "fileName": "a.txt"}
    pending_media = {"kind": "file", "path": "/tmp/b.txt", "fileName": "b.txt"}
    sess.running = True
    sess.active_run_media = [active_media]
    sess.active_run_media_notes = ["active-note"]
    sess.pending_media = [pending_media]
    sess.pending_media_notes = ["pending-note"]

    bridge_module.interrupt_session(bot, "single:test-user", sess, clear_thread=False, clear_chat=False, clear_queue=False)

    assert sess.pending_media == [active_media, pending_media]
    assert sess.pending_media_notes == ["active-note", "pending-note"]
    assert sess.active_run_media == []
    assert sess.active_run_media_notes == []


@pytest.mark.asyncio
async def test_respond_info_keeps_intermediate_reply_open(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = []

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            payloads.append(payload)

    bot.ws = FakeWS()
    bridge_module.register_reply_session(bot, "req-1", sess)

    await bridge_module.respond_info(bot, "req-1", "processing", final=False)
    await bridge_module.respond_info(bot, "req-1", "done")

    assert payloads[0]["body"]["stream"]["finish"] is False
    assert payloads[1]["body"]["stream"]["finish"] is True
    assert "req-1" not in bot.reply_sessions


@pytest.mark.asyncio
async def test_send_session_status_times_out_stuck_websocket_write(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    class StuckWS:
        closed = False

        async def send_json(self, payload):
            await asyncio.Event().wait()

    bot.ws = StuckWS()
    monkeypatch.setattr(bridge_module, "STATUS_SEND_TIMEOUT_SEC", 1)
    monkeypatch.setattr(bridge_module, "STATUS_SEND_LOCK_TIMEOUT_SEC", 1)

    delivered = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status")

    assert delivered is False
    assert sess.send_lock.locked() is False
    assert sess.pending_stream_payload["body"]["stream"]["content"] == "status"


@pytest.mark.asyncio
async def test_send_session_status_times_out_when_send_lock_is_busy(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            raise AssertionError("send_json should not be reached while lock is busy")

    bot.ws = FakeWS()
    monkeypatch.setattr(bridge_module, "STATUS_SEND_LOCK_TIMEOUT_SEC", 1)

    await sess.send_lock.acquire()
    try:
        delivered = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status")
    finally:
        sess.send_lock.release()

    assert delivered is False
    assert sess.pending_stream_payload["body"]["stream"]["content"] == "status"


@pytest.mark.asyncio
async def test_send_session_status_sends_long_text_in_chunks(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sent_payloads = []

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            sent_payloads.append(payload)

    bot.ws = FakeWS()

    delivered = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "x" * 8000)

    assert delivered is True
    assert len(sent_payloads) == 3
    assert sent_payloads[0]["body"]["stream"]["finish"] is False
    assert sent_payloads[1]["body"]["stream"]["finish"] is False
    assert sent_payloads[2]["body"]["stream"]["finish"] is False
    assert "".join(item["body"]["stream"]["content"] for item in sent_payloads) == "x" * 8000


@pytest.mark.asyncio
async def test_send_ws_payload_with_ack_times_out_stuck_websocket_write(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)

    class StuckWS:
        closed = False

        async def send_json(self, payload):
            await asyncio.Event().wait()

    bot.ws = StuckWS()
    monkeypatch.setattr(bridge_module, "WEBSOCKET_SEND_TIMEOUT_SEC", 1)
    payload = {"cmd": "aibot_send_msg", "headers": {"req_id": "req-1"}, "body": {}}

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        await bridge_module.send_ws_payload_with_ack(bot, payload, 10)

    assert excinfo.value.status_code == 504
    assert excinfo.value.message == "websocket send timeout"
    assert "req-1" not in bot.pending_requests


def test_process_queue_waits_for_cached_final_reply(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.pending_final_payload = bridge_module.build_session_text_payload("single:test-user", sess, "req-1", "final", True)
    sess.queue.append({"text": "next", "reqId": "req-2"})

    bridge_module.process_queue(bot, sess, "single:test-user")

    assert sess.run_task is None
    assert sess.queue == [{"text": "next", "reqId": "req-2"}]


@pytest.mark.asyncio
async def test_stale_run_done_callback_does_not_resume_queue(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.queue.extend(
        [
            {"text": "old", "reqId": "req-old"},
            {"text": "next", "reqId": "req-next"},
        ]
    )

    async def fake_run_codex(*args, **kwargs):
        return None

    monkeypatch.setattr(bridge_module, "run_codex", fake_run_codex)

    bridge_module.process_queue(bot, sess, "single:test-user")
    sess.running = False
    sess.run_generation += 1

    await asyncio.sleep(0)

    assert sess.queue == [{"text": "next", "reqId": "req-next"}]


@pytest.mark.asyncio
async def test_flush_cached_final_reply_resumes_queue(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.pending_final_payload = bridge_module.build_session_text_payload("single:test-user", sess, "req-1", "final", True)
    sess.queue.append({"text": "next", "reqId": "req-2"})
    started = []

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            started.append(("sent", payload["body"]["stream"]["content"]))

    def fake_process_queue(_bot, _sess, key):
        started.append(("queue", key))

    bot.ws = FakeWS()
    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    await bridge_module.flush_session_pending_payloads(bot, "single:test-user", sess)

    assert started == [("sent", "final"), ("queue", "single:test-user")]
    assert sess.pending_final_payload is None


@pytest.mark.asyncio
async def test_final_reply_after_idle_gap_falls_back_to_proactive(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    sent_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        sent_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_last_sent_at["req-1"] = time.time() - bridge_module.REPLY_IDLE_FALLBACK_SEC - 1

    payload = bridge_module.build_session_text_payload("single:test-user", sess, "req-1", "final", True)
    delivered = await bridge_module.send_or_store_session_payload(bot, "single:test-user", sess, payload, True)

    assert delivered is True
    assert sent_payloads[0]["cmd"] == "aibot_send_msg"
    assert sent_payloads[0]["body"]["markdown"]["content"] == "final"


@pytest.mark.asyncio
async def test_stream_status_closes_reply_when_max_age_reached(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = []

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            payloads.append(payload)

    bot.ws = FakeWS()
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_started_at["req-1"] = time.time() - bridge_module.REPLY_MAX_AGE_FALLBACK_SEC - 1

    payload = bridge_module.build_session_text_payload("single:test-user", sess, "req-1", "status", False)
    delivered = await bridge_module.send_or_store_session_payload(bot, "single:test-user", sess, payload, False)

    assert delivered is False
    assert payloads[0]["cmd"] == "aibot_respond_msg"
    assert payloads[0]["body"]["stream"]["finish"] is True
    assert "后台运行" in payloads[0]["body"]["stream"]["content"]
    assert "req-1" in sess.reply_proactive_req_ids
    assert sess.pending_stream_payload is None


@pytest.mark.asyncio
async def test_expired_stream_status_uses_proactive_with_interval(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    sent_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        sent_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    bridge_module.mark_reply_proactive(sess, "req-1")

    first = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status 1")
    second = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status 2")

    assert first is True
    assert second is False
    assert len(sent_payloads) == 1
    assert sent_payloads[0]["cmd"] == "aibot_send_msg"
    assert sent_payloads[0]["body"]["markdown"]["content"] == "status 1"


@pytest.mark.asyncio
async def test_expired_stream_status_mentions_group_user_after_max_age(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot, "group-user:group-1:user-a")
    bot.ws = SimpleNamespace(closed=False)
    sent_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        sent_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    bridge_module.mark_reply_proactive(sess, "req-1")
    sess.reply_started_at["req-1"] = time.time() - bridge_module.REPLY_MAX_AGE_FALLBACK_SEC - 1

    delivered = await bridge_module.send_session_status(bot, "group-user:group-1:user-a", sess, "req-1", "status 1")

    assert delivered is True
    assert sent_payloads[0]["cmd"] == "aibot_send_msg"
    assert sent_payloads[0]["body"]["chatid"] == "group-1"
    assert sent_payloads[0]["body"]["markdown"]["content"] == "<@user-a>\nstatus 1"


@pytest.mark.asyncio
async def test_send_session_status_closes_stream_before_proactive_after_max_age(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    stream_payloads = []
    proactive_payloads = []

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            stream_payloads.append(payload)

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        proactive_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    bot.ws = FakeWS()
    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_started_at["req-1"] = time.time() - bridge_module.REPLY_MAX_AGE_FALLBACK_SEC - 1

    delivered = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status")

    assert delivered is True
    assert stream_payloads[0]["cmd"] == "aibot_respond_msg"
    assert stream_payloads[0]["body"]["stream"]["finish"] is True
    assert "后台运行" in stream_payloads[0]["body"]["stream"]["content"]
    assert proactive_payloads[0]["cmd"] == "aibot_send_msg"
    assert proactive_payloads[0]["body"]["markdown"]["content"] == "status"


@pytest.mark.asyncio
async def test_final_reply_after_max_age_falls_back_to_proactive(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    sent_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        sent_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_started_at["req-1"] = time.time() - bridge_module.REPLY_MAX_AGE_FALLBACK_SEC - 1

    payload = bridge_module.build_session_text_payload("single:test-user", sess, "req-1", "final", True)
    delivered = await bridge_module.send_or_store_session_payload(bot, "single:test-user", sess, payload, True)

    assert delivered is True
    assert sent_payloads[0]["cmd"] == "aibot_send_msg"
    assert sent_payloads[0]["body"]["markdown"]["content"] == "final"
    assert "req-1" not in bot.reply_sessions


@pytest.mark.asyncio
async def test_final_reply_after_max_age_mentions_group_user(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot, "group-user:group-1:user-a")
    bot.ws = SimpleNamespace(closed=False)
    sent_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        sent_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_started_at["req-1"] = time.time() - bridge_module.REPLY_MAX_AGE_FALLBACK_SEC - 1

    payload = bridge_module.build_session_text_payload("group-user:group-1:user-a", sess, "req-1", "final", True)
    delivered = await bridge_module.send_or_store_session_payload(bot, "group-user:group-1:user-a", sess, payload, True)

    assert delivered is True
    assert sent_payloads[0]["cmd"] == "aibot_send_msg"
    assert sent_payloads[0]["body"]["chatid"] == "group-1"
    assert sent_payloads[0]["body"]["markdown"]["content"] == "<@user-a>\nfinal"
    assert "req-1" not in bot.reply_sessions


@pytest.mark.asyncio
async def test_interrupt_command_resumes_preserved_queue_after_reply(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.queue.append({"text": "queued", "reqId": "req-queued"})
    events = []

    async def fake_respond_info(_bot, req_id, message):
        events.append(("reply", req_id, message))

    def fake_process_queue(_bot, _sess, key):
        events.append(("queue", key))

    monkeypatch.setattr(bridge_module, "respond_info", fake_respond_info)
    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    await bridge_module.interrupt_session_command(bot, "single:test-user", "req-interrupt")

    assert events == [
        ("reply", "req-interrupt", "Current task interrupted."),
        ("queue", "single:test-user"),
    ]


@pytest.mark.asyncio
async def test_cancelled_old_run_does_not_clear_new_run_state(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    old_started = asyncio.Event()
    unblock_old = asyncio.Event()

    class FakeProcess:
        def __init__(self, name):
            self.name = name
            self.returncode = None
            self.killed = False
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            old_started.set()
            await unblock_old.wait()
            self.returncode = 0
            return 0

        def kill(self):
            self.killed = True
            self.returncode = -9

    old_process = FakeProcess("old")
    new_process = FakeProcess("new")

    async def fake_create_subprocess_exec(*args, **kwargs):
        return old_process

    async def fake_send_session_status(*args, **kwargs):
        return True

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)

    sess.running = True
    sess.run_generation = 1
    old_task = asyncio.create_task(
        bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-old", [], run_generation=1)
    )
    sess.run_task = old_task
    await old_started.wait()

    new_task = asyncio.create_task(asyncio.sleep(60))
    sess.run_generation = 2
    sess.run_task = new_task
    sess.proc = new_process
    sess.running = True

    old_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await old_task

    assert old_process.killed is True
    assert new_process.killed is False
    assert sess.proc is new_process
    assert sess.run_task is new_task
    assert sess.running is True

    new_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await new_task


@pytest.mark.asyncio
async def test_run_codex_sends_running_status_before_final(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bridge_module.CODEX_EXEC_MODE = "host"
    sess = make_session(bridge_module, bot)
    sess.running = True
    payloads = []
    captured = {}

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            event = {"type": "item.completed", "item": {"type": "agent_message", "text": "final answer"}}
            self.stdout.feed_data((json.dumps(event) + "\n").encode("utf-8"))
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = 0
            return 0

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["cwd"] = kwargs["cwd"]
        captured["env"] = kwargs["env"]
        process = FakeProcess()
        captured["args"] = list(args)
        captured["stdin"] = process.stdin
        return process

    async def fake_send_session_status(_bot, _key, _sess, _req_id, content):
        payloads.append((False, content))
        return True

    async def fake_send_or_store_session_payload(_bot, _key, _sess, payload, final):
        payloads.append((final, payload["body"]["stream"]["content"]))
        return True

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", [])

    assert payloads[-1] == (True, "final answer")
    assert payloads[0] == (False, "运行状态：正在启动处理。")
    assert payloads[1] == (False, "运行状态：思考中，已运行 0s。")
    assert captured["cwd"] == str(bridge_module.get_workfile_dir(bot.config["id"], "test-user"))
    assert captured["args"][-1] == "-"
    assert bytes(captured["stdin"].buffer).decode("utf-8") == "prompt"
    assert captured["stdin"].closed is True
    assert captured["env"]["CODEX_HOME"].startswith(str(bridge_module.get_bridge_codex_home_root()))
    chatfile_dir = str(bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")["chatfile"])
    workfile_dir = str(bridge_module.get_workfile_dir(bot.config["id"], "test-user"))
    assert captured["env"]["WECOM_BRIDGE_BOT_NAME"] == bot.config["name"]
    assert captured["env"]["WECOM_BRIDGE_BOT_CONFIG_ID"] == bot.config["id"]
    assert captured["env"]["WECOM_BRIDGE_CWD_DIR"] == workfile_dir
    assert captured["env"]["WECOM_BRIDGE_WORKSPACE_SKILL_DIR"] == str(Path(workfile_dir) / ".codex" / "skills")
    assert captured["env"]["WECOM_BRIDGE_GLOBAL_SKILL_DIR"] == str(bridge_module.get_session_global_skills_root(sess.session_id))
    assert captured["env"]["WECOM_BRIDGE_CHATFILE_DIR"] == chatfile_dir
    assert captured["env"]["WECOM_BRIDGE_EXPORT_DIR"] == chatfile_dir
    assert captured["env"]["TMPDIR"] == chatfile_dir
    assert captured["env"]["TMP"] == chatfile_dir
    assert captured["env"]["TEMP"] == chatfile_dir


@pytest.mark.asyncio
async def test_run_codex_uses_workfile_as_cwd_in_sandbox_mode(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))
    Path(bot.config["workDir"]).mkdir(parents=True, exist_ok=True)
    sess = make_session(bridge_module, bot)
    sess.running = True
    captured = {}

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            event = {"type": "item.completed", "item": {"type": "agent_message", "text": "ok"}}
            self.stdout.feed_data((json.dumps(event) + "\n").encode("utf-8"))
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = 0
            return 0

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["cwd"] = kwargs["cwd"]
        captured["env"] = kwargs["env"]
        return FakeProcess()

    async def fake_send_session_status(*args, **kwargs):
        return True

    async def fake_send_or_store_session_payload(*args, **kwargs):
        return True

    monkeypatch.setattr(bridge_module, "CODEX_EXEC_MODE", "sandboxed")
    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", [])

    expected_workfile_dir = str(bridge_module.get_workfile_dir(bot.config["id"], "test-user").resolve())
    assert captured["cwd"] == expected_workfile_dir
    assert captured["env"]["WECOM_BRIDGE_CWD_DIR"] == expected_workfile_dir


@pytest.mark.asyncio
async def test_run_codex_uses_roomfile_as_cwd_for_group_session_in_sandbox_mode(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))
    Path(bot.config["workDir"]).mkdir(parents=True, exist_ok=True)
    bot.config["groupSessionMode"] = "shared"
    sess = make_session(bridge_module, bot, key="group:test-room")
    sess.running = True
    captured = {}

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            event = {"type": "item.completed", "item": {"type": "agent_message", "text": "ok"}}
            self.stdout.feed_data((json.dumps(event) + "\n").encode("utf-8"))
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = 0
            return 0

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["cwd"] = kwargs["cwd"]
        captured["env"] = kwargs["env"]
        return FakeProcess()

    async def fake_send_session_status(*args, **kwargs):
        return True

    async def fake_send_or_store_session_payload(*args, **kwargs):
        return True

    monkeypatch.setattr(bridge_module, "CODEX_EXEC_MODE", "sandboxed")
    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)

    await bridge_module.run_codex(bot, sess, "group:test-room", "prompt", "req-1", [])

    expected_roomfile_dir = str(bridge_module.get_roomfile_dir(bot.config["id"], "test-room").resolve())
    assert captured["cwd"] == expected_roomfile_dir
    assert captured["env"]["WECOM_BRIDGE_CWD_DIR"] == expected_roomfile_dir


def test_build_codex_home_for_subprocess_preserves_global_skills_and_adds_project_skills(bridge_module):
    global_skill = bridge_module.DEFAULT_CODEX_HOME / "skills" / "global-skill"
    global_skill.mkdir(parents=True, exist_ok=True)
    (global_skill / "SKILL.md").write_text("global", encoding="utf-8")
    extra_state = bridge_module.DEFAULT_CODEX_HOME / "installation_id"
    extra_state.write_text("install-1", encoding="utf-8")
    project_skill = bridge_module.PROJECT_SHARED_SKILLS_ROOT / "project-skill"
    project_skill.mkdir(parents=True, exist_ok=True)
    (project_skill / "SKILL.md").write_text("project", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-1")

    assert (codex_home / "skills" / "global-skill" / "SKILL.md").is_file()
    assert (codex_home / "skills" / "project-skill" / "SKILL.md").is_file()
    assert (codex_home / "installation_id").read_text(encoding="utf-8") == "install-1"


def test_build_codex_home_for_subprocess_skips_volatile_tmp_arg0_runtime(bridge_module):
    arg0_dir = bridge_module.DEFAULT_CODEX_HOME / "tmp" / "arg0" / "codex-arg0deadbeef"
    arg0_dir.mkdir(parents=True, exist_ok=True)
    (arg0_dir / ".lock").write_text("", encoding="utf-8")
    (bridge_module.DEFAULT_CODEX_HOME / "installation_id").write_text("install-1", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-volatile")

    assert (codex_home / "installation_id").read_text(encoding="utf-8") == "install-1"
    assert (codex_home / "tmp").is_dir()
    assert not (codex_home / "tmp" / "arg0").exists()

@pytest.mark.asyncio
async def test_run_codex_retries_fresh_exec_when_stdin_prompt_error_variant_appears(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sess.thread_id = "old-thread"
    payloads = []
    invocations = []
    stdin_payloads = []

    class FakeProcess:
        def __init__(self, *, returncode: int, stdout_events: list[dict[str, object]], stderr_lines: list[str]):
            self.returncode = None
            self._planned_returncode = returncode
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            for event in stdout_events:
                self.stdout.feed_data((json.dumps(event) + "\n").encode("utf-8"))
            for line in stderr_lines:
                self.stderr.feed_data((line + "\n").encode("utf-8"))
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = self._planned_returncode
            return self.returncode

        def kill(self):
            self.returncode = -9

    async def fake_create_subprocess_exec(*args, **kwargs):
        invocations.append(list(args))
        process = None
        if len(invocations) == 1:
            process = FakeProcess(
                returncode=1,
                stdout_events=[{"type": "thread.started", "thread_id": "thread-from-failed-first-exec"}],
                stderr_lines=["Error: no prompt provide via stdin"],
            )
        else:
            process = FakeProcess(
                returncode=0,
                stdout_events=[
                    {"type": "thread.started", "thread_id": "thread-from-second-exec"},
                    {"type": "item.completed", "item": {"type": "agent_message", "text": "final answer"}},
                ],
                stderr_lines=[],
            )
        stdin_payloads.append(process.stdin)
        return process

    async def fake_send_session_status(_bot, _key, _sess, _req_id, content):
        payloads.append((False, content))
        return True

    async def fake_send_or_store_session_payload(_bot, _key, _sess, payload, final):
        payloads.append((final, payload["body"]["stream"]["content"]))
        return True

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", ["image.png"])

    assert len(invocations) == 2
    assert invocations[0][0:3] == ["codex", "exec", "resume"]
    assert invocations[1][0:2] == ["codex", "exec"]
    assert invocations[0][-1] == "-"
    assert "resume" not in invocations[1]
    assert invocations[1][-1] == "-"
    assert bytes(stdin_payloads[0].buffer).decode("utf-8") == "prompt"
    assert bytes(stdin_payloads[1].buffer).decode("utf-8") == "prompt"
    assert sess.thread_id == "thread-from-second-exec"
    assert payloads[-1] == (True, "final answer")


@pytest.mark.asyncio
async def test_run_codex_retries_fresh_exec_when_prompt_write_broken_pipe_occurs(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sess.thread_id = "old-thread"
    payloads = []
    invocations = []

    class BrokenPipeStdin(FakeStdin):
        async def drain(self) -> None:
            raise BrokenPipeError("broken pipe")

    class FakeProcess:
        def __init__(self, *, returncode: int, stdout_events: list[dict[str, object]], stderr_lines: list[str], broken_stdin: bool = False):
            self.returncode = None
            self._planned_returncode = returncode
            self.stdin = BrokenPipeStdin() if broken_stdin else FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            for event in stdout_events:
                self.stdout.feed_data((json.dumps(event) + "\n").encode("utf-8"))
            for line in stderr_lines:
                self.stderr.feed_data((line + "\n").encode("utf-8"))
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = self._planned_returncode
            return self.returncode

        def kill(self):
            self.returncode = -9

    async def fake_create_subprocess_exec(*args, **kwargs):
        invocations.append(list(args))
        if len(invocations) == 1:
            return FakeProcess(
                returncode=1,
                stdout_events=[],
                stderr_lines=["Reading prompt from stdin", "Error: no prompt provided via stdin"],
                broken_stdin=True,
            )
        return FakeProcess(
            returncode=0,
            stdout_events=[
                {"type": "thread.started", "thread_id": "thread-from-second-exec"},
                {"type": "item.completed", "item": {"type": "agent_message", "text": "final answer"}},
            ],
            stderr_lines=[],
        )

    async def fake_send_session_status(_bot, _key, _sess, _req_id, content):
        payloads.append((False, content))
        return True

    async def fake_send_or_store_session_payload(_bot, _key, _sess, payload, final):
        payloads.append((final, payload["body"]["stream"]["content"]))
        return True

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", [])

    assert len(invocations) == 2
    assert invocations[0][0:3] == ["codex", "exec", "resume"]
    assert invocations[1][0:2] == ["codex", "exec"]
    assert "resume" not in invocations[1]
    assert sess.thread_id == "thread-from-second-exec"
    assert payloads[-1] == (True, "final answer")


@pytest.mark.asyncio
async def test_reset_then_image_then_text_uses_fresh_exec_with_image_and_stdin_prompt(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "old-thread"
    bridge_module.update_session_record(sess.session_id, lambda record: {**record, "threadId": "old-thread"})

    async def fake_respond_info(*args, **kwargs):
        return None

    async def fake_send_session_status(*args, **kwargs):
        return True

    async def fake_send_or_store_session_payload(*args, **kwargs):
        return True

    image_path = bridge_module.BASE_DIR / "attached.png"
    image_path.write_bytes(b"fake-image")

    async def fake_download_incoming_media(_bot, _sess, _key, kind, _payload):
        assert kind == "image"
        return {
            "kind": "image",
            "path": str(image_path),
            "size": image_path.stat().st_size,
            "contentType": "image/png",
            "fileName": image_path.name,
        }

    captured = {}

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stdout.feed_data((json.dumps({"type": "thread.started", "thread_id": "new-thread"}) + "\n").encode("utf-8"))
            self.stdout.feed_data(
                (json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "image analyzed"}}) + "\n").encode("utf-8")
            )
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = 0
            return 0

        def kill(self):
            self.returncode = -9

    async def fake_create_subprocess_exec(*args, **kwargs):
        process = FakeProcess()
        captured["args"] = list(args)
        captured["env"] = kwargs["env"]
        captured["stdin"] = process.stdin
        return process

    monkeypatch.setattr(bridge_module, "respond_info", fake_respond_info)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)
    monkeypatch.setattr(bridge_module, "download_incoming_media", fake_download_incoming_media)
    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    await bridge_module.reset_session_command(bot, "single:test-user", None)
    assert sess.thread_id is None

    await bridge_module.enqueue_media_message(
        bot,
        {
            "headers": {},
            "body": {
                "msgtype": "image",
                "from": {"userid": "test-user"},
                "image": {"url": "https://example.test/image.png"},
            },
        },
        "image",
    )
    assert sess.pending_media == [
        {
            "kind": "image",
            "path": str(image_path),
            "size": image_path.stat().st_size,
            "contentType": "image/png",
            "fileName": image_path.name,
        }
    ]

    accepted = await bridge_module.enqueue_message(bot, "single:test-user", "分析一下", "req-1")

    assert accepted is True
    assert sess.run_task is not None
    await sess.run_task

    assert captured["args"][0:2] == ["codex", "exec"]
    assert "resume" not in captured["args"]
    assert "-i" in captured["args"]
    assert str(image_path) in captured["args"]
    assert captured["args"][-1] == "-"
    prompt = bytes(captured["stdin"].buffer).decode("utf-8")
    assert "Attachment 1: image" in prompt
    assert str(image_path) in prompt
    assert "User request:\n分析一下" in prompt
    assert captured["stdin"].closed is True
    assert captured["env"]["WECOM_BRIDGE_CHATFILE_DIR"]
    assert captured["env"]["TMPDIR"] == captured["env"]["WECOM_BRIDGE_CHATFILE_DIR"]
    assert sess.thread_id == "new-thread"
