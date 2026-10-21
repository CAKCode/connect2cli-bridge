from __future__ import annotations

import asyncio
import base64
import contextlib
import fcntl
import importlib.util
import json
import os
import shutil
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
CHECK_BRIDGE_HEALTH_SH_PATH = REPO_ROOT / "check_bridge_health.sh"
CHECK_RESTART_NOISE_SH_PATH = REPO_ROOT / "check_restart_noise.sh"
DEPLOY_FROM_CODEX_SH_PATH = REPO_ROOT / "deploy_from_codex.sh"


def load_runtime_config_module():
    spec = importlib.util.spec_from_file_location(
        f"bridge_runtime_config_test_{time.time_ns()}",
        BRIDGE_RUNTIME_CONFIG_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def load_bridge_module():
    spec = importlib.util.spec_from_file_location(
        f"bridge_test_env_{time.time_ns()}",
        REPO_ROOT / "bridge.py",
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
            "workspaceMode": "team",
            "groupSessionMode": "per-user",
            "agentBackend": "codex",
            "agentCommand": None,
            "agentRunAsUser": None,
            "agentRunAsGroup": None,
            "agentRuntimeRoot": None,
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


def test_bridge_fixture_isolates_all_writable_runtime_roots(bridge_module):
    isolated_roots = (
        bridge_module.DATA_FILE,
        bridge_module.SHARED_RUNTIME_ROOT,
        bridge_module.INSTANCE_RUNTIME_ROOT,
        bridge_module.DEFAULT_CODEX_HOME,
        bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT,
        bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT,
        bridge_module.TEMPLATE_CARD_STATE_ROOT,
    )

    for target in isolated_roots:
        target.resolve().relative_to(bridge_module.BASE_DIR.resolve())


def test_bridge_fixture_ignores_host_import_configuration(request, monkeypatch):
    monkeypatch.setenv("BRIDGE_BIND", "203.0.113.10:4567")
    monkeypatch.setenv("BRIDGE_TOKEN", "host-token")
    monkeypatch.setenv("CODEX_EXEC_MODE", "sandboxed")
    monkeypatch.setenv("MAX_JSON_BODY", "17")
    monkeypatch.setenv("STREAM_TEXT_MAX_CHARS", "9999")

    module = request.getfixturevalue("bridge_module")

    assert (module.HOST, module.PORT) == ("127.0.0.1", 9299)
    assert module.BRIDGE_TOKEN == ""
    assert module.CODEX_EXEC_MODE == "host"
    assert module.MAX_JSON_BODY == 1024 * 1024
    assert module.STREAM_TEXT_MAX_CHARS == 3500


def write_local_codex_rollout(
    base_home: Path,
    *,
    session_id: str,
    cwd: Path,
    transcript=None,
    session_timestamp: str = "2026-07-20T06:13:28.929Z",
    rollout_timestamp: str = "2026-07-20T06-13-28",
    mtime_ms: int | None = None,
) -> Path:
    transcript = list(transcript or [])
    rollout_dir = base_home / "sessions" / "2026" / "07" / "20"
    rollout_dir.mkdir(parents=True, exist_ok=True)
    rollout_file = rollout_dir / f"rollout-{rollout_timestamp}-{session_id}.jsonl"
    started_at = session_timestamp
    lines = [
        {
            "timestamp": started_at,
            "type": "session_meta",
            "payload": {
                "session_id": session_id,
                "id": session_id,
                "timestamp": started_at,
                "cwd": str(cwd),
            },
        }
    ]
    for index, entry in enumerate(transcript, start=1):
        if isinstance(entry, dict):
            role = str(entry["role"])
            text = str(entry["text"])
            phase = str(entry.get("phase") or "").strip()
        else:
            role, text = entry
            phase = ""
        lines.append(
            {
                "timestamp": f"2026-07-20T06:13:{28 + index:02d}.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": role,
                    "content": [
                        {
                            "type": "input_text" if role == "user" else "output_text",
                            "text": text,
                        }
                    ],
                    **({"phase": phase} if phase else {}),
                },
            }
        )
    rollout_file.write_text("\n".join(json.dumps(line, ensure_ascii=False) for line in lines) + "\n", encoding="utf-8")
    if mtime_ms is not None:
        os.utime(rollout_file, ns=(mtime_ms * 1_000_000, mtime_ms * 1_000_000))
    return rollout_file


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


def make_acking_ws(bot, on_send=None):
    class AckingWS:
        closed = False

        async def send_json(self, payload):
            if on_send is not None:
                on_send(payload)
            req_id = ((payload.get("headers") or {}).get("req_id"))
            future = bot.pending_requests.get(req_id) if req_id else None
            if future is not None and not future.done():
                future.set_result({"headers": {"req_id": req_id}, "errcode": 0, "body": {}})

    return AckingWS()


def make_flaky_acking_ws(bot, *, fail_on_send_number: int, on_send=None, error: Exception | None = None):
    failure = error or RuntimeError("socket closed")

    class FlakyAckingWS:
        closed = False

        def __init__(self) -> None:
            self.send_count = 0

        async def send_json(self, payload):
            self.send_count += 1
            if self.send_count == fail_on_send_number:
                raise failure
            if on_send is not None:
                on_send(payload)
            req_id = ((payload.get("headers") or {}).get("req_id"))
            future = bot.pending_requests.get(req_id) if req_id else None
            if future is not None and not future.done():
                future.set_result({"headers": {"req_id": req_id}, "errcode": 0, "body": {}})

    return FlakyAckingWS()


def test_create_session_record_does_not_persist_workdir(bridge_module):
    bot = make_bot(bridge_module)
    record = bridge_module.create_session_record(bot, "single:test-user")
    persisted = bridge_module.read_json_file(bridge_module.get_registry_session_file(record["sessionId"]), None)
    assert persisted is not None
    assert "workDir" not in persisted


def test_create_session_record_persists_personal_cwd_dir(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-record"
    work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"

    record = bridge_module.create_session_record(bot, "single:test-user")
    persisted = bridge_module.read_json_file(bridge_module.get_registry_session_file(record["sessionId"]), None)

    assert persisted is not None
    assert persisted["cwdDir"] == str(work_dir.resolve())


def test_get_or_create_session_restores_persisted_recent_chat(bridge_module):
    bot = make_bot(bridge_module)
    record = bridge_module.create_session_record(bot, "single:test-user")
    bridge_module.update_session_record(
        record["sessionId"],
        lambda current: {
            **current,
            "recentChat": [
                {"role": "user", "text": "older user turn", "time": 1},
                {"role": "bot", "text": "older bot turn", "time": 2},
            ],
        },
    )

    sess = bridge_module.get_or_create_session(bot, "single:test-user")

    assert [item["text"] for item in sess.chat] == ["older user turn", "older bot turn"]


def test_normalize_bot_config_preserves_workspace_namespace(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret.txt", "secret\n")

    normalized = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "codex1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "workspaceNamespace": "personal-main",
            "groupSessionMode": "per-user",
        }
    )

    assert normalized["workspaceNamespace"] == "personal-main"


def test_normalize_bot_config_defaults_workspace_namespace_to_generated_id(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-default.txt", "secret\n")

    normalized = bridge_module.normalize_bot_config(
        {
            "name": "codex1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
        }
    )

    assert normalized["id"]
    assert normalized["workspaceNamespace"] == normalized["id"]


def test_normalize_bot_config_rejects_null_bot_id(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-null-botid.txt", "secret\n")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.normalize_bot_config(
            {
                "name": "codex1",
                "botId": "null",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "groupSessionMode": "per-user",
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "botId required"


def test_normalize_bot_config_treats_null_config_id_as_generated(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-null-config-id.txt", "secret\n")

    normalized = bridge_module.normalize_bot_config(
        {
            "id": "null",
            "name": "codex1",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
        }
    )

    assert normalized["id"]
    assert normalized["id"] != "null"


def test_bridge_module_treats_null_codex_exec_mode_as_host(monkeypatch):
    monkeypatch.setenv("CODEX_EXEC_MODE", "null")

    module = load_bridge_module()

    assert module.CODEX_EXEC_MODE == "host"


def test_normalize_bot_config_treats_null_work_dir_as_default_work_dir(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-null-workdir.txt", "secret\n")
    monkeypatch.setattr(bridge_module, "DEFAULT_WORK_DIR", str(bridge_module.BASE_DIR))

    normalized = bridge_module.normalize_bot_config(
        {
            "name": "codex1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": "null",
            "groupSessionMode": "per-user",
        }
    )

    assert normalized["workDir"] == str(bridge_module.BASE_DIR.resolve())


def test_normalize_bot_config_treats_null_workspace_namespace_as_default_id(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-null-namespace.txt", "secret\n")

    normalized = bridge_module.normalize_bot_config(
        {
            "name": "codex1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "workspaceNamespace": "null",
            "groupSessionMode": "per-user",
        }
    )

    assert normalized["workspaceNamespace"] == normalized["id"]


def test_normalize_bot_config_defaults_workspace_mode_by_backend_for_new_configs(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-workspace-mode.txt", "secret\n")

    codex_config = bridge_module.normalize_bot_config(
        {
            "name": "codex1",
            "botId": "remote-bot-codex",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "agentBackend": "codex",
        }
    )
    claude_config = bridge_module.normalize_bot_config(
        {
            "name": "claude1",
            "botId": "remote-bot-claude",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "agentBackend": "claude",
        }
    )

    assert codex_config["workspaceMode"] == "team"
    assert claude_config["workspaceMode"] == "personal"


def test_normalize_bot_config_treats_null_workspace_mode_as_backend_default(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-workspace-null.txt", "secret\n")

    codex_config = bridge_module.normalize_bot_config(
        {
            "botId": "remote-bot-codex",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentBackend": "codex",
            "workspaceMode": "null",
        }
    )
    claude_config = bridge_module.normalize_bot_config(
        {
            "botId": "remote-bot-claude",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentBackend": "claude",
            "workspaceMode": "null",
        }
    )

    assert codex_config["workspaceMode"] == "team"
    assert claude_config["workspaceMode"] == "personal"


def test_normalize_bot_config_treats_null_group_session_mode_as_default_per_user(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-group-mode-null.txt", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "botId": "remote-bot-codex",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "null",
        }
    )

    assert config["groupSessionMode"] == "per-user"


def test_normalize_bot_config_treats_null_agent_backend_as_default_codex(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-agent-backend-null.txt", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "botId": "remote-bot-codex",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentBackend": "null",
        }
    )

    assert config["agentBackend"] == "codex"
    assert config["workspaceMode"] == "team"


def test_normalize_persisted_bot_config_defaults_missing_workspace_mode_to_team(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-persisted.txt", "secret\n")

    normalized = bridge_module.normalize_persisted_bot_config(
        {
            "id": "bot-1",
            "name": "claude1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "agentBackend": "claude",
            "createdAt": 1,
            "updatedAt": 2,
        }
    )

    assert normalized["workspaceMode"] == "team"


def test_normalize_persisted_bot_config_treats_null_welcome_as_empty_string(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-persisted-welcome-null.txt", "secret\n")

    normalized = bridge_module.normalize_persisted_bot_config(
        {
            "id": "bot-1",
            "name": "claude1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "welcome": "null",
            "groupSessionMode": "per-user",
        }
    )

    assert normalized["welcome"] == ""


def test_normalize_persisted_bot_config_treats_null_name_as_default_unnamed(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-persisted-name-null.txt", "secret\n")

    normalized = bridge_module.normalize_persisted_bot_config(
        {
            "id": "bot-1",
            "name": "null",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
        }
    )

    assert normalized["name"] == "unnamed"


def test_normalize_persisted_bot_config_rejects_null_bot_id(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-persisted-botid-null.txt", "secret\n")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.normalize_persisted_bot_config(
            {
                "id": "bot-1",
                "name": "claude1",
                "botId": "null",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
            }
        )

    assert excinfo.value.status_code == 500
    assert excinfo.value.message == "botId required"


def test_normalize_persisted_bot_config_treats_null_optional_agent_fields_as_unset(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-persisted-null-agent.txt", "secret\n")

    normalized = bridge_module.normalize_persisted_bot_config(
        {
            "id": "bot-1",
            "name": "claude1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "agentBackend": "null",
            "groupSessionMode": "null",
            "agentCommand": "null",
            "agentRunAsUser": "null",
            "agentRunAsGroup": "none",
            "agentRuntimeRoot": "null",
        }
    )

    assert normalized["agentBackend"] == "codex"
    assert normalized["groupSessionMode"] == "per-user"
    assert normalized["agentCommand"] is None
    assert normalized["agentRunAsUser"] is None
    assert normalized["agentRunAsGroup"] is None
    assert normalized["agentRuntimeRoot"] is None


def test_normalize_persisted_bot_config_coerces_string_false_enabled(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "secret-persisted-enabled-false.txt", "secret\n")

    normalized = bridge_module.normalize_persisted_bot_config(
        {
            "id": "bot-1",
            "name": "codex1",
            "botId": "remote-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "enabled": "false",
        }
    )

    assert normalized["enabled"] is False


def test_recycle_session_removes_idle_session_from_memory(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot, "single:test-user")
    bridge_module.ensure_dir(bridge_module.get_session_codex_home_root(sess.session_id))
    assert bridge_module.get_session_codex_home_root(sess.session_id).exists()

    bridge_module.recycle_session(bot, sess, "single:test-user")

    assert "single:test-user" not in bot.sessions
    assert bridge_module.get_session_codex_home_root(sess.session_id).exists() is True


def test_cleanup_orphan_session_codex_homes_removes_unregistered_runtime_dirs(bridge_module):
    orphan = bridge_module.get_session_codex_home_root("sess-orphan")
    live = bridge_module.get_session_codex_home_root("sess-live")
    bridge_module.ensure_dir(orphan)
    bridge_module.ensure_dir(live)
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file("sess-live"),
        {
            "sessionId": "sess-live",
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", "sess-live")),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        },
    )

    removed = bridge_module.cleanup_orphan_session_codex_homes()

    assert removed == 1
    assert orphan.exists() is False
    assert live.exists() is True


def test_cleanup_orphan_session_codex_homes_preserves_home_for_malformed_registered_session(bridge_module):
    malformed_id = "sess-malformed"
    malformed_home = bridge_module.get_session_codex_home_root(malformed_id)
    orphan = bridge_module.get_session_codex_home_root("sess-orphan")
    bridge_module.ensure_dir(malformed_home)
    bridge_module.ensure_dir(orphan)
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(malformed_id),
        {
            "sessionId": malformed_id,
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        },
    )

    removed = bridge_module.cleanup_orphan_session_codex_homes()

    assert removed == 1
    assert orphan.exists() is False
    assert malformed_home.exists() is True


def test_cleanup_orphan_session_codex_homes_unlinks_symlink_without_deleting_target(bridge_module):
    orphan = bridge_module.get_session_codex_home_root("sess-orphan")
    target = bridge_module.BASE_DIR / "external-session-home"
    bridge_module.ensure_dir(target)
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    bridge_module.ensure_dir_for(orphan)
    orphan.symlink_to(target, target_is_directory=True)

    removed = bridge_module.cleanup_orphan_session_codex_homes()

    assert removed == 1
    assert not os.path.lexists(orphan)
    assert target.exists() is True
    assert (target / "keep.txt").exists() is True


def test_cleanup_orphan_session_codex_homes_removes_broken_symlink_home(bridge_module):
    orphan = bridge_module.get_session_codex_home_root("sess-broken")
    target = bridge_module.BASE_DIR / "missing-session-home"
    bridge_module.ensure_dir_for(orphan)
    orphan.symlink_to(target, target_is_directory=True)

    removed = bridge_module.cleanup_orphan_session_codex_homes()

    assert removed == 1
    assert not os.path.lexists(orphan)


def test_cleanup_stale_session_codex_homes_removes_only_expired_unleased_dirs(bridge_module):
    stale_id = "sess-stale"
    fresh_id = "sess-fresh"
    active_id = "sess-active"
    stale_home = bridge_module.get_session_codex_home_root(stale_id)
    fresh_home = bridge_module.get_session_codex_home_root(fresh_id)
    active_home = bridge_module.get_session_codex_home_root(active_id)
    for path in (stale_home, fresh_home, active_home):
        bridge_module.ensure_dir(path)
    now_value = bridge_module.now_ms()
    stale_ms = now_value - (bridge_module.SESSION_TTL * 1000) - 1000
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(stale_id),
        {
            "sessionId": stale_id,
            "botId": "bot-1",
            "chatKey": "single:stale",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", stale_id)),
            "createdAt": stale_ms,
            "updatedAt": stale_ms,
            "lastRunAt": stale_ms,
            "status": "idle",
        },
    )
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(fresh_id),
        {
            "sessionId": fresh_id,
            "botId": "bot-1",
            "chatKey": "single:fresh",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", fresh_id)),
            "createdAt": now_value,
            "updatedAt": now_value,
            "lastRunAt": now_value,
            "status": "idle",
        },
    )
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(active_id),
        {
            "sessionId": active_id,
            "botId": "bot-1",
            "chatKey": "single:active",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", active_id)),
            "createdAt": stale_ms,
            "updatedAt": stale_ms,
            "lastRunAt": stale_ms,
            "leaseExpiresAt": now_value + 60000,
            "status": "leased",
        },
    )

    removed = bridge_module.cleanup_stale_session_codex_homes(now_value)

    assert removed == 1
    assert stale_home.exists() is False
    assert fresh_home.exists() is True
    assert active_home.exists() is True


def test_cleanup_stale_session_codex_homes_unlinks_symlink_without_deleting_target(bridge_module):
    session_id = "sess-stale-link"
    stale_home = bridge_module.get_session_codex_home_root(session_id)
    target = bridge_module.BASE_DIR / "external-stale-home"
    bridge_module.ensure_dir(target)
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    bridge_module.ensure_dir_for(stale_home)
    stale_home.symlink_to(target, target_is_directory=True)
    now_value = bridge_module.now_ms()
    stale_ms = now_value - (bridge_module.SESSION_TTL * 1000) - 1000
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "createdAt": stale_ms,
            "updatedAt": stale_ms,
            "lastRunAt": stale_ms,
            "status": "idle",
        },
    )

    removed = bridge_module.cleanup_stale_session_codex_homes(now_value)

    assert removed == 1
    assert not os.path.lexists(stale_home)
    assert target.exists() is True
    assert (target / "keep.txt").exists() is True


def test_cleanup_stale_session_codex_homes_removes_broken_symlink_home(bridge_module):
    session_id = "sess-broken-stale"
    stale_home = bridge_module.get_session_codex_home_root(session_id)
    target = bridge_module.BASE_DIR / "missing-stale-home"
    bridge_module.ensure_dir_for(stale_home)
    stale_home.symlink_to(target, target_is_directory=True)
    now_value = bridge_module.now_ms()
    stale_ms = now_value - (bridge_module.SESSION_TTL * 1000) - 1000
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "createdAt": stale_ms,
            "updatedAt": stale_ms,
            "lastRunAt": stale_ms,
            "status": "idle",
        },
    )

    removed = bridge_module.cleanup_stale_session_codex_homes(now_value)

    assert removed == 1
    assert not os.path.lexists(stale_home)


def test_cleanup_stale_session_codex_homes_removes_malformed_expired_record_home(bridge_module):
    session_id = "sess-malformed-stale"
    stale_home = bridge_module.get_session_codex_home_root(session_id)
    bridge_module.ensure_dir(stale_home)
    now_value = bridge_module.now_ms()
    stale_ms = now_value - (bridge_module.SESSION_TTL * 1000) - 1000
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "createdAt": stale_ms,
            "updatedAt": stale_ms,
            "lastRunAt": stale_ms,
            "status": "idle",
        },
    )

    removed = bridge_module.cleanup_stale_session_codex_homes(now_value)

    assert removed == 1
    assert stale_home.exists() is False


def test_cleanup_stale_session_codex_homes_preserves_malformed_leased_home(bridge_module):
    session_id = "sess-malformed-active"
    active_home = bridge_module.get_session_codex_home_root(session_id)
    bridge_module.ensure_dir(active_home)
    now_value = bridge_module.now_ms()
    stale_ms = now_value - (bridge_module.SESSION_TTL * 1000) - 1000
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "createdAt": stale_ms,
            "updatedAt": stale_ms,
            "lastRunAt": stale_ms,
            "leaseExpiresAt": now_value + 60_000,
            "status": "leased",
        },
    )

    removed = bridge_module.cleanup_stale_session_codex_homes(now_value)

    assert removed == 0
    assert active_home.exists() is True


def test_cleanup_stale_session_codex_homes_preserves_active_session_id_even_when_record_looks_stale(
    bridge_module,
):
    session_id = "sess-active-memory"
    active_home = bridge_module.get_session_codex_home_root(session_id)
    bridge_module.ensure_dir(active_home)
    now_value = bridge_module.now_ms()
    stale_ms = now_value - (bridge_module.SESSION_TTL * 1000) - 1000
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "createdAt": stale_ms,
            "updatedAt": stale_ms,
            "lastRunAt": stale_ms,
            "status": "idle",
        },
    )

    removed = bridge_module.cleanup_stale_session_codex_homes(now_value, active_session_ids={session_id})

    assert removed == 0
    assert active_home.exists() is True


def test_migrate_legacy_workspace_dir_preserves_symlinked_entries(bridge_module):
    source_root = bridge_module.BASE_DIR / "legacy-source"
    target_root = bridge_module.BASE_DIR / "migrated-target"
    external_target = bridge_module.BASE_DIR / "legacy-external-target"
    bridge_module.ensure_dir(source_root)
    bridge_module.ensure_dir(external_target)
    (external_target / "nested.txt").write_text("nested", encoding="utf-8")
    (source_root / "linked-dir").symlink_to(external_target, target_is_directory=True)
    bridge_module.ensure_dir(source_root / "pkg")
    (source_root / "pkg" / "linked-nested").symlink_to(external_target, target_is_directory=True)

    bridge_module.migrate_legacy_workspace_dir(source_root, target_root)

    linked_dir = target_root / "linked-dir"
    nested_link = target_root / "pkg" / "linked-nested"
    assert linked_dir.is_symlink()
    assert linked_dir.resolve() == external_target.resolve()
    assert nested_link.is_symlink()
    assert nested_link.resolve() == external_target.resolve()
    assert os.path.lexists(linked_dir)
    assert os.path.lexists(nested_link)
    assert (linked_dir / "nested.txt").read_text(encoding="utf-8") == "nested"
    assert (nested_link / "nested.txt").read_text(encoding="utf-8") == "nested"


def test_migrate_legacy_workspace_dir_does_not_recurse_into_existing_symlinked_target_dir(bridge_module):
    source_root = bridge_module.BASE_DIR / "legacy-source-existing-target"
    target_root = bridge_module.BASE_DIR / "migrated-target-existing-target"
    external_target = bridge_module.BASE_DIR / "existing-symlink-target"
    bridge_module.ensure_dir(source_root / "pkg")
    bridge_module.ensure_dir(external_target)
    (source_root / "pkg" / "from-legacy.txt").write_text("legacy", encoding="utf-8")
    (external_target / "keep.txt").write_text("keep", encoding="utf-8")
    bridge_module.ensure_dir(target_root)
    (target_root / "pkg").symlink_to(external_target, target_is_directory=True)

    bridge_module.migrate_legacy_workspace_dir(source_root, target_root)

    linked_dir = target_root / "pkg"
    assert linked_dir.is_symlink()
    assert linked_dir.resolve() == external_target.resolve()
    assert (external_target / "keep.txt").read_text(encoding="utf-8") == "keep"
    assert not (external_target / "from-legacy.txt").exists()


def test_migrate_legacy_workspace_dir_keeps_existing_broken_symlink_destination(bridge_module):
    source_root = bridge_module.BASE_DIR / "legacy-source-broken-target"
    target_root = bridge_module.BASE_DIR / "migrated-target-broken-target"
    bridge_module.ensure_dir(source_root / "pkg")
    (source_root / "pkg" / "from-legacy.txt").write_text("legacy", encoding="utf-8")
    bridge_module.ensure_dir(target_root)
    broken_target = bridge_module.BASE_DIR / "missing-existing-target"
    (target_root / "pkg").symlink_to(broken_target, target_is_directory=True)

    bridge_module.migrate_legacy_workspace_dir(source_root, target_root)

    linked_dir = target_root / "pkg"
    assert linked_dir.is_symlink()
    assert not linked_dir.exists()
    assert os.path.lexists(linked_dir)


@pytest.mark.asyncio
async def test_message_after_idle_recycle_reuses_session_id_and_recreates_runtime_session(bridge_module):
    bot = make_bot(bridge_module)
    first = bridge_module.get_or_create_session(bot, "single:test-user")
    first.thread_id = "stale-thread"
    first_session_id = first.session_id

    bridge_module.recycle_session(bot, first, "single:test-user")

    assert "single:test-user" not in bot.sessions

    captured = []
    original_enqueue_message = bridge_module.enqueue_message

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        accepted = await original_enqueue_message(_bot, key, text, req_id, **kwargs)
        recreated = _bot.sessions[key]
        captured.append((key, text, req_id, accepted, recreated.session_id, recreated.thread_id))
        assert recreated is not first
        assert recreated.session_id == first_session_id
        assert recreated.thread_id is None
        return accepted

    bridge_module.enqueue_message = fake_enqueue_message

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-after-idle"},
            "body": {
                "msgtype": "text",
                "chattype": "single",
                "chatid": "test-user",
                "text": {"content": "hello again"},
                "from": {"userid": "test-user"},
            },
        },
    )

    assert captured == [("single:test-user", "hello again", "req-after-idle", True, first_session_id, None)]


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


def test_acquire_session_lease_reclaims_malformed_lock_file(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    record = bridge_module.create_session_record(bot, key)
    sess = bridge_module.SessionState(
        session_id=record["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, key)),
        lock_file=Path(record["lockFile"]),
    )
    sess.lock_file.parent.mkdir(parents=True, exist_ok=True)
    sess.lock_file.write_text("[]", encoding="utf-8")

    acquired = bridge_module.acquire_session_lease(bot, sess, key)

    assert acquired is True
    assert sess.lease_owned is True
    lease = bridge_module.read_json_file(sess.lock_file, None)
    assert lease["instanceId"] == bridge_module.INSTANCE_ID
    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted["ownerInstance"] == bridge_module.INSTANCE_ID


def test_acquire_session_lease_replaces_broken_symlink_lock_file(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    record = bridge_module.create_session_record(bot, key)
    sess = bridge_module.SessionState(
        session_id=record["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, key)),
        lock_file=Path(record["lockFile"]),
    )
    sess.lock_file.parent.mkdir(parents=True, exist_ok=True)
    sess.lock_file.symlink_to(bridge_module.BASE_DIR / "missing-lease.lock")

    acquired = bridge_module.acquire_session_lease(bot, sess, key)

    assert acquired is True
    assert sess.lease_owned is True
    assert sess.lock_file.exists()
    assert not sess.lock_file.is_symlink()
    lease = bridge_module.read_json_file(sess.lock_file, None)
    assert lease["instanceId"] == bridge_module.INSTANCE_ID


def test_acquire_session_lease_replaces_valid_symlink_lock_file_without_touching_target(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    record = bridge_module.create_session_record(bot, key)
    sess = bridge_module.SessionState(
        session_id=record["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, key)),
        lock_file=Path(record["lockFile"]),
    )
    external_lock = bridge_module.BASE_DIR / "external-lease.lock"
    external_lock.write_text('{"instanceId":"external"}', encoding="utf-8")
    sess.lock_file.parent.mkdir(parents=True, exist_ok=True)
    sess.lock_file.symlink_to(external_lock)

    acquired = bridge_module.acquire_session_lease(bot, sess, key)

    assert acquired is True
    assert sess.lease_owned is True
    assert sess.lock_file.exists()
    assert not sess.lock_file.is_symlink()
    assert external_lock.read_text(encoding="utf-8") == '{"instanceId":"external"}'
    lease = bridge_module.read_json_file(sess.lock_file, None)
    assert lease["instanceId"] == bridge_module.INSTANCE_ID


def test_renew_session_lease_treats_malformed_lock_as_lost(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.lock_file.write_text("[]", encoding="utf-8")

    renewed = bridge_module.renew_session_lease(bot, "single:test-user", sess)

    assert renewed is False
    assert sess.lease_owned is False


def test_release_session_lease_tolerates_malformed_lock_file(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.lock_file.write_text("[]", encoding="utf-8")

    bridge_module.release_session_lease(bot, "single:test-user", sess)

    assert sess.lease_owned is False
    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted["ownerInstance"] is None
    assert persisted["leaseExpiresAt"] is None


def test_release_session_lease_preserves_foreign_takeover(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    lease_expires_at = bridge_module.now_ms() + 60_000
    foreign_lease = {
        "instanceId": "foreign-instance",
        "pid": 12345,
        "botId": bot.config["id"],
        "chatKey": "single:test-user",
        "updatedAt": bridge_module.now_ms(),
        "expiresAt": lease_expires_at,
    }
    bridge_module.write_lease(sess.lock_file, foreign_lease)
    bridge_module.update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": "foreign-instance",
            "ownerPid": 12345,
            "leaseExpiresAt": lease_expires_at,
            "status": "running",
            "activeScheduleId": "schedule-1",
        },
    )
    job = {"sessionId": sess.session_id, "scheduleId": "schedule-1"}

    bridge_module.release_session_lease(bot, "single:test-user", sess)

    assert sess.lease_owned is False
    assert bridge_module.read_lease(sess.lock_file) == foreign_lease
    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted["ownerInstance"] == "foreign-instance"
    assert persisted["ownerPid"] == 12345
    assert persisted["leaseExpiresAt"] == lease_expires_at
    assert persisted["status"] == "running"
    assert persisted["activeScheduleId"] == "schedule-1"
    assert bridge_module.is_schedule_job_running(job, bridge_module.now_ms()) is True


def test_interrupt_session_preserves_foreign_takeover_record(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    lease_expires_at = bridge_module.now_ms() + 60_000
    bridge_module.write_lease(
        sess.lock_file,
        {
            "instanceId": "foreign-instance",
            "pid": 12345,
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "updatedAt": bridge_module.now_ms(),
            "expiresAt": lease_expires_at,
        },
    )
    bridge_module.update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": "foreign-instance",
            "ownerPid": 12345,
            "leaseExpiresAt": lease_expires_at,
            "status": "running",
            "activeScheduleId": "schedule-1",
        },
    )

    bridge_module.interrupt_session(
        bot,
        "single:test-user",
        sess,
        clear_thread=False,
        clear_chat=False,
        clear_queue=True,
    )

    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted["ownerInstance"] == "foreign-instance"
    assert persisted["ownerPid"] == 12345
    assert persisted["leaseExpiresAt"] == lease_expires_at
    assert persisted["status"] == "running"
    assert persisted["activeScheduleId"] == "schedule-1"


@pytest.mark.asyncio
async def test_interrupt_session_finishes_open_reply_stream_before_cleanup(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sent = []
    bot.ws = make_acking_ws(bot, on_send=sent.append)
    bridge_module.register_reply_session(bot, "req-running", sess)

    class FakeProcess:
        returncode = None

        def terminate(self):
            self.returncode = -15

        async def wait(self):
            return self.returncode

    sess.running = True
    sess.run_task = asyncio.create_task(asyncio.sleep(3600))
    sess.proc = FakeProcess()

    await bridge_module.interrupt_session_and_wait(bot, "single:test-user")

    stream_payloads = [payload for payload in sent if payload.get("cmd") == "aibot_respond_msg"]
    assert stream_payloads
    assert stream_payloads[-1]["headers"]["req_id"] == "req-running"
    assert stream_payloads[-1]["body"]["stream"] == {
        "id": sess.session_id,
        "finish": True,
        "content": "任务已中断。",
    }
    assert "req-running" not in bot.reply_sessions


def test_owned_session_record_update_skips_after_foreign_takeover(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    lease_expires_at = bridge_module.now_ms() + 60_000
    foreign_lease = {
        "instanceId": "foreign-instance",
        "pid": 12345,
        "botId": bot.config["id"],
        "chatKey": "single:test-user",
        "updatedAt": bridge_module.now_ms(),
        "expiresAt": lease_expires_at,
    }
    bridge_module.write_lease(sess.lock_file, foreign_lease)
    bridge_module.update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": "foreign-instance",
            "ownerPid": 12345,
            "leaseExpiresAt": lease_expires_at,
            "status": "running",
            "activeScheduleId": "schedule-1",
        },
    )

    updated = bridge_module.update_owned_session_record(
        bot,
        "single:test-user",
        sess,
        lambda record: {**record, "status": "idle", "activeScheduleId": None},
    )

    assert updated is None
    assert sess.lease_owned is False
    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted["ownerInstance"] == "foreign-instance"
    assert persisted["status"] == "running"
    assert persisted["activeScheduleId"] == "schedule-1"


@pytest.mark.asyncio
async def test_abandon_session_after_lease_loss_cancels_local_run_without_touching_foreign_record(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    lease_expires_at = bridge_module.now_ms() + 60_000
    foreign_lease = {
        "instanceId": "foreign-instance",
        "pid": 12345,
        "botId": bot.config["id"],
        "chatKey": "single:test-user",
        "updatedAt": bridge_module.now_ms(),
        "expiresAt": lease_expires_at,
    }
    bridge_module.write_lease(sess.lock_file, foreign_lease)
    bridge_module.update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": "foreign-instance",
            "ownerPid": 12345,
            "leaseExpiresAt": lease_expires_at,
            "status": "running",
            "activeScheduleId": "schedule-1",
        },
    )
    started = asyncio.Event()

    async def wait_forever():
        started.set()
        await asyncio.Event().wait()

    class FakeProcess:
        returncode = None
        killed = False

        def kill(self):
            self.killed = True
            self.returncode = -9

    task = asyncio.create_task(wait_forever())
    await started.wait()
    process = FakeProcess()
    sess.running = True
    sess.run_task = task
    sess.proc = process
    sess.queue.append({"text": "next", "reqId": "req-next"})
    sess.pending_final_payload = {"headers": {"req_id": "req-current"}}

    bridge_module.abandon_session_after_lease_loss(bot, "single:test-user", sess)
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.killed is True
    assert sess.running is False
    assert sess.queue == [{"text": "next", "reqId": "req-next"}]
    assert sess.pending_final_payload is None
    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted["ownerInstance"] == "foreign-instance"
    assert persisted["status"] == "running"
    assert persisted["activeScheduleId"] == "schedule-1"


@pytest.mark.asyncio
async def test_lease_renew_loop_renews_lease_during_pending_final_delivery(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = False
    sess.pending_final_payload = {"headers": {"req_id": "req-final"}}
    renewed = []

    async def stop_after_first_sleep(_delay):
        bridge_module.SHUTDOWN_EVENT.set()

    def fake_renew(_bot, key, _sess):
        renewed.append(key)
        return True

    monkeypatch.setattr(bridge_module.asyncio, "sleep", stop_after_first_sleep)
    monkeypatch.setattr(bridge_module, "renew_session_lease", fake_renew)

    await bridge_module.lease_renew_loop()

    assert renewed == ["single:test-user"]


def test_normalize_session_record_rejects_path_like_session_id(bridge_module):
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "../escape",
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", "escape")),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        }
    )

    assert record is None


def test_normalize_session_record_rejects_lock_file_outside_managed_roots(bridge_module):
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "sess-1",
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "lockFile": "/tmp/not-managed.lock.json",
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        }
    )

    assert record is None


def test_normalize_session_record_accepts_legacy_managed_lock_file_path(bridge_module):
    legacy_lock = bridge_module.legacy_managed_session_lock_file("bot-1", "sess-1")
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "sess-1",
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "lockFile": str(legacy_lock),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        }
    )

    assert record is not None
    assert record["lockFile"] == str(legacy_lock)


def test_normalize_session_record_infers_bot_id_from_managed_lock_file(bridge_module):
    lock_file = bridge_module.get_managed_session_lock_file("bot-1", "sess-1")
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "sess-1",
            "chatKey": "single:test-user",
            "lockFile": str(lock_file),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        }
    )

    assert record is not None
    assert record["botId"] == "bot-1"


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


def test_create_session_record_recovers_existing_session_missing_bot_id_when_managed_lock_matches(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    session_id = "sess-managed"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "chatKey": key,
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        },
    )

    record = bridge_module.create_session_record(bot, key)

    assert record["sessionId"] == session_id
    stored_key = bridge_module.read_json_file(bridge_module.get_registry_key_file(bot.config["id"], key), None)
    assert stored_key is not None
    assert stored_key["sessionId"] == session_id


def test_create_session_record_recovers_existing_session_missing_chat_key_when_key_file_matches(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    session_id = "sess-missing-chat"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": bot.config["id"],
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        },
    )
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": bot.config["id"], "chatKey": key}, ensure_ascii=False),
        encoding="utf-8",
    )

    record = bridge_module.create_session_record(bot, key)

    assert record["sessionId"] == session_id
    stored_record = bridge_module.read_json_file(bridge_module.get_registry_session_file(session_id), None)
    assert stored_record is not None
    assert stored_record["chatKey"] == key


def test_read_session_record_by_key_repairs_key_file_missing_bot_id_using_recoverable_session(bridge_module):
    bot = make_bot(bridge_module)
    key = "single:test-user"
    session_id = "sess-key-missing-botid"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": bot.config["id"],
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        },
    )
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "chatKey": key}, ensure_ascii=False),
        encoding="utf-8",
    )

    record = bridge_module.read_session_record_by_key(bot.config["id"], key)

    assert record is not None
    assert record["sessionId"] == session_id
    stored_key = bridge_module.read_json_file(key_file, None)
    assert stored_key is not None
    assert stored_key["botId"] == bot.config["id"]
    assert stored_key["chatKey"] == key


def test_read_session_record_by_id_recovers_missing_bot_id_from_managed_lock_file(bridge_module):
    session_id = "sess-1"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "chatKey": "single:test-user",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        },
    )

    record = bridge_module.read_session_record_by_id(session_id)

    assert record is not None
    assert record["botId"] == "bot-1"


def test_read_session_record_by_id_recovers_missing_chat_key_from_key_registry(bridge_module):
    session_id = "sess-missing-chatkey"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        },
    )
    key = "single:test-user"
    key_file = bridge_module.get_registry_key_file("bot-1", key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": "bot-1", "chatKey": key}, ensure_ascii=False),
        encoding="utf-8",
    )

    record = bridge_module.read_session_record_by_id(session_id)

    assert record is not None
    assert record["botId"] == "bot-1"
    assert record["chatKey"] == key


def test_read_session_record_by_id_recovers_missing_chat_key_from_raw_bot_id_and_key_registry(bridge_module):
    session_id = "sess-missing-chatkey-raw-bot"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        },
    )
    key = "single:test-user"
    key_file = bridge_module.get_registry_key_file("bot-1", key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": "bot-1", "chatKey": key}, ensure_ascii=False),
        encoding="utf-8",
    )

    record = bridge_module.read_session_record_by_id(session_id)

    assert record is not None
    assert record["botId"] == "bot-1"
    assert record["chatKey"] == key


def test_read_session_record_by_id_prefers_latest_key_registry_entry_when_multiple_match(bridge_module):
    session_id = "sess-key-latest"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        },
    )
    old_key = bridge_module.get_registry_key_file("bot-1", "single:old-user")
    new_key = bridge_module.get_registry_key_file("bot-1", "single:new-user")
    bridge_module.ensure_dir_for(old_key)
    bridge_module.write_json_atomic(
        old_key,
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "chatKey": "single:old-user",
            "createdAt": 1,
            "updatedAt": 1,
        },
    )
    bridge_module.write_json_atomic(
        new_key,
        {
            "sessionId": session_id,
            "botId": "bot-1",
            "chatKey": "single:new-user",
            "createdAt": 2,
            "updatedAt": 2,
        },
    )

    record = bridge_module.read_session_record_by_id(session_id)

    assert record is not None
    assert record["chatKey"] == "single:new-user"


def test_read_session_record_by_id_recovers_chat_key_from_key_file_name_when_field_missing(bridge_module):
    session_id = "sess-key-name-fallback"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        },
    )
    key = "single:test-user"
    key_file = bridge_module.get_registry_key_file("bot-1", key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": "bot-1"}, ensure_ascii=False),
        encoding="utf-8",
    )

    record = bridge_module.read_session_record_by_id(session_id)

    assert record is not None
    assert record["chatKey"] == key


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


def test_control_session_repairs_missing_key_mapping(bridge_module):
    bot = make_bot(bridge_module)
    key = "group-user:room-1:test-user"
    record = bridge_module.create_session_record(bot, key)
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    key_file.unlink()

    result = bridge_module.control_session(bot, key, clear_thread=False, clear_chat=False)

    assert result["sessionId"] == record["sessionId"]
    repaired_key = bridge_module.read_json_file(key_file, None)
    assert repaired_key is not None
    assert repaired_key["sessionId"] == record["sessionId"]


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


def test_get_or_create_session_revokes_local_binding_after_owner_changes(bridge_module):
    bot = make_bot(bridge_module)
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "new-owner"
    record = bridge_module.create_session_record(bot, "single:former-owner")
    bridge_module.update_session_record(
        record["sessionId"],
        lambda current: {
            **current,
            "threadId": "local-thread",
            "useGlobalCodexSessions": True,
            "recentChat": [{"role": "user", "text": "local conversation"}],
            "localResumeBackup": {
                "threadId": "bridge-thread",
                "recentChat": [{"role": "user", "text": "bridge conversation"}],
                "useGlobalCodexSessions": False,
            },
        },
    )

    sess = bridge_module.get_or_create_session(bot, "single:former-owner")

    persisted = bridge_module.read_session_record_by_id(record["sessionId"])
    assert sess.thread_id == "bridge-thread"
    assert sess.use_global_codex_sessions is False
    assert [item["text"] for item in sess.chat] == ["bridge conversation"]
    assert persisted.get("threadId") == "bridge-thread"
    assert persisted.get("useGlobalCodexSessions") is False
    assert persisted.get("localResumeBackup") is None


def test_get_or_create_session_revokes_preexisting_duplicate_local_binding(bridge_module):
    bot = make_bot(bridge_module)
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = bridge_module.create_session_record(bot, "single:test-user")
    other = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        current["sessionId"],
        lambda record: {
            **record,
            "threadId": "duplicate-local-thread",
            "useGlobalCodexSessions": True,
            "recentChat": [{"role": "user", "text": "local conversation"}],
            "localResumeBackup": {
                "threadId": "bridge-thread",
                "recentChat": [{"role": "user", "text": "bridge conversation"}],
                "useGlobalCodexSessions": False,
            },
        },
    )
    bridge_module.update_session_record(
        other["sessionId"],
        lambda record: {
            **record,
            "threadId": "duplicate-local-thread",
            "useGlobalCodexSessions": True,
        },
    )

    sess = bridge_module.get_or_create_session(bot, "single:test-user")

    persisted = bridge_module.read_session_record_by_id(current["sessionId"])
    assert sess.thread_id == "bridge-thread"
    assert sess.use_global_codex_sessions is False
    assert [item["text"] for item in sess.chat] == ["bridge conversation"]
    assert persisted.get("threadId") == "bridge-thread"
    assert persisted.get("useGlobalCodexSessions") is False
    assert persisted.get("localResumeBackup") is None


@pytest.mark.asyncio
async def test_start_bot_clears_session_runtime_state_when_workdir_changes(bridge_module, monkeypatch):
    old_work_dir = bridge_module.BASE_DIR / "old-workdir"
    new_work_dir = bridge_module.BASE_DIR / "new-workdir"
    old_work_dir.mkdir(parents=True, exist_ok=True)
    new_work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, config_id="bot-1", work_dir=str(old_work_dir))
    sess = make_session(bridge_module, bot)
    sess.thread_id = "thread-1"
    sess.chat.append({"role": "user", "text": "hello"})
    sess.queue.append({"text": "queued", "reqId": "req-1"})

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal", **_kwargs):
        return None

    monkeypatch.setattr(bridge_module, "stop_bot", fake_stop_bot)

    await bridge_module.start_bot(
        {
            "id": bot.config["id"],
            "name": bot.config["name"],
            "botId": bot.config["botId"],
            "secret": "secret",
            "workDir": str(new_work_dir),
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )

    assert sess.thread_id is None
    assert sess.chat == []
    assert sess.queue == []


@pytest.mark.asyncio
async def test_start_bot_recreates_session_record_when_workspace_identity_changes(bridge_module, monkeypatch):
    old_work_dir = bridge_module.BASE_DIR / "old-workdir"
    new_work_dir = bridge_module.BASE_DIR / "new-workdir"
    old_work_dir.mkdir(parents=True, exist_ok=True)
    new_work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, config_id="bot-1", work_dir=str(old_work_dir))
    record = bridge_module.create_session_record(bot, "single:test-user")
    old_session_id = record["sessionId"]
    old_lock_file = Path(record["lockFile"])
    bridge_module.ensure_dir_for(old_lock_file)
    old_lock_file.write_text("{}", encoding="utf-8")
    old_chatfile = bridge_module.get_chatfile_dir(old_session_id)
    old_chatfile.mkdir(parents=True, exist_ok=True)
    (old_chatfile / "stale.txt").write_text("stale", encoding="utf-8")
    old_codex_home = bridge_module.get_session_codex_home_root(old_session_id)
    old_codex_home.mkdir(parents=True, exist_ok=True)
    (old_codex_home / "state.txt").write_text("state", encoding="utf-8")

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal", **_kwargs):
        return None

    monkeypatch.setattr(bridge_module, "stop_bot", fake_stop_bot)

    await bridge_module.start_bot(
        {
            "id": bot.config["id"],
            "name": bot.config["name"],
            "botId": bot.config["botId"],
            "secret": "secret",
            "workDir": str(new_work_dir),
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )

    new_record = bridge_module.create_session_record(bridge_module.BOTS["bot-1"], "single:test-user")

    assert new_record["sessionId"] != old_session_id
    assert bridge_module.read_session_record_by_id(old_session_id) is None
    assert not old_lock_file.exists()
    assert not old_chatfile.exists()
    assert not old_codex_home.exists()


@pytest.mark.asyncio
async def test_start_bot_rebinds_schedule_definition_session_id_when_workspace_identity_changes(bridge_module, monkeypatch):
    old_work_dir = bridge_module.BASE_DIR / "old-workdir"
    new_work_dir = bridge_module.BASE_DIR / "new-workdir"
    old_work_dir.mkdir(parents=True, exist_ok=True)
    new_work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, config_id="bot-1", work_dir=str(old_work_dir))
    record = bridge_module.create_session_record(bot, "single:test-user")
    old_session_id = record["sessionId"]
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": old_session_id,
            "chatKey": "single:test-user",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal", **_kwargs):
        return None

    monkeypatch.setattr(bridge_module, "stop_bot", fake_stop_bot)

    await bridge_module.start_bot(
        {
            "id": bot.config["id"],
            "name": bot.config["name"],
            "botId": bot.config["botId"],
            "secret": "secret",
            "workDir": str(new_work_dir),
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )

    updated = bridge_module.read_schedule_definition(definition["scheduleId"])
    assert updated is not None
    assert updated["sessionId"] != old_session_id


@pytest.mark.asyncio
async def test_start_bot_rebinds_scheduled_job_session_id_when_workspace_identity_changes(bridge_module, monkeypatch):
    old_work_dir = bridge_module.BASE_DIR / "old-workdir"
    new_work_dir = bridge_module.BASE_DIR / "new-workdir"
    old_work_dir.mkdir(parents=True, exist_ok=True)
    new_work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, config_id="bot-1", work_dir=str(old_work_dir))
    record = bridge_module.create_session_record(bot, "single:test-user")
    old_session_id = record["sessionId"]
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-workspace-switch.json"
    bridge_module.ensure_dir(processing.parent)
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-workspace-switch",
            "scheduleId": "sched-1",
            "botId": bot.config["id"],
            "sessionId": old_session_id,
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal", **_kwargs):
        return None

    monkeypatch.setattr(bridge_module, "stop_bot", fake_stop_bot)

    await bridge_module.start_bot(
        {
            "id": bot.config["id"],
            "name": bot.config["name"],
            "botId": bot.config["botId"],
            "secret": "secret",
            "workDir": str(new_work_dir),
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )

    updated = bridge_module.read_json_file(processing, None)
    assert updated is not None
    assert updated["sessionId"] != old_session_id


@pytest.mark.asyncio
async def test_start_bot_rebinds_scheduled_job_session_id_when_job_bot_id_missing_but_definition_matches(
    bridge_module, monkeypatch
):
    old_work_dir = bridge_module.BASE_DIR / "old-workdir"
    new_work_dir = bridge_module.BASE_DIR / "new-workdir"
    old_work_dir.mkdir(parents=True, exist_ok=True)
    new_work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, config_id="bot-1", work_dir=str(old_work_dir))
    record = bridge_module.create_session_record(bot, "single:test-user")
    old_session_id = record["sessionId"]
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": old_session_id,
            "chatKey": "single:test-user",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-workspace-switch-missing-bot.json"
    bridge_module.ensure_dir(processing.parent)
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-workspace-switch-missing-bot",
            "scheduleId": definition["scheduleId"],
            "sessionId": old_session_id,
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal", **_kwargs):
        return None

    monkeypatch.setattr(bridge_module, "stop_bot", fake_stop_bot)

    await bridge_module.start_bot(
        {
            "id": bot.config["id"],
            "name": bot.config["name"],
            "botId": bot.config["botId"],
            "secret": "secret",
            "workDir": str(new_work_dir),
            "enabled": True,
            "welcome": "",
            "groupSessionMode": "per-user",
        }
    )

    updated = bridge_module.read_json_file(processing, None)
    assert updated is not None
    assert updated["sessionId"] != old_session_id


def test_build_bridge_context_mentions_skill_dirs(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo"
    work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bridge_module.CODEX_EXEC_MODE = "host"
    workspace_skills = bridge_module.get_workfile_dir(bot.config["id"], "test-user") / ".codex" / "skills" / "workspace-skill"
    workspace_skills.mkdir(parents=True, exist_ok=True)
    (workspace_skills / "SKILL.md").write_text("workspace", encoding="utf-8")
    global_skill = bridge_module.DEFAULT_CODEX_HOME / "skills" / "global-skill"
    global_skill.mkdir(parents=True, exist_ok=True)
    (global_skill / "SKILL.md").write_text("global", encoding="utf-8")
    sess = make_session(bridge_module, bot)

    context = bridge_module.build_bridge_context(bot, sess, "single:test-user")

    assert f"CWD_DIR: {bridge_module.get_workfile_dir(bot.config['id'], 'test-user')}" in context
    assert f"WORKSPACE_CODEX_SKILLS_DIR: {bridge_module.get_workfile_dir(bot.config['id'], 'test-user') / '.codex' / 'skills'}" in context
    assert "HOME_CODEX_SKILLS_DIR: ~/.codex/skills" in context
    assert "Export send-back files under CHATFILE_DIR." in context
    assert "User-global Codex skills should live in ~/.codex/skills." in context
    assert "Personal Codex skills should live in CWD_DIR/.codex/skills." in context


def test_build_bridge_context_uses_lightweight_workspace_prepare(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))
    Path(bot.config["workDir"]).mkdir(parents=True, exist_ok=True)
    sess = make_session(bridge_module, bot)
    calls = []

    original_light = bridge_module.ensure_session_workspace_dirs_light

    def fake_light(_bot, key):
        calls.append(("light", key))
        return original_light(_bot, key)

    def fail_bootstrap(*_args, **_kwargs):
        raise AssertionError("heavy bootstrap should not be called from build_bridge_context")

    monkeypatch.setattr(bridge_module, "ensure_session_workspace_dirs_light", fake_light)
    monkeypatch.setattr(bridge_module, "bootstrap_workspace_from_workdir", fail_bootstrap)

    bridge_module.build_bridge_context(bot, sess, "single:test-user")

    assert calls
    assert all(item == ("light", "single:test-user") for item in calls)


def test_start_sh_exports_runtime_tuning_envs():
    content = START_SH_PATH.read_text(encoding="utf-8")

    assert "load_bridge_runtime_env" in content
    assert "export_bridge_runtime_env" in content
    assert 'nohup setsid "$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py"' in content
    assert 'nohup "$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py"' in content
    assert ".bridge.guard.pid" in content
    assert 'CONTROL_STATE_ROOT="$(prepare_bridge_state_root' in content
    assert 'if is_truthy "$CONTROL_STATE_IS_CUSTOM"; then' in content
    assert 'bridge_pid_matches_instance_process' in content


def test_prepare_bridge_state_root_does_not_follow_predictable_probe_symlink(tmp_path):
    state_root = tmp_path / "state"
    state_root.mkdir()
    victim = tmp_path / "victim.txt"
    victim.write_text("keep-content", encoding="utf-8")

    result = subprocess.run(
        [
            "sh",
            "-c",
            (
                '. "$1/bridge_env.sh"; '
                'ln -s "$3" "$2/.bridge-write-test.$$"; '
                'prepare_bridge_state_root "$1" "$2" "TEST_STATE_ROOT"'
            ),
            "sh",
            str(REPO_ROOT),
            str(state_root),
            str(victim),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(state_root.resolve())
    assert victim.read_text(encoding="utf-8") == "keep-content"


def test_prepare_bridge_state_root_accepts_group_writable_directory(tmp_path):
    state_root = tmp_path / "state"
    state_root.mkdir()
    state_root.chmod(0o770)

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; prepare_bridge_state_root "$1" "$2" "TEST_STATE_ROOT"',
            "sh",
            str(REPO_ROOT),
            str(state_root),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(state_root.resolve())


def test_prepare_bridge_state_root_rejects_world_writable_directory(tmp_path):
    state_root = tmp_path / "state"
    state_root.mkdir()
    state_root.chmod(0o777)

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; prepare_bridge_state_root "$1" "$2" "TEST_STATE_ROOT"',
            "sh",
            str(REPO_ROOT),
            str(state_root),
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode != 0
    assert "TEST_STATE_ROOT must not be world-writable" in result.stderr


def test_shutdown_reason_writer_replaces_symlink_without_touching_victim(tmp_path):
    victim = tmp_path / "victim.txt"
    victim.write_text("keep-content", encoding="utf-8")
    state_file = tmp_path / "shutdown.json"
    state_file.symlink_to(victim)

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; write_bridge_shutdown_reason_file "$2" "watchdog_restart"',
            "sh",
            str(REPO_ROOT),
            str(state_file),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert victim.read_text(encoding="utf-8") == "keep-content"
    assert not state_file.is_symlink()
    assert json.loads(state_file.read_text(encoding="utf-8"))["reason"] == "watchdog_restart"
    assert state_file.stat().st_mode & 0o777 == 0o600


def test_start_sh_cleans_watchdog_on_failed_start():
    content = START_SH_PATH.read_text(encoding="utf-8")

    assert 'LAUNCHED_PID=$!' in content
    assert 'bridge_pid_matches_instance_process "$SCRIPT_DIR" "$CONTROL_STATE_ROOT" false "$LAUNCHED_KIND" "$LAUNCHED_PID"' in content
    assert 'kill "$LAUNCHED_PID" 2>/dev/null || true' in content
    assert 'rm -f "$GUARD_PID_FILE" "$PID_FILE"' in content


def test_start_sh_fails_and_cleans_bridge_when_watchdog_exits_during_startup(tmp_path, unused_tcp_port):
    script_dir = tmp_path / "bridge-scripts"
    control_root = tmp_path / "control"
    runtime_root = tmp_path / "runtime"
    script_dir.mkdir()
    control_root.mkdir()
    for source in (START_SH_PATH, BRIDGE_ENV_SH_PATH, BRIDGE_RUNTIME_CONFIG_PATH):
        shutil.copy2(source, script_dir / source.name)
    (script_dir / "bridge.py").write_text(
        "import time\nwhile True:\n    time.sleep(1)\n",
        encoding="utf-8",
    )
    (script_dir / "bridge_watchdog.sh").write_text(
        "#!/bin/sh\n"
        'SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"\n'
        'python3 "$SCRIPT_DIR/bridge.py" </dev/null >> "$BRIDGE_CONTROL_ROOT/bridge.log" 2>&1 &\n'
        "child_pid=$!\n"
        'printf "%s\\n" "$child_pid" > "$BRIDGE_CONTROL_ROOT/.bridge.pid"\n'
        'printf "%s\\n" "$child_pid" > "$BRIDGE_CONTROL_ROOT/launched-bridge.pid"\n'
        "sleep 0.2\n"
        "exit 0\n",
        encoding="utf-8",
    )
    env_file = tmp_path / "instance.env"
    env_file.write_text(
        f"BRIDGE_CONTROL_ROOT='{control_root}'\n"
        f"BRIDGE_RUNTIME_ROOT='{runtime_root}'\n"
        f"BRIDGE_BIND='127.0.0.1:{unused_tcp_port}'\n"
        "BRIDGE_WATCHDOG_ENABLED='true'\n",
        encoding="utf-8",
    )
    env = {
        key: os.environ[key]
        for key in ("HOME", "PATH", "PYTHONPATH")
        if key in os.environ
    }
    env["BRIDGE_ENV_FILE"] = str(env_file)
    env["BRIDGE_ENV_FILE_REQUIRED"] = "true"
    bridge_pid = 0

    try:
        result = subprocess.run(
            ["sh", str(script_dir / "start.sh")],
            cwd=script_dir,
            capture_output=True,
            text=True,
            env=env,
            timeout=20,
        )
        bridge_pid = int((control_root / "launched-bridge.pid").read_text(encoding="utf-8").strip())

        assert result.returncode == 1
        assert "start failed" in result.stdout
        assert not (control_root / ".bridge.guard.pid").exists()
        assert not (control_root / ".bridge.pid").exists()
        with pytest.raises(ProcessLookupError):
            os.kill(bridge_pid, 0)
    finally:
        if bridge_pid:
            with contextlib.suppress(ProcessLookupError):
                os.kill(bridge_pid, 9)


def test_env_example_declares_watchdog_controls():
    content = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")

    assert "BRIDGE_WATCHDOG_ENABLED=true" in content
    assert "BRIDGE_WATCHDOG_POLL_SEC=5" in content
    assert "BRIDGE_WATCHDOG_FAIL_THRESHOLD=3" in content
    assert "BRIDGE_WATCHDOG_COOLDOWN_SEC=60" in content


def test_watchdog_script_exists_and_uses_livez_probe():
    content = WATCHDOG_SH_PATH.read_text(encoding="utf-8")

    assert "export_bridge_runtime_env" in content
    assert 'setsid "$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py"' in content
    assert '"$BRIDGE_PYTHON" "$SCRIPT_DIR/bridge.py"' in content
    assert "GET /" not in content
    assert "bridge_health_ok()" in content
    assert "/livez" in content
    assert ".bridge.guard.pid" in content
    assert "restart triggered" in content
    assert "bridge recovered before restart; skip restart" in content
    assert 'CONTROL_STATE_ROOT="$(prepare_bridge_state_root' in content
    assert 'bridge_pid_matches_instance_process' in content
    assert 'load_bridge_runtime_env "$SCRIPT_DIR" || exit 1' in content.split("start_bridge_process() {", 1)[1]


def test_watchdog_preserves_fatal_exit_when_guard_pid_target_is_directory(tmp_path):
    control_root = tmp_path / "control"
    runtime_root = tmp_path / "runtime"
    control_root.mkdir()
    (control_root / ".bridge.guard.pid").mkdir()
    env_file = tmp_path / "instance.env"
    env_file.write_text(
        f"BRIDGE_CONTROL_ROOT='{control_root}'\n"
        f"BRIDGE_RUNTIME_ROOT='{runtime_root}'\n"
        "BRIDGE_BIND='127.0.0.1:19490'\n",
        encoding="utf-8",
    )
    env = {
        key: os.environ[key]
        for key in ("HOME", "PATH", "PYTHONPATH")
        if key in os.environ
    }
    env["BRIDGE_ENV_FILE"] = str(env_file)
    env["BRIDGE_ENV_FILE_REQUIRED"] = "true"

    result = subprocess.run(
        ["sh", str(WATCHDOG_SH_PATH)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )

    assert result.returncode == 1
    assert "failed to write watchdog PID file" in (control_root / "bridge.log").read_text(encoding="utf-8")
    assert list((control_root / ".bridge.guard.pid").iterdir()) == []
    assert not (control_root / ".bridge.pid").exists()


def test_check_bridge_health_uses_shared_runtime_schedule_dirs(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "check_bridge_health.sh").write_text(
        CHECK_BRIDGE_HEALTH_SH_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    shared_runtime_root = tmp_path / "shared-runtime"
    processing_root = shared_runtime_root / ".scheduled-messages" / "processing"
    pending_root = shared_runtime_root / ".scheduled-messages" / "pending"
    processing_root.mkdir(parents=True, exist_ok=True)
    pending_root.mkdir(parents=True, exist_ok=True)
    (processing_root / "job-1.json").write_text("{}", encoding="utf-8")
    (pending_root / "job-2.json").write_text("{}", encoding="utf-8")
    (pending_root / "job-3.json").write_text("{}", encoding="utf-8")

    python_hook_dir = tmp_path / "python-hook"
    python_hook_dir.mkdir(parents=True, exist_ok=True)
    (python_hook_dir / "sitecustomize.py").write_text(
        """
import io
import json
import urllib.request

ROOT = json.dumps({"ok": True}).encode("utf-8")
BOTS = json.dumps([{"id": "bot-1", "name": "codex1", "status": "running", "enabled": True, "sessions": []}]).encode("utf-8")

class _Resp:
    def __init__(self, body):
        self._body = body
    def read(self):
        return self._body
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

def _fake_urlopen(req, timeout=None):
    url = req.full_url if hasattr(req, "full_url") else str(req)
    if url.endswith("/api/bots"):
        return _Resp(BOTS)
    if url.endswith("/livez"):
        return _Resp(ROOT)
    raise RuntimeError(f"unexpected url: {url}")

urllib.request.urlopen = _fake_urlopen
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (script_dir / ".env").write_text(
        "BRIDGE_BIND=127.0.0.1:19399\n"
        f"BRIDGE_SHARED_RUNTIME_ROOT={shared_runtime_root}\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
    env["PYTHONPATH"] = (
        f"{python_hook_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(python_hook_dir)
    )

    result = subprocess.run(
        ["sh", str(script_dir / "check_bridge_health.sh")],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert "bridge root: ok" in result.stdout
    assert "bots: 1" in result.stdout
    assert "scheduled processing=1 pending=2" in result.stdout


def test_check_restart_noise_uses_configured_api_auth(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "check_restart_noise.sh").write_text(
        CHECK_RESTART_NOISE_SH_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "start.sh").write_text("#!/bin/sh\n: > bridge.log\n: > bridge.log.prev\nexit 0\n", encoding="utf-8")
    (script_dir / "bridge.log").write_text("", encoding="utf-8")
    (script_dir / "bridge.log.prev").write_text("", encoding="utf-8")

    python_hook_dir = tmp_path / "python-hook"
    python_hook_dir.mkdir(parents=True, exist_ok=True)
    (python_hook_dir / "sitecustomize.py").write_text(
        """
import json
import urllib.request

ROOT = json.dumps({"ok": True}).encode("utf-8")
EXPECTED = "Bearer token-from-env"
EXPECTED_URL = "http://127.0.0.1:19399/healthz"

class _Resp:
    def __init__(self, body):
        self._body = body
    def read(self):
        return self._body
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

def _fake_urlopen(req, timeout=None):
    header = req.headers.get("Authorization") if hasattr(req, "headers") else ""
    url = req.full_url if hasattr(req, "full_url") else str(req)
    if header != EXPECTED:
        raise RuntimeError(f"unexpected auth header: {header!r}")
    if url != EXPECTED_URL:
        raise RuntimeError(f"unexpected url: {url!r}")
    return _Resp(ROOT)

urllib.request.urlopen = _fake_urlopen
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (script_dir / ".env").write_text(
        "BRIDGE_BIND=127.0.0.1:19399\nBRIDGE_TOKEN=token-from-env\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
    env["PYTHONPATH"] = (
        f"{python_hook_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(python_hook_dir)
    )

    result = subprocess.run(
        ["sh", str(script_dir / "check_restart_noise.sh")],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert "health ok: http://127.0.0.1:19399/healthz" in result.stdout
    assert "restart noise check passed" in result.stdout


def test_check_restart_noise_uses_healthz_not_root(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "check_restart_noise.sh").write_text(
        CHECK_RESTART_NOISE_SH_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "start.sh").write_text("#!/bin/sh\n: > bridge.log\n: > bridge.log.prev\nexit 0\n", encoding="utf-8")
    (script_dir / "bridge.log").write_text("", encoding="utf-8")
    (script_dir / "bridge.log.prev").write_text("", encoding="utf-8")

    python_hook_dir = tmp_path / "python-hook"
    python_hook_dir.mkdir(parents=True, exist_ok=True)
    (python_hook_dir / "sitecustomize.py").write_text(
        """
import json
import urllib.request

ROOT = json.dumps({"ok": True}).encode("utf-8")
HEALTHZ = json.dumps({"ok": False}).encode("utf-8")

class _Resp:
    def __init__(self, body):
        self._body = body
    def read(self):
        return self._body
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc, tb):
        return False

def _fake_urlopen(req, timeout=None):
    url = req.full_url if hasattr(req, "full_url") else str(req)
    if url.endswith("/healthz"):
        return _Resp(HEALTHZ)
    if url.endswith("/"):
        return _Resp(ROOT)
    raise RuntimeError(f"unexpected url: {url!r}")

urllib.request.urlopen = _fake_urlopen
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (script_dir / ".env").write_text(
        "BRIDGE_BIND=127.0.0.1:19399\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
    env["PYTHONPATH"] = (
        f"{python_hook_dir}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(python_hook_dir)
    )

    result = subprocess.run(
        ["sh", str(script_dir / "check_restart_noise.sh")],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 1
    assert "health check failed" in result.stderr


def test_deploy_from_codex_quotes_env_values_for_paths_with_spaces(tmp_path):
    script_dir = tmp_path / "bridge scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    work_root = tmp_path / "work root"
    target_repo_dir = work_root / "demo-repo"
    (target_repo_dir / ".git").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "init"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "remote", "add", "origin", "git@github.com:example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    input_secret = tmp_path / "input.secret"
    input_secret.write_text("secret-value\n", encoding="utf-8")

    subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret-file",
            str(input_secret),
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-install",
            "--skip-start",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    env_file = script_dir / ".env"
    assert env_file.exists()
    assert env_file.stat().st_mode & 0o777 == 0o600
    assert list(script_dir.glob(".env.tmp.*")) == []

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; printf "%s\\n%s\\n%s\\n" "$WORK_DIR" "$WECOM_BOT_WORK_DIR" "$WECOM_BOT_SECRET_FILE"',
            "sh",
            str(script_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    lines = result.stdout.strip().splitlines()
    assert lines[0] == str(target_repo_dir)
    assert lines[1] == str(target_repo_dir)
    assert lines[2] == str(script_dir / ".secrets" / "demo-repo.secret")


def test_deploy_from_codex_installs_dependencies_in_project_venv(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(
        DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")
    input_secret = tmp_path / "input.secret"
    input_secret.write_text("secret-value\n", encoding="utf-8")
    work_root = tmp_path / "work-root"
    target_repo_dir = work_root / "demo-repo"
    target_repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=target_repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "git@github.com:example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret-file",
            str(input_secret),
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-start",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    venv_python = script_dir / ".venv" / "bin" / "python"
    assert venv_python.is_file()
    env_content = (script_dir / ".env").read_text(encoding="utf-8")
    assert f"BRIDGE_PYTHON='{venv_python}'" in env_content


def test_deploy_from_codex_skip_install_keeps_system_python_without_existing_venv(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(
        DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")
    input_secret = tmp_path / "input.secret"
    input_secret.write_text("secret-value\n", encoding="utf-8")
    work_root = tmp_path / "work-root"
    target_repo_dir = work_root / "demo-repo"
    target_repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=target_repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "git@github.com:example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret-file",
            str(input_secret),
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "BRIDGE_PYTHON='python3'" in (script_dir / ".env").read_text(encoding="utf-8")
    assert not (script_dir / ".venv").exists()


def test_deploy_from_codex_copies_secret_file_with_private_permissions(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(
        DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")
    input_secret = tmp_path / "input.secret"
    input_secret.write_text("secret-value\n", encoding="utf-8")
    work_root = tmp_path / "work-root"
    target_repo_dir = work_root / "demo-repo"
    target_repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=target_repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "git@github.com:example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret-file",
            str(input_secret),
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    destination = script_dir / ".secrets" / "demo-repo.secret"
    assert destination.read_text(encoding="utf-8") == "secret-value\n"
    assert destination.stat().st_mode & 0o777 == 0o600
    assert destination.parent.stat().st_mode & 0o777 == 0o700
    assert list(destination.parent.glob(".*.tmp.*")) == []
    assert "secret-value" not in result.stdout
    assert "secret-value" not in result.stderr


def test_deploy_from_codex_replaces_env_symlink_without_touching_target(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(
        DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")
    input_secret = tmp_path / "input.secret"
    input_secret.write_text("secret-value\n", encoding="utf-8")
    work_root = tmp_path / "work-root"
    target_repo_dir = work_root / "demo-repo"
    target_repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=target_repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "git@github.com:example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    victim = tmp_path / "victim.env"
    victim.write_text("KEEP=true\n", encoding="utf-8")
    env_file = script_dir / ".env"
    env_file.symlink_to(victim)

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret-file",
            str(input_secret),
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert victim.read_text(encoding="utf-8") == "KEEP=true\n"
    assert env_file.is_file() and not env_file.is_symlink()
    assert "WECOM_BOT_ID='bot-id'" in env_file.read_text(encoding="utf-8")
    assert env_file.stat().st_mode & 0o777 == 0o600
    assert list(script_dir.glob(".env.tmp.*")) == []


@pytest.mark.parametrize("kind", ["missing", "directory", "empty", "unreadable"])
def test_deploy_from_codex_rejects_invalid_secret_file(tmp_path, kind):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(
        DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")
    input_secret = tmp_path / "input.secret"
    if kind == "directory":
        input_secret.mkdir()
    elif kind == "empty":
        input_secret.write_text(" \n", encoding="utf-8")
    elif kind == "unreadable":
        input_secret.write_text("secret-value\n", encoding="utf-8")
        input_secret.chmod(0)

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret-file",
            str(input_secret),
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--skip-install",
            "--skip-start",
            "--dry-run",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    expected_error = {
        "missing": "--bot-secret-file does not exist",
        "directory": "--bot-secret-file is not a regular file",
        "empty": "--bot-secret-file is empty",
        "unreadable": "--bot-secret-file is not readable UTF-8 text",
    }[kind]
    assert expected_error in result.stderr


def test_deploy_from_codex_rejects_both_secret_inputs(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(
        DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")
    input_secret = tmp_path / "input.secret"
    input_secret.write_text("secret-value\n", encoding="utf-8")

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret-file",
            str(input_secret),
            "--bot-secret",
            "legacy-secret",
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "mutually exclusive" in result.stderr


def test_deploy_from_codex_legacy_inline_secret_warns(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(
        DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8"
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret",
            "legacy-secret",
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--skip-install",
            "--skip-start",
            "--dry-run",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "exposes the secret in shell history and process listings" in result.stderr


def test_deploy_from_codex_rejects_path_like_bot_name(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret",
            "secret-value",
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--bot-name",
            "../escape",
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "--bot-name must not contain '/'" in result.stderr
    assert not (script_dir.parent / "escape.secret").exists()


def test_deploy_from_codex_rejects_path_like_repository_name(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret",
            "secret-value",
            "--github-repo",
            "..",
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "--github-repo repository name must not be empty, '.' or '..'" in result.stderr


def test_deploy_from_codex_expands_tilde_work_root(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    home_dir = tmp_path / "home"
    work_root = home_dir / "work-root"
    target_repo_dir = work_root / "demo-repo"
    (target_repo_dir / ".git").mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "init"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ["git", "remote", "add", "origin", "git@github.com:example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    env = dict(os.environ)
    env["HOME"] = str(home_dir)
    subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret",
            "secret-value",
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            "~/work-root",
            "--skip-install",
            "--skip-start",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; printf "%s\\n%s\\n" "$WORK_DIR" "$WECOM_BOT_WORK_DIR"',
            "sh",
            str(script_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    lines = result.stdout.strip().splitlines()
    assert lines == [str(target_repo_dir), str(target_repo_dir)]


def test_deploy_from_codex_rejects_existing_repo_with_mismatched_origin(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    work_root = tmp_path / "work-root"
    target_repo_dir = work_root / "demo-repo"
    target_repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=target_repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "git@github.com:example/other-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret",
            "secret-value",
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "origin mismatch" in result.stderr


def test_deploy_from_codex_accepts_equivalent_ssh_origin_forms(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    work_root = tmp_path / "work-root"
    target_repo_dir = work_root / "demo-repo"
    target_repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=target_repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "ssh://git@github.com/example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret",
            "secret-value",
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Reusing existing git repo" in result.stdout


def test_deploy_from_codex_accepts_equivalent_https_and_ssh_repo_urls(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "deploy_from_codex.sh").write_text(DEPLOY_FROM_CODEX_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / "requirements.txt").write_text("", encoding="utf-8")

    work_root = tmp_path / "work-root"
    target_repo_dir = work_root / "demo-repo"
    target_repo_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init"], cwd=target_repo_dir, check=True, capture_output=True, text=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://github.com/example/demo-repo.git"],
        cwd=target_repo_dir,
        check=True,
        capture_output=True,
        text=True,
    )

    result = subprocess.run(
        [
            "sh",
            str(script_dir / "deploy_from_codex.sh"),
            "--bot-id",
            "bot-id",
            "--bot-secret",
            "secret-value",
            "--github-repo",
            "git@github.com:example/demo-repo.git",
            "--work-root",
            str(work_root),
            "--skip-install",
            "--skip-start",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Reusing existing git repo" in result.stdout


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


def test_runtime_config_strips_ipv6_brackets_from_split_host_port_env():
    module = load_runtime_config_module()

    host, port = module.resolve_host_port({"BRIDGE_HOST": "[::1]", "BRIDGE_PORT": "19301"})

    assert host == "::1"
    assert port == 19301
    assert module.build_bridge_api_base(host, port) == "http://[::1]:19301"


def test_runtime_config_rejects_empty_split_host_after_ipv6_bracket_stripping():
    module = load_runtime_config_module()

    with pytest.raises(ValueError, match="BRIDGE_HOST/HOST cannot be empty"):
        module.resolve_host_port({"BRIDGE_HOST": "[]", "BRIDGE_PORT": "19301"})


def test_runtime_config_rejects_unbracketed_ipv6_bind():
    module = load_runtime_config_module()

    with pytest.raises(ValueError, match="IPv6"):
        module.resolve_host_port({"BRIDGE_BIND": "::1:19301"})


def test_bridge_module_rejects_file_shared_runtime_root(monkeypatch, tmp_path):
    bad_root = tmp_path / "shared-runtime-root.txt"
    bad_root.write_text("not a dir", encoding="utf-8")
    monkeypatch.setenv("BRIDGE_SHARED_RUNTIME_ROOT", str(bad_root))

    with pytest.raises(Exception) as excinfo:
        load_bridge_module()

    assert str(excinfo.value) == f"BRIDGE_SHARED_RUNTIME_ROOT is not a directory: {bad_root.resolve()}"


def test_bridge_module_rejects_file_instance_runtime_root(monkeypatch, tmp_path):
    bad_root = tmp_path / "instance-runtime-root.txt"
    bad_root.write_text("not a dir", encoding="utf-8")
    monkeypatch.setenv("BRIDGE_RUNTIME_ROOT", str(bad_root))

    with pytest.raises(Exception) as excinfo:
        load_bridge_module()

    assert str(excinfo.value) == f"BRIDGE_RUNTIME_ROOT is not a directory: {bad_root.resolve()}"


def test_secure_state_root_accepts_group_writable_directory(bridge_module, tmp_path):
    state_root = tmp_path / "shared-state"
    state_root.mkdir()
    state_root.chmod(0o2770)

    assert bridge_module.ensure_secure_state_root(state_root, "TEST_STATE_ROOT") == state_root


def test_secure_state_root_rejects_world_writable_directory(bridge_module, tmp_path):
    state_root = tmp_path / "shared-state"
    state_root.mkdir()
    state_root.chmod(0o777)

    with pytest.raises(RuntimeError, match="must not be world-writable"):
        bridge_module.ensure_secure_state_root(state_root, "TEST_STATE_ROOT")


def test_secure_state_root_accepts_owner_writable_directory(bridge_module, tmp_path):
    state_root = tmp_path / "private-state"
    state_root.mkdir(mode=0o700)

    assert bridge_module.ensure_secure_state_root(state_root, "TEST_STATE_ROOT") == state_root


def test_bridge_module_rejects_file_send_root_that_is_not_directory(monkeypatch, tmp_path):
    bad_root = tmp_path / "file-send-root.txt"
    bad_root.write_text("not a dir", encoding="utf-8")
    monkeypatch.setenv("FILE_SEND_ROOTS", str(bad_root))

    with pytest.raises(Exception) as excinfo:
        load_bridge_module()

    assert "FILE_SEND_ROOTS entry is not a directory" in str(excinfo.value)


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


def test_bridge_env_load_uses_explicit_env_file_without_reading_default(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / ".env").write_text("BRIDGE_BIND=127.0.0.1:19399\n", encoding="utf-8")
    alternate_env = tmp_path / "isolated.env"
    alternate_env.write_text("BRIDGE_BIND=127.0.0.1:19400\n", encoding="utf-8")
    env = dict(os.environ)
    env["BRIDGE_ENV_FILE"] = str(alternate_env)

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; printf "%s %s\\n%s\\n" "$HOST" "$PORT" "$BRIDGE_ENV_FILE"',
            "sh",
            str(script_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.stdout.strip().splitlines() == ["127.0.0.1 19400", str(alternate_env.resolve())]


def test_bridge_env_load_rejects_directory_env_file(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    env_dir = tmp_path / "env-dir"
    env_dir.mkdir()
    env = dict(os.environ)
    env["BRIDGE_ENV_FILE"] = str(env_dir)

    result = subprocess.run(
        ["sh", "-c", '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"', "sh", str(script_dir)],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 1
    assert f"BRIDGE_ENV_FILE is not a file: {env_dir.resolve()}" in result.stderr


def test_bridge_env_load_rejects_explicit_missing_env_file(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    missing_env = tmp_path / "missing.env"
    env = dict(os.environ)
    env["BRIDGE_ENV_FILE"] = str(missing_env)

    result = subprocess.run(
        ["sh", "-c", '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"', "sh", str(script_dir)],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 1
    assert f"BRIDGE_ENV_FILE does not exist: {missing_env.resolve()}" in result.stderr


def test_bridge_env_default_missing_env_file_remains_optional_across_reloads(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("BRIDGE_ENV_FILE", None)
    env.pop("BRIDGE_ENV_FILE_REQUIRED", None)

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; load_bridge_runtime_env "$1"; '
            'printf "%s %s %s\\n" "$HOST" "$PORT" "$BRIDGE_ENV_FILE_REQUIRED"',
            "sh",
            str(script_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.stdout.strip() == "127.0.0.1 9299 false"


def test_bridge_env_defaults_bridge_python_to_python3(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; export_bridge_runtime_env; '
            'sh -c \'printf "%s\\n" "$BRIDGE_PYTHON"\'',
            "sh",
            str(script_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "python3"


def test_bridge_env_rejects_missing_bridge_python(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / ".env").write_text("BRIDGE_PYTHON=/missing/bridge-python\n", encoding="utf-8")

    result = subprocess.run(
        ["sh", "-c", '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"', "sh", str(script_dir)],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "BRIDGE_PYTHON is not executable: /missing/bridge-python" in result.stderr


def test_start_sh_rejects_control_root_file_before_launch(tmp_path):
    control_file = tmp_path / "control-file"
    control_file.write_text("not a directory", encoding="utf-8")
    runtime_root = tmp_path / "runtime"
    env_file = tmp_path / "instance.env"
    env_file.write_text(
        f"BRIDGE_CONTROL_ROOT='{control_file}'\n"
        f"BRIDGE_RUNTIME_ROOT='{runtime_root}'\n"
        "BRIDGE_BIND='127.0.0.1:19491'\n"
        "BRIDGE_WATCHDOG_ENABLED='false'\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["BRIDGE_ENV_FILE"] = str(env_file)

    result = subprocess.run(
        ["sh", str(START_SH_PATH)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )

    assert result.returncode == 1
    assert f"BRIDGE_CONTROL_ROOT is not a directory: {control_file.resolve()}" in result.stderr
    assert not (control_file / ".bridge.pid").exists()


def test_start_sh_rejects_concurrent_start_for_same_control_root(tmp_path):
    control_root = tmp_path / "control"
    runtime_root = tmp_path / "runtime"
    control_root.mkdir()
    env_file = tmp_path / "instance.env"
    env_file.write_text(
        f"BRIDGE_CONTROL_ROOT='{control_root}'\n"
        f"BRIDGE_RUNTIME_ROOT='{runtime_root}'\n"
        "BRIDGE_BIND='127.0.0.1:19489'\n"
        "BRIDGE_WATCHDOG_ENABLED='false'\n",
        encoding="utf-8",
    )
    env = {
        key: os.environ[key]
        for key in ("HOME", "PATH", "PYTHONPATH")
        if key in os.environ
    }
    env["BRIDGE_ENV_FILE"] = str(env_file)
    env["BRIDGE_ENV_FILE_REQUIRED"] = "true"
    lock_fd = os.open(control_root, os.O_RDONLY)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        result = subprocess.run(
            ["sh", str(START_SH_PATH)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
            timeout=10,
        )
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)

    assert result.returncode == 1
    assert f"another start.sh is already running for BRIDGE_CONTROL_ROOT={control_root.resolve()}" in result.stderr
    assert not (control_root / ".bridge.pid").exists()
    assert not (control_root / ".bridge.guard.pid").exists()
    assert not (control_root / "bridge.log").exists()


def test_start_sh_does_not_kill_unrelated_pid_from_custom_control_root(tmp_path):
    control_root = tmp_path / "control"
    control_root.mkdir()
    runtime_root = tmp_path / "runtime"
    (control_root / "bridge.log").mkdir()
    unrelated = subprocess.Popen(["sleep", "30"])
    try:
        (control_root / ".bridge.pid").write_text(f"{unrelated.pid}\n", encoding="utf-8")
        env_file = tmp_path / "instance.env"
        env_file.write_text(
            f"BRIDGE_CONTROL_ROOT='{control_root}'\n"
            f"BRIDGE_RUNTIME_ROOT='{runtime_root}'\n"
            "BRIDGE_BIND='127.0.0.1:19492'\n"
            "BRIDGE_WATCHDOG_ENABLED='false'\n",
            encoding="utf-8",
        )
        env = dict(os.environ)
        env["BRIDGE_ENV_FILE"] = str(env_file)

        result = subprocess.run(
            ["sh", str(START_SH_PATH)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            env=env,
            timeout=10,
        )

        assert result.returncode == 1
        assert f"refusing to stop unrelated process pid={unrelated.pid}" in result.stderr
        assert unrelated.poll() is None
    finally:
        unrelated.terminate()
        unrelated.wait(timeout=5)


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
        "CODEX_COMMAND=/opt/codex/bin/codex\n"
        "CODEX_HOME=/tmp/custom-codex-home\n"
        "WECOM_BOT_WORKSPACE_MODE=personal\n"
        "WECOM_WORKSPACE_MODE=team\n"
        "WECOM_BOT_LOCAL_RESUME_OWNER_USER_ID=local-owner\n"
        "LOCAL_RESUME_OWNER_USER_ID=fallback-owner\n"
        "WECOM_BOT_AGENT_BACKEND=claude\n"
        'WECOM_BOT_AGENT_COMMAND="claude --model sonnet"\n'
        "WECOM_BOT_AGENT_RUN_AS_USER=claudebot\n"
        "WECOM_BOT_AGENT_RUN_AS_GROUP=claudew\n"
        "WECOM_BOT_AGENT_RUNTIME_ROOT=/tmp/claude-runtime\n"
        "LOCAL_FILE_SEND_POLL_MS=4321\n"
        "LOCAL_SEND_MESSAGE_POLL_MS=5432\n"
        "LOCAL_FILE_SEND_RESULT_RETENTION_MS=654321\n"
        "REPLY_SEND_ACK_TIMEOUT_SEC=17\n"
        "STREAM_TEXT_MAX_CHARS=777\n"
        "WECOM_INBOUND_IDLE_TIMEOUT_MS=888000\n"
        "WECOM_HEARTBEAT_ACK_TIMEOUT_MS=777000\n"
        "WECOM_DISCONNECT_GRACE_MS=9999\n"
        "WECOM_RAW_INBOUND_LOG_ENABLED=false\n"
        "PROACTIVE_TEXT_MAX_CHARS=987\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"; export_bridge_runtime_env; '
            'python3 -c "import os; print(os.getenv(\'WORK_DIR\')); print(os.getenv(\'BRIDGE_TOKEN\')); print(os.getenv(\'CODEX_COMMAND\')); print(os.getenv(\'CODEX_HOME\')); print(os.getenv(\'WECOM_BOT_WORKSPACE_MODE\')); print(os.getenv(\'WECOM_WORKSPACE_MODE\')); print(os.getenv(\'WECOM_BOT_LOCAL_RESUME_OWNER_USER_ID\')); print(os.getenv(\'LOCAL_RESUME_OWNER_USER_ID\')); print(os.getenv(\'WECOM_BOT_AGENT_BACKEND\')); print(os.getenv(\'WECOM_BOT_AGENT_COMMAND\')); print(os.getenv(\'WECOM_BOT_AGENT_RUN_AS_USER\')); print(os.getenv(\'WECOM_BOT_AGENT_RUN_AS_GROUP\')); print(os.getenv(\'WECOM_BOT_AGENT_RUNTIME_ROOT\')); print(os.getenv(\'LOCAL_FILE_SEND_POLL_MS\')); print(os.getenv(\'LOCAL_SEND_MESSAGE_POLL_MS\')); print(os.getenv(\'LOCAL_FILE_SEND_RESULT_RETENTION_MS\')); print(os.getenv(\'REPLY_SEND_ACK_TIMEOUT_SEC\')); print(os.getenv(\'STREAM_TEXT_MAX_CHARS\')); print(os.getenv(\'WECOM_INBOUND_IDLE_TIMEOUT_MS\')); print(os.getenv(\'WECOM_HEARTBEAT_ACK_TIMEOUT_MS\')); print(os.getenv(\'WECOM_DISCONNECT_GRACE_MS\')); print(os.getenv(\'WECOM_RAW_INBOUND_LOG_ENABLED\')); print(os.getenv(\'PROACTIVE_TEXT_MAX_CHARS\'))"',
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
        "/opt/codex/bin/codex",
        "/tmp/custom-codex-home",
        "personal",
        "team",
        "local-owner",
        "fallback-owner",
        "claude",
        "claude --model sonnet",
        "claudebot",
        "claudew",
        "/tmp/claude-runtime",
        "4321",
        "5432",
        "654321",
        "17",
        "777",
        "888000",
        "777000",
        "9999",
        "false",
        "987",
    ]


def test_bridge_env_load_rejects_deprecated_plaintext_secret(tmp_path):
    script_dir = tmp_path / "bridge-scripts"
    script_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "bridge_env.sh").write_text(BRIDGE_ENV_SH_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    (script_dir / "bridge_runtime_config.py").write_text(
        BRIDGE_RUNTIME_CONFIG_PATH.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (script_dir / ".env").write_text(
        "BRIDGE_BIND=127.0.0.1:19399\n"
        "WECOM_BOT_ID=bot-id\n"
        "WECOM_BOT_SECRET=deprecated-secret\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            "sh",
            "-c",
            '. "$1/bridge_env.sh"; load_bridge_runtime_env "$1"',
            "sh",
            str(script_dir),
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "WECOM_BOT_SECRET is no longer supported" in result.stderr


def test_load_env_bootstrap_bot_configs_reads_workspace_mode_from_single_bot_env(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "single-bot.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_AGENT_BACKEND", "claude")
    monkeypatch.setenv("WECOM_BOT_WORKSPACE_MODE", "personal")

    configs = bridge_module.load_env_bootstrap_bot_configs()

    assert len(configs) == 1
    assert configs[0]["workspaceMode"] == "personal"
    assert configs[0]["agentBackend"] == "claude"


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

    assert bridge_module.read_session_record_by_id("sess-1") is None


def test_start_bot_invalidates_persisted_threads_when_workspace_mode_changes(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-workspace-mode.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "claude1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "workspaceNamespace": "personal-main",
                "workspaceMode": "team",
                "agentBackend": "claude",
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
                "name": "claude1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "workspaceNamespace": "personal-main",
                "workspaceMode": "personal",
                "agentBackend": "claude",
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        )
    )

    assert bridge_module.read_session_record_by_id("sess-1") is None
    recreated = bridge_module.create_session_record(bridge_module.BOTS["bot-1"], "single:test-user")
    assert recreated["sessionId"] != "sess-1"


def test_start_bot_invalidates_persisted_threads_when_workspace_namespace_changes(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-workspace-namespace.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "workspaceNamespace": "personal-main",
                "workspaceMode": "team",
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
                "workspaceNamespace": "other-main",
                "workspaceMode": "team",
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        )
    )

    assert bridge_module.read_session_record_by_id("sess-1") is None
    recreated = bridge_module.create_session_record(bridge_module.BOTS["bot-1"], "single:test-user")
    assert recreated["sessionId"] != "sess-1"


def test_start_bot_invalidates_persisted_threads_when_group_session_mode_changes(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-group-mode.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "workspaceNamespace": "team-main",
                "workspaceMode": "team",
                "groupSessionMode": "shared",
                "enabled": True,
            }
        ],
    )
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "sess-1",
            "botId": "bot-1",
            "chatKey": "group:test-room",
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
                "workspaceNamespace": "team-main",
                "workspaceMode": "team",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        )
    )

    assert bridge_module.read_session_record_by_id("sess-1") is None
    recreated = bridge_module.create_session_record(bridge_module.BOTS["bot-1"], "group-user:test-room:test-user")
    assert recreated["sessionId"] != "sess-1"


def test_reset_bot_session_runtime_state_clears_persisted_threads_for_loaded_bot(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR))
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "sess-1",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "threadId": "thread-old",
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], "sess-1")),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "idle",
        }
    )
    bridge_module.write_session_record(record)
    sess = bridge_module.SessionState(
        session_id="sess-1",
        work_dir=str(bridge_module.BASE_DIR),
        lock_file=Path(record["lockFile"]),
        thread_id="thread-old",
    )
    bot.sessions["single:test-user"] = sess

    cleared = bridge_module.reset_bot_session_runtime_state(bot.config["id"])

    persisted = bridge_module.read_session_record_by_id("sess-1")
    assert cleared >= 1
    assert sess.thread_id is None
    assert persisted is None


def test_reset_bot_session_runtime_state_clears_persisted_running_schedule_state(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR))
    record = bridge_module.normalize_session_record(
        {
            "sessionId": "sess-1",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "threadId": None,
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], "sess-1")),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
            "status": "running",
            "ownerInstance": "other-instance",
            "ownerPid": 1234,
            "leaseExpiresAt": bridge_module.now_ms() + 60000,
            "activeScheduleId": "sch-1",
        }
    )
    bridge_module.write_session_record(record)

    cleared = bridge_module.reset_bot_session_runtime_state(bot.config["id"])

    persisted = bridge_module.read_session_record_by_id("sess-1")
    assert cleared >= 1
    assert persisted is None


def test_reset_bot_session_runtime_state_removes_malformed_persisted_session_record_for_unloaded_bot(bridge_module):
    session_id = "sess-bad-unloaded"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "sessionId": session_id,
                "botId": "bot-1",
                "chatKey": "single:test-user",
                "lockFile": "/tmp/not-managed.lock.json",
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
                "threadId": "thread-old",
                "status": "leased",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    cleared = bridge_module.reset_bot_session_runtime_state("bot-1")

    assert cleared == 0
    assert not session_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_reset_bot_session_runtime_state_removes_artifacts_for_malformed_persisted_session_record_missing_session_id(
    bridge_module,
):
    session_id = "sess-missing-id-unloaded"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "botId": "bot-1",
                "chatKey": "single:test-user",
                "lockFile": "/tmp/not-managed.lock.json",
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
                "threadId": "thread-old",
                "status": "leased",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    cleared = bridge_module.reset_bot_session_runtime_state("bot-1")

    assert cleared == 0
    assert not session_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_reset_bot_session_runtime_state_removes_artifacts_for_session_id_referenced_only_by_key_file(
    bridge_module,
):
    session_id = "sess-key-only-unloaded"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "chatKey": "single:test-user",
                "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", session_id)),
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
                "threadId": "thread-old",
                "status": "leased",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    key_file = bridge_module.get_registry_key_file("bot-1", "single:test-user")
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": "bot-1", "chatKey": "single:test-user"}, ensure_ascii=False),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    cleared = bridge_module.reset_bot_session_runtime_state("bot-1")

    assert cleared == 1
    assert not session_file.exists()
    assert not key_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_reset_bot_session_runtime_state_removes_artifacts_for_session_record_missing_bot_id_but_managed_lock(
    bridge_module,
):
    session_id = "sess-missing-bot-id-unloaded"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "sessionId": session_id,
                "chatKey": "single:test-user",
                "lockFile": str(bridge_module.get_managed_session_lock_file("bot-1", session_id)),
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
                "threadId": "thread-old",
                "status": "leased",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    cleared = bridge_module.reset_bot_session_runtime_state("bot-1")

    assert cleared == 1
    assert not session_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


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

    assert bridge_module.read_session_record_by_id("sess-1") is None


def test_prepare_and_start_bot_rebinds_schedule_definition_session_id_when_bootstrap_workspace_identity_changes(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-bootstrap-schedule.secret", "secret\n")
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
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": "sess-1",
            "chatKey": "single:test-user",
            "botId": "bot-1",
            "botName": "codex1",
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    monkeypatch.setenv("WECOM_BOT_NAME", "codex1")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(new_work_dir))

    configs = bridge_module.prepare_bot_configs()
    asyncio.run(bridge_module.start_bot(configs[0]))

    updated = bridge_module.read_schedule_definition(definition["scheduleId"])
    assert updated is not None
    assert updated["sessionId"] != "sess-1"


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
    request = SimpleNamespace(headers={"Authorization": header}, remote="203.0.113.10")

    result = asyncio.run(bridge_module.require_api_access(request))

    assert result is None


def test_require_api_access_rejects_missing_basic_auth(bridge_module):
    bridge_module.BRIDGE_BASIC_AUTH = "czh:test"
    bridge_module.BRIDGE_TOKEN = ""
    request = SimpleNamespace(headers={}, remote="203.0.113.10")

    result = asyncio.run(bridge_module.require_api_access(request))

    assert result is not None
    assert result.status == 401
    assert result.headers["WWW-Authenticate"] == 'Basic realm="WeCom Codex Bridge"'


def test_require_api_access_accepts_non_ascii_basic_auth(bridge_module):
    credential = "user:\u5bc6\u7801"
    bridge_module.BRIDGE_BASIC_AUTH = credential
    bridge_module.BRIDGE_TOKEN = ""
    header = "Basic " + base64.b64encode(credential.encode("utf-8")).decode("ascii")
    request = SimpleNamespace(headers={"Authorization": header}, remote="203.0.113.10")

    result = asyncio.run(bridge_module.require_api_access(request))

    assert result is None


@pytest.mark.parametrize(
    ("configured_token", "configured_basic_auth", "authorization"),
    [
        ("expected-token", "", "Bearer wrong-\u51ed\u636e"),
        ("", "expected:credential", "Basic " + base64.b64encode("wrong:\u51ed\u636e".encode("utf-8")).decode("ascii")),
    ],
)
def test_require_api_access_rejects_non_ascii_invalid_credentials(
    bridge_module,
    configured_token,
    configured_basic_auth,
    authorization,
):
    bridge_module.BRIDGE_TOKEN = configured_token
    bridge_module.BRIDGE_BASIC_AUTH = configured_basic_auth
    request = SimpleNamespace(headers={"Authorization": authorization}, remote="203.0.113.10")

    result = asyncio.run(bridge_module.require_api_access(request))

    assert result is not None
    assert result.status == 401


def test_api_access_middleware_protects_detailed_health(bridge_module):
    bridge_module.BRIDGE_TOKEN = "expected-token"
    bridge_module.BRIDGE_BASIC_AUTH = ""
    request = SimpleNamespace(path="/healthz", headers={}, remote="203.0.113.10")
    handler_called = False

    async def handler(_request):
        nonlocal handler_called
        handler_called = True
        return object()

    response = asyncio.run(bridge_module.api_access_middleware(request, handler))

    assert response.status == 401
    assert handler_called is False


def test_api_access_middleware_keeps_livez_public(bridge_module):
    bridge_module.BRIDGE_TOKEN = "expected-token"
    bridge_module.BRIDGE_BASIC_AUTH = ""
    request = SimpleNamespace(path="/livez", headers={}, remote="203.0.113.10")
    expected = object()

    async def handler(_request):
        return expected

    response = asyncio.run(bridge_module.api_access_middleware(request, handler))

    assert response is expected


def test_prepare_bot_configs_bootstraps_single_bot_from_env(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_WORKSPACE_NAMESPACE", "personal-main")
    monkeypatch.setenv("WECOM_BOT_GROUP_SESSION_MODE", "shared")

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == bridge_module.default_bot_config_id("bot-id")
    assert configs[0]["name"] == "env-bot"
    assert configs[0]["botId"] == "bot-id"
    assert configs[0]["secret"] == "secret"
    assert configs[0]["workDir"] == str(bridge_module.BASE_DIR)
    assert configs[0]["workspaceNamespace"] == "personal-main"
    assert configs[0]["groupSessionMode"] == "shared"
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    expected = bridge_module.serialize_bot_config_for_disk(configs[0])
    for field in ("createdAt", "updatedAt"):
        assert isinstance(stored[0][field], int)
        expected[field] = stored[0][field]
    assert stored == [expected]
    assert "secret" not in stored[0]


def test_prepare_bot_configs_treats_null_explicit_config_id_as_default_id(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-null-config-id.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_CONFIG_ID", "null")

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == bridge_module.default_bot_config_id("bot-id")


def test_serialize_bot_config_for_disk_omits_unset_local_resume_owner(bridge_module):
    assert bridge_module.serialize_bot_config_for_disk(
        {"id": "bot-1", "localResumeOwnerUserId": None}
    ) == {"id": "bot-1"}


def test_prepare_bot_configs_rejects_null_env_bot_id_when_secret_file_present(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-null-botid.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "null")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.prepare_bot_configs()

    assert excinfo.value.message == "WECOM_BOT_ID must be set together with WECOM_BOT_SECRET_FILE"


def test_prepare_bot_configs_treats_null_enabled_as_default_true(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-enabled-null.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_ENABLED", "null")

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["enabled"] is True


def test_prepare_bot_configs_treats_null_group_session_mode_as_default_per_user(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-group-mode-null.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_GROUP_SESSION_MODE", "null")

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["groupSessionMode"] == "per-user"


def test_prepare_bot_configs_treats_null_workspace_namespace_as_default_id(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-namespace-null.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_WORKSPACE_NAMESPACE", "null")

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["workspaceNamespace"] == bridge_module.default_bot_config_id("bot-id")


def test_prepare_bot_configs_treats_null_work_dir_as_default_work_dir(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-workdir-null.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", "null")
    monkeypatch.setattr(bridge_module, "DEFAULT_WORK_DIR", str(bridge_module.BASE_DIR))

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["workDir"] == str(bridge_module.BASE_DIR.resolve())


def test_prepare_bot_configs_treats_null_welcome_as_empty_string(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-welcome-null.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "env-bot")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))
    monkeypatch.setenv("WECOM_BOT_WELCOME", "null")

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["welcome"] == ""


def test_prepare_bot_configs_treats_null_name_as_default(bridge_module, monkeypatch):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "env-name-null.secret", "secret\n")
    monkeypatch.setenv("WECOM_BOT_NAME", "null")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", str(secret_file))
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["name"] == "default"


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


def test_prepare_bot_configs_prunes_legacy_generated_bootstrap_bot_missing_from_current_bootstrap(
    bridge_module, monkeypatch
):
    stale_secret_file = write_secret_file(bridge_module.BASE_DIR / "stale.secret", "stale-secret\n")
    live_secret_file = write_secret_file(bridge_module.BASE_DIR / "live.secret", "live-secret\n")
    manual_secret_file = write_secret_file(bridge_module.BASE_DIR / "manual.secret", "manual-secret\n")
    stale_bot_id = "stale-bot"
    manual_bot_id = "manual-bot"
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bridge_module.default_bot_config_id(stale_bot_id),
                "name": "stale-env-bot",
                "botId": stale_bot_id,
                "secretFile": str(stale_secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            },
            {
                "id": "manual-bot-id",
                "name": "manual-bot",
                "botId": manual_bot_id,
                "secretFile": str(manual_secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            },
        ],
    )
    monkeypatch.setenv(
        "WECOM_BOOTSTRAP_BOTS_JSON",
        json.dumps(
            [
                {
                    "name": "live-env-bot",
                    "botId": "live-bot",
                    "secretFile": str(live_secret_file),
                    "workDir": str(bridge_module.BASE_DIR),
                }
            ]
        ),
    )

    configs = bridge_module.prepare_bot_configs()

    assert {item["botId"] for item in configs} == {"live-bot", manual_bot_id}
    persisted = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert {item["botId"] for item in persisted} == {"live-bot", manual_bot_id}
    assert all(item["botId"] != stale_bot_id for item in persisted)


def test_prepare_bot_configs_prunes_stale_explicit_bootstrap_managed_bot_when_bootstrap_changes(
    bridge_module, monkeypatch
):
    old_secret_file = write_secret_file(bridge_module.BASE_DIR / "old-bootstrap.secret", "old-secret\n")
    new_secret_file = write_secret_file(bridge_module.BASE_DIR / "new-bootstrap.secret", "new-secret\n")
    monkeypatch.setenv(
        "WECOM_BOOTSTRAP_BOTS_JSON",
        json.dumps(
            [
                {
                    "id": "custom-bootstrap-id",
                    "name": "old-bootstrap-bot",
                    "botId": "old-bootstrap-bot",
                    "secretFile": str(old_secret_file),
                    "workDir": str(bridge_module.BASE_DIR),
                }
            ]
        ),
    )

    first_configs = bridge_module.prepare_bot_configs()

    assert first_configs[0]["bootstrapManaged"] is True
    first_persisted = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert first_persisted[0]["bootstrapManaged"] is True

    monkeypatch.setenv(
        "WECOM_BOOTSTRAP_BOTS_JSON",
        json.dumps(
            [
                {
                    "id": "replacement-bootstrap-id",
                    "name": "replacement-bootstrap-bot",
                    "botId": "replacement-bootstrap-bot",
                    "secretFile": str(new_secret_file),
                    "workDir": str(bridge_module.BASE_DIR),
                }
            ]
        ),
    )

    configs = bridge_module.prepare_bot_configs()

    assert len(configs) == 1
    assert configs[0]["id"] == "replacement-bootstrap-id"
    persisted = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert len(persisted) == 1
    assert persisted[0]["id"] == "replacement-bootstrap-id"


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


def test_prepare_bot_configs_filters_tombstoned_malformed_persisted_bot(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-malformed.secret", "secret\n")
    payload = [
        {
            "id": "bot-1",
            "name": "bad-backend",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentBackend": "not-a-backend",
            "enabled": True,
            "createdAt": bridge_module.now_ms() - 1000,
            "updatedAt": bridge_module.now_ms() - 1000,
        }
    ]
    bridge_module.write_json_atomic(bridge_module.DATA_FILE, payload)
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


def test_prepare_bot_configs_ignores_invalid_tombstoned_env_bootstrap_bot(bridge_module, monkeypatch):
    bridge_module.mark_bot_deleted_globally("bot-1", "bot-id")
    monkeypatch.setenv("WECOM_BOT_ID", "bot-id")
    monkeypatch.setenv("WECOM_BOT_SECRET", "deprecated-secret")
    monkeypatch.setenv("WECOM_BOT_WORK_DIR", str(bridge_module.BASE_DIR))

    configs = bridge_module.prepare_bot_configs()

    assert configs == []


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


def test_prepare_bot_configs_skips_invalid_persisted_agent_backend(bridge_module, capsys):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-invalid-backend.secret", "secret\n")
    payload = [
        {
            "id": "bot-bad",
            "name": "bad-backend",
            "botId": "bad-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentBackend": "not-a-backend",
            "enabled": True,
        },
        {
            "id": "bot-good",
            "name": "good-backend",
            "botId": "good-bot",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentBackend": "codex",
            "enabled": True,
        },
    ]
    bridge_module.write_json_atomic(bridge_module.DATA_FILE, payload)

    configs = bridge_module.prepare_bot_configs()

    assert [item["id"] for item in configs] == ["bot-good"]
    captured = capsys.readouterr()
    assert "ignore invalid bot config bot-bad" in captured.out


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


def test_persisted_bot_configs_lock_replaces_symlink_lock_file_without_touching_target(bridge_module):
    lock_file = bridge_module.get_persisted_bot_configs_lock_file()
    external_lock = bridge_module.BASE_DIR / "external-bots.lock"
    external_lock.write_text("external", encoding="utf-8")
    bridge_module.ensure_dir_for(lock_file)
    lock_file.symlink_to(external_lock)

    with bridge_module.persisted_bot_configs_lock():
        assert lock_file.exists()
        assert not lock_file.is_symlink()

    assert external_lock.read_text(encoding="utf-8") == "external"


def test_start_bot_restores_template_card_delivery_meta(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    state_file = bridge_module.get_template_card_state_file("bot-1")
    bridge_module.write_json_atomic(
        state_file,
        {
            "task-1": {
                "taskId": "task-1",
                "chatKey": "single:test-user",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "hello"},
                    "button_list": [{"text": "go", "style": 1, "key": "go"}],
                    "task_id": "task-1",
                },
            }
        },
    )

    bot = asyncio.run(
        bridge_module.start_bot(
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "enabled": True,
                "welcome": "",
                "groupSessionMode": "per-user",
            }
        )
    )

    assert bot.template_card_payloads["task-1"]["task_id"] == "task-1"


def test_start_bot_restores_reply_url_state(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    state_file = bridge_module.get_reply_url_state_file("bot-1")
    bridge_module.write_json_atomic(
        state_file,
        {
            "req-1": {
                "responseUrl": "https://example.com/response",
                "chatKey": "single:test-user",
                "capturedAtMs": bridge_module.now_ms(),
                "consumed": False,
            }
        },
    )

    bot = asyncio.run(
        bridge_module.start_bot(
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "enabled": True,
                "welcome": "",
                "groupSessionMode": "per-user",
            }
        )
    )

    assert bot.reply_urls["req-1"]["responseUrl"] == "https://example.com/response"


def test_start_bot_ignores_reply_url_state_without_response_url(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    state_file = bridge_module.get_reply_url_state_file("bot-1")
    bridge_module.write_json_atomic(
        state_file,
        {
            "req-bad": {
                "chatKey": "single:test-user",
                "capturedAtMs": bridge_module.now_ms(),
                "consumed": False,
            },
            "req-good": {
                "responseUrl": "https://example.com/response",
                "chatKey": "single:test-user",
                "capturedAtMs": bridge_module.now_ms(),
                "consumed": False,
            },
        },
    )

    bot = asyncio.run(
        bridge_module.start_bot(
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "enabled": True,
                "welcome": "",
                "groupSessionMode": "per-user",
            }
        )
    )

    assert "req-bad" not in bot.reply_urls
    assert bot.reply_urls["req-good"]["responseUrl"] == "https://example.com/response"


def test_register_template_card_button_texts_clears_stale_mapping_when_buttons_removed(bridge_module):
    bot = make_bot(bridge_module)
    bot.template_card_button_texts["task-1"] = {"old": "Old"}

    bridge_module.register_template_card_button_texts(
        bot,
        {
            "card_type": "text_notice",
            "main_title": {"title": "hello"},
            "card_action": {"type": 1, "url": "https://example.com"},
            "task_id": "task-1",
        },
    )

    assert "task-1" not in bot.template_card_button_texts


def test_remove_bot_deletes_template_card_state(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    state_file = bridge_module.get_template_card_state_file("bot-1")
    bridge_module.write_json_atomic(state_file, {"task-1": {"taskId": "task-1"}})
    bot = asyncio.run(
        bridge_module.start_bot(
            {
                "id": "bot-1",
                "name": "codex1",
                "botId": "bot-id",
                "secretFile": str(secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "enabled": True,
                "welcome": "",
                "groupSessionMode": "per-user",
            }
        )
    )
    assert state_file.exists() is True

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert state_file.exists() is False


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
    assert f"--bot-name '{bot.config['name']}'" in context


def test_get_session_workspace_paths_for_single_chat(bridge_module):
    bot = make_bot(bridge_module)

    paths = bridge_module.get_session_workspace_paths(bot, "single:test-user")

    assert paths["workDir"] == Path(bot.config["workDir"]).resolve()
    assert paths["chatfile"] == bridge_module.CHATFILE_ROOT / paths["chatfile"].name
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

    assert paths["chatfile"] == bridge_module.CHATFILE_ROOT / paths["chatfile"].name
    assert paths["workfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "users" / "user-a" / "workfile"
    assert paths["roomfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "rooms" / "group-1" / "roomfile"


def test_get_session_workspace_paths_for_group_shared_chat(bridge_module):
    bot = make_bot(bridge_module)

    paths = bridge_module.get_session_workspace_paths(bot, "group:group-1")

    assert paths["chatfile"] == bridge_module.CHATFILE_ROOT / paths["chatfile"].name
    assert paths["workfile"] is None
    assert paths["roomfile"] == bridge_module.WORKSPACE_ROOT / bot.config["id"] / "rooms" / "group-1" / "roomfile"


def test_get_session_runtime_cwd_uses_workfile_for_single_chat(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))

    runtime_cwd = bridge_module.get_session_runtime_cwd(bot, "single:test-user")

    assert runtime_cwd == bridge_module.get_workfile_dir(bot.config["id"], "test-user").resolve()


def test_get_session_runtime_cwd_uses_workdir_for_personal_mode(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal"
    work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"

    runtime_cwd = bridge_module.get_session_runtime_cwd(bot, "single:test-user")

    assert runtime_cwd == work_dir.resolve()


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


def test_ensure_session_workspace_dirs_skips_bootstrap_in_personal_mode(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-bootstrap"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "tracked.txt").write_text("hello", encoding="utf-8")
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    assert not (paths["workfile"] / "tracked.txt").exists()


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


def test_ensure_session_workspace_dirs_replaces_broken_symlinked_file_during_bootstrap(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-broken-file"
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "tracked.txt").write_text("hello", encoding="utf-8")
    (work_dir / ".git").mkdir()
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    workfile = bridge_module.get_workfile_dir(bot.config["id"], "test-user")
    workfile.mkdir(parents=True, exist_ok=True)
    (workfile / "tracked.txt").symlink_to(bridge_module.BASE_DIR / "missing-tracked.txt")

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    restored = paths["workfile"] / "tracked.txt"
    assert restored.is_file()
    assert not restored.is_symlink()
    assert restored.read_text(encoding="utf-8") == "hello"


def test_ensure_session_workspace_dirs_replaces_broken_symlinked_symlink_during_bootstrap(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-broken-link"
    external_target = bridge_module.BASE_DIR / "external-target"
    work_dir.mkdir(parents=True, exist_ok=True)
    external_target.mkdir(parents=True, exist_ok=True)
    (external_target / "nested.txt").write_text("nested", encoding="utf-8")
    (work_dir / "linked-dir").symlink_to(external_target, target_is_directory=True)
    (work_dir / ".git").mkdir()
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    workfile = bridge_module.get_workfile_dir(bot.config["id"], "test-user")
    workfile.mkdir(parents=True, exist_ok=True)
    (workfile / "linked-dir").symlink_to(bridge_module.BASE_DIR / "missing-linked-dir", target_is_directory=True)

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    restored = paths["workfile"] / "linked-dir"
    assert restored.is_symlink()
    assert restored.resolve() == external_target.resolve()
    assert os.path.lexists(restored)
    assert (restored / "nested.txt").read_text(encoding="utf-8") == "nested"


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


def test_ensure_session_workspace_dirs_migrates_legacy_workfile_without_overwrite(bridge_module):
    bot = make_bot(bridge_module)
    legacy = bridge_module.get_legacy_workfile_dir(bot.config["id"], "test-user")
    legacy.mkdir(parents=True, exist_ok=True)
    (legacy / "from-legacy.txt").write_text("legacy", encoding="utf-8")
    target = bridge_module.get_workfile_dir(bot.config["id"], "test-user")
    target.mkdir(parents=True, exist_ok=True)
    (target / "keep.txt").write_text("new", encoding="utf-8")

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")

    assert paths["workfile"] is not None
    assert (paths["workfile"] / "from-legacy.txt").read_text(encoding="utf-8") == "legacy"
    assert (paths["workfile"] / "keep.txt").read_text(encoding="utf-8") == "new"
    assert (paths["workfile"] / ".workspace-layout-migration.json").exists()


def test_ensure_session_workspace_dirs_migrates_legacy_alias_workfile_without_overwrite(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_user_alias(bot.config["id"], "wo-user", "friendly-user")
    legacy = bridge_module.get_legacy_workfile_dir(bot.config["id"], "friendly-user")
    legacy.mkdir(parents=True, exist_ok=True)
    (legacy / "from-legacy-alias.txt").write_text("legacy", encoding="utf-8")
    target = bridge_module.get_workfile_dir(bot.config["id"], "friendly-user")
    target.mkdir(parents=True, exist_ok=True)
    (target / "keep.txt").write_text("new", encoding="utf-8")

    paths = bridge_module.ensure_session_workspace_dirs(bot, "single:wo-user")

    assert paths["workfile"] is not None
    assert (paths["workfile"] / "from-legacy-alias.txt").read_text(encoding="utf-8") == "legacy"
    assert (paths["workfile"] / "keep.txt").read_text(encoding="utf-8") == "new"


def test_ensure_session_workspace_dirs_migrates_legacy_roomfile_without_overwrite(bridge_module):
    bot = make_bot(bridge_module)
    legacy = bridge_module.get_legacy_roomfile_dir(bot.config["id"], "group-1")
    legacy.mkdir(parents=True, exist_ok=True)
    (legacy / "from-legacy.txt").write_text("legacy", encoding="utf-8")
    target = bridge_module.get_roomfile_dir(bot.config["id"], "group-1")
    target.mkdir(parents=True, exist_ok=True)
    (target / "keep.txt").write_text("new", encoding="utf-8")

    paths = bridge_module.ensure_session_workspace_dirs(bot, "group:group-1")

    assert paths["roomfile"] is not None
    assert (paths["roomfile"] / "from-legacy.txt").read_text(encoding="utf-8") == "legacy"
    assert (paths["roomfile"] / "keep.txt").read_text(encoding="utf-8") == "new"
    assert (paths["roomfile"] / ".workspace-layout-migration.json").exists()


def test_get_session_runtime_cwd_uses_workfile_for_group_user_chat(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))

    runtime_cwd = bridge_module.get_session_runtime_cwd(bot, "group-user:group-1:user-a")

    assert runtime_cwd == bridge_module.get_workfile_dir(bot.config["id"], "user-a").resolve()


def test_get_session_runtime_cwd_uses_roomfile_for_group_shared_chat(bridge_module):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo"))

    runtime_cwd = bridge_module.get_session_runtime_cwd(bot, "group:group-1")

    assert runtime_cwd == bridge_module.get_roomfile_dir(bot.config["id"], "group-1").resolve()


def test_build_bridge_context_mentions_workspace_mode_and_workdir(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-context"
    work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"
    sess = make_session(bridge_module, bot)

    context = bridge_module.build_bridge_context(bot, sess, "single:test-user")

    assert "workspaceMode: personal" in context
    assert f"WORKDIR_DIR: {Path(bot.config['workDir']).resolve()}" in context


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


def test_validate_file_for_upload_prefers_allowlist_error_over_size_for_outside_file(bridge_module):
    bot = make_bot(bridge_module)
    outside_dir = bridge_module.BASE_DIR / "outside"
    outside_dir.mkdir(parents=True, exist_ok=True)
    file_path = outside_dir / "big.bin"
    file_path.write_bytes(b"x" * (bridge_module.MAX_UPLOAD_SIZE + 1))

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.validate_file_for_upload(bot, "single:test-user", str(file_path))

    assert excinfo.value.status_code == 403
    assert excinfo.value.message.startswith("filePath is outside allowed roots:")


def test_extract_mixed_images_accepts_download_url_variants_in_classic_bridge(bridge_module):
    mixed = {
        "msg_item": [
            {"msgtype": "image", "image": {"download_url": "https://example.test/a.png"}},
            {"msgtype": "image", "image": {"downloadUrl": "https://example.test/b.png"}},
        ]
    }

    assert bridge_module.extract_mixed_images(mixed) == [
        {"download_url": "https://example.test/a.png"},
        {"downloadUrl": "https://example.test/b.png"},
    ]


def test_validate_file_for_upload_accepts_tilde_expanded_file_send_root(monkeypatch, tmp_path):
    home_dir = tmp_path / "home"
    allowed_root = home_dir / "allowed"
    allowed_root.mkdir(parents=True, exist_ok=True)
    file_path = allowed_root / "artifact.txt"
    file_path.write_text("hello", encoding="utf-8")
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("FILE_SEND_ROOTS", "~/allowed")

    bridge_module = load_bridge_module()
    bot = make_bot(bridge_module)

    resolved = bridge_module.validate_file_for_upload(bot, "single:test-user", str(file_path))

    assert bridge_module.EXTRA_FILE_ROOTS == [allowed_root.resolve()]
    assert resolved == file_path.resolve()


def test_chat_key_to_send_target_rejects_invalid_chat_key(bridge_module):
    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.chat_key_to_send_target("group-user:room-only")

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "invalid chat key: group-user:room-only"


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


def test_resolve_file_send_request_ignores_null_session_id(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_file_send_request(
            {
                "filePath": str(chat_file),
                "chatKey": "group-user:room-only",
                "sessionId": "null",
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "invalid chatKey: group-user:room-only"


def test_resolve_file_send_request_ignores_null_target_config_id(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.create_session_record(bot, "single:test-user")
    bot.ws = SimpleNamespace(closed=False)
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    chat_file = chatfile_dir / "artifact.txt"
    chat_file.write_text("hello", encoding="utf-8")

    resolved_bot, resolved_chat_key, resolved_file = bridge_module.resolve_file_send_request(
        {
            "filePath": str(chat_file),
            "chatKey": "single:test-user",
            "targetConfigId": "null",
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


def test_resolve_file_send_request_recovers_session_missing_chat_key_from_key_registry(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    key = "single:test-user"
    session_id = "sess-missing-chatkey"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        },
    )
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": bot.config["id"], "chatKey": key}, ensure_ascii=False),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    bot.ws = SimpleNamespace(closed=False)

    resolved_bot, resolved_chat_key, resolved_file = bridge_module.resolve_file_send_request(
        {
            "sessionId": session_id,
            "filePath": str(file_path.resolve()),
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == key
    assert resolved_file == file_path.resolve()


def test_resolve_file_send_request_recovers_session_missing_chat_key_from_raw_bot_id_and_key_registry(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    key = "single:test-user"
    session_id = "sess-missing-chatkey-raw-bot"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(session_id),
        {
            "sessionId": session_id,
            "botId": bot.config["id"],
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], session_id)),
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        },
    )
    key_file = bridge_module.get_registry_key_file(bot.config["id"], key)
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": bot.config["id"], "chatKey": key}, ensure_ascii=False),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    bot.ws = SimpleNamespace(closed=False)

    resolved_bot, resolved_chat_key, resolved_file = bridge_module.resolve_file_send_request(
        {
            "sessionId": session_id,
            "filePath": str(file_path.resolve()),
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == key
    assert resolved_file == file_path.resolve()


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


def test_resolve_schedule_target_ignores_null_session_id(bridge_module):
    bot = make_bot(bridge_module)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_schedule_target(
            {
                "chatKey": "group-user:room-only",
                "sessionId": "null",
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "invalid chatKey: group-user:room-only"


def test_resolve_schedule_target_ignores_null_bot_name(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.create_session_record(bot, "single:test-user")

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "botName": "null",
        }
    )

    assert target["botId"] == bot.config["id"]
    assert target["botName"] == bot.config["name"]
    assert target["chatKey"] == "single:test-user"


def test_resolve_schedule_target_ignores_null_target_config_id(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.create_session_record(bot, "single:test-user")

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "targetConfigId": "null",
        }
    )

    assert target["botId"] == bot.config["id"]
    assert target["botName"] == bot.config["name"]
    assert target["chatKey"] == "single:test-user"


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


def test_submit_schedule_message_request_accepts_reply_req_id_for_template_card(bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    current_now = 1_800_000_300_000
    bridge_module.now_ms = lambda: current_now
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")
    requested_run_at = current_now + 30_000

    result = bridge_module.submit_schedule_message_request(
        {
            "replyReqId": "req-1",
            "runAt": requested_run_at,
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
        },
        "test",
    )

    stored = bridge_module.read_schedule_definition(result["scheduleId"])
    assert stored is not None
    assert stored["replyReqId"] == "req-1"
    assert stored["msgtype"] == "template_card"
    assert stored["templateCard"]["card_type"] == "button_interaction"
    assert stored["chatKey"] == "single:test-user"


def test_resolve_schedule_target_prefers_reply_req_id_over_unconnected_chat_key(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = None
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "replyReqId": "req-1",
        }
    )

    assert target["botId"] == bot.config["id"]
    assert target["chatKey"] == "single:test-user"
    assert target["replyReqId"] == "req-1"


def test_resolve_schedule_target_ignores_null_reply_req_id(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    target = bridge_module.resolve_schedule_target(
        {
            "chatKey": "single:test-user",
            "replyReqId": "null",
        }
    )

    assert target["botId"] == bot.config["id"]
    assert target["botName"] == bot.config["name"]
    assert target["chatKey"] == "single:test-user"
    assert target["sessionId"] == sess.session_id


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


def test_normalize_bot_config_treats_null_secret_as_missing_when_secret_file_present(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-null-secret.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secret": "null",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "enabled": True,
        }
    )

    assert config["secret"] == "secret"
    assert config["secretFile"] == str(secret_file)


def test_normalize_bot_config_treats_null_welcome_as_empty_string(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-welcome-null.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "welcome": "null",
            "enabled": True,
        }
    )

    assert config["welcome"] == ""


def test_normalize_bot_config_treats_null_name_as_default_unnamed(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-name-null.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "null",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "enabled": True,
        }
    )

    assert config["name"] == "unnamed"


def test_normalize_bot_config_accepts_agent_backend_and_command(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "claude1",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "agentBackend": "claude",
            "agentCommand": "claude --model sonnet",
            "enabled": True,
        }
    )

    assert config["agentBackend"] == "claude"
    assert config["agentCommand"] == "claude --model sonnet"


def test_normalize_bot_config_treats_null_agent_command_as_unset(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-agent-command-null.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "agentBackend": "codex",
            "agentCommand": "null",
            "enabled": True,
        }
    )

    assert config["agentCommand"] is None


def test_normalize_bot_config_treats_null_restart_token_as_unset(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-restart-token-null.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "codex1",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "restartToken": "null",
        }
    )

    assert config["restartToken"] is None


def test_normalize_bot_config_ignores_legacy_agent_run_as_fields(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "id": "bot-1",
            "name": "claude1",
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "agentBackend": "claude",
            "agentRunAsUser": "nobody",
            "agentRunAsGroup": "nogroup",
            "agentRuntimeRoot": str(bridge_module.BASE_DIR / "claude-runtime"),
            "enabled": True,
        }
    )

    assert config["agentRunAsUser"] is None
    assert config["agentRunAsGroup"] is None
    assert config["agentRuntimeRoot"] is None


def test_start_bot_does_not_stop_existing_bot_when_new_config_is_invalid(bridge_module):
    existing = make_bot(bridge_module)
    stop_calls = []

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
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


def test_reset_clears_local_resume_backup(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "local-thread"
    sess.use_global_codex_sessions = True
    sess.chat = [{"role": "user", "text": "local conversation"}]
    bridge_module.update_session_record(
        sess.session_id,
        lambda current: {
            **current,
            "threadId": sess.thread_id,
            "useGlobalCodexSessions": True,
            "recentChat": list(sess.chat),
            "localResumeBackup": {
                "threadId": "bridge-thread",
                "recentChat": [{"role": "user", "text": "bridge conversation"}],
                "useGlobalCodexSessions": False,
            },
        },
    )

    bridge_module.interrupt_session(
        bot,
        "single:test-user",
        sess,
        clear_thread=True,
        clear_chat=True,
        clear_queue=True,
    )

    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted.get("threadId") is None
    assert persisted.get("recentChat") == []
    assert persisted.get("useGlobalCodexSessions") is False
    assert persisted.get("localResumeBackup") is None


def test_normalize_local_resume_backup_cannot_restore_global_session_mode(bridge_module):
    backup = bridge_module.normalize_local_resume_backup(
        {
            "threadId": "bridge-thread",
            "recentChat": [{"role": "user", "text": "bridge conversation"}],
            "useGlobalCodexSessions": True,
        }
    )

    assert backup == {
        "threadId": "bridge-thread",
        "recentChat": [{"role": "user", "text": "bridge conversation"}],
        "useGlobalCodexSessions": False,
    }


def test_stop_bot_preserves_session_codex_home_for_persisted_thread(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "thread-to-resume"
    codex_home = bridge_module.build_codex_home_for_subprocess(sess.session_id)
    rollout = codex_home / "sessions" / "rollout.jsonl"
    rollout.parent.mkdir(parents=True, exist_ok=True)
    rollout.write_text("persisted rollout", encoding="utf-8")

    asyncio.run(
        bridge_module.stop_bot(
            bot.config["id"],
            persist_disable=False,
            persist_state=False,
            reason="test_restart",
        )
    )

    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted.get("threadId") == "thread-to-resume"
    assert rollout.read_text(encoding="utf-8") == "persisted rollout"


def test_reset_inactive_session_clears_chat_and_codex_home(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "thread-to-reset"
    sess.chat = [{"role": "user", "text": "conversation to clear"}]
    bridge_module.update_session_record(
        sess.session_id,
        lambda current: {
            **current,
            "threadId": sess.thread_id,
            "recentChat": list(sess.chat),
        },
    )
    codex_home = bridge_module.build_codex_home_for_subprocess(sess.session_id)
    (codex_home / "state.txt").write_text("state", encoding="utf-8")
    bot.sessions.clear()

    result = bridge_module.control_session(bot, "single:test-user", clear_thread=True, clear_chat=True)

    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert result["live"] is False
    assert persisted.get("threadId") is None
    assert persisted.get("recentChat") == []
    assert codex_home.exists() is False


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
async def test_process_scheduled_messages_dispatches_template_card_via_reply_req_id(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-card.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-card",
            "scheduleId": "sched-card",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "chatKey": "single:test-user",
            "replyReqId": "req-1",
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
            "message": "[template_card scheduled follow-up]",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    captured = {}

    async def fake_submit_send_message_request(data, source="api"):
        captured.update(data)
        assert source == "scheduled-message"
        return {"ok": True, "msgtype": "template_card", "chatKey": "single:test-user"}

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)
    await bridge_module.process_scheduled_messages_once()

    assert captured["replyReqId"] == "req-1"
    assert captured["chatKey"] is None
    assert captured["msgtype"] == "template_card"
    assert captured["templateCard"]["card_type"] == "button_interaction"
    assert not pending.exists()
    assert (bridge_module.SCHEDULE_DONE_ROOT / "job-card.json").exists()
    assert not (bridge_module.SCHEDULE_PROCESSING_ROOT / "job-card.json").exists()


@pytest.mark.asyncio
async def test_process_scheduled_messages_dispatches_template_card_via_reply_req_id_when_chat_key_missing(
    bridge_module, monkeypatch
):
    bot = make_bot(bridge_module)
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-card-missing-chatkey.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-card-missing-chatkey",
            "scheduleId": "sched-card-missing-chatkey",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "replyReqId": "req-1",
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
            "message": "[template_card scheduled follow-up]",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    captured = {}

    async def fake_submit_send_message_request(data, source="api"):
        captured.update(data)
        assert source == "scheduled-message"
        return {"ok": True, "msgtype": "template_card", "chatKey": "single:test-user"}

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)
    await bridge_module.process_scheduled_messages_once()

    assert captured["replyReqId"] == "req-1"
    assert captured["chatKey"] is None
    assert not pending.exists()
    assert (bridge_module.SCHEDULE_DONE_ROOT / "job-card-missing-chatkey.json").exists()


@pytest.mark.asyncio
async def test_process_scheduled_messages_dispatches_template_card_missing_bot_id_via_definition_owner(
    bridge_module, monkeypatch
):
    bot = make_bot(bridge_module, config_id="bot-1", name="bot-a")
    other = make_bot(bridge_module, config_id="bot-2", name="bot-b")
    bridge_module.create_session_record(bot, "single:test-user")
    bridge_module.create_session_record(other, "single:test-user")
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    other.status = "running"
    other.ws = SimpleNamespace(closed=False)
    definition = bridge_module.create_schedule_definition_record(
        {
            "chatKey": "single:test-user",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "message": "[template_card scheduled follow-up]",
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-card-missing-botid.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-card-missing-botid",
            "scheduleId": definition["scheduleId"],
            "chatKey": "single:test-user",
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
            "message": "[template_card scheduled follow-up]",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    captured = {}

    async def fake_submit_send_message_request(data, source="api"):
        captured.update(data)
        assert source == "scheduled-message"
        return {"ok": True, "msgtype": "template_card", "chatKey": "single:test-user"}

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)
    await bridge_module.process_scheduled_messages_once()

    assert captured["targetConfigId"] == bot.config["id"]
    assert not pending.exists()
    assert (bridge_module.SCHEDULE_DONE_ROOT / "job-card-missing-botid.json").exists()


@pytest.mark.asyncio
async def test_process_scheduled_messages_dispatches_template_card_with_current_bot_name_not_stale_job_name(
    bridge_module, monkeypatch
):
    bot = make_bot(bridge_module, config_id="bot-1", name="bot-a")
    bridge_module.create_session_record(bot, "single:test-user")
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-card-stale-name.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-card-stale-name",
            "scheduleId": "sched-card-stale-name",
            "botId": bot.config["id"],
            "botName": "old-name",
            "chatKey": "single:test-user",
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
            "message": "[template_card scheduled follow-up]",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    captured = {}

    async def fake_submit_send_message_request(data, source="api"):
        captured.update(data)
        assert source == "scheduled-message"
        return {"ok": True, "msgtype": "template_card", "chatKey": "single:test-user"}

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)
    await bridge_module.process_scheduled_messages_once()

    assert captured["botName"] == bot.config["name"]
    assert not pending.exists()
    assert (bridge_module.SCHEDULE_DONE_ROOT / "job-card-stale-name.json").exists()


@pytest.mark.asyncio
async def test_process_scheduled_messages_moves_template_card_job_to_failed_on_permanent_bridge_error(
    bridge_module, monkeypatch
):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-card-failed.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-card-failed",
            "scheduleId": "sched-card-failed",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "chatKey": "single:test-user",
            "replyReqId": "req-missing",
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
            "message": "[template_card scheduled follow-up]",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    async def fake_submit_send_message_request(_data, source="api"):
        assert source == "scheduled-message"
        raise bridge_module.BridgeError(404, "reply response_url not found or expired: req-missing")

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)
    await bridge_module.process_scheduled_messages_once()

    assert not pending.exists()
    assert (bridge_module.SCHEDULE_FAILED_ROOT / "job-card-failed.json").exists()
    assert not (bridge_module.SCHEDULE_PROCESSING_ROOT / "job-card-failed.json").exists()
    assert not (bridge_module.SCHEDULE_DONE_ROOT / "job-card-failed.json").exists()


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
async def test_process_scheduled_messages_dispatches_job_missing_bot_id_when_definition_matches(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "message": "scheduled text",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-missing-bot-id.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-missing-bot-id",
            "scheduleId": definition["scheduleId"],
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    called = []

    async def fake_enqueue_message(_bot, chat_key, text, req_id, **kwargs):
        called.append((chat_key, text, req_id, kwargs.get("schedule_id"), kwargs.get("schedule_request_id")))
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert called == [("single:test-user", "scheduled text", None, definition["scheduleId"], "job-missing-bot-id")]
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-missing-bot-id.json"
    updated = bridge_module.read_json_file(processing, None)
    assert updated is not None
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
async def test_bridge_reset_command_uses_full_session_reset(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    calls = []

    async def fake_reset_session_command(_bot, key, req_id):
        calls.append((key, req_id))

    monkeypatch.setattr(bridge_module, "reset_session_command", fake_reset_session_command)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-reset"},
            "body": {
                "msgtype": "text",
                "text": {"content": "/bridge-reset"},
                "from": {"userid": "test-user"},
            },
        },
    )

    assert calls == [("single:test-user", "req-reset")]


@pytest.mark.asyncio
async def test_cwd_command_rejected_in_team_mode(bridge_module):
    bot = make_bot(bridge_module)
    replies = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    bridge_module.respond_info = fake_respond_info

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-cwd-team"},
        "body": {
            "msgtype": "text",
            "text": {"content": "/cwd repo"},
            "from": {"userid": "test-user"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert replies == [("req-cwd-team", "仅 personal 模式支持 /cwd；team 模式不允许切换。")]


@pytest.mark.asyncio
async def test_cwd_command_updates_personal_session_dir(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-cwd"
    target_dir = work_dir / "services" / "api"
    target_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"
    sess = make_session(bridge_module, bot)
    sess.thread_id = "thread-old"
    sess.resume_candidates = [{"sessionId": "session-old", "threadId": "thread-old"}]
    sess.resume_selection_expires_at = bridge_module.now_ms() + 60_000
    bridge_module.update_session_record(
        sess.session_id,
        lambda current: {
            **current,
            "threadId": "thread-old",
        },
    )
    replies = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-cwd-personal"},
            "body": {
                "msgtype": "text",
                "text": {"content": "/cwd services/api"},
                "from": {"userid": "test-user"},
            },
        },
    )

    record = bridge_module.read_session_record_by_id(sess.session_id)

    assert sess.work_dir == str(target_dir.resolve())
    assert sess.thread_id is None
    assert sess.resume_candidates == []
    assert sess.resume_selection_expires_at == 0
    assert record is not None
    assert record["cwdDir"] == str(target_dir.resolve())
    assert record["threadId"] is None
    assert replies == [("req-cwd-personal", f"cwd changed to {target_dir.resolve()}")]


@pytest.mark.asyncio
async def test_cwd_command_rejected_while_running(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-running"
    target_dir = work_dir / "services" / "api"
    target_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"
    sess = make_session(bridge_module, bot)
    sess.running = True
    replies = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-cwd-running"},
            "body": {
                "msgtype": "text",
                "text": {"content": "/cwd services/api"},
                "from": {"userid": "test-user"},
            },
        },
    )

    assert replies == [("req-cwd-running", "当前有任务在运行，不能切换 cwd。请先等待完成或使用 /bridge-interrupt。")]


@pytest.mark.asyncio
async def test_cwd_command_resolves_relative_paths_from_current_personal_dir(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-relative-cwd"
    nested_dir = work_dir / "services" / "api"
    nested_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"
    sess = make_session(bridge_module, bot)
    bridge_module.update_session_record(
        sess.session_id,
        lambda current: {
            **current,
            "cwdDir": str(nested_dir.resolve()),
            "updatedAt": bridge_module.now_ms(),
        },
    )
    sess.work_dir = str(nested_dir.resolve())
    replies = []

    async def fake_respond_info(_bot, req_id, message):
        replies.append((req_id, message))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-cwd-parent"},
            "body": {
                "msgtype": "text",
                "text": {"content": "/cwd .."},
                "from": {"userid": "test-user"},
            },
        },
    )

    parent_dir = (nested_dir / "..").resolve()
    record = bridge_module.read_session_record_by_id(sess.session_id)

    assert sess.work_dir == str(parent_dir)
    assert record is not None
    assert record["cwdDir"] == str(parent_dir)
    assert replies == [("req-cwd-parent", f"cwd changed to {parent_dir}")]


@pytest.mark.asyncio
async def test_cwd_command_rejects_switch_while_work_is_queued(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-queued-cwd"
    next_dir = work_dir / "next"
    next_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"
    sess = make_session(bridge_module, bot)
    sess.queue.append({"text": "earlier request", "reqId": "req-earlier"})
    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.cwd_session_command(bot, "single:test-user", "req-cwd", str(next_dir))

    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert sess.work_dir != str(next_dir)
    assert persisted.get("cwdDir") != str(next_dir)
    assert "不能切换 cwd" in replies[-1][1]


@pytest.mark.asyncio
async def test_cwd_command_rejects_foreign_session_lease(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    next_dir = repo_dir / "next"
    next_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    current = make_session(bridge_module, bot, "single:test-user")
    foreign_lease = {
        "instanceId": "foreign-instance",
        "pid": 12345,
        "botId": bot.config["id"],
        "chatKey": "single:test-user",
        "updatedAt": bridge_module.now_ms(),
        "expiresAt": bridge_module.now_ms() + 60_000,
    }
    bridge_module.write_lease(current.lock_file, foreign_lease)
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "ownerInstance": foreign_lease["instanceId"],
            "ownerPid": foreign_lease["pid"],
            "leaseExpiresAt": foreign_lease["expiresAt"],
        },
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.cwd_session_command(bot, "single:test-user", "req-cwd", str(next_dir))

    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert exc_info.value.status_code == 409
    assert "owned by another instance" in exc_info.value.message
    assert current.work_dir == str(repo_dir.resolve())
    assert persisted.get("cwdDir") == str(repo_dir.resolve())


@pytest.mark.asyncio
async def test_cwd_command_acquires_unowned_session_lease_before_switching(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    next_dir = repo_dir / "next"
    next_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    current = make_session(bridge_module, bot, "single:test-user")
    bridge_module.release_session_lease(bot, "single:test-user", current)

    async def fake_respond_info(_bot, req_id, message, final=True):
        return None

    bridge_module.respond_info = fake_respond_info
    await bridge_module.cwd_session_command(bot, "single:test-user", "req-cwd", str(next_dir))

    lease = bridge_module.read_lease(current.lock_file)
    assert current.work_dir == str(next_dir.resolve())
    assert current.lease_owned is True
    assert lease.get("instanceId") == bridge_module.INSTANCE_ID


@pytest.mark.asyncio
async def test_cwd_command_allows_read_only_query_while_work_is_queued(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-query-cwd"
    work_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"
    sess = make_session(bridge_module, bot)
    sess.queue.append({"text": "earlier request", "reqId": "req-earlier"})
    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.cwd_session_command(bot, "single:test-user", "req-cwd", None)

    assert replies == [("req-cwd", f"Current cwd: {work_dir.resolve()}", True)]
    assert sess.queue == [{"text": "earlier request", "reqId": "req-earlier"}]


@pytest.mark.asyncio
async def test_cwd_command_clears_stale_local_resume_backup(bridge_module):
    work_dir = bridge_module.BASE_DIR / "repo-personal-backup-cwd"
    next_dir = work_dir / "next"
    next_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(work_dir))
    bot.config["workspaceMode"] = "personal"
    sess = make_session(bridge_module, bot)
    bridge_module.update_session_record(
        sess.session_id,
        lambda current: {
            **current,
            "localResumeBackup": {
                "threadId": "stale-thread",
                "recentChat": [{"role": "user", "text": "stale conversation"}],
                "useGlobalCodexSessions": False,
            },
        },
    )

    async def fake_respond_info(_bot, req_id, message, final=True):
        return None

    bridge_module.respond_info = fake_respond_info

    await bridge_module.cwd_session_command(bot, "single:test-user", "req-cwd", str(next_dir))

    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted.get("localResumeBackup") is None


@pytest.mark.asyncio
async def test_resume_command_lists_candidates_for_current_chat_scope(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "thread-current"
    bridge_module.update_session_record(current.session_id, lambda record: {**record, "lastRunAt": bridge_module.now_ms()})
    other_visible = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    visible_sess = bridge_module.SessionState(
        session_id=other_visible["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, "group-user:room-1:test-user")),
        lock_file=Path(other_visible["lockFile"]),
        thread_id="thread-old",
    )
    bot.sessions["group-user:room-1:test-user"] = visible_sess
    bridge_module.update_session_record(other_visible["sessionId"], lambda record: {**record, "lastRunAt": bridge_module.now_ms() - 1000})
    other_user = bridge_module.create_session_record(bot, "single:someone-else")
    hidden_sess = bridge_module.SessionState(
        session_id=other_user["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, "single:someone-else")),
        lock_file=Path(other_user["lockFile"]),
        thread_id="thread-hidden",
    )
    bot.sessions["single:someone-else"] = hidden_sess
    bridge_module.update_session_record(other_user["sessionId"], lambda record: {**record, "lastRunAt": bridge_module.now_ms() - 2000})

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
async def test_resume_command_lists_persisted_candidate_missing_bot_id_when_managed_lock_matches(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "thread-current"
    bridge_module.update_session_record(current.session_id, lambda record: {**record, "lastRunAt": bridge_module.now_ms()})
    persisted_session_id = "sess-persisted"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(persisted_session_id),
        {
            "sessionId": persisted_session_id,
            "chatKey": "group-user:room-1:test-user",
            "threadId": "thread-persisted",
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], persisted_session_id)),
            "createdAt": bridge_module.now_ms() - 2000,
            "updatedAt": bridge_module.now_ms() - 2000,
            "lastRunAt": bridge_module.now_ms() - 1000,
            "status": "idle",
        },
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
            "body": {
                "msgtype": "text",
                "text": {"content": "/bridge-resume"},
                "from": {"userid": "test-user"},
            },
        },
    )

    assert replies
    content = replies[0][1]
    assert persisted_session_id in content
    assert len(current.resume_candidates) == 2


@pytest.mark.asyncio
async def test_resume_command_lists_persisted_candidate_missing_chat_key_when_key_file_matches(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "thread-current"
    bridge_module.update_session_record(current.session_id, lambda record: {**record, "lastRunAt": bridge_module.now_ms()})
    persisted_session_id = "sess-persisted-chat-missing"
    bridge_module.write_json_atomic(
        bridge_module.get_registry_session_file(persisted_session_id),
        {
            "sessionId": persisted_session_id,
            "botId": bot.config["id"],
            "threadId": "thread-persisted",
            "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], persisted_session_id)),
            "createdAt": bridge_module.now_ms() - 2000,
            "updatedAt": bridge_module.now_ms() - 2000,
            "lastRunAt": bridge_module.now_ms() - 1000,
            "status": "idle",
        },
    )
    key_file = bridge_module.get_registry_key_file(bot.config["id"], "group-user:room-1:test-user")
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps(
            {
                "sessionId": persisted_session_id,
                "botId": bot.config["id"],
                "chatKey": "group-user:room-1:test-user",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
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
            "body": {
                "msgtype": "text",
                "text": {"content": "/bridge-resume"},
                "from": {"userid": "test-user"},
            },
        },
    )

    assert replies
    content = replies[0][1]
    assert persisted_session_id in content
    assert len(current.resume_candidates) == 2


@pytest.mark.asyncio
async def test_resume_command_binds_selected_thread_to_current_chat(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bot.sessions["group-user:room-1:test-user"] = bridge_module.SessionState(
        session_id=target["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, "group-user:room-1:test-user")),
        lock_file=Path(target["lockFile"]),
        thread_id="thread-target",
    )
    bridge_module.update_session_record(target["sessionId"], lambda record: {**record, "lastRunAt": bridge_module.now_ms()})

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
    assert persisted.get("threadId") == "thread-target"
    assert current.resume_candidates == []
    assert "已选择会话" in replies[-1][1]


@pytest.mark.asyncio
async def test_resume_command_copies_recent_chat_from_selected_session(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bot.sessions["group-user:room-1:test-user"] = bridge_module.SessionState(
        session_id=target["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, "group-user:room-1:test-user")),
        lock_file=Path(target["lockFile"]),
        thread_id="thread-target",
    )
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {
            **record,
            "lastRunAt": bridge_module.now_ms(),
            "recentChat": [
                {"role": "user", "text": "older user turn"},
                {"role": "bot", "text": "older bot turn"},
            ],
        },
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

    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert [item["text"] for item in current.chat] == ["older user turn", "older bot turn"]
    assert [item["text"] for item in persisted.get("recentChat") or []] == ["older user turn", "older bot turn"]
    assert current.resume_candidates == []
    assert "已选择会话" in replies[-1][1]


@pytest.mark.asyncio
async def test_resume_command_rejects_switch_while_work_is_queued(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    current.queue.append({"text": "earlier request", "reqId": "req-earlier"})
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {**record, "threadId": "thread-target"},
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.resume_session_command(bot, "single:test-user", "req-resume", target["sessionId"])

    assert exc_info.value.status_code == 409
    assert current.thread_id is None
    assert current.queue == [{"text": "earlier request", "reqId": "req-earlier"}]


@pytest.mark.asyncio
async def test_resume_command_acquires_unowned_session_lease_before_binding(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    bridge_module.release_session_lease(bot, "single:test-user", current)
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {**record, "threadId": "thread-target"},
    )

    async def fake_respond_info(_bot, req_id, message, final=True):
        return None

    bridge_module.respond_info = fake_respond_info
    await bridge_module.resume_session_command(bot, "single:test-user", "req-resume", target["sessionId"])

    lease = bridge_module.read_lease(current.lock_file)
    assert current.thread_id == "thread-target"
    assert current.lease_owned is True
    assert lease.get("instanceId") == bridge_module.INSTANCE_ID


@pytest.mark.asyncio
async def test_resume_command_rejects_foreign_session_lease(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {**record, "threadId": "thread-target"},
    )
    foreign_lease = {
        "instanceId": "foreign-instance",
        "pid": 12345,
        "botId": bot.config["id"],
        "chatKey": "single:test-user",
        "updatedAt": bridge_module.now_ms(),
        "expiresAt": bridge_module.now_ms() + 60_000,
    }
    bridge_module.write_lease(current.lock_file, foreign_lease)
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "ownerInstance": foreign_lease["instanceId"],
            "ownerPid": foreign_lease["pid"],
            "leaseExpiresAt": foreign_lease["expiresAt"],
        },
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.resume_session_command(bot, "single:test-user", "req-resume", target["sessionId"])

    assert exc_info.value.status_code == 409
    assert "owned by another instance" in exc_info.value.message
    assert current.thread_id is None


@pytest.mark.asyncio
async def test_resume_selection_revalidates_target_thread_before_binding(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {**record, "threadId": "thread-target", "lastRunAt": bridge_module.now_ms()},
    )
    current.resume_candidates = bridge_module.build_resume_candidates(bot, "single:test-user")
    current.resume_selection_expires_at = bridge_module.now_ms() + bridge_module.RESUME_SELECTION_TTL_MS
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {**record, "threadId": None},
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.apply_resume_selection_command(
            bot,
            "single:test-user",
            "req-select",
            target["sessionId"],
            sender_user_id="test-user",
        )

    assert exc_info.value.status_code == 404
    assert current.thread_id is None


@pytest.mark.asyncio
async def test_resume_command_rejects_session_owned_by_another_bot(bridge_module):
    current_bot = make_bot(bridge_module, config_id="bot-current", remote_bot_id="remote-current")
    make_session(bridge_module, current_bot, "single:test-user")
    other_bot = make_bot(bridge_module, config_id="bot-other", remote_bot_id="remote-other")
    target = bridge_module.create_session_record(other_bot, "single:test-user")
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {**record, "threadId": "thread-other-bot"},
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.resume_session_command(
            current_bot,
            "single:test-user",
            "req-resume",
            target["sessionId"],
        )

    assert exc_info.value.status_code == 403
    assert current_bot.sessions["single:test-user"].thread_id is None


@pytest.mark.asyncio
async def test_resume_selection_invalid_choice_is_reported(bridge_module):
    bot = make_bot(bridge_module)
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "thread-current"
    bridge_module.update_session_record(current.session_id, lambda record: {**record, "lastRunAt": bridge_module.now_ms()})

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
async def test_local_resume_command_lists_matching_local_codex_sessions(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        write_local_codex_rollout(
            local_home,
            session_id="local-match",
            cwd=repo_dir,
            transcript=[("user", "local question"), ("assistant", "local answer")],
        )
        write_local_codex_rollout(
            local_home,
            session_id="local-other",
            cwd=other_dir,
            transcript=[("user", "other question")],
        )

        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {"msgtype": "text", "text": {"content": "/local-resume"}, "from": {"userid": "test-user"}},
            },
        )

        assert replies
        content = replies[0][1]
        assert "当前目录下的本地会话" in content
        assert "local-match" in content
        assert "local-other" not in content
        assert len(current.resume_candidates) == 1
        assert current.resume_candidates[0]["source"] == "local"
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_command_lists_candidates_without_loading_transcripts(bridge_module, tmp_path, monkeypatch):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        write_local_codex_rollout(
            local_home,
            session_id="local-match",
            cwd=repo_dir,
            transcript=[("user", "local question"), ("assistant", "local answer")],
        )

        def fail_load(*args, **kwargs):
            raise AssertionError("candidate listing should not load transcript content")

        monkeypatch.setattr(bridge_module, "load_local_codex_recent_chat", fail_load)
        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {"msgtype": "text", "text": {"content": "/local-resume"}, "from": {"userid": "test-user"}},
            },
        )

        assert replies
        assert "local-match" in replies[0][1]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_command_binds_local_session_and_persists_global_session_mode(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        write_local_codex_rollout(
            local_home,
            session_id="local-bind",
            cwd=repo_dir,
            transcript=[("user", "older user turn"), ("assistant", "older bot turn")],
        )

        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {"msgtype": "text", "text": {"content": "/local-resume"}, "from": {"userid": "test-user"}},
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

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert current.thread_id == "local-bind"
        assert current.use_global_codex_sessions is True
        assert [item["text"] for item in current.chat] == ["older user turn", "older bot turn"]
        assert persisted.get("threadId") == "local-bind"
        assert persisted.get("useGlobalCodexSessions") is True
        assert "已切换到本地会话" in replies[-1][1]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_detach_restores_previous_bridge_state_after_local_resume(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "bridge-thread"
    current.chat = [
        {"role": "user", "text": "bridge user turn"},
        {"role": "bot", "text": "bridge bot turn"},
    ]
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "threadId": "bridge-thread",
            "recentChat": list(current.chat),
        },
    )
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        write_local_codex_rollout(
            local_home,
            session_id="local-bind",
            cwd=repo_dir,
            transcript=[("user", "local user turn"), ("assistant", "local bot turn")],
        )

        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {"msgtype": "text", "text": {"content": "/local-resume local-bind"}, "from": {"userid": "test-user"}},
            },
        )
        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-detach"},
                "body": {"msgtype": "text", "text": {"content": "/local-detach"}, "from": {"userid": "test-user"}},
            },
        )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert current.thread_id == "bridge-thread"
        assert current.use_global_codex_sessions is False
        assert [item["text"] for item in current.chat] == ["bridge user turn", "bridge bot turn"]
        assert persisted.get("threadId") == "bridge-thread"
        assert persisted.get("useGlobalCodexSessions") is False
        assert [item["text"] for item in persisted.get("recentChat") or []] == ["bridge user turn", "bridge bot turn"]
        assert persisted.get("localResumeBackup") is None
        assert "已解除本地会话绑定" in replies[-1][1]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_new_local_resume_replaces_stale_backup_after_binding_was_cleared(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "current-thread"
    current.chat = [
        {"role": "user", "text": "current user turn"},
        {"role": "bot", "text": "current bot turn"},
    ]
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "threadId": current.thread_id,
            "useGlobalCodexSessions": False,
            "recentChat": list(current.chat),
            "localResumeBackup": {
                "threadId": "stale-thread",
                "recentChat": [{"role": "user", "text": "stale user turn"}],
                "useGlobalCodexSessions": False,
            },
        },
    )

    bridge_module.bind_resume_candidate_to_session(
        bot,
        current,
        "single:test-user",
        {
            "sessionId": "new-local-thread",
            "threadId": "new-local-thread",
            "source": "local",
            "recentChat": [{"role": "user", "text": "local user turn"}],
            "cwd": repo_dir,
        },
    )

    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info
    await bridge_module.local_detach_session_command(
        bot,
        "single:test-user",
        "req-local-detach",
        sender_user_id="test-user",
    )

    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert current.thread_id == "current-thread"
    assert [item["text"] for item in current.chat] == ["current user turn", "current bot turn"]
    assert persisted.get("threadId") == "current-thread"
    assert [item["text"] for item in persisted.get("recentChat") or []] == ["current user turn", "current bot turn"]


@pytest.mark.asyncio
async def test_local_resume_rejects_thread_already_bound_to_another_chat(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    first = make_session(bridge_module, bot, "single:test-user")
    second = make_session(bridge_module, bot, "group-user:room-1:test-user")
    candidate = {
        "sessionId": "local-shared-thread",
        "threadId": "local-shared-thread",
        "source": "local",
        "recentChat": [{"role": "user", "text": "local conversation"}],
        "cwd": repo_dir.resolve(),
    }

    bridge_module.bind_resume_candidate_to_session(bot, first, "single:test-user", candidate)

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        bridge_module.bind_resume_candidate_to_session(bot, second, "group-user:room-1:test-user", candidate)

    persisted_second = bridge_module.read_session_record_by_id(second.session_id)
    assert exc_info.value.status_code == 409
    assert "/local-detach" in exc_info.value.message
    assert second.thread_id is None
    assert second.use_global_codex_sessions is False
    assert persisted_second.get("threadId") is None
    assert persisted_second.get("useGlobalCodexSessions") is False


@pytest.mark.asyncio
async def test_local_resume_allows_rebinding_after_original_chat_detaches(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    first = make_session(bridge_module, bot, "single:test-user")
    second = make_session(bridge_module, bot, "group-user:room-1:test-user")
    candidate = {
        "sessionId": "local-transfer-thread",
        "threadId": "local-transfer-thread",
        "source": "local",
        "recentChat": [{"role": "user", "text": "local conversation"}],
        "cwd": repo_dir.resolve(),
    }
    bridge_module.bind_resume_candidate_to_session(bot, first, "single:test-user", candidate)

    async def fake_respond_info(_bot, req_id, message, final=True):
        return None

    bridge_module.respond_info = fake_respond_info
    await bridge_module.local_detach_session_command(
        bot,
        "single:test-user",
        "req-detach",
        sender_user_id="test-user",
    )
    bridge_module.bind_resume_candidate_to_session(bot, second, "group-user:room-1:test-user", candidate)

    assert first.use_global_codex_sessions is False
    assert second.thread_id == "local-transfer-thread"
    assert second.use_global_codex_sessions is True


def test_local_resume_ignores_stale_team_mode_binding(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    team_bot = make_bot(
        bridge_module,
        config_id="bot-team",
        remote_bot_id="remote-team",
        work_dir=str(repo_dir),
    )
    stale = make_session(bridge_module, team_bot, "single:test-user")
    stale.thread_id = "local-thread"
    stale.use_global_codex_sessions = True
    bridge_module.update_session_record(
        stale.session_id,
        lambda record: {
            **record,
            "threadId": "local-thread",
            "useGlobalCodexSessions": True,
        },
    )
    personal_bot = make_bot(
        bridge_module,
        config_id="bot-personal",
        remote_bot_id="remote-personal",
        work_dir=str(repo_dir),
    )
    personal_bot.config["workspaceMode"] = "personal"
    personal_bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, personal_bot, "single:test-user")

    bridge_module.bind_resume_candidate_to_session(
        personal_bot,
        current,
        "single:test-user",
        {
            "sessionId": "local-thread",
            "threadId": "local-thread",
            "source": "local",
            "recentChat": [],
            "cwd": repo_dir.resolve(),
        },
    )

    assert current.thread_id == "local-thread"
    assert current.use_global_codex_sessions is True


def test_local_resume_rejects_thread_bound_by_another_personal_bot(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    first_bot = make_bot(
        bridge_module,
        config_id="bot-personal-a",
        remote_bot_id="remote-personal-a",
        work_dir=str(repo_dir),
    )
    first_bot.config["workspaceMode"] = "personal"
    first_bot.config["localResumeOwnerUserId"] = "test-user"
    first = make_session(bridge_module, first_bot, "single:test-user")
    candidate = {
        "sessionId": "shared-global-thread",
        "threadId": "shared-global-thread",
        "source": "local",
        "recentChat": [],
        "cwd": repo_dir.resolve(),
    }
    bridge_module.bind_resume_candidate_to_session(first_bot, first, "single:test-user", candidate)
    second_bot = make_bot(
        bridge_module,
        config_id="bot-personal-b",
        remote_bot_id="remote-personal-b",
        work_dir=str(repo_dir),
    )
    second_bot.config["workspaceMode"] = "personal"
    second_bot.config["localResumeOwnerUserId"] = "test-user"
    second = make_session(bridge_module, second_bot, "single:test-user")

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        bridge_module.bind_resume_candidate_to_session(second_bot, second, "single:test-user", candidate)

    assert exc_info.value.status_code == 409
    assert second.thread_id is None
    assert second.use_global_codex_sessions is False


@pytest.mark.asyncio
async def test_local_resume_rejected_while_earlier_work_is_queued(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.queue.append({"text": "earlier request", "reqId": "req-earlier"})

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        bridge_module.ensure_local_resume_allowed(
            bot,
            current,
            "single:test-user",
            sender_user_id="test-user",
        )

    assert exc_info.value.status_code == 409
    assert current.queue == [{"text": "earlier request", "reqId": "req-earlier"}]


def test_local_resume_rejected_while_final_delivery_is_pending(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.pending_final_payload = {"cmd": "aibot_respond_msg"}

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        bridge_module.ensure_local_resume_allowed(
            bot,
            current,
            "single:test-user",
            sender_user_id="test-user",
        )

    assert exc_info.value.status_code == 409


def test_local_resume_rejected_while_foreign_instance_owns_session_lease(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    foreign_lease = {
        "instanceId": "foreign-instance",
        "pid": 12345,
        "botId": bot.config["id"],
        "chatKey": "single:test-user",
        "updatedAt": bridge_module.now_ms(),
        "expiresAt": bridge_module.now_ms() + 60_000,
    }
    bridge_module.write_lease(current.lock_file, foreign_lease)
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "ownerInstance": foreign_lease["instanceId"],
            "ownerPid": foreign_lease["pid"],
            "leaseExpiresAt": foreign_lease["expiresAt"],
        },
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        bridge_module.ensure_local_resume_allowed(
            bot,
            current,
            "single:test-user",
            sender_user_id="test-user",
        )

    assert exc_info.value.status_code == 409
    assert "owned by another instance" in exc_info.value.message


@pytest.mark.parametrize(
    ("field_name", "pending_value"),
    [
        ("pending_media_downloads", 1),
        ("pending_media", [{"kind": "image", "path": "/tmp/pending.png"}]),
        ("pending_media_notes", ["attachment download failed"]),
    ],
)
def test_local_resume_rejected_while_attachment_state_is_pending(
    bridge_module,
    tmp_path,
    field_name,
    pending_value,
):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    setattr(current, field_name, pending_value)

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        bridge_module.ensure_local_resume_allowed(
            bot,
            current,
            "single:test-user",
            sender_user_id="test-user",
        )

    assert exc_info.value.status_code == 409
    assert getattr(current, field_name) == pending_value


@pytest.mark.asyncio
async def test_local_resume_command_strips_bridge_wrapper_from_imported_local_chat(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        wrapped_user_turn = (
            "[BridgeContext]\n"
            "chatKey: single:test-user\n"
            "sessionId: old-session\n"
            "[/BridgeContext]\n\n"
            "User request:\n"
            "真正的问题\n\n"
            "[RecentConversation]\n"
            "User: stale context\n"
            "[/RecentConversation]"
        )
        write_local_codex_rollout(
            local_home,
            session_id="local-wrapped",
            cwd=repo_dir,
            transcript=[("user", wrapped_user_turn), ("assistant", "older bot turn")],
        )

        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {"msgtype": "text", "text": {"content": "/local-resume local-wrapped"}, "from": {"userid": "test-user"}},
            },
        )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert [item["text"] for item in current.chat] == ["真正的问题", "older bot turn"]
        assert [item["text"] for item in persisted.get("recentChat") or []] == ["真正的问题", "older bot turn"]
        assert "BridgeContext" not in current.chat[0]["text"]
        assert "RecentConversation" not in current.chat[0]["text"]
        assert "已切换到本地会话" in replies[-1][1]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_command_strips_image_prefixed_bridge_wrapper_from_imported_local_chat(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        wrapped_user_turn = (
            "<image name=[Image #1] path=\"/tmp/a.png\">\n"
            "</image>\n"
            "<image name=[Image #2] path=\"/tmp/b.png\">\n"
            "</image>\n"
            "[BridgeContext]\n"
            "chatKey: single:test-user\n"
            "sessionId: old-session\n"
            "[/BridgeContext]\n\n"
            "User request:\n"
            "图片相关的问题"
        )
        write_local_codex_rollout(
            local_home,
            session_id="local-image-wrapped",
            cwd=repo_dir,
            transcript=[("user", wrapped_user_turn), ("assistant", "图片答案")],
        )

        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {"msgtype": "text", "text": {"content": "/local-resume local-image-wrapped"}, "from": {"userid": "test-user"}},
            },
        )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert [item["text"] for item in current.chat] == ["图片相关的问题", "图片答案"]
        assert [item["text"] for item in persisted.get("recentChat") or []] == ["图片相关的问题", "图片答案"]
        assert "BridgeContext" not in current.chat[0]["text"]
        assert "<image" not in current.chat[0]["text"]
        assert "已切换到本地会话" in replies[-1][1]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_command_filters_protocol_noise_from_imported_local_chat(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        write_local_codex_rollout(
            local_home,
            session_id="local-protocol-noise",
            cwd=repo_dir,
            transcript=[
                {"role": "user", "text": "<codex_internal_context source=\"goal\">\ninternal\n</codex_internal_context>"},
                {"role": "user", "text": "<goal_context>\nignore me\n</goal_context>"},
                {"role": "user", "text": "<user_action>\nignore me too\n</user_action>"},
                {
                    "role": "user",
                    "text": "⚠ Codex could not find bubblewrap on PATH. Install bubblewrap with your OS package manager. See the sandbox prerequisites:\n  https://developers.openai.com/codex/concepts/sandboxing#prerequisites. Codex will use the bundled bubblewrap in the meantime.\n帮我解决这个环境问题",
                },
                {"role": "assistant", "text": "我先检查当前目录和最近改动。", "phase": "commentary"},
                {"role": "user", "text": "真正的问题"},
                {"role": "assistant", "text": "最终答案", "phase": "final_answer"},
            ],
        )

        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {"msgtype": "text", "text": {"content": "/local-resume local-protocol-noise"}, "from": {"userid": "test-user"}},
            },
        )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert [item["text"] for item in current.chat] == ["帮我解决这个环境问题", "真正的问题", "最终答案"]
        assert [item["text"] for item in persisted.get("recentChat") or []] == ["帮我解决这个环境问题", "真正的问题", "最终答案"]
        assert "已切换到本地会话" in replies[-1][1]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


def test_local_resume_candidates_use_rollout_mtime_for_recency(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        older_mtime = 1_784_528_000_000
        newer_mtime = older_mtime + 60_000
        write_local_codex_rollout(
            local_home,
            session_id="older-session",
            cwd=repo_dir,
            session_timestamp="2026-07-20T06:59:28.929Z",
            mtime_ms=older_mtime,
        )
        write_local_codex_rollout(
            local_home,
            session_id="newer-session",
            cwd=repo_dir,
            session_timestamp="2026-07-20T05:13:28.929Z",
            mtime_ms=newer_mtime,
        )

        candidates = bridge_module.list_local_codex_resume_candidates(repo_dir)

        assert [item["sessionId"] for item in candidates[:2]] == ["newer-session", "older-session"]
        assert candidates[0]["lastRunAt"] == newer_mtime
        assert candidates[1]["lastRunAt"] == older_mtime
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


def test_local_resume_candidates_ignore_non_object_rollout_metadata(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        valid_rollout = write_local_codex_rollout(
            local_home,
            session_id="valid-session",
            cwd=repo_dir,
        )
        malformed_rollout = valid_rollout.parent / "rollout-2026-07-20T06-14-28-malformed.jsonl"
        malformed_rollout.write_text("[]\n", encoding="utf-8")

        candidates = bridge_module.list_local_codex_resume_candidates(repo_dir)

        assert [item["sessionId"] for item in candidates] == ["valid-session"]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


def test_local_resume_history_ignores_non_object_jsonl_entries(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    rollout = write_local_codex_rollout(
        tmp_path / "codex-home",
        session_id="valid-session",
        cwd=repo_dir,
        transcript=[("user", "question"), ("assistant", "answer")],
    )
    lines = rollout.read_text(encoding="utf-8").splitlines()
    rollout.write_text("\n".join([lines[0], "[]", *lines[1:]]) + "\n", encoding="utf-8")

    history = bridge_module.load_local_codex_recent_chat(rollout)

    assert [(item["role"], item["text"]) for item in history] == [
        ("user", "question"),
        ("bot", "answer"),
    ]


def test_local_resume_history_is_bounded_while_scanning(bridge_module, tmp_path, monkeypatch):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    transcript = [
        ("user" if index % 2 == 0 else "assistant", f"message-{index}")
        for index in range(20)
    ]
    rollout = write_local_codex_rollout(
        tmp_path / "codex-home",
        session_id="long-session",
        cwd=repo_dir,
        transcript=transcript,
    )
    normalized_input_lengths = []
    original_normalize = bridge_module.normalize_session_chat_items

    def tracking_normalize(items, *, limit=bridge_module.MAX_PERSISTED_SESSION_CHAT_ITEMS):
        normalized_input_lengths.append(len(items))
        return original_normalize(items, limit=limit)

    monkeypatch.setattr(bridge_module, "normalize_session_chat_items", tracking_normalize)

    history = bridge_module.load_local_codex_recent_chat(rollout, limit=3)

    assert normalized_input_lengths == [3]
    assert [(item["role"], item["text"]) for item in history] == [
        ("bot", "message-17"),
        ("user", "message-18"),
        ("bot", "message-19"),
    ]


def test_local_resume_candidates_tolerate_rollout_deleted_during_sort(bridge_module, tmp_path, monkeypatch):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    valid_rollout = write_local_codex_rollout(
        local_home,
        session_id="valid-session",
        cwd=repo_dir,
    )
    disappearing_rollout = valid_rollout.parent / "rollout-2026-07-20T06-14-28-disappearing.jsonl"
    disappearing_rollout.write_text("{}\n", encoding="utf-8")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    original_exists = Path.exists
    original_stat = Path.stat
    disappearing_stat_calls = 0

    def always_exists_during_sort(path):
        if path == disappearing_rollout:
            return True
        return original_exists(path)

    def disappear_after_discovery(path, *args, **kwargs):
        nonlocal disappearing_stat_calls
        if path == disappearing_rollout:
            disappearing_stat_calls += 1
            if disappearing_stat_calls >= 2:
                raise FileNotFoundError(str(path))
        return original_stat(path, *args, **kwargs)

    bridge_module.DEFAULT_CODEX_HOME = local_home
    monkeypatch.setattr(Path, "exists", always_exists_during_sort)
    monkeypatch.setattr(Path, "stat", disappear_after_discovery)
    try:
        candidates = bridge_module.list_local_codex_resume_candidates(repo_dir)

        assert [item["sessionId"] for item in candidates] == ["valid-session"]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


def test_local_resume_candidates_exclude_sessions_whose_latest_rollout_moved_to_other_cwd(bridge_module, tmp_path):
    repo_a = tmp_path / "repo-a"
    repo_a.mkdir()
    repo_b = tmp_path / "repo-b"
    repo_b.mkdir()
    local_home = tmp_path / "codex-home"
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        older_mtime = 1_784_528_000_000
        newer_mtime = older_mtime + 60_000
        write_local_codex_rollout(
            local_home,
            session_id="moved-session",
            cwd=repo_a,
            rollout_timestamp="2026-07-20T06-13-28",
            mtime_ms=older_mtime,
        )
        write_local_codex_rollout(
            local_home,
            session_id="moved-session",
            cwd=repo_b,
            rollout_timestamp="2026-07-20T06-14-28",
            mtime_ms=newer_mtime,
        )
        write_local_codex_rollout(
            local_home,
            session_id="stable-a",
            cwd=repo_a,
            mtime_ms=older_mtime + 30_000,
        )

        repo_a_candidates = bridge_module.list_local_codex_resume_candidates(repo_a)
        repo_b_candidates = bridge_module.list_local_codex_resume_candidates(repo_b)

        assert [item["sessionId"] for item in repo_a_candidates] == ["stable-a"]
        assert [item["sessionId"] for item in repo_b_candidates] == ["moved-session"]
        assert bridge_module.find_local_codex_resume_candidate("moved-session", repo_a) is None
        assert bridge_module.find_local_codex_resume_candidate("moved-session", repo_b) is not None
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_detach_clears_local_session_binding(bridge_module):
    repo_dir = bridge_module.BASE_DIR / "repo-local-detach"
    repo_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "local-bind"
    current.use_global_codex_sessions = True
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "threadId": "local-bind",
            "useGlobalCodexSessions": True,
        },
    )

    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-local-detach"},
            "body": {"msgtype": "text", "text": {"content": "/local-detach"}, "from": {"userid": "test-user"}},
        },
    )

    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert current.thread_id is None
    assert current.use_global_codex_sessions is False
    assert current.chat == []
    assert persisted.get("threadId") is None
    assert persisted.get("useGlobalCodexSessions") is False
    assert persisted.get("recentChat") == []
    assert "已解除本地会话绑定" in replies[-1][1]


@pytest.mark.asyncio
async def test_local_detach_restores_backup_after_native_resume_falls_back_to_fresh_session(bridge_module):
    repo_dir = bridge_module.BASE_DIR / "repo-local-detach-after-fallback"
    repo_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "fresh-fallback-thread"
    current.chat = [{"role": "user", "text": "fallback conversation"}]
    current.use_global_codex_sessions = False
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "threadId": current.thread_id,
            "recentChat": list(current.chat),
            "useGlobalCodexSessions": False,
            "localResumeBackup": {
                "threadId": "bridge-thread",
                "recentChat": [{"role": "user", "text": "bridge conversation"}],
                "useGlobalCodexSessions": False,
            },
        },
    )

    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.local_detach_session_command(
        bot,
        "single:test-user",
        "req-local-detach",
        sender_user_id="test-user",
    )

    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert current.thread_id == "bridge-thread"
    assert current.use_global_codex_sessions is False
    assert [item["text"] for item in current.chat] == ["bridge conversation"]
    assert persisted.get("threadId") == "bridge-thread"
    assert persisted.get("useGlobalCodexSessions") is False
    assert [item["text"] for item in persisted.get("recentChat") or []] == ["bridge conversation"]
    assert persisted.get("localResumeBackup") is None
    assert "已解除本地会话绑定" in replies[-1][1]


@pytest.mark.asyncio
async def test_local_resume_command_rejects_non_owner_user(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "owner-user"
    make_session(bridge_module, bot, "single:test-user")

    replies = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    bridge_module.respond_info = fake_respond_info

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-local-resume"},
            "body": {"msgtype": "text", "text": {"content": "/local-resume"}, "from": {"userid": "test-user"}},
        },
    )

    assert replies
    assert replies[-1][1] == "Bridge command failed: 当前用户无权使用本地会话命令。"


@pytest.mark.asyncio
async def test_local_resume_command_rejects_shared_group_mode_even_for_owner(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["groupSessionMode"] = "shared"
    bot.config["localResumeOwnerUserId"] = "test-user"
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        write_local_codex_rollout(
            local_home,
            session_id="shared-group-local",
            cwd=repo_dir,
            transcript=[("user", "hello"), ("assistant", "world")],
        )

        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        bridge_module.respond_info = fake_respond_info

        await bridge_module.handle_wecom_message(
            bot,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-local-resume"},
                "body": {
                    "msgtype": "text",
                    "chattype": "group",
                    "chatid": "room-1",
                    "text": {"content": "/local-resume"},
                    "from": {"userid": "test-user"},
                },
            },
        )

        assert replies
        assert replies[-1][1] == "Bridge command failed: shared 群会话不支持本地会话命令。"
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_selection_rechecks_owner_before_binding(bridge_module, tmp_path):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "owner-user"
    current = make_session(bridge_module, bot, "group:room-1")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        rollout = write_local_codex_rollout(
            local_home,
            session_id="owner-local-session",
            cwd=repo_dir,
            transcript=[("user", "secret"), ("assistant", "answer")],
        )
        current.resume_candidates = [
            {
                "sessionId": "owner-local-session",
                "threadId": "owner-local-session",
                "cwd": repo_dir.resolve(),
                "rolloutFile": rollout,
                "updatedAt": 1,
                "lastRunAt": 1,
                "status": "local",
                "source": "local",
            }
        ]
        current.resume_selection_expires_at = bridge_module.now_ms() + 60_000

        with pytest.raises(bridge_module.BridgeError) as exc_info:
            await bridge_module.apply_resume_selection_command(
                bot,
                "group:room-1",
                "req-select",
                "1",
                sender_user_id="intruder-user",
            )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert current.thread_id is None
        assert persisted.get("threadId") is None
        assert exc_info.value.message == "shared 群会话不支持本地会话命令。"
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_selection_rechecks_current_cwd_before_binding(bridge_module, tmp_path):
    repo_a = tmp_path / "repo-a"
    repo_a.mkdir()
    repo_b = tmp_path / "repo-b"
    repo_b.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_a))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = local_home
    try:
        older_mtime = 1_784_528_000_000
        newer_mtime = older_mtime + 60_000
        write_local_codex_rollout(
            local_home,
            session_id="moved-session",
            cwd=repo_a,
            transcript=[("user", "repo a conversation")],
            mtime_ms=older_mtime,
        )
        current.resume_candidates = bridge_module.list_local_codex_resume_candidates(repo_a)
        current.resume_selection_expires_at = bridge_module.now_ms() + 60_000
        write_local_codex_rollout(
            local_home,
            session_id="moved-session",
            cwd=repo_b,
            transcript=[("user", "repo b conversation")],
            mtime_ms=newer_mtime,
        )

        with pytest.raises(bridge_module.BridgeError) as exc_info:
            await bridge_module.apply_resume_selection_command(
                bot,
                "single:test-user",
                "req-select",
                "1",
                sender_user_id="test-user",
            )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert exc_info.value.status_code == 404
        assert current.thread_id is None
        assert current.use_global_codex_sessions is False
        assert current.resume_candidates == []
        assert persisted.get("threadId") is None
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_rejects_rollout_that_moves_during_transcript_load(
    bridge_module,
    tmp_path,
    monkeypatch,
):
    repo_a = tmp_path / "repo-a"
    repo_a.mkdir()
    repo_b = tmp_path / "repo-b"
    repo_b.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_a))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "bridge-thread"
    current.chat = [{"role": "user", "text": "bridge conversation"}]
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "threadId": current.thread_id,
            "recentChat": list(current.chat),
        },
    )
    original_home = bridge_module.DEFAULT_CODEX_HOME
    original_load = bridge_module.load_local_codex_recent_chat
    bridge_module.DEFAULT_CODEX_HOME = local_home
    older_mtime = 1_784_528_000_000
    try:
        write_local_codex_rollout(
            local_home,
            session_id="moving-during-load",
            cwd=repo_a,
            transcript=[("user", "repo a conversation")],
            mtime_ms=older_mtime,
        )

        def move_rollout_during_load(rollout_file, *, limit=bridge_module.MAX_PERSISTED_SESSION_CHAT_ITEMS):
            recent_chat = original_load(rollout_file, limit=limit)
            write_local_codex_rollout(
                local_home,
                session_id="moving-during-load",
                cwd=repo_b,
                transcript=[("user", "repo b conversation")],
                mtime_ms=older_mtime + 60_000,
            )
            return recent_chat

        monkeypatch.setattr(bridge_module, "load_local_codex_recent_chat", move_rollout_during_load)

        with pytest.raises(bridge_module.BridgeError) as exc_info:
            await bridge_module.local_resume_session_command(
                bot,
                "single:test-user",
                "req-local-resume",
                "moving-during-load",
                sender_user_id="test-user",
            )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert exc_info.value.status_code == 404
        assert current.thread_id == "bridge-thread"
        assert current.use_global_codex_sessions is False
        assert [item["text"] for item in current.chat] == ["bridge conversation"]
        assert persisted.get("threadId") == "bridge-thread"
        assert persisted.get("useGlobalCodexSessions") is False
        assert [item["text"] for item in persisted.get("recentChat") or []] == ["bridge conversation"]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_local_resume_reloads_rollout_changed_in_same_cwd_during_transcript_load(
    bridge_module,
    tmp_path,
    monkeypatch,
):
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    local_home = tmp_path / "codex-home"
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    original_load = bridge_module.load_local_codex_recent_chat
    bridge_module.DEFAULT_CODEX_HOME = local_home
    older_mtime = 1_784_528_000_000
    loads = 0
    try:
        write_local_codex_rollout(
            local_home,
            session_id="updated-during-load",
            cwd=repo_dir,
            transcript=[("user", "older conversation")],
            mtime_ms=older_mtime,
        )

        def update_rollout_during_first_load(rollout_file, *, limit=bridge_module.MAX_PERSISTED_SESSION_CHAT_ITEMS):
            nonlocal loads
            loads += 1
            recent_chat = original_load(rollout_file, limit=limit)
            if loads == 1:
                write_local_codex_rollout(
                    local_home,
                    session_id="updated-during-load",
                    cwd=repo_dir,
                    transcript=[("user", "newer conversation"), ("assistant", "newer answer")],
                    mtime_ms=older_mtime + 60_000,
                )
            return recent_chat

        monkeypatch.setattr(bridge_module, "load_local_codex_recent_chat", update_rollout_during_first_load)
        replies = []

        async def fake_respond_info(_bot, req_id, message, final=True):
            replies.append((req_id, message, final))

        monkeypatch.setattr(bridge_module, "respond_info", fake_respond_info)

        await bridge_module.local_resume_session_command(
            bot,
            "single:test-user",
            "req-local-resume",
            "updated-during-load",
            sender_user_id="test-user",
        )

        persisted = bridge_module.read_session_record_by_id(current.session_id)
        assert loads == 2
        assert current.thread_id == "updated-during-load"
        assert current.use_global_codex_sessions is True
        assert [item["text"] for item in current.chat] == ["newer conversation", "newer answer"]
        assert persisted.get("threadId") == "updated-during-load"
        assert persisted.get("useGlobalCodexSessions") is True
        assert "已切换到本地会话" in replies[-1][1]
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home


@pytest.mark.asyncio
async def test_bridge_resume_rejects_unauthorized_inherited_local_binding(bridge_module):
    bot = make_bot(bridge_module)
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "other-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "current-thread"
    current.chat = [{"role": "user", "text": "current conversation"}]
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "threadId": current.thread_id,
            "recentChat": list(current.chat),
        },
    )
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {
            **record,
            "threadId": "local-thread",
            "useGlobalCodexSessions": True,
        },
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.resume_session_command(
            bot,
            "single:test-user",
            "req-resume",
            target["sessionId"],
        )

    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert exc_info.value.status_code == 403
    assert current.thread_id == "current-thread"
    assert current.use_global_codex_sessions is False
    assert [item["text"] for item in current.chat] == ["current conversation"]
    assert persisted.get("threadId") == "current-thread"
    assert persisted.get("useGlobalCodexSessions") is False
    assert persisted.get("localResumeBackup") is None


@pytest.mark.asyncio
async def test_bridge_resume_rejects_inherited_local_thread_binding(bridge_module):
    repo_dir = bridge_module.BASE_DIR / "repo-bridge-resume-global"
    repo_dir.mkdir(parents=True, exist_ok=True)
    bot = make_bot(bridge_module, work_dir=str(repo_dir))
    bot.config["workspaceMode"] = "personal"
    bot.config["localResumeOwnerUserId"] = "test-user"
    current = make_session(bridge_module, bot, "single:test-user")
    current.thread_id = "bridge-thread"
    current.chat = [
        {"role": "user", "text": "bridge user turn"},
        {"role": "bot", "text": "bridge bot turn"},
    ]
    bridge_module.update_session_record(
        current.session_id,
        lambda record: {
            **record,
            "threadId": current.thread_id,
            "recentChat": list(current.chat),
            "lastRunAt": bridge_module.now_ms() - 1000,
        },
    )
    target = bridge_module.create_session_record(bot, "group-user:room-1:test-user")
    target_session = bridge_module.SessionState(
        session_id=target["sessionId"],
        work_dir=str(bridge_module.get_session_runtime_cwd(bot, "group-user:room-1:test-user")),
        lock_file=Path(target["lockFile"]),
        thread_id="local-thread",
        use_global_codex_sessions=True,
    )
    bot.sessions["group-user:room-1:test-user"] = target_session
    bridge_module.update_session_record(
        target["sessionId"],
        lambda record: {
            **record,
            "threadId": "local-thread",
            "useGlobalCodexSessions": True,
            "recentChat": [{"role": "user", "text": "local conversation"}],
            "lastRunAt": bridge_module.now_ms(),
        },
    )

    assert target["sessionId"] not in {
        candidate["sessionId"] for candidate in bridge_module.build_resume_candidates(bot, "single:test-user")
    }

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.resume_session_command(
            bot,
            "single:test-user",
            "req-resume",
            target["sessionId"],
        )

    persisted = bridge_module.read_session_record_by_id(current.session_id)
    assert exc_info.value.status_code == 409
    assert "/local-resume" in exc_info.value.message
    assert current.thread_id == "bridge-thread"
    assert current.use_global_codex_sessions is False
    assert [item["text"] for item in current.chat] == ["bridge user turn", "bridge bot turn"]
    assert persisted.get("threadId") == "bridge-thread"
    assert persisted.get("useGlobalCodexSessions") is False
    assert [item["text"] for item in persisted.get("recentChat") or []] == ["bridge user turn", "bridge bot turn"]
    assert persisted.get("localResumeBackup") is None


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
    assert "Codex /status" in replies[0][1]


@pytest.mark.asyncio
async def test_bridge_status_counts_job_missing_bot_id_when_definition_matches(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "chatKey": "single:test-user",
            "botId": bot.config["id"],
            "botName": bot.config["name"],
            "message": "scheduled text",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(definition)
    bridge_module.write_json_atomic(
        bridge_module.SCHEDULE_PENDING_ROOT / "job-missing-botid-status.json",
        {
            "requestId": "job-missing-botid-status",
            "scheduleId": definition["scheduleId"],
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
async def test_heartbeat_loop_raises_when_ping_ack_times_out(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.config["enabled"] = True
    bot.ws = SimpleNamespace(closed=False)

    sleeps = 0

    async def fake_sleep(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps > 1:
            raise AssertionError("heartbeat loop did not stop after ack timeout")

    async def fake_send_ws_payload_with_ack(_bot, payload, timeout_sec, **kwargs):
        raise bridge_module.BridgeError(504, f"websocket ack timeout ({payload['cmd']}:{timeout_sec})")

    monkeypatch.setattr(bridge_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    with pytest.raises(bridge_module.BridgeError, match="websocket ack timeout"):
        await bridge_module.heartbeat_loop(bot)

    assert any("heartbeat failed" in line for line in bot.logs)


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


@pytest.mark.asyncio
async def test_process_scheduled_messages_discards_job_with_invalid_run_at(bridge_module):
    pending = bridge_module.SCHEDULE_PENDING_ROOT / "job-invalid-runat.json"
    bridge_module.write_json_atomic(
        pending,
        {
            "requestId": "job-invalid-runat",
            "botId": "missing-bot",
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": "bad",
            "createdAt": bridge_module.now_ms(),
        },
    )

    await bridge_module.process_scheduled_messages_once()

    failed = bridge_module.SCHEDULE_FAILED_ROOT / "job-invalid-runat.json"
    assert failed.exists()
    assert not pending.exists()


@pytest.mark.asyncio
async def test_process_scheduled_messages_discards_processing_job_with_invalid_enqueued_at(bridge_module):
    bot = make_bot(bridge_module)
    make_session(bridge_module, bot)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    processing = bridge_module.SCHEDULE_PROCESSING_ROOT / "job-invalid-enqueued.json"
    bridge_module.write_json_atomic(
        processing,
        {
            "requestId": "job-invalid-enqueued",
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "scheduled text",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
            "enqueuedAt": "bad",
            "enqueuedByInstance": "other-instance",
        },
    )

    async def fake_enqueue_message(*args, **kwargs):
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.process_scheduled_messages_once()

    assert processing.exists()
    updated = bridge_module.read_json_file(processing, None)
    assert updated["enqueuedByInstance"] == bridge_module.INSTANCE_ID
    assert isinstance(updated["enqueuedAt"], int)


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


def test_chat_key_for_bot_rejects_missing_sender_userid(bridge_module):
    bot = make_bot(bridge_module)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.chat_key_for_bot(
            bot,
            {
                "body": {
                    "from": {},
                }
            },
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "message sender userid required"


def test_chat_key_for_bot_rejects_group_message_without_chatid(bridge_module):
    bot = make_bot(bridge_module)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.chat_key_for_bot(
            bot,
            {
                "body": {
                    "chattype": "group",
                    "from": {"userid": "user-a"},
                }
            },
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "group message chatid required"


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
async def test_handle_group_bridge_interrupt_command_falls_back_to_leading_mention_strip(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, name="Leo")
    bot.config["groupSessionMode"] = "per-user"
    replies = []
    interrupted = []

    async def fake_respond_info(_bot, req_id, message, final=True):
        replies.append((req_id, message, final))

    async def fake_interrupt_session_command(_bot, key, req_id):
        interrupted.append((key, req_id))

    bridge_module.respond_info = fake_respond_info
    monkeypatch.setattr(bridge_module, "interrupt_session_command", fake_interrupt_session_command)

    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-group-interrupt"},
        "body": {
            "msgtype": "text",
            "chattype": "group",
            "chatid": "group-1",
            "text": {"content": "@Leo2 /bridge-interrupt"},
            "from": {"userid": "user-a"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload)

    assert interrupted == [("group-user:group-1:user-a", "req-group-interrupt")]
    assert replies == []


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
async def test_handle_wecom_message_dedupe_fallback_keeps_different_text_messages(bridge_module):
    bot = make_bot(bridge_module)
    captured = []

    async def fake_enqueue_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))
        return True

    bridge_module.enqueue_message = fake_enqueue_message

    payload_a = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "same-req"},
        "body": {
            "msgtype": "text",
            "chattype": "single",
            "chatid": "user-a",
            "text": {"content": "first"},
            "from": {"userid": "user-a"},
        },
    }
    payload_b = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "same-req"},
        "body": {
            "msgtype": "text",
            "chattype": "single",
            "chatid": "user-a",
            "text": {"content": "second"},
            "from": {"userid": "user-a"},
        },
    }

    await bridge_module.handle_wecom_message(bot, payload_a)
    await bridge_module.handle_wecom_message(bot, payload_b)

    assert captured == [
        ("single:user-a", "first", "same-req"),
        ("single:user-a", "second", "same-req"),
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


def test_acquire_schedule_definition_lock_replaces_symlink_lock_file_without_touching_target(bridge_module):
    schedule_id = "sched-lock"
    lock_file = bridge_module.get_schedule_definition_lock_file(schedule_id)
    external_lock = bridge_module.BASE_DIR / "external-schedule.lock"
    external_lock.write_text("external", encoding="utf-8")
    bridge_module.ensure_dir_for(lock_file)
    lock_file.symlink_to(external_lock)

    assert bridge_module.acquire_schedule_definition_lock(schedule_id) is True
    try:
        assert lock_file.exists()
        assert not lock_file.is_symlink()
    finally:
        bridge_module.release_schedule_definition_lock(schedule_id)

    assert external_lock.read_text(encoding="utf-8") == "external"


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
    workspace_root = bridge_module.get_workspace_namespace_dir(bot.config["id"])
    workspace_root.mkdir(parents=True, exist_ok=True)
    (workspace_root / "keep.txt").write_text("workspace", encoding="utf-8")
    alias_dir = bridge_module.get_user_alias_dir(bot.config["id"])
    alias_dir.mkdir(parents=True, exist_ok=True)
    (alias_dir / "alias.json").write_text("{}", encoding="utf-8")

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
    assert not workspace_root.exists()
    assert not alias_dir.exists()
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored == [{"id": "keep-bot", "name": "keep", "workspaceNamespace": "keep-bot", "workspaceMode": "team"}]


def test_remove_bot_does_not_delete_shared_workspace_namespace_used_by_other_bots(bridge_module):
    shared_namespace = "personal-main"
    secret_file = write_secret_file(bridge_module.BASE_DIR / "shared-workspace.secret", "secret\n")
    first = make_bot(bridge_module, config_id="bot-a", remote_bot_id="bot-a-id")
    second = make_bot(bridge_module, config_id="bot-b", remote_bot_id="bot-b-id")
    first.config["workspaceNamespace"] = shared_namespace
    second.config["workspaceNamespace"] = shared_namespace
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-a",
                "name": first.config["name"],
                "botId": first.config["botId"],
                "secretFile": str(secret_file),
                "workDir": first.config["workDir"],
                "workspaceNamespace": shared_namespace,
                "groupSessionMode": "per-user",
                "enabled": True,
            },
            {
                "id": "bot-b",
                "name": second.config["name"],
                "botId": second.config["botId"],
                "secretFile": str(secret_file),
                "workDir": second.config["workDir"],
                "workspaceNamespace": shared_namespace,
                "groupSessionMode": "per-user",
                "enabled": True,
            },
        ],
    )
    shared_workfile = bridge_module.get_workfile_dir(shared_namespace, "test-user")
    shared_workfile.mkdir(parents=True, exist_ok=True)
    (shared_workfile / "keep.txt").write_text("keep", encoding="utf-8")
    shared_alias_dir = bridge_module.get_user_alias_dir(shared_namespace)
    shared_alias_dir.mkdir(parents=True, exist_ok=True)
    (shared_alias_dir / "alias.json").write_text("{}", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot("bot-a"))

    assert shared_workfile.exists()
    assert (shared_workfile / "keep.txt").read_text(encoding="utf-8") == "keep"
    assert shared_alias_dir.exists()
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored == [
        {
            "id": "bot-b",
            "name": second.config["name"],
            "botId": second.config["botId"],
            "secretFile": str(secret_file),
            "workDir": second.config["workDir"],
            "workspaceNamespace": shared_namespace,
            "groupSessionMode": "per-user",
            "workspaceMode": "team",
            "enabled": True,
        }
    ]


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

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
        calls.append((bot_id, persist_disable))
        target = bridge_module.BOTS[bot_id]
        target.status = "stopped"

    bridge_module.stop_bot = fake_stop_bot

    asyncio.run(bridge_module.remove_deleted_bots_from_memory_once())

    assert calls == [(bot.config["id"], False)]
    assert bot.config["id"] not in bridge_module.BOTS


def test_remove_deleted_bots_from_memory_once_preserves_newer_recreated_persisted_state(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.started_at_ms = bridge_module.now_ms() - 1000
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])
    deleted_at = bridge_module.bot_tombstone_deleted_at(bot.config["botId"])
    secret_file = write_secret_file(bridge_module.BASE_DIR / "recreated.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": bot.config["name"],
                "botId": bot.config["botId"],
                "secretFile": str(secret_file),
                "workDir": bot.config["workDir"],
                "groupSessionMode": "per-user",
                "enabled": True,
                "createdAt": deleted_at + 1,
                "updatedAt": deleted_at + 1,
            }
        ],
    )
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=deleted_at + 1,
    )
    bridge_module.write_schedule_definition(definition)

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
        target = bridge_module.BOTS[bot_id]
        target.status = "stopped"

    bridge_module.stop_bot = fake_stop_bot

    asyncio.run(bridge_module.remove_deleted_bots_from_memory_once())

    assert bot.config["id"] not in bridge_module.BOTS
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored == [
        {
            "id": bot.config["id"],
            "name": bot.config["name"],
            "botId": bot.config["botId"],
            "secretFile": str(secret_file),
            "workDir": bot.config["workDir"],
            "groupSessionMode": "per-user",
            "enabled": True,
            "createdAt": deleted_at + 1,
            "updatedAt": deleted_at + 1,
        }
    ]
    assert bridge_module.read_session_record_by_id(sess.session_id) is not None
    assert bridge_module.read_schedule_definition(definition["scheduleId"]) is not None


def test_remove_deleted_bots_from_memory_once_cleans_old_config_artifacts_when_recreated_with_new_id(
    bridge_module,
):
    old_bot = make_bot(bridge_module, config_id="bot-old", remote_bot_id="bot-id")
    sess = make_session(bridge_module, old_bot)
    old_bot.started_at_ms = bridge_module.now_ms() - 1000
    chatfile_dir = bridge_module.get_chatfile_dir(old_bot.config["id"], "single:test-user")
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
    bridge_module.mark_bot_deleted_globally(old_bot.config["id"], old_bot.config["botId"])
    deleted_at = bridge_module.bot_tombstone_deleted_at(old_bot.config["botId"])
    secret_file = write_secret_file(bridge_module.BASE_DIR / "recreated-new-id.secret", "secret\n")
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": "bot-new",
                "name": "codex1",
                "botId": old_bot.config["botId"],
                "secretFile": str(secret_file),
                "workDir": old_bot.config["workDir"],
                "groupSessionMode": "per-user",
                "enabled": True,
                "createdAt": deleted_at + 1,
                "updatedAt": deleted_at + 1,
            }
        ],
    )

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
        target = bridge_module.BOTS[bot_id]
        target.status = "stopped"

    bridge_module.stop_bot = fake_stop_bot

    asyncio.run(bridge_module.remove_deleted_bots_from_memory_once())

    assert old_bot.config["id"] not in bridge_module.BOTS
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == [
        {
            "id": "bot-new",
            "name": "codex1",
            "botId": old_bot.config["botId"],
            "secretFile": str(secret_file),
            "workDir": old_bot.config["workDir"],
            "groupSessionMode": "per-user",
            "enabled": True,
            "createdAt": deleted_at + 1,
            "updatedAt": deleted_at + 1,
        }
    ]
    assert bridge_module.read_session_record_by_id(sess.session_id) is None
    assert bridge_module.read_schedule_definition(definition["scheduleId"]) is None
    assert not chatfile_dir.exists()
    assert not codex_home.exists()
    assert not bridge_module.get_template_card_state_file(old_bot.config["id"]).exists()


def test_remove_deleted_bots_from_memory_once_cleans_malformed_persisted_entry_matching_stale_runtime_bot(
    bridge_module,
):
    bot = make_bot(bridge_module)
    bot.started_at_ms = bridge_module.now_ms() - 1000
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "name": "broken-entry",
                "botId": bot.config["botId"],
                "secretFile": str(write_secret_file(bridge_module.BASE_DIR / "stale-recreated.secret", "secret\n")),
                "workDir": bot.config["workDir"],
                "groupSessionMode": "per-user",
                "agentBackend": "not-a-backend",
                "enabled": True,
                "createdAt": bridge_module.now_ms() - 1000,
                "updatedAt": bridge_module.now_ms() - 1000,
            }
        ],
    )

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
        target = bridge_module.BOTS[bot_id]
        target.status = "stopped"

    bridge_module.stop_bot = fake_stop_bot
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    asyncio.run(bridge_module.remove_deleted_bots_from_memory_once())

    assert bot.config["id"] not in bridge_module.BOTS
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_remove_deleted_bots_from_memory_once_does_not_preserve_newer_invalid_persisted_config(
    bridge_module,
):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.started_at_ms = bridge_module.now_ms() - 1000
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(sess.session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])
    deleted_at = bridge_module.bot_tombstone_deleted_at(bot.config["botId"])
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": bot.config["id"],
                "name": "broken-entry",
                "botId": bot.config["botId"],
                "secretFile": str(write_secret_file(bridge_module.BASE_DIR / "broken-newer.secret", "secret\n")),
                "workDir": bot.config["workDir"],
                "groupSessionMode": "per-user",
                "agentBackend": "not-a-backend",
                "enabled": True,
                "createdAt": deleted_at + 1,
                "updatedAt": deleted_at + 1,
            }
        ],
    )

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
        target = bridge_module.BOTS[bot_id]
        target.status = "stopped"

    bridge_module.stop_bot = fake_stop_bot

    asyncio.run(bridge_module.remove_deleted_bots_from_memory_once())

    assert bot.config["id"] not in bridge_module.BOTS
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []
    assert bridge_module.read_session_record_by_id(sess.session_id) is None
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_remove_deleted_bots_from_memory_once_cleans_artifacts_for_tombstoned_bot(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.started_at_ms = bridge_module.now_ms() - 1000
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
    workspace_root = bridge_module.get_workspace_namespace_dir(bot.config["id"])
    workspace_root.mkdir(parents=True, exist_ok=True)
    (workspace_root / "keep.txt").write_text("workspace", encoding="utf-8")
    alias_dir = bridge_module.get_user_alias_dir(bot.config["id"])
    alias_dir.mkdir(parents=True, exist_ok=True)
    (alias_dir / "alias.json").write_text("{}", encoding="utf-8")
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
            }
        ],
    )

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
        target = bridge_module.BOTS[bot_id]
        target.status = "stopped"

    bridge_module.stop_bot = fake_stop_bot
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    asyncio.run(bridge_module.remove_deleted_bots_from_memory_once())

    assert bot.config["id"] not in bridge_module.BOTS
    assert bridge_module.read_session_record_by_id(sess.session_id) is None
    assert not pending.exists()
    assert bridge_module.read_schedule_definition(definition["scheduleId"]) is None
    assert not chatfile_dir.exists()
    assert not codex_home.exists()
    assert not workspace_root.exists()
    assert not alias_dir.exists()
    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == []


def test_remove_bot_rejects_unknown_bot_id(bridge_module):
    with pytest.raises(bridge_module.BridgeError) as excinfo:
        asyncio.run(bridge_module.remove_bot("missing-bot"))

    assert excinfo.value.status_code == 404


def test_remove_bot_cleans_malformed_session_record_artifacts(bridge_module):
    bot = make_bot(bridge_module)
    session_id = "sess-bad"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "sessionId": session_id,
                "botId": bot.config["id"],
                "chatKey": "single:test-user",
                "lockFile": "/tmp/not-managed.lock.json",
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not session_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_remove_bot_cleans_artifacts_for_malformed_session_record_missing_session_id(bridge_module):
    bot = make_bot(bridge_module)
    session_id = "sess-missing-id"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "botId": bot.config["id"],
                "chatKey": "single:test-user",
                "lockFile": "/tmp/not-managed.lock.json",
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not session_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_remove_bot_cleans_artifacts_for_session_id_referenced_only_by_key_file(bridge_module):
    bot = make_bot(bridge_module)
    session_id = "sess-key-only"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "chatKey": "single:test-user",
                "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], session_id)),
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    key_file = bridge_module.get_registry_key_file(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir_for(key_file)
    key_file.write_text(
        json.dumps({"sessionId": session_id, "botId": bot.config["id"], "chatKey": "single:test-user"}, ensure_ascii=False),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not session_file.exists()
    assert not key_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_remove_bot_cleans_artifacts_for_session_record_missing_bot_id_but_managed_lock(bridge_module):
    bot = make_bot(bridge_module)
    session_id = "sess-missing-bot-id"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "sessionId": session_id,
                "chatKey": "single:test-user",
                "lockFile": str(bridge_module.get_managed_session_lock_file(bot.config["id"], session_id)),
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not session_file.exists()
    assert not chatfile_dir.exists()
    assert not codex_home.exists()


def test_remove_bot_does_not_delete_foreign_artifacts_when_key_file_points_to_foreign_malformed_session(
    bridge_module,
):
    first = make_bot(bridge_module, config_id="bot-a", remote_bot_id="bot-a-id")
    second = make_bot(bridge_module, config_id="bot-b", remote_bot_id="bot-b-id")
    session_id = "sess-foreign-malformed"
    session_file = bridge_module.get_registry_session_file(session_id)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_text(
        json.dumps(
            {
                "chatKey": "single:test-user",
                "lockFile": str(bridge_module.get_managed_session_lock_file(second.config["id"], session_id)),
                "createdAt": bridge_module.now_ms(),
                "updatedAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    bad_key_file = bridge_module.get_registry_key_file(first.config["id"], "single:test-user")
    bridge_module.ensure_dir_for(bad_key_file)
    bad_key_file.write_text(
        json.dumps(
            {
                "sessionId": session_id,
                "botId": first.config["id"],
                "chatKey": "single:test-user",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    chatfile_dir = bridge_module.get_chatfile_dir(session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot(first.config["id"]))

    assert session_file.exists()
    assert chatfile_dir.exists()
    assert codex_home.exists()


def test_remove_bot_does_not_delete_foreign_live_artifacts_when_key_file_points_to_missing_foreign_session_record(
    bridge_module,
):
    first = make_bot(bridge_module, config_id="bot-a", remote_bot_id="bot-a-id")
    second = make_bot(bridge_module, config_id="bot-b", remote_bot_id="bot-b-id")
    second_sess = make_session(bridge_module, second, "single:test-user")
    session_file = bridge_module.get_registry_session_file(second_sess.session_id)
    session_file.unlink()
    chatfile_dir = bridge_module.get_chatfile_dir(second_sess.session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(second_sess.session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")
    bad_key_file = bridge_module.get_registry_key_file(first.config["id"], "single:test-user")
    bridge_module.ensure_dir_for(bad_key_file)
    bad_key_file.write_text(
        json.dumps(
            {
                "sessionId": second_sess.session_id,
                "botId": first.config["id"],
                "chatKey": "single:test-user",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    asyncio.run(bridge_module.remove_bot(first.config["id"]))

    assert chatfile_dir.exists()
    assert codex_home.exists()


def test_remove_bot_does_not_delete_other_bot_artifacts_when_key_file_points_to_foreign_session(bridge_module):
    first = make_bot(bridge_module, config_id="bot-a", remote_bot_id="bot-a-id")
    second = make_bot(bridge_module, config_id="bot-b", remote_bot_id="bot-b-id")
    second_sess = make_session(bridge_module, second, "single:test-user")
    chatfile_dir = bridge_module.get_chatfile_dir(second_sess.session_id)
    bridge_module.ensure_dir(chatfile_dir)
    (chatfile_dir / "artifact.txt").write_text("hello", encoding="utf-8")
    codex_home = bridge_module.build_codex_home_for_subprocess(second_sess.session_id)
    (codex_home / "state.txt").write_text("keep", encoding="utf-8")
    bad_key_file = bridge_module.get_registry_key_file(first.config["id"], "single:test-user")
    bridge_module.ensure_dir_for(bad_key_file)
    bad_key_file.write_text(
        json.dumps(
            {
                "sessionId": second_sess.session_id,
                "botId": first.config["id"],
                "chatKey": "single:test-user",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    asyncio.run(bridge_module.remove_bot(first.config["id"]))

    assert bridge_module.read_session_record_by_id(second_sess.session_id) is not None
    assert chatfile_dir.exists()
    assert codex_home.exists()


def test_remove_bot_cleans_malformed_schedule_definition_for_target_bot(bridge_module):
    bot = make_bot(bridge_module)
    malformed_schedule_id = "sched-bad"
    definition_file = bridge_module.get_schedule_definition_file(malformed_schedule_id)
    definition_file.parent.mkdir(parents=True, exist_ok=True)
    definition_file.write_text(
        json.dumps(
            {
                "scheduleId": malformed_schedule_id,
                "botId": bot.config["id"],
                "chatKey": "single:test-user",
                "message": "hello",
                "mode": "broken",
                "createdAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    lock_file = bridge_module.get_schedule_definition_lock_file(malformed_schedule_id)
    bridge_module.ensure_dir_for(lock_file)
    lock_file.write_text("{}", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not definition_file.exists()
    assert not lock_file.exists()


def test_remove_bot_cleans_lock_for_malformed_schedule_definition_missing_schedule_id(bridge_module):
    bot = make_bot(bridge_module)
    schedule_id = "folder/evil"
    definition_file = bridge_module.get_schedule_definition_file(schedule_id)
    definition_file.parent.mkdir(parents=True, exist_ok=True)
    definition_file.write_text(
        json.dumps(
            {
                "botId": bot.config["id"],
                "chatKey": "single:test-user",
                "message": "hello",
                "mode": "broken",
                "createdAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    lock_file = bridge_module.get_schedule_definition_lock_file(schedule_id)
    bridge_module.ensure_dir_for(lock_file)
    lock_file.write_text("{}", encoding="utf-8")

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not definition_file.exists()
    assert not lock_file.exists()


def test_remove_bot_cleans_malformed_persisted_bot_entry_matching_wecom_bot_id(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "name": "broken-entry",
                "botId": bot.config["botId"],
                "secretFile": str(write_secret_file(bridge_module.BASE_DIR / "persisted.secret", "secret\n")),
                "workDir": bot.config["workDir"],
                "groupSessionMode": "per-user",
                "enabled": True,
            },
            {"id": bot.config["id"], "name": bot.config["name"], "botId": bot.config["botId"], "enabled": True},
            {"id": "keep-bot", "name": "keep", "botId": "keep-id", "enabled": True},
        ],
    )

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert bridge_module.read_json_file(bridge_module.DATA_FILE, None) == [
        {
            "id": "keep-bot",
            "name": "keep",
            "botId": "keep-id",
            "enabled": True,
            "workspaceNamespace": "keep-bot",
            "workspaceMode": "team",
        }
    ]


def test_remove_bot_cleans_malformed_schedule_definition_missing_bot_id_when_job_references_it(bridge_module):
    bot = make_bot(bridge_module)
    schedule_id = "sched-missing-bot-id"
    definition_file = bridge_module.get_schedule_definition_file(schedule_id)
    definition_file.parent.mkdir(parents=True, exist_ok=True)
    definition_file.write_text(
        json.dumps(
            {
                "chatKey": "single:test-user",
                "message": "hello",
                "mode": "broken",
                "createdAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    lock_file = bridge_module.get_schedule_definition_lock_file(schedule_id)
    bridge_module.ensure_dir_for(lock_file)
    lock_file.write_text("{}", encoding="utf-8")
    pending_job = bridge_module.SCHEDULE_PENDING_ROOT / "job-missing-bot.json"
    bridge_module.write_json_atomic(
        pending_job,
        {
            "requestId": "job-missing-bot",
            "scheduleId": schedule_id,
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "hello",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not definition_file.exists()
    assert not lock_file.exists()
    assert not pending_job.exists()


def test_remove_bot_cleans_non_object_schedule_definition_when_current_bot_job_references_it(bridge_module):
    bot = make_bot(bridge_module)
    schedule_id = "sched-non-object"
    definition_file = bridge_module.get_schedule_definition_file(schedule_id)
    definition_file.parent.mkdir(parents=True, exist_ok=True)
    definition_file.write_text("[]", encoding="utf-8")
    pending_job = bridge_module.SCHEDULE_PENDING_ROOT / "job-non-object.json"
    bridge_module.write_json_atomic(
        pending_job,
        {
            "requestId": "job-non-object",
            "scheduleId": schedule_id,
            "botId": bot.config["id"],
            "chatKey": "single:test-user",
            "message": "hello",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not definition_file.exists()
    assert not pending_job.exists()


def test_remove_bot_does_not_delete_foreign_schedule_definition_referenced_by_malformed_job(bridge_module):
    first = make_bot(bridge_module, config_id="bot-a", remote_bot_id="bot-a-id")
    second = make_bot(bridge_module, config_id="bot-b", remote_bot_id="bot-b-id")
    foreign_definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": bridge_module.create_session_record(second, "single:test-user")["sessionId"],
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(foreign_definition)
    pending_job = bridge_module.SCHEDULE_PENDING_ROOT / "job-foreign.json"
    bridge_module.write_json_atomic(
        pending_job,
        {
            "requestId": "job-foreign",
            "scheduleId": foreign_definition["scheduleId"],
            "botId": first.config["id"],
            "chatKey": "single:test-user",
            "message": "hello",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    asyncio.run(bridge_module.remove_bot(first.config["id"]))

    assert bridge_module.read_schedule_definition(foreign_definition["scheduleId"]) is not None
    assert bridge_module.get_schedule_definition_lock_file(foreign_definition["scheduleId"]).exists() is False


def test_remove_bot_does_not_delete_foreign_job_even_when_schedule_id_matches_removed_set(bridge_module):
    first = make_bot(bridge_module, config_id="bot-a", remote_bot_id="bot-a-id")
    second = make_bot(bridge_module, config_id="bot-b", remote_bot_id="bot-b-id")
    foreign_definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": bridge_module.create_session_record(second, "single:test-user")["sessionId"],
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        },
        created_at_ms=1_800_000_500_000,
    )
    bridge_module.write_schedule_definition(foreign_definition)
    foreign_job = bridge_module.SCHEDULE_PENDING_ROOT / "job-foreign-owner.json"
    bridge_module.write_json_atomic(
        foreign_job,
        {
            "requestId": "job-foreign-owner",
            "scheduleId": foreign_definition["scheduleId"],
            "botId": second.config["id"],
            "chatKey": "single:test-user",
            "message": "hello",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )
    malformed_definition = bridge_module.get_schedule_definition_file("sched-bad-a")
    malformed_definition.parent.mkdir(parents=True, exist_ok=True)
    malformed_definition.write_text(
        json.dumps(
            {
                "scheduleId": foreign_definition["scheduleId"],
                "botId": first.config["id"],
                "chatKey": "single:test-user",
                "message": "hello",
                "mode": "broken",
                "createdAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    asyncio.run(bridge_module.remove_bot(first.config["id"]))

    assert foreign_job.exists()


def test_remove_bot_cleans_jobs_for_malformed_definition_even_when_job_bot_id_missing(bridge_module):
    bot = make_bot(bridge_module)
    malformed_schedule_id = "sched-bad-job"
    definition_file = bridge_module.get_schedule_definition_file(malformed_schedule_id)
    definition_file.parent.mkdir(parents=True, exist_ok=True)
    definition_file.write_text(
        json.dumps(
            {
                "scheduleId": malformed_schedule_id,
                "botId": bot.config["id"],
                "chatKey": "single:test-user",
                "message": "hello",
                "mode": "broken",
                "createdAt": bridge_module.now_ms(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    pending_job = bridge_module.SCHEDULE_PENDING_ROOT / "bad-job.json"
    bridge_module.write_json_atomic(
        pending_job,
        {
            "requestId": "bad-job",
            "scheduleId": malformed_schedule_id,
            "chatKey": "single:test-user",
            "message": "hello",
            "runAt": bridge_module.now_ms() - 1000,
            "createdAt": bridge_module.now_ms(),
        },
    )

    asyncio.run(bridge_module.remove_bot(bot.config["id"]))

    assert not definition_file.exists()
    assert not pending_job.exists()


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

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, reason: str = "internal"):
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


def test_reconcile_bots_once_prunes_and_stops_stale_bootstrap_managed_bot(bridge_module, monkeypatch):
    stale_secret_file = write_secret_file(bridge_module.BASE_DIR / "stale-reconcile.secret", "stale-secret\n")
    live_secret_file = write_secret_file(bridge_module.BASE_DIR / "live-reconcile.secret", "live-secret\n")
    stale_bot_id = "stale-bot"
    stale_config_id = bridge_module.default_bot_config_id(stale_bot_id)
    stale_bot = make_bot(bridge_module, config_id=stale_config_id, name="env-bot", remote_bot_id=stale_bot_id)
    bridge_module.write_json_atomic(
        bridge_module.DATA_FILE,
        [
            {
                "id": stale_config_id,
                "name": "env-bot",
                "botId": stale_bot_id,
                "secretFile": str(stale_secret_file),
                "workDir": str(bridge_module.BASE_DIR),
                "welcome": "",
                "groupSessionMode": "per-user",
                "enabled": True,
            }
        ],
    )
    monkeypatch.setenv(
        "WECOM_BOOTSTRAP_BOTS_JSON",
        json.dumps(
            [
                {
                    "name": "live-env-bot",
                    "botId": "live-bot",
                    "secretFile": str(live_secret_file),
                    "workDir": str(bridge_module.BASE_DIR),
                }
            ]
        ),
    )
    stopped = []
    started = []

    async def fake_stop_bot(bot_id: str, persist_disable: bool = True, persist_state: bool = True, reason: str = "internal"):
        stopped.append((bot_id, persist_disable, persist_state, reason))
        bridge_module.BOTS.pop(bot_id, None)

    async def fake_start_bot(config):
        normalized = bridge_module.normalize_bot_config(config, allow_inline_secret=True)
        started.append(normalized)
        bridge_module.upsert_persisted_bot_config(normalized)
        return bridge_module.BotState(config=normalized)

    bridge_module.stop_bot = fake_stop_bot
    bridge_module.start_bot = fake_start_bot

    asyncio.run(bridge_module.reconcile_bots_once())

    assert stopped == [(stale_config_id, False, False, "config_reconcile_missing")]
    assert len(started) == 1
    assert started[0]["botId"] == "live-bot"
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert len(stored) == 1
    assert stored[0]["botId"] == "live-bot"


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


def test_resolve_local_send_message_queue_root_uses_runtime_root_for_relative_env(bridge_module, monkeypatch):
    monkeypatch.setenv("BRIDGE_RUNTIME_ROOT", "runtime-root")
    monkeypatch.setenv("LOCAL_SEND_MESSAGE_QUEUE_ROOT", "relative-queue")

    resolved = bridge_module.resolve_local_send_message_queue_root(bridge_module.BASE_DIR)

    assert resolved == (bridge_module.BASE_DIR / "runtime-root" / "relative-queue").resolve()


def test_list_local_file_send_queue_path_groups_ignores_symlinked_target_dirs(bridge_module):
    bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT = bridge_module.BASE_DIR / "local-file-send-queue"
    bridge_module.LOCAL_FILE_SEND_PENDING_ROOT = bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT / "pending"
    bridge_module.LOCAL_FILE_SEND_PROCESSING_ROOT = bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT / "processing"
    bridge_module.LOCAL_FILE_SEND_RESULT_ROOT = bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT / "results"
    bridge_module.LOCAL_FILE_SEND_DONE_ROOT = bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT / "done"
    bridge_module.LOCAL_FILE_SEND_FAILED_ROOT = bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT / "failed"
    targets_root = bridge_module.LOCAL_FILE_SEND_QUEUE_ROOT / "targets"
    external_root = bridge_module.BASE_DIR / "external-file-send-root"
    bridge_module.ensure_dir(external_root / "pending")
    bridge_module.ensure_dir(targets_root)
    (targets_root / "bot-2").symlink_to(external_root, target_is_directory=True)

    groups = bridge_module.list_local_file_send_queue_path_groups()

    roots = {str(item["root"]) for item in groups}
    assert str(external_root.resolve()) not in roots
    assert str((targets_root / "bot-2").resolve()) not in roots


def test_list_local_send_message_queue_path_groups_ignores_symlinked_target_dirs(bridge_module):
    bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT = bridge_module.BASE_DIR / "local-send-message-queue"
    bridge_module.LOCAL_SEND_MESSAGE_PENDING_ROOT = bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "pending"
    bridge_module.LOCAL_SEND_MESSAGE_PROCESSING_ROOT = bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "processing"
    bridge_module.LOCAL_SEND_MESSAGE_RESULT_ROOT = bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "results"
    bridge_module.LOCAL_SEND_MESSAGE_DONE_ROOT = bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "done"
    bridge_module.LOCAL_SEND_MESSAGE_FAILED_ROOT = bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "failed"
    targets_root = bridge_module.LOCAL_SEND_MESSAGE_QUEUE_ROOT / "targets"
    external_root = bridge_module.BASE_DIR / "external-send-message-root"
    bridge_module.ensure_dir(external_root / "pending")
    bridge_module.ensure_dir(targets_root)
    (targets_root / "bot-2").symlink_to(external_root, target_is_directory=True)

    groups = bridge_module.list_local_send_message_queue_path_groups()

    roots = {str(item["root"]) for item in groups}
    assert str(external_root.resolve()) not in roots
    assert str((targets_root / "bot-2").resolve()) not in roots


def test_bridge_module_rejects_file_local_file_send_queue_root(monkeypatch, tmp_path):
    bad_root = tmp_path / "local-file-send-root.txt"
    bad_root.write_text("not a dir", encoding="utf-8")
    monkeypatch.setenv("LOCAL_FILE_SEND_QUEUE_ROOT", str(bad_root))

    with pytest.raises(Exception) as excinfo:
        load_bridge_module()

    assert str(excinfo.value) == f"LOCAL_FILE_SEND_QUEUE_ROOT is not a directory: {bad_root.resolve()}"


def test_bridge_module_rejects_file_local_send_message_queue_root(monkeypatch, tmp_path):
    bad_root = tmp_path / "local-send-message-root.txt"
    bad_root.write_text("not a dir", encoding="utf-8")
    monkeypatch.setenv("LOCAL_SEND_MESSAGE_QUEUE_ROOT", str(bad_root))

    with pytest.raises(Exception) as excinfo:
        load_bridge_module()

    assert str(excinfo.value) == f"LOCAL_SEND_MESSAGE_QUEUE_ROOT is not a directory: {bad_root.resolve()}"


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


def test_local_send_message_loop_uses_dedicated_poll_interval(bridge_module, monkeypatch):
    sleep_calls = []

    async def fake_process_local_send_message_queue_once():
        bridge_module.SHUTDOWN_EVENT.set()

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)
        return None

    monkeypatch.setattr(bridge_module, "LOCAL_SEND_MESSAGE_POLL_MS", 5432)
    monkeypatch.setattr(bridge_module, "LOCAL_FILE_SEND_POLL_MS", 1111)
    monkeypatch.setattr(bridge_module, "process_local_send_message_queue_once", fake_process_local_send_message_queue_once)
    monkeypatch.setattr(bridge_module.asyncio, "sleep", fake_sleep)

    asyncio.run(bridge_module.local_send_message_loop())

    assert sleep_calls == [5.432]


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
    assert body["reason"] == "api_stop"
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

    async def fake_stop_bot(_bot_id: str, persist_disable: bool = True, reason: str = "internal"):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "stop_bot", fake_stop_bot)

    request = SimpleNamespace(match_info={"bot_id": bot.config["id"]})
    response = asyncio.run(bridge_module.api_stop_bot(request))
    body = json.loads(response.text)

    assert response.status == 200
    assert body["ok"] is True
    assert body["reason"] == "api_stop"
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
    assert body["reason"] == "api"
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
    assert body[0]["healthy"] is True


def test_bot_to_payload_includes_health_and_stop_reason(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "disconnected"
    bot.stop_reason = "api_stop"
    bot.ws = SimpleNamespace(closed=True)
    loop = asyncio.new_event_loop()
    try:
        bot.runner_task = loop.create_future()
        bot.upload_worker_task = loop.create_future()

        payload = bridge_module.bot_to_payload(bot)

        assert payload["healthy"] is False
        assert payload["stopReason"] == "api_stop"
        assert payload["wsConnected"] is False
        assert payload["inboundStale"] is False
        assert payload["heartbeatAckStale"] is False
    finally:
        loop.close()


def test_api_get_bots_ignores_stale_runtime_bot(monkeypatch, bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1", remote_bot_id="bot-id")
    bridge_module.mark_bot_deleted_globally(bot.config["id"], bot.config["botId"])

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(bridge_module.api_get_bots(SimpleNamespace()))
    body = json.loads(response.text)

    assert body == []


def test_bot_to_payload_marks_queued_session_busy_and_alive(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = False
    sess.queue.append({"text": "queued", "reqId": "req-1"})

    payload = bridge_module.bot_to_payload(bot)

    assert payload["sessions"][0]["status"] == "queued"
    assert payload["sessions"][0]["alive"] is True
    assert payload["sessions"][0]["busy"] is True
    assert payload["sessions"][0]["queueLen"] == 1


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


def test_api_get_chat_returns_not_found_for_missing_bot(monkeypatch, bridge_module):
    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(
        bridge_module.api_get_chat(SimpleNamespace(match_info={"bot_id": "missing", "chat_key": "single%3Atest-user"}))
    )
    body = json.loads(response.text)

    assert response.status == 404
    assert body["error"] == "bot not found"


def test_build_app_exposes_distinct_new_and_reset_session_apis(bridge_module):
    app = bridge_module.build_app()

    paths = {route.resource.canonical for route in app.router.routes()}

    assert "/api/bots/{bot_id}/sessions/{chat_key}/new" in paths
    assert "/api/bots/{bot_id}/sessions/{chat_key}/reset" in paths


def test_api_get_chat_returns_not_found_for_missing_session(monkeypatch, bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(
        bridge_module.api_get_chat(SimpleNamespace(match_info={"bot_id": bot.config["id"], "chat_key": "single%3Atest-user"}))
    )
    body = json.loads(response.text)

    assert response.status == 404
    assert body["error"] == "session not found"


def test_api_get_chat_returns_session_chat(monkeypatch, bridge_module):
    bot = make_bot(bridge_module, config_id="bot-1")
    sess = make_session(bridge_module, bot)
    sess.chat = [{"role": "user", "content": "hello"}]

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    response = asyncio.run(
        bridge_module.api_get_chat(SimpleNamespace(match_info={"bot_id": bot.config["id"], "chat_key": "single%3Atest-user"}))
    )
    body = json.loads(response.text)

    assert response.status == 200
    assert body == [{"role": "user", "content": "hello"}]


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
    assert body["reason"] == "api"
    stored = bridge_module.read_json_file(bridge_module.DATA_FILE, None)
    assert stored[0]["enabled"] is True
    assert stored[0]["welcome"] == "persisted-new"
    assert stored[0]["restartToken"]


def test_api_healthz_reports_healthy_running_bot(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    loop = asyncio.new_event_loop()
    try:
        bot.runner_task = loop.create_future()
        bot.upload_worker_task = loop.create_future()
        now_value = bridge_module.now_ms()
        bot.last_subscribed_at_ms = now_value
        bot.last_inbound_at_ms = now_value
        bot.last_heartbeat_ack_at_ms = now_value

        response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
        body = json.loads(response.text)

        assert response.status == 200
        assert body["ok"] is True
        assert body["botCount"] == 1
        assert body["bots"][0]["botConfigId"] == bot.config["id"]
        assert body["bots"][0]["botId"] == bot.config["botId"]
        assert body["bots"][0]["healthy"] is True
        assert body["bots"][0]["status"] == "running"
        assert body["bots"][0]["wsConnected"] is True
        assert body["bots"][0]["inboundStale"] is False
        assert body["bots"][0]["heartbeatAckStale"] is False
        assert body["bots"][0]["disconnectGraceActive"] is False
    finally:
        loop.close()


def test_api_healthz_reports_unhealthy_bot_when_runner_task_done(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    loop = asyncio.new_event_loop()
    try:
        done_future = loop.create_future()
        done_future.set_result(None)
        bot.runner_task = done_future
        bot.upload_worker_task = loop.create_future()

        response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
        body = json.loads(response.text)

        assert response.status == 503
        assert body["ok"] is False
        assert body["bots"][0]["healthy"] is False
        assert body["bots"][0]["runnerTaskDone"] is True
    finally:
        loop.close()


def test_api_livez_stays_ok_when_healthz_is_unhealthy(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    loop = asyncio.new_event_loop()
    try:
        done_future = loop.create_future()
        done_future.set_result(None)
        bot.runner_task = done_future
        bot.upload_worker_task = loop.create_future()

        response = asyncio.run(bridge_module.api_livez(SimpleNamespace()))
        body = json.loads(response.text)

        assert response.status == 200
        assert body["ok"] is True
        assert body["shutdown"] is False
        assert body["botCount"] == 1
    finally:
        loop.close()


def test_api_healthz_reports_unhealthy_when_no_inbound_after_subscribe(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    loop = asyncio.new_event_loop()
    try:
        bot.runner_task = loop.create_future()
        bot.upload_worker_task = loop.create_future()
        now_value = bridge_module.now_ms()
        bot.last_subscribed_at_ms = now_value - bridge_module.WECOM_INBOUND_IDLE_TIMEOUT_MS - 1000
        bot.last_inbound_at_ms = None
        bot.last_heartbeat_ack_at_ms = now_value

        response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
        body = json.loads(response.text)

        assert response.status == 503
        assert body["ok"] is False
        assert body["bots"][0]["healthy"] is False
        assert body["bots"][0]["inboundStale"] is True
    finally:
        loop.close()


def test_api_healthz_reports_unhealthy_when_inbound_idle_is_stale(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    loop = asyncio.new_event_loop()
    try:
        bot.runner_task = loop.create_future()
        bot.upload_worker_task = loop.create_future()
        now_value = bridge_module.now_ms()
        bot.last_subscribed_at_ms = now_value
        bot.last_inbound_at_ms = now_value - bridge_module.WECOM_INBOUND_IDLE_TIMEOUT_MS - 1000
        bot.last_heartbeat_ack_at_ms = now_value

        response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
        body = json.loads(response.text)

        assert response.status == 503
        assert body["ok"] is False
        assert body["bots"][0]["healthy"] is False
        assert body["bots"][0]["inboundStale"] is True
    finally:
        loop.close()


def test_api_healthz_reports_unhealthy_when_heartbeat_ack_is_stale(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "running"
    bot.ws = SimpleNamespace(closed=False)
    loop = asyncio.new_event_loop()
    try:
        bot.runner_task = loop.create_future()
        bot.upload_worker_task = loop.create_future()
        now_value = bridge_module.now_ms()
        bot.last_subscribed_at_ms = now_value
        bot.last_inbound_at_ms = now_value
        bot.last_heartbeat_ack_at_ms = now_value - bridge_module.WECOM_HEARTBEAT_ACK_TIMEOUT_MS - 1000

        response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
        body = json.loads(response.text)

        assert response.status == 503
        assert body["ok"] is False
        assert body["bots"][0]["healthy"] is False
        assert body["bots"][0]["heartbeatAckStale"] is True
    finally:
        loop.close()


def test_api_healthz_keeps_recently_disconnected_bot_healthy_during_grace_window(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "disconnected"
    bot.ws = SimpleNamespace(closed=True)
    loop = asyncio.new_event_loop()
    try:
        bot.runner_task = loop.create_future()
        bot.upload_worker_task = loop.create_future()
        now_value = bridge_module.now_ms()
        bot.last_disconnect_at_ms = now_value

        response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
        body = json.loads(response.text)

        assert response.status == 200
        assert body["ok"] is True
        assert body["bots"][0]["healthy"] is True
        assert body["bots"][0]["disconnectGraceActive"] is True
    finally:
        loop.close()


def test_bot_to_payload_includes_disconnect_grace_flag(bridge_module):
    bot = make_bot(bridge_module)
    bot.status = "disconnected"
    bot.ws = SimpleNamespace(closed=True)
    bot.last_disconnect_at_ms = bridge_module.now_ms()
    loop = asyncio.new_event_loop()
    try:
        bot.runner_task = loop.create_future()
        bot.upload_worker_task = loop.create_future()
        payload = bridge_module.bot_to_payload(bot)
    finally:
        loop.close()

    assert payload["disconnectGraceActive"] is True


def test_api_healthz_includes_missing_enabled_bot_payloads(bridge_module):
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

    response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
    body = json.loads(response.text)

    assert response.status == 503
    assert body["ok"] is False
    assert body["missingEnabledBotIds"] == ["bot-1"]
    assert len(body["missingEnabledBots"]) == 1
    assert body["missingEnabledBots"][0]["id"] == "bot-1"
    assert body["missingEnabledBots"][0]["botId"] == "bot-id"
    assert body["missingEnabledBots"][0]["status"] == "not_loaded"


def test_submit_schedule_definition_request_exposes_bot_config_id_alias(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    result = bridge_module.submit_schedule_definition_request(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        }
    )

    assert result["botId"] == bot.config["id"]
    assert result["botConfigId"] == bot.config["id"]


def test_submit_schedule_message_request_exposes_bot_config_id_alias(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    result = bridge_module.submit_schedule_message_request(
        {
            "sessionId": sess.session_id,
            "message": "run once",
            "runAt": bridge_module.now_ms() + 60_000,
        }
    )

    assert result["botId"] == bot.config["id"]
    assert result["botConfigId"] == bot.config["id"]
    assert result["botName"] == bot.config["name"]
    assert result["sessionId"] == sess.session_id


def test_submit_schedule_message_request_accepts_zero_delay_seconds(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    current_now = 1_800_000_300_000
    bridge_module.now_ms = lambda: current_now

    result = bridge_module.submit_schedule_message_request(
        {
            "sessionId": sess.session_id,
            "delaySeconds": 0,
            "message": "run once",
        }
    )

    assert result["scheduleId"]
    assert result["requestedRunAt"] == current_now


def test_submit_schedule_message_request_rejects_zero_run_at_as_not_in_future(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.submit_schedule_message_request(
            {
                "sessionId": sess.session_id,
                "runAt": 0,
                "message": "run once",
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "runAt must be in the future"


def test_submit_schedule_message_request_treats_null_timezone_as_utc(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    result = bridge_module.submit_schedule_message_request(
        {
            "sessionId": sess.session_id,
            "message": "run once",
            "runAt": bridge_module.now_ms() + 60_000,
            "timezone": "null",
        }
    )

    stored = bridge_module.read_schedule_definition(result["scheduleId"])
    assert stored is not None
    assert stored["timezone"] == "UTC"


def test_normalize_schedule_definition_coerces_string_false_enabled(bridge_module):
    normalized = bridge_module.normalize_schedule_definition(
        {
            "scheduleId": "sched-1",
            "botId": "bot-1",
            "chatKey": "single:test-user",
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
            "enabled": "false",
            "createdAt": bridge_module.now_ms(),
            "updatedAt": bridge_module.now_ms(),
        }
    )

    assert normalized is not None
    assert normalized["enabled"] is False


def test_submit_schedule_message_request_treats_null_schedule_id_as_generated(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    result = bridge_module.submit_schedule_message_request(
        {
            "sessionId": sess.session_id,
            "message": "run once",
            "runAt": bridge_module.now_ms() + 60_000,
            "scheduleId": "null",
        }
    )

    assert result["scheduleId"] != "null"


def test_submit_schedule_definition_request_treats_null_schedule_policies_as_defaults(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    result = bridge_module.submit_schedule_definition_request(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "misfirePolicy": "null",
            "concurrencyPolicy": "null",
        }
    )

    stored = bridge_module.read_schedule_definition(result["scheduleId"])
    assert stored is not None
    assert stored["misfirePolicy"] == "fire_once_now"
    assert stored["concurrencyPolicy"] == "skip_if_running"


def test_submit_schedule_definition_request_rejects_zero_max_runs_from_maxRuns_field(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.submit_schedule_definition_request(
            {
                "sessionId": sess.session_id,
                "message": "daily summary",
                "mode": "cron",
                "cron": "* * * * *",
                "maxRuns": 0,
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "maxRuns must be > 0"


@pytest.mark.asyncio
async def test_api_get_schedule_and_list_expose_bot_config_id_alias(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        }
    )
    bridge_module.create_schedule_definition(definition)

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    list_response = await bridge_module.api_get_schedules(SimpleNamespace())
    list_body = json.loads(list_response.text)
    assert list_body[0]["botConfigId"] == bot.config["id"]

    get_response = await bridge_module.api_get_schedule(SimpleNamespace(match_info={"schedule_id": definition["scheduleId"]}))
    get_body = json.loads(get_response.text)
    assert get_body["botConfigId"] == bot.config["id"]


@pytest.mark.asyncio
async def test_api_pause_and_resume_schedule_expose_bot_config_id_alias(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    definition = bridge_module.create_schedule_definition_record(
        {
            "sessionId": sess.session_id,
            "message": "daily summary",
            "mode": "cron",
            "cron": "* * * * *",
            "timezone": "UTC",
        }
    )
    bridge_module.create_schedule_definition(definition)

    async def fake_require_api_access(_request):
        return None

    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)

    pause_response = await bridge_module.api_pause_schedule(SimpleNamespace(match_info={"schedule_id": definition["scheduleId"]}))
    pause_body = json.loads(pause_response.text)
    assert pause_body["botConfigId"] == bot.config["id"]

    resume_response = await bridge_module.api_resume_schedule(SimpleNamespace(match_info={"schedule_id": definition["scheduleId"]}))
    resume_body = json.loads(resume_response.text)
    assert resume_body["botConfigId"] == bot.config["id"]


@pytest.mark.asyncio
async def test_handle_wecom_message_updates_inbound_and_heartbeat_ack_times(bridge_module):
    bot = make_bot(bridge_module)
    first = bridge_module.now_ms()
    second = first + 1234
    values = [first, second]
    bridge_module.now_ms = lambda: values.pop(0) if values else second

    ping_req_id = "ping-1"
    ping_future = bridge_module.create_request_future(bot, ping_req_id)
    handled = await bridge_module.handle_wecom_message(
        bot,
        {"cmd": "ping", "headers": {"req_id": ping_req_id}, "errcode": 0, "errmsg": "ok"},
    )

    assert handled is None
    assert ping_future.done() is True
    assert bot.last_inbound_at_ms == first
    assert bot.last_heartbeat_ack_at_ms == first

    async def fake_enqueue_message(_bot, _key, _text, _req_id, **_kwargs):
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "msg-1"},
            "body": {
                "msgtype": "text",
                "chattype": "single",
                "chatid": "user-a",
                "text": {"content": "hello"},
                "from": {"userid": "user-a"},
            },
        },
    )

    assert bot.last_inbound_at_ms == second
    assert bot.last_heartbeat_ack_at_ms == first


@pytest.mark.asyncio
async def test_create_request_future_rejects_empty_req_id(bridge_module):
    bot = make_bot(bridge_module)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.create_request_future(bot, "")

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "req_id required"


@pytest.mark.asyncio
async def test_create_request_future_rejects_duplicate_pending_req_id(bridge_module):
    bot = make_bot(bridge_module)
    first = bridge_module.create_request_future(bot, "req-1")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.create_request_future(bot, "req-1")

    assert excinfo.value.status_code == 409
    assert excinfo.value.message == "duplicate pending request: req-1"
    assert bot.pending_requests["req-1"] is first


@pytest.mark.asyncio
async def test_resolve_request_future_strips_req_id_whitespace(bridge_module):
    bot = make_bot(bridge_module)
    future = bridge_module.create_request_future(bot, "req-1")

    handled = bridge_module.resolve_request_future(bot, " req-1 ", {"headers": {"req_id": " req-1 "}})

    assert handled is True
    assert future.done() is True
    assert "req-1" not in bot.pending_requests


@pytest.mark.asyncio
async def test_handle_wecom_disconnected_event_closes_websocket(bridge_module):
    bot = make_bot(bridge_module)

    class FakeWS:
        def __init__(self) -> None:
            self.closed = False
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            self.closed = True

    bot.ws = FakeWS()

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-1"},
            "body": {"msgtype": "event", "event": {"eventtype": "disconnected_event"}},
        },
    )

    assert bot.ws.closed is True
    assert bot.ws.close_calls == 1
    assert bot.status == "disconnected"
    assert bot.last_disconnect_at_ms is not None


@pytest.mark.asyncio
async def test_handle_wecom_template_card_event_updates_card(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sent = []
    enqueued = []
    info_messages = []
    bot.template_card_button_texts["task-1"] = {"ack_received": "确认"}
    bot.template_card_payloads["task-1"] = {
        "card_type": "button_interaction",
        "main_title": {"title": "请确认执行", "desc": "converter smoke"},
        "button_list": [{"text": "确认", "style": 0, "key": "ack_received"}],
        "task_id": "task-1",
    }

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        sent.append(payload)
        return {"errcode": 0, "body": {}}

    async def fake_enqueue_message(_bot, _key, text, _req_id, **_kwargs):
        enqueued.append(text)
        return True

    async def fake_respond_info(_bot, req_id, message, final=True):
        info_messages.append((req_id, message, final))

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "enqueue_message", fake_enqueue_message)
    monkeypatch.setattr(bridge_module, "respond_info", fake_respond_info)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-req-1"},
            "body": {
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "user-a"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "ack_received",
                        "task_id": "task-1",
                    },
                },
            },
        },
    )

    assert len(sent) == 1
    assert sent[0]["cmd"] == "aibot_respond_update_msg"
    assert sent[0]["headers"]["req_id"] == "evt-req-1"
    assert sent[0]["body"]["response_type"] == "update_template_card"
    assert sent[0]["body"]["template_card"]["card_type"] == "button_interaction"
    assert sent[0]["body"]["template_card"]["main_title"]["title"] == "请确认执行"
    assert sent[0]["body"]["template_card"]["button_list"][0]["text"] == "确认"
    assert info_messages == []
    assert enqueued == ["确认"]


@pytest.mark.asyncio
async def test_handle_wecom_template_card_event_ignores_duplicate_owner_click(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sent = []
    enqueued = []
    bot.template_card_button_texts["task-1"] = {"ack_received": "确认"}
    bot.consumed_template_card_actions.add("task-1\nack_received\nuser-a")

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        sent.append(payload)
        return {"errcode": 0, "body": {}}

    async def fake_enqueue_message(_bot, _key, text, _req_id, **_kwargs):
        enqueued.append(text)
        return True

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "enqueue_message", fake_enqueue_message)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-req-dup"},
            "body": {
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "user-a"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "ack_received",
                        "task_id": "task-1",
                    },
                },
            },
        },
    )

    assert enqueued == []
    assert len(sent) == 1
    assert sent[0]["cmd"] == "aibot_send_msg"
    assert "该操作已处理，请勿重复点击。" in sent[0]["body"]["markdown"]["content"]


@pytest.mark.asyncio
async def test_handle_wecom_template_card_event_allows_repeatable_refresh_progress(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sent = []
    enqueued = []
    bot.template_card_button_texts["task-1"] = {"refresh_progress": "刷新进度"}
    bot.template_card_payloads["task-1"] = {
        "card_type": "button_interaction",
        "main_title": {"title": "进度刷新卡片演示", "desc": "当前进度: 20%"},
        "sub_title_text": "进度条: [##........] 20%\n状态: 进行中",
        "button_list": [{"text": "刷新进度", "style": 1, "key": "refresh_progress"}],
        "task_id": "task-1",
    }
    bot.consumed_template_card_actions.add("task-1\nrefresh_progress\nuser-a")

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        sent.append(payload)
        return {"errcode": 0, "body": {}}

    async def fake_enqueue_message(_bot, _key, text, _req_id, **_kwargs):
        enqueued.append(text)
        return True

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "enqueue_message", fake_enqueue_message)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-req-refresh"},
            "body": {
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "user-a"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "refresh_progress",
                        "task_id": "task-1",
                    },
                },
            },
        },
    )

    assert len(sent) == 1
    assert sent[0]["cmd"] == "aibot_respond_update_msg"
    updated_card = sent[0]["body"]["template_card"]
    assert updated_card["main_title"]["desc"] == "当前进度: 30%"
    assert "30%" in updated_card["sub_title_text"]
    assert enqueued == []


@pytest.mark.asyncio
async def test_handle_wecom_template_card_event_still_enqueues_when_card_update_fails(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    enqueued = []
    info_messages = []
    bot.template_card_button_texts["task-1"] = {"ack_received": "确认执行"}
    bot.template_card_payloads["task-1"] = {
        "card_type": "button_interaction",
        "main_title": {"title": "请确认执行"},
        "button_list": [{"text": "确认执行", "style": 0, "key": "ack_received"}],
        "task_id": "task-1",
    }

    async def fake_send_ws_payload_with_ack(_bot, _payload, _timeout_sec):
        raise bridge_module.BridgeError(504, "websocket ack timeout")

    async def fake_enqueue_message(_bot, _key, text, _req_id, **_kwargs):
        enqueued.append(text)
        return True

    async def fake_respond_info(_bot, req_id, message, final=True):
        info_messages.append((req_id, message, final))

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "enqueue_message", fake_enqueue_message)
    monkeypatch.setattr(bridge_module, "respond_info", fake_respond_info)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-req-timeout"},
            "body": {
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "user-a"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "ack_received",
                        "task_id": "task-1",
                    },
                },
            },
        },
    )

    assert enqueued == ["确认执行"]
    assert info_messages == []


@pytest.mark.asyncio
async def test_handle_wecom_template_card_event_resends_card_when_update_paths_fail(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    enqueued = []
    resent = []
    bot.template_card_button_texts["task-1"] = {"ack_received": "确认执行"}
    bot.template_card_payloads["task-1"] = {
        "card_type": "button_interaction",
        "main_title": {"title": "请确认执行"},
        "button_list": [{"text": "确认执行", "style": 0, "key": "ack_received"}],
        "task_id": "task-1",
    }
    async def fake_send_ws_payload_with_ack(_bot, _payload, _timeout_sec):
        raise bridge_module.BridgeError(504, "websocket ack timeout")

    async def fake_enqueue_message(_bot, _key, text, _req_id, **_kwargs):
        enqueued.append(text)
        return True

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "enqueue_message", fake_enqueue_message)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-req-resend"},
            "body": {
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "user-a"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "ack_received",
                        "task_id": "task-1",
                    },
                },
            },
        },
    )

    assert enqueued == ["确认执行"]


@pytest.mark.asyncio
async def test_handle_wecom_template_card_event_from_non_owner_only_replies_info(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sent = []
    events = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        sent.append(payload)
        return {"errcode": 0, "body": {}}

    def fake_add_event_log(_bot, event, **fields):
        events.append((event, fields))

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "add_event_log", fake_add_event_log)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-req-2"},
            "body": {
                "msgtype": "event",
                "chattype": "group",
                "chatid": "room-1",
                "from": {"userid": "user-b"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "ack_received",
                        "task_id": "task-1__owner__user-a",
                    },
                },
            },
        },
    )

    assert len(sent) == 1
    assert sent[0]["cmd"] == "aibot_send_msg"
    assert "这是 user-a 的卡片" in sent[0]["body"]["markdown"]["content"]
    assert events[0][0] == "template_card.click_rejected_non_owner"


@pytest.mark.asyncio
async def test_handle_wecom_template_card_event_non_button_type_only_replies_info(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sent = []
    events = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        sent.append(payload)
        return {"errcode": 0, "body": {}}

    def fake_add_event_log(_bot, event, **fields):
        events.append((event, fields))

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "add_event_log", fake_add_event_log)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "evt-req-3"},
            "body": {
                "msgtype": "event",
                "chattype": "single",
                "from": {"userid": "user-a"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "vote_interaction",
                        "event_key": "submit_vote",
                        "task_id": "task-1",
                    },
                },
            },
        },
    )

    assert len(sent) == 1
    assert sent[0]["cmd"] == "aibot_send_msg"
    assert "当前类型暂不自动更新卡片" in sent[0]["body"]["markdown"]["content"]
    assert events[0][0] == "template_card.click_no_auto_update"


@pytest.mark.asyncio
async def test_handle_wecom_message_logs_raw_inbound_summary(bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.WECOM_RAW_INBOUND_LOG_ENABLED = True

    async def fake_enqueue_message(_bot, _key, _text, _req_id, **_kwargs):
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "msg-raw-1"},
            "body": {
                "msgid": "m-1",
                "msgtype": "text",
                "chattype": "single",
                "chatid": "user-a",
                "text": {"content": "hello"},
                "from": {"userid": "user-a"},
            },
        },
    )

    assert any("raw inbound:" in line and "reqId=msg-raw-1" in line and "msgid=m-1" in line for line in bot.logs)


def test_api_healthz_reports_missing_enabled_persisted_bot(bridge_module):
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

    response = asyncio.run(bridge_module.api_healthz(SimpleNamespace()))
    body = json.loads(response.text)

    assert response.status == 503
    assert body["ok"] is False
    assert body["missingEnabledBotIds"] == ["bot-1"]


def test_shutdown_reason_file_follows_runtime_base_dir(bridge_module):
    assert bridge_module.get_shutdown_reason_file() == (
        bridge_module.INSTANCE_RUNTIME_ROOT / ".bridge.shutdown-reason.json"
    )

    bridge_module.write_shutdown_reason("api_stop", "detail")

    payload = bridge_module.consume_shutdown_reason()

    assert payload["reason"] == "api_stop"
    assert payload["detail"] == "detail"
    assert not bridge_module.get_shutdown_reason_file().exists()
    assert not (bridge_module.BASE_DIR / ".bridge.shutdown-reason.json").exists()


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


def test_api_add_bot_preserves_existing_workspace_mode_when_updating_existing_bot(monkeypatch, bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "api-add-bot.secret", "secret\n")
    existing = {
        "id": "custom-id",
        "name": "claude-existing",
        "botId": "bot-id",
        "secretFile": str(secret_file),
        "workDir": str(bridge_module.BASE_DIR),
        "workspaceNamespace": "personal-main",
        "workspaceMode": "personal",
        "groupSessionMode": "per-user",
        "agentBackend": "claude",
        "agentCommand": "claude --model sonnet",
        "enabled": True,
    }
    bridge_module.write_json_atomic(bridge_module.DATA_FILE, [existing])
    payload = {
        "botId": "bot-id",
        "name": "claude-existing-renamed",
        "secretFile": str(secret_file),
        "workDir": str(bridge_module.BASE_DIR),
    }
    captured = {}

    async def fake_read_json_body(_request):
        return payload

    async def fake_require_api_access(_request):
        return None

    async def fake_start_bot(config):
        captured["config"] = config
        return bridge_module.BotState(config=config)

    monkeypatch.setattr(bridge_module, "read_json_body", fake_read_json_body)
    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "start_bot", fake_start_bot)

    response = asyncio.run(bridge_module.api_add_bot(SimpleNamespace()))
    body = json.loads(response.text)

    assert response.status == 200
    assert body["ok"] is True
    assert body["id"] == "custom-id"
    assert captured["config"]["workspaceMode"] == "personal"
    assert captured["config"]["agentBackend"] == "claude"
    assert captured["config"]["agentCommand"] == "claude --model sonnet"
    assert captured["config"]["workspaceNamespace"] == "personal-main"


def test_build_bridge_context_mentions_agent_backend(bridge_module):
    bot = make_bot(bridge_module)
    bot.config["agentBackend"] = "claude"
    bot.config["agentCommand"] = "claude --model sonnet"
    sess = make_session(bridge_module, bot)

    context = bridge_module.build_bridge_context(bot, sess, "single:test-user")

    assert "agentBackend: claude" in context
    assert "agentCommand: claude --model sonnet" in context


def test_submit_send_message_request_sends_template_card(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")
    ack_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        ack_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    result = asyncio.run(
        bridge_module.submit_send_message_request(
            {
                "chatKey": "single:test-user",
                "msgtype": "template_card",
                "templateCard": {
                    "card_type": "text_notice",
                    "main_title": {"title": "hello"},
                    "card_action": {"type": 1, "url": "https://example.com"},
                },
            }
        )
    )

    assert result["ok"] is True
    assert result["msgtype"] == "template_card"
    assert result["cardType"] == "text_notice"
    assert ack_payloads[0]["cmd"] == "aibot_send_msg"
    assert ack_payloads[0]["body"]["msgtype"] == "template_card"
    assert ack_payloads[0]["body"]["template_card"]["card_type"] == "text_notice"
    assert "taskId" not in result


def test_submit_send_message_request_accepts_string_zero_errcode(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    async def fake_send_ws_payload_with_ack(_bot, _payload, _timeout_sec):
        return {"errcode": "0", "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    result = asyncio.run(
        bridge_module.submit_send_message_request(
            {
                "chatKey": "single:test-user",
                "msgtype": "markdown",
                "content": "hello",
            }
        )
    )

    assert result["ok"] is True
    assert result["msgtype"] == "markdown"


def test_submit_send_message_request_returns_task_id_for_interaction_card(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    async def fake_send_ws_payload_with_ack(_bot, _payload, _timeout_sec):
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    result = asyncio.run(
        bridge_module.submit_send_message_request(
            {
                "chatKey": "single:test-user",
                "msgtype": "template_card",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "hello"},
                    "button_list": [{"text": "go", "style": 1, "key": "go"}],
                },
            }
        )
    )

    assert result["taskId"].startswith("codex-card-")


def test_submit_send_message_request_preserves_feedback_id(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")
    ack_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        ack_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    asyncio.run(
        bridge_module.submit_send_message_request(
            {
                "chatKey": "single:test-user",
                "msgtype": "template_card",
                "feedbackId": "feedback-1",
                "templateCard": {
                    "card_type": "text_notice",
                    "main_title": {"title": "hello"},
                    "card_action": {"type": 1, "url": "https://example.com"},
                },
            }
        )
    )

    assert ack_payloads[0]["body"]["template_card"]["feedback"]["id"] == "feedback-1"


def test_resolve_message_send_request_rejects_oversized_feedback_id(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_message_send_request(
            {
                "chatKey": "single:test-user",
                "msgtype": "markdown",
                "content": "hello",
                "feedbackId": "x" * 257,
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "feedback.id exceeds 256 bytes"


def test_api_send_message_returns_template_card_result(monkeypatch, bridge_module):
    payload = {
        "chatKey": "single:test-user",
        "msgtype": "template_card",
        "templateCard": {
            "card_type": "text_notice",
            "main_title": {"title": "hello"},
            "card_action": {"type": 1, "url": "https://example.com"},
        },
    }

    async def fake_read_json_body(_request):
        return payload

    async def fake_require_api_access(_request):
        return None

    async def fake_submit_send_message_request(data, source="api"):
        assert data == payload
        assert source == "api"
        return {"ok": True, "chatKey": "single:test-user", "msgtype": "template_card", "cardType": "text_notice"}

    monkeypatch.setattr(bridge_module, "read_json_body", fake_read_json_body)
    monkeypatch.setattr(bridge_module, "require_api_access", fake_require_api_access)
    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)

    response = asyncio.run(bridge_module.api_send_message(SimpleNamespace()))
    body = json.loads(response.text)

    assert body["ok"] is True
    assert body["msgtype"] == "template_card"
    assert body["cardType"] == "text_notice"


@pytest.mark.asyncio
async def test_handle_wecom_message_saves_response_url_for_text_callback(bridge_module):
    bot = make_bot(bridge_module)

    async def fake_enqueue_message(_bot, _key, _text, _req_id, **_kwargs):
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-msg-1"},
            "body": {
                "msgtype": "text",
                "chattype": "single",
                "chatid": "user-a",
                "text": {"content": "hello"},
                "from": {"userid": "user-a"},
                "response_url": "https://qyapi.weixin.qq.com/cgi-bin/aibot/response?response_code=abc",
            },
        },
    )

    assert bot.reply_urls["req-msg-1"]["responseUrl"].startswith("https://qyapi.weixin.qq.com/cgi-bin/aibot/response")


@pytest.mark.asyncio
async def test_handle_wecom_message_ignores_text_callback_without_sender_userid(bridge_module):
    bot = make_bot(bridge_module)
    called = False

    async def fake_enqueue_message(_bot, _key, _text, _req_id, **_kwargs):
        nonlocal called
        called = True
        return True

    bridge_module.enqueue_message = fake_enqueue_message
    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-msg-missing-user"},
            "body": {
                "msgtype": "text",
                "chattype": "single",
                "text": {"content": "hello"},
                "from": {},
                "response_url": "https://qyapi.weixin.qq.com/cgi-bin/aibot/response?response_code=abc",
            },
        },
    )

    assert called is False
    assert "req-msg-missing-user" not in bot.reply_urls


def test_resolve_message_send_request_supports_reply_req_id(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")

    resolved_bot, resolved_chat_key, payload_data, reply_req_id = bridge_module.resolve_message_send_request(
        {
            "replyReqId": "req-1",
            "msgtype": "markdown",
            "content": "follow up",
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert payload_data["content"] == "follow up"
    assert reply_req_id == "req-1"


def test_resolve_message_send_request_prefers_reply_req_id_over_unconnected_chat_key(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = None
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")

    resolved_bot, resolved_chat_key, payload_data, reply_req_id = bridge_module.resolve_message_send_request(
        {
            "chatKey": "single:test-user",
            "replyReqId": "req-1",
            "msgtype": "markdown",
            "content": "follow up",
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert payload_data["content"] == "follow up"
    assert reply_req_id == "req-1"


def test_resolve_message_send_request_ignores_null_reply_req_id(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    resolved_bot, resolved_chat_key, payload_data, reply_req_id = bridge_module.resolve_message_send_request(
        {
            "chatKey": "single:test-user",
            "replyReqId": "null",
            "msgtype": "markdown",
            "content": "follow up",
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert payload_data["content"] == "follow up"
    assert reply_req_id is None


@pytest.mark.asyncio
async def test_handle_wecom_message_tolerates_non_dict_headers_and_body(bridge_module):
    bot = make_bot(bridge_module)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "cmd": "aibot_event_callback",
            "headers": "bad",
            "body": "bad",
            "response_url": "https://example.com/response",
        },
    )

    assert bot.reply_urls == {}


@pytest.mark.asyncio
async def test_handle_wecom_message_ignores_non_dict_payload(bridge_module):
    bot = make_bot(bridge_module)

    await bridge_module.handle_wecom_message(bot, ["bad"])

    assert bot.reply_urls == {}


@pytest.mark.asyncio
async def test_submit_send_message_request_uses_response_url_for_reply_req_id(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")

    async def fake_send_reply_url_message(_bot, req_id, payload_data):
        assert _bot is bot
        assert req_id == "req-1"
        assert payload_data["content"] == "follow up"
        return {"ok": True, "chatKey": "single:test-user", "msgtype": "markdown"}

    monkeypatch.setattr(bridge_module, "send_reply_url_message", fake_send_reply_url_message)

    result = await bridge_module.submit_send_message_request(
        {
            "replyReqId": "req-1",
            "msgtype": "markdown",
            "content": "follow up",
        }
    )

    assert result["ok"] is True
    assert result["msgtype"] == "markdown"


@pytest.mark.asyncio
async def test_send_reply_url_message_uses_response_url_body_without_chat_target(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")
    captured = {}

    class FakeResponse:
        status = 200

        async def json(self, content_type=None):
            return {"errcode": 0, "errmsg": "ok"}

    class FakePostContext:
        async def __aenter__(self):
            return FakeResponse()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeSession:
        def post(self, url, json=None, timeout=None):
            captured["url"] = url
            captured["body"] = json
            return FakePostContext()

    bridge_module.HTTP_SESSION = FakeSession()

    result = await bridge_module.send_reply_url_message(
        bot,
        "req-1",
        {"msgtype": "markdown", "content": "follow up"},
    )

    assert result["ok"] is True
    assert captured["url"] == "https://example.com/response"
    assert captured["body"]["msgtype"] == "markdown"
    assert "chatid" not in captured["body"]
    assert "chat_type" not in captured["body"]
    stored = bridge_module.read_json_file(bridge_module.get_reply_url_state_file(bot.config["id"]), None)
    assert stored is not None
    assert stored["req-1"]["consumed"] is True


@pytest.mark.asyncio
async def test_send_reply_url_message_succeeds_when_state_disappears_after_send(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")

    class FakeResponse:
        status = 200

        async def json(self, content_type=None):
            return {"errcode": 0, "errmsg": "ok"}

    class FakePostContext:
        async def __aenter__(self):
            bot.reply_urls.pop("req-1", None)
            return FakeResponse()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeSession:
        def post(self, url, json=None, timeout=None):
            return FakePostContext()

    bridge_module.HTTP_SESSION = FakeSession()

    result = await bridge_module.send_reply_url_message(
        bot,
        "req-1",
        {"msgtype": "markdown", "content": "follow up"},
    )

    assert result["ok"] is True
    assert "req-1" not in bot.reply_urls


@pytest.mark.asyncio
async def test_send_reply_url_message_rejects_invalid_errcode_shape(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bridge_module.save_reply_url(bot, "req-1", "single:test-user", "https://example.com/response")

    class FakeResponse:
        status = 200

        async def json(self, content_type=None):
            return {"errcode": "bad", "errmsg": "oops"}

    class FakePostContext:
        async def __aenter__(self):
            return FakeResponse()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeSession:
        def post(self, url, json=None, timeout=None):
            return FakePostContext()

    bridge_module.HTTP_SESSION = FakeSession()

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        await bridge_module.send_reply_url_message(
            bot,
            "req-1",
            {"msgtype": "markdown", "content": "follow up"},
        )

    assert excinfo.value.status_code == 502
    assert excinfo.value.message == "response_url send failed: invalid errcode"


@pytest.mark.asyncio
async def test_send_reply_url_message_rejects_missing_response_url(monkeypatch, bridge_module):
    bot = make_bot(bridge_module)
    bot.reply_urls["req-1"] = {
        "chatKey": "single:test-user",
        "capturedAtMs": bridge_module.now_ms(),
        "consumed": False,
    }

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        await bridge_module.send_reply_url_message(
            bot,
            "req-1",
            {"msgtype": "markdown", "content": "follow up"},
        )

    assert excinfo.value.status_code == 404
    assert excinfo.value.message == "reply response_url not found or expired: req-1"


def test_build_template_card_update_payload_uses_update_template_card_response_type(bridge_module):
    payload = bridge_module.build_template_card_update_payload(
        "req-update-1",
        {
            "card_type": "button_interaction",
            "main_title": {"title": "updated"},
            "button_list": [{"text": "go", "key": "go"}],
        },
    )

    assert payload["cmd"] == "aibot_respond_update_msg"
    assert payload["headers"]["req_id"] == "req-update-1"
    assert payload["body"]["response_type"] == "update_template_card"
    assert payload["body"]["template_card"]["card_type"] == "button_interaction"


def test_build_template_card_update_payload_rejects_invalid_payload(bridge_module):
    with pytest.raises(ValueError) as excinfo:
        bridge_module.build_template_card_update_payload(
            "req-update-1",
            {"card_type": "button_interaction", "main_title": {"title": "updated"}},
        )

    assert str(excinfo.value) == "button_interaction.button_list required"


def test_build_template_card_update_payload_rejects_empty_req_id(bridge_module):
    with pytest.raises(ValueError) as excinfo:
        bridge_module.build_template_card_update_payload(
            "",
            {
                "card_type": "button_interaction",
                "main_title": {"title": "updated"},
                "button_list": [{"text": "go", "key": "go"}],
            },
        )

    assert str(excinfo.value) == "reqId required"


def test_normalize_bot_config_ignores_non_directory_legacy_agent_runtime_root(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot.secret", "secret\n")
    bad_root = bridge_module.BASE_DIR / "claude-runtime.txt"
    bad_root.write_text("not a dir", encoding="utf-8")

    config = bridge_module.normalize_bot_config(
        {
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentRuntimeRoot": str(bad_root),
        }
    )

    assert config["agentRuntimeRoot"] is None


def test_normalize_bot_config_treats_null_agent_runtime_root_as_unset(bridge_module):
    secret_file = write_secret_file(bridge_module.BASE_DIR / "bot-null.secret", "secret\n")

    config = bridge_module.normalize_bot_config(
        {
            "botId": "bot-id",
            "secretFile": str(secret_file),
            "workDir": str(bridge_module.BASE_DIR),
            "groupSessionMode": "per-user",
            "agentRuntimeRoot": "null",
        }
    )

    assert config["agentRuntimeRoot"] is None


def test_build_proactive_template_card_payload_marks_group_owner(bridge_module):
    payload = bridge_module.build_proactive_template_card_payload(
        "group-user:room-1:user-a",
        {
            "card_type": "button_interaction",
            "sub_title_text": "原始说明",
            "button_list": [{"text": "处理", "style": 1, "key": "go"}],
            "task_id": "task-1",
        },
    )

    assert payload["body"]["chat_type"] == 2
    assert payload["body"]["chatid"] == "room-1"
    assert payload["body"]["template_card"]["sub_title_text"].startswith("此卡片归属：user-a")
    assert payload["body"]["template_card"]["task_id"].startswith("task-1--owner--user-a--uniq--")


def test_build_proactive_template_card_payload_generates_task_id_for_interaction_card(bridge_module):
    payload = bridge_module.build_proactive_template_card_payload(
        "group-user:room-1:user-a",
        {
            "card_type": "button_interaction",
            "button_list": [{"text": "处理", "style": 1, "key": "go"}],
        },
    )

    assert payload["body"]["template_card"]["task_id"].startswith("codex-card-")
    assert "--owner--user-a--uniq--" in payload["body"]["template_card"]["task_id"]


def test_resolve_message_send_request_allows_missing_task_id_for_interaction_card(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    resolved_bot, resolved_chat_key, payload_data, reply_req_id = bridge_module.resolve_message_send_request(
        {
            "chatKey": "single:test-user",
            "msgtype": "template_card",
            "templateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "hello"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
            },
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert payload_data["templateCard"]["card_type"] == "button_interaction"
    assert reply_req_id is None


def test_resolve_message_send_request_ignores_null_bot_name(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    resolved_bot, resolved_chat_key, payload_data, reply_req_id = bridge_module.resolve_message_send_request(
        {
            "chatKey": "single:test-user",
            "botName": "null",
            "msgtype": "markdown",
            "content": "hello",
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert payload_data["content"] == "hello"
    assert reply_req_id is None


def test_resolve_message_send_request_ignores_null_session_id(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.resolve_message_send_request(
            {
                "chatKey": "group-user:room-only",
                "sessionId": "null",
                "msgtype": "markdown",
                "content": "hello",
            }
        )

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "invalid chatKey: group-user:room-only"


def test_resolve_message_send_request_ignores_null_target_config_id(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    resolved_bot, resolved_chat_key, payload_data, reply_req_id = bridge_module.resolve_message_send_request(
        {
            "chatKey": "single:test-user",
            "targetConfigId": "null",
            "msgtype": "markdown",
            "content": "hello",
        }
    )

    assert resolved_bot is bot
    assert resolved_chat_key == "single:test-user"
    assert payload_data["content"] == "hello"
    assert reply_req_id is None


def test_resolve_message_send_request_treats_null_feedback_id_as_missing(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    _resolved_bot, _resolved_chat_key, payload_data, _reply_req_id = bridge_module.resolve_message_send_request(
        {
            "chatKey": "single:test-user",
            "msgtype": "markdown",
            "content": "hello",
            "feedbackId": "null",
        }
    )

    assert payload_data["feedbackId"] is None


def test_extract_template_card_owner_user_id_supports_new_and_legacy_formats(bridge_module):
    assert bridge_module.extract_template_card_owner_user_id("task-1--owner--user-a--uniq--abcdef") == "user-a"
    assert bridge_module.extract_template_card_owner_user_id("task-1__owner__user-a__uniq__abcdef") == "user-a"


@pytest.mark.asyncio
async def test_process_local_send_message_queue_writes_success_result(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")
    request_id = "local-msg-1"
    queue_paths = bridge_module.get_local_send_message_queue_paths(bot.config["id"])
    for root in ("pending", "processing", "results", "done"):
        with contextlib.suppress(FileNotFoundError):
            (queue_paths[root] / f"{request_id}.json").unlink()
    bridge_module.write_json_atomic(
        queue_paths["pending"] / f"{request_id}.json",
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "msgtype": "markdown",
            "content": "hello",
            "requestedAt": bridge_module.now_ms(),
            "timeoutMs": bridge_module.LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS,
            "expiresAt": bridge_module.now_ms() + bridge_module.LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS,
        },
    )

    async def fake_submit_send_message_request(data, source="api"):
        assert data["chatKey"] == "single:test-user"
        assert source == "local-command"
        return {"ok": True, "chatKey": "single:test-user", "msgtype": "markdown"}

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)

    await bridge_module.process_local_send_message_queue_once()

    result = bridge_module.read_json_file(queue_paths["results"] / f"{request_id}.json", None)
    assert result["ok"] is True
    assert (queue_paths["done"] / f"{request_id}.json").exists()


@pytest.mark.asyncio
async def test_process_local_send_message_queue_treats_zero_expires_at_as_expired(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")
    request_id = "local-msg-zero-expiry"
    queue_paths = bridge_module.get_local_send_message_queue_paths(bot.config["id"])
    for root in ("pending", "processing", "results", "done", "failed"):
        with contextlib.suppress(FileNotFoundError):
            (queue_paths[root] / f"{request_id}.json").unlink()
    bridge_module.write_json_atomic(
        queue_paths["pending"] / f"{request_id}.json",
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "msgtype": "markdown",
            "content": "hello",
            "requestedAt": bridge_module.now_ms(),
            "timeoutMs": 120000,
            "expiresAt": 0,
        },
    )
    called = False

    async def fake_submit_send_message_request(_data, source="api"):
        nonlocal called
        called = True
        return {"ok": True}

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)

    await bridge_module.process_local_send_message_queue_once()

    assert called is False
    result = bridge_module.read_json_file(queue_paths["results"] / f"{request_id}.json", None)
    assert result["ok"] is False
    assert result["statusCode"] == 408
    assert (queue_paths["failed"] / f"{request_id}.json").exists()


def test_submit_local_send_message_request_preserves_zero_timeout_ms(bridge_module):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")

    result = bridge_module.submit_local_send_message_request(
        {
            "chatKey": "single:test-user",
            "msgtype": "markdown",
            "content": "hello",
            "timeoutMs": 0,
        }
    )

    queue_paths = bridge_module.get_local_send_message_queue_paths(bot.config["id"])
    stored = bridge_module.read_json_file(queue_paths["pending"] / f"{result['requestId']}.json", None)
    assert stored["timeoutMs"] == 0


@pytest.mark.asyncio
async def test_process_local_send_message_queue_sanitizes_slashed_request_id(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")
    request_id = "folder-sanitize/local-msg"
    queue_paths = bridge_module.get_local_send_message_queue_paths(bot.config["id"])
    default_paths = bridge_module.get_local_send_message_queue_paths()
    safe_name = bridge_module.local_queue_request_file_name(request_id)
    pending_file = queue_paths["pending"] / safe_name
    result_file = queue_paths["results"] / safe_name
    done_file = queue_paths["done"] / safe_name
    nested_dir = queue_paths["pending"] / "folder-sanitize"
    with contextlib.suppress(FileNotFoundError):
        pending_file.unlink()
    with contextlib.suppress(FileNotFoundError):
        result_file.unlink()
    with contextlib.suppress(FileNotFoundError):
        done_file.unlink()
    with contextlib.suppress(FileNotFoundError):
        (default_paths["pending"] / safe_name).unlink()
    if nested_dir.exists():
        shutil.rmtree(nested_dir, ignore_errors=True)

    result = bridge_module.submit_local_send_message_request(
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "msgtype": "markdown",
            "content": "hello",
        }
    )

    async def fake_submit_send_message_request(data, source="api"):
        assert data["requestId"] == request_id
        assert source == "local-command"
        return {"ok": True, "chatKey": "single:test-user", "msgtype": "markdown"}

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)

    await bridge_module.process_local_send_message_queue_once()

    assert result["ok"] is True
    assert result["requestId"] == request_id
    assert pending_file.exists() is False
    assert (default_paths["pending"] / safe_name).exists() is False
    assert result_file.exists() is True
    assert done_file.exists() is True
    assert nested_dir.exists() is False
    stored = bridge_module.read_json_file(result_file, None)
    assert stored["ok"] is True


@pytest.mark.asyncio
async def test_process_local_send_message_queue_requeues_transient_503(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.create_session_record(bot, "single:test-user")
    request_id = "local-msg-2"
    queue_paths = bridge_module.get_local_send_message_queue_paths(bot.config["id"])
    for root in ("pending", "processing", "results", "done"):
        with contextlib.suppress(FileNotFoundError):
            (queue_paths[root] / f"{request_id}.json").unlink()
    bridge_module.write_json_atomic(
        queue_paths["pending"] / f"{request_id}.json",
        {
            "requestId": request_id,
            "chatKey": "single:test-user",
            "msgtype": "markdown",
            "content": "hello",
            "requestedAt": bridge_module.now_ms(),
            "timeoutMs": bridge_module.LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS,
            "expiresAt": bridge_module.now_ms() + bridge_module.LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS,
        },
    )

    async def fake_submit_send_message_request(_data, source="api"):
        raise bridge_module.BridgeError(503, "bot not connected")

    monkeypatch.setattr(bridge_module, "submit_send_message_request", fake_submit_send_message_request)

    await bridge_module.process_local_send_message_queue_once()

    assert (queue_paths["pending"] / f"{request_id}.json").exists()
    assert not (queue_paths["results"] / f"{request_id}.json").exists()


@pytest.mark.asyncio
async def test_process_local_send_message_queue_cleans_up_stale_result_files(bridge_module):
    bridge_module.ensure_local_send_message_dirs()
    old_result = bridge_module.LOCAL_SEND_MESSAGE_RESULT_ROOT / "old.json"
    fresh_result = bridge_module.LOCAL_SEND_MESSAGE_RESULT_ROOT / "fresh.json"
    retained_result = bridge_module.LOCAL_SEND_MESSAGE_RESULT_ROOT / "retained.json"
    bridge_module.write_json_atomic(
        old_result,
        {
            "ok": False,
            "statusCode": 504,
            "error": "expired",
            "processedAt": bridge_module.now_ms() - bridge_module.LOCAL_SEND_MESSAGE_RESULT_RETENTION_MS - 1000,
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
            "ok": False,
            "statusCode": 409,
            "error": "keep",
            "processedAt": bridge_module.now_ms() - bridge_module.LOCAL_SEND_MESSAGE_RESULT_RETENTION_MS - 1000,
            "retainUntil": bridge_module.now_ms() + 60_000,
        },
    )

    await bridge_module.process_local_send_message_queue_once()

    assert not old_result.exists()
    assert fresh_result.exists()
    assert retained_result.exists()


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
        if payload["cmd"] == "aibot_upload_media_chunk":
            return {"errcode": 0, "body": {}}
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
async def test_upload_and_send_file_accepts_string_zero_errcode(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.ws = SimpleNamespace(closed=False)
    file_path = bridge_module.BASE_DIR / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec):
        if payload["cmd"] == "aibot_upload_media_init":
            return {"errcode": "0", "body": {"upload_id": "upload-1"}}
        if payload["cmd"] == "aibot_upload_media_chunk":
            return {"errcode": "0", "body": {}}
        if payload["cmd"] == "aibot_upload_media_finish":
            return {"errcode": "0", "body": {"media_id": "media-1"}}
        if payload["cmd"] == "aibot_send_msg":
            return {"errcode": "0", "body": {}}
        raise AssertionError(payload["cmd"])

    async def fake_send_ws_payload(_bot, payload):
        future = _bot.pending_requests.pop(payload["headers"]["req_id"], None)
        if future and not future.done():
            future.set_result({"errcode": "0"})

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    monkeypatch.setattr(bridge_module, "send_ws_payload", fake_send_ws_payload)

    await bridge_module.upload_and_send_file(bot, "single:test-user", str(file_path))


@pytest.mark.asyncio
async def test_download_incoming_media_keeps_same_named_files(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)

    async def fake_download_buffer(url: str, _max_bytes: int):
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
async def test_download_buffer_stops_before_exceeding_memory_limit(bridge_module):
    class FakeContent:
        async def iter_chunked(self, _chunk_size):
            for chunk in (b"abc", b"def"):
                yield chunk

    class FakeResponse:
        status = 200
        headers = {}
        content = FakeContent()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        def get(self, *_args, **_kwargs):
            return FakeResponse()

    bridge_module.HTTP_SESSION = FakeSession()

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        await bridge_module.download_buffer("https://example.invalid/media", 5)

    assert excinfo.value.status_code == 413
    assert "max 5 bytes" in excinfo.value.message


@pytest.mark.asyncio
async def test_download_buffer_via_curl_rejects_oversized_partial_file(bridge_module, monkeypatch):
    captured_args = []

    class FakeProcess:
        returncode = 0

        async def communicate(self):
            return b"", b""

    async def fake_create_subprocess_exec(*args, **_kwargs):
        captured_args.extend(args)
        body_path = Path(args[args.index("-o") + 1])
        body_path.write_bytes(b"123456")
        return FakeProcess()

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        await bridge_module.download_buffer_via_curl("https://example.invalid/media", 5)

    assert excinfo.value.status_code == 413
    assert captured_args[captured_args.index("--max-filesize") + 1] == "5"


@pytest.mark.asyncio
async def test_download_incoming_encrypted_media_checks_plaintext_limit_after_decrypt(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    key = b"12345678901234567890123456789012"
    plaintext = b"x" * 16
    encrypted = bridge_module.AES.new(key, bridge_module.AES.MODE_CBC, key[:16]).encrypt(
        plaintext + bytes([16]) * 16
    )
    monkeypatch.setattr(bridge_module, "MAX_INBOUND_FILE_SIZE", len(plaintext))

    async def fake_download_buffer(_url: str, max_bytes: int):
        assert max_bytes == len(plaintext) + 32
        return {"data": encrypted, "contentType": "application/octet-stream", "contentDisposition": ""}

    monkeypatch.setattr(bridge_module, "download_buffer", fake_download_buffer)

    result = await bridge_module.download_incoming_media(
        bot,
        sess,
        "single:test-user",
        "file",
        {"url": "https://example.test/file", "filename": "payload.bin", "aeskey": key.decode("ascii")},
    )

    assert Path(result["path"]).read_bytes() == plaintext


@pytest.mark.asyncio
async def test_download_incoming_encrypted_media_rejects_oversized_plaintext(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    key = b"12345678901234567890123456789012"
    plaintext = b"x" * 17
    encrypted = bridge_module.AES.new(key, bridge_module.AES.MODE_CBC, key[:16]).encrypt(
        plaintext + bytes([15]) * 15
    )
    monkeypatch.setattr(bridge_module, "MAX_INBOUND_FILE_SIZE", 16)

    async def fake_download_buffer(_url: str, _max_bytes: int):
        return {"data": encrypted, "contentType": "application/octet-stream", "contentDisposition": ""}

    monkeypatch.setattr(bridge_module, "download_buffer", fake_download_buffer)

    with pytest.raises(bridge_module.BridgeError) as excinfo:
        await bridge_module.download_incoming_media(
            bot,
            sess,
            "single:test-user",
            "file",
            {"url": "https://example.test/file", "filename": "payload.bin", "aeskey": key.decode("ascii")},
        )

    assert excinfo.value.status_code == 413
    assert "too large after decrypt" in excinfo.value.message


def test_decrypt_media_buffer_rejects_invalid_aes_key_in_classic_bridge(bridge_module):
    with pytest.raises(bridge_module.BridgeError) as excinfo:
        bridge_module.decrypt_media_buffer(b"abc", "abcd")

    assert excinfo.value.status_code == 400
    assert excinfo.value.message == "invalid aes key"


@pytest.mark.asyncio
async def test_download_incoming_media_skips_broken_symlink_name(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    chatfile_dir = bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")["chatfile"]
    assert chatfile_dir is not None
    external_dir = bridge_module.BASE_DIR / "external-media-target"
    external_dir.mkdir(parents=True, exist_ok=True)
    broken_target = external_dir / "report.txt"
    (chatfile_dir / "report.txt").symlink_to(broken_target)

    async def fake_download_buffer(_url: str, _max_bytes: int):
        return {"data": b"payload", "contentType": "text/plain", "contentDisposition": ""}

    monkeypatch.setattr(bridge_module, "download_buffer", fake_download_buffer)

    result = await bridge_module.download_incoming_media(
        bot,
        sess,
        "single:test-user",
        "file",
        {"url": "https://example.test/file/1", "filename": "report.txt"},
    )

    assert Path(result["path"]).name == "report-1.txt"
    assert not broken_target.exists()
    assert Path(result["path"]).read_bytes() == b"payload"


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
async def test_supervise_bot_task_restarts_failed_worker(bridge_module):
    bot = make_bot(bridge_module)
    calls = []
    restarted = asyncio.Event()

    async def flaky_worker():
        calls.append("run")
        if len(calls) == 1:
            raise RuntimeError("boom")
        restarted.set()

    task = bridge_module.supervise_bot_task(bot, "upload_worker_task", "upload_worker", flaky_worker)
    with contextlib.suppress(RuntimeError):
        await task
    await asyncio.wait_for(restarted.wait(), 1)

    assert len(calls) >= 2
    assert bot.task_restart_counts["upload_worker"] == 1


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
async def test_process_local_file_send_queue_treats_zero_expires_at_as_expired(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, config_id="bot-2", remote_bot_id="remote-bot-2")
    bot.ws = SimpleNamespace(closed=True)
    make_session(bridge_module, bot)
    queue_paths = bridge_module.get_local_file_send_queue_paths(bot.config["id"])
    bridge_module.ensure_local_file_send_dirs([bot.config["id"]])
    chatfile_dir = bridge_module.get_chatfile_dir(bot.config["id"], "single:test-user")
    bridge_module.ensure_dir(chatfile_dir)
    file_path = chatfile_dir / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request_file = queue_paths["pending"] / "zero-expired-bot.json"
    bridge_module.write_json_atomic(
        request_file,
        {
            "requestId": "zero-expired-bot",
            "chatKey": "single:test-user",
            "targetConfigId": bot.config["id"],
            "filePath": str(file_path.resolve()),
            "requestedAt": bridge_module.now_ms(),
            "timeoutMs": 120000,
            "expiresAt": 0,
        },
    )
    called = False

    async def fake_upload_and_send_file(_bot, _chat_key, _file_path, **_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(bridge_module, "upload_and_send_file", fake_upload_and_send_file)

    await bridge_module.process_local_file_send_queue_once()

    assert called is False
    result = json.loads((queue_paths["results"] / "zero-expired-bot.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 504
    assert "expired before delivery" in result["error"]
    assert (queue_paths["failed"] / "zero-expired-bot.json").exists()
    assert not (queue_paths["processing"] / "zero-expired-bot.json").exists()


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


def test_write_local_file_send_result_sanitizes_slashed_request_id(bridge_module):
    queue_paths = bridge_module.get_local_file_send_queue_paths("bot-1")
    bridge_module.ensure_local_file_send_dirs(["bot-1"])
    request_id = "folder-sanitize/file-send"
    safe_name = bridge_module.local_queue_request_file_name(request_id)
    result_file = queue_paths["results"] / safe_name
    nested_dir = queue_paths["results"] / "folder-sanitize"
    with contextlib.suppress(FileNotFoundError):
        result_file.unlink()
    if nested_dir.exists():
        shutil.rmtree(nested_dir, ignore_errors=True)

    bridge_module.write_local_file_send_result(request_id, {"ok": True, "message": "sent"}, queue_paths)

    assert result_file.exists() is True
    assert nested_dir.exists() is False
    loaded = bridge_module.read_local_file_send_result(request_id, queue_paths)
    assert loaded["ok"] is True
    assert loaded["message"] == "sent"


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

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "new session before file send")

    assert cancelled == 1
    result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 409
    assert result["error"] == "new session before file send"
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

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "new session before file send")

    assert cancelled == 1
    assert bot.upload_queue.qsize() == 0
    assert request_id not in bot.active_local_file_request_ids
    result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 409
    assert result["error"] == "new session before file send"
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

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "new session before file send")

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

        cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "new session before file send")

        assert cancelled == 1
        await asyncio.wait_for(cancelled_upload.wait(), 1)
        await asyncio.wait_for(bot.upload_queue.join(), 1)
        await asyncio.sleep(0)

        result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
        assert result["ok"] is False
        assert result["statusCode"] == 409
        assert result["error"] == "new session before file send"
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

    cancelled = bridge_module.cancel_pending_file_send_requests(bot, "single:test-user", sess.session_id, "new session before file send")

    assert cancelled == 1
    result = json.loads((queue_paths["results"] / f"{request_id}.json").read_text("utf-8"))
    assert result["ok"] is False
    assert result["statusCode"] == 409
    assert result["error"] == "new session before file send"
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


def test_extract_agent_progress_handles_codex_phase_events(bridge_module):
    started = json.dumps({
        "type": "item.started",
        "item": {"type": "command_execution", "command": "pytest tests/test_api.py"},
    })
    output = json.dumps({
        "type": "item.commandExecution.outputDelta",
        "delta": "1 failed, 4 passed",
    })

    assert bridge_module.extract_agent_progress("codex", started) == bridge_module.AgentProgress("执行中")
    assert bridge_module.extract_agent_progress("codex", output) is None
    assert bridge_module.extract_agent_progress(
        "codex",
        json.dumps({
            "type": "item.updated",
            "item": {
                "type": "command_execution",
                "command": "pytest tests/test_api.py",
                "aggregated_output": "1 failed, 4 passed",
            },
        }),
    ) == bridge_module.AgentProgress("执行中")
    assert bridge_module.extract_agent_progress(
        "codex",
        json.dumps({
            "type": "item.reasoning.summaryPartAdded",
            "summary": "正在核对配置并整理结果",
        }),
    ) == bridge_module.AgentProgress("分析中", "正在核对配置并整理结果")
    assert bridge_module.extract_agent_progress(
        "codex",
        json.dumps({
            "type": "item.completed",
            "item": {"type": "agent_message", "text": "正在核对配置并整理结果"},
        }),
    ) == bridge_module.AgentProgress("处理中", "正在核对配置并整理结果")


def test_extract_agent_progress_handles_claude_tool_events(bridge_module):
    event = json.dumps({
        "type": "assistant",
        "message": {"content": [{"type": "tool_use", "name": "Bash"}]},
    })

    assert bridge_module.extract_agent_progress("claude", event) == bridge_module.AgentProgress("处理中")


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


def test_build_status_stream_content_keeps_summary_visible(bridge_module):
    result = bridge_module.build_status_stream_content("运行状态：整理回复中. 已运行 5s。", "阶段性摘要")
    assert result == "运行状态：整理回复中. 已运行 5s。\n\n阶段性摘要"


@pytest.mark.asyncio
async def test_enqueue_message_sends_queue_status_when_session_busy(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    payloads = []
    bot.ws = make_acking_ws(bot, payloads.append)

    result = await bridge_module.enqueue_message(bot, "single:test-user", "queued", "req-1")

    assert result is True
    assert payloads[0]["body"]["stream"]["finish"] is True
    assert payloads[0]["body"]["stream"]["content"] == "运行状态：排队中，前方还有 1 个任务。 任务完成后会主动发送结果。"
    assert "req-1" in sess.reply_proactive_req_ids


@pytest.mark.asyncio
async def test_enqueue_message_allows_independent_chats_to_progress_concurrently(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    first = make_session(bridge_module, bot, "single:alice")
    second = make_session(bridge_module, bot, "single:bob")
    started = []

    async def fake_run_codex(_bot, _sess, key, *_args, **_kwargs):
        started.append(key)
        return None

    monkeypatch.setattr(bridge_module, "run_codex", fake_run_codex)

    accepted_a = await bridge_module.enqueue_message(bot, "single:alice", "hello", "req-a")
    accepted_b = await bridge_module.enqueue_message(bot, "single:bob", "world", "req-b")
    await asyncio.sleep(0)

    assert accepted_a is True
    assert accepted_b is True
    assert sorted(started) == ["single:alice", "single:bob"]
    assert first.running is True
    assert second.running is True


@pytest.mark.asyncio
async def test_enqueue_message_bootstraps_workspace_via_to_thread(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append((func.__name__, args[1] if len(args) > 1 else None))
        return func(*args, **kwargs)

    def fake_process_queue(*_args, **_kwargs):
        return None

    monkeypatch.setattr(bridge_module, "run_blocking", fake_run_blocking)
    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    accepted = await bridge_module.enqueue_message(bot, "single:test-user", "hello", "req-1")

    assert accepted is True
    assert ("ensure_session_workspace_dirs", "single:test-user") in calls


@pytest.mark.asyncio
async def test_enqueue_message_reports_lease_conflict_instead_of_silent_drop(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    make_session(bridge_module, bot)
    responses = []
    bot.ws = make_acking_ws(bot, responses.append)
    monkeypatch.setattr(bridge_module, "acquire_session_lease", lambda *_args, **_kwargs: False)

    accepted = await bridge_module.enqueue_message(bot, "single:test-user", "hello", "req-1")

    assert accepted is False
    assert responses
    assert responses[0]["body"]["stream"]["finish"] is True
    assert "temporarily owned by another bridge instance" in responses[0]["body"]["stream"]["content"]


@pytest.mark.asyncio
async def test_enqueue_message_uses_proactive_for_attachment_notice_after_queue_finish(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sess.pending_media_downloads = 1
    stream_payloads = []
    proactive_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        if payload["cmd"] == "aibot_respond_msg":
            stream_payloads.append(payload)
        else:
            proactive_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    bot.ws = SimpleNamespace(closed=False)
    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    result = await bridge_module.enqueue_message(bot, "single:test-user", "queued", "req-1")

    assert result is True
    assert stream_payloads[0]["body"]["stream"]["finish"] is True
    assert len(proactive_payloads) == 1
    assert proactive_payloads[0]["cmd"] == "aibot_send_msg"
    assert "Attachments are still downloading" in proactive_payloads[0]["body"]["markdown"]["content"]


@pytest.mark.asyncio
async def test_enqueue_message_does_not_cache_queue_notice_when_ack_times_out(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    bot.ws = SimpleNamespace(closed=False)

    async def fake_send_ws_payload_with_ack(_bot, _payload, _timeout_sec, **_kwargs):
        raise bridge_module.BridgeError(504, "websocket ack timeout")

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    accepted = await bridge_module.enqueue_message(bot, "single:test-user", "queued", "req-1")

    assert accepted is True
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None
    assert "req-1" in sess.reply_proactive_req_ids
    assert sess.queue == [{"text": "queued", "reqId": "req-1", "scheduledJobFile": None, "scheduleId": None, "scheduleRequestId": None}]


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


def test_interrupt_session_with_clear_queue_removes_session_codex_home(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bridge_module.ensure_dir(bridge_module.get_session_codex_home_root(sess.session_id))
    assert bridge_module.get_session_codex_home_root(sess.session_id).exists()

    bridge_module.interrupt_session(bot, "single:test-user", sess, clear_thread=True, clear_chat=True, clear_queue=True)

    assert bridge_module.get_session_codex_home_root(sess.session_id).exists() is False


@pytest.mark.asyncio
async def test_reset_session_waits_for_active_run_before_removing_thread_state(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "old-thread"
    sess.running = True
    order: list[str] = []

    async def active_run():
        try:
            await asyncio.Future()
        finally:
            order.append("task-exit")

    class FakeProcess:
        returncode = None

        def kill(self):
            order.append("kill")
            self.returncode = -9

        async def wait(self):
            order.append("wait")
            return self.returncode

    process = FakeProcess()
    task = asyncio.create_task(active_run())
    await asyncio.sleep(0)
    sess.run_task = task
    sess.proc = process

    original_remove = bridge_module.remove_session_codex_home

    def tracked_remove(session_id):
        order.append("remove")
        original_remove(session_id)

    monkeypatch.setattr(bridge_module, "remove_session_codex_home", tracked_remove)

    result = await bridge_module.reset_session_context(bot, "single:test-user")

    assert order.index("kill") < order.index("wait") < order.index("task-exit") < order.index("remove")
    assert result["threadId"] is None
    assert result["sessionId"] == sess.session_id
    assert bot.sessions["single:test-user"] is sess
    assert sess.thread_id is None
    assert sess.running is False


@pytest.mark.asyncio
async def test_new_session_preserves_previous_context_and_rebinds_chat(bridge_module):
    bot = make_bot(bridge_module)
    previous = make_session(bridge_module, bot)
    previous.thread_id = "old-thread"
    previous.chat = [{"role": "user", "text": "keep-chat"}]
    bridge_module.update_owned_session_record(
        bot,
        "single:test-user",
        previous,
        lambda record: {
            **record,
            "threadId": previous.thread_id,
            "recentChat": list(previous.chat),
        },
    )
    rollout = bridge_module.get_session_codex_home_root(previous.session_id) / "sessions" / "rollout-old.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text("old rollout", encoding="utf-8")

    result = await bridge_module.new_session_context(bot, "single:test-user")

    current = bot.sessions["single:test-user"]
    previous_record = bridge_module.read_session_record_by_id(previous.session_id)
    current_record = bridge_module.read_session_record_by_key(bot.config["id"], "single:test-user")
    assert result["previousSessionId"] == previous.session_id
    assert result["sessionId"] == current.session_id
    assert current.session_id != previous.session_id
    assert current.thread_id is None
    assert current.chat == []
    assert previous_record["threadId"] == "old-thread"
    assert previous_record["recentChat"] == [{"role": "user", "text": "keep-chat"}]
    assert current_record["sessionId"] == current.session_id
    assert current_record["threadId"] is None
    assert current_record["recentChat"] == []
    assert rollout.read_text(encoding="utf-8") == "old rollout"

    candidates = bridge_module.build_resume_candidates(bot, "single:test-user")
    assert [candidate["sessionId"] for candidate in candidates] == [previous.session_id]
    bridge_module.bind_resume_candidate_to_session(bot, current, "single:test-user", candidates[0])
    copied_rollout = bridge_module.get_session_codex_home_root(current.session_id) / "sessions" / rollout.name
    assert current.thread_id == "old-thread"
    assert current.chat == [{"role": "user", "text": "keep-chat"}]
    assert copied_rollout.read_text(encoding="utf-8") == "old rollout"


@pytest.mark.asyncio
async def test_new_session_rejects_foreign_lease_without_clearing_context(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "keep-thread"
    sess.chat.append({"role": "user", "text": "keep-chat"})
    session_home = bridge_module.get_session_codex_home_root(sess.session_id)
    session_home.mkdir(parents=True)
    marker = session_home / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    expires_at = bridge_module.now_ms() + 60_000
    foreign_lease = {
        "instanceId": "foreign-instance",
        "pid": 12345,
        "botId": bot.config["id"],
        "chatKey": "single:test-user",
        "updatedAt": bridge_module.now_ms(),
        "expiresAt": expires_at,
    }
    bridge_module.write_lease(sess.lock_file, foreign_lease)
    bridge_module.update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "ownerInstance": foreign_lease["instanceId"],
            "ownerPid": foreign_lease["pid"],
            "leaseExpiresAt": foreign_lease["expiresAt"],
            "threadId": sess.thread_id,
            "recentChat": list(sess.chat),
        },
    )

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.new_session_context(bot, "single:test-user")

    assert exc_info.value.status_code == 409
    assert sess.thread_id == "keep-thread"
    assert sess.chat == [{"role": "user", "text": "keep-chat"}]
    assert marker.read_text(encoding="utf-8") == "keep"
    assert bot.sessions["single:test-user"] is sess
    assert bridge_module.read_session_record_by_key(bot.config["id"], "single:test-user")["sessionId"] == sess.session_id


@pytest.mark.asyncio
async def test_new_session_keeps_context_when_process_exit_times_out(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "keep-thread"
    sess.chat.append({"role": "user", "text": "keep-chat"})
    session_home = bridge_module.get_session_codex_home_root(sess.session_id)
    session_home.mkdir(parents=True)
    marker = session_home / "keep.txt"
    marker.write_text("keep", encoding="utf-8")

    class StuckProcess:
        returncode = None

        def kill(self):
            return None

        async def wait(self):
            await asyncio.Future()

    sess.proc = StuckProcess()
    sess.running = True
    monkeypatch.setattr(bridge_module, "SESSION_CONTROL_EXIT_TIMEOUT_SEC", 0.01)

    with pytest.raises(bridge_module.BridgeError) as exc_info:
        await bridge_module.new_session_context(bot, "single:test-user")

    assert exc_info.value.status_code == 409
    assert sess.thread_id == "keep-thread"
    assert sess.chat == [{"role": "user", "text": "keep-chat"}]
    assert marker.read_text(encoding="utf-8") == "keep"
    assert bot.sessions["single:test-user"] is sess
    assert bridge_module.read_session_record_by_key(bot.config["id"], "single:test-user")["sessionId"] == sess.session_id


@pytest.mark.asyncio
async def test_api_new_session_clears_resume_selection(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.resume_candidates = [{"sessionId": "old-session", "threadId": "old-thread"}]
    sess.resume_selection_expires_at = bridge_module.now_ms() + 60_000

    response = await bridge_module.api_new_session(
        SimpleNamespace(match_info={"bot_id": bot.config["id"], "chat_key": "single:test-user"})
    )
    payload = json.loads(response.text)

    assert response.status == 200
    assert payload["action"] == "new"
    assert payload["previousSessionId"] == sess.session_id
    assert payload["sessionId"] != sess.session_id
    assert bot.sessions["single:test-user"].session_id == payload["sessionId"]
    assert sess.resume_candidates == []
    assert sess.resume_selection_expires_at == 0


@pytest.mark.asyncio
async def test_api_reset_session_clears_current_context_without_rebinding(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "old-thread"
    sess.chat = [{"role": "user", "text": "clear-chat"}]
    session_home = bridge_module.get_session_codex_home_root(sess.session_id)
    session_home.mkdir(parents=True)

    response = await bridge_module.api_reset_session(
        SimpleNamespace(match_info={"bot_id": bot.config["id"], "chat_key": "single:test-user"})
    )
    payload = json.loads(response.text)

    assert response.status == 200
    assert payload["action"] == "reset"
    assert payload["sessionId"] == sess.session_id
    assert bot.sessions["single:test-user"] is sess
    assert sess.thread_id is None
    assert sess.chat == []
    assert session_home.exists() is False


@pytest.mark.asyncio
async def test_new_session_waits_for_message_registration_lock(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "old-thread"
    registration_started = asyncio.Event()
    finish_registration = asyncio.Event()

    async def blocked_registration(*_args, **_kwargs):
        registration_started.set()
        await finish_registration.wait()
        return True

    monkeypatch.setattr(bridge_module, "enqueue_message_for_session", blocked_registration)
    enqueue_task = asyncio.create_task(
        bridge_module.enqueue_message(bot, "single:test-user", "hello", "req-message")
    )
    await asyncio.wait_for(registration_started.wait(), timeout=1)

    new_task = asyncio.create_task(bridge_module.new_session_context(bot, "single:test-user"))
    await asyncio.sleep(0)

    assert new_task.done() is False
    assert sess.thread_id == "old-thread"

    finish_registration.set()
    assert await enqueue_task is True
    result = await new_task

    assert result["threadId"] is None
    assert result["sessionId"] != sess.session_id
    assert sess.thread_id == "old-thread"
    assert bot.sessions["single:test-user"].thread_id is None


@pytest.mark.asyncio
async def test_respond_info_keeps_intermediate_reply_open(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = []
    bot.ws = make_acking_ws(bot, payloads.append)
    bridge_module.register_reply_session(bot, "req-1", sess)

    await bridge_module.respond_info(bot, "req-1", "processing", final=False)
    await bridge_module.respond_info(bot, "req-1", "done")

    assert payloads[0]["body"]["stream"]["finish"] is False
    assert payloads[1]["body"]["stream"]["finish"] is True
    assert "req-1" not in bot.reply_sessions


@pytest.mark.asyncio
async def test_respond_info_sends_long_messages_in_chunks(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sent = []
    bot.ws = make_acking_ws(bot, sent.append)
    bridge_module.register_reply_session(bot, "req-1", sess)

    message = "x" * (bridge_module.STREAM_TEXT_MAX_CHARS * 2 + 1)
    await bridge_module.respond_info(bot, "req-1", message)

    assert len(sent) == 3
    assert all(payload["body"]["stream"]["finish"] is True for payload in sent)
    visible = {payload["body"]["stream"]["id"]: payload["body"]["stream"]["content"] for payload in sent}
    assert "".join(visible.values()) == message
    assert "".join(payload["body"]["stream"]["content"] for payload in sent) == message


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [3000, 8000])
async def test_long_proactive_fallback_preserves_the_complete_final_reply(bridge_module, length):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sent = []
    bot.ws = make_acking_ws(bot, sent.append)
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_proactive_req_ids.add("req-1")
    message = "x" * length

    await bridge_module.respond_info(bot, "req-1", message)

    assert all(payload["cmd"] == "aibot_send_msg" for payload in sent)
    assert "".join(payload["body"]["markdown"]["content"] for payload in sent) == message
    assert len({payload["headers"]["req_id"] for payload in sent}) == len(sent)


def test_split_text_chunks_preserves_internal_whitespace(bridge_module):
    message = "alpha beta\n\ngamma delta  epsilon"

    chunks = bridge_module.split_text_chunks(message, max_chars=12)

    assert all(0 < len(chunk) <= 12 for chunk in chunks)
    assert "".join(chunks) == message


@pytest.mark.asyncio
async def test_respond_info_caches_final_reply_when_ack_times_out(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.register_reply_session(bot, "req-1", sess)

    async def fake_send_ws_payload_with_ack(_bot, _payload, _timeout_sec, **_kwargs):
        raise bridge_module.BridgeError(504, "websocket ack timeout")

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    await bridge_module.respond_info(bot, "req-1", "done")

    assert sess.pending_final_payload is not None
    assert sess.pending_final_payload["body"]["stream"]["content"] == "done"
    assert "req-1" in bot.reply_sessions


@pytest.mark.asyncio
async def test_ack_only_message_does_not_retry_uncertain_delivery_before_backoff(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    sent_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **_kwargs):
        sent_payloads.append(payload)
        if len(sent_payloads) == 1:
            raise bridge_module.BridgeError(504, "websocket ack timeout")
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    payload = bridge_module.build_proactive_chat_payload("single:test-user", "final answer")

    delivered = await bridge_module.send_or_store_session_payload(bot, "single:test-user", sess, payload, True)

    assert delivered is False
    assert len(sent_payloads) == 1

    await bridge_module.handle_wecom_message(
        bot,
        {"headers": {"req_id": "unrelated-ack"}, "errcode": 0, "errmsg": "ok"},
    )

    assert len(sent_payloads) == 1
    assert sess.pending_final_payload is not None

    sess.pending_payload_retry_after = bridge_module.time.monotonic() - 1
    await bridge_module.handle_wecom_message(
        bot,
        {"headers": {"req_id": "another-ack"}, "errcode": 0, "errmsg": "ok"},
    )

    assert len(sent_payloads) == 2
    assert sess.pending_final_payload is None


@pytest.mark.asyncio
async def test_late_ack_clears_same_cached_payload_after_req_id_refresh(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    old_payload = bridge_module.build_proactive_chat_payload("single:test-user", "final answer")
    current_payload = bridge_module.clone_payload_sequence([old_payload])[0]
    current_payload["headers"] = {**current_payload["headers"], "req_id": "refreshed-req-id"}
    sess.pending_final_payload = dict(current_payload)
    sess.pending_final_payloads = [dict(current_payload)]
    sess.late_ack_payloads = [dict(old_payload)]
    sess.pending_payload_retry_after = (
        bridge_module.time.monotonic() + bridge_module.UNCERTAIN_DELIVERY_RETRY_SEC
    )
    sent_payloads = []

    async def unexpected_send(*_args, **_kwargs):
        sent_payloads.append(True)
        raise AssertionError("late ack should clear the cached payload without resending it")

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", unexpected_send)

    await bridge_module.handle_wecom_message(
        bot,
        {"headers": {"req_id": bridge_module.payload_req_id(old_payload)}, "errcode": 0, "errmsg": "ok"},
    )

    assert sent_payloads == []
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None
    assert sess.pending_payload_retry_after == 0


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
    assert sess.late_ack_payloads == [sess.pending_stream_payload]


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
    assert sess.late_ack_payloads == []


@pytest.mark.asyncio
async def test_send_session_status_keeps_one_updatable_preview(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sent_payloads = []
    bot.ws = make_acking_ws(bot, sent_payloads.append)

    delivered = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "x" * 8000)

    assert delivered is True
    assert len(sent_payloads) == 1
    stream = sent_payloads[0]["body"]["stream"]
    assert stream["finish"] is False
    assert stream["id"] == sess.session_id
    assert len(stream["content"]) <= bridge_module.STREAM_TEXT_MAX_CHARS
    assert stream["content"].startswith("x")
    assert "完整内容将在最终回复中显示" in stream["content"]


@pytest.mark.asyncio
async def test_final_reply_preserves_only_unsent_tail_after_partial_failure(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sent_payloads = []
    expected = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * 8000, True)
    bot.ws = make_flaky_acking_ws(bot, fail_on_send_number=2, on_send=sent_payloads.append)

    delivered = await bridge_module.send_or_store_session_payloads(bot, "single:test-user", sess, expected, True)

    assert delivered is False
    assert sent_payloads == [expected[0]]
    assert sess.pending_final_payloads == expected[1:]
    assert sess.pending_final_payload == expected[-1]


@pytest.mark.asyncio
async def test_send_or_store_session_payloads_cache_multi_chunk_final_reply(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * 8000, True)

    delivered = await bridge_module.send_or_store_session_payloads(bot, "single:test-user", sess, payloads, True)

    assert delivered is False
    assert sess.pending_final_payload is not None
    assert sess.pending_final_payloads is not None
    assert len(sess.pending_final_payloads) == 3
    assert sess.pending_final_payloads[-1]["body"]["stream"]["finish"] is True


@pytest.mark.asyncio
async def test_send_or_store_session_payloads_preserves_only_unsent_tail_after_partial_failure(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * 8000, True)
    sent = []
    bot.ws = make_flaky_acking_ws(bot, fail_on_send_number=2, on_send=sent.append)

    delivered = await bridge_module.send_or_store_session_payloads(bot, "single:test-user", sess, payloads, True)

    assert delivered is False
    assert sent == [payloads[0]]
    assert sess.pending_final_payloads == payloads[1:]
    assert sess.pending_final_payload == payloads[-1]


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


def test_session_compat_history_snapshot_excludes_current_and_queued_requests(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    key = "single:test-user"
    sess.chat = [
        {"role": "user", "text": "first request", "from": key},
        {"role": "user", "text": "current request", "from": key},
        {"role": "user", "text": "future request", "from": key},
        {"role": "bot", "text": "first answer"},
    ]

    history = bridge_module.session_compat_history_snapshot(
        sess,
        key,
        "current request",
        [{"text": "future request", "reqId": "req-future"}],
    )
    prompt = bridge_module.build_compat_history_prompt(
        bot,
        sess,
        "User request:\ncurrent request",
        compat_history=history,
    )

    assert [(item["role"], item["text"]) for item in history] == [
        ("user", "first request"),
        ("bot", "first answer"),
    ]
    assert prompt.count("current request") == 1
    assert "future request" not in prompt


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

    def fake_process_queue(_bot, _sess, key):
        started.append(("queue", key))

    bot.ws = make_acking_ws(bot, lambda payload: started.append(("sent", payload["body"]["stream"]["content"])))
    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    await bridge_module.flush_all_pending_session_payloads(bot)

    assert started == [("sent", "final"), ("queue", "single:test-user")]
    assert sess.pending_final_payload is None


@pytest.mark.asyncio
async def test_flush_cached_multi_chunk_final_reply_resumes_queue(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.pending_final_payloads = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * 8000, True)
    sess.pending_final_payload = dict(sess.pending_final_payloads[-1])
    sess.queue.append({"text": "next", "reqId": "req-2"})
    started = []

    def fake_process_queue(_bot, _sess, key):
        started.append(("queue", key))

    bot.ws = make_acking_ws(
        bot,
        lambda payload: started.append(("sent", payload["body"]["stream"]["finish"], payload["body"]["stream"]["content"])),
    )
    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    await bridge_module.flush_all_pending_session_payloads(bot)

    assert len([item for item in started if item[0] == "sent"]) == 3
    assert started[-1] == ("queue", "single:test-user")
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None


@pytest.mark.asyncio
async def test_handle_wecom_ack_only_message_flushes_cached_final_reply(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * 8000, True)
    sent = []
    bot.ws = make_acking_ws(bot, sent.append)
    sess.pending_final_payloads = payloads[1:]
    sess.pending_final_payload = dict(payloads[-1])

    await bridge_module.handle_wecom_message(
        bot,
        {
            "headers": {"req_id": "ack-1"},
            "errcode": 0,
            "errmsg": "ok",
        },
    )

    assert sent == payloads[1:]
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None


@pytest.mark.asyncio
async def test_handle_wecom_ack_only_message_flushes_cached_final_reply_for_string_zero_errcode(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * 8000, True)
    sent = []
    bot.ws = make_acking_ws(bot, sent.append)
    sess.pending_final_payloads = payloads[1:]
    sess.pending_final_payload = dict(payloads[-1])

    await bridge_module.handle_wecom_message(
        bot,
        {
            "headers": {"req_id": "ack-1"},
            "errcode": "0",
            "errmsg": "ok",
        },
    )

    assert sent == payloads[1:]
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None


@pytest.mark.asyncio
async def test_handle_wecom_ack_only_message_accepts_late_single_cached_final_reply_and_resumes_queue(
    bridge_module, monkeypatch
):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    req_id = "req-1"
    payload = bridge_module.build_session_text_payload("single:test-user", sess, req_id, "Received image. You can continue asking.", True)
    assert payload is not None
    bridge_module.register_reply_session(bot, req_id, sess)
    sess.pending_final_payload = dict(payload)
    sess.pending_final_payloads = [dict(payload)]
    sess.late_ack_payloads = [dict(payload)]
    sess.queue.append({"text": "next", "reqId": "req-2"})
    started = []

    def fake_process_queue(_bot, _sess, key):
        started.append(key)

    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "headers": {"req_id": req_id},
            "errcode": 0,
            "errmsg": "ok",
        },
    )

    assert started == ["single:test-user"]
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None
    assert req_id not in bot.reply_sessions


@pytest.mark.asyncio
async def test_handle_wecom_late_ack_consumes_first_cached_final_fragment_before_flushing_tail(
    bridge_module, monkeypatch
):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    req_id = "req-1"
    payloads = bridge_module.build_session_text_payloads("single:test-user", sess, req_id, "x" * 7001, True)
    bridge_module.register_reply_session(bot, req_id, sess)
    sess.pending_final_payloads = bridge_module.clone_payload_sequence(payloads)
    sess.pending_final_payload = dict(payloads[-1])
    sess.late_ack_payloads = [dict(payloads[0])]
    sess.queue.append({"text": "next", "reqId": "req-2"})
    sent = []
    started = []
    bot.ws = make_acking_ws(bot, sent.append)

    def fake_process_queue(_bot, _sess, key):
        started.append(key)

    monkeypatch.setattr(bridge_module, "process_queue", fake_process_queue)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "headers": {"req_id": req_id},
            "errcode": 0,
            "errmsg": "ok",
        },
    )

    assert sent == payloads[1:]
    assert started == ["single:test-user"]
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None
    assert req_id not in bot.reply_sessions


@pytest.mark.asyncio
async def test_handle_wecom_late_ack_for_superseded_payload_keeps_current_cache(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    req_id = "req-1"
    old_payload = bridge_module.build_session_text_payload("single:test-user", sess, req_id, "old", False)
    current_payload = bridge_module.build_session_text_payload("single:test-user", sess, req_id, "current", True)
    assert old_payload is not None
    assert current_payload is not None
    sess.late_ack_payloads = [dict(old_payload)]
    sess.pending_final_payload = dict(current_payload)
    sess.pending_final_payloads = [dict(current_payload)]
    sent = []
    bot.ws = make_acking_ws(bot, sent.append)

    await bridge_module.handle_wecom_message(
        bot,
        {
            "headers": {"req_id": req_id},
            "errcode": 0,
            "errmsg": "ok",
        },
    )

    assert sent == [current_payload]
    assert sess.pending_final_payload is None
    assert sess.late_ack_payloads == []


@pytest.mark.asyncio
async def test_flush_cached_final_reply_does_not_reenter_on_chunk_ack(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * 8000, True)
    sess.pending_final_payloads = payloads
    sess.pending_final_payload = dict(payloads[-1])
    sent = []

    class ReentrantAckWS:
        closed = False

        async def send_json(self, payload):
            sent.append(payload)
            await bridge_module.handle_wecom_message(
                bot,
                {
                    "headers": {"req_id": payload["headers"]["req_id"]},
                    "errcode": 0,
                    "errmsg": "ok",
                },
            )

    bot.ws = ReentrantAckWS()

    await bridge_module.flush_all_pending_session_payloads(bot)

    assert sent == payloads
    assert sess.pending_final_payload is None
    assert sess.pending_final_payloads is None


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
    assert "req-1" not in bot.reply_sessions


@pytest.mark.asyncio
async def test_final_reply_after_uncertain_stream_delivery_uses_new_proactive_request(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    req_id = "req-1"
    bridge_module.register_reply_session(bot, req_id, sess)
    uncertain_payload = bridge_module.build_session_text_payload(
        "single:test-user",
        sess,
        req_id,
        "uncertain status",
        False,
    )
    final_payload = bridge_module.build_session_text_payload(
        "single:test-user",
        sess,
        req_id,
        "final answer",
        True,
    )
    assert uncertain_payload is not None
    assert final_payload is not None
    sess.pending_stream_payload = dict(uncertain_payload)
    sess.pending_stream_payloads = [dict(uncertain_payload)]
    sess.late_ack_payloads = [dict(uncertain_payload)]
    sent = []
    bot.ws = make_acking_ws(bot, sent.append)

    delivered = await bridge_module.send_or_store_session_payload(
        bot,
        "single:test-user",
        sess,
        final_payload,
        True,
    )

    assert delivered is True
    assert len(sent) == 1
    assert sent[0]["cmd"] == "aibot_send_msg"
    assert sent[0]["headers"]["req_id"] != req_id
    assert sent[0]["body"]["markdown"]["content"] == "final answer"
    assert sess.pending_stream_payload is None
    assert sess.late_ack_payloads == []
    assert req_id not in bot.reply_sessions


@pytest.mark.asyncio
async def test_stream_status_closes_reply_when_max_age_reached(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    payloads = []
    bot.ws = make_acking_ws(bot, payloads.append)
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
async def test_send_session_status_waits_for_heartbeat_interval_after_closing_stream(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    stream_payloads = []
    proactive_payloads = []

    async def fake_send_ws_payload_with_ack(_bot, payload, _timeout_sec, **kwargs):
        if payload["cmd"] == "aibot_respond_msg":
            stream_payloads.append(payload)
        else:
            proactive_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    bot.ws = SimpleNamespace(closed=False)
    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", fake_send_ws_payload_with_ack)
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_started_at["req-1"] = time.time() - bridge_module.REPLY_MAX_AGE_FALLBACK_SEC - 1

    delivered = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status")

    assert delivered is False
    assert stream_payloads[0]["cmd"] == "aibot_respond_msg"
    assert stream_payloads[0]["body"]["stream"]["finish"] is True
    assert "后台运行" in stream_payloads[0]["body"]["stream"]["content"]
    assert proactive_payloads == []


@pytest.mark.asyncio
async def test_send_session_status_retries_heartbeat_immediately_when_stream_close_fails(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    proactive_payloads = []

    bot.ws = make_flaky_acking_ws(
        bot, fail_on_send_number=1, on_send=proactive_payloads.append,
        error=bridge_module.BridgeError(503, "websocket closed"),
    )
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_started_at["req-1"] = time.time() - bridge_module.REPLY_MAX_AGE_FALLBACK_SEC - 1

    delivered = await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status")

    assert delivered is True
    assert len(proactive_payloads) == 1
    assert proactive_payloads[0]["cmd"] == "aibot_send_msg"
    assert proactive_payloads[0]["body"]["markdown"]["content"] == "status"
    assert await bridge_module.send_session_status(bot, "single:test-user", sess, "req-1", "status") is False
    assert len(proactive_payloads) == 1


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
async def test_interrupt_stream_close_is_cached_when_websocket_is_unavailable(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bridge_module.register_reply_session(bot, "req-running", sess)

    await bridge_module.finish_interrupted_reply_streams(
        bot, "single:test-user", sess, ["req-running"]
    )

    assert sess.pending_final_payload is not None
    assert sess.pending_final_payload["body"]["stream"] == {
        "id": sess.session_id, "finish": True, "content": "任务已中断。"
    }


@pytest.mark.asyncio
async def test_interrupt_command_discards_provisional_thread_from_fresh_run(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.queue.append({"text": "long-running request", "reqId": "req-run"})
    process_started = asyncio.Event()

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stdout.feed_data(
                (json.dumps({"type": "thread.started", "thread_id": "provisional-thread"}) + "\n").encode("utf-8")
            )
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            process_started.set()
            while self.returncode is None:
                await asyncio.sleep(0)
            return self.returncode

        def kill(self):
            self.returncode = -9

    async def fake_create_subprocess_exec(*args, **kwargs):
        return FakeProcess()

    async def fake_send_session_status(*args, **kwargs):
        return True

    async def fake_respond_info(*args, **kwargs):
        return None

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "respond_info", fake_respond_info)

    bridge_module.process_queue(bot, sess, "single:test-user")
    run_task = sess.run_task
    assert run_task is not None
    await process_started.wait()
    for _ in range(100):
        if sess.thread_id == "provisional-thread":
            break
        await asyncio.sleep(0)
    assert sess.thread_id == "provisional-thread"

    await bridge_module.interrupt_session_command(bot, "single:test-user", "req-interrupt")
    with contextlib.suppress(asyncio.CancelledError):
        await run_task

    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert sess.thread_id is None
    assert sess.active_run_thread_checkpoint_set is False
    assert persisted.get("threadId") is None


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
    to_thread_calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        to_thread_calls.append(func.__name__)
        return func(*args, **kwargs)

    monkeypatch.setattr(bridge_module, "run_blocking", fake_run_blocking)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", [])

    assert payloads[-1] == (True, "final answer")
    assert payloads[0] == (False, "**运行状态：启动中**")
    assert payloads[1] == (False, "**运行状态：分析中**")
    assert str(captured["cwd"]) == str(bridge_module.get_workfile_dir(bot.config["id"], "test-user"))
    assert captured["args"][-1] == "-"
    assert bytes(captured["stdin"].buffer).decode("utf-8") == "prompt"
    assert captured["stdin"].closed is True
    assert captured["env"]["CODEX_HOME"].startswith(str(bridge_module.get_bridge_codex_home_root()))
    chatfile_dir = str(bridge_module.ensure_session_workspace_dirs(bot, "single:test-user")["chatfile"])
    workfile_dir = str(bridge_module.get_workfile_dir(bot.config["id"], "test-user"))
    assert "ensure_session_workspace_dirs" in to_thread_calls
    assert "build_codex_home_for_subprocess" in to_thread_calls
    assert captured["env"]["WECOM_BRIDGE_BOT_NAME"] == bot.config["name"]
    assert captured["env"]["WECOM_BRIDGE_BOT_CONFIG_ID"] == bot.config["id"]
    assert captured["env"]["WECOM_BRIDGE_CWD_DIR"] == workfile_dir
    assert captured["env"]["WECOM_BRIDGE_WORKSPACE_MODE"] == "team"
    assert captured["env"]["WECOM_BRIDGE_WORKSPACE_SKILL_DIR"] == str(Path(workfile_dir) / ".codex" / "skills")
    assert captured["env"]["WECOM_BRIDGE_CHATFILE_DIR"] == chatfile_dir
    assert captured["env"]["WECOM_BRIDGE_EXPORT_DIR"] == chatfile_dir
    assert captured["env"]["TMPDIR"] == chatfile_dir
    assert captured["env"]["TMP"] == chatfile_dir
    assert captured["env"]["TEMP"] == chatfile_dir


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["codex", "claude"])
async def test_run_codex_refreshes_only_changed_progress_before_stream_expiry(bridge_module, monkeypatch, backend):
    bot = make_bot(bridge_module)
    bot.config["agentBackend"] = backend
    sess = make_session(bridge_module, bot)
    sess.running = True
    ticks = asyncio.Queue()
    finish = asyncio.Event()
    statuses = []
    finals = []
    real_sleep = asyncio.sleep
    now = bridge_module.time.monotonic()

    class Clock:
        def __getattr__(self, name):
            return getattr(time, name)

        def monotonic(self):
            return now

    async def controlled_sleep(seconds):
        nonlocal now
        if seconds != bridge_module.STATUS_STREAM_INTERVAL_SEC:
            await real_sleep(seconds)
            return
        gate = asyncio.get_running_loop().create_future()
        ticks.put_nowait(gate)
        await gate
        now += seconds

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stderr.feed_eof()

        def feed(self, text):
            event = (
                {"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}}
                if backend == "claude"
                else {"type": "item.completed", "item": {"type": "agent_message", "text": text}}
            )
            self.stdout.feed_data((json.dumps(event) + "\n").encode())

        def feed_event(self, event):
            self.stdout.feed_data((json.dumps(event) + "\n").encode())

        async def wait(self):
            await finish.wait()
            self.feed("final answer")
            self.stdout.feed_eof()
            self.returncode = 0
            return 0

    process = FakeProcess()

    async def create_process(*args, **kwargs):
        return process

    async def send_status(_bot, _key, _sess, _req, content):
        statuses.append(content)
        return True

    async def send_final(_bot, _key, _sess, payload, final):
        finals.append(payload)
        return True

    monkeypatch.setattr(bridge_module, "time", Clock())
    monkeypatch.setattr(bridge_module.asyncio, "sleep", controlled_sleep)
    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(bridge_module, "send_session_status", send_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", send_final)
    monkeypatch.setattr(bridge_module, "reply_age_too_long", lambda *_args: False)
    run = asyncio.create_task(bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", []))

    async def next_tick():
        return await asyncio.wait_for(ticks.get(), timeout=3)

    try:
        gate = await next_tick()
        initial_statuses = list(statuses)
        for _ in range(3):
            gate.set_result(None)
            gate = await next_tick()
        assert statuses == initial_statuses
        summary_event = {
            "type": "item.reasoning.summaryPartAdded",
            "summary": "正在核对配置并整理结果",
        } if backend == "codex" else {
            "type": "assistant", "message": {"content": [{"type": "text", "text": "正在核对配置并整理结果"}]},
        }
        process.feed_event(summary_event)
        gate.set_result(None)
        gate = await next_tick()
        stage = "分析中" if backend == "codex" else "整理中"
        assert statuses[-1] == f"**运行状态：{stage}**\n\n正在核对配置并整理结果"
        for _ in range(3):
            gate.set_result(None)
            gate = await next_tick()
        assert statuses.count(f"**运行状态：{stage}**\n\n正在核对配置并整理结果") == 1

        tool_event = {
            "type": "item.started",
            "item": {"type": "command_execution", "command": "pytest tests/test_api.py", "aggregated_output": "private output"},
        } if backend == "codex" else {
            "type": "assistant", "message": {"content": [{"type": "tool_use", "name": "Bash", "input": {"command": "pytest tests/test_api.py"}}]},
        }
        process.feed_event(tool_event)
        gate.set_result(None)
        gate = await next_tick()
        stage = "执行中" if backend == "codex" else "处理中"
        assert statuses[-1] == f"**运行状态：{stage}**\n\n正在核对配置并整理结果"
        for _ in range(2):
            process.feed_event(tool_event)
            gate.set_result(None)
            gate = await next_tick()
        assert statuses.count(f"**运行状态：{stage}**\n\n正在核对配置并整理结果") == 1

        updates = (
            ("checking logs", "checking logs"),
            ("checking logs", "checking logs"),
            ("checking logs  \n", "checking logs"),
            ("collecting results", "collecting results"),
            ("long summary " + "x" * 4000, "long summary"),
            ("long summary " + "x" * 4000 + "new hidden tail", "long summary"),
        )
        for text, visible_text in updates:
            process.feed(text)
            gate.set_result(None)
            gate = await next_tick()
            assert sum(visible_text in status for status in statuses) == 1
        assert not any("已运行" in status for status in statuses)
        assert not any("分析：" in status or "Process requested" in status or "pytest tests/test_api.py" in status or "private output" in status for status in statuses)
    finally:
        finish.set()
        await asyncio.wait_for(run, timeout=3)

    assert finals[-1]["body"]["stream"]["content"] == "final answer"
    assert finals[-1]["body"]["stream"]["finish"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["codex", "claude"])
@pytest.mark.parametrize("progress_enabled", [False, True])
@pytest.mark.parametrize("stop_action", ["complete", "interrupt"])
async def test_run_codex_sends_fixed_heartbeats_after_stream_expiry(
    bridge_module, monkeypatch, backend, progress_enabled, stop_action
):
    bot = make_bot(bridge_module)
    bot.config["agentBackend"] = backend
    sess = make_session(bridge_module, bot)
    sess.running = True
    ticks = asyncio.Queue()
    started = asyncio.Event()
    finish = asyncio.Event()
    sent = []
    real_sleep = asyncio.sleep
    now = time.time()
    reply_started_at = now

    class Clock:
        def __getattr__(self, name):
            return getattr(time, name)

        def time(self):
            return now

    async def controlled_sleep(seconds):
        if seconds != bridge_module.STATUS_STREAM_INTERVAL_SEC:
            await real_sleep(seconds)
            return
        gate = asyncio.get_running_loop().create_future()
        ticks.put_nowait(gate)
        await gate

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stderr.feed_eof()

        def feed(self, text):
            event = (
                {"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}}
                if backend == "claude"
                else {"type": "item.completed", "item": {"type": "agent_message", "text": text}}
            )
            self.stdout.feed_data((json.dumps(event) + "\n").encode())

        async def wait(self):
            started.set()
            await finish.wait()
            if self.returncode is None:
                self.feed("final answer")
                self.returncode = 0
            self.stdout.feed_eof()
            return self.returncode

        def kill(self):
            self.returncode = -9
            finish.set()

    process = FakeProcess()

    async def create_process(*args, **kwargs):
        return process

    monkeypatch.setattr(bridge_module, "time", Clock())
    monkeypatch.setattr(bridge_module, "STATUS_STREAM_PROGRESS_ENABLED", progress_enabled)
    monkeypatch.setattr(bridge_module.asyncio, "sleep", controlled_sleep)
    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", create_process)
    bot.ws = make_acking_ws(bot, lambda payload: sent.append((now, payload)))
    bridge_module.register_reply_session(bot, "req-1", sess)
    run = asyncio.create_task(bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", []))

    async def next_tick():
        return await asyncio.wait_for(ticks.get(), timeout=3)

    async def tick_at(timestamp):
        nonlocal now, gate
        now = timestamp
        gate.set_result(None)
        gate = await next_tick()

    def proactive_messages():
        return [(timestamp, payload) for timestamp, payload in sent if payload["cmd"] == "aibot_send_msg"]

    try:
        await asyncio.wait_for(started.wait(), timeout=3)
        gate = await next_tick()
        initial_count = len(sent)
        process.feed("正在检查日志")
        await real_sleep(0)
        await tick_at(reply_started_at + 5)
        assert len(sent) == initial_count + int(progress_enabled)

        expires_at = reply_started_at + bridge_module.REPLY_MAX_AGE_FALLBACK_SEC
        await tick_at(expires_at - 5)
        assert proactive_messages() == []
        assert not sent[-1][1]["body"]["stream"]["finish"]

        await tick_at(expires_at)
        assert proactive_messages() == []
        close_notice = sent[-1][1]["body"]["stream"]
        assert close_notice["finish"] is True
        assert "每 120 秒" in close_notice["content"]

        for heartbeat_number in (1, 2):
            due_at = expires_at + heartbeat_number * 120
            await tick_at(due_at - 5)
            assert len(proactive_messages()) == heartbeat_number - 1
            await tick_at(due_at)
            assert len(proactive_messages()) == heartbeat_number
        heartbeats = proactive_messages()
        assert [timestamp for timestamp, _payload in heartbeats] == [expires_at + 120, expires_at + 240]
        assert heartbeats[0][1]["body"]["markdown"]["content"] == heartbeats[1][1]["body"]["markdown"]["content"]
        if progress_enabled:
            assert "正在检查日志" in heartbeats[0][1]["body"]["markdown"]["content"]
        else:
            assert "后台运行中" in heartbeats[0][1]["body"]["markdown"]["content"]
            assert "正在检查日志" not in heartbeats[0][1]["body"]["markdown"]["content"]

        if stop_action == "interrupt":
            sess.interrupt_requested = True
            run.cancel()
        else:
            finish.set()
        await asyncio.wait_for(run, timeout=3)
        assert sess.running is False
        assert gate.cancelled()
        if stop_action == "complete":
            assert proactive_messages()[-1][1]["body"]["markdown"]["content"] == "final answer"
        sent_count = len(sent)
        now += 120
        await real_sleep(0)
        assert len(sent) == sent_count
    finally:
        finish.set()
        await asyncio.wait_for(run, timeout=3)


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
    assert str(captured["cwd"]) == expected_workfile_dir
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
    assert str(captured["cwd"]) == expected_roomfile_dir
    assert captured["env"]["WECOM_BRIDGE_CWD_DIR"] == expected_roomfile_dir


@pytest.mark.asyncio
async def test_run_codex_uses_workdir_as_cwd_for_personal_workspace_mode(bridge_module, monkeypatch):
    bot = make_bot(bridge_module, work_dir=str(bridge_module.BASE_DIR / "repo-personal-run"))
    Path(bot.config["workDir"]).mkdir(parents=True, exist_ok=True)
    bot.config["workspaceMode"] = "personal"
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

    expected_work_dir = str(Path(bot.config["workDir"]).resolve())
    assert str(captured["cwd"]) == expected_work_dir
    assert captured["env"]["WECOM_BRIDGE_CWD_DIR"] == expected_work_dir
    assert captured["env"]["WECOM_BRIDGE_WORKSPACE_MODE"] == "personal"
    assert captured["env"]["WECOM_BRIDGE_WORKSPACE_SKILL_DIR"] == str(Path(expected_work_dir) / ".codex" / "skills")


def test_build_codex_home_for_subprocess_preserves_global_skills(bridge_module):
    global_skill = bridge_module.DEFAULT_CODEX_HOME / "skills" / "global-skill"
    global_skill.mkdir(parents=True, exist_ok=True)
    (global_skill / "SKILL.md").write_text("---\nname: global-skill\n---\nglobal\n", encoding="utf-8")
    extra_state = bridge_module.DEFAULT_CODEX_HOME / "installation_id"
    extra_state.write_text("install-1", encoding="utf-8")
    noisy_state = bridge_module.DEFAULT_CODEX_HOME / "logs_2.sqlite"
    noisy_state.write_text("ignore-me", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-1")

    assert (codex_home / "skills" / "global-skill" / "SKILL.md").is_file()
    assert (codex_home / "skills").is_symlink()
    assert (codex_home / "skills").resolve() == (bridge_module.DEFAULT_CODEX_HOME / "skills").resolve()
    assert (codex_home / "installation_id").read_text(encoding="utf-8") == "install-1"
    assert (codex_home / "logs_2.sqlite").exists() is False


def test_build_codex_home_for_subprocess_replaces_stale_session_skills_dir_with_symlink(bridge_module):
    global_skill = bridge_module.DEFAULT_CODEX_HOME / "skills" / "global-skill"
    global_skill.mkdir(parents=True, exist_ok=True)
    (global_skill / "SKILL.md").write_text("global", encoding="utf-8")
    session_home = bridge_module.get_session_codex_home_root("sess-stale")
    stale_skill = session_home / "skills" / "stale-skill"
    stale_skill.mkdir(parents=True, exist_ok=True)
    (stale_skill / "SKILL.md").write_text("stale", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-stale")

    assert (codex_home / "skills").is_symlink()
    assert not (codex_home / "skills" / "stale-skill").exists()
    assert (codex_home / "skills" / "global-skill" / "SKILL.md").is_file()


def test_build_codex_home_for_subprocess_skips_volatile_tmp_arg0_runtime(bridge_module):
    arg0_dir = bridge_module.DEFAULT_CODEX_HOME / "tmp" / "arg0" / "codex-arg0deadbeef"
    arg0_dir.mkdir(parents=True, exist_ok=True)
    (arg0_dir / ".lock").write_text("", encoding="utf-8")
    (bridge_module.DEFAULT_CODEX_HOME / "installation_id").write_text("install-1", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-volatile")

    assert (codex_home / "installation_id").read_text(encoding="utf-8") == "install-1"
    assert (codex_home / "tmp").is_dir()
    assert not (codex_home / "tmp" / "arg0").exists()


def test_build_codex_home_for_subprocess_links_global_sessions_when_requested(bridge_module):
    global_sessions_root = bridge_module.DEFAULT_CODEX_HOME / "sessions"
    global_sessions_root.mkdir(parents=True, exist_ok=True)
    marker = global_sessions_root / "marker.txt"
    marker.write_text("shared", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-global-sessions", use_global_sessions=True)

    assert (codex_home / "sessions").is_symlink()
    assert (codex_home / "sessions").resolve() == global_sessions_root.resolve()
    assert (codex_home / "sessions" / "marker.txt").read_text(encoding="utf-8") == "shared"


def test_create_agent_output_file_uses_private_exclusive_random_file(bridge_module, monkeypatch, tmp_path):
    monkeypatch.setattr(bridge_module.tempfile, "tempdir", str(tmp_path))
    victim = tmp_path / "victim.txt"
    victim.write_text("keep-content", encoding="utf-8")
    predictable = tmp_path / "wecom-codex-py-session-123456.txt"
    predictable.symlink_to(victim)

    first = bridge_module.create_agent_output_file("session")
    second = bridge_module.create_agent_output_file("session")

    try:
        assert first.parent == tmp_path
        assert second.parent == tmp_path
        assert first != second
        assert first != predictable
        assert first.is_file() and not first.is_symlink()
        assert first.stat().st_mode & 0o777 == 0o600
        assert victim.read_text(encoding="utf-8") == "keep-content"
    finally:
        first.unlink(missing_ok=True)
        second.unlink(missing_ok=True)


def test_build_codex_home_for_subprocess_keeps_global_sessions_link_on_repeated_prepare(bridge_module):
    global_sessions_root = bridge_module.DEFAULT_CODEX_HOME / "sessions"
    global_sessions_root.mkdir(parents=True, exist_ok=True)
    marker = global_sessions_root / "marker.txt"
    marker.write_text("shared", encoding="utf-8")
    session_id = "sess-global-sessions-repeated"
    local_sessions_root = bridge_module.get_session_codex_home_root(session_id) / "sessions"
    local_sessions_root.mkdir(parents=True, exist_ok=True)
    (local_sessions_root / "local-rollout.jsonl").write_text("local", encoding="utf-8")

    first = bridge_module.build_codex_home_for_subprocess(session_id, use_global_sessions=True)
    second = bridge_module.build_codex_home_for_subprocess(session_id, use_global_sessions=True)

    assert first == second
    assert (second / "sessions").is_symlink()
    assert (second / "sessions").resolve() == global_sessions_root.resolve()
    assert (second / "sessions" / "marker.txt").read_text(encoding="utf-8") == "shared"
    assert (
        bridge_module.get_session_codex_home_local_sessions_backup_root(session_id) / "local-rollout.jsonl"
    ).read_text(encoding="utf-8") == "local"


def test_build_codex_home_for_subprocess_restores_local_sessions_after_global_mode(bridge_module):
    global_sessions_root = bridge_module.DEFAULT_CODEX_HOME / "sessions"
    global_sessions_root.mkdir(parents=True, exist_ok=True)
    (global_sessions_root / "global-marker.txt").write_text("global", encoding="utf-8")
    session_id = "sess-restore-local-sessions"
    session_home = bridge_module.get_session_codex_home_root(session_id)
    local_sessions_root = session_home / "sessions"
    bridge_module.ensure_dir(local_sessions_root)
    (local_sessions_root / "local-rollout.jsonl").write_text("local", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess(session_id, use_global_sessions=True)

    assert (codex_home / "sessions").is_symlink()
    assert (bridge_module.get_session_codex_home_local_sessions_backup_root(session_id) / "local-rollout.jsonl").read_text(
        encoding="utf-8"
    ) == "local"

    restored = bridge_module.build_codex_home_for_subprocess(session_id, use_global_sessions=False)

    assert restored == codex_home
    assert (restored / "sessions").is_dir()
    assert not (restored / "sessions").is_symlink()
    assert (restored / "sessions" / "local-rollout.jsonl").read_text(encoding="utf-8") == "local"
    assert bridge_module.get_session_codex_home_local_sessions_backup_root(session_id).exists() is False


def test_build_codex_home_for_subprocess_skips_same_file_copy_when_base_home_is_session_home(bridge_module):
    session_id = "sess-same-home"
    session_home = bridge_module.get_session_codex_home_root(session_id)
    bridge_module.ensure_dir(session_home)
    (session_home / "config.toml").write_text("model = 'x'\n", encoding="utf-8")

    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = session_home
    try:
        result = bridge_module.build_codex_home_for_subprocess(session_id)
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home

    assert result == session_home
    assert (session_home / "config.toml").read_text(encoding="utf-8") == "model = 'x'\n"


def test_build_codex_home_for_subprocess_avoids_self_linking_sessions_when_base_home_is_session_home(bridge_module):
    session_id = "sess-same-home-global"
    session_home = bridge_module.get_session_codex_home_root(session_id)
    bridge_module.ensure_dir(session_home / "sessions")

    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = session_home
    try:
        result = bridge_module.build_codex_home_for_subprocess(session_id, use_global_sessions=True)
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home

    assert result == session_home
    assert (session_home / "sessions").is_dir()
    assert not (session_home / "sessions").is_symlink()


def test_build_codex_home_for_subprocess_replaces_broken_symlinked_root_state_file(bridge_module):
    (bridge_module.DEFAULT_CODEX_HOME / "installation_id").write_text("install-1", encoding="utf-8")
    session_home = bridge_module.get_session_codex_home_root("sess-broken-root-file")
    bridge_module.ensure_dir(session_home)
    stale = session_home / "installation_id"
    stale.symlink_to(bridge_module.BASE_DIR / "missing-installation-id")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-broken-root-file")

    restored = codex_home / "installation_id"
    assert restored.exists()
    assert not restored.is_symlink()
    assert restored.read_text(encoding="utf-8") == "install-1"


def test_build_codex_home_for_subprocess_replaces_directory_blocking_root_state_file(bridge_module):
    (bridge_module.DEFAULT_CODEX_HOME / "installation_id").write_text("install-1", encoding="utf-8")
    session_home = bridge_module.get_session_codex_home_root("sess-root-dir-block")
    bridge_module.ensure_dir(session_home / "installation_id")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-root-dir-block")

    restored = codex_home / "installation_id"
    assert restored.exists()
    assert not restored.is_dir()
    assert restored.read_text(encoding="utf-8") == "install-1"


def test_build_codex_home_for_subprocess_tolerates_file_default_codex_home(bridge_module):
    file_home = bridge_module.BASE_DIR / "codex-home.txt"
    file_home.write_text("not a dir", encoding="utf-8")
    original_home = bridge_module.DEFAULT_CODEX_HOME
    bridge_module.DEFAULT_CODEX_HOME = file_home
    try:
        codex_home = bridge_module.build_codex_home_for_subprocess("sess-file-home")
    finally:
        bridge_module.DEFAULT_CODEX_HOME = original_home

    assert codex_home.exists()
    assert (codex_home / "skills").is_dir()
    assert not (codex_home / "installation_id").exists()


def test_build_codex_home_for_subprocess_tolerates_file_skills_root(bridge_module):
    bridge_module.ensure_dir(bridge_module.DEFAULT_CODEX_HOME)
    skills_file = bridge_module.DEFAULT_CODEX_HOME / "skills"
    skills_file.write_text("not a dir", encoding="utf-8")

    codex_home = bridge_module.build_codex_home_for_subprocess("sess-file-skills")

    assert (codex_home / "skills").is_dir()
    assert not (codex_home / "skills").is_symlink()


def test_build_codex_base_args_uses_resolved_codex_command(monkeypatch, bridge_module):
    monkeypatch.setattr(bridge_module.shutil, "which", lambda name: "/usr/local/bin/codex" if name == "codex" else None)

    args = bridge_module.build_codex_base_args(Path("/tmp/out.jsonl"), [], resume=False)

    assert args[:2] == ["/usr/local/bin/codex", "exec"]


def test_build_codex_base_args_honors_codex_command_env(monkeypatch, bridge_module):
    monkeypatch.setenv("CODEX_COMMAND", "/opt/codex/bin/codex --profile automation")

    args = bridge_module.build_codex_base_args(Path("/tmp/out.jsonl"), [], resume=True)

    assert args[:4] == ["/opt/codex/bin/codex", "--profile", "automation", "exec"]
    assert args[4] == "resume"


def test_build_codex_base_args_supports_claude_backend(bridge_module):
    args = bridge_module.build_codex_base_args(
        Path("/tmp/out.jsonl"),
        [],
        resume=False,
        backend="claude",
        command="claude --model sonnet",
    )

    assert args[:4] == ["claude", "--model", "sonnet", "-p"]
    assert "--verbose" in args
    assert "--output-format" in args
    assert "stream-json" in args
    assert "--image" not in args


def test_build_codex_base_args_supports_claude_resume(bridge_module):
    args = bridge_module.build_codex_base_args(
        Path("/tmp/out.jsonl"),
        [],
        resume=True,
        backend="claude",
        command="claude --model sonnet",
        resume_thread_id="550e8400-e29b-41d4-a716-446655440000",
    )

    assert args[:4] == ["claude", "--model", "sonnet", "-p"]
    assert "--resume" in args
    assert "550e8400-e29b-41d4-a716-446655440000" in args


@pytest.mark.asyncio
async def test_run_codex_sends_long_final_reply_in_chunks(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sent = []
    captured = {}

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            event = {"type": "item.completed", "item": {"type": "agent_message", "text": "x" * 8000}}
            self.stdout.feed_data((json.dumps(event) + "\n").encode("utf-8"))
            self.stdout.feed_eof()
            self.stderr.feed_eof()

        async def wait(self):
            self.returncode = 0
            return 0

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["env"] = kwargs["env"]
        captured["args"] = list(args)
        return FakeProcess()

    async def fake_send_session_status(*args, **kwargs):
        return True

    async def fake_send_or_store_session_payloads(_bot, _key, _sess, payloads, final):
        for payload in payloads:
            sent.append((payload["body"]["stream"]["finish"], payload["body"]["stream"]["content"]))
        return True

    def fake_prepare_codex_subprocess_env(env, *, command=None):
        updated = dict(env)
        updated["PATH"] = f"/tmp/fake-bwrap{bridge_module.os.pathsep}{env.get('PATH', '')}"
        captured["command"] = command
        return updated

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payloads", fake_send_or_store_session_payloads)
    monkeypatch.setattr(bridge_module.codex_runtime, "prepare_codex_subprocess_env", fake_prepare_codex_subprocess_env)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", [])

    assert len(sent) == 3
    assert sent[-1][0] is True
    assert "".join(chunk for _, chunk in sent) == "x" * 8000
    assert captured["command"] == captured["args"][0]
    assert captured["env"]["PATH"].split(bridge_module.os.pathsep)[0] == "/tmp/fake-bwrap"


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
    assert invocations[0][1:3] == ["exec", "resume"]
    assert invocations[0][0].endswith("codex")
    assert invocations[1][1:2] == ["exec"]
    assert invocations[1][0].endswith("codex")
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
    assert invocations[0][1:3] == ["exec", "resume"]
    assert invocations[0][0].endswith("codex")
    assert invocations[1][1:2] == ["exec"]
    assert invocations[1][0].endswith("codex")
    assert "resume" not in invocations[1]
    assert sess.thread_id == "thread-from-second-exec"
    assert payloads[-1] == (True, "final answer")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resume_error",
    [
        "2026-05-28T10:15:41.542083Z ERROR codex_core::session: failed to record rollout items: "
        "thread 019e6e10-fa64-73c2-9be5-4fdf694161ea not found",
        "Error: thread/resume: thread/resume failed: failed to resolve rollout path "
        "`/tmp/codex-home/sessions/rollout-019e6e10.jsonl`: file does not exist (code -32600)",
    ],
)
async def test_run_codex_retries_fresh_exec_when_resume_state_is_missing(
    bridge_module, monkeypatch, resume_error
):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sess.thread_id = "019e6e10-fa64-73c2-9be5-4fdf694161ea"
    sess.use_global_codex_sessions = True
    sess.chat = [{"role": "user", "text": "older user turn"}, {"role": "bot", "text": "older bot turn"}]
    bridge_module.update_session_record(
        sess.session_id,
        lambda record: {
            **record,
            "threadId": sess.thread_id,
            "useGlobalCodexSessions": True,
            "localResumeBackup": {
                "threadId": "bridge-thread",
                "recentChat": [{"role": "user", "text": "bridge conversation"}],
                "useGlobalCodexSessions": False,
            },
        },
    )
    payloads = []
    invocations = []
    processes = []
    global_session_modes = []

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
        if len(invocations) == 1:
            process = FakeProcess(
                returncode=1,
                stdout_events=[],
                stderr_lines=[resume_error],
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
        processes.append(process)
        return process

    async def fake_send_session_status(_bot, _key, _sess, _req_id, content):
        payloads.append((False, content))
        return True

    async def fake_send_or_store_session_payload(_bot, _key, _sess, payload, final):
        payloads.append((final, payload["body"]["stream"]["content"]))
        return True

    original_build_codex_home = bridge_module.build_codex_home_for_subprocess

    def tracking_build_codex_home(session_id, *, use_global_sessions=False):
        global_session_modes.append(use_global_sessions)
        return original_build_codex_home(session_id, use_global_sessions=use_global_sessions)

    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)
    monkeypatch.setattr(bridge_module, "build_codex_home_for_subprocess", tracking_build_codex_home)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", [])

    assert len(invocations) == 2
    assert invocations[0][1:3] == ["exec", "resume"]
    assert invocations[1][1:2] == ["exec"]
    assert "resume" not in invocations[1]
    second_prompt = bytes(processes[1].stdin.buffer).decode("utf-8")
    assert "[RecentConversation]" in second_prompt
    assert "older user turn" in second_prompt
    assert "older bot turn" in second_prompt
    assert sess.thread_id == "thread-from-second-exec"
    assert sess.use_global_codex_sessions is False
    assert global_session_modes == [True, False]
    persisted = bridge_module.read_session_record_by_id(sess.session_id)
    assert persisted.get("threadId") == "thread-from-second-exec"
    assert persisted.get("useGlobalCodexSessions") is False
    assert persisted.get("localResumeBackup") == {
        "threadId": "bridge-thread",
        "recentChat": [{"role": "user", "text": "bridge conversation"}],
        "useGlobalCodexSessions": False,
    }
    assert payloads[-1] == (
        True,
        "提示：原本地 Codex 会话已不可用，已基于近期聊天创建新会话。\n\nfinal answer",
    )


@pytest.mark.asyncio
async def test_run_codex_retries_fresh_exec_when_resume_thread_not_found_but_exit_zero(
    bridge_module, monkeypatch
):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.running = True
    sess.thread_id = "019e6e10-fa64-73c2-9be5-4fdf694161ea"
    payloads = []
    invocations = []

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
        if len(invocations) == 1:
            return FakeProcess(
                returncode=0,
                stdout_events=[],
                stderr_lines=[
                    "2026-05-28T10:15:41.542083Z ERROR codex_core::session: failed to record rollout items: thread 019e6e10-fa64-73c2-9be5-4fdf694161ea not found"
                ],
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
    assert invocations[0][1:3] == ["exec", "resume"]
    assert invocations[1][1:2] == ["exec"]
    assert "resume" not in invocations[1]
    assert sess.thread_id == "thread-from-second-exec"
    assert payloads[-1] == (True, "final answer")


@pytest.mark.asyncio
async def test_new_then_image_then_text_uses_fresh_exec_with_image_and_stdin_prompt(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    sess.thread_id = "old-thread"

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

    await bridge_module.new_session_command(bot, "single:test-user", None)
    previous = sess
    sess = bot.sessions["single:test-user"]
    assert sess.thread_id is None
    assert sess.session_id != previous.session_id
    assert previous.thread_id == "old-thread"

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

    assert captured["args"][0].endswith("codex")
    assert captured["args"][1] == "exec"
    assert "resume" not in captured["args"]
    assert "-i" in captured["args"]
    assert str(image_path) in captured["args"]
    assert captured["args"][-1] == "-"
    prompt = bytes(captured["stdin"].buffer).decode("utf-8")
    assert "Attachment 1: image file available at" in prompt
    assert "Read the local file directly if image analysis is needed." in prompt
    assert str(image_path) in prompt
    assert "User request:\n分析一下" in prompt
    assert captured["stdin"].closed is True
    assert captured["env"]["WECOM_BRIDGE_CHATFILE_DIR"]
    assert captured["env"]["TMPDIR"] == captured["env"]["WECOM_BRIDGE_CHATFILE_DIR"]
    assert sess.thread_id == "new-thread"


@pytest.mark.asyncio
async def test_run_codex_ignores_legacy_claude_run_as_configuration(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    bot.config["agentBackend"] = "claude"
    bot.config["agentCommand"] = "claude"
    bot.config["agentRunAsUser"] = "nobody"
    bot.config["agentRunAsGroup"] = "nogroup"
    bot.config["agentRuntimeRoot"] = str(bridge_module.BASE_DIR / "claude-runtime")
    sess = make_session(bridge_module, bot)
    sess.running = True
    captured = {}

    async def fake_send_session_status(*args, **kwargs):
        return True

    async def fake_send_or_store_session_payload(*args, **kwargs):
        return True

    class FakeProcess:
        def __init__(self):
            self.returncode = None
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.stdout.feed_data(
                (json.dumps({"type": "system", "subtype": "init", "session_id": "550e8400-e29b-41d4-a716-446655440000"}) + "\n").encode("utf-8")
            )
            self.stdout.feed_data((json.dumps({"type": "result", "result": "done"}) + "\n").encode("utf-8"))
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
        captured["cwd"] = kwargs["cwd"]
        captured["stdin"] = process.stdin
        return process

    monkeypatch.setattr(bridge_module, "send_session_status", fake_send_session_status)
    monkeypatch.setattr(bridge_module, "send_or_store_session_payload", fake_send_or_store_session_payload)
    monkeypatch.setattr(bridge_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

    await bridge_module.run_codex(bot, sess, "single:test-user", "prompt", "req-1", [])

    assert captured["args"][0] == "claude"
    assert "setpriv" not in captured["args"]
    assert str(captured["cwd"]) == str(bridge_module.get_workfile_dir(bot.config["id"], "test-user").resolve())
    assert not (bridge_module.BASE_DIR / "claude-runtime").exists()
    assert captured["env"]["WECOM_BRIDGE_WORKFILE_DIR"] == str(
        bridge_module.get_workfile_dir(bot.config["id"], "test-user").resolve()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [3000, 8000])
async def test_quiet_five_minute_run_finishes_existing_stream(bridge_module, monkeypatch, length):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot)
    bot.ws = SimpleNamespace(closed=False)
    bridge_module.register_reply_session(bot, "req-1", sess)
    sess.reply_started_at["req-1"] = time.time() - 300
    sess.reply_last_sent_at["req-1"] = time.time() - 300
    sent = []

    async def send_with_ack(_bot, payload, _timeout_sec, **kwargs):
        sent.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", send_with_ack)
    payloads = bridge_module.build_session_text_payloads("single:test-user", sess, "req-1", "x" * length, True)
    delivered = await bridge_module.send_or_store_session_payloads(bot, "single:test-user", sess, payloads, True)

    assert delivered is True
    assert all(payload["cmd"] == "aibot_respond_msg" for payload in sent)
    assert all(payload["body"]["stream"]["finish"] is True for payload in sent)
    assert "".join(payload["body"]["stream"]["content"] for payload in sent) == "x" * length


@pytest.mark.asyncio
async def test_wecom_voice_and_quote_are_business_context_not_control(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    received = []

    async def enqueue(_bot, key, content, req_id, **kwargs):
        received.append((key, content, req_id))
        return True

    monkeypatch.setattr(bridge_module, "enqueue_message", enqueue)
    await bridge_module.handle_wecom_message(bot, {
        "cmd": "aibot_msg_callback", "headers": {"req_id": "voice-1"},
        "body": {"msgtype": "voice", "chattype": "single", "from": {"userid": "alice"},
                 "voice": {"content": "重跑这个"}, "quote": {"msgtype": "text", "text": {"content": "/bridge-reset"}}},
    })
    assert len(received) == 1
    assert received[0][0] == "single:alice"
    assert "用户请求：\n重跑这个" in received[0][1]
    assert "引用消息：\n/bridge-reset" in received[0][1]
    assert received[0][2] == "voice-1"


@pytest.mark.asyncio
async def test_wecom_voice_transcription_alias_reaches_business_session(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    received = []

    async def enqueue(_bot, key, content, req_id, **kwargs):
        received.append((key, content, req_id))
        return True

    monkeypatch.setattr(bridge_module, "enqueue_message", enqueue)
    await bridge_module.handle_wecom_message(bot, {
        "cmd": "aibot_msg_callback", "headers": {"req_id": "voice-alias-1"},
        "body": {"msgtype": "voice", "chattype": "single", "from": {"userid": "alice"},
                 "voice": {"recognition": "哈喽"}},
    })

    assert received == [("single:alice", "哈喽", "voice-alias-1")]


@pytest.mark.asyncio
async def test_assistant_parts_invoke_business_once_after_all_inputs(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    calls = []

    async def dispatch(_bot, frames):
        calls.append(frames)

    monkeypatch.setattr(bridge_module, "dispatch_assistant_call", dispatch)
    def frame(part):
        return {"cmd": "aibot_msg_callback", "headers": {"req_id": f"part-{part}"},
                "body": {"msgid": f"message-{part}", "msgtype": "text", "chattype": "group", "chatid": "assistant-room",
                         "from": {"userid": "alice"}, "text": {"content": f"part {part}"},
                         "compat_info": {"source": "ai_assistant", "from_ai_assistant": {"task_id": "assistant-call", "sub_id": part, "total_sub_count": 2}}}}
    await bridge_module.handle_wecom_message(bot, frame(2))
    assert calls == []
    await bridge_module.handle_wecom_message(bot, frame(1))
    await bridge_module.handle_wecom_message(bot, frame(2))
    assert len(calls) == 1
    assert [item["headers"]["req_id"] for item in calls[0]] == ["part-1", "part-2"]


@pytest.mark.asyncio
async def test_assistant_long_result_finishes_on_last_message_only(bridge_module):
    bot = make_bot(bridge_module)
    sess = make_session(bridge_module, bot, "group-user:assistant-room:alice")
    bot.callback_contexts["req-ai"] = {"assistant_task_id": "call-1", "source": "ai_assistant", "received_at": time.monotonic(), "streams": {}, "target": (2, "assistant-room")}
    bridge_module.register_reply_session(bot, "req-ai", sess)
    payloads = bridge_module.build_session_text_payloads("group-user:assistant-room:alice", sess, "req-ai", "x" * 8000, True)
    assert len(payloads) >= 3
    assert all(payload["body"]["to_ai_assistant"]["reply_status"] == "waiting" for payload in payloads[:-1])
    assert payloads[-1]["body"]["to_ai_assistant"]["reply_status"] == "finished"
    assert bridge_module.reply_payload_finished(payloads[0]) is False
    assert bridge_module.reply_payload_finished(payloads[-1]) is True


@pytest.mark.asyncio
async def test_feedback_is_recorded_without_sending_an_event_reply(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    sent = []

    async def send(*args, **kwargs):
        sent.append(args)

    monkeypatch.setattr(bridge_module, "send_ws_payload_with_ack", send)
    def frame(verdict):
        return {"cmd": "aibot_event_callback", "headers": {"req_id": f"feedback-{verdict}"},
                "body": {"msgtype": "event", "from": {"userid": "alice"},
                         "event": {"eventtype": "feedback_event", "feedback_event": {"id": "reply-1", "type": verdict}}}}
    await bridge_module.handle_wecom_message(bot, frame(2))
    await bridge_module.handle_wecom_message(bot, frame(3))
    assert sent == []
    assert bot.feedback_records["reply-1"]["type"] == 3
    bot.feedback_records.clear()
    assert bridge_module.load_wecom_feedback(bot)["reply-1"]["type"] == 3


@pytest.mark.asyncio
async def test_ws_reader_receives_card_ack_before_starting_business(bridge_module, monkeypatch):
    bot = make_bot(bridge_module)
    messages = asyncio.Queue()
    finished = asyncio.Event()
    order = []
    bot.template_card_payloads["card-1"] = {"card_type": "button_interaction", "task_id": "card-1", "main_title": {"title": "执行"},
                                          "button_list": [{"text": "开始", "key": "go", "style": 1}]}
    bot.template_card_button_texts["card-1"] = {"go": "开始"}

    class Socket:
        closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            return await messages.get()

        async def send_json(self, payload):
            order.append(payload["cmd"])
            response = {"headers": {"req_id": payload["headers"]["req_id"]}, "errcode": 0}
            await messages.put(SimpleNamespace(type=bridge_module.WSMsgType.TEXT, data=json.dumps(response)))

    async def enqueue(*args, **kwargs):
        order.append("business")
        finished.set()
        return True

    monkeypatch.setattr(bridge_module, "enqueue_message", enqueue)
    bot.ws = Socket()
    frame = {"cmd": "aibot_event_callback", "headers": {"req_id": "click-1"},
             "body": {"msgtype": "event", "from": {"userid": "alice"}, "event": {"eventtype": "template_card_event",
                      "template_card_event": {"card_type": "button_interaction", "task_id": "card-1", "event_key": "go"}}}}
    await messages.put(SimpleNamespace(type=bridge_module.WSMsgType.TEXT, data=json.dumps(frame)))
    reader = asyncio.create_task(bridge_module.ws_reader_loop(bot, bot.ws))
    try:
        await asyncio.wait_for(finished.wait(), timeout=1)
    finally:
        reader.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reader
        if bot.callback_tasks:
            await asyncio.gather(*bot.callback_tasks, return_exceptions=True)
    assert order == ["aibot_respond_update_msg", "business"]


@pytest.mark.asyncio
@pytest.mark.parametrize("name,mention", [("robot", "@robot"), ("Leo C", "@Leo C"), ("robot", "@实际显示名")])
@pytest.mark.parametrize("mode", ["per-user", "shared"])
@pytest.mark.parametrize("command,confirmation", [
    ("/new", "New session ready. Your next message will start a new thread."),
    ("/bridge-reset", "Session reset. Your next message will start a new thread."),
    ("/bridge-interrupt", "Current task interrupted."),
])
async def test_group_session_commands_control_only_the_addressed_session(
    bridge_module, monkeypatch, name, mention, mode, command, confirmation,
):
    bot = make_bot(bridge_module, name=name)
    bot.config["groupSessionMode"] = mode
    key = "group-user:room-1:alice" if mode == "per-user" else "group:room-1"
    other_key = "group-user:room-1:bob" if mode == "per-user" else "group:room-2"
    previous = make_session(bridge_module, bot, key)
    other = make_session(bridge_module, bot, other_key)
    for session_key, sess in [(key, previous), (other_key, other)]:
        sess.thread_id = f"thread-{session_key}"
        sess.chat = [{"role": "user", "text": session_key}]
        bridge_module.update_owned_session_record(
            bot, session_key, sess,
            lambda record: {**record, "threadId": sess.thread_id, "recentChat": list(sess.chat)},
        )
    previous_record = bridge_module.read_session_record_by_id(previous.session_id)
    other_record = bridge_module.read_session_record_by_id(other.session_id)
    sent = []

    bot.ws = make_acking_ws(bot, sent.append)

    async def reject_enqueue(*args, **kwargs):
        raise AssertionError("session commands must not be sent to the agent")

    monkeypatch.setattr(bridge_module, "enqueue_message", reject_enqueue)
    await bridge_module.handle_wecom_message(bot, {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-group-command"},
        "body": {
            "msgtype": "text", "chattype": "group", "chatid": "room-1",
            "from": {"userid": "alice"}, "text": {"content": f"{mention} {command}"},
        },
    })

    current = bot.sessions[key]
    assert bot.sessions[other_key] is other
    assert bridge_module.read_session_record_by_id(other.session_id) == other_record
    assert other.thread_id == f"thread-{other_key}"
    assert other.chat == [{"role": "user", "text": other_key}]
    assert bridge_module.read_session_record_by_key(bot.config["id"], key)["sessionId"] == current.session_id
    if command == "/new":
        assert current.session_id != previous.session_id
        assert current.thread_id is None
        assert current.chat == []
        stored = bridge_module.read_session_record_by_id(previous.session_id)
        assert stored["threadId"] == previous_record["threadId"]
        assert stored["recentChat"] == previous_record["recentChat"]
    elif command == "/bridge-reset":
        assert current is previous
        assert current.thread_id is None
        assert current.chat == []
    else:
        assert current is previous
        assert current.thread_id == previous_record["threadId"]
        assert current.chat == previous_record["recentChat"]
    assert len(sent) == 1
    assert sent[0]["cmd"] == "aibot_respond_msg"
    assert sent[0]["headers"]["req_id"] == "req-group-command"
    assert sent[0]["body"]["stream"]["content"] == confirmation
    assert sent[0]["body"]["stream"]["finish"] is True
    assert bot.callback_contexts["req-group-command"]["target"] == (2, "room-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ["解释 /new", "`/new`", "/newton", "/new extra", "> /new"])
async def test_group_ordinary_text_is_not_a_session_command(bridge_module, monkeypatch, content):
    bot = make_bot(bridge_module, name="robot")
    captured = []

    async def capture_message(_bot, key, text, req_id, **kwargs):
        captured.append((key, text, req_id))

    monkeypatch.setattr(bridge_module, "enqueue_message", capture_message)
    await bridge_module.handle_wecom_message(bot, {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-group-text"},
        "body": {
            "msgtype": "text", "chattype": "group", "chatid": "room-1",
            "from": {"userid": "alice"}, "text": {"content": f"@robot {content}"},
        },
    })
    assert captured == [("group-user:room-1:alice", content, "req-group-text")]
    assert bot.sessions == {}
