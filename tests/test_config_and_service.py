import asyncio
from pathlib import Path
import json

from aiohttp import web
from aiohttp.test_utils import make_mocked_request
import pytest

from workspace_bridge.config import build_bot_from_app_config, load_app_config
from workspace_bridge.service import APP_WECOM_RUNTIME_KEY, create_app, load_app


def write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def test_load_app_config_reads_secret_file_and_paths(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")

    config = load_app_config(
        {
            "BRIDGE_BIND": "127.0.0.1:9499",
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )

    assert config.bind_host == "127.0.0.1"
    assert config.bind_port == 9499
    assert config.bot_id == "bot-1"
    assert config.bot_secret == "secret-value"
    assert config.source_dir == source_dir.resolve()


def test_load_app_config_reads_agent_run_as_settings(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")

    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
            "WECOM_AGENT_RUN_AS_USER": "nobody",
            "WECOM_AGENT_RUN_AS_GROUP": "nogroup",
            "WECOM_AGENT_RUNTIME_ROOT": str(tmp_path / "claude-runtime"),
        }
    )

    assert config.agent_run_as_user == "nobody"
    assert config.agent_run_as_group == "nogroup"
    assert config.agent_runtime_root == (tmp_path / "claude-runtime").resolve()


def test_load_app_config_reads_workspace_mode(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")

    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
            "WECOM_BOT_WORKSPACE_MODE": "team",
        }
    )

    assert config.workspace_mode == "team"


async def test_service_health_and_prepare_session(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "BRIDGE_BIND": "127.0.0.1:9499",
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    app = create_app(config)
    health_request = make_mocked_request("GET", "/", app=app)

    health_match = next(route for route in app.router.routes() if route.method == "GET")
    health_response = await health_match.handler(health_request)
    health_payload = json.loads(health_response.text)
    assert health_payload["ok"] is True
    assert health_payload["botId"] == "bot-1"
    assert health_payload["wecomEnabled"] is False
    assert health_payload["wecomConnected"] is False
    assert health_payload["wecomStatus"] is None
    assert health_payload["wecomLastError"] is None
    assert health_payload["runtimeStatus"] is None
    assert health_payload["runtimeLastError"] is None
    assert health_payload["wecomTaskPresent"] is False
    assert health_payload["wecomTaskDone"] is None
    assert health_payload["pendingRequests"] == 0
    assert health_payload["pendingStreams"] == 0
    assert health_payload["pendingFinals"] == 0
    assert health_payload["replyStates"] == 0

    healthz_match = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/healthz")
    healthz_response = await healthz_match.handler(health_request)
    healthz_payload = json.loads(healthz_response.text)
    assert healthz_payload["ok"] is True

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "message": "hello"}

    prepare_match = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/prepare")
    prepared_response = await prepare_match.handler(JsonRequest(app))
    prepared_payload = json.loads(prepared_response.text)
    assert prepared_payload["ok"] is True
    assert prepared_payload["workspaceId"].startswith("user:")
    assert prepared_payload["workspaceScope"] == "user"
    assert prepared_payload["cwd"].endswith("/workfile")
    assert prepared_payload["workfileDir"].endswith("/workfile")
    assert prepared_payload["roomfileDir"] is None
    assert prepared_payload["ownerUserId"] == "alice"
    assert prepared_payload["ownerRoomId"] is None
    assert "prompt" in prepared_payload
    assert prepared_payload["sessionId"].startswith("session-")


async def test_service_prepare_uses_thread_offload_for_prepare_session_run(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    app = create_app(config)
    from workspace_bridge import service as service_module
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    monkeypatch.setattr(service_module, "run_blocking", fake_run_blocking)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "message": "hello"}

    prepare_match = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/prepare")
    await prepare_match.handler(JsonRequest(app))

    assert "prepare_session_run" in calls


async def test_service_prepare_exposes_group_user_workspace_metadata(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    app = create_app(config)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "group-user:room-1:alice", "message": "hello"}

    prepare_match = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/prepare")
    prepared_response = await prepare_match.handler(JsonRequest(app))
    prepared_payload = json.loads(prepared_response.text)

    assert prepared_payload["workspaceScope"] == "user"
    assert prepared_payload["workfileDir"].endswith("/workfile")
    assert prepared_payload["roomfileDir"].endswith("/roomfile")
    assert prepared_payload["ownerUserId"] == "alice"
    assert prepared_payload["ownerRoomId"] == "room-1"


async def test_service_prepare_reuses_shared_personal_workspace_across_bots(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    first = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default-a",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
            "WORKSPACE_NAMESPACE": "team-a",
        }
    )
    second = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default-b",
            "WECOM_BOT_ID": "bot-2",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
            "WORKSPACE_NAMESPACE": "team-a",
        }
    )
    first_app = create_app(first)
    second_app = create_app(second)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "message": "hello"}

    route_a = next(route for route in first_app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/prepare")
    route_b = next(route for route in second_app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/prepare")
    payload_a = json.loads((await route_a.handler(JsonRequest(first_app))).text)
    payload_b = json.loads((await route_b.handler(JsonRequest(second_app))).text)

    assert payload_a["workfileDir"] == payload_b["workfileDir"]
    assert payload_a["sessionId"] != payload_b["sessionId"]


async def test_service_prepare_rejects_invalid_json_and_missing_fields(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    app = create_app(config)
    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/prepare")

    class BadJsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            raise json.JSONDecodeError("bad", "{", 1)

    class JsonRequest:
        def __init__(self, app, payload):
            self.app = app
            self._payload = payload

        async def json(self):
            return self._payload

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(BadJsonRequest(app))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "request body must be valid JSON"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "chatKey required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "single:alice"}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "message required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "invalid", "message": "hello"}))
    assert excinfo.value.status == 400
    assert "invalid chat key" in excinfo.value.text


def test_build_bot_from_app_config_returns_runtime_bot(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )

    bot = build_bot_from_app_config(config)

    assert bot.bot_id == "bot-1"
    assert bot.source.source_dir == source_dir.resolve()


def test_load_app_config_reads_agent_backend_and_command(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")

    config = load_app_config(
        {
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
            "WECOM_AGENT_BACKEND": "claude",
            "WECOM_AGENT_COMMAND": "claude --model sonnet",
        }
    )

    assert config.agent_backend == "claude"
    assert config.agent_command == "claude --model sonnet"


def test_load_app_accepts_aiohttp_web_factory_signature(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    env_file = tmp_path / ".env"
    write_secret(secret_file, "secret-value\n")
    env_file.write_text(
        "\n".join(
            [
                "BRIDGE_BIND=127.0.0.1:6288",
                f"RUNTIME_ROOT={tmp_path / 'runtime'}",
                "WECOM_BOT_NAME=default",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
                f"WECOM_BOT_SOURCE_DIR={source_dir}",
            ]
        ),
        encoding="utf-8",
    )

    app = load_app([], env_file=env_file)

    assert app is not None


async def test_service_startup_cleans_orphan_session_codex_homes(tmp_path: Path, monkeypatch) -> None:
    from workspace_bridge import service as service_module
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    app = create_app(config)
    calls = {"orphan": [], "stale": []}

    monkeypatch.setattr(service_module, "cleanup_outdated_session_artifacts", lambda bot: 0)
    monkeypatch.setattr(service_module, "cleanup_orphan_session_codex_homes", lambda runtime_root: calls["orphan"].append(runtime_root) or 0)
    monkeypatch.setattr(
        service_module,
        "cleanup_stale_session_codex_homes",
        lambda runtime_root, current_ms, ttl_ms, active_session_ids=None: calls["stale"].append((runtime_root, ttl_ms, active_session_ids)) or 0,
    )

    for callback in app.on_startup:
        await callback(app)

    assert calls["orphan"] == [config.runtime_root]
    assert len(calls["stale"]) == 1
    assert calls["stale"][0][0] == config.runtime_root
    assert calls["stale"][0][1] == service_module.SESSION_HOME_TTL_MS
    assert calls["stale"][0][2] == set()

    for callback in app.on_cleanup:
        await callback(app)


async def test_service_cleanup_clears_runtime_state_and_rejects_pending_requests(tmp_path: Path) -> None:
    from workspace_bridge.wecom_upload import create_request_future

    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.connected = True
    runtime.wecom_status = "subscribe_ok"
    runtime.wecom_last_error = None
    runtime.last_status = "message_failed"
    runtime.last_error = "boom"
    runtime.ws = object()
    future = create_request_future(runtime, "req-1")
    runtime.pending_streams["req-1"] = {"headers": {"req_id": "req-1"}}
    runtime.pending_finals["req-2"] = {"headers": {"req_id": "req-2"}}
    runtime.reply_states["req-1"] = object()
    runtime.active_processes["single:alice"] = type("Proc", (), {"terminate": lambda self: None})()
    task = asyncio.create_task(asyncio.sleep(3600))
    schedule_task = asyncio.create_task(asyncio.sleep(3600))
    runtime.message_tasks.add(task)
    runtime.active_message_tasks["single:alice"] = task
    runtime.active_schedule_tasks["single:alice"] = schedule_task
    runtime.active_schedule_runs["single:alice"] = ("schedule-1", "job-1")
    runtime.active_session_ids.add("session-1")
    runtime.session_threads["single:alice"] = "thread-1"
    runtime.resume_candidates["single:alice"] = [{"sessionId": "session-1"}]
    runtime.resume_selection_expires_at["single:alice"] = 123

    for callback in app.on_cleanup:
        await callback(app)

    assert runtime.connected is False
    assert runtime.ws is None
    assert task.cancelled() is True
    assert schedule_task.cancelled() is True
    assert runtime.wecom_status is None
    assert runtime.wecom_last_error is None
    assert runtime.last_status is None
    assert runtime.last_error is None
    assert runtime.pending_streams == {}
    assert runtime.pending_finals == {}
    assert runtime.reply_states == {}
    assert runtime.active_processes == {}
    assert runtime.active_message_tasks == {}
    assert runtime.active_schedule_tasks == {}
    assert runtime.active_schedule_runs == {}
    assert runtime.suppressed_schedule_cancels == set()
    assert runtime.message_tasks == set()
    assert runtime.active_session_ids == set()
    assert runtime.session_threads == {}
    assert runtime.resume_candidates == {}
    assert runtime.resume_selection_expires_at == {}
    assert future.done() is True
    try:
        future.result()
    except RuntimeError as exc:
        assert "service shutting down" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
