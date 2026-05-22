import asyncio
import json

from aiohttp import WSMsgType
import pytest

from workspace_bridge.config import load_app_config
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.reply_state import get_or_create_reply_state
from workspace_bridge.runtime import prepare_session_run
from workspace_bridge.service import APP_WECOM_RUNTIME_KEY, APP_WECOM_TASK_KEY, create_app
from workspace_bridge.wecom_runtime import handle_wecom_payload, run_wecom_runtime
from workspace_bridge.wecom_upload import create_request_future, ws_send_json


def write_secret(path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def make_config(tmp_path):
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    return load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )


class FakeWS:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)

    async def receive(self):
        payload = self.payloads.pop(0)
        return type("Msg", (), {"type": WSMsgType.TEXT, "data": json.dumps(payload)})()


async def test_subscribe_bot_returns_failed_subscribe_response(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    future = create_request_future(bot, "req-1")
    ws = FakeWS([])

    async def fake_handler(*_args, **_kwargs):
        return "session-1", "done"

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {"cmd": "aibot_subscribe", "headers": {"req_id": "req-1"}, "errcode": 40001, "errmsg": "bad secret"},
        fake_handler,
    )

    response = await future
    assert response["errcode"] == 40001


async def test_health_exposes_runtime_error_fields(tmp_path) -> None:
    config = make_config(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.last_status = "subscribe_failed"
    runtime.last_error = "bad secret"
    route = next(route for route in app.router.routes() if route.method == "GET")
    response = await route.handler(type("Req", (), {"app": app})())
    payload = json.loads(response.text)

    assert payload["wecomStatus"] == "subscribe_failed"
    assert payload["wecomLastError"] == "bad secret"


async def test_runtime_status_model_requires_subscribe_success_for_connected(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    runtime.connected = False
    runtime.last_status = "subscribe_failed"
    runtime.last_error = "bad secret"

    assert runtime.connected is False
    assert runtime.last_status == "subscribe_failed"


async def test_bridge_status_command_returns_runtime_status(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.connected = True
    bot.active_processes["single:alice"] = object()
    get_or_create_reply_state(bot, "req-running", "session-1", "single:alice")

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-status"}},
        },
        fake_handler,
    )

    assert len(ws.sent) == 1
    content = ws.sent[0]["body"]["stream"]["content"]
    assert "chatKey: single:alice" in content
    assert "running: yes" in content


async def test_bridge_reset_command_clears_reply_state(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    class FakeProcess:
        def __init__(self) -> None:
            self.terminated = False

        def terminate(self) -> None:
            self.terminated = True

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    process = FakeProcess()
    bot.active_processes["single:alice"] = process
    get_or_create_reply_state(bot, "req-running", "session-1", "single:alice")

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-2"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-reset"}},
        },
        fake_handler,
    )

    assert bot.reply_states == {}
    assert process.terminated is True
    assert ws.sent[0]["body"]["stream"]["content"] == "Session reset."


async def test_bridge_reset_command_only_clears_current_chat_state(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    class FakeProcess:
        def terminate(self) -> None:
            return None

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.active_processes["single:alice"] = FakeProcess()
    get_or_create_reply_state(bot, "req-alice", "session-1", "single:alice")
    get_or_create_reply_state(bot, "req-bob", "session-2", "single:bob")
    bot.pending_streams["req-alice"] = {"headers": {"req_id": "req-alice"}}
    bot.pending_streams["req-bob"] = {"headers": {"req_id": "req-bob"}}

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-reset"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-reset"}},
        },
        fake_handler,
    )

    assert "req-alice" not in bot.reply_states
    assert "req-bob" in bot.reply_states
    assert "req-alice" not in bot.pending_streams
    assert "req-bob" in bot.pending_streams


async def test_bridge_reset_command_cancels_active_message_task(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    task = asyncio.create_task(asyncio.sleep(3600))
    bot.active_message_tasks["single:alice"] = task

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-reset"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-reset"}},
        },
        fake_handler,
    )


async def test_ws_send_json_initializes_runtime_send_lock(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    class LockingWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload):
            self.sent.append(payload)

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = LockingWS()

    await ws_send_json(runtime, {"cmd": "x"})

    assert runtime.ws_send_lock is not None
    assert runtime.ws.sent == [{"cmd": "x"}]


async def test_bridge_interrupt_command_terminates_active_process(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    class FakeProcess:
        def __init__(self) -> None:
            self.returncode = None
            self.terminated = False

        def terminate(self) -> None:
            self.terminated = True
            self.returncode = -15

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    process = FakeProcess()
    bot.active_processes["single:alice"] = process

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-3"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-interrupt"}},
        },
        fake_handler,
    )

    assert process.terminated is True


async def test_resume_command_lists_candidates(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import prepare_session_run, store_session_record
    from dataclasses import replace

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    launch = prepare_session_run(bot.config, "single:alice")
    other = prepare_session_run(bot.config, "single:bob")
    store_session_record(bot.config.runtime_root, replace(launch.session, thread_id="thread-a", last_run_at=2000))
    store_session_record(bot.config.runtime_root, replace(other.session, thread_id="thread-b", last_run_at=1000))

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for resume list")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-resume"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-resume"}},
        },
        fake_handler,
    )

    assert len(ws.sent) == 1
    content = ws.sent[0]["body"]["stream"]["content"]
    assert "可恢复会话" in content
    assert launch.session.session_id in content
    assert other.session.session_id not in content


async def test_resume_selection_binds_selected_thread(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run, store_session_record
    from dataclasses import replace

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws
    current = prepare_session_run(runtime.config, "single:alice")
    target = prepare_session_run(runtime.config, "group-user:room-1:alice")
    store_session_record(runtime.config.runtime_root, replace(target.session, thread_id="thread-target", last_run_at=2000))

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for resume selection")

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-resume"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-resume"}},
        },
        fake_handler,
    )
    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-select"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "1"}},
        },
        fake_handler,
    )

    assert any(target.session.session_id in item["body"]["stream"]["content"] for item in ws.sent if item["body"]["stream"]["finish"])
    updated = load_session_record(runtime.config.runtime_root, current.session.session_id)
    assert updated is not None
    assert updated.thread_id == "thread-target"
    assert runtime.resume_candidates == {}


async def test_workfile_dir_is_rejected_for_send_file_by_default(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.file_send import create_file_send_request

    config = make_config(tmp_path)
    bot = build_bot_from_app_config(config)
    launch = prepare_session_run(bot, "group-user:room-1:alice")
    workfile = launch.runtime_context.workfile_dir / "note.txt"
    workfile.write_text("hello", encoding="utf-8")

    with pytest.raises(PermissionError) as excinfo:
        create_file_send_request(
            launch.runtime_context,
            session_id=launch.session.session_id,
            chat_key="group-user:room-1:alice",
            file_path=workfile,
        )

    assert "outside allowed roots" in str(excinfo.value)


async def test_workfile_dir_is_allowed_for_send_file_when_allowlisted(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config, load_app_config
    from workspace_bridge.file_send import create_file_send_request

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
            "FILE_SEND_ROOTS": str(tmp_path / "runtime" / "workspaces" / "users"),
        }
    )
    bot = build_bot_from_app_config(config)
    launch = prepare_session_run(bot, "group-user:room-1:alice")
    workfile = launch.runtime_context.workfile_dir / "note.txt"
    workfile.write_text("hello", encoding="utf-8")

    request = create_file_send_request(
        launch.runtime_context,
        session_id=launch.session.session_id,
        chat_key="group-user:room-1:alice",
        file_path=workfile,
    )

    assert request.file_path == workfile.resolve()


async def test_run_wecom_runtime_marks_subscribe_failure(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    class FakeMsg:
        def __init__(self, payload):
            self.type = WSMsgType.TEXT
            self.data = json.dumps(payload)

    class FakeWSClient:
        def __init__(self):
            self.sent = []
            self.payloads = [
                FakeMsg({"cmd": "aibot_subscribe", "headers": {"req_id": "req-1"}, "errcode": 40001, "errmsg": "bad secret"})
            ]

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def send_json(self, payload):
            self.sent.append(payload)

        async def receive(self):
            return self.payloads.pop(0)

        def exception(self):
            return None

    class FakeClientSession:
        def __init__(self, *args, **kwargs):
            self.ws = FakeWSClient()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def ws_connect(self, _url):
            return self.ws

    monkeypatch.setattr(runtime_module.aiohttp, "ClientSession", FakeClientSession)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})

    try:
        await run_wecom_runtime(config, runtime)
    except RuntimeError as exc:
        assert "bad secret" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")

    assert runtime.connected is False
    assert runtime.last_status == "subscribe_failed"
    assert runtime.last_error == "bad secret"


async def test_run_wecom_runtime_retries_after_failure(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    calls = {"count": 0}

    async def fake_run_once(_config, runtime):
        calls["count"] += 1
        runtime.last_error = "boom"
        if calls["count"] == 1:
            raise RuntimeError("boom")
        raise asyncio.CancelledError

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runtime_module, "run_wecom_runtime_once", fake_run_once)
    monkeypatch.setattr(runtime_module.asyncio, "sleep", fake_sleep)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})

    try:
        await run_wecom_runtime(config, runtime)
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("expected CancelledError")

    assert calls["count"] == 2


async def test_service_startup_creates_wecom_task_when_enabled(tmp_path, monkeypatch) -> None:
    from workspace_bridge import service as service_module

    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    started = {"value": False}

    async def fake_run_wecom_runtime(_config, _runtime):
        started["value"] = True
        await __import__("asyncio").sleep(3600)

    monkeypatch.setattr(service_module, "run_wecom_runtime", fake_run_wecom_runtime)
    for callback in app.on_startup:
        await callback(app)
    await __import__("asyncio").sleep(0)

    assert app[APP_WECOM_TASK_KEY] is not None
    assert started["value"] is True

    for callback in app.on_cleanup:
        await callback(app)


async def test_service_cleanup_terminates_active_processes(tmp_path) -> None:
    class FakeProcess:
        def __init__(self) -> None:
            self.terminated = False

        def terminate(self) -> None:
            self.terminated = True

    config = make_config(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    process = FakeProcess()
    runtime.active_processes["single:alice"] = process

    for callback in app.on_cleanup:
        await callback(app)

    assert process.terminated is True


async def test_dispatch_message_uses_streaming_execution(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    calls = []

    async def fake_stream_text_message_once(_config, _runtime, parsed, **_kwargs):
        calls.append(parsed.chat_key)
        return "session-1", "done"

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
        },
        runtime_module._dispatch_message,
    )
    await asyncio.sleep(0)

    assert calls == ["single:alice"]


async def test_dispatch_message_rejects_concurrent_same_chat(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    started = asyncio.Event()
    pending = asyncio.Future()

    async def fake_stream_text_message_once(_config, _runtime, _parsed, **_kwargs):
        started.set()
        await pending
        return "session-1", "done"

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])

    first_payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
    }
    second_payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-2"},
        "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "again"}},
    }

    await handle_wecom_payload(config, bot, ws, first_payload, runtime_module._dispatch_message)
    await started.wait()
    await handle_wecom_payload(config, bot, ws, second_payload, runtime_module._dispatch_message)
    pending.cancel()
    await asyncio.sleep(0)

    assert ws.sent
    assert "已有任务在运行" in ws.sent[-1]["body"]["stream"]["content"]
