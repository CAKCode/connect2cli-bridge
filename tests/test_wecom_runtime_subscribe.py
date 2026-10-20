import asyncio
import json
from dataclasses import replace

from aiohttp import WSMsgType
import pytest

from workspace_bridge.config import load_app_config
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.reply_state import cache_reply_payload, get_or_create_reply_state
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


async def test_handle_wecom_payload_ignores_non_dict_payload(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    called = {"value": False}

    async def fake_handler(*_args, **_kwargs):
        called["value"] = True

    await handle_wecom_payload(config, bot, ws, ["bad"], fake_handler)

    assert called["value"] is False
    assert ws.sent == []


async def test_handle_wecom_payload_tolerates_non_dict_headers_and_body(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    called = {"value": False}

    async def fake_handler(*_args, **_kwargs):
        called["value"] = True

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_event_callback",
            "headers": "bad",
            "body": "bad",
            "response_url": "https://example.com/response",
        },
        fake_handler,
    )

    assert called["value"] is False
    assert bot.reply_urls == {}


async def test_health_exposes_runtime_error_fields(tmp_path) -> None:
    config = make_config(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.wecom_status = "subscribe_failed"
    runtime.wecom_last_error = "bad secret"
    runtime.last_status = "message_failed"
    runtime.last_error = "boom"
    route = next(route for route in app.router.routes() if route.method == "GET")
    response = await route.handler(type("Req", (), {"app": app})())
    payload = json.loads(response.text)

    assert payload["wecomStatus"] == "subscribe_failed"
    assert payload["wecomLastError"] == "bad secret"
    assert payload["runtimeStatus"] == "message_failed"
    assert payload["runtimeLastError"] == "boom"


async def test_health_reports_unhealthy_for_disconnected_runtime_statuses(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    route = next(route for route in app.router.routes() if route.method == "GET")

    for status in ("connect_failed", "websocket_closed", "websocket_error", "websocket_disconnected_event"):
        runtime.connected = False
        runtime.wecom_status = status
        runtime.wecom_last_error = status
        response = await route.handler(type("Req", (), {"app": app})())
        payload = json.loads(response.text)

        assert response.status == 503
        assert payload["ok"] is False
        assert payload["wecomStatus"] == status


async def test_runtime_status_model_requires_subscribe_success_for_connected(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    runtime.connected = False
    runtime.wecom_status = "subscribe_failed"
    runtime.wecom_last_error = "bad secret"

    assert runtime.connected is False
    assert runtime.wecom_status == "subscribe_failed"


async def test_bridge_status_command_returns_runtime_status(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.layout import build_workspace_ref
    from workspace_bridge import wecom_runtime as runtime_module
    from workspace_bridge.runtime import stable_session_id

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.connected = True
    bot.active_processes["single:alice"] = object()
    state = get_or_create_reply_state(bot, "req-running", "session-1", "single:alice")
    bot.pending_streams["req-running"] = {"headers": {"req_id": "req-running"}}
    bot.pending_finals["req-running"] = {"headers": {"req_id": "req-running"}}
    bot.session_threads["single:alice"] = "thread-1"
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
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
    finally:
        runtime_module.run_blocking = original_run_blocking

    assert len(ws.sent) == 1
    assert ws.sent[0]["body"]["stream"]["id"] == stable_session_id(bot.config.bot_id, "single:alice", bot.config.source.source_dir)
    content = ws.sent[0]["body"]["stream"]["content"]
    assert "chatKey: single:alice" in content
    assert f"sessionId: {stable_session_id(bot.config.bot_id, 'single:alice', bot.config.source.source_dir)}" in content
    assert "threadId: thread-1" in content
    expected_cwd = build_workspace_ref(
        bot.config.runtime_root,
        bot.config.workspace_namespace,
        bot.config.source.source_dir,
        "single:alice",
        workspace_mode=bot.config.workspace_mode,
        agent_backend=bot.config.agent_backend,
    ).cwd_dir
    assert f"cwd: {expected_cwd}" in content
    assert "running: yes" in content
    assert "pendingStreams: 1" in content
    assert "pendingFinals: 1" in content
    assert "Codex /status" in content
    assert "Model:" in content
    assert "prepare_session_run" not in calls


async def test_cwd_command_rejects_team_mode(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for cwd command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-cwd-team"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/cwd repo"}},
        },
        fake_handler,
    )

    assert ws.sent[-1]["body"]["stream"]["content"] == "仅 personal 模式支持 /cwd；team 模式不允许切换。"


async def test_cwd_command_updates_personal_session_cwd(tmp_path) -> None:
    from dataclasses import replace

    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run, stable_session_id, update_session_record

    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "workspace_mode": "personal", "agent_backend": "claude"})
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    target_dir = config.source_dir / "nested" / "service"
    target_dir.mkdir(parents=True)
    launch = prepare_session_run(bot.config, "single:alice")
    update_session_record(
        bot.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(current, thread_id="thread-old"),
    )
    bot.session_threads["single:alice"] = "thread-old"
    bot.resume_candidates["single:alice"] = [{"sessionId": "session-old", "threadId": "thread-old", "chatKey": "single:alice"}]
    bot.resume_selection_expires_at["single:alice"] = 60_000

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for cwd command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-cwd-personal"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/cwd nested/service"}},
        },
        fake_handler,
    )

    session_id = stable_session_id(bot.config.bot_id, "single:alice", bot.config.source.source_dir, bot.config.workspace_namespace, bot.config.workspace_mode)
    stored = load_session_record(bot.config.runtime_root, session_id)
    assert stored is not None
    assert stored.cwd_dir == target_dir.resolve()
    assert stored.thread_id is None
    assert "single:alice" not in bot.session_threads
    assert "single:alice" not in bot.resume_candidates
    assert "single:alice" not in bot.resume_selection_expires_at
    assert ws.sent[-1]["body"]["stream"]["content"] == f"cwd changed to {target_dir.resolve()}"


async def test_bridge_status_command_uses_persisted_thread_id_after_restart(tmp_path) -> None:
    from dataclasses import replace

    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import prepare_session_run, update_session_record

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.connected = True
    launch = prepare_session_run(bot.config, "single:alice")
    update_session_record(
        bot.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(current, thread_id="thread-persisted"),
    )

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-status-persisted-thread"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-status"}},
        },
        fake_handler,
    )

    content = ws.sent[-1]["body"]["stream"]["content"]
    assert "threadId: thread-persisted" in content


async def test_bridge_status_command_reports_persisted_personal_cwd(tmp_path) -> None:
    from dataclasses import replace

    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import prepare_session_run, update_session_record

    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "workspace_mode": "personal", "agent_backend": "claude"})
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.connected = True
    launch = prepare_session_run(bot.config, "single:alice")
    target_dir = (config.source_dir / "nested" / "service").resolve()
    target_dir.mkdir(parents=True)
    update_session_record(
        bot.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(current, cwd_dir=target_dir),
    )

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-status-persisted-cwd"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-status"}},
        },
        fake_handler,
    )

    content = ws.sent[-1]["body"]["stream"]["content"]
    assert f"cwd: {target_dir}" in content


async def test_cwd_command_rejects_switch_while_running(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "workspace_mode": "personal", "agent_backend": "claude"})
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    task = asyncio.create_task(asyncio.sleep(3600))
    bot.active_message_tasks["single:alice"] = task

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for cwd command")

    try:
        await handle_wecom_payload(
            config,
            bot,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-cwd-running"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/cwd nested/service"}},
            },
            fake_handler,
        )
    finally:
        task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError):
            await task

    assert ws.sent[-1]["body"]["stream"]["content"] == "当前有任务在运行，不能切换 cwd。请先等待完成或使用 /bridge-interrupt。"


async def test_bridge_status_command_reports_running_for_active_message_task_without_process(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.connected = True
    task = asyncio.create_task(asyncio.sleep(3600))
    bot.active_message_tasks["single:alice"] = task

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    try:
        await handle_wecom_payload(
            config,
            bot,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-status"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-status"}},
            },
            fake_handler,
        )
    finally:
        task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError):
            await task

    content = ws.sent[-1]["body"]["stream"]["content"]
    assert "running: yes" in content


async def test_bridge_status_command_reports_running_for_active_schedule_task_without_process(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.connected = True
    task = asyncio.create_task(asyncio.sleep(3600))
    bot.active_schedule_tasks["single:alice"] = task
    bot.active_schedule_runs["single:alice"] = ("schedule-1", "job-1")

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    try:
        await handle_wecom_payload(
            config,
            bot,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-status"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-status"}},
            },
            fake_handler,
        )
    finally:
        task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError):
            await task

    content = ws.sent[-1]["body"]["stream"]["content"]
    assert "running: yes" in content


async def test_new_session_command_clears_reply_state(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import (
        current_session_id,
        load_session_record,
        prepare_session_run,
        session_codex_home_root,
        update_session_record,
    )

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
    get_or_create_reply_state(bot, "req-running", "session-1", "single:alice")
    launch = prepare_session_run(bot.config, "single:alice")
    update_session_record(
        bot.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(
            current,
            thread_id="thread-old",
            recent_chat=(
                {"role": "user", "text": "older user turn"},
                {"role": "bot", "text": "older bot turn"},
            ),
        ),
    )
    session_home = session_codex_home_root(bot.config.runtime_root) / launch.session.session_id
    session_home.mkdir(parents=True, exist_ok=True)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-2"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
        },
        fake_handler,
    )

    assert bot.reply_states == {}
    assert process.terminated is True
    assert ws.sent[0]["body"]["stream"]["content"] == "New session ready. Your next message will start a new thread."
    new_session_id = current_session_id(bot.config, "single:alice")
    assert new_session_id != launch.session.session_id
    assert ws.sent[0]["body"]["stream"]["id"] == new_session_id
    assert session_home.exists() is True
    previous = load_session_record(bot.config.runtime_root, launch.session.session_id)
    current = load_session_record(bot.config.runtime_root, new_session_id)
    assert previous is not None
    assert previous.thread_id == "thread-old"
    assert previous.recent_chat == (
        {"role": "user", "text": "older user turn"},
        {"role": "bot", "text": "older bot turn"},
    )
    assert current is not None
    assert current.thread_id is None
    assert current.recent_chat == ()


async def test_template_card_event_is_not_dispatched_to_text_handler(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    called = {"value": False}

    async def fake_handler(*_args, **_kwargs):
        called["value"] = True

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "req-evt-1"},
            "body": {
                "msgtype": "event",
                "chatid": "room-1",
                "chattype": "group",
                "from": {"userid": "alice"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "approve",
                        "task_id": "task-1",
                    },
                },
            },
        },
        fake_handler,
    )

    assert called["value"] is False


async def test_template_card_event_preserves_existing_delivery_metadata(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.template_card_payloads["task-1"] = {
        "card_type": "button_interaction",
        "main_title": {"title": "hello"},
        "button_list": [{"text": "go", "style": 1, "key": "go"}],
        "task_id": "task-1",
    }
    bot.template_card_delivery_meta["task-1"] = {
        "taskId": "task-1",
        "chatKey": "single:alice",
        "templateCard": dict(bot.template_card_payloads["task-1"]),
        "sentAtMs": 1234,
    }

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for template card event")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "req-evt-2"},
            "body": {
                "msgtype": "event",
                "from": {"userid": "alice"},
                "event": {
                    "eventtype": "template_card_event",
                    "template_card_event": {
                        "card_type": "button_interaction",
                        "event_key": "approve",
                        "task_id": "task-1",
                    },
                },
            },
        },
        fake_handler,
    )

    assert bot.template_card_delivery_meta["task-1"]["sentAtMs"] == 1234
    assert bot.template_card_delivery_meta["task-1"]["templateCard"]["task_id"] == "task-1"


async def test_template_card_event_or_text_callback_saves_response_url(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])

    async def fake_handler(*_args, **_kwargs):
        return None

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {
                "msgtype": "text",
                "chattype": "single",
                "chatid": "alice",
                "text": {"content": "hello"},
                "from": {"userid": "alice"},
                "response_url": "https://example.com/response",
            },
        },
        fake_handler,
    )

    assert bot.reply_urls["req-1"]["responseUrl"] == "https://example.com/response"
    assert bot.reply_urls["req-1"]["chatKey"] == "single:alice"


async def test_file_callback_saves_response_url_with_chat_key(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for file callback")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-file-1"},
            "body": {
                "msgtype": "file",
                "chattype": "group",
                "chatid": "room-1",
                "from": {"userid": "alice"},
                "file": {"filename": "report.txt"},
                "response_url": "https://example.com/file-response",
            },
        },
        fake_handler,
    )

    assert bot.reply_urls["req-file-1"]["responseUrl"] == "https://example.com/file-response"
    assert bot.reply_urls["req-file-1"]["chatKey"] == "group-user:room-1:alice"


async def test_text_callback_with_missing_sender_does_not_save_response_url(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for malformed sender")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-missing-user"},
            "body": {
                "msgtype": "text",
                "chattype": "single",
                "text": {"content": "hello"},
                "from": {},
                "response_url": "https://example.com/response",
            },
        },
        fake_handler,
    )

    assert "req-missing-user" not in bot.reply_urls


async def test_new_session_command_only_clears_current_chat_state(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    class FakeProcess:
        returncode = None

        def terminate(self) -> None:
            self.returncode = -15
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
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
        },
        fake_handler,
    )

    assert "req-alice" not in bot.reply_states
    assert "req-bob" in bot.reply_states
    assert "req-alice" not in bot.pending_streams
    assert "req-bob" in bot.pending_streams


@pytest.mark.parametrize("bot_name,mention", [("default", "@default"), ("Leo C", "@Leo C"), ("default", "@实际显示名")])
@pytest.mark.parametrize("command", ["/new", "/bridge-reset", "/bridge-interrupt"])
async def test_group_session_command_keeps_other_members_context(tmp_path, bot_name, mention, command) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import current_session_id, load_session_record, update_session_record

    config = make_config(tmp_path)
    bot_config = replace(build_bot_from_app_config(config), bot_name=bot_name)
    bot = WeComBotRuntime(config=bot_config, pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    alice_key, bob_key = "group-user:room-1:alice", "group-user:room-1:bob"
    alice = prepare_session_run(bot.config, alice_key)
    bob = prepare_session_run(bot.config, bob_key)
    for key, launch in [(alice_key, alice), (bob_key, bob)]:
        update_session_record(
            bot.config.runtime_root, launch.session.session_id,
            lambda current: replace(current, thread_id=f"thread-{key}", recent_chat=({"role": "user", "text": key},)),
        )
        get_or_create_reply_state(bot, f"req-{key}", launch.session.session_id, key)
        bot.pending_streams[f"req-{key}"] = {"headers": {"req_id": f"req-{key}"}}
    other_record = load_session_record(bot.config.runtime_root, bob.session.session_id)

    async def reject_handler(*args, **kwargs):
        raise AssertionError("session commands must not be sent to the agent")

    await handle_wecom_payload(config, bot, ws, {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-group-command"},
        "body": {
            "msgtype": "text", "chattype": "group", "chatid": "room-1",
            "from": {"userid": "alice"}, "text": {"content": f"{mention} {command}"},
        },
    }, reject_handler)

    current_id = current_session_id(bot.config, alice_key)
    current = load_session_record(bot.config.runtime_root, current_id)
    assert current_session_id(bot.config, bob_key) == bob.session.session_id
    assert load_session_record(bot.config.runtime_root, bob.session.session_id) == other_record
    assert f"req-{alice_key}" not in bot.reply_states
    assert f"req-{alice_key}" not in bot.pending_streams
    assert f"req-{bob_key}" in bot.reply_states
    assert f"req-{bob_key}" in bot.pending_streams
    if command == "/new":
        assert current_id != alice.session.session_id
        assert current.thread_id is None
        assert current.recent_chat == ()
        old = load_session_record(bot.config.runtime_root, alice.session.session_id)
        assert old.thread_id == f"thread-{alice_key}"
        assert old.recent_chat == ({"role": "user", "text": alice_key},)
    elif command == "/bridge-reset":
        assert current_id == alice.session.session_id
        assert current.thread_id is None
        assert current.recent_chat == ()
    else:
        assert current_id == alice.session.session_id
        assert current.thread_id == f"thread-{alice_key}"
    assert len(ws.sent) == 1
    assert ws.sent[0]["cmd"] == "aibot_respond_msg"
    assert ws.sent[0]["headers"]["req_id"] == "req-group-command"
    assert ws.sent[0]["body"]["stream"]["finish"] is True


async def test_new_session_command_cancels_active_message_task(tmp_path) -> None:
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
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
        },
        fake_handler,
    )


async def test_new_session_command_uses_thread_offload_for_session_creation(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module
    from workspace_bridge.runtime import current_session_id

    class FakeProcess:
        returncode = None

        def terminate(self) -> None:
            self.returncode = -15
            return None

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.active_processes["single:alice"] = FakeProcess()
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
        await handle_wecom_payload(
            config,
            bot,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-reset"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
            },
            fake_handler,
        )
    finally:
        runtime_module.run_blocking = original_run_blocking

    assert "create_new_session_run" in calls
    assert current_session_id(bot.config, "single:alice").startswith("session-")


async def test_new_session_command_terminates_before_creating_session(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    order: list[str] = []

    class FakeProcess:
        returncode = None

        def terminate(self) -> None:
            order.append("terminate")

        async def wait(self) -> None:
            order.append("wait")
            self.returncode = -15

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.active_processes["single:alice"] = FakeProcess()

    async def fake_run_blocking(func, *args, **kwargs):
        order.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
        await handle_wecom_payload(
            config,
            bot,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-reset"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
            },
            fake_handler,
        )
    finally:
        runtime_module.run_blocking = original_run_blocking

    assert order.index("terminate") < order.index("wait") < order.index("create_new_session_run")


async def test_new_session_command_waits_for_process_exit_before_switching_binding(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    order: list[str] = []

    class FakeProcess:
        returncode = None

        def terminate(self) -> None:
            order.append("terminate")

        async def wait(self) -> None:
            order.append("wait")
            self.returncode = -15

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    bot.active_processes["single:alice"] = FakeProcess()

    async def fake_run_blocking(func, *args, **kwargs):
        order.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
        await handle_wecom_payload(
            config,
            bot,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-reset"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
            },
            fake_handler,
        )
    finally:
        runtime_module.run_blocking = original_run_blocking

    assert order.index("terminate") < order.index("wait") < order.index("create_new_session_run")


async def test_new_session_keeps_context_when_process_does_not_exit(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module
    from workspace_bridge.runtime import prepare_session_run, session_codex_home_root, update_session_record

    class StuckProcess:
        returncode = None

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

        async def wait(self) -> None:
            await asyncio.Future()

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    launch = prepare_session_run(bot.config, "single:alice")
    update_session_record(
        bot.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(current, thread_id="keep-thread", recent_chat=({"role": "user", "text": "keep-chat"},)),
    )
    bot.session_threads["single:alice"] = "keep-thread"
    session_home = session_codex_home_root(bot.config.runtime_root) / launch.session.session_id
    session_home.mkdir(parents=True, exist_ok=True)
    marker = session_home / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    process = StuckProcess()
    bot.active_processes["single:alice"] = process

    original_confirm = runtime_module._confirm_process_exit

    async def confirm_quickly(target, timeout_sec=0.01):
        await original_confirm(target, timeout_sec=timeout_sec)

    monkeypatch.setattr(runtime_module, "_confirm_process_exit", confirm_quickly)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-new"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
        },
        fake_handler,
    )

    stored = runtime_module.load_session_record(bot.config.runtime_root, launch.session.session_id)
    assert stored.thread_id == "keep-thread"
    assert stored.recent_chat == ({"role": "user", "text": "keep-chat"},)
    assert bot.session_threads["single:alice"] == "keep-thread"
    assert bot.active_processes["single:alice"] is process
    assert marker.read_text(encoding="utf-8") == "keep"
    assert "did not exit" in ws.sent[-1]["body"]["stream"]["content"]


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


async def test_bridge_interrupt_command_cancels_active_message_task(tmp_path) -> None:
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
    task = asyncio.create_task(asyncio.sleep(3600))
    bot.active_processes["single:alice"] = process
    bot.active_message_tasks["single:alice"] = task

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-interrupt"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-interrupt"}},
        },
        fake_handler,
    )
    await asyncio.sleep(0)

    assert process.terminated is True
    assert task.cancelled() is True
    assert "single:alice" not in bot.active_message_tasks
    assert ws.sent[-1]["body"]["stream"]["content"] == "Current task interrupted."


async def test_bridge_interrupt_command_only_clears_current_chat_state(tmp_path) -> None:
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
    bot.pending_finals["req-alice"] = {"headers": {"req_id": "req-alice"}}
    bot.pending_finals["req-bob"] = {"headers": {"req_id": "req-bob"}}

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-interrupt"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-interrupt"}},
        },
        fake_handler,
    )

    assert "req-alice" not in bot.reply_states
    assert "req-bob" in bot.reply_states
    assert "req-alice" not in bot.pending_streams
    assert "req-bob" in bot.pending_streams
    assert "req-alice" not in bot.pending_finals
    assert "req-bob" in bot.pending_finals


async def test_bridge_interrupt_command_cancels_active_schedule_run(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.schedule import ScheduledJob, create_one_shot_schedule, read_schedule_definition, schedule_processing_root, write_scheduled_job

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    task = asyncio.create_task(asyncio.sleep(3600))
    create_one_shot_schedule(
        bot.config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    bot.active_schedule_tasks["single:alice"] = task
    bot.active_schedule_runs["single:alice"] = ("schedule-1", "job-1")
    get_or_create_reply_state(bot, "job:job-1", "session-1", "single:alice")
    bot.pending_finals["job:job-1"] = {"headers": {"req_id": "job:job-1"}}
    write_scheduled_job(
        schedule_processing_root(bot.config.runtime_root) / "0000000000000-job-1.1.processing.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-interrupt-schedule"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-interrupt"}},
        },
        fake_handler,
    )
    await asyncio.sleep(0)

    assert task.cancelled() is True
    assert "single:alice" not in bot.active_schedule_tasks
    assert "single:alice" not in bot.active_schedule_runs
    assert "job:job-1" not in bot.pending_finals
    assert "job:job-1" not in bot.reply_states
    assert not list(schedule_processing_root(bot.config.runtime_root).glob("*.json"))
    definition = read_schedule_definition(bot.config.runtime_root, "schedule-1")
    assert definition is not None
    assert definition.enabled is False
    assert definition.run_count == 1
    assert ws.sent[-1]["body"]["stream"]["content"] == "Current task interrupted."


async def test_bridge_interrupt_command_tolerates_non_object_processing_payload(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.schedule import ScheduledJob, create_one_shot_schedule, read_schedule_definition, schedule_processing_root, write_scheduled_job

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    task = asyncio.create_task(asyncio.sleep(3600))
    create_one_shot_schedule(
        bot.config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    bot.active_schedule_tasks["single:alice"] = task
    bot.active_schedule_runs["single:alice"] = ("schedule-1", "job-1")
    get_or_create_reply_state(bot, "job:job-1", "session-1", "single:alice")
    bot.pending_finals["job:job-1"] = {"headers": {"req_id": "job:job-1"}}
    processing_root = schedule_processing_root(bot.config.runtime_root)
    write_scheduled_job(
        processing_root / "0000000000000-job-1.1.processing.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    (processing_root / "broken.processing.json").write_text("[]", encoding="utf-8")

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-interrupt-schedule"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-interrupt"}},
        },
        fake_handler,
    )
    await asyncio.sleep(0)

    assert task.cancelled() is True
    assert "single:alice" not in bot.active_schedule_tasks
    assert "single:alice" not in bot.active_schedule_runs
    assert "job:job-1" not in bot.pending_finals
    assert "job:job-1" not in bot.reply_states
    assert (processing_root / "broken.processing.json").exists()
    definition = read_schedule_definition(bot.config.runtime_root, "schedule-1")
    assert definition is not None
    assert definition.enabled is False
    assert definition.run_count == 1
    assert ws.sent[-1]["body"]["stream"]["content"] == "Current task interrupted."


async def test_new_session_command_cancels_active_schedule_run(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.schedule import ScheduledJob, create_one_shot_schedule, read_schedule_definition, schedule_processing_root, write_scheduled_job

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    task = asyncio.create_task(asyncio.sleep(3600))
    create_one_shot_schedule(
        bot.config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    bot.active_schedule_tasks["single:alice"] = task
    bot.active_schedule_runs["single:alice"] = ("schedule-1", "job-1")
    get_or_create_reply_state(bot, "job:job-1", "session-1", "single:alice")
    bot.pending_streams["job:job-1"] = {"headers": {"req_id": "job:job-1"}}
    write_scheduled_job(
        schedule_processing_root(bot.config.runtime_root) / "0000000000000-job-1.1.processing.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-reset-schedule"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/new"}},
        },
        fake_handler,
    )
    await asyncio.sleep(0)

    assert task.cancelled() is True
    assert "single:alice" not in bot.active_schedule_tasks
    assert "single:alice" not in bot.active_schedule_runs
    assert "job:job-1" not in bot.pending_streams
    assert "job:job-1" not in bot.reply_states
    assert not list(schedule_processing_root(bot.config.runtime_root).glob("*.json"))
    definition = read_schedule_definition(bot.config.runtime_root, "schedule-1")
    assert definition is not None
    assert definition.enabled is False
    assert definition.run_count == 1
    assert ws.sent[-1]["body"]["stream"]["content"] == "New session ready. Your next message will start a new thread."


async def test_resume_command_lists_candidates(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import prepare_session_run
    from workspace_bridge import wecom_runtime as runtime_module

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    launch = prepare_session_run(bot.config, "single:alice")
    other = prepare_session_run(bot.config, "single:bob")
    bot.session_threads["single:alice"] = "thread-a"
    bot.session_threads["single:bob"] = "thread-b"
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for resume list")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
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
    finally:
        runtime_module.run_blocking = original_run_blocking

    assert len(ws.sent) == 1
    content = ws.sent[0]["body"]["stream"]["content"]
    assert "可恢复会话" in content
    assert launch.session.session_id in content
    assert other.session.session_id not in content
    assert "list_session_records" in calls


async def test_resume_command_lists_candidates_from_persisted_thread_ids(tmp_path) -> None:
    from dataclasses import replace
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import prepare_session_run, update_session_record
    from workspace_bridge import wecom_runtime as runtime_module

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    launch = prepare_session_run(bot.config, "single:alice")
    update_session_record(
        bot.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(current, thread_id="thread-a"),
    )
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for resume list")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
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
    finally:
        runtime_module.run_blocking = original_run_blocking

    assert len(ws.sent) == 1
    content = ws.sent[0]["body"]["stream"]["content"]
    assert "可恢复会话" in content
    assert launch.session.session_id in content
    assert "list_session_records" in calls


async def test_resume_selection_binds_selected_thread(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run, session_codex_home_root
    from workspace_bridge import wecom_runtime as runtime_module

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws
    current = prepare_session_run(runtime.config, "single:alice")
    target = prepare_session_run(runtime.config, "group-user:room-1:alice")
    runtime.session_threads["group-user:room-1:alice"] = "thread-target"
    source_rollout = (
        session_codex_home_root(runtime.config.runtime_root)
        / target.session.session_id
        / "sessions"
        / "rollout-target.jsonl"
    )
    source_rollout.parent.mkdir(parents=True, exist_ok=True)
    source_rollout.write_text("target rollout", encoding="utf-8")
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for resume selection")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
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
    finally:
        runtime_module.run_blocking = original_run_blocking

    assert any(target.session.session_id in item["body"]["stream"]["content"] for item in ws.sent if item["body"]["stream"]["finish"])
    updated = load_session_record(runtime.config.runtime_root, current.session.session_id)
    assert updated is not None
    assert updated.thread_id == "thread-target"
    copied_rollout = (
        session_codex_home_root(runtime.config.runtime_root)
        / current.session.session_id
        / "sessions"
        / source_rollout.name
    )
    assert copied_rollout.read_text(encoding="utf-8") == "target rollout"
    assert runtime.session_threads["single:alice"] == "thread-target"
    assert runtime.resume_candidates == {}
    assert "list_session_records" in calls
    assert "prepare_session_run" in calls


async def test_resume_binding_uses_latest_source_thread_not_stale_candidate(tmp_path) -> None:
    from dataclasses import replace
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run, update_session_record
    from workspace_bridge.wecom_runtime import _bind_resume_candidate

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    current = prepare_session_run(runtime.config, "single:alice")
    source = prepare_session_run(runtime.config, "group-user:room-1:alice")
    update_session_record(
        runtime.config.runtime_root,
        source.session.session_id,
        lambda record: replace(record, thread_id="thread-latest"),
    )

    source_session_id = await _bind_resume_candidate(
        runtime,
        "single:alice",
        {
            "sessionId": source.session.session_id,
            "threadId": "thread-stale",
            "chatKey": "group-user:room-1:alice",
            "updatedAt": source.session.updated_at,
            "lastRunAt": 0,
        },
    )

    updated = load_session_record(runtime.config.runtime_root, current.session.session_id)
    assert source_session_id == source.session.session_id
    assert updated is not None
    assert updated.thread_id == "thread-latest"
    assert runtime.session_threads["single:alice"] == "thread-latest"


async def test_resume_binding_reports_missing_source_record_cleanly(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import prepare_session_run
    from workspace_bridge import wecom_runtime as runtime_module

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    prepare_session_run(runtime.config, "single:alice")
    monkeypatch.setattr(runtime_module, "list_session_records", lambda *_args: [])

    with pytest.raises(RuntimeError, match="selected session is no longer resumable"):
        await runtime_module._bind_resume_candidate(
            runtime,
            "single:alice",
            {
                "sessionId": "missing-session",
                "threadId": "stale-thread",
                "chatKey": "group-user:room-1:alice",
                "updatedAt": 0,
                "lastRunAt": 0,
            },
        )


async def test_resume_selection_copies_recent_chat_from_selected_session(tmp_path) -> None:
    from dataclasses import replace
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run, update_session_record
    from workspace_bridge import wecom_runtime as runtime_module

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws
    current = prepare_session_run(runtime.config, "single:alice")
    target = prepare_session_run(runtime.config, "group-user:room-1:alice")
    update_session_record(
        runtime.config.runtime_root,
        target.session.session_id,
        lambda current_record: replace(
            current_record,
            thread_id="thread-target",
            recent_chat=(
                {"role": "user", "text": "older user turn"},
                {"role": "bot", "text": "older bot turn"},
            ),
        ),
    )
    runtime.session_threads["group-user:room-1:alice"] = "thread-target"
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for resume selection")

    original_run_blocking = runtime_module.run_blocking
    runtime_module.run_blocking = fake_run_blocking
    try:
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
    finally:
        runtime_module.run_blocking = original_run_blocking

    updated = load_session_record(runtime.config.runtime_root, current.session.session_id)
    assert updated is not None
    assert updated.thread_id == "thread-target"
    assert tuple(item["text"] for item in updated.recent_chat) == ("older user turn", "older bot turn")
    assert runtime.resume_candidates == {}
    assert "list_session_records" in calls
    assert "prepare_session_run" in calls


async def test_ack_only_payload_flushes_cached_final_reply(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.wecom_protocol import build_text_response_payloads

    class AckingWS:
        def __init__(self, runtime) -> None:
            self.runtime = runtime
            self.sent = []

        async def send_json(self, payload):
            self.sent.append(payload)
            req_id = ((payload.get("headers") or {}).get("req_id"))
            future = self.runtime.pending_requests.get(req_id) if req_id else None
            if future is not None and not future.done():
                future.set_result({"headers": {"req_id": req_id}, "errcode": 0, "body": {}})

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = AckingWS(runtime)
    runtime.ws = ws
    payloads = build_text_response_payloads("req-1", "session-1", "x" * 8000, final=True)
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    cache_reply_payload(state, payloads[-1], final=True, payloads=payloads[1:])
    runtime.pending_finals["req-1"] = payloads[1:]

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for ack-only payload")

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "headers": {"req_id": "ack-1"},
            "errcode": 0,
            "errmsg": "ok",
        },
        fake_handler,
    )

    assert ws.sent == payloads[1:]
    assert runtime.pending_finals == {}
    assert "req-1" not in runtime.reply_states


async def test_ack_only_payload_flushes_cached_final_reply_for_string_zero_errcode(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.wecom_protocol import build_text_response_payloads

    class AckingWS:
        def __init__(self, runtime) -> None:
            self.runtime = runtime
            self.sent = []

        async def send_json(self, payload):
            self.sent.append(payload)
            req_id = ((payload.get("headers") or {}).get("req_id"))
            future = self.runtime.pending_requests.get(req_id) if req_id else None
            if future is not None and not future.done():
                future.set_result({"headers": {"req_id": req_id}, "errcode": "0", "body": {}})

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = AckingWS(runtime)
    runtime.ws = ws
    payloads = build_text_response_payloads("req-1", "session-1", "x" * 8000, final=True)
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    cache_reply_payload(state, payloads[-1], final=True, payloads=payloads[1:])
    runtime.pending_finals["req-1"] = payloads[1:]

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for ack-only payload")

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "headers": {"req_id": "ack-1"},
            "errcode": "0",
            "errmsg": "ok",
        },
        fake_handler,
    )

    assert ws.sent == payloads[1:]
    assert runtime.pending_finals == {}
    assert "req-1" not in runtime.reply_states


async def test_flush_cached_runtime_payloads_do_not_reenter_on_chunk_ack(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.wecom_protocol import build_text_response_payloads

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    payloads = build_text_response_payloads("req-1", "session-1", "x" * 8000, final=True)
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    cache_reply_payload(state, payloads[-1], final=True, payloads=payloads)
    runtime.pending_finals["req-1"] = payloads
    sent = []

    class ReentrantAckWS:
        def __init__(self, runtime_) -> None:
            self.runtime = runtime_
            self.sent = []

        async def send_json(self, payload):
            self.sent.append(payload)
            sent.append(payload)
            await handle_wecom_payload(
                config,
                self.runtime,
                self,
                {
                    "headers": {"req_id": payload["headers"]["req_id"]},
                    "errcode": 0,
                    "errmsg": "ok",
                },
                lambda *_args, **_kwargs: None,
            )

    ws = ReentrantAckWS(runtime)
    runtime.ws = ws

    from workspace_bridge.execution import flush_cached_runtime_payloads

    await flush_cached_runtime_payloads(runtime)

    assert sent == payloads
    assert runtime.pending_finals == {}
    assert "req-1" not in runtime.reply_states


async def test_disconnected_event_closes_active_websocket(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    class ClosableWS(FakeWS):
        def __init__(self):
            super().__init__([])
            self.closed = False
            self.close_calls = 0

        async def close(self):
            self.close_calls += 1
            self.closed = True

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = ClosableWS()
    runtime.ws = ws

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for disconnected event")

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_event_callback",
            "headers": {"req_id": "req-disconnect"},
            "body": {"msgtype": "event", "event": {"eventtype": "disconnected_event"}},
        },
        fake_handler,
    )

    assert ws.closed is True
    assert ws.close_calls == 1
    assert runtime.wecom_status == "websocket_disconnected_event"


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
            "FILE_SEND_ROOTS": str(tmp_path / "runtime" / "workspaces" / "bot-1" / "users"),
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
    assert runtime.wecom_status == "subscribe_failed"
    assert runtime.wecom_last_error == "bad secret"


async def test_run_wecom_runtime_rejects_non_dict_subscribe_payload(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    class FakeMsg:
        def __init__(self, data):
            self.type = WSMsgType.TEXT
            self.data = data

    class FakeWSClient:
        def __init__(self):
            self.sent = []
            self.payloads = [FakeMsg("[]")]

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
        assert "unexpected subscribe payload shape" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")

    assert runtime.connected is False
    assert runtime.wecom_status == "subscribe_failed"
    assert runtime.wecom_last_error == "unexpected subscribe payload shape"


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


async def test_run_wecom_runtime_once_marks_websocket_error_when_cached_replay_fails(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    class FakeWSClient:
        def __init__(self) -> None:
            self.sent = []
            self.send_calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def send_json(self, payload):
            self.send_calls += 1
            if self.send_calls == 1:
                self.sent.append(payload)
                return None
            raise RuntimeError("socket write failed")

        async def receive(self):
            return type("Msg", (), {"type": WSMsgType.TEXT, "data": json.dumps({"errcode": 0, "errmsg": "ok"})})()

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
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    payload = {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream", "stream": {"content": "cached"}}}
    cache_reply_payload(state, payload, final=False, payloads=[payload])
    runtime.pending_streams["req-1"] = [payload]

    with pytest.raises(RuntimeError, match="socket write failed"):
        await runtime_module.run_wecom_runtime_once(config, runtime)

    assert runtime.connected is False
    assert runtime.ws is None
    assert runtime.wecom_status == "websocket_error"
    assert runtime.wecom_last_error == "socket write failed"
    assert runtime.pending_streams["req-1"] == [payload]
    assert runtime.reply_states["req-1"].pending_stream_payloads == [payload]


async def test_run_wecom_runtime_once_raises_from_wecom_error_field_after_state_split(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    class FakeMsg:
        def __init__(self, payload):
            self.type = WSMsgType.TEXT
            self.data = json.dumps(payload)

    class FakeWSClient:
        def __init__(self) -> None:
            self.sent = []
            self.payloads = [
                FakeMsg({"cmd": "aibot_subscribe", "headers": {"req_id": "req-1"}, "errcode": 40001, "errmsg": "bad secret"}),
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
    runtime.last_error = "stale runtime error"

    with pytest.raises(RuntimeError, match="bad secret"):
        await runtime_module.run_wecom_runtime_once(config, runtime)

    assert runtime.wecom_status == "subscribe_failed"
    assert runtime.wecom_last_error == "bad secret"


async def test_message_failure_reply_uses_stable_session_id(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, stable_session_id
    from workspace_bridge import wecom_runtime as runtime_module

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws
    launch = prepare_session_run(runtime.config, "single:alice")
    before = load_session_record(runtime.config.runtime_root, launch.session.session_id)

    async def fake_stream_text_message_once(*_args, **_kwargs):
        raise RuntimeError("boom")

    original_stream = runtime_module.stream_text_message_once
    runtime_module.stream_text_message_once = fake_stream_text_message_once
    try:
        await handle_wecom_payload(
            config,
            runtime,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-1"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
            },
            runtime_module._dispatch_message,
        )
        for _ in range(100):
            if ws.sent:
                break
            await asyncio.sleep(0.01)
    finally:
        runtime_module.stream_text_message_once = original_stream

    assert ws.sent[-1]["body"]["stream"]["id"] == stable_session_id(runtime.config.bot_id, "single:alice", runtime.config.source.source_dir)
    assert "执行失败: boom" in ws.sent[-1]["body"]["stream"]["content"]
    after = load_session_record(runtime.config.runtime_root, launch.session.session_id)
    assert after is not None and before is not None
    assert after.last_run_at is not None
    assert int(after.last_run_at) >= int(before.updated_at)


async def test_failed_message_task_preserves_message_failed_status_after_reply(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    async def fake_stream_text_message_once(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
        },
        runtime_module._dispatch_message,
    )
    for _ in range(100):
        if any("执行失败: boom" in item["body"]["stream"]["content"] for item in ws.sent if item["body"]["stream"]["finish"]):
            break
        await asyncio.sleep(0.01)

    assert runtime.last_status == "message_failed"
    assert runtime.last_error == "boom"
    assert any("执行失败: boom" in item["body"]["stream"]["content"] for item in ws.sent if item["body"]["stream"]["finish"])


async def test_failed_message_record_update_waits_for_session_guard(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import session_run_guards
    from workspace_bridge import wecom_runtime as runtime_module

    async def fake_stream_text_message_once(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-guarded-failure"},
        "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
    }

    async with session_run_guards(runtime.config, "single:alice"):
        await handle_wecom_payload(config, runtime, ws, payload, runtime_module._dispatch_message)
        await asyncio.sleep(0.05)
        assert not any("执行失败: boom" in item["body"]["stream"]["content"] for item in ws.sent)

    for _ in range(100):
        if any("执行失败: boom" in item["body"]["stream"]["content"] for item in ws.sent):
            break
        await asyncio.sleep(0.01)

    assert any("执行失败: boom" in item["body"]["stream"]["content"] for item in ws.sent)


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


async def test_successful_message_clears_previous_message_failed_status(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    async def fake_stream_text_message_once(_config, _runtime, _parsed, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    runtime.last_status = "message_failed"
    runtime.last_error = "old boom"
    ws = FakeWS([])

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
        },
        runtime_module._dispatch_message,
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert runtime.last_status is None
    assert runtime.last_error is None


async def test_interrupt_suppressed_failure_does_not_mark_runtime_failed(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    async def fake_stream_text_message_once(*_args, **_kwargs):
        raise RuntimeError("process terminated by interrupt")

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-1"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
        },
        runtime_module._dispatch_message,
    )
    await asyncio.sleep(0)

    assert runtime.last_status is None
    assert runtime.last_error is None
    assert ws.sent == []


async def test_interrupt_command_suppresses_nonmatching_failure_after_task_cancel(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    started = asyncio.Event()

    async def fake_stream_text_message_once(_config, _runtime, _parsed, **_kwargs):
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            raise RuntimeError("signal 15")

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-run"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
        },
        runtime_module._dispatch_message,
    )
    await started.wait()
    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-interrupt"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-interrupt"}},
        },
        runtime_module._dispatch_message,
    )
    await asyncio.sleep(0)

    assert runtime.last_status is None
    assert runtime.last_error is None
    await asyncio.sleep(0)
    assert "single:alice" not in runtime.active_message_tasks


async def test_interrupt_suppression_does_not_hide_next_real_failure(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    started = asyncio.Event()
    release_old = asyncio.Event()
    calls = {"count": 0}

    async def fake_stream_text_message_once(_config, _runtime, _parsed, **_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await release_old.wait()
                raise RuntimeError("signal 15")
        raise RuntimeError("real boom")

    monkeypatch.setattr(runtime_module, "stream_text_message_once", fake_stream_text_message_once)
    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    runtime.ws = ws

    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-run"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "hello"}},
        },
        runtime_module._dispatch_message,
    )
    await started.wait()
    release_old.set()
    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-interrupt"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-interrupt"}},
        },
        runtime_module._dispatch_message,
    )
    await handle_wecom_payload(
        config,
        runtime,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-next"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "retry"}},
        },
        runtime_module._dispatch_message,
    )
    await asyncio.sleep(0)
    for _ in range(100):
        if any("执行失败: real boom" in item["body"]["stream"]["content"] for item in ws.sent if item["body"]["stream"]["finish"]):
            break
        await asyncio.sleep(0.01)

    assert runtime.last_status == "message_failed"
    assert runtime.last_error == "real boom"
    assert any("执行失败: real boom" in item["body"]["stream"]["content"] for item in ws.sent if item["body"]["stream"]["finish"])


async def test_dispatch_message_rejects_concurrent_same_chat(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module
    from workspace_bridge.runtime import stable_session_id

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
    assert ws.sent[-1]["body"]["stream"]["id"] == stable_session_id(bot.config.bot_id, "single:alice", bot.config.source.source_dir)


async def test_dispatch_message_rejects_when_schedule_run_is_active_for_chat(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module
    from workspace_bridge.runtime import stable_session_id

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    task = asyncio.create_task(asyncio.sleep(3600))
    bot.active_schedule_tasks["single:alice"] = task
    bot.active_schedule_runs["single:alice"] = ("schedule-1", "job-1")

    try:
        await handle_wecom_payload(
            config,
            bot,
            ws,
            {
                "cmd": "aibot_msg_callback",
                "headers": {"req_id": "req-2"},
                "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "again"}},
            },
            runtime_module._dispatch_message,
        )
    finally:
        task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError):
            await task

    assert ws.sent
    assert "已有任务在运行" in ws.sent[-1]["body"]["stream"]["content"]
    assert ws.sent[-1]["body"]["stream"]["id"] == stable_session_id(bot.config.bot_id, "single:alice", bot.config.source.source_dir)


async def test_dispatch_message_allows_concurrent_different_chats(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import wecom_runtime as runtime_module

    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    async def fake_stream_text_message_once(_config, _runtime, parsed, **_kwargs):
        calls.append(parsed.chat_key)
        started.set()
        await release.wait()
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
        "body": {"msgtype": "text", "from": {"userid": "bob"}, "text": {"content": "again"}},
    }

    await handle_wecom_payload(config, bot, ws, first_payload, runtime_module._dispatch_message)
    await started.wait()
    await handle_wecom_payload(config, bot, ws, second_payload, runtime_module._dispatch_message)
    await asyncio.sleep(0)
    release.set()
    await asyncio.sleep(0)

    assert sorted(calls) == ["single:alice", "single:bob"]
    assert ws.sent == []


async def test_bridge_reset_command_clears_thread_and_chat_context(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run, update_session_record

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    ws = FakeWS([])
    bot.ws = ws
    launch = prepare_session_run(bot.config, "single:alice")
    update_session_record(
        bot.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(
            current,
            thread_id="reset-thread",
            recent_chat=({"role": "user", "text": "clear me"},),
        ),
    )
    bot.session_threads["single:alice"] = "keep-thread"

    async def fake_handler(*_args, **_kwargs):
        raise AssertionError("handler should not be called for bridge command")

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {
            "cmd": "aibot_msg_callback",
            "headers": {"req_id": "req-old-reset"},
            "body": {"msgtype": "text", "from": {"userid": "alice"}, "text": {"content": "/bridge-reset"}},
        },
        fake_handler,
    )

    assert "single:alice" not in bot.session_threads
    stored = load_session_record(bot.config.runtime_root, launch.session.session_id)
    assert stored is not None
    assert stored.thread_id is None
    assert stored.recent_chat == ()
    assert ws.sent[-1]["body"]["stream"]["content"] == "Session reset. Your next message will start a new thread."
