from pathlib import Path
from types import SimpleNamespace

from workspace_bridge.config import load_app_config
from workspace_bridge import execution as execution_module
from workspace_bridge.execution import execute_and_deliver_message, run_text_message_once, stream_text_message_once
from workspace_bridge.service import APP_WECOM_RUNTIME_KEY, APP_WECOM_TASK_KEY, create_app
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.wecom_protocol import WeComTextMessage


def write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def make_config(tmp_path: Path, *, wecom_enabled: bool = False):
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
            "WECOM_ENABLED": "true" if wecom_enabled else "false",
        }
    )


async def test_run_text_message_once_executes_override_and_returns_output(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    session_id, reply = await run_text_message_once(
        config,
        bot,
        message,
        argv_override=("python", "-c", "print('done')"),
    )

    assert session_id.startswith("session-")
    assert "done" in reply


async def test_stream_text_message_once_emits_status_then_final(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    class FakeWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation
    execution_module.run_invocation = lambda _invocation: SimpleNamespace(returncode=0, stdout="done\n", stderr="")
    try:
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            message,
            argv_override=("python", "-c", "print('done')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation

    assert session_id.startswith("session-")
    assert "done" in reply
    assert len(runtime.ws.sent) == 2
    assert runtime.ws.sent[0]["body"]["stream"]["finish"] is False
    assert "思考中" in runtime.ws.sent[0]["body"]["stream"]["content"]
    assert runtime.ws.sent[1]["body"]["stream"]["finish"] is False


async def test_service_lifecycle_skips_wecom_task_when_disabled(tmp_path: Path) -> None:
    config = make_config(tmp_path, wecom_enabled=False)
    app = create_app(config)

    assert app[APP_WECOM_RUNTIME_KEY] is not None
    for callback in app.on_startup:
        await callback(app)
    assert app[APP_WECOM_TASK_KEY] is None
    for callback in app.on_cleanup:
        await callback(app)


async def test_execute_and_deliver_message_caches_final_when_ws_missing(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation
    execution_module.run_invocation = lambda _invocation: SimpleNamespace(returncode=0, stdout="done\n", stderr="")
    try:
        session_id, reply = await execute_and_deliver_message(
            config,
            runtime,
            message,
            argv_override=("python", "-c", "print('done')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation

    assert session_id.startswith("session-")
    assert "done" in reply
    assert runtime.pending_finals == {}
    assert runtime.reply_states["req-1"].pending_final_payload is not None


async def test_send_or_cache_runtime_payload_uses_reply_state_cache_for_req_id(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "status", final=False)

    assert delivered is False
    assert runtime.pending_streams == {}
    assert runtime.reply_states["req-1"].pending_stream_payload is not None
