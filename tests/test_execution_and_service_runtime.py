import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from workspace_bridge.config import load_app_config
from workspace_bridge import execution as execution_module
from workspace_bridge.execution import (
    execute_and_deliver_message,
    extract_codex_stdout_text,
    run_text_message_once,
    stream_text_message_once,
)
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

    class FakeStdin:
        def write(self, _data: bytes) -> None:
            return None

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            return None

        async def wait_closed(self) -> None:
            return None

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.returncode = None

        async def wait(self) -> int:
            self.stdout.feed_data(b"done\n")
            self.returncode = 0
            self.stdout.feed_eof()
            self.stderr.feed_eof()
            return 0

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            message,
            argv_override=("python", "-c", "print('done')"),
        )
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert session_id.startswith("session-")
    assert "done" in reply
    assert len(runtime.ws.sent) >= 2
    assert runtime.ws.sent[0]["body"]["stream"]["finish"] is False
    assert "思考中" in runtime.ws.sent[0]["body"]["stream"]["content"]
    assert runtime.ws.sent[-1]["body"]["stream"]["finish"] is True
    assert "single:alice" not in runtime.active_processes


async def test_service_lifecycle_skips_wecom_task_when_disabled(tmp_path: Path) -> None:
    config = make_config(tmp_path, wecom_enabled=False)
    app = create_app(config)

    assert app[APP_WECOM_RUNTIME_KEY] is not None
    for callback in app.on_startup:
        await callback(app)
    assert app[APP_WECOM_TASK_KEY] is None
    for callback in app.on_cleanup:
        await callback(app)


async def test_service_lifecycle_keeps_health_safe_when_wecom_enabled(tmp_path: Path) -> None:
    from aiohttp.test_utils import make_mocked_request
    from workspace_bridge import service as service_module

    config = make_config(tmp_path, wecom_enabled=True)
    app = create_app(config)
    started = {"value": False}

    async def fake_run_wecom_runtime(_config, _runtime):
        started["value"] = True
        await asyncio.sleep(3600)

    original = service_module.run_wecom_runtime
    service_module.run_wecom_runtime = fake_run_wecom_runtime
    try:
        for callback in app.on_startup:
            await callback(app)

        request = make_mocked_request("GET", "/", app=app)
        route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/")
        response = await route.handler(request)
        payload = json.loads(response.text)

        assert payload["ok"] is True
        assert payload["wecomTaskPresent"] is True
        assert payload["wecomTaskDone"] is False
        assert started["value"] is False

        await asyncio.sleep(0)
        assert started["value"] is True
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.run_wecom_runtime = original


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
    assert "req-1" in runtime.pending_finals
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
    assert "req-1" in runtime.pending_streams
    assert runtime.reply_states["req-1"].pending_stream_payload is not None


async def test_send_or_cache_runtime_payload_falls_back_to_cache_on_send_error(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    class BrokenWS:
        async def send_json(self, _payload: dict) -> None:
            raise RuntimeError("socket closed")

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = BrokenWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "final", final=True)

    assert delivered is False
    assert "req-1" in runtime.pending_finals
    assert runtime.reply_states["req-1"].pending_final_payload is not None
    assert runtime.last_error == "socket closed"


def test_extract_codex_stdout_text_prefers_latest_agent_message() -> None:
    stdout = "\n".join(
        [
            json.dumps({"type": "thread.started", "thread_id": "t-1"}, ensure_ascii=False),
            json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "first"}}, ensure_ascii=False),
            json.dumps({"type": "turn.completed", "usage": {"outputtokens": 1}}, ensure_ascii=False),
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "second"}}, ensure_ascii=False),
        ]
    )

    assert extract_codex_stdout_text(stdout) == "second"


async def test_run_text_message_once_prefers_output_file_text_over_json_stdout(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    output_root = Path(config.codex_output_root)
    original_run_invocation = execution_module.run_invocation

    def fake_run_invocation(invocation):
        output_path = output_root / "session-1.jsonl"
        output_path.write_text("final from output file\n", encoding="utf-8")
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "from stdout"}}) + "\n",
            stderr="",
        )

    execution_module.run_invocation = fake_run_invocation
    original_prepare_session_run = execution_module.prepare_session_run
    original_build_prompt = execution_module.build_prompt
    execution_module.prepare_session_run = lambda _bot, _chat_key: SimpleNamespace(
        cwd=bot.source.source_dir,
        env={},
        session=SimpleNamespace(session_id="session-1"),
    )
    execution_module.build_prompt = lambda _bot, _launch, _content: "hello"
    try:
        session_id, reply = await run_text_message_once(
            config,
            bot,
            message,
            argv_override=("python", "-c", "print('unused')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation
        execution_module.prepare_session_run = original_prepare_session_run
        execution_module.build_prompt = original_build_prompt

    assert session_id == "session-1"
    assert reply == "final from output file"


async def test_stream_text_message_once_uses_json_stdout_when_output_file_missing(tmp_path: Path) -> None:
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
    class FakeStdin:
        def write(self, _data: bytes) -> None:
            return None

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            return None

        async def wait_closed(self) -> None:
            return None

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.returncode = None

        async def wait(self) -> int:
            self.stdout.feed_data(
                (
                    "\n".join(
                        [
                            json.dumps({"type": "thread.started", "thread_id": "019e062c"}, ensure_ascii=False),
                            json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "Hi. What do you need help with?"}}, ensure_ascii=False),
                            json.dumps({"type": "turn.completed", "usage": {"outputtokens": 107}}, ensure_ascii=False),
                        ]
                    )
                    + "\n"
                ).encode("utf-8")
            )
            self.returncode = 0
            self.stdout.feed_eof()
            self.stderr.feed_eof()
            return 0

    original_prepare_session_run = execution_module.prepare_session_run
    original_build_prompt = execution_module.build_prompt
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    execution_module.prepare_session_run = lambda _bot, _chat_key: SimpleNamespace(
        cwd=bot.source.source_dir,
        env={},
        session=SimpleNamespace(session_id="session-2"),
    )
    execution_module.build_prompt = lambda _bot, _launch, _content: "hello"
    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()
    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            message,
            argv_override=("python", "-c", "print('unused')"),
        )
    finally:
        execution_module.prepare_session_run = original_prepare_session_run
        execution_module.build_prompt = original_build_prompt
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert session_id == "session-2"
    assert reply == "Hi. What do you need help with?"


async def test_stream_text_message_once_uses_communicate_when_available(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    class FakeWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    class FakeStdin:
        def write(self, _data: bytes) -> None:
            return None

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            return None

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = FakeStdin()
            self.stdout = None
            self.stderr = None

        async def communicate(self):
            return (
                (
                    json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "from communicate"}}, ensure_ascii=False)
                    + "\n"
                ).encode("utf-8"),
                b"",
            )

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()
    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            message,
            argv_override=("python", "-c", "print('unused')"),
        )
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert session_id.startswith("session-")
    assert reply == "from communicate"


async def test_stream_text_message_once_streams_latest_message_during_run(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    class FakeWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

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

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = FakeStdin()
            self.stdout = asyncio.StreamReader()
            self.stderr = asyncio.StreamReader()
            self.returncode = None

        async def wait(self) -> int:
            await asyncio.sleep(0.05)
            self.stdout.feed_data(
                (
                    json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "stream body"}}, ensure_ascii=False)
                    + "\n"
                ).encode("utf-8")
            )
            await asyncio.sleep(0.35)
            self.returncode = 0
            self.stdout.feed_eof()
            self.stderr.feed_eof()
            return 0

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    original_prepare_session_run = execution_module.prepare_session_run
    original_build_prompt = execution_module.build_prompt
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    original_interval = execution_module.STATUS_STREAM_INTERVAL_SEC
    execution_module.prepare_session_run = lambda _bot, _chat_key: SimpleNamespace(
        cwd=bot.source.source_dir,
        env={},
        session=SimpleNamespace(session_id="session-3"),
    )
    execution_module.build_prompt = lambda _bot, _launch, _content: "hello"
    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()
    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    execution_module.STATUS_STREAM_INTERVAL_SEC = 0.1
    try:
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            message,
            argv_override=("python", "-c", "print('unused')"),
        )
    finally:
        execution_module.prepare_session_run = original_prepare_session_run
        execution_module.build_prompt = original_build_prompt
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec
        execution_module.STATUS_STREAM_INTERVAL_SEC = original_interval

    assert session_id == "session-3"
    assert reply == "stream body"
    assert len(runtime.ws.sent) >= 3
    assert "思考中" in runtime.ws.sent[0]["body"]["stream"]["content"]
    assert any("stream body" in payload["body"]["stream"]["content"] for payload in runtime.ws.sent[1:])
    assert runtime.ws.sent[-1]["body"]["stream"]["finish"] is True


async def test_run_text_message_once_ignores_stale_output_file(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    output_root = Path(config.codex_output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    stale = output_root / "session-stale.jsonl"
    stale.write_text("old output\n", encoding="utf-8")
    original_run_invocation = execution_module.run_invocation
    original_prepare_session_run = execution_module.prepare_session_run
    original_build_prompt = execution_module.build_prompt

    def fake_run_invocation(_invocation):
        return SimpleNamespace(returncode=0, stdout="fresh stdout\n", stderr="")

    execution_module.run_invocation = fake_run_invocation
    execution_module.prepare_session_run = lambda _bot, _chat_key: SimpleNamespace(
        cwd=bot.source.source_dir,
        env={},
        session=SimpleNamespace(session_id="session-stale"),
    )
    execution_module.build_prompt = lambda _bot, _launch, _content: "hello"
    try:
        session_id, reply = await run_text_message_once(
            config,
            bot,
            message,
            argv_override=("python", "-c", "print('unused')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation
        execution_module.prepare_session_run = original_prepare_session_run
        execution_module.build_prompt = original_build_prompt

    assert session_id == "session-stale"
    assert reply == "fresh stdout"


async def test_run_text_message_once_uses_thread_offload_for_blocking_runner(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    marker = {"value": False}
    original_run_invocation = execution_module.run_invocation
    original_to_thread = execution_module.asyncio.to_thread

    def fake_run_invocation(_invocation):
        marker["value"] = True
        return SimpleNamespace(returncode=0, stdout="done\n", stderr="")

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    execution_module.run_invocation = fake_run_invocation
    execution_module.asyncio.to_thread = fake_to_thread
    try:
        await run_text_message_once(
            config,
            bot,
            message,
            argv_override=("python", "-c", "print('done')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation
        execution_module.asyncio.to_thread = original_to_thread

    assert marker["value"] is True


async def test_run_text_message_once_releases_session_lock_after_completion(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import execution as execution_runtime

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    await run_text_message_once(
        config,
        bot,
        message,
        argv_override=("python", "-c", "print('done')"),
    )

    assert execution_runtime._SESSION_RUN_LOCKS == {}


async def test_run_text_message_once_releases_session_lock_after_failure(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import execution as execution_runtime

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation
    original_to_thread = execution_module.asyncio.to_thread

    def boom(_invocation):
        raise RuntimeError("boom")

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    execution_module.run_invocation = boom
    execution_module.asyncio.to_thread = fake_to_thread
    try:
        try:
            await run_text_message_once(
                config,
                bot,
                message,
                argv_override=("python", "-c", "print('done')"),
            )
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected RuntimeError")
    finally:
        execution_module.run_invocation = original_run_invocation
        execution_module.asyncio.to_thread = original_to_thread

    assert execution_runtime._SESSION_RUN_LOCKS == {}
