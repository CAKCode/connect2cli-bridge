import asyncio
import os
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import pytest
from aiohttp.test_utils import make_mocked_request

from workspace_bridge.config import load_app_config
from workspace_bridge import execution as execution_module
from workspace_bridge.execution import (
    _read_execution_reply,
    execute_and_deliver_message,
    extract_codex_stdout_text,
    run_text_message_once,
    stream_text_message_once,
)
from workspace_bridge.service import APP_SCHEDULE_TASK_KEY, APP_WECOM_RUNTIME_KEY, APP_WECOM_TASK_KEY, create_app
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
    original_prepare_session_run = execution_module.prepare_session_run
    original_build_prompt = execution_module.build_prompt
    original_run_invocation = execution_module.run_invocation
    execution_module.prepare_session_run = lambda _bot, _chat_key: SimpleNamespace(
        cwd=bot.source.source_dir,
        env={},
        session=SimpleNamespace(session_id="session-1"),
    )
    execution_module.build_prompt = lambda _bot, _launch, _content: "hello"
    execution_module.run_invocation = lambda _invocation: SimpleNamespace(returncode=0, stdout="done\n", stderr="")

    try:
        session_id, reply = await run_text_message_once(config, bot, message, argv_override=("python", "-c", "print('done')"))
    finally:
        execution_module.prepare_session_run = original_prepare_session_run
        execution_module.build_prompt = original_build_prompt
        execution_module.run_invocation = original_run_invocation

    assert session_id.startswith("session-")
    assert "done" in reply


def test_read_execution_reply_prefers_output_file(tmp_path: Path) -> None:
    output_file = tmp_path / "reply.txt"
    output_file.write_text("final reply\n", encoding="utf-8")

    reply = _read_execution_reply(output_file, "stdout", "stderr")

    assert reply == "final reply"


def test_read_execution_reply_falls_back_when_output_file_empty(tmp_path: Path) -> None:
    output_file = tmp_path / "reply.txt"
    output_file.write_text("\n", encoding="utf-8")

    reply = _read_execution_reply(output_file, "stdout reply\n", "stderr reply\n")

    assert reply == "stdout reply"


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
    from workspace_bridge import service as service_module

    config = make_config(tmp_path, wecom_enabled=False)
    app = create_app(config)
    started = {"schedules": 0}

    async def fake_process_due_schedules_once(_config, _runtime):
        started["schedules"] += 1
        await asyncio.sleep(3600)

    async def fake_process_scheduled_jobs_once(_config, _runtime):
        await asyncio.sleep(3600)

    original_due = service_module.process_due_schedules_once
    original_jobs = service_module.process_scheduled_jobs_once
    service_module.process_due_schedules_once = fake_process_due_schedules_once
    service_module.process_scheduled_jobs_once = fake_process_scheduled_jobs_once

    try:
        assert app[APP_WECOM_RUNTIME_KEY] is not None
        for callback in app.on_startup:
            await callback(app)
        assert app[APP_WECOM_TASK_KEY] is None
        assert app[APP_SCHEDULE_TASK_KEY] is None
        await asyncio.sleep(0)
        assert started["schedules"] == 0
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.process_due_schedules_once = original_due
        service_module.process_scheduled_jobs_once = original_jobs


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
        assert payload["scheduleTaskPresent"] is True
        assert payload["scheduleTaskDone"] is False
        assert started["value"] is False

        await asyncio.sleep(0)
        assert started["value"] is True
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.run_wecom_runtime = original


async def test_service_health_reports_unhealthy_when_wecom_task_done(tmp_path: Path) -> None:
    from aiohttp.test_utils import make_mocked_request
    from workspace_bridge import service as service_module

    config = make_config(tmp_path, wecom_enabled=True)
    app = create_app(config)

    async def fake_run_wecom_runtime(_config, _runtime):
        return None

    original = service_module.run_wecom_runtime
    service_module.run_wecom_runtime = fake_run_wecom_runtime
    try:
        for callback in app.on_startup:
            await callback(app)
        await asyncio.sleep(0)

        request = make_mocked_request("GET", "/", app=app)
        route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/")
        response = await route.handler(request)
        payload = json.loads(response.text)

        assert response.status == 503
        assert payload["ok"] is False
        assert payload["wecomTaskPresent"] is True
        assert payload["wecomTaskDone"] is True
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.run_wecom_runtime = original


async def test_service_schedule_loop_does_not_consume_jobs_while_wecom_disconnected(tmp_path: Path) -> None:
    from workspace_bridge import service as service_module

    config = make_config(tmp_path, wecom_enabled=True)
    app = create_app(config)
    calls = {"due": 0, "jobs": 0}

    async def fake_run_wecom_runtime(_config, _runtime):
        await asyncio.sleep(3600)

    async def fake_process_due_schedules_once(_config, _runtime):
        calls["due"] += 1

    async def fake_process_scheduled_jobs_once(_config, _runtime):
        calls["jobs"] += 1

    original_run = service_module.run_wecom_runtime
    original_due = service_module.process_due_schedules_once
    original_jobs = service_module.process_scheduled_jobs_once
    service_module.run_wecom_runtime = fake_run_wecom_runtime
    service_module.process_due_schedules_once = fake_process_due_schedules_once
    service_module.process_scheduled_jobs_once = fake_process_scheduled_jobs_once
    try:
        for callback in app.on_startup:
            await callback(app)
        await asyncio.sleep(0)
        assert calls == {"due": 0, "jobs": 0}
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.run_wecom_runtime = original_run
        service_module.process_due_schedules_once = original_due
        service_module.process_scheduled_jobs_once = original_jobs


async def test_schedule_processing_is_deferred_until_connection_recovers(tmp_path: Path) -> None:
    from workspace_bridge.schedule import create_one_shot_schedule, read_schedule_definition
    from workspace_bridge import service as service_module

    config = make_config(tmp_path, wecom_enabled=True)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    app = create_app(config)

    calls = {"executions": 0}

    async def fake_run_wecom_runtime(_config, _runtime):
        await asyncio.sleep(3600)

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        calls["executions"] += 1
        return "session-1", "done"

    original_run = service_module.run_wecom_runtime
    original_exec = service_module.process_due_schedules_once
    service_module.run_wecom_runtime = fake_run_wecom_runtime

    async def wrapped_due(config_arg, runtime_arg):
        from workspace_bridge.schedule_runtime import process_due_schedules_once as real_due
        from workspace_bridge import schedule_runtime as schedule_runtime_module

        original_delivery = schedule_runtime_module.execute_and_deliver_message
        schedule_runtime_module.execute_and_deliver_message = fake_execute_and_deliver_message
        try:
            return await real_due(config_arg, runtime_arg)
        finally:
            schedule_runtime_module.execute_and_deliver_message = original_delivery

    service_module.process_due_schedules_once = wrapped_due
    try:
        for callback in app.on_startup:
            await callback(app)
        await asyncio.sleep(0)
        runtime = app[APP_WECOM_RUNTIME_KEY]
        assert runtime.connected is False
        assert calls["executions"] == 0
        definition = read_schedule_definition(config.runtime_root, "schedule-1")
        assert definition is not None
        assert definition.run_count == 0
        assert definition.enabled is True
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.run_wecom_runtime = original_run
        service_module.process_due_schedules_once = original_exec


async def test_service_schedule_loop_survives_helper_exception(tmp_path: Path) -> None:
    from aiohttp.test_utils import make_mocked_request
    from workspace_bridge import service as service_module

    config = make_config(tmp_path, wecom_enabled=True)
    config = config.__class__(**{**config.__dict__, "schedule_poll_ms": 0})
    app = create_app(config)
    calls = {"due": 0, "jobs": 0}

    async def fake_run_wecom_runtime(_config, runtime):
        runtime.connected = True
        await asyncio.sleep(3600)

    async def fake_process_due_schedules_once(_config, _runtime):
        calls["due"] += 1
        if calls["due"] == 1:
            raise RuntimeError("schedule boom")
        return []

    async def fake_process_scheduled_jobs_once(_config, _runtime):
        calls["jobs"] += 1
        return []

    async def fast_sleep(_seconds: float) -> None:
        return None

    original_run = service_module.run_wecom_runtime
    original_due = service_module.process_due_schedules_once
    original_jobs = service_module.process_scheduled_jobs_once
    service_module.run_wecom_runtime = fake_run_wecom_runtime
    service_module.process_due_schedules_once = fake_process_due_schedules_once
    service_module.process_scheduled_jobs_once = fake_process_scheduled_jobs_once
    try:
        for callback in app.on_startup:
            await callback(app)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        runtime = app[APP_WECOM_RUNTIME_KEY]
        schedule_task = app[APP_SCHEDULE_TASK_KEY]
        request = make_mocked_request("GET", "/", app=app)
        route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/")
        response = await route.handler(request)
        payload = json.loads(response.text)

        assert calls["due"] >= 2
        assert calls["jobs"] >= 1
        assert schedule_task is not None
        assert schedule_task.done() is False
        assert runtime.last_status is None
        assert runtime.last_error is None
        assert response.status == 200
        assert payload["ok"] is True
        assert payload["wecomStatus"] is None
        assert payload["wecomLastError"] is None
        assert payload["runtimeStatus"] is None
        assert payload["runtimeLastError"] is None
        assert payload["scheduleTaskPresent"] is True
        assert payload["scheduleTaskDone"] is False
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.run_wecom_runtime = original_run
        service_module.process_due_schedules_once = original_due
        service_module.process_scheduled_jobs_once = original_jobs


async def test_service_schedule_loop_allows_jobs_after_due_failure_same_iteration(tmp_path: Path) -> None:
    from workspace_bridge import service as service_module

    config = make_config(tmp_path, wecom_enabled=True)
    config = config.__class__(**{**config.__dict__, "schedule_poll_ms": 0})
    app = create_app(config)
    calls = {"due": 0, "jobs": 0}

    async def fake_run_wecom_runtime(_config, runtime):
        runtime.connected = True
        await asyncio.sleep(3600)

    async def fake_process_due_schedules_once(_config, _runtime):
        calls["due"] += 1
        raise RuntimeError("due boom")

    async def fake_process_scheduled_jobs_once(_config, _runtime):
        calls["jobs"] += 1
        return []

    original_run = service_module.run_wecom_runtime
    original_due = service_module.process_due_schedules_once
    original_jobs = service_module.process_scheduled_jobs_once
    service_module.run_wecom_runtime = fake_run_wecom_runtime
    service_module.process_due_schedules_once = fake_process_due_schedules_once
    service_module.process_scheduled_jobs_once = fake_process_scheduled_jobs_once
    try:
        for callback in app.on_startup:
            await callback(app)
        await asyncio.sleep(0)

        runtime = app[APP_WECOM_RUNTIME_KEY]
        request = make_mocked_request("GET", "/", app=app)
        route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/")
        response = await route.handler(request)
        payload = json.loads(response.text)
        assert calls["due"] >= 1
        assert calls["jobs"] >= 1
        assert runtime.last_status == "schedule_failed"
        assert runtime.last_error == "due boom"
        assert response.status == 503
        assert payload["ok"] is False
        assert payload["runtimeStatus"] == "schedule_failed"
        assert payload["runtimeLastError"] == "due boom"
    finally:
        for callback in app.on_cleanup:
            await callback(app)
        service_module.run_wecom_runtime = original_run
        service_module.process_due_schedules_once = original_due
        service_module.process_scheduled_jobs_once = original_jobs


async def test_execute_and_deliver_message_rejects_when_final_delivery_is_only_cached(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

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
            self.returncode = None

        async def communicate(self):
            self.returncode = 0
            return b'{"type":"item.completed","item":{"type":"agentmessage","text":"done"}}\n', b""

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        with pytest.raises(RuntimeError, match="final delivery deferred until connection recovers"):
            await execute_and_deliver_message(
                config,
                runtime,
                message,
                argv_override=("python", "-c", "print('unused')"),
            )
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert "req-1" in runtime.pending_finals
    assert runtime.reply_states["req-1"].pending_final_payload is not None


async def test_execute_and_deliver_message_uses_runtime_thread_state_when_final_delivery_succeeds(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    class FakeWS:
        async def send_json(self, _payload: dict) -> None:
            return None

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
            self.returncode = None

        async def communicate(self):
            self.returncode = 0
            return (
                (
                    "\n".join(
                        [
                            json.dumps({"type": "thread.started", "thread_id": "thread-new"}, ensure_ascii=False),
                            json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "done"}}, ensure_ascii=False),
                        ]
                    )
                    + "\n"
                ).encode("utf-8"),
                b"",
            )

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    runtime.session_threads[message.chat_key] = "thread-resume"
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    invocations: list[tuple[str, ...]] = []

    async def fake_create_subprocess_exec(*args, **_kwargs):
        invocations.append(tuple(args))
        return FakeProcess()

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        session_id, reply = await execute_and_deliver_message(config, runtime, message)
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert session_id.startswith("session-")
    assert reply == "done"
    assert invocations[0][0:3] == ("codex", "exec", "resume")
    assert runtime.session_threads[message.chat_key] == "thread-new"


async def test_stream_text_message_once_uses_claude_resume_without_local_history_duplication(tmp_path: Path) -> None:
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

        def write(self, data: bytes) -> None:
            self.buffer.extend(data)

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            return None

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = FakeStdin()
            self.stdout = None
            self.stderr = None
            self.returncode = None

        async def communicate(self):
            self.returncode = 0
            return (
                (
                    "\n".join(
                        [
                            json.dumps(
                                {
                                    "type": "system",
                                    "subtype": "init",
                                    "session_id": "550e8400-e29b-41d4-a716-446655440000",
                                },
                                ensure_ascii=False,
                            ),
                            json.dumps({"type": "result", "result": "done"}, ensure_ascii=False),
                        ]
                    )
                    + "\n"
                ).encode("utf-8"),
                b"",
            )

    bot = build_bot_from_app_config(config)
    bot = replace(
        bot,
        agent_backend="claude",
        agent_command="claude --permission-mode bypassPermissions",
        agent_run_as_user=None,
        agent_run_as_group=None,
    )
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    runtime.session_threads["single:alice"] = "550e8400-e29b-41d4-a716-446655440000"
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    invocations: list[tuple[str, ...]] = []
    stdin_buffers: list[str] = []

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*args, **_kwargs):
        process = FakeProcess()
        invocations.append(tuple(args))
        stdin_buffers.append(process.stdin.buffer.decode("utf-8", "ignore"))
        return process

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        session_id, reply = await stream_text_message_once(config, runtime, message)
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert session_id.startswith("session-")
    assert reply == "done"
    assert invocations[0][0] == "claude"
    assert "-p" in invocations[0]
    assert "--resume" in invocations[0]
    assert "550e8400-e29b-41d4-a716-446655440000" in invocations[0]


async def test_run_text_message_once_prepares_private_runtime_for_claude_run_as_user(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    bot = replace(
        bot,
        agent_backend="claude",
        agent_command="claude",
        agent_run_as_user="nobody",
        agent_run_as_group="nogroup",
        agent_runtime_root=(tmp_path / "claude-runtime").resolve(),
    )
    launch = execution_module.prepare_session_run(bot, "single:alice")
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    captured = {}

    def fake_run_invocation(invocation):
        captured["invocation"] = invocation
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"type": "result", "result": "done"}, ensure_ascii=False) + "\n",
            stderr="",
        )

    original_run_invocation = execution_module.run_invocation
    execution_module.run_invocation = fake_run_invocation
    try:
        session_id, reply = await run_text_message_once(config, bot, message)
    finally:
        execution_module.run_invocation = original_run_invocation

    invocation = captured["invocation"]
    assert session_id.startswith("session-")
    assert reply == "done"
    assert invocation.run_as_user == "nobody"
    assert invocation.run_as_group == "nogroup"
    assert invocation.env["CLAUDE_CONFIG_DIR"].startswith(str((tmp_path / "claude-runtime").resolve()))
    assert invocation.env["CODEX_HOME"].startswith(str((tmp_path / "claude-runtime").resolve()))
    assert invocation.env["TMPDIR"].startswith(str((tmp_path / "claude-runtime").resolve()))
    assert invocation.cwd == launch.cwd
    assert invocation.env["WECOM_BRIDGE_WORKFILE_DIR"] == str(launch.runtime_context.workfile_dir)


async def test_stream_text_message_once_falls_back_to_local_history_for_claude_when_resume_session_missing(tmp_path: Path) -> None:
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

        def write(self, data: bytes) -> None:
            self.buffer.extend(data)

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            return None

    class FakeProcess:
        def __init__(self, *, stdout_text: str, stderr_text: str, returncode: int = 0) -> None:
            self.stdin = FakeStdin()
            self.stdout = None
            self.stderr = None
            self._stdout = stdout_text.encode("utf-8")
            self._stderr = stderr_text.encode("utf-8")
            self.returncode = None
            self._planned_returncode = returncode

        async def communicate(self):
            self.returncode = self._planned_returncode
            return self._stdout, self._stderr

        async def wait(self):
            self.returncode = self._planned_returncode
            return self.returncode

    bot = build_bot_from_app_config(config)
    bot = replace(
        bot,
        agent_backend="claude",
        agent_command="claude --permission-mode bypassPermissions",
        agent_run_as_user=None,
        agent_run_as_group=None,
    )
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    runtime.session_threads["single:alice"] = "550e8400-e29b-41d4-a716-446655440000"
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="current turn", raw_payload={})
    runtime_session = SimpleNamespace(chat=[{"role": "user", "text": "older user turn"}, {"role": "bot", "text": "older bot turn"}])
    runtime.sessions = {"single:alice": runtime_session}
    invocations: list[tuple[str, ...]] = []
    processes: list[FakeProcess] = []

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*args, **_kwargs):
        invocations.append(tuple(args))
        if len(invocations) == 1:
            process = FakeProcess(
                stdout_text="",
                stderr_text='{"type":"result","subtype":"error_during_execution","is_error":true,"session_id":"550e8400-e29b-41d4-a716-446655440000","errors":["No conversation found with session ID: 550e8400-e29b-41d4-a716-446655440000"]}',
                returncode=1,
            )
        else:
            process = FakeProcess(
                stdout_text=json.dumps({"type": "result", "result": "done"}, ensure_ascii=False),
                stderr_text="",
                returncode=0,
            )
        processes.append(process)
        return process

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        session_id, reply = await stream_text_message_once(config, runtime, message)
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert session_id.startswith("session-")
    assert reply == "done"
    assert len(invocations) == 2
    assert "--resume" in invocations[0]
    assert "--resume" not in invocations[1]
    assert "[RecentConversation]" in processes[1].stdin.buffer.decode("utf-8", "ignore")
    assert "older user turn" in processes[1].stdin.buffer.decode("utf-8", "ignore")
    assert "older bot turn" in processes[1].stdin.buffer.decode("utf-8", "ignore")


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


async def test_send_or_cache_runtime_payload_keeps_group_stream_reply_for_final_fallback(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    message = WeComTextMessage(req_id="req-1", chat_key="group-user:room-1:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "final", final=True)

    assert delivered is False
    assert "req-1" in runtime.pending_finals
    assert runtime.pending_finals["req-1"][-1]["cmd"] == "aibot_respond_msg"
    assert runtime.pending_finals["req-1"][-1]["body"]["stream"]["content"] == "final"
    assert runtime.pending_finals["req-1"][-1]["body"]["stream"]["finish"] is True
    assert runtime.reply_states["req-1"].pending_final_payload is not None
    assert runtime.reply_states["req-1"].pending_final_payloads is not None


async def test_send_or_cache_runtime_payload_keeps_single_stream_reply_for_final_fallback(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "final", final=True)

    assert delivered is False
    assert "req-1" in runtime.pending_finals
    assert runtime.pending_finals["req-1"][-1]["cmd"] == "aibot_respond_msg"
    assert runtime.pending_finals["req-1"][-1]["body"]["stream"]["content"] == "final"
    assert runtime.pending_finals["req-1"][-1]["body"]["stream"]["finish"] is True
    assert runtime.reply_states["req-1"].pending_final_payload is not None
    assert runtime.reply_states["req-1"].pending_final_payloads is not None


async def test_send_or_cache_runtime_payload_does_not_cache_empty_req_id_state(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    message = WeComTextMessage(req_id="", chat_key="single:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "final", final=True)

    assert delivered is False
    assert len(runtime.pending_finals or {}) == 1
    cached_req_id = next(iter(runtime.pending_finals))
    assert cached_req_id
    assert runtime.pending_finals[cached_req_id][-1]["cmd"] == "aibot_send_msg"
    assert runtime.pending_streams == {}
    assert cached_req_id in runtime.reply_states


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


async def test_send_or_cache_runtime_payload_clears_transient_last_error_after_success(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    class RecordingWS:
        async def send_json(self, _payload: dict) -> None:
            return None

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = RecordingWS()
    runtime.last_error = "socket closed"
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "final", final=True)

    assert delivered is True
    assert runtime.last_error is None


async def test_send_or_cache_runtime_payload_keeps_only_unsent_chunks_after_partial_send_failure(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload
    from workspace_bridge.wecom_protocol import build_text_response_payloads

    sent: list[dict] = []

    class FlakyWS:
        async def send_json(self, payload: dict) -> None:
            if sent:
                raise RuntimeError("socket closed")
            sent.append(payload)

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FlakyWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    content = "x" * 8000
    expected = build_text_response_payloads("req-1", "session-1", content, final=True)

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", content, final=True)

    assert delivered is False
    assert sent == [expected[0]]
    assert runtime.last_error == "socket closed"
    assert runtime.pending_finals["req-1"] == expected[1:]
    assert runtime.reply_states["req-1"].pending_final_payloads == expected[1:]


async def test_send_or_cache_runtime_payload_sends_stream_chunks_in_order(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    sent: list[dict] = []

    class RecordingWS:
        async def send_json(self, payload: dict) -> None:
            sent.append(payload)

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = RecordingWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "x" * 8000, final=True)

    assert delivered is True
    assert len(sent) == 3
    assert sent[0]["body"]["stream"]["finish"] is False
    assert sent[1]["body"]["stream"]["finish"] is False
    assert sent[2]["body"]["stream"]["finish"] is True
    assert "".join(item["body"]["stream"]["content"] for item in sent) == "x" * 8000


async def test_send_or_cache_runtime_payload_sends_proactive_chunks_in_order(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.execution import send_or_cache_runtime_payload

    sent: list[dict] = []

    class RecordingWS:
        async def send_json(self, payload: dict) -> None:
            sent.append(payload)

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = RecordingWS()
    message = WeComTextMessage(req_id="", chat_key="group-user:room-1:alice", content="hello", raw_payload={})

    delivered = await send_or_cache_runtime_payload(runtime, message, "session-1", "x" * 5000, final=True)

    assert delivered is True
    assert len(sent) >= 2
    assert all(item["cmd"] == "aibot_send_msg" for item in sent)
    assert all(item["body"]["markdown"]["content"].startswith("<@alice>\n") for item in sent)


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


async def test_run_text_message_once_does_not_persist_thread_id_from_stdout(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation

    def fake_run_invocation(_invocation):
        return SimpleNamespace(
            returncode=0,
            stdout="\n".join(
                [
                    json.dumps({"type": "thread.started", "thread_id": "thread-123"}, ensure_ascii=False),
                    json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "done"}}, ensure_ascii=False),
                ]
            ),
            stderr="",
        )

    execution_module.run_invocation = fake_run_invocation
    try:
        session_id, reply = await run_text_message_once(
            config,
            bot,
            message,
            argv_override=("python", "-c", "print('unused')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation

    stored = load_session_record(bot.runtime_root, session_id)
    assert reply == "done"
    assert stored is not None
    assert stored.thread_id is None


async def test_run_text_message_once_updates_last_run_at_on_failure(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run

    bot = build_bot_from_app_config(config)
    launch = prepare_session_run(bot, "single:alice")
    before = load_session_record(bot.runtime_root, launch.session.session_id)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation

    def fake_run_invocation(_invocation):
        return SimpleNamespace(returncode=3, stdout="", stderr="failed")

    execution_module.run_invocation = fake_run_invocation
    try:
        try:
            await run_text_message_once(
                config,
                bot,
                message,
                argv_override=("python", "-c", "print('unused')"),
            )
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected RuntimeError")
    finally:
        execution_module.run_invocation = original_run_invocation

    after = load_session_record(bot.runtime_root, launch.session.session_id)
    assert before is not None and after is not None
    assert after.last_run_at is not None
    assert int(after.last_run_at) >= int(before.updated_at)


async def test_run_text_message_once_retries_fresh_exec_when_resume_thread_not_found(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation
    invocations: list[tuple[str, ...]] = []

    def fake_run_invocation(invocation):
        invocations.append(tuple(invocation.argv))
        if len(invocations) == 1:
            return SimpleNamespace(
                returncode=0,
                stdout="",
                stderr="2026-05-28T11:17:33.253308Z ERROR codex_core::session: failed to record rollout items: thread 019e6e4d-4d11-78a2-9636-843225e13202 not found",
            )
        return SimpleNamespace(
            returncode=0,
            stdout="\n".join(
                [
                    json.dumps({"type": "thread.started", "thread_id": "thread-456"}, ensure_ascii=False),
                    json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "done"}}, ensure_ascii=False),
                ]
            ),
            stderr="",
        )

    execution_module.run_invocation = fake_run_invocation
    try:
        session_id, reply = await run_text_message_once(
            config,
            bot,
            message,
            launch_thread_id_override="019e6e4d-4d11-78a2-9636-843225e13202",
        )
    finally:
        execution_module.run_invocation = original_run_invocation

    stored = load_session_record(bot.runtime_root, session_id)
    assert reply == "done"
    assert len(invocations) == 2
    assert invocations[0][0:3] == ("codex", "exec", "resume")
    assert invocations[1][0:2] == ("codex", "exec")
    assert "resume" not in invocations[1]
    assert stored is not None
    assert stored.thread_id is None


async def test_run_text_message_once_raises_on_nonzero_exit(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation

    def fake_run_invocation(_invocation):
        return SimpleNamespace(returncode=7, stdout="", stderr="runner failed")

    execution_module.run_invocation = fake_run_invocation
    try:
        try:
            await run_text_message_once(
                config,
                bot,
                message,
                argv_override=("python", "-c", "print('unused')"),
            )
        except RuntimeError as exc:
            assert str(exc) == "codex exited with status 7: runner failed"
        else:
            raise AssertionError("expected RuntimeError")
    finally:
        execution_module.run_invocation = original_run_invocation


async def test_run_text_message_once_does_not_retry_when_thread_not_found_only_appears_in_stdout(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation
    invocations: list[tuple[str, ...]] = []

    def fake_run_invocation(invocation):
        invocations.append(tuple(invocation.argv))
        return SimpleNamespace(
            returncode=0,
            stdout="\n".join(
                [
                    json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "explain why thread 019e6e4d-4d11-78a2-9636-843225e13202 not found can happen"}}, ensure_ascii=False),
                ]
            ),
            stderr="",
        )

    execution_module.run_invocation = fake_run_invocation
    try:
        launch = execution_module.prepare_session_run(bot, message.chat_key)
        execution_module.update_session_record(
            bot.runtime_root,
            launch.session.session_id,
            lambda current: replace(current, thread_id="019e6e4d-4d11-78a2-9636-843225e13202"),
        )
        session_id, reply = await run_text_message_once(
            config,
            bot,
            message,
        )
    finally:
        execution_module.run_invocation = original_run_invocation

    stored = load_session_record(bot.runtime_root, session_id)
    assert len(invocations) == 1
    assert reply == "explain why thread 019e6e4d-4d11-78a2-9636-843225e13202 not found can happen"
    assert stored is not None
    assert stored.thread_id is None


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


async def test_stream_text_message_once_uses_fresh_codex_invocation_without_override(tmp_path: Path) -> None:
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
            self.returncode = None

        async def communicate(self):
            self.returncode = 0
            return (
                (json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "fresh stream"}}, ensure_ascii=False) + "\n").encode("utf-8"),
                b"",
            )

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    captured: dict[str, object] = {}

    original_parent_env = os.environ.get("EXEC_PARENT_ENV")
    os.environ["EXEC_PARENT_ENV"] = "present"
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    original_prepare_session_run = execution_module.prepare_session_run
    original_build_prompt = execution_module.build_prompt
    execution_module.prepare_session_run = lambda _bot, _chat_key: SimpleNamespace(
        cwd=bot.source.source_dir,
        env={
            "WECOM_BRIDGE_CHATFILE_DIR": str(tmp_path / "chatfile"),
            "WECOM_BRIDGE_PROJECT_DIR": str(bot.source.source_dir),
            "WECOM_BRIDGE_CHAT_KEY": message.chat_key,
            "CODEX_HOME": str(config.runtime_root / ".bridge-codex-home" / "sessions" / "session-fresh"),
        },
        runtime_context=SimpleNamespace(codex_exec_mode=bot.codex_exec_mode),
        session=SimpleNamespace(session_id="session-fresh"),
    )
    execution_module.build_prompt = lambda _bot, _launch, _content: "hello"

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured["args"] = tuple(args)
        captured["env"] = dict(kwargs["env"])
        return FakeProcess()

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        session_id, reply = await stream_text_message_once(config, runtime, message)
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec
        execution_module.prepare_session_run = original_prepare_session_run
        execution_module.build_prompt = original_build_prompt
        if original_parent_env is None:
            os.environ.pop("EXEC_PARENT_ENV", None)
        else:
            os.environ["EXEC_PARENT_ENV"] = original_parent_env

    assert session_id == "session-fresh"
    assert reply == "fresh stream"
    assert captured["args"][0:2] == ("codex", "exec")
    assert captured["args"][-1] == "-"
    assert captured["env"]["EXEC_PARENT_ENV"] == "present"
    assert captured["env"]["WECOM_BRIDGE_CHATFILE_DIR"]
    assert captured["env"]["CODEX_HOME"].startswith(str(config.runtime_root / ".bridge-codex-home" / "sessions"))


async def test_stream_text_message_once_retries_fresh_exec_when_resume_thread_not_found(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record

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
        def __init__(self, *, stdout_text: str, stderr_text: str, returncode: int = 0) -> None:
            self.stdin = FakeStdin()
            self.stdout = None
            self.stderr = None
            self._stdout = stdout_text.encode("utf-8")
            self._stderr = stderr_text.encode("utf-8")
            self.returncode = None
            self._planned_returncode = returncode

        async def communicate(self):
            self.returncode = self._planned_returncode
            return self._stdout, self._stderr

        async def wait(self):
            self.returncode = self._planned_returncode
            return self.returncode

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    invocations: list[tuple[str, ...]] = []

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    try:
        launch = execution_module.prepare_session_run(bot, message.chat_key)
        runtime.session_threads[message.chat_key] = "019e6e4d-4d11-78a2-9636-843225e13202"

        async def fake_create_subprocess_exec(*args, **_kwargs):
            invocations.append(tuple(args))
            if len(invocations) == 1:
                return FakeProcess(
                    stdout_text="",
                    stderr_text="2026-05-28T11:17:33.253308Z ERROR codex_core::session: failed to record rollout items: thread 019e6e4d-4d11-78a2-9636-843225e13202 not found",
                )
            return FakeProcess(
                stdout_text="\n".join(
                    [
                        json.dumps({"type": "thread.started", "thread_id": "thread-789"}, ensure_ascii=False),
                        json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "stream ok"}}, ensure_ascii=False),
                    ]
                ),
                stderr_text="",
            )

        execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            message,
        )
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    stored = load_session_record(bot.runtime_root, session_id)
    assert reply == "stream ok"
    assert len(invocations) == 2
    assert invocations[0][0:3] == ("codex", "exec", "resume")
    assert invocations[1][0:2] == ("codex", "exec")
    assert stored is not None
    assert stored.thread_id is None
    assert runtime.session_threads[message.chat_key] == "thread-789"


async def test_stream_text_message_once_retries_fresh_exec_when_resume_stdin_breaks(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record

    class FakeWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    class BrokenStdin:
        def write(self, _data: bytes) -> None:
            raise BrokenPipeError("no prompt provided via stdin")

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            return None

    class FakeStdin:
        def write(self, _data: bytes) -> None:
            return None

        async def drain(self) -> None:
            return None

        def close(self) -> None:
            return None

    class FakeProcess:
        def __init__(self, *, stdin, stdout_text: str, stderr_text: str, returncode: int = 0) -> None:
            self.stdin = stdin
            self.stdout = None
            self.stderr = None
            self._stdout = stdout_text.encode("utf-8")
            self._stderr = stderr_text.encode("utf-8")
            self.returncode = None
            self._planned_returncode = returncode

        async def communicate(self):
            self.returncode = self._planned_returncode
            return self._stdout, self._stderr

        async def wait(self):
            self.returncode = self._planned_returncode
            return self.returncode

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    invocations: list[tuple[str, ...]] = []

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    try:
        launch = execution_module.prepare_session_run(bot, message.chat_key)
        runtime.session_threads[message.chat_key] = "thread-resume"

        async def fake_create_subprocess_exec(*args, **_kwargs):
            invocations.append(tuple(args))
            if len(invocations) == 1:
                return FakeProcess(stdin=BrokenStdin(), stdout_text="", stderr_text="")
            return FakeProcess(
                stdin=FakeStdin(),
                stdout_text="\n".join(
                    [
                        json.dumps({"type": "thread.started", "thread_id": "thread-stdin-fallback"}, ensure_ascii=False),
                        json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "stream ok"}}, ensure_ascii=False),
                    ]
                ),
                stderr_text="",
            )

        execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
        session_id, reply = await stream_text_message_once(config, runtime, message)
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    stored = load_session_record(bot.runtime_root, session_id)
    assert reply == "stream ok"
    assert len(invocations) == 2
    assert invocations[0][0:3] == ("codex", "exec", "resume")
    assert invocations[1][0:2] == ("codex", "exec")
    assert stored is not None
    assert stored.thread_id is None
    assert runtime.session_threads[message.chat_key] == "thread-stdin-fallback"


async def test_stream_text_message_once_does_not_retry_when_thread_not_found_only_appears_in_stdout(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record

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
        def __init__(self, *, stdout_text: str, stderr_text: str, returncode: int = 0) -> None:
            self.stdin = FakeStdin()
            self.stdout = None
            self.stderr = None
            self._stdout = stdout_text.encode("utf-8")
            self._stderr = stderr_text.encode("utf-8")
            self.returncode = None
            self._planned_returncode = returncode

        async def communicate(self):
            self.returncode = self._planned_returncode
            return self._stdout, self._stderr

        async def wait(self):
            self.returncode = self._planned_returncode
            return self.returncode

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    invocations: list[tuple[str, ...]] = []

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    try:
        launch = execution_module.prepare_session_run(bot, message.chat_key)
        runtime.session_threads[message.chat_key] = "019e6e4d-4d11-78a2-9636-843225e13202"

        async def fake_create_subprocess_exec(*args, **_kwargs):
            invocations.append(tuple(args))
            return FakeProcess(
                stdout_text="\n".join(
                    [
                        json.dumps({"type": "item.completed", "item": {"type": "agentmessage", "text": "thread 019e6e4d-4d11-78a2-9636-843225e13202 not found is one possible failure mode"}}, ensure_ascii=False),
                    ]
                ),
                stderr_text="",
            )

        execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
        session_id, reply = await stream_text_message_once(
            config,
            runtime,
            message,
        )
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    stored = load_session_record(bot.runtime_root, session_id)
    assert len(invocations) == 1
    assert reply == "thread 019e6e4d-4d11-78a2-9636-843225e13202 not found is one possible failure mode"
    assert stored is not None
    assert stored.thread_id is None
    assert runtime.session_threads[message.chat_key] == "019e6e4d-4d11-78a2-9636-843225e13202"


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


async def test_stream_text_message_once_returns_stderr_text_on_nonzero_exit(tmp_path: Path) -> None:
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
            self.returncode = None

        async def communicate(self):
            self.returncode = 3
            return b"", b"stream failed"

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        try:
            await stream_text_message_once(config, runtime, message, argv_override=("python", "-c", "print('unused')"))
        except RuntimeError as exc:
            assert str(exc) == "codex exited with status 3: stream failed"
        else:
            raise AssertionError("expected RuntimeError")
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec


async def test_stream_text_message_once_updates_last_run_at_on_failure(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.runtime import load_session_record, prepare_session_run

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
            self.returncode = None

        async def communicate(self):
            self.returncode = 3
            return b"", b"stream failed"

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    launch = prepare_session_run(bot, "single:alice")
    before = load_session_record(bot.runtime_root, launch.session.session_id)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()

    execution_module.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        try:
            await stream_text_message_once(config, runtime, message, argv_override=("python", "-c", "print('unused')"))
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected RuntimeError")
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    after = load_session_record(bot.runtime_root, launch.session.session_id)
    assert before is not None and after is not None
    assert after.last_run_at is not None
    assert int(after.last_run_at) >= int(before.updated_at)


async def test_bridge_reset_removes_workspace_bridge_chatfile_and_codex_home(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge.wecom_protocol import build_text_response_payload
    from workspace_bridge.runtime import prepare_session_run, session_codex_home_root
    from workspace_bridge import wecom_runtime as runtime_module

    class FakeWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    launch = prepare_session_run(bot, "single:alice")
    chatfile_dir = launch.runtime_context.chatfile_dir
    chatfile_dir.mkdir(parents=True, exist_ok=True)
    (chatfile_dir / "artifact.txt").write_text("artifact", encoding="utf-8")
    codex_home = session_codex_home_root(bot.runtime_root) / launch.session.session_id
    codex_home.mkdir(parents=True, exist_ok=True)
    (codex_home / "state.txt").write_text("state", encoding="utf-8")
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgtype": "text",
            "text": {"content": "/bridge-reset"},
            "from": {"userid": "alice"},
        },
    }

    await runtime_module.handle_wecom_payload(config, runtime, runtime.ws, payload, runtime_module._dispatch_message)

    assert not chatfile_dir.exists()
    assert not codex_home.exists()
    assert runtime.ws.sent[-1] == build_text_response_payload("req-1", launch.session.session_id, "Session reset.", final=True)


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
    status_payloads = [payload for payload in runtime.ws.sent if "思考中" in payload["body"]["stream"]["content"]]
    assert len(status_payloads) >= 2
    assert "已运行 0s" in status_payloads[0]["body"]["stream"]["content"]
    assert any(payload["body"]["stream"]["content"] != status_payloads[0]["body"]["stream"]["content"] for payload in status_payloads[1:])
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
    original_run_blocking = execution_module.run_blocking

    def fake_run_invocation(_invocation):
        marker["value"] = True
        return SimpleNamespace(returncode=0, stdout="done\n", stderr="")

    async def fake_run_blocking(func, *args, **kwargs):
        return func(*args, **kwargs)

    execution_module.run_invocation = fake_run_invocation
    execution_module.run_blocking = fake_run_blocking
    try:
        await run_text_message_once(
            config,
            bot,
            message,
            argv_override=("python", "-c", "print('done')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation
        execution_module.run_blocking = original_run_blocking

    assert marker["value"] is True


async def test_run_text_message_once_uses_thread_offload_for_prepare_session_run(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.config import build_bot_from_app_config

    bot = build_bot_from_app_config(config)
    message = WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={})
    original_run_invocation = execution_module.run_invocation
    original_run_blocking = execution_module.run_blocking
    calls = []

    def fake_run_invocation(_invocation):
        return SimpleNamespace(returncode=0, stdout="done\n", stderr="")

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    execution_module.run_invocation = fake_run_invocation
    execution_module.run_blocking = fake_run_blocking
    try:
        await run_text_message_once(
            config,
            bot,
            message,
            argv_override=("python", "-c", "print('done')"),
        )
    finally:
        execution_module.run_invocation = original_run_invocation
        execution_module.run_blocking = original_run_blocking

    assert "prepare_session_run" in calls


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
    original_run_blocking = execution_module.run_blocking

    def boom(_invocation):
        raise RuntimeError("boom")

    async def fake_run_blocking(func, *args, **kwargs):
        return func(*args, **kwargs)

    execution_module.run_invocation = boom
    execution_module.run_blocking = fake_run_blocking
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
        execution_module.run_blocking = original_run_blocking

    assert execution_runtime._SESSION_RUN_LOCKS == {}
