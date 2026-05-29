import asyncio

from workspace_bridge.config import load_app_config
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.wecom_protocol import WeComTextMessage


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
            "WECOM_ENABLED": "true",
        }
    )


async def test_multi_user_runtime_keeps_independent_thread_state(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import execution as execution_module

    class FakeWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    class FakeProcess:
        def __init__(self, *, thread_id: str, final_text: str) -> None:
            self.stdin = type(
                "FakeStdin",
                (),
                {
                    "write": lambda self, _data: None,
                    "drain": lambda self: asyncio.sleep(0),
                    "close": lambda self: None,
                },
            )()
            self.stdout = None
            self.stderr = None
            self.returncode = None
            self._stdout = (
                f'{{"type":"thread.started","thread_id":"{thread_id}"}}\n'
                f'{{"type":"item.completed","item":{{"type":"agentmessage","text":"{final_text}"}}}}\n'
            ).encode("utf-8")
            self._stderr = b""

        async def communicate(self):
            self.returncode = 0
            return self._stdout, self._stderr

    config = make_config(tmp_path)
    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec
    calls: list[tuple[str, ...]] = []

    async def fake_create_subprocess_exec(*args, **_kwargs):
        calls.append(tuple(args))
        if "alice" in str(_kwargs.get("env", {}).get("WECOM_BRIDGE_CHAT_KEY", "")):
            return FakeProcess(thread_id="thread-alice", final_text="done alice")
        return FakeProcess(thread_id="thread-bob", final_text="done bob")

    monkeypatch.setattr(execution_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    try:
        first = await execution_module.stream_text_message_once(
            config,
            runtime,
            WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello", raw_payload={}),
        )
        second = await execution_module.stream_text_message_once(
            config,
            runtime,
            WeComTextMessage(req_id="req-2", chat_key="single:bob", content="hello", raw_payload={}),
        )
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    assert first[1] == "done alice"
    assert second[1] == "done bob"
    assert runtime.session_threads["single:alice"] == "thread-alice"
    assert runtime.session_threads["single:bob"] == "thread-bob"
    assert runtime.session_threads["single:alice"] != runtime.session_threads["single:bob"]
    assert all(args[0:2] == ("codex", "exec") for args in calls)


async def test_multi_user_runtime_recycles_chat_without_losing_stable_session_id(tmp_path, monkeypatch) -> None:
    from workspace_bridge.config import build_bot_from_app_config
    from workspace_bridge import execution as execution_module
    from workspace_bridge.runtime import load_session_record, prepare_session_run

    class FakeWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            self.sent.append(payload)

    class FakeProcess:
        def __init__(self) -> None:
            self.stdin = type(
                "FakeStdin",
                (),
                {
                    "write": lambda self, _data: None,
                    "drain": lambda self: asyncio.sleep(0),
                    "close": lambda self: None,
                },
            )()
            self.stdout = None
            self.stderr = None
            self.returncode = None

        async def communicate(self):
            self.returncode = 0
            return (
                b'{"type":"thread.started","thread_id":"thread-after-idle"}\n{"type":"item.completed","item":{"type":"agentmessage","text":"ok"}}\n',
                b"",
            )

    config = make_config(tmp_path)
    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()

    launch = prepare_session_run(bot, "single:alice")
    stable_session_id = launch.session.session_id
    runtime.session_threads["single:alice"] = "stale-thread"
    runtime.session_threads.pop("single:alice", None)

    original_create_subprocess_exec = execution_module.asyncio.create_subprocess_exec

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return FakeProcess()

    monkeypatch.setattr(execution_module.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    try:
        session_id, reply = await execution_module.stream_text_message_once(
            config,
            runtime,
            WeComTextMessage(req_id="req-1", chat_key="single:alice", content="hello again", raw_payload={}),
        )
    finally:
        execution_module.asyncio.create_subprocess_exec = original_create_subprocess_exec

    stored = load_session_record(bot.runtime_root, stable_session_id)
    assert session_id == stable_session_id
    assert reply == "ok"
    assert stored is not None
    assert stored.session_id == stable_session_id
    assert stored.thread_id is None
    assert runtime.session_threads["single:alice"] == "thread-after-idle"
