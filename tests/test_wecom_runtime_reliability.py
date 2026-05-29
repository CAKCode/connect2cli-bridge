import asyncio

from workspace_bridge.config import load_app_config
from workspace_bridge.execution import flush_cached_runtime_payloads
from workspace_bridge.models import BotConfig, WeComBotRuntime, SourceConfig
from workspace_bridge.reply_state import get_or_create_reply_state, cache_reply_payload
from workspace_bridge.wecom_upload import create_request_future, reject_pending_requests


class FakeWS:
    def __init__(self) -> None:
        self.sent = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


def make_runtime() -> WeComBotRuntime:
    bot = BotConfig(
        bot_id="bot-1",
        bot_name="codex",
        bot_secret=None,
        source=SourceConfig(source_id="src-1", source_dir=__import__("pathlib").Path(".")),
        runtime_root=__import__("pathlib").Path("."),
        global_skill_dir=__import__("pathlib").Path("."),
        chatfile_root=__import__("pathlib").Path("."),
        codex_exec_mode="sandboxed",
    )
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    return runtime


async def test_flush_cached_runtime_payloads_delivers_reply_state_payloads() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    payload = {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=False)
    runtime.pending_streams["req-1"] = payload

    await flush_cached_runtime_payloads(runtime)

    assert runtime.ws.sent == [payload]
    assert state.pending_stream_payload is None
    assert "req-1" in runtime.reply_states


async def test_flush_cached_runtime_payloads_cleans_final_reply_state() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    payload = {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=True)
    runtime.pending_finals["req-1"] = payload

    await flush_cached_runtime_payloads(runtime)

    assert runtime.ws.sent == [payload]
    assert "req-1" not in runtime.reply_states


async def test_flush_cached_runtime_payloads_replays_all_cached_chunks() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    payloads = [
        {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream", "stream": {"content": "part-1"}}},
        {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream", "stream": {"content": "part-2"}}},
    ]
    cache_reply_payload(state, payloads[-1], final=False, payloads=payloads)
    runtime.pending_streams["req-1"] = payloads

    await flush_cached_runtime_payloads(runtime)

    assert runtime.ws.sent == payloads
    assert state.pending_stream_payload is None
    assert state.pending_stream_payloads is None


async def test_reject_pending_requests_sets_future_exception() -> None:
    runtime = make_runtime()
    future = create_request_future(runtime, "req-1")

    reject_pending_requests(runtime, "bot websocket closed")

    assert future.done() is True
    try:
        future.result()
    except RuntimeError as exc:
        assert "bot websocket closed" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")
