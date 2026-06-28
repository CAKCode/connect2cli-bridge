import asyncio
import pytest

from workspace_bridge.config import load_app_config
from workspace_bridge.execution import flush_cached_runtime_payloads
from workspace_bridge.models import BotConfig, ScheduledJob, WeComBotRuntime, SourceConfig
from workspace_bridge.reply_state import get_or_create_reply_state, cache_reply_payload
from workspace_bridge.schedule import schedule_done_root, write_scheduled_job
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
        workspace_namespace="bot-1",
        chatfile_root=__import__("pathlib").Path("."),
        codex_exec_mode="sandboxed",
    )
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.ws = FakeWS()
    return runtime


async def test_flush_cached_runtime_payloads_delivers_reply_state_payloads() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    before = state.last_sent_at
    payload = {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=False)
    runtime.pending_streams["req-1"] = payload

    await flush_cached_runtime_payloads(runtime)

    assert runtime.ws.sent == [payload]
    assert state.pending_stream_payload is None
    assert state.last_sent_at >= before
    assert "req-1" in runtime.reply_states


async def test_flush_cached_runtime_payloads_cleans_final_reply_state() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    before = state.last_sent_at
    payload = {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=True)
    runtime.pending_finals["req-1"] = payload

    await flush_cached_runtime_payloads(runtime)

    assert runtime.ws.sent == [payload]
    assert state.last_sent_at >= before
    assert "req-1" not in runtime.reply_states


async def test_flush_cached_runtime_payloads_marks_deferred_job_done_after_successful_replay(tmp_path) -> None:
    runtime = make_runtime()
    runtime.config = BotConfig(
        bot_id=runtime.config.bot_id,
        bot_name=runtime.config.bot_name,
        bot_secret=runtime.config.bot_secret,
        source=runtime.config.source,
        runtime_root=tmp_path,
        workspace_namespace=runtime.config.workspace_namespace,
        chatfile_root=runtime.config.chatfile_root,
        codex_exec_mode=runtime.config.codex_exec_mode,
    )
    pending_path = tmp_path / "schedules" / "pending" / "0000000000000-job-1.json"
    write_scheduled_job(
        pending_path,
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    state = get_or_create_reply_state(runtime, "job:job-1", "session-1", "single:alice")
    payload = {"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=True)
    runtime.pending_finals["job:job-1"] = payload

    await flush_cached_runtime_payloads(runtime)

    assert not pending_path.exists()
    assert (schedule_done_root(tmp_path) / "0000000000000-job-1.json").exists()
    assert (schedule_done_root(tmp_path) / "schedule-1.json").exists()


async def test_flush_cached_runtime_payloads_clears_stale_schedule_failed_marker_after_replay_success(tmp_path) -> None:
    runtime = make_runtime()
    runtime.config = BotConfig(
        bot_id=runtime.config.bot_id,
        bot_name=runtime.config.bot_name,
        bot_secret=runtime.config.bot_secret,
        source=runtime.config.source,
        runtime_root=tmp_path,
        workspace_namespace=runtime.config.workspace_namespace,
        chatfile_root=runtime.config.chatfile_root,
        codex_exec_mode=runtime.config.codex_exec_mode,
    )
    pending_path = tmp_path / "schedules" / "pending" / "0000000000000-job-1.json"
    write_scheduled_job(
        pending_path,
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    failed_marker = tmp_path / "schedules" / "failed" / "schedule-1.json"
    failed_marker.parent.mkdir(parents=True, exist_ok=True)
    failed_marker.write_text("{}", encoding="utf-8")
    state = get_or_create_reply_state(runtime, "job:job-1", "session-1", "single:alice")
    payload = {"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=True)
    runtime.pending_finals["job:job-1"] = payload

    await flush_cached_runtime_payloads(runtime)

    assert failed_marker.exists() is False


async def test_flush_cached_runtime_payloads_advances_schedule_before_removing_pending_job(tmp_path, monkeypatch) -> None:
    from workspace_bridge import execution as execution_module

    runtime = make_runtime()
    runtime.config = BotConfig(
        bot_id=runtime.config.bot_id,
        bot_name=runtime.config.bot_name,
        bot_secret=runtime.config.bot_secret,
        source=runtime.config.source,
        runtime_root=tmp_path,
        workspace_namespace=runtime.config.workspace_namespace,
        chatfile_root=runtime.config.chatfile_root,
        codex_exec_mode=runtime.config.codex_exec_mode,
    )
    pending_path = tmp_path / "schedules" / "pending" / "0000000000000-job-1.json"
    write_scheduled_job(
        pending_path,
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    state = get_or_create_reply_state(runtime, "job:job-1", "session-1", "single:alice")
    payload = {"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=True)
    runtime.pending_finals["job:job-1"] = payload
    calls = []

    original_advance = execution_module.advance_schedule_definition_after_success

    def fake_advance(runtime_root, schedule_id, *, current_ms=None):
        calls.append((schedule_id, pending_path.exists()))
        return original_advance(runtime_root, schedule_id, current_ms=current_ms)

    monkeypatch.setattr(execution_module, "advance_schedule_definition_after_success", fake_advance)
    await flush_cached_runtime_payloads(runtime)

    assert calls == [("schedule-1", True)]


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


async def test_flush_cached_runtime_payloads_preserves_unsent_tail_on_partial_send_failure() -> None:
    runtime = make_runtime()

    class FlakyWS:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload: dict) -> None:
            if self.sent:
                raise RuntimeError("socket closed")
            self.sent.append(payload)

    runtime.ws = FlakyWS()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    payloads = [
        {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream", "stream": {"content": "part-1"}}},
        {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream", "stream": {"content": "part-2"}}},
        {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream", "stream": {"content": "part-3"}}},
    ]
    cache_reply_payload(state, payloads[-1], final=False, payloads=payloads)
    runtime.pending_streams["req-1"] = payloads

    with pytest.raises(RuntimeError, match="socket closed"):
        await flush_cached_runtime_payloads(runtime)

    assert runtime.last_error == "socket closed"
    assert runtime.ws.sent == [payloads[0]]
    assert runtime.pending_streams["req-1"] == payloads[1:]
    assert state.pending_stream_payload == payloads[-1]
    assert state.pending_stream_payloads == payloads[1:]


async def test_flush_cached_runtime_payloads_clears_transient_last_error_after_successful_replay() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    payload = {"headers": {"req_id": "req-1"}, "body": {"msgtype": "stream"}}
    cache_reply_payload(state, payload, final=False)
    runtime.pending_streams["req-1"] = payload
    runtime.last_error = "socket closed"

    await flush_cached_runtime_payloads(runtime)

    assert runtime.last_error is None


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
