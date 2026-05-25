from workspace_bridge.models import ReplyState, WeComBotRuntime, BotConfig, SourceConfig
from workspace_bridge.reply_state import (
    cache_reply_payload,
    cleanup_reply_state,
    get_or_create_reply_state,
    mark_reply_proactive,
    mark_reply_sent,
    proactive_status_due,
    reply_should_use_proactive,
)


def make_runtime() -> WeComBotRuntime:
    bot = BotConfig(
        bot_id="bot-1",
        bot_name="codex",
        bot_secret=None,
        source=SourceConfig(source_id="src-1", source_dir=__import__("pathlib").Path(".")),
        runtime_root=__import__("pathlib").Path("."),
        global_skill_dir=__import__("pathlib").Path("."),
        chatfile_root=__import__("pathlib").Path("."),
    )
    return WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})


def test_get_or_create_reply_state_reuses_same_req_id() -> None:
    runtime = make_runtime()

    first = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    second = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")

    assert first is second


def test_cache_and_cleanup_reply_state() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")

    cache_reply_payload(state, {"body": "stream"}, final=False)
    assert state.pending_stream_payload == {"body": "stream"}
    assert state.pending_stream_payloads == [{"body": "stream"}]
    cache_reply_payload(state, {"body": "final"}, final=True)
    assert state.pending_final_payload == {"body": "final"}
    assert state.pending_final_payloads == [{"body": "final"}]
    mark_reply_sent(state, final=True)
    assert state.pending_final_payload is None
    assert state.pending_final_payloads is None
    cleanup_reply_state(runtime, "req-1")
    assert "req-1" not in runtime.reply_states


def test_mark_reply_proactive_changes_delivery_mode() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")

    mark_reply_proactive(state)

    assert reply_should_use_proactive(state) is True
    assert proactive_status_due(state) is True
