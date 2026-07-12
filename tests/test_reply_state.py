from workspace_bridge.models import ReplyState, WeComBotRuntime, BotConfig, SourceConfig
from workspace_bridge.reply_state import (
    cache_reply_payload,
    cleanup_reply_state,
    get_or_create_reply_state,
    mark_reply_sent,
)


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
    return WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})


def test_get_or_create_reply_state_reuses_same_req_id() -> None:
    runtime = make_runtime()

    first = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    second = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")

    assert first is second


def test_get_or_create_reply_state_rejects_empty_req_id() -> None:
    runtime = make_runtime()

    try:
        get_or_create_reply_state(runtime, "", "session-1", "single:alice")
    except ValueError as exc:
        assert str(exc) == "req_id required"
    else:
        raise AssertionError("expected ValueError")


def test_get_or_create_reply_state_replaces_stale_req_id_binding_for_new_session() -> None:
    runtime = make_runtime()

    first = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    second = get_or_create_reply_state(runtime, "req-1", "session-2", "single:bob")

    assert second is not first
    assert runtime.reply_states["req-1"] is second
    assert second.session_id == "session-2"
    assert second.chat_key == "single:bob"


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


def test_cache_reply_payload_copies_payload_dicts() -> None:
    runtime = make_runtime()
    state = get_or_create_reply_state(runtime, "req-1", "session-1", "single:alice")
    stream_payload = {"body": {"stream": {"content": "hello"}}}
    final_payload = {"body": {"stream": {"content": "done"}}}

    cache_reply_payload(state, stream_payload, final=False)
    cache_reply_payload(state, final_payload, final=True)
    stream_payload["body"]["stream"]["content"] = "mutated"
    final_payload["body"]["stream"]["content"] = "mutated"

    assert state.pending_stream_payload == {"body": {"stream": {"content": "hello"}}}
    assert state.pending_final_payload == {"body": {"stream": {"content": "done"}}}
