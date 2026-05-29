from pathlib import Path

from workspace_bridge.models import BotConfig, SourceConfig
from workspace_bridge.wecom_protocol import (
    build_proactive_text_payload,
    build_proactive_text_payloads,
    build_subscribe_payload,
    build_text_response_payload,
    build_text_response_payloads,
    chat_key_from_message,
    chat_key_to_send_target,
    is_subscribe_ok,
    normalize_bridge_command_text,
    parse_text_callback,
    split_text_chunks,
    strip_text_mentions,
)


def make_bot(tmp_path: Path) -> BotConfig:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    return BotConfig(
        bot_id="bot-1",
        bot_name="codex",
        bot_secret="secret-value",
        source=SourceConfig(source_id="src-1", source_dir=source_dir),
        runtime_root=tmp_path / "runtime",
        global_skill_dir=tmp_path / "global",
        chatfile_root=tmp_path / "chatfiles",
        codex_exec_mode="sandboxed",
    )


def test_build_subscribe_payload_uses_bot_credentials(tmp_path: Path) -> None:
    bot = make_bot(tmp_path)

    payload = build_subscribe_payload(bot, req_id="req-1")

    assert payload["cmd"] == "aibot_subscribe"
    assert payload["headers"]["req_id"] == "req-1"
    assert payload["body"]["bot_id"] == "bot-1"
    assert payload["body"]["secret"] == "secret-value"


def test_chat_key_from_group_user_message(tmp_path: Path) -> None:
    payload = {
        "body": {
            "chattype": "group",
            "chatid": "room-1",
            "from": {"userid": "alice"},
        }
    }

    assert chat_key_from_message(payload) == "group-user:room-1:alice"


def test_parse_text_callback_extracts_text_message(tmp_path: Path) -> None:
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-1"},
        "body": {
            "msgtype": "text",
            "text": {"content": "hello"},
            "from": {"userid": "alice"},
        },
    }

    message = parse_text_callback(payload)

    assert message is not None
    assert message.req_id == "req-1"
    assert message.chat_key == "single:alice"
    assert message.content == "hello"


def test_parse_text_callback_keeps_raw_content_for_later_strip(tmp_path: Path) -> None:
    payload = {
        "cmd": "aibot_msg_callback",
        "headers": {"req_id": "req-2"},
        "body": {
            "msgtype": "text",
            "text": {"content": "@robot, hello"},
            "from": {"userid": "alice"},
        },
    }

    message = parse_text_callback(payload)

    assert message is not None
    assert message.content == "@robot, hello"


def test_build_text_response_payload_uses_stream_format() -> None:
    payload = build_text_response_payload("req-1", "session-1", "done", final=True)

    assert payload["cmd"] == "aibot_respond_msg"
    assert payload["body"]["msgtype"] == "stream"
    assert payload["body"]["stream"]["id"] == "session-1"
    assert payload["body"]["stream"]["finish"] is True


def test_chat_key_to_send_target_preserves_group_for_per_user_mode() -> None:
    assert chat_key_to_send_target("group-user:room-1:alice") == (2, "room-1")
    assert chat_key_to_send_target("single:alice") == (1, "alice")


def test_build_proactive_text_payload_mentions_group_user() -> None:
    payload = build_proactive_text_payload("group-user:room-1:alice", "done")

    assert payload["cmd"] == "aibot_send_msg"
    assert payload["body"]["chat_type"] == 2
    assert payload["body"]["chatid"] == "room-1"
    assert payload["body"]["markdown"]["content"] == "<@alice>\ndone"


def test_build_proactive_text_payload_does_not_mention_single_chat_user() -> None:
    payload = build_proactive_text_payload("single:alice", "done")

    assert payload["cmd"] == "aibot_send_msg"
    assert payload["body"]["chat_type"] == 1
    assert payload["body"]["chatid"] == "alice"
    assert payload["body"]["markdown"]["content"] == "done"


def test_build_proactive_text_payload_truncates_long_content() -> None:
    payload = build_proactive_text_payload("group-user:room-1:alice", "x" * 5000)

    assert payload["body"]["markdown"]["content"].startswith("<@alice>\n")
    assert payload["body"]["markdown"]["content"].endswith("...(truncated)")


def test_split_text_chunks_prefers_word_boundaries() -> None:
    chunks = split_text_chunks("alpha beta gamma delta", max_chars=10)

    assert chunks == ["alpha beta", "gamma", "delta"]


def test_build_text_response_payloads_splits_long_content() -> None:
    payloads = build_text_response_payloads("req-1", "session-1", "x" * 8000, final=True)

    assert len(payloads) == 3
    assert payloads[0]["body"]["stream"]["finish"] is False
    assert payloads[1]["body"]["stream"]["finish"] is False
    assert payloads[2]["body"]["stream"]["finish"] is True
    assert "".join(item["body"]["stream"]["content"] for item in payloads) == "x" * 8000


def test_build_proactive_text_payloads_split_long_content() -> None:
    payloads = build_proactive_text_payloads("group-user:room-1:alice", "x" * 5000)

    assert len(payloads) >= 2
    assert all(item["body"]["markdown"]["content"].startswith("<@alice>\n") for item in payloads)
    assert not any(item["body"]["markdown"]["content"].endswith("...(truncated)") for item in payloads)


def test_strip_text_mentions_supports_bot_name_with_spaces() -> None:
    assert strip_text_mentions("@Leo C /bridge-interrupt", "Leo C") == "/bridge-interrupt"
    assert strip_text_mentions("@Leo C 请分析这句话里的 @Leo C 是否会被保留", "Leo C") == "请分析这句话里的 @Leo C 是否会被保留"
    assert strip_text_mentions("@Alice Bob @Leo C hello", "Leo C") == "hello"
    assert strip_text_mentions("@robot, /bridge-status", "robot") == "/bridge-status"
    assert strip_text_mentions("@bot2 hello", "bot") == "@bot2 hello"


def test_normalize_bridge_command_text_allows_leading_mention_fallback_for_bridge_commands() -> None:
    assert normalize_bridge_command_text("@Leo2 /bridge-interrupt", "Leo") == "/bridge-interrupt"
    assert normalize_bridge_command_text("@robot, /bridge-status", "robot") == "/bridge-status"
    assert normalize_bridge_command_text("@someone hello", "robot") == "@someone hello"


def test_is_subscribe_ok_accepts_success_payload() -> None:
    assert is_subscribe_ok({"cmd": "aibot_subscribe", "errcode": 0}) is True
    assert is_subscribe_ok({"cmd": "aibot_subscribe", "errcode": 1}) is False
    assert is_subscribe_ok({"headers": {"req_id": "req-1"}, "errcode": 0, "errmsg": "ok"}) is True


def test_uid_is_unique_within_same_millisecond() -> None:
    from workspace_bridge import wecom_protocol

    original_time = wecom_protocol.time.time
    wecom_protocol.time.time = lambda: 1000.0
    try:
        first = wecom_protocol.uid()
        second = wecom_protocol.uid()
    finally:
        wecom_protocol.time.time = original_time

    assert first != second
