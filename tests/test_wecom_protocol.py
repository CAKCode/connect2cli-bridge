from pathlib import Path

from workspace_bridge.models import BotConfig, SourceConfig
from workspace_bridge.wecom_protocol import (
    build_subscribe_payload,
    build_text_response_payload,
    chat_key_from_message,
    is_subscribe_ok,
    parse_text_callback,
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


def test_build_text_response_payload_uses_stream_format() -> None:
    payload = build_text_response_payload("req-1", "session-1", "done", final=True)

    assert payload["cmd"] == "aibot_respond_msg"
    assert payload["body"]["msgtype"] == "stream"
    assert payload["body"]["stream"]["id"] == "session-1"
    assert payload["body"]["stream"]["finish"] is True


def test_is_subscribe_ok_accepts_success_payload() -> None:
    assert is_subscribe_ok({"cmd": "aibot_subscribe", "errcode": 0}) is True
    assert is_subscribe_ok({"cmd": "aibot_subscribe", "errcode": 1}) is False
    assert is_subscribe_ok({"headers": {"req_id": "req-1"}, "errcode": 0, "errmsg": "ok"}) is True
