import asyncio
from pathlib import Path

from workspace_bridge.models import BotConfig, FileSendRequest, SourceConfig, WeComBotRuntime
from workspace_bridge.wecom_upload import build_send_file_payload, chat_key_to_send_target, resolve_pending_request, upload_and_send_file


class FakeWS:
    def __init__(self, bot_runtime: WeComBotRuntime) -> None:
        self.bot_runtime = bot_runtime
        self.sent: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)
        cmd = payload["cmd"]
        req_id = payload["headers"]["req_id"]
        if cmd == "aibot_upload_media_init":
            resolve_pending_request(self.bot_runtime, {"headers": {"req_id": req_id}, "errcode": 0, "body": {"upload_id": "upload-1"}})
        elif cmd == "aibot_upload_media_chunk":
            resolve_pending_request(self.bot_runtime, {"headers": {"req_id": req_id}, "errcode": 0})
        elif cmd == "aibot_upload_media_finish":
            resolve_pending_request(self.bot_runtime, {"headers": {"req_id": req_id}, "errcode": 0, "body": {"media_id": "media-1"}})
        elif cmd == "aibot_send_msg":
            resolve_pending_request(self.bot_runtime, {"headers": {"req_id": req_id}, "errcode": 0, "body": {}})


def make_bot_runtime(tmp_path: Path) -> WeComBotRuntime:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    bot = BotConfig(
        bot_id="bot-1",
        bot_name="codex",
        bot_secret="secret-value",
        source=SourceConfig(source_id="src-1", source_dir=source_dir),
        runtime_root=tmp_path / "runtime",
        global_skill_dir=tmp_path / "global",
        chatfile_root=tmp_path / "chatfiles",
    )
    runtime = WeComBotRuntime(config=bot, pending_requests={})
    runtime.ws = FakeWS(runtime)
    return runtime


def test_chat_key_to_send_target_routes_group_user_to_group() -> None:
    assert chat_key_to_send_target("group-user:room-1:alice") == (2, "room-1")
    assert chat_key_to_send_target("single:alice") == (1, "alice")


def test_build_send_file_payload_uses_file_message_shape() -> None:
    payload = build_send_file_payload("group-user:room-1:alice", "media-1")

    assert payload["cmd"] == "aibot_send_msg"
    assert payload["body"]["chat_type"] == 2
    assert payload["body"]["chatid"] == "room-1"
    assert payload["body"]["msgtype"] == "file"
    assert payload["body"]["file"]["media_id"] == "media-1"


async def test_upload_and_send_file_runs_full_protocol(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)
    file_path = tmp_path / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request = FileSendRequest(
        session_id="session-1",
        chat_key="group-user:room-1:alice",
        workspace_id="ws-1",
        file_path=file_path,
        file_name="reply.txt",
    )

    result = await upload_and_send_file(runtime, request)

    assert result["ok"] is True
    assert result["mediaId"] == "media-1"
    assert [payload["cmd"] for payload in runtime.ws.sent] == [
        "aibot_upload_media_init",
        "aibot_upload_media_chunk",
        "aibot_upload_media_finish",
        "aibot_send_msg",
    ]
