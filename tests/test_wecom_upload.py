import asyncio
from pathlib import Path

from workspace_bridge.models import BotConfig, FileSendRequest, OutboundMessage, SourceConfig, WeComBotRuntime
from workspace_bridge.wecom_upload import build_send_file_payload, chat_key_to_send_target, create_request_future, require_ack_ok, resolve_pending_request, upload_and_send_file
from workspace_bridge.wecom_messaging import WeComMessagingProvider
from workspace_bridge.models import TemplateCardUpdateRequest


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
        workspace_namespace="bot-1",
        chatfile_root=tmp_path / "chatfiles",
        codex_exec_mode="sandboxed",
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


def test_build_send_file_payload_rejects_empty_media_id() -> None:
    try:
        build_send_file_payload("single:alice", "")
    except ValueError as exc:
        assert str(exc) == "media_id required"
    else:
        raise AssertionError("expected ValueError")


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


def test_require_ack_ok_rejects_error_payload() -> None:
    try:
        require_ack_ok({"errcode": 40001, "errmsg": "bad secret"}, "upload init")
    except RuntimeError as exc:
        assert "upload init failed: bad secret" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_require_ack_ok_rejects_invalid_errcode_shape() -> None:
    try:
        require_ack_ok({"errcode": "bad", "errmsg": "oops"}, "upload init")
    except RuntimeError as exc:
        assert "upload init failed: invalid errcode" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_require_ack_ok_treats_non_dict_body_as_missing_required_key() -> None:
    try:
        require_ack_ok({"errcode": 0, "body": "bad"}, "upload init", required_body_key="upload_id")
    except RuntimeError as exc:
        assert "upload init failed: missing upload_id" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_wecom_upload_chat_key_to_send_target_rejects_invalid_chat_key() -> None:
    try:
        chat_key_to_send_target("group-user:room-only")
    except ValueError as exc:
        assert str(exc) == "invalid chat key: group-user:room-only"
    else:
        raise AssertionError("expected ValueError")


def test_upload_uid_is_unique_within_same_millisecond() -> None:
    from workspace_bridge import wecom_upload

    original_time = __import__("time").time
    __import__("time").time = lambda: 1000.0
    try:
        first = wecom_upload.uid()
        second = wecom_upload.uid()
    finally:
        __import__("time").time = original_time

    assert first != second


async def test_upload_and_send_file_rejects_failed_send_ack(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)

    async def fake_send_json(payload: dict) -> None:
        runtime.ws.sent.append(payload)
        req_id = payload["headers"]["req_id"]
        cmd = payload["cmd"]
        if cmd == "aibot_upload_media_init":
            resolve_pending_request(runtime, {"headers": {"req_id": req_id}, "errcode": 0, "body": {"upload_id": "upload-1"}})
        elif cmd == "aibot_upload_media_chunk":
            resolve_pending_request(runtime, {"headers": {"req_id": req_id}, "errcode": 0})
        elif cmd == "aibot_upload_media_finish":
            resolve_pending_request(runtime, {"headers": {"req_id": req_id}, "errcode": 0, "body": {"media_id": "media-1"}})
        elif cmd == "aibot_send_msg":
            resolve_pending_request(runtime, {"headers": {"req_id": req_id}, "errcode": 50001, "errmsg": "send failed"})

    runtime.ws.send_json = fake_send_json
    file_path = tmp_path / "reply.txt"
    file_path.write_text("hello", encoding="utf-8")
    request = FileSendRequest(
        session_id="session-1",
        chat_key="group-user:room-1:alice",
        workspace_id="ws-1",
        file_path=file_path,
        file_name="reply.txt",
    )

    try:
        await upload_and_send_file(runtime, request)
    except RuntimeError as exc:
        assert "send file failed: send failed" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


async def test_create_request_future_rejects_duplicate_pending_req_id(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)

    first = create_request_future(runtime, "req-1")

    try:
        create_request_future(runtime, "req-1")
    except RuntimeError as exc:
        assert str(exc) == "duplicate pending request: req-1"
    else:
        raise AssertionError("expected RuntimeError")

    assert runtime.pending_requests["req-1"] is first


async def test_create_request_future_rejects_empty_req_id(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)

    try:
        create_request_future(runtime, "")
    except ValueError as exc:
        assert str(exc) == "req_id required"
    else:
        raise AssertionError("expected ValueError")


async def test_wecom_provider_updates_template_card_with_ack(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)
    provider = WeComMessagingProvider()

    async def fake_send_json(payload: dict) -> None:
        runtime.ws.sent.append(payload)
        req_id = payload["headers"]["req_id"]
        if payload["cmd"] == "aibot_respond_update_msg":
            resolve_pending_request(runtime, {"headers": {"req_id": req_id}, "errcode": 0, "body": {}})

    runtime.ws.send_json = fake_send_json

    result = await provider.update_template_card(
        runtime,
        TemplateCardUpdateRequest(
            req_id="req-update-1",
            template_card={
                "card_type": "button_interaction",
                "main_title": {"title": "updated"},
                "button_list": [{"text": "go", "key": "go"}],
            },
        ),
    )

    assert result["ok"] is True
    assert runtime.ws.sent[0]["cmd"] == "aibot_respond_update_msg"
    assert runtime.ws.sent[0]["body"]["response_type"] == "update_template_card"


async def test_wecom_provider_rejects_invalid_template_card_update_before_send(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)
    provider = WeComMessagingProvider()

    try:
        await provider.update_template_card(
            runtime,
            TemplateCardUpdateRequest(
                req_id="req-update-1",
                template_card={"card_type": "button_interaction", "main_title": {"title": "updated"}},
            ),
        )
    except ValueError as exc:
        assert str(exc) == "button_interaction.button_list required"
    else:
        raise AssertionError("expected ValueError")

    assert runtime.ws.sent == []


async def test_wecom_provider_rejects_proactive_send_ack_error(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)
    provider = WeComMessagingProvider()

    async def fake_send_json(payload: dict) -> None:
        runtime.ws.sent.append(payload)
        req_id = payload["headers"]["req_id"]
        if payload["cmd"] == "aibot_send_msg":
            resolve_pending_request(runtime, {"headers": {"req_id": req_id}, "errcode": 50001, "errmsg": "send failed", "body": {}})

    runtime.ws.send_json = fake_send_json

    try:
        await provider.send_proactive_message(
            runtime,
            OutboundMessage(chat_key="single:alice", msgtype="markdown", content="hello"),
        )
    except RuntimeError as exc:
        assert "proactive send 1 failed: send failed" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


async def test_wecom_provider_rejects_template_card_update_ack_error(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)
    provider = WeComMessagingProvider()

    async def fake_send_json(payload: dict) -> None:
        runtime.ws.sent.append(payload)
        req_id = payload["headers"]["req_id"]
        if payload["cmd"] == "aibot_respond_update_msg":
            resolve_pending_request(runtime, {"headers": {"req_id": req_id}, "errcode": 40001, "errmsg": "bad update", "body": {}})

    runtime.ws.send_json = fake_send_json

    try:
        await provider.update_template_card(
            runtime,
            TemplateCardUpdateRequest(
                req_id="req-update-1",
                template_card={
                    "card_type": "button_interaction",
                    "main_title": {"title": "updated"},
                    "button_list": [{"text": "go", "key": "go"}],
                },
            ),
        )
    except RuntimeError as exc:
        assert "template card update failed: bad update" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


async def test_wecom_provider_response_url_truncates_long_markdown(tmp_path: Path, monkeypatch) -> None:
    runtime = make_bot_runtime(tmp_path)
    provider = WeComMessagingProvider()
    runtime.reply_urls["req-1"] = {
        "responseUrl": "https://example.com/response",
        "chatKey": "group-user:room-1:alice",
        "capturedAtMs": 9999999999999,
        "consumed": False,
    }
    captured = {}

    class FakeResponse:
        status = 200

        async def json(self, content_type=None):
            return {"errcode": 0, "errmsg": "ok"}

    class FakePostContext:
        async def __aenter__(self):
            return FakeResponse()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeSession:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def post(self, url, json=None):
            captured["url"] = url
            captured["body"] = json
            return FakePostContext()

    monkeypatch.setattr("workspace_bridge.wecom_messaging.aiohttp.ClientSession", FakeSession)

    result = await provider.send_via_response_url(
        runtime,
        reply_req_id="req-1",
        message=OutboundMessage(chat_key="group-user:room-1:alice", msgtype="markdown", content="x" * 5000),
    )

    assert result["ok"] is True
    assert captured["url"] == "https://example.com/response"
    assert captured["body"]["msgtype"] == "markdown"
    assert captured["body"]["markdown"]["content"].startswith("<@alice>\n")
    assert captured["body"]["markdown"]["content"].endswith("...(truncated)")


async def test_resolve_pending_request_ignores_non_dict_headers(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)
    pending = create_request_future(runtime, "req-1")
    handled = resolve_pending_request(runtime, {"headers": "bad"})

    assert handled is False
    assert pending.done() is False
    assert "req-1" in runtime.pending_requests


async def test_resolve_pending_request_strips_req_id_whitespace(tmp_path: Path) -> None:
    runtime = make_bot_runtime(tmp_path)
    pending = create_request_future(runtime, "req-1")
    handled = resolve_pending_request(runtime, {"headers": {"req_id": " req-1 "}})

    assert handled is True
    assert pending.done() is True
    assert "req-1" not in runtime.pending_requests
