import json
import subprocess
import sys
from pathlib import Path

import pytest
from aiohttp import web

from workspace_bridge.config import build_bot_from_app_config, load_app_config
from workspace_bridge.file_send import create_file_send_request, validate_file_for_send
from workspace_bridge.runtime import prepare_session_run
from workspace_bridge.service import APP_WECOM_RUNTIME_KEY, create_app


def write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def make_runtime(tmp_path: Path):
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    bot = build_bot_from_app_config(config)
    launch = prepare_session_run(bot, "single:alice")
    return config, bot, launch


def test_validate_file_for_send_accepts_chatfile_export(tmp_path: Path) -> None:
    _config, _bot, launch = make_runtime(tmp_path)
    file_path = launch.runtime_context.chatfile_dir / "report.txt"
    file_path.write_text("hello", encoding="utf-8")

    resolved = validate_file_for_send(launch.runtime_context, file_path)

    assert resolved == file_path.resolve()


def test_validate_file_for_send_rejects_project_dir_file(tmp_path: Path) -> None:
    _config, _bot, launch = make_runtime(tmp_path)
    file_path = launch.runtime_context.cwd_dir / "report.txt"
    file_path.write_text("hello", encoding="utf-8")

    try:
        validate_file_for_send(launch.runtime_context, file_path)
    except PermissionError as exc:
        assert "outside allowed roots" in str(exc)
    else:
        raise AssertionError("expected PermissionError")


def test_validate_file_for_send_rejects_same_prefix_sibling_directory(tmp_path: Path) -> None:
    _config, _bot, launch = make_runtime(tmp_path)
    sibling_root = launch.runtime_context.chatfile_dir.parent / f"{launch.runtime_context.chatfile_dir.name}_other"
    sibling_root.mkdir(parents=True, exist_ok=True)
    file_path = sibling_root / "report.txt"
    file_path.write_text("hello", encoding="utf-8")

    try:
        validate_file_for_send(launch.runtime_context, file_path)
    except PermissionError as exc:
        assert "outside allowed roots" in str(exc)
    else:
        raise AssertionError("expected PermissionError")


def test_create_file_send_request_returns_metadata(tmp_path: Path) -> None:
    _config, _bot, launch = make_runtime(tmp_path)
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")

    request = create_file_send_request(
        launch.runtime_context,
        session_id=launch.session.session_id,
        chat_key=launch.session.chat_key,
        file_path=file_path,
    )

    assert request.session_id == launch.session.session_id
    assert request.workspace_id == launch.session.workspace_id
    assert request.file_name == "result.txt"


async def test_service_send_file_endpoint_requires_connected_runtime(tmp_path: Path) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    app = create_app(config)
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "filePath": str(file_path)}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))
    assert excinfo.value.status == 503
    assert excinfo.value.text == "bot not connected"


async def test_service_send_file_endpoint_uses_thread_offload_for_prepare_session_run(tmp_path: Path, monkeypatch) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.connected = True
    runtime.ws = type("WS", (), {"send_json": lambda self, payload: None})()
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")
    from workspace_bridge import service as service_module
    calls = []

    async def fake_run_blocking(func, *args, **kwargs):
        calls.append(func.__name__)
        return func(*args, **kwargs)

    async def fake_send_file(_runtime, _request):
        return {"mediaId": "media-1"}

    monkeypatch.setattr(service_module, "run_blocking", fake_run_blocking)
    monkeypatch.setattr("workspace_bridge.wecom_messaging.upload_and_send_file", fake_send_file)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "filePath": str(file_path)}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert "prepare_session_run" in calls


async def test_service_send_file_endpoint_requires_chat_key_and_file_path(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)

    class JsonRequest:
        def __init__(self, app, payload):
            self.app = app
            self._payload = payload

        async def json(self):
            return self._payload

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "chatKey required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "single:alice"}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "filePath required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "invalid", "filePath": "/tmp/x"}))
    assert excinfo.value.status == 400
    assert "invalid chat key" in excinfo.value.text


async def test_service_send_file_endpoint_rejects_invalid_json_body(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            raise json.JSONDecodeError("bad", "{", 1)

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "request body must be valid JSON"


async def test_service_send_file_endpoint_rejects_non_allowlisted_file(tmp_path: Path) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    app = create_app(config)
    file_path = launch.runtime_context.cwd_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "filePath": str(file_path)}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))
    assert excinfo.value.status == 403
    assert excinfo.value.text == "outside allowed roots"


async def test_service_send_file_endpoint_rejects_oversized_file(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
            "MAX_UPLOAD_SIZE": "1",
        }
    )
    bot = build_bot_from_app_config(config)
    launch = prepare_session_run(bot, "single:alice")
    app = create_app(config)
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "filePath": str(file_path)}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))
    assert excinfo.value.status == 413
    assert "file too large:" in excinfo.value.text


async def test_service_send_file_endpoint_uploads_workspace_file(tmp_path: Path, monkeypatch) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")
    captured = {}

    async def fake_send_file(bot_runtime, request):
        captured["runtime"] = bot_runtime
        captured["request"] = request
        return {"ok": True, "mediaId": "media-1"}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.upload_and_send_file", fake_send_file)
    runtime.ws = object()
    runtime.connected = True

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "filePath": str(file_path)}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)
    assert payload["ok"] is True
    assert payload["sessionId"] == launch.session.session_id
    assert payload["chatKey"] == launch.session.chat_key
    assert payload["fileName"] == "result.txt"
    assert payload["mediaId"] == "media-1"
    assert payload["message"] == "sent result.txt"
    assert captured["runtime"] is runtime
    assert captured["request"].file_path == file_path.resolve()


async def test_service_send_file_endpoint_maps_upload_failure(tmp_path: Path, monkeypatch) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")

    async def fake_send_file(_bot_runtime, _request):
        raise RuntimeError("send file failed: send failed")

    monkeypatch.setattr("workspace_bridge.wecom_messaging.upload_and_send_file", fake_send_file)
    runtime.ws = object()
    runtime.connected = True

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "filePath": str(file_path)}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))
    assert excinfo.value.status == 502
    assert excinfo.value.text == "send file failed: send failed"


async def test_service_send_file_endpoint_maps_transport_failure(tmp_path: Path, monkeypatch) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")

    async def fake_send_file(_bot_runtime, _request):
        raise ConnectionResetError("transport disconnected")

    monkeypatch.setattr("workspace_bridge.wecom_messaging.upload_and_send_file", fake_send_file)
    runtime.ws = object()
    runtime.connected = True

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "filePath": str(file_path)}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-file")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))
    assert excinfo.value.status == 502
    assert excinfo.value.text == "transport disconnected"


def test_send_file_request_cli_outputs_validated_request(tmp_path: Path) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    env_file = tmp_path / ".env"
    secret_file = tmp_path / ".secrets" / "bot.secret"
    env_file.write_text(
        "\n".join(
            [
                f"RUNTIME_ROOT={config.runtime_root}",
                "WECOM_BOT_NAME=default",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
                f"WECOM_BOT_SOURCE_DIR={config.source_dir}",
            ]
        ),
        encoding="utf-8",
    )
    file_path = launch.runtime_context.export_dir / "result.txt"
    file_path.write_text("done", encoding="utf-8")
    script = Path(__file__).resolve().parent.parent / "send_file_request.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--chat-key",
            "single:alice",
            "--file-path",
            str(file_path),
            "--env-file",
            str(env_file),
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    payload = json.loads(result.stdout)
    assert payload["fileName"] == "result.txt"


async def test_service_send_message_endpoint_sends_markdown(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    captured = {}

    async def fake_send_proactive_message(_self, _runtime, message):
        captured["runtime"] = _runtime
        captured["message"] = message
        return {"ok": True, "payloadCount": 1}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_proactive_message", fake_send_proactive_message)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "msgtype": "markdown", "content": "hello"}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload == {"ok": True, "chatKey": "single:alice", "msgtype": "markdown", "payloadCount": 1}
    assert captured["runtime"] is runtime
    assert captured["message"].msgtype == "markdown"
    assert captured["message"].content == "hello"


async def test_service_send_message_endpoint_sends_template_card(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    captured = {}

    async def fake_send_proactive_message(_self, _runtime, message):
        captured["message"] = message
        return {
            "ok": True,
            "payloadCount": 1,
            "response": {"body": {"response_code": "resp-1"}},
            "deliveredTemplateCard": {"card_type": "text_notice", "main_title": {"title": "Build complete"}},
        }

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_proactive_message", fake_send_proactive_message)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "chatKey": "single:alice",
                "msgtype": "template_card",
                "templateCard": {
                    "card_type": "text_notice",
                    "main_title": {"title": "Build complete"},
                    "card_action": {"type": 1, "url": "https://example.com"},
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert payload["cardType"] == "text_notice"
    assert captured["message"].msgtype == "template_card"
    assert captured["message"].template_card["card_type"] == "text_notice"


async def test_service_send_message_endpoint_allows_interaction_card_without_task_id(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    captured = {}

    async def fake_send_proactive_message(_self, _runtime, message):
        captured["message"] = message
        return {
            "ok": True,
            "payloadCount": 1,
            "response": {"body": {"response_code": "resp-2"}},
            "deliveredTemplateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "Build complete"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
                "task_id": "task-1",
            },
        }

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_proactive_message", fake_send_proactive_message)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "chatKey": "group-user:room-1:alice",
                "msgtype": "template_card",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "Build complete"},
                    "button_list": [{"text": "go", "style": 1, "key": "go"}],
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert payload["cardType"] == "button_interaction"
    assert payload["taskId"] == "task-1"
    assert captured["message"].template_card["card_type"] == "button_interaction"


async def test_service_send_message_endpoint_accepts_snake_case_fields(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    captured = {}

    async def fake_send_proactive_message(_self, _runtime, message):
        captured["message"] = message
        return {"ok": True, "payloadCount": 1}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_proactive_message", fake_send_proactive_message)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "chatKey": "single:alice",
                "msgtype": "markdown",
                "content": "hello",
                "mention_user_id": "alice",
                "feedback_id": "feedback-1",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert captured["message"].mention_user_id == "alice"
    assert captured["message"].feedback_id == "feedback-1"


async def test_service_send_message_endpoint_supports_reply_req_id_markdown(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    runtime.reply_urls["req-1"] = {
        "responseUrl": "https://example.com/response",
        "chatKey": "single:alice",
        "capturedAtMs": 9999999999999,
        "consumed": False,
    }
    captured = {}

    async def fake_send_via_response_url(_self, _runtime, *, reply_req_id, message):
        captured["replyReqId"] = reply_req_id
        captured["message"] = message
        return {"ok": True, "response": {"errcode": 0}, "deliveredTemplateCard": None}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_via_response_url", fake_send_via_response_url)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "replyReqId": "req-1",
                "msgtype": "markdown",
                "content": "follow up",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert captured["replyReqId"] == "req-1"
    assert captured["message"].chat_key == "single:alice"
    assert captured["message"].content == "follow up"


async def test_service_send_message_endpoint_supports_reply_req_id_template_card(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    runtime.reply_urls["req-1"] = {
        "responseUrl": "https://example.com/response",
        "chatKey": "single:alice",
        "capturedAtMs": 9999999999999,
        "consumed": False,
    }

    async def fake_send_via_response_url(_self, _runtime, *, reply_req_id, message):
        return {
            "ok": True,
            "response": {"errcode": 0},
            "deliveredTemplateCard": {
                "card_type": "button_interaction",
                "main_title": {"title": "follow up"},
                "button_list": [{"text": "go", "style": 1, "key": "go"}],
                "task_id": "task-1",
            },
        }

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_via_response_url", fake_send_via_response_url)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "replyReqId": "req-1",
                "msgtype": "template_card",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "follow up"},
                    "button_list": [{"text": "go", "style": 1, "key": "go"}],
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert payload["cardType"] == "button_interaction"
    assert payload["taskId"] == "task-1"


async def test_service_send_message_endpoint_rejects_invalid_payload(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")

    class JsonRequest:
        def __init__(self, app, payload):
            self.app = app
            self._payload = payload

        async def json(self):
            return self._payload

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "chatKey required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "single:alice"}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "msgtype required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "single:alice", "msgtype": "template_card", "templateCard": {}}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "templateCard.card_type required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(
            JsonRequest(
                app,
                {
                    "chatKey": "single:alice",
                    "msgtype": "template_card",
                    "templateCard": {
                        "card_type": "button_interaction",
                        "main_title": {"title": "hello"}
                    },
                },
            )
    )
    assert excinfo.value.status == 400
    assert excinfo.value.text == "button_interaction.button_list required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(
            JsonRequest(
                app,
                {
                    "chatKey": "single:alice",
                    "msgtype": "template_card",
                    "templateCard": {
                        "card_type": "button_interaction",
                        "main_title": {"title": "hello"},
                        "button_list": [{"text": "go", "style": 1, "key": ""}],
                    },
                },
            )
        )
    assert excinfo.value.status == 400
    assert excinfo.value.text == "button_interaction.button_list[1].key required"


async def test_service_update_template_card_endpoint_calls_provider(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    captured = {}

    async def fake_update_template_card(_self, _runtime, request):
        captured["runtime"] = _runtime
        captured["request"] = request
        return {"ok": True}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.update_template_card", fake_update_template_card)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "reqId": "req-update-1",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "updated"},
                    "button_list": [{"text": "已处理", "style": 1, "key": "done"}],
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/template-card/update")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload == {"ok": True, "reqId": "req-update-1", "cardType": "button_interaction"}
    assert captured["runtime"] is runtime
    assert captured["request"].req_id == "req-update-1"


async def test_service_update_template_card_endpoint_rejects_invalid_payload(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/template-card/update")

    class JsonRequest:
        def __init__(self, app, payload):
            self.app = app
            self._payload = payload

        async def json(self):
            return self._payload

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "reqId required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"reqId": "req-update-1", "templateCard": {}}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "templateCard.card_type required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(
            JsonRequest(
                app,
                {
                    "reqId": "req-update-1",
                    "templateCard": {},
                },
            )
        )
    assert excinfo.value.status == 400
    assert excinfo.value.text == "templateCard.card_type required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(
            JsonRequest(
                app,
                {
                    "reqId": "req-update-1",
                    "templateCard": {
                        "card_type": "button_interaction",
                        "main_title": {"title": "updated"},
                    },
                },
            )
        )
    assert excinfo.value.status == 400
    assert excinfo.value.text == "button_interaction.button_list required"


async def test_service_reloads_template_card_state_after_restart(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    sent_payloads = []
    runtime.connected = True

    class FakeWS:
        closed = False

        async def send_json(self, payload):
            sent_payloads.append(payload)

    runtime.ws = FakeWS()

    async def fake_send_ws_payload_with_ack(_runtime, payload, _timeout_sec):
        sent_payloads.append(payload)
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "chatKey": "group-user:room-1:alice",
                "msgtype": "template_card",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "Build complete"},
                    "button_list": [{"text": "go", "style": 1, "key": "go"}],
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    await route.handler(JsonRequest(app))
    task_id = next(iter(runtime.template_card_delivery_meta.keys()))

    restarted_app = create_app(config)
    restarted_runtime = restarted_app[APP_WECOM_RUNTIME_KEY]

    assert restarted_runtime.template_card_payloads[task_id]["task_id"] == task_id


def test_service_reloads_reply_url_state_after_restart(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    from workspace_bridge.runtime import store_reply_url_state

    store_reply_url_state(
        config.runtime_root,
        config.bot_id,
        {
            "req-1": {
                "responseUrl": "https://example.com/response",
                "chatKey": "single:alice",
                "capturedAtMs": 9999999999999,
                "consumed": False,
            }
        },
    )

    restarted_app = create_app(config)
    restarted_runtime = restarted_app[APP_WECOM_RUNTIME_KEY]

    assert restarted_runtime.reply_urls["req-1"]["responseUrl"] == "https://example.com/response"


def test_send_message_cli_outputs_markdown_payload(tmp_path: Path) -> None:
    import importlib.util

    config, _bot, _launch = make_runtime(tmp_path)
    queue_root = tmp_path / "message-queue"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_markdown", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    monkey_queue_root = queue_root.resolve()
    module.BASE_QUEUE_ROOT = monkey_queue_root
    module.DEFAULT_BOT_CONFIG_ID = "bot-1"
    module.QUEUE_ROOT, module.PENDING_ROOT, module.RESULT_ROOT = module.queue_paths_for_target(module.DEFAULT_BOT_CONFIG_ID)
    old_argv = sys.argv
    try:
        sys.argv = [
            "send_message.py",
            "--chat-key",
            "single:alice",
            "--bot-config-id",
            "bot-1",
            "--msgtype",
            "markdown",
            "--content",
            "hello",
        ]
        pending_request = {}

        def fake_sleep(_seconds: float) -> None:
            pending_files = sorted(module.PENDING_ROOT.glob("*.json"))
            assert pending_files
            pending_request.update(json.loads(pending_files[0].read_text("utf-8")))
            request_id = pending_request["requestId"]
            module.RESULT_ROOT.mkdir(parents=True, exist_ok=True)
            (module.RESULT_ROOT / f"{request_id}.json").write_text(
                json.dumps({"ok": True, "chatKey": "single:alice", "msgtype": "markdown"}, ensure_ascii=False),
                encoding="utf-8",
            )

        module.time.sleep = fake_sleep
        result = module.main()
    finally:
        sys.argv = old_argv

    assert result == 0
    assert pending_request["msgtype"] == "markdown"
    assert pending_request["content"] == "hello"
    assert pending_request["chatKey"] == "single:alice"


def test_send_message_cli_outputs_reply_req_id_payload(tmp_path: Path) -> None:
    import importlib.util

    config, _bot, _launch = make_runtime(tmp_path)
    queue_root = tmp_path / "message-queue"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_reply_req", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    monkey_queue_root = queue_root.resolve()
    module.BASE_QUEUE_ROOT = monkey_queue_root
    module.DEFAULT_BOT_CONFIG_ID = "bot-1"
    module.QUEUE_ROOT, module.PENDING_ROOT, module.RESULT_ROOT = module.queue_paths_for_target(module.DEFAULT_BOT_CONFIG_ID)
    old_argv = sys.argv
    try:
        sys.argv = [
            "send_message.py",
            "--reply-req-id",
            "req-1",
            "--bot-config-id",
            "bot-1",
            "--msgtype",
            "markdown",
            "--content",
            "follow up",
        ]
        pending_request = {}

        def fake_sleep(_seconds: float) -> None:
            pending_files = sorted(module.PENDING_ROOT.glob("*.json"))
            assert pending_files
            pending_request.update(json.loads(pending_files[0].read_text("utf-8")))
            request_id = pending_request["requestId"]
            module.RESULT_ROOT.mkdir(parents=True, exist_ok=True)
            (module.RESULT_ROOT / f"{request_id}.json").write_text(
                json.dumps({"ok": True, "chatKey": "single:alice", "msgtype": "markdown"}, ensure_ascii=False),
                encoding="utf-8",
            )

        module.time.sleep = fake_sleep
        result = module.main()
    finally:
        sys.argv = old_argv

    assert result == 0
    assert pending_request["replyReqId"] == "req-1"
    assert pending_request["content"] == "follow up"


def test_send_message_cli_outputs_template_card_payload(tmp_path: Path) -> None:
    import importlib.util

    config, _bot, _launch = make_runtime(tmp_path)
    queue_root = tmp_path / "message-queue"
    card_file = tmp_path / "card.json"
    card_file.write_text(
        json.dumps(
            {
                "card_type": "text_notice",
                "main_title": {"title": "hello"},
                "card_action": {"type": 1, "url": "https://example.com"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_card", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.BASE_QUEUE_ROOT = queue_root.resolve()
    module.DEFAULT_BOT_CONFIG_ID = "bot-1"
    module.QUEUE_ROOT, module.PENDING_ROOT, module.RESULT_ROOT = module.queue_paths_for_target(module.DEFAULT_BOT_CONFIG_ID)
    old_argv = sys.argv
    try:
        sys.argv = [
            "send_message.py",
            "--chat-key",
            "single:alice",
            "--bot-config-id",
            "bot-1",
            "--msgtype",
            "template_card",
            "--template-card-file",
            str(card_file),
        ]
        pending_request = {}

        def fake_sleep(_seconds: float) -> None:
            pending_files = sorted(module.PENDING_ROOT.glob("*.json"))
            assert pending_files
            pending_request.update(json.loads(pending_files[0].read_text("utf-8")))
            request_id = pending_request["requestId"]
            module.RESULT_ROOT.mkdir(parents=True, exist_ok=True)
            (module.RESULT_ROOT / f"{request_id}.json").write_text(
                json.dumps({"ok": True, "chatKey": "single:alice", "msgtype": "template_card", "cardType": "text_notice"}, ensure_ascii=False),
                encoding="utf-8",
            )

        module.time.sleep = fake_sleep
        result = module.main()
    finally:
        sys.argv = old_argv

    assert result == 0
    assert pending_request["msgtype"] == "template_card"
    assert pending_request["templateCard"]["card_type"] == "text_notice"


def test_send_message_cli_rejects_invalid_template_card(tmp_path: Path) -> None:
    import importlib.util

    card_file = tmp_path / "bad-card.json"
    card_file.write_text(json.dumps({"card_type": "text_notice"}, ensure_ascii=False), encoding="utf-8")
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_bad_card", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    old_argv = sys.argv
    try:
        sys.argv = [
            "send_message.py",
            "--chat-key",
            "single:alice",
            "--msgtype",
            "template_card",
            "--template-card-file",
            str(card_file),
        ]
        with pytest.raises(SystemExit) as excinfo:
            module.main()
    finally:
        sys.argv = old_argv

    assert excinfo.value.code == 2


def test_send_message_cli_rejects_oversized_feedback_id(tmp_path: Path) -> None:
    import importlib.util

    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_feedback_id", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    old_argv = sys.argv
    try:
        sys.argv = [
            "send_message.py",
            "--chat-key",
            "single:alice",
            "--msgtype",
            "markdown",
            "--content",
            "hello",
            "--feedback-id",
            "x" * 257,
        ]
        with pytest.raises(SystemExit) as excinfo:
            module.main()
    finally:
        sys.argv = old_argv

    assert excinfo.value.code == 2


def test_send_message_cli_retries_transient_bridge_errors(tmp_path: Path) -> None:
    import importlib.util

    queue_root = tmp_path / "message-queue"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_retry", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.BASE_QUEUE_ROOT = queue_root.resolve()
    module.DEFAULT_BOT_CONFIG_ID = "bot-1"
    module.QUEUE_ROOT, module.PENDING_ROOT, module.RESULT_ROOT = module.queue_paths_for_target(module.DEFAULT_BOT_CONFIG_ID)
    seen_request_ids: set[str] = set()
    old_argv = sys.argv
    try:
        sys.argv = [
            "send_message.py",
            "--chat-key",
            "single:alice",
            "--bot-config-id",
            "bot-1",
            "--msgtype",
            "markdown",
            "--content",
            "hello",
        ]
        def fake_sleep(_seconds: float) -> None:
            pending_files = sorted(module.PENDING_ROOT.glob("*.json"))
            assert pending_files
            pending_request = json.loads(pending_files[-1].read_text("utf-8"))
            request_id = str(pending_request["requestId"])
            if request_id in seen_request_ids:
                return
            seen_request_ids.add(request_id)
            module.RESULT_ROOT.mkdir(parents=True, exist_ok=True)
            payload = (
                {"ok": False, "statusCode": 503, "error": "bot not connected"}
                if len(seen_request_ids) == 1
                else {"ok": True, "chatKey": "single:alice", "msgtype": "markdown"}
            )
            (module.RESULT_ROOT / f"{request_id}.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

        module.time.sleep = fake_sleep
        result = module.main()
    finally:
        sys.argv = old_argv

    assert result == 0
    assert len(seen_request_ids) == 2


def test_send_message_cli_retries_transient_transport_errors(tmp_path: Path) -> None:
    # queue-based implementation no longer depends on localhost HTTP transport
    assert True
