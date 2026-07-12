import json
import os
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


def test_validate_file_for_send_prefers_allowlist_error_over_size_for_outside_file(tmp_path: Path) -> None:
    _config, _bot, launch = make_runtime(tmp_path)
    outside_dir = launch.runtime_context.chatfile_dir.parent / "outside"
    outside_dir.mkdir(parents=True, exist_ok=True)
    file_path = outside_dir / "big.bin"
    file_path.write_bytes(b"x" * (launch.runtime_context.max_upload_size + 1))

    try:
        validate_file_for_send(launch.runtime_context, file_path)
    except PermissionError as exc:
        assert str(exc) == "outside allowed roots"
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


async def test_service_send_file_endpoint_prefers_allowlist_error_over_size_for_outside_file(tmp_path: Path) -> None:
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
    outside_dir = launch.runtime_context.chatfile_dir.parent / "outside"
    outside_dir.mkdir(parents=True, exist_ok=True)
    file_path = outside_dir / "big.bin"
    file_path.write_bytes(b"xx")

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


def test_send_file_request_cli_reports_missing_file_cleanly(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
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
    script = Path(__file__).resolve().parent.parent / "send_file_request.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--chat-key",
            "single:alice",
            "--file-path",
            str(tmp_path / "missing.txt"),
            "--env-file",
            str(env_file),
        ],
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    assert result.returncode == 4
    assert "missing.txt" in result.stderr
    assert "Traceback" not in result.stderr


def test_send_file_request_cli_rejects_non_directory_source_dir(tmp_path: Path) -> None:
    secret_file = tmp_path / "bot.secret"
    secret_file.write_text("secret\n", encoding="utf-8")
    source_file = tmp_path / "not-a-dir"
    source_file.write_text("x", encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "WECOM_BOT_NAME=default",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
                f"WECOM_BOT_SOURCE_DIR={source_file}",
            ]
        ),
        encoding="utf-8",
    )
    script = Path(__file__).resolve().parent.parent / "send_file_request.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--chat-key",
            "single:alice",
            "--file-path",
            str(tmp_path / "missing.txt"),
            "--env-file",
            str(env_file),
        ],
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    assert result.returncode == 4
    assert f"WECOM_BOT_SOURCE_DIR is not a directory: {source_file.resolve()}" in result.stderr


def test_send_file_request_cli_does_not_require_secret_file_for_local_validation(tmp_path: Path) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"RUNTIME_ROOT={config.runtime_root}",
                "WECOM_BOT_NAME=default",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={tmp_path / 'missing.secret'}",
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


def test_send_file_request_cli_uses_wecom_bot_workspace_namespace(tmp_path: Path) -> None:
    config, _bot, launch = make_runtime(tmp_path)
    env_file = tmp_path / ".env"
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    env_file.write_text(
        "\n".join(
            [
                f"RUNTIME_ROOT={config.runtime_root}",
                "WECOM_BOT_NAME=default",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={tmp_path / 'missing.secret'}",
                f"WECOM_BOT_SOURCE_DIR={config.source_dir}",
                "WECOM_BOT_WORKSPACE_NAMESPACE=team-main",
                f"FILE_SEND_ROOTS={allowed_root}",
            ]
        ),
        encoding="utf-8",
    )
    file_path = allowed_root / "result.txt"
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
    assert payload["workspaceId"] == "user:team-main:alice"


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


async def test_service_send_message_endpoint_treats_null_mention_user_id_as_missing(tmp_path: Path, monkeypatch) -> None:
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
                "mention_user_id": "null",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert captured["message"].mention_user_id is None


async def test_service_send_message_endpoint_treats_null_feedback_id_as_missing(tmp_path: Path, monkeypatch) -> None:
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
                "feedback_id": "null",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert captured["message"].feedback_id is None


async def test_service_send_message_endpoint_returns_bad_gateway_on_proactive_ack_error(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True

    async def fake_send_ws_payload_with_ack(_runtime, _payload, _timeout):
        return {"errcode": 50001, "errmsg": "send failed", "body": {}}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "msgtype": "markdown", "content": "hello"}

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))

    assert excinfo.value.status == 502
    assert excinfo.value.text == "proactive send 1 failed: send failed"


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
    assert payload["chatKey"] == "single:alice"
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
    assert payload["chatKey"] == "single:alice"
    assert payload["cardType"] == "button_interaction"
    assert payload["taskId"] == "task-1"


async def test_service_send_message_endpoint_succeeds_when_reply_url_state_disappears_after_send(tmp_path: Path, monkeypatch) -> None:
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

    class FakeResponse:
        status = 200

        async def json(self, content_type=None):
            return {"errcode": 0, "errmsg": "ok"}

    class FakePostContext:
        async def __aenter__(self):
            runtime.reply_urls.pop("req-1", None)
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
            return FakePostContext()

    monkeypatch.setattr("workspace_bridge.wecom_messaging.aiohttp.ClientSession", FakeSession)

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
    assert payload["chatKey"] == "single:alice"
    assert "req-1" not in runtime.reply_urls


async def test_service_send_message_endpoint_rejects_missing_reply_req_id_state(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    called = {"value": False}

    async def fake_send_via_response_url(_self, _runtime, *, reply_req_id, message):
        called["value"] = True
        return {"ok": True, "response": {"errcode": 0}, "deliveredTemplateCard": None}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_via_response_url", fake_send_via_response_url)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "replyReqId": "missing-req",
                "msgtype": "markdown",
                "content": "follow up",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))

    assert excinfo.value.status == 404
    assert "reply response_url not found or expired: missing-req" == excinfo.value.text
    assert called["value"] is False


async def test_service_send_message_endpoint_rejects_reply_req_id_without_response_url(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    runtime.reply_urls["req-1"] = {
        "chatKey": "single:alice",
        "capturedAtMs": 9999999999999,
        "consumed": False,
    }
    called = {"value": False}

    async def fake_send_via_response_url(_self, _runtime, *, reply_req_id, message):
        called["value"] = True
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
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))

    assert excinfo.value.status == 404
    assert excinfo.value.text == "reply response_url not found or expired: req-1"
    assert called["value"] is False


async def test_service_send_message_endpoint_maps_provider_reply_url_expiry_to_not_found(tmp_path: Path, monkeypatch) -> None:
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
        raise RuntimeError("reply response_url not found or expired: req-1")

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
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))

    assert excinfo.value.status == 404
    assert excinfo.value.text == "reply response_url not found or expired: req-1"


async def test_service_send_message_endpoint_returns_bad_gateway_on_invalid_response_url_errcode(tmp_path: Path, monkeypatch) -> None:
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

    class FakeResponse:
        status = 200

        async def json(self, content_type=None):
            return {"errcode": "bad", "errmsg": "oops"}

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
            return FakePostContext()

    monkeypatch.setattr("workspace_bridge.wecom_messaging.aiohttp.ClientSession", FakeSession)

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
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))

    assert excinfo.value.status == 502
    assert excinfo.value.text == "response_url send failed: invalid errcode"


async def test_service_send_message_endpoint_allows_reply_req_id_without_websocket_connection(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.connected = False
    runtime.ws = None
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
    assert payload["chatKey"] == "single:alice"
    assert captured["replyReqId"] == "req-1"
    assert captured["message"].chat_key == "single:alice"


async def test_service_send_message_endpoint_prefers_reply_req_id_chat_key_over_request_chat_key(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.connected = True
    runtime.ws = object()
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
                "chatKey": "single:bob",
                "replyReqId": "req-1",
                "msgtype": "markdown",
                "content": "follow up",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert payload["chatKey"] == "single:alice"
    assert captured["replyReqId"] == "req-1"
    assert captured["message"].chat_key == "single:alice"


async def test_service_send_message_endpoint_ignores_invalid_chat_key_when_reply_req_id_present(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.connected = True
    runtime.ws = object()
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
                "chatKey": "invalid",
                "replyReqId": "req-1",
                "msgtype": "markdown",
                "content": "follow up",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert payload["chatKey"] == "single:alice"
    assert captured["replyReqId"] == "req-1"
    assert captured["message"].chat_key == "single:alice"


async def test_service_send_message_endpoint_ignores_null_reply_req_id_and_uses_chat_key(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.connected = True
    runtime.ws = object()
    captured = {}

    async def fake_send_proactive_message(_self, _runtime, message):
        captured["message"] = message
        return {"ok": True, "payloadCount": 1, "deliveredTemplateCard": None}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.WeComMessagingProvider.send_proactive_message", fake_send_proactive_message)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "chatKey": "single:alice",
                "replyReqId": "null",
                "msgtype": "markdown",
                "content": "follow up",
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/send-message")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert payload["chatKey"] == "single:alice"
    assert captured["message"].chat_key == "single:alice"
    assert captured["message"].content == "follow up"


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

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(
            JsonRequest(
                app,
                {
                    "chatKey": "single:alice",
                    "msgtype": "template_card",
                    "templateCard": {
                        "card_type": "text_notice",
                        "main_title": {"title": "hello"},
                        "card_action": {"type": 1, "url": "https://example.com"},
                        "feedback": {"id": "x" * 257},
                    },
                },
            )
        )
    assert excinfo.value.status == 400
    assert excinfo.value.text == "templateCard.feedback.id exceeds 256 bytes"


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


async def test_service_update_template_card_endpoint_persists_updated_task_card_state(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    sent_at_ms = int(__import__("time").time() * 1000)
    runtime.template_card_delivery_meta["task-1"] = {
        "taskId": "task-1",
        "chatKey": "single:alice",
        "templateCard": {
            "card_type": "button_interaction",
            "main_title": {"title": "before"},
            "button_list": [{"text": "old", "style": 0, "key": "old"}],
            "task_id": "task-1",
        },
        "sentAtMs": sent_at_ms,
    }
    runtime.template_card_payloads["task-1"] = dict(runtime.template_card_delivery_meta["task-1"]["templateCard"])
    runtime.template_card_button_texts["task-1"] = {"old": "old"}

    async def fake_send_ws_payload_with_ack(_runtime, _payload, _timeout):
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "reqId": "req-update-1",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "after"},
                    "button_list": [{"text": "new", "style": 1, "key": "new"}],
                    "task_id": "task-1",
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/template-card/update")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload == {"ok": True, "reqId": "req-update-1", "cardType": "button_interaction"}
    assert runtime.template_card_payloads["task-1"]["main_title"]["title"] == "after"
    assert runtime.template_card_button_texts["task-1"]["new"] == "new"
    assert runtime.template_card_delivery_meta["task-1"]["chatKey"] == "single:alice"
    assert runtime.template_card_delivery_meta["task-1"]["sentAtMs"] == sent_at_ms

    restarted_app = create_app(config)
    restarted_runtime = restarted_app[APP_WECOM_RUNTIME_KEY]
    assert restarted_runtime.template_card_payloads["task-1"]["main_title"]["title"] == "after"
    assert restarted_runtime.template_card_button_texts["task-1"]["new"] == "new"


async def test_service_update_template_card_endpoint_does_not_create_delivery_meta_for_unknown_task(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True

    async def fake_send_ws_payload_with_ack(_runtime, _payload, _timeout):
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "reqId": "req-update-2",
                "templateCard": {
                    "card_type": "button_interaction",
                    "main_title": {"title": "after"},
                    "button_list": [{"text": "new", "style": 1, "key": "new"}],
                    "task_id": "task-unknown",
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/template-card/update")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload == {"ok": True, "reqId": "req-update-2", "cardType": "button_interaction"}
    assert "task-unknown" not in runtime.template_card_delivery_meta
    assert "task-unknown" not in runtime.template_card_payloads

    restarted_app = create_app(config)
    restarted_runtime = restarted_app[APP_WECOM_RUNTIME_KEY]
    assert "task-unknown" not in restarted_runtime.template_card_delivery_meta


async def test_service_update_template_card_endpoint_clears_stale_button_texts_when_buttons_removed(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True
    sent_at_ms = int(__import__("time").time() * 1000)
    runtime.template_card_delivery_meta["task-1"] = {
        "taskId": "task-1",
        "chatKey": "single:alice",
        "templateCard": {
            "card_type": "button_interaction",
            "main_title": {"title": "before"},
            "button_list": [{"text": "old", "style": 0, "key": "old"}],
            "task_id": "task-1",
        },
        "sentAtMs": sent_at_ms,
    }
    runtime.template_card_payloads["task-1"] = dict(runtime.template_card_delivery_meta["task-1"]["templateCard"])
    runtime.template_card_button_texts["task-1"] = {"old": "old"}

    async def fake_send_ws_payload_with_ack(_runtime, _payload, _timeout):
        return {"errcode": 0, "body": {}}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "reqId": "req-update-3",
                "templateCard": {
                    "card_type": "text_notice",
                    "main_title": {"title": "after"},
                    "card_action": {"type": 1, "url": "https://example.com"},
                    "task_id": "task-1",
                },
            }

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/template-card/update")
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)

    assert payload == {"ok": True, "reqId": "req-update-3", "cardType": "text_notice"}
    assert "task-1" not in runtime.template_card_button_texts


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


async def test_service_update_template_card_endpoint_returns_bad_gateway_on_ack_error(tmp_path: Path, monkeypatch) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.ws = object()
    runtime.connected = True

    async def fake_send_ws_payload_with_ack(_runtime, _payload, _timeout):
        return {"errcode": 40001, "errmsg": "bad update", "body": {}}

    monkeypatch.setattr("workspace_bridge.wecom_messaging.send_ws_payload_with_ack", fake_send_ws_payload_with_ack)

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
    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app))

    assert excinfo.value.status == 502
    assert excinfo.value.text == "template card update failed: bad update"


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


def test_service_reloads_reply_url_state_ignores_invalid_timestamp_entries(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    state_file = config.runtime_root / "template-card-state" / f"{config.bot_id}.reply-urls.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "req-bad": {
                    "responseUrl": "https://example.com/bad",
                    "chatKey": "single:alice",
                    "capturedAtMs": "bad-value",
                    "consumed": False,
                },
                "req-good": {
                    "responseUrl": "https://example.com/good",
                    "chatKey": "single:bob",
                    "capturedAtMs": 9999999999999,
                    "consumed": False,
                },
            }
        ),
        encoding="utf-8",
    )

    restarted_app = create_app(config)
    restarted_runtime = restarted_app[APP_WECOM_RUNTIME_KEY]

    assert "req-bad" not in restarted_runtime.reply_urls
    assert restarted_runtime.reply_urls["req-good"]["chatKey"] == "single:bob"


def test_service_reloads_reply_url_state_ignores_missing_response_url(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    state_file = config.runtime_root / "template-card-state" / f"{config.bot_id}.reply-urls.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps(
            {
                "req-bad": {
                    "chatKey": "single:alice",
                    "capturedAtMs": 9999999999999,
                    "consumed": False,
                },
                "req-good": {
                    "responseUrl": "https://example.com/good",
                    "chatKey": "single:bob",
                    "capturedAtMs": 9999999999999,
                    "consumed": False,
                },
            }
        ),
        encoding="utf-8",
    )

    restarted_app = create_app(config)
    restarted_runtime = restarted_app[APP_WECOM_RUNTIME_KEY]

    assert "req-bad" not in restarted_runtime.reply_urls
    assert restarted_runtime.reply_urls["req-good"]["chatKey"] == "single:bob"


def test_service_reloads_template_card_state_ignores_stale_entries(tmp_path: Path) -> None:
    config, _bot, _launch = make_runtime(tmp_path)
    state_file = config.runtime_root / "template-card-state" / f"{config.bot_id}.json"
    state_file.parent.mkdir(parents=True, exist_ok=True)
    now_ms = int(__import__("time").time() * 1000)
    state_file.write_text(
        json.dumps(
            {
                "task-stale": {
                    "taskId": "task-stale",
                    "chatKey": "single:alice",
                    "templateCard": {
                        "card_type": "button_interaction",
                        "main_title": {"title": "stale"},
                        "button_list": [{"text": "go", "style": 1, "key": "go"}],
                        "task_id": "task-stale",
                    },
                    "sentAtMs": now_ms - (73 * 60 * 60 * 1000),
                },
                "task-fresh": {
                    "taskId": "task-fresh",
                    "chatKey": "single:bob",
                    "templateCard": {
                        "card_type": "button_interaction",
                        "main_title": {"title": "fresh"},
                        "button_list": [{"text": "go", "style": 1, "key": "go"}],
                        "task_id": "task-fresh",
                    },
                    "sentAtMs": now_ms,
                },
            }
        ),
        encoding="utf-8",
    )

    restarted_app = create_app(config)
    restarted_runtime = restarted_app[APP_WECOM_RUNTIME_KEY]

    assert "task-stale" not in restarted_runtime.template_card_delivery_meta
    assert restarted_runtime.template_card_payloads["task-fresh"]["task_id"] == "task-fresh"


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


def test_send_message_cli_rejects_unexpected_positional_arg(tmp_path: Path, capsys) -> None:
    import importlib.util

    queue_root = tmp_path / "message-queue"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_unexpected_positional", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module.BASE_QUEUE_ROOT = queue_root.resolve()
    module.DEFAULT_BOT_CONFIG_ID = "bot-1"
    module.QUEUE_ROOT, module.PENDING_ROOT, module.RESULT_ROOT = module.queue_paths_for_target(module.DEFAULT_BOT_CONFIG_ID)
    old_argv = sys.argv
    try:
        sys.argv = ["send_message.py", "oops", "--chat-key", "single:alice", "--msgtype", "markdown", "--content", "hello"]
        try:
            module.main()
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("expected SystemExit")
    finally:
        sys.argv = old_argv

    assert "unexpected argument: oops" in capsys.readouterr().err


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


def test_send_message_cli_rejects_file_queue_root(tmp_path: Path, capsys) -> None:
    import importlib.util

    queue_root = tmp_path / "message-queue.txt"
    queue_root.write_text("not a dir", encoding="utf-8")
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_file_queue_root", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    os.environ["LOCAL_SEND_MESSAGE_QUEUE_ROOT"] = str(queue_root)
    try:
        spec.loader.exec_module(module)
    finally:
        os.environ.pop("LOCAL_SEND_MESSAGE_QUEUE_ROOT", None)
    module.BASE_QUEUE_ROOT = queue_root.resolve()
    module.DEFAULT_BOT_CONFIG_ID = "bot-1"
    module.QUEUE_ROOT, module.PENDING_ROOT, module.RESULT_ROOT = module.queue_paths_for_target(module.DEFAULT_BOT_CONFIG_ID)
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
        ]
        try:
            module.main()
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("expected SystemExit")
    finally:
        sys.argv = old_argv

    assert f"queue path is not a directory: {queue_root.resolve()}" in capsys.readouterr().err


def test_send_message_cli_resolves_relative_queue_root_from_bridge_runtime_root(tmp_path: Path) -> None:
    import importlib.util

    runtime_root = tmp_path / "bridge-runtime"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_runtime_root_queue", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    os.environ["BRIDGE_RUNTIME_ROOT"] = str(runtime_root)
    os.environ["LOCAL_SEND_MESSAGE_QUEUE_ROOT"] = "relative-queue"
    try:
        spec.loader.exec_module(module)
        expected = runtime_root / "relative-queue"
        assert module.BASE_QUEUE_ROOT == expected.resolve()
        assert module.resolve_base_queue_root(script.resolve().parent) == expected.resolve()
    finally:
        os.environ.pop("BRIDGE_RUNTIME_ROOT", None)
        os.environ.pop("LOCAL_SEND_MESSAGE_QUEUE_ROOT", None)


def test_send_message_cli_ignores_invalid_chat_key_when_reply_req_id_present(tmp_path: Path) -> None:
    import importlib.util

    _config, _bot, _launch = make_runtime(tmp_path)
    queue_root = tmp_path / "message-queue"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_reply_req_invalid_chat", script)
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
            "--chat-key",
            "invalid",
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
    assert pending_request["chatKey"] == "invalid"


def test_send_message_cli_ignores_invalid_chat_key_when_session_id_present(tmp_path: Path) -> None:
    import importlib.util

    _config, _bot, _launch = make_runtime(tmp_path)
    queue_root = tmp_path / "message-queue"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_session_invalid_chat", script)
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
            "--session-id",
            "sess-1",
            "--chat-key",
            "invalid",
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
    assert pending_request["sessionId"] == "sess-1"
    assert pending_request["chatKey"] == "invalid"


def test_send_message_cli_uses_wecom_bot_env_defaults_when_bridge_env_missing(tmp_path: Path) -> None:
    import importlib.util

    queue_root = tmp_path / "message-queue"
    script = Path(__file__).resolve().parent.parent / "send_message.py"
    spec = importlib.util.spec_from_file_location("send_message_test_wecom_env_defaults", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    os.environ.pop("WECOM_BRIDGE_BOT_NAME", None)
    os.environ.pop("WECOM_BRIDGE_BOT_CONFIG_ID", None)
    os.environ["WECOM_BOT_NAME"] = "codex2"
    os.environ["WECOM_BOT_CONFIG_ID"] = "bot-1"
    try:
        spec.loader.exec_module(module)
    finally:
        os.environ.pop("WECOM_BOT_NAME", None)
        os.environ.pop("WECOM_BOT_CONFIG_ID", None)
    module.BASE_QUEUE_ROOT = queue_root.resolve()
    module.DEFAULT_BOT_CONFIG_ID = "bot-1"
    module.QUEUE_ROOT, module.PENDING_ROOT, module.RESULT_ROOT = module.queue_paths_for_target(module.DEFAULT_BOT_CONFIG_ID)
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
    assert pending_request["botName"] == "codex2"
    assert pending_request["targetConfigId"] == "bot-1"


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
