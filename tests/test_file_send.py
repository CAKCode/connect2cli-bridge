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
    file_path = launch.runtime_context.project_dir / "report.txt"
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
    file_path = launch.runtime_context.project_dir / "result.txt"
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

    async def fake_upload_and_send_file(bot_runtime, request):
        captured["runtime"] = bot_runtime
        captured["request"] = request
        return {"ok": True, "mediaId": "media-1"}

    monkeypatch.setattr("workspace_bridge.service.upload_and_send_file", fake_upload_and_send_file)
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

    async def fake_upload_and_send_file(_bot_runtime, _request):
        raise RuntimeError("send file failed: send failed")

    monkeypatch.setattr("workspace_bridge.service.upload_and_send_file", fake_upload_and_send_file)
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

    async def fake_upload_and_send_file(_bot_runtime, _request):
        raise ConnectionResetError("transport disconnected")

    monkeypatch.setattr("workspace_bridge.service.upload_and_send_file", fake_upload_and_send_file)
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
