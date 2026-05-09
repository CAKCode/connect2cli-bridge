import json
import subprocess
import sys
from pathlib import Path

from workspace_bridge.config import build_bot_from_app_config, load_app_config
from workspace_bridge.file_send import create_file_send_request, validate_file_for_send
from workspace_bridge.runtime import prepare_session_run
from workspace_bridge.service import create_app


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


async def test_service_send_file_endpoint_validates_workspace_file(tmp_path: Path) -> None:
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
    response = await route.handler(JsonRequest(app))
    payload = json.loads(response.text)
    assert payload["ok"] is True
    assert payload["fileName"] == "result.txt"


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
