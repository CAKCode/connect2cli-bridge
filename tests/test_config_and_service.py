from pathlib import Path
import json

from aiohttp.test_utils import make_mocked_request

from workspace_bridge.config import build_bot_from_app_config, load_app_config
from workspace_bridge.service import create_app, load_app


def write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def test_load_app_config_reads_secret_file_and_paths(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")

    config = load_app_config(
        {
            "BRIDGE_BIND": "127.0.0.1:9499",
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )

    assert config.bind_host == "127.0.0.1"
    assert config.bind_port == 9499
    assert config.bot_id == "bot-1"
    assert config.bot_secret == "secret-value"
    assert config.source_dir == source_dir.resolve()


async def test_service_health_and_prepare_session(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "BRIDGE_BIND": "127.0.0.1:9499",
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )
    app = create_app(config)
    health_request = make_mocked_request("GET", "/", app=app)

    health_match = next(route for route in app.router.routes() if route.method == "GET")
    health_response = await health_match.handler(health_request)
    health_payload = json.loads(health_response.text)
    assert health_payload["ok"] is True
    assert health_payload["botId"] == "bot-1"
    assert health_payload["wecomEnabled"] is False
    assert health_payload["wecomConnected"] is False
    assert health_payload["wecomStatus"] is None
    assert health_payload["wecomLastError"] is None
    assert health_payload["wecomTaskPresent"] is False
    assert health_payload["wecomTaskDone"] is None
    assert health_payload["pendingRequests"] == 0
    assert health_payload["pendingStreams"] == 0
    assert health_payload["pendingFinals"] == 0
    assert health_payload["replyStates"] == 0

    class JsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "message": "hello"}

    prepare_match = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/prepare")
    prepared_response = await prepare_match.handler(JsonRequest(app))
    prepared_payload = json.loads(prepared_response.text)
    assert prepared_payload["ok"] is True
    assert prepared_payload["workspaceId"].startswith("user:")
    assert prepared_payload["cwd"].endswith("/project")
    assert prepared_payload["workfileDir"].endswith("/workfile")
    assert "prompt" in prepared_payload
    assert prepared_payload["sessionId"].startswith("session-")


def test_build_bot_from_app_config_returns_runtime_bot(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    config = load_app_config(
        {
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )

    bot = build_bot_from_app_config(config)

    assert bot.bot_id == "bot-1"
    assert bot.source.source_dir == source_dir.resolve()


def test_load_app_accepts_aiohttp_web_factory_signature(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    env_file = tmp_path / ".env"
    write_secret(secret_file, "secret-value\n")
    env_file.write_text(
        "\n".join(
            [
                "BRIDGE_BIND=127.0.0.1:6288",
                f"RUNTIME_ROOT={tmp_path / 'runtime'}",
                "WECOM_BOT_NAME=default",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
                f"WECOM_BOT_SOURCE_DIR={source_dir}",
            ]
        ),
        encoding="utf-8",
    )

    app = load_app([], env_file=env_file)

    assert app is not None
