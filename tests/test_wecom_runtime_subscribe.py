import json

from aiohttp import WSMsgType

from workspace_bridge.config import load_app_config
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.service import APP_WECOM_RUNTIME_KEY, create_app
from workspace_bridge.wecom_runtime import handle_wecom_payload
from workspace_bridge.wecom_upload import create_request_future


def write_secret(path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def make_config(tmp_path):
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    return load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
        }
    )


class FakeWS:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)

    async def receive(self):
        payload = self.payloads.pop(0)
        return type("Msg", (), {"type": WSMsgType.TEXT, "data": json.dumps(payload)})()


async def test_subscribe_bot_returns_failed_subscribe_response(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    bot = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    future = create_request_future(bot, "req-1")
    ws = FakeWS([])

    async def fake_handler(*_args, **_kwargs):
        return "session-1", "done"

    await handle_wecom_payload(
        config,
        bot,
        ws,
        {"cmd": "aibot_subscribe", "headers": {"req_id": "req-1"}, "errcode": 40001, "errmsg": "bad secret"},
        fake_handler,
    )

    response = await future
    assert response["errcode"] == 40001


async def test_health_exposes_runtime_error_fields(tmp_path) -> None:
    config = make_config(tmp_path)
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.last_status = "subscribe_failed"
    runtime.last_error = "bad secret"
    route = next(route for route in app.router.routes() if route.method == "GET")
    response = await route.handler(type("Req", (), {"app": app})())
    payload = json.loads(response.text)

    assert payload["wecomStatus"] == "subscribe_failed"
    assert payload["wecomLastError"] == "bad secret"


async def test_runtime_status_model_requires_subscribe_success_for_connected(tmp_path) -> None:
    from workspace_bridge.config import build_bot_from_app_config

    config = make_config(tmp_path)
    runtime = WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    runtime.connected = False
    runtime.last_status = "subscribe_failed"
    runtime.last_error = "bad secret"

    assert runtime.connected is False
    assert runtime.last_status == "subscribe_failed"
