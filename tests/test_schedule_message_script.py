from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
BRIDGE_PATH = REPO_ROOT / "bridge.py"
SCHEDULE_MESSAGE_PATH = REPO_ROOT / "schedule_message.py"


def load_schedule_message_module():
    bridge_spec = importlib.util.spec_from_file_location("bridge", BRIDGE_PATH)
    bridge_module = importlib.util.module_from_spec(bridge_spec)
    assert bridge_spec.loader is not None
    bridge_spec.loader.exec_module(bridge_module)
    sys.modules["bridge"] = bridge_module
    spec = importlib.util.spec_from_file_location("schedule_message_test_module", SCHEDULE_MESSAGE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_schedule_message_parse_args_allows_values_starting_with_dashes():
    module = load_schedule_message_module()

    parsed = module.parse_args(["--message", "--hello", "--session-id", "sess-1", "--run-at", "123"])

    assert parsed["message"] == "--hello"
    assert parsed["session-id"] == "sess-1"
    assert parsed["run-at"] == "123"


def test_schedule_message_parse_args_supports_equals_syntax():
    module = load_schedule_message_module()

    parsed = module.parse_args(["--message=--hello", "--session-id=sess-1", "--run-at=123"])

    assert parsed["message"] == "--hello"
    assert parsed["session-id"] == "sess-1"
    assert parsed["run-at"] == "123"


def test_schedule_message_parse_args_supports_chat_key():
    module = load_schedule_message_module()

    parsed = module.parse_args(["--message=hello", "--chat-key=single:test-user", "--run-at=123"])

    assert parsed["message"] == "hello"
    assert parsed["chat-key"] == "single:test-user"
    assert parsed["run-at"] == "123"


def test_schedule_message_parse_args_rejects_unknown_option(capsys):
    module = load_schedule_message_module()

    try:
        module.parse_args(["--message", "hello", "--cronn", "* * * * *"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit")

    assert "unknown option: --cronn" in capsys.readouterr().err


def test_schedule_message_parse_args_rejects_unknown_option_used_as_value(capsys):
    module = load_schedule_message_module()

    try:
        module.parse_args(["--chat-key", "--cronn", "--run-at", "123", "--message", "hello"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit")

    assert "unknown option: --cronn" in capsys.readouterr().err


def test_schedule_message_main_accepts_chat_key(monkeypatch, capsys):
    module = load_schedule_message_module()
    captured = {}

    def fake_write_one_shot_schedule(data):
        captured.update(data)
        return {"ok": True, "chatKey": data.get("chatKey")}

    monkeypatch.setattr(module, "write_one_shot_schedule", fake_write_one_shot_schedule)
    monkeypatch.setattr(sys, "argv", ["schedule_message.py", "--chat-key", "single:test-user", "--run-at", "123", "--message", "hello"])

    module.main()

    output = capsys.readouterr().out
    assert captured["chatKey"] == "single:test-user"
    assert captured["sessionId"] is None
    assert '"ok": true' in output.lower()


def test_schedule_message_main_uses_env_bot_name(monkeypatch, capsys):
    monkeypatch.setenv("WECOM_BRIDGE_BOT_NAME", "codex2")
    monkeypatch.setenv("WECOM_BRIDGE_BOT_CONFIG_ID", "bot-1")
    module = load_schedule_message_module()
    captured = {}

    def fake_write_one_shot_schedule(data):
        captured.update(data)
        return {"ok": True, "botName": data.get("botName")}

    monkeypatch.setattr(module, "write_one_shot_schedule", fake_write_one_shot_schedule)
    monkeypatch.setattr(sys, "argv", ["schedule_message.py", "--chat-key", "single:test-user", "--run-at", "123", "--message", "hello"])

    module.main()

    output = capsys.readouterr().out
    assert captured["botName"] == "codex2"
    assert captured["targetConfigId"] == "bot-1"
    assert '"ok": true' in output.lower()


def test_schedule_message_main_rejects_conflicting_one_shot_selectors(monkeypatch, capsys):
    module = load_schedule_message_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "schedule_message.py",
            "--chat-key",
            "single:test-user",
            "--run-at",
            "123",
            "--delay-seconds",
            "5",
            "--message",
            "hello",
        ],
    )

    try:
        module.main()
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit")

    assert "choose exactly one of --run-at or --delay-seconds for one-shot scheduling" in capsys.readouterr().err


def test_schedule_message_main_reports_bridge_error_from_local_command(monkeypatch, capsys):
    module = load_schedule_message_module()

    def fake_write_one_shot_schedule(_data):
        raise module.bridge.BridgeError(503, "bot not connected")

    monkeypatch.setattr(module, "write_one_shot_schedule", fake_write_one_shot_schedule)
    monkeypatch.setattr(
        sys,
        "argv",
        ["schedule_message.py", "--chat-key", "single:test-user", "--run-at", "123", "--message", "hello"],
    )

    try:
        module.main()
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("expected SystemExit")

    assert "bot not connected" in capsys.readouterr().err
