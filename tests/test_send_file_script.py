from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SEND_FILE_PATH = REPO_ROOT / "send_file.py"


def load_send_file_module(monkeypatch, queue_root: Path, *, bot_name: str | None = None, bot_config_id: str | None = None):
    monkeypatch.setenv("LOCAL_FILE_SEND_QUEUE_ROOT", str(queue_root))
    monkeypatch.delenv("WECOM_LOCAL_FILE_SEND_QUEUE_ROOT", raising=False)
    if bot_name is None:
        monkeypatch.delenv("WECOM_BRIDGE_BOT_NAME", raising=False)
    else:
        monkeypatch.setenv("WECOM_BRIDGE_BOT_NAME", bot_name)
    if bot_config_id is None:
        monkeypatch.delenv("WECOM_BRIDGE_BOT_CONFIG_ID", raising=False)
    else:
        monkeypatch.setenv("WECOM_BRIDGE_BOT_CONFIG_ID", bot_config_id)
    spec = importlib.util.spec_from_file_location(f"send_file_test_{id(queue_root)}", SEND_FILE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_send_file_script_uses_local_file_send_queue_root(monkeypatch, tmp_path):
    queue_root = tmp_path / "queue"

    module = load_send_file_module(monkeypatch, queue_root)

    assert module.QUEUE_ROOT == queue_root.resolve()
    assert module.PENDING_ROOT == queue_root.resolve() / "pending"
    assert module.RESULT_ROOT == queue_root.resolve() / "results"
    assert module.DEFAULT_RESULT_TIMEOUT_MS == 120000


def test_send_file_script_resolves_relative_queue_root_from_script_dir(monkeypatch):
    monkeypatch.setenv("LOCAL_FILE_SEND_QUEUE_ROOT", "relative-queue")
    monkeypatch.delenv("WECOM_BRIDGE_BOT_NAME", raising=False)
    monkeypatch.delenv("WECOM_BRIDGE_BOT_CONFIG_ID", raising=False)
    spec = importlib.util.spec_from_file_location("send_file_relative_queue_root", SEND_FILE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    expected = SEND_FILE_PATH.resolve().parent / "relative-queue"
    assert module.BASE_QUEUE_ROOT == expected.resolve()
    assert module.resolve_base_queue_root(SEND_FILE_PATH.resolve().parent) == expected.resolve()


def test_send_file_script_uses_configurable_default_timeout(monkeypatch, tmp_path):
    monkeypatch.setenv("LOCAL_FILE_SEND_RESULT_TIMEOUT_MS", "45000")

    module = load_send_file_module(monkeypatch, tmp_path / "queue")

    assert module.DEFAULT_RESULT_TIMEOUT_MS == 45000


def test_send_file_script_identifies_retryable_bridge_result(monkeypatch, tmp_path):
    module = load_send_file_module(monkeypatch, tmp_path / "queue")

    assert module.is_retryable_bridge_result({"ok": False, "statusCode": 503, "error": "bot not connected"}) is True
    assert module.is_retryable_bridge_result({"ok": False, "statusCode": 503, "error": "bot websocket closed"}) is False
    assert module.is_retryable_bridge_result({"ok": False, "statusCode": 503, "error": "bot not running: bot-1"}) is True
    assert module.is_retryable_bridge_result({"ok": False, "statusCode": 503, "error": "other"}) is False
    assert module.is_retryable_bridge_result({"ok": False, "statusCode": 404, "error": "bot not found"}) is False


def test_send_file_parse_timeout_ms_validates_input(monkeypatch, tmp_path, capsys):
    module = load_send_file_module(monkeypatch, tmp_path / "queue")

    assert module.parse_timeout_ms("500") == 1000

    for value, message in (("oops", "timeout-ms must be an integer"), ("0", "timeout-ms must be greater than 0")):
        try:
            module.parse_timeout_ms(value)
        except SystemExit as exc:
            assert exc.code == 2
        else:
            raise AssertionError("expected SystemExit")
        assert message in capsys.readouterr().err


def test_send_file_script_uses_env_default_queue_namespace(monkeypatch, tmp_path):
    queue_root = tmp_path / "queue"
    module = load_send_file_module(monkeypatch, queue_root, bot_config_id="bot:1")

    assert module.DEFAULT_BOT_CONFIG_ID == "bot:1"
    assert module.QUEUE_ROOT == queue_root.resolve() / "targets" / "bot%3A1"
    assert module.PENDING_ROOT == module.QUEUE_ROOT / "pending"
    assert module.RESULT_ROOT == module.QUEUE_ROOT / "results"


def test_send_file_script_uses_env_default_bot_name(monkeypatch, tmp_path):
    module = load_send_file_module(monkeypatch, tmp_path / "queue", bot_name="codex2")

    assert module.DEFAULT_BOT_NAME == "codex2"


def test_send_file_parse_args_allows_values_starting_with_dashes(monkeypatch, tmp_path):
    module = load_send_file_module(monkeypatch, tmp_path / "queue")

    parsed = module.parse_args(["--file-path", "--hello", "--session-id", "sess-1"])

    assert parsed["file-path"] == "--hello"
    assert parsed["session-id"] == "sess-1"


def test_send_file_parse_args_supports_equals_syntax(monkeypatch, tmp_path):
    module = load_send_file_module(monkeypatch, tmp_path / "queue")

    parsed = module.parse_args(["--file-path=--hello", "--session-id=sess-1"])

    assert parsed["file-path"] == "--hello"
    assert parsed["session-id"] == "sess-1"


def test_send_file_parse_args_rejects_unknown_option(monkeypatch, tmp_path, capsys):
    module = load_send_file_module(monkeypatch, tmp_path / "queue")

    try:
        module.parse_args(["--chat-key", "single:test-user", "--timeot-ms", "123"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit")

    assert "unknown option: --timeot-ms" in capsys.readouterr().err


def test_send_file_parse_args_rejects_unknown_option_used_as_value(monkeypatch, tmp_path, capsys):
    module = load_send_file_module(monkeypatch, tmp_path / "queue")

    try:
        module.parse_args(["--chat-key", "--timeot-ms", "123"])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected SystemExit")

    assert "unknown option: --timeot-ms" in capsys.readouterr().err


def test_send_file_script_runs_via_shebang():
    result = subprocess.run([str(SEND_FILE_PATH)], capture_output=True, text=True)

    assert result.returncode == 2
    assert "session-id or chat-key required" in result.stderr


def test_send_file_main_uses_env_bot_name_when_arg_missing(monkeypatch, tmp_path, capsys):
    module = load_send_file_module(monkeypatch, tmp_path / "queue", bot_name="codex2", bot_config_id="bot-1")
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello", encoding="utf-8")
    pending_request: dict[str, object] = {}

    def fake_sleep(_seconds: float) -> None:
        pending_files = sorted(module.PENDING_ROOT.glob("*.json"))
        assert pending_files
        pending_request.update(json.loads(pending_files[0].read_text("utf-8")))
        request_id = pending_request["requestId"]
        module.RESULT_ROOT.mkdir(parents=True, exist_ok=True)
        (module.RESULT_ROOT / f"{request_id}.json").write_text(
            json.dumps({"ok": True, "message": "sent report.txt"}, ensure_ascii=False),
            encoding="utf-8",
        )

    monkeypatch.setattr(module.time, "sleep", fake_sleep)
    monkeypatch.setattr(sys, "argv", ["send_file.py", "--chat-key", "single:test-user", "--file-path", str(file_path)])

    module.main()

    output = capsys.readouterr().out
    assert pending_request["botName"] == "codex2"
    assert pending_request["targetConfigId"] == "bot-1"
    assert pending_request["chatKey"] == "single:test-user"
    assert pending_request["filePath"] == str(file_path.resolve())
    assert isinstance(pending_request["requestedAt"], int)
    assert module.DEFAULT_RESULT_TIMEOUT_MS - 1000 <= pending_request["timeoutMs"] <= module.DEFAULT_RESULT_TIMEOUT_MS
    assert pending_request["expiresAt"] >= pending_request["requestedAt"] + module.DEFAULT_RESULT_TIMEOUT_MS - 1000
    assert '"ok": true' in output.lower()


def test_send_file_main_retries_transient_bridge_errors(monkeypatch, tmp_path, capsys):
    module = load_send_file_module(monkeypatch, tmp_path / "queue", bot_name="codex2", bot_config_id="bot-1")
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello", encoding="utf-8")
    pending_request_ids: list[str] = []
    seen_request_ids: set[str] = set()
    now = {"value": 1000.0}

    def fake_time() -> float:
        return now["value"]

    def fake_sleep(seconds: float) -> None:
        now["value"] += seconds
        pending_files = sorted(module.PENDING_ROOT.glob("*.json"))
        assert pending_files
        pending_request = json.loads(pending_files[-1].read_text("utf-8"))
        request_id = str(pending_request["requestId"])
        if request_id in seen_request_ids:
            return
        seen_request_ids.add(request_id)
        pending_request_ids.append(request_id)
        module.RESULT_ROOT.mkdir(parents=True, exist_ok=True)
        payload = (
            {"ok": False, "statusCode": 503, "error": "bot not connected"}
            if len(pending_request_ids) == 1
            else {"ok": True, "message": "sent report.txt"}
        )
        (module.RESULT_ROOT / f"{request_id}.json").write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )

    monkeypatch.setattr(module.time, "time", fake_time)
    monkeypatch.setattr(module.time, "sleep", fake_sleep)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "send_file.py",
            "--chat-key",
            "single:test-user",
            "--file-path",
            str(file_path),
            "--timeout-ms",
            "5000",
        ],
    )

    module.main()

    output = capsys.readouterr().out
    assert len(pending_request_ids) == 2
    assert '"ok": true' in output.lower()


def test_send_file_main_does_not_retry_ambiguous_websocket_closed(monkeypatch, tmp_path, capsys):
    module = load_send_file_module(monkeypatch, tmp_path / "queue", bot_name="codex2", bot_config_id="bot-1")
    file_path = tmp_path / "report.txt"
    file_path.write_text("hello", encoding="utf-8")
    pending_request_ids: list[str] = []

    def fake_sleep(_seconds: float) -> None:
        pending_files = sorted(module.PENDING_ROOT.glob("*.json"))
        assert pending_files
        pending_request = json.loads(pending_files[-1].read_text("utf-8"))
        request_id = str(pending_request["requestId"])
        if request_id in pending_request_ids:
            return
        pending_request_ids.append(request_id)
        module.RESULT_ROOT.mkdir(parents=True, exist_ok=True)
        (module.RESULT_ROOT / f"{request_id}.json").write_text(
            json.dumps({"ok": False, "statusCode": 503, "error": "bot websocket closed"}, ensure_ascii=False),
            encoding="utf-8",
        )

    monkeypatch.setattr(module.time, "sleep", fake_sleep)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "send_file.py",
            "--chat-key",
            "single:test-user",
            "--file-path",
            str(file_path),
        ],
    )

    try:
        module.main()
    except SystemExit as exc:
        assert exc.code == 1
    else:
        raise AssertionError("expected SystemExit")

    assert len(pending_request_ids) == 1
    assert "bot websocket closed" in capsys.readouterr().err
