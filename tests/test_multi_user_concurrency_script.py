from pathlib import Path
import subprocess
import sys

import pytest

from check_multi_user_concurrency import _build_summary, _real_mode_preflight, run_many, run_once


SCRIPT = Path(__file__).resolve().parent.parent / "check_multi_user_concurrency.py"


def write_secret(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def make_env(tmp_path: Path) -> Path:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"RUNTIME_ROOT={tmp_path / 'runtime'}",
                "WECOM_BOT_NAME=default",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
                f"WECOM_BOT_SOURCE_DIR={source_dir}",
                "WECOM_ENABLED=true",
            ]
        ),
        encoding="utf-8",
    )
    return env_file


async def test_multi_user_concurrency_script_mock_mode_produces_unique_sessions_and_threads(tmp_path: Path) -> None:
    payload = await run_once(make_env(tmp_path), 5, "hello", mode="mock", delay_ms=1)

    assert payload["chatCount"] == 5
    assert payload["allSessionsUnique"] is True
    assert payload["allThreadsUnique"] is True
    assert payload["uniqueSessionCount"] == 5
    assert payload["uniqueThreadCount"] == 5
    assert payload["allSessionIdsPresent"] is True
    assert payload["allThreadIdsPresent"] is True
    assert payload["maxConcurrentProcesses"] >= 2
    assert payload["concurrencyProven"] is True
    assert len(payload["results"]) == 5


async def test_multi_user_concurrency_script_mock_mode_does_not_wait_for_ack_timeouts(tmp_path: Path) -> None:
    payload = await run_once(make_env(tmp_path), 2, "hello", mode="mock", delay_ms=1)

    assert payload["elapsedMs"] < 5000
    assert payload["wsPayloadCount"] >= 2
    assert payload["pass"] is True


async def test_multi_user_concurrency_script_mock_mode_needs_no_env_or_secret(tmp_path: Path) -> None:
    payload = await run_once(tmp_path / "missing.env", 3, "hello", mode="mock", delay_ms=1)

    assert payload["pass"] is True
    assert payload["uniqueSessionCount"] == 3
    assert payload["uniqueThreadCount"] == 3


def test_multi_user_concurrency_summary_rejects_missing_thread_id() -> None:
    payload = _build_summary(
        [{"sessionId": "session-1", "threadId": None}],
        ws_payload_count=1,
        elapsed_ms=1,
        mode="real",
    )

    assert payload["allThreadIdsPresent"] is False
    assert payload["pass"] is False


def test_multi_user_concurrency_summary_rejects_serial_mock_execution() -> None:
    payload = _build_summary(
        [
            {"sessionId": "session-1", "threadId": "thread-1"},
            {"sessionId": "session-2", "threadId": "thread-2"},
        ],
        ws_payload_count=2,
        elapsed_ms=1,
        mode="mock",
        max_concurrent_processes=1,
    )

    assert payload["concurrencyProven"] is False
    assert payload["pass"] is False


async def test_multi_user_concurrency_script_mock_mode_ignores_configured_secret_and_runtime(
    tmp_path: Path, monkeypatch
) -> None:
    import check_multi_user_concurrency as script_module

    configured_runtime = tmp_path / "production-runtime"
    configured_chatfiles = tmp_path / "production-chatfiles"
    configured_output = tmp_path / "production-output"
    configured_file_send = tmp_path / "production-file-send"
    configured_agent_runtime = tmp_path / "production-agent-runtime"
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"RUNTIME_ROOT={configured_runtime}",
                f"CHATFILE_ROOT={configured_chatfiles}",
                f"CODEX_OUTPUT_ROOT={configured_output}",
                f"FILE_SEND_ROOTS={configured_file_send}",
                f"AGENT_RUNTIME_ROOT={configured_agent_runtime}",
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SECRET_FILE={tmp_path / 'missing-production.secret'}",
                f"WECOM_BOT_SOURCE_DIR={tmp_path}",
            ]
        ),
        encoding="utf-8",
    )
    isolated_runtime = tmp_path / "isolated-runtime"

    def fake_mkdtemp(*_args, **_kwargs):
        isolated_runtime.mkdir(parents=True, exist_ok=True)
        return str(isolated_runtime)

    monkeypatch.setattr(script_module.tempfile, "mkdtemp", fake_mkdtemp)

    payload = await script_module.run_once(env_file, 2, "hello", mode="mock", delay_ms=1)

    assert payload["pass"] is True
    assert not configured_runtime.exists()
    assert not configured_chatfiles.exists()
    assert not configured_output.exists()
    assert not configured_file_send.exists()
    assert not configured_agent_runtime.exists()
    assert not isolated_runtime.exists()


async def test_multi_user_concurrency_script_rejects_zero_chat_count(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="chat_count must be >= 1"):
        await run_once(make_env(tmp_path), 0, "hello", mode="mock", delay_ms=1)


async def test_multi_user_concurrency_script_supports_multiple_rounds(tmp_path: Path) -> None:
    payload = await run_many(make_env(tmp_path), 3, "hello", mode="mock", delay_ms=1, rounds=3)

    assert payload["rounds"] == 3
    assert payload["passCount"] == 3
    assert payload["allPass"] is True
    assert len(payload["roundsPayload"]) == 3


async def test_multi_user_concurrency_script_rejects_zero_rounds(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="rounds must be >= 1"):
        await run_many(make_env(tmp_path), 3, "hello", mode="mock", delay_ms=1, rounds=0)


def test_multi_user_concurrency_script_main_rejects_zero_chat_count() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--chat-count", "0"],
        capture_output=True,
        text=True,
        cwd=SCRIPT.parent,
        check=False,
    )

    assert result.returncode == 2
    assert '"--chat-count must be >= 1"' in result.stdout


def test_multi_user_concurrency_script_main_rejects_directory_env_file(tmp_path: Path) -> None:
    env_dir = tmp_path / ".env"
    env_dir.mkdir()

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--env-file", str(env_dir)],
        capture_output=True,
        text=True,
        cwd=SCRIPT.parent,
        check=False,
    )

    assert result.returncode == 2
    assert f'"env file is not a file: {env_dir.resolve()}"' in result.stdout


async def test_multi_user_concurrency_script_captures_round_failure_as_structured_payload(tmp_path: Path, monkeypatch) -> None:
    import check_multi_user_concurrency as script_module

    async def fake_run_once(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(script_module, "run_once", fake_run_once)

    payload = await script_module.run_many(make_env(tmp_path), 2, "hello", mode="real", delay_ms=1, rounds=2)

    assert payload["allPass"] is False
    assert payload["passCount"] == 0
    assert len(payload["roundsPayload"]) == 2
    assert all(item["pass"] is False for item in payload["roundsPayload"])
    assert all(item["error"] == "boom" for item in payload["roundsPayload"])


def test_multi_user_concurrency_script_real_mode_preflight_reports_missing_settings(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("WECOM_BOT_ID=bot-1\n", encoding="utf-8")

    errors = _real_mode_preflight(env_file)

    assert "missing required setting for real mode: WECOM_BOT_SOURCE_DIR" in errors
    assert "missing required setting for real mode: WECOM_BOT_SECRET_FILE" in errors


def test_multi_user_concurrency_script_real_mode_preflight_rejects_directory_env_file(tmp_path: Path) -> None:
    env_dir = tmp_path / ".env"
    env_dir.mkdir()

    errors = _real_mode_preflight(env_dir)

    assert errors == [f"env file is not a file: {env_dir.resolve()}"]


def test_multi_user_concurrency_script_real_mode_preflight_reports_missing_paths(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SOURCE_DIR={tmp_path / 'missing-source'}",
                f"WECOM_BOT_SECRET_FILE={tmp_path / 'missing.secret'}",
            ]
        ),
        encoding="utf-8",
    )

    errors = _real_mode_preflight(env_file)

    assert any("WECOM_BOT_SOURCE_DIR does not exist" in item for item in errors)
    assert any("WECOM_BOT_SECRET_FILE does not exist" in item for item in errors)


def test_multi_user_concurrency_script_real_mode_preflight_rejects_non_directory_source_dir(tmp_path: Path) -> None:
    source_file = tmp_path / "not-a-dir"
    source_file.write_text("x", encoding="utf-8")
    secret_file = tmp_path / "bot.secret"
    secret_file.write_text("secret\n", encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SOURCE_DIR={source_file}",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
            ]
        ),
        encoding="utf-8",
    )

    errors = _real_mode_preflight(env_file)

    assert errors == [f"WECOM_BOT_SOURCE_DIR is not a directory: {source_file}"]


def test_multi_user_concurrency_script_real_mode_preflight_rejects_non_file_secret_path(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    secret_dir = tmp_path / "secret-dir"
    secret_dir.mkdir()
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SOURCE_DIR={source_dir}",
                f"WECOM_BOT_SECRET_FILE={secret_dir}",
            ]
        ),
        encoding="utf-8",
    )

    errors = _real_mode_preflight(env_file)

    assert errors == [f"WECOM_BOT_SECRET_FILE is not a file: {secret_dir}"]


def test_multi_user_concurrency_script_real_mode_preflight_passes_when_minimum_paths_exist(tmp_path: Path) -> None:
    secret_file = tmp_path / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    secret_file.write_text("secret\n", encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SOURCE_DIR={source_dir}",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
            ]
        ),
        encoding="utf-8",
    )

    errors = _real_mode_preflight(env_file)

    assert errors == []


def test_multi_user_concurrency_script_real_mode_preflight_accepts_quoted_env_values(tmp_path: Path) -> None:
    secret_file = tmp_path / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    secret_file.write_text("secret\n", encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                'WECOM_BOT_ID="bot-1"',
                f'WECOM_BOT_SOURCE_DIR="{source_dir}"',
                f'WECOM_BOT_SECRET_FILE="{secret_file}"',
            ]
        ),
        encoding="utf-8",
    )

    errors = _real_mode_preflight(env_file)

    assert errors == []


def test_multi_user_concurrency_script_real_mode_preflight_uses_configured_backend_executable(
    tmp_path: Path, monkeypatch
) -> None:
    import check_multi_user_concurrency as script_module

    secret_file = tmp_path / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    secret_file.write_text("secret\n", encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SOURCE_DIR={source_dir}",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
                "WECOM_BOT_AGENT_BACKEND=claude",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        script_module.shutil,
        "which",
        lambda name: "/usr/bin/claude" if name == "claude" else None,
    )

    errors = script_module._real_mode_preflight(env_file)

    assert errors == []


def test_multi_user_concurrency_script_real_mode_preflight_reports_invalid_backend(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    secret_file = tmp_path / "bot.secret"
    secret_file.write_text("secret\n", encoding="utf-8")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "WECOM_BOT_ID=bot-1",
                f"WECOM_BOT_SOURCE_DIR={source_dir}",
                f"WECOM_BOT_SECRET_FILE={secret_file}",
                "WECOM_BOT_AGENT_BACKEND=not-a-backend",
            ]
        ),
        encoding="utf-8",
    )

    errors = _real_mode_preflight(env_file)

    assert errors == ["unsupported agent backend: not-a-backend"]


async def test_multi_user_concurrency_script_mock_mode_accepts_quoted_env_values(tmp_path: Path) -> None:
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                f'RUNTIME_ROOT="{tmp_path / "runtime"}"',
                'WECOM_BOT_NAME="default"',
                'WECOM_BOT_ID="bot-1"',
                f'WECOM_BOT_SECRET_FILE="{secret_file}"',
                f'WECOM_BOT_SOURCE_DIR="{source_dir}"',
                'WECOM_ENABLED="true"',
            ]
        ),
        encoding="utf-8",
    )

    payload = await run_once(env_file, 3, "hello", mode="mock", delay_ms=1)

    assert payload["chatCount"] == 3
    assert payload["allSessionsUnique"] is True
    assert payload["allThreadsUnique"] is True


async def test_multi_user_concurrency_script_removes_temp_runtime_root_when_env_omits_it(
    tmp_path: Path, monkeypatch
) -> None:
    import check_multi_user_concurrency as script_module

    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                'WECOM_BOT_NAME="default"',
                'WECOM_BOT_ID="bot-1"',
                f'WECOM_BOT_SECRET_FILE="{secret_file}"',
                f'WECOM_BOT_SOURCE_DIR="{source_dir}"',
                'WECOM_ENABLED="true"',
            ]
        ),
        encoding="utf-8",
    )
    temp_runtime_root = tmp_path / "temp-runtime"

    def fake_mkdtemp(*_args, **_kwargs):
        temp_runtime_root.mkdir(parents=True, exist_ok=True)
        return str(temp_runtime_root)

    monkeypatch.setattr(script_module.tempfile, "mkdtemp", fake_mkdtemp)

    payload = await script_module.run_once(env_file, 2, "hello", mode="mock", delay_ms=1)

    assert payload["pass"] is True
    assert not temp_runtime_root.exists()
