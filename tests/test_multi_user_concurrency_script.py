from pathlib import Path

from check_multi_user_concurrency import _real_mode_preflight, run_many, run_once


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
    assert len(payload["results"]) == 5


async def test_multi_user_concurrency_script_supports_multiple_rounds(tmp_path: Path) -> None:
    payload = await run_many(make_env(tmp_path), 3, "hello", mode="mock", delay_ms=1, rounds=3)

    assert payload["rounds"] == 3
    assert payload["passCount"] == 3
    assert payload["allPass"] is True
    assert len(payload["roundsPayload"]) == 3


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
