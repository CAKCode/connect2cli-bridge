from pathlib import Path

from check_multi_user_concurrency import run_many


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


async def test_multi_user_concurrency_soak_mock_mode_keeps_all_rounds_passing(tmp_path: Path) -> None:
    payload = await run_many(make_env(tmp_path), 5, "hello", mode="mock", delay_ms=1, rounds=5)

    assert payload["rounds"] == 5
    assert payload["passCount"] == 5
    assert payload["allPass"] is True
    assert payload["chatCount"] == 5
    for item in payload["roundsPayload"]:
        assert item["allSessionsUnique"] is True
        assert item["allThreadsUnique"] is True
        assert len(item["results"]) == 5
