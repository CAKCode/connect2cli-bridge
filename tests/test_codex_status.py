from __future__ import annotations

import json
from pathlib import Path

from workspace_bridge.codex_status import collect_codex_status, format_codex_status


def _write_rollout(session_home: Path, thread_id: str, records: list[dict]) -> None:
    rollout_dir = session_home / "sessions" / "2026" / "08" / "10"
    rollout_dir.mkdir(parents=True)
    rollout_path = rollout_dir / f"rollout-2026-08-10T00-00-00-{thread_id}.jsonl"
    rollout_path.write_text(
        "".join(f"{json.dumps(record, separators=(',', ':'))}\n" for record in records),
        encoding="utf-8",
    )


def test_codex_status_uses_rollout_model_for_existing_thread(tmp_path: Path) -> None:
    default_home = tmp_path / "codex-home"
    session_home = tmp_path / "session-home"
    default_home.mkdir()
    (default_home / "config.toml").write_text(
        'model_provider = "OpenAI"\nmodel = "gpt-5.6-sol"\nmodel_reasoning_effort = "xhigh"\n',
        encoding="utf-8",
    )
    thread_id = "thread-existing"
    _write_rollout(
        session_home,
        thread_id,
        [
            {"type": "session_meta", "payload": {"cli_version": "0.147.0"}},
            {
                "type": "turn_context",
                "payload": {
                    "model": "gpt-5.4",
                    "effort": "high",
                    "summary": "concise",
                    "approval_policy": "never",
                    "sandbox_policy": {"type": "danger-full-access"},
                    "collaboration_mode": {"mode": "default"},
                },
            },
            {
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {
                        "total_token_usage": {"total_tokens": 120, "input_tokens": 100, "output_tokens": 20},
                        "last_token_usage": {"total_tokens": 30},
                        "model_context_window": 1000,
                    },
                },
            },
        ],
    )

    snapshot = collect_codex_status(
        default_codex_home=default_home,
        session_codex_home=session_home,
        directory=tmp_path,
        thread_id=thread_id,
        exec_mode="host",
        agent_backend="codex",
    )
    rendered = format_codex_status(snapshot)

    assert snapshot.model == "gpt-5.4"
    assert snapshot.model_source == "thread"
    assert "Model: gpt-5.4 (reasoning high, summaries concise)" in rendered
    assert "Session: thread-existing" in rendered
    assert "Token usage: 120 total (100 input + 20 output)" in rendered
    assert "[next fresh exec]" not in rendered


def test_codex_status_uses_latest_config_model_without_thread(tmp_path: Path) -> None:
    default_home = tmp_path / "codex-home"
    default_home.mkdir()
    config_path = default_home / "config.toml"
    config_path.write_text('model = "gpt-5.4"\n', encoding="utf-8")
    config_path.write_text(
        'model_provider = "OpenAI"\nmodel = "gpt-5.6-sol"\nmodel_reasoning_effort = "xhigh"\n',
        encoding="utf-8",
    )

    snapshot = collect_codex_status(
        default_codex_home=default_home,
        session_codex_home=tmp_path / "missing-session-home",
        directory=tmp_path,
        thread_id=None,
        exec_mode="host",
        agent_backend="codex",
    )
    rendered = format_codex_status(snapshot)

    assert snapshot.model == "gpt-5.6-sol"
    assert snapshot.model_source == "config"
    assert "Model: gpt-5.6-sol (reasoning xhigh) [next fresh exec]" in rendered
    assert "Session: <not started>" in rendered
