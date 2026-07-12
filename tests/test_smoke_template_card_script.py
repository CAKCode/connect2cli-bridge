from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "smoke_template_card.sh"


def test_smoke_template_card_script_requires_chat_key() -> None:
    result = subprocess.run(["sh", str(SCRIPT)], capture_output=True, text=True, cwd=REPO_ROOT)

    assert result.returncode == 2
    assert "usage: sh ./smoke_template_card.sh <chat-key> [bot-config-id] [bot-name]" in result.stderr


def test_smoke_template_card_script_does_not_use_shell_eval_wrapper() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert 'sh -c "$CMD"' not in content


def test_smoke_template_card_script_uses_unique_temp_file_and_cleanup_trap() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert 'mktemp /tmp/wecom-smoke-button-card.' in content
    assert 'trap cleanup EXIT INT TERM' in content


def test_smoke_template_card_script_log_filter_matches_all_send_sources() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "message request accepted via api" not in content
    assert "message request accepted via " in content


def test_smoke_template_card_script_does_not_force_default_bot_name_when_omitted(tmp_path: Path) -> None:
    script_path = tmp_path / "smoke_template_card.sh"
    send_message_path = tmp_path / "send_message.py"
    argv_capture = tmp_path / "argv.json"
    script_path.write_text(SCRIPT.read_text(encoding="utf-8"), encoding="utf-8")
    send_message_path.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, sys, pathlib",
                f"pathlib.Path({str(argv_capture)!r}).write_text(json.dumps(sys.argv[1:]), encoding='utf-8')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    send_message_path.chmod(0o755)

    result = subprocess.run(
        ["sh", str(script_path), "single:test-user", "bot-1"],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )

    assert result.returncode == 0
    argv = json.loads(argv_capture.read_text(encoding="utf-8"))
    assert "--bot-config-id" in argv
    assert "bot-1" in argv
    assert "--bot-name" not in argv
