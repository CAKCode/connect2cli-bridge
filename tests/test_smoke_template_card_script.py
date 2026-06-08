from __future__ import annotations

import subprocess
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
