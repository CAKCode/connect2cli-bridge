from __future__ import annotations

import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "check_template_card_smoke.sh"


def test_check_template_card_smoke_script_handles_missing_log(tmp_path: Path) -> None:
    missing_log = tmp_path / "missing.log"
    result = subprocess.run(
        ["sh", str(SCRIPT)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={"BRIDGE_LOG_FILE": str(missing_log)},
    )

    assert result.returncode == 1
    assert "bridge log not found:" in result.stderr


def test_check_template_card_smoke_script_expands_tilde_log_path(tmp_path: Path) -> None:
    home_dir = tmp_path / "home"
    home_dir.mkdir(parents=True, exist_ok=True)
    log_file = home_dir / "bridge.log"
    log_file.write_text("42:message.send_accepted\n", encoding="utf-8")

    result = subprocess.run(
        ["sh", str(SCRIPT)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env={"HOME": str(home_dir), "BRIDGE_LOG_FILE": "~/bridge.log"},
    )

    assert result.returncode == 0
    assert "[check] recent template-card smoke signals" in result.stdout
    assert "42:message.send_accepted" in result.stdout


def test_check_template_card_smoke_script_matches_text_send_logs_too() -> None:
    content = SCRIPT.read_text(encoding="utf-8")

    assert "message.send_accepted" in content
    assert "message request accepted via " in content
