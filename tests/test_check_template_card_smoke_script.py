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
