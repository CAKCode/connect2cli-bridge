from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "ops_service_control.sh"


def test_ops_service_control_script_passes_service_name_after_double_dash(tmp_path: Path) -> None:
    fake_systemctl = tmp_path / "fake-systemctl.sh"
    captured = tmp_path / "captured.txt"
    fake_systemctl.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                f"printf '%s\\n' \"$@\" > {str(captured)!r}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)

    env = dict(os.environ)
    env["OPS_ALLOWED_SERVICE_NAMES"] = "--danger.service"
    env["OPS_SYSTEMCTL_BIN"] = str(fake_systemctl)

    result = subprocess.run(
        ["bash", str(SCRIPT), "restart", "--danger.service"],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert captured.read_text(encoding="utf-8").splitlines() == ["restart", "--", "--danger.service"]
