from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "check_multi_user_readiness.sh"


def test_check_multi_user_readiness_script_includes_agent_runtime_regression_gate(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir(parents=True, exist_ok=True)
    fake_pytest = fake_bin / "pytest"
    args_file = tmp_path / "args.txt"
    env_file = tmp_path / "env.txt"
    fake_pytest.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                f"printf '%s\\n' \"$@\" > {str(args_file)!r}",
                f"printf '%s' \"$PYTEST_DISABLE_PLUGIN_AUTOLOAD\" > {str(env_file)!r}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    fake_pytest.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}:{env.get('PATH', '')}"

    result = subprocess.run(
        ["sh", str(SCRIPT)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    args = args_file.read_text(encoding="utf-8").splitlines()
    assert args[:3] == ["-q", "-p", "pytest_asyncio.plugin"]
    assert "tests/test_agent_runtime.py" in args
    assert env_file.read_text(encoding="utf-8") == "1"
