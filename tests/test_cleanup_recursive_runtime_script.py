from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "cleanup_recursive_runtime.py"


def test_cleanup_recursive_runtime_apply_unlinks_nested_symlink_only(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".workspace-bridge-runtime"
    workspace_dir = runtime_root / "workspaces" / "team-a" / "users" / "alice" / "workfile"
    nested = workspace_dir / ".workspace-bridge-runtime"
    target = tmp_path / "external-runtime-target"
    target.mkdir(parents=True)
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    workspace_dir.mkdir(parents=True, exist_ok=True)
    nested.symlink_to(target, target_is_directory=True)

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--runtime-root", str(runtime_root), "--apply"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )

    assert result.returncode == 0
    assert f"runtime_root={runtime_root.resolve()}" in result.stdout
    assert "matches=1" in result.stdout
    assert "removed=1" in result.stdout
    assert not os.path.lexists(nested)
    assert target.exists()
    assert (target / "keep.txt").exists()


def test_cleanup_recursive_runtime_rejects_file_runtime_root(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".workspace-bridge-runtime"
    runtime_root.write_text("not a dir", encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--runtime-root", str(runtime_root)],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )

    assert result.returncode == 2
    assert f"runtime_root is not a directory: {runtime_root.resolve()}" in result.stderr
