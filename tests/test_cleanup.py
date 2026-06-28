from pathlib import Path

from workspace_bridge.cleanup import cleanup_nested_runtime_dirs, find_nested_runtime_dirs


def test_find_nested_runtime_dirs_returns_only_top_level_nested_runtime(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".workspace-bridge-runtime"
    nested = runtime_root / "workspaces" / "team-a" / "users" / "alice" / "workfile" / ".workspace-bridge-runtime"
    deeper = nested / "workspaces" / "team-a" / "users" / "alice" / "workfile" / ".workspace-bridge-runtime"
    deeper.mkdir(parents=True)

    matches = find_nested_runtime_dirs(runtime_root)

    assert matches == [nested.resolve()]


def test_cleanup_nested_runtime_dirs_removes_nested_runtime_only(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".workspace-bridge-runtime"
    workspace_dir = runtime_root / "workspaces" / "team-a" / "users" / "alice" / "workfile"
    nested = workspace_dir / ".workspace-bridge-runtime"
    normal = workspace_dir / "README.md"
    nested.mkdir(parents=True)
    normal.parent.mkdir(parents=True, exist_ok=True)
    normal.write_text("repo", encoding="utf-8")
    (runtime_root / "keep.txt").parent.mkdir(parents=True, exist_ok=True)
    (runtime_root / "keep.txt").write_text("keep", encoding="utf-8")

    removed = cleanup_nested_runtime_dirs(runtime_root)

    assert removed == [nested.resolve()]
    assert not nested.exists()
    assert normal.exists()
    assert (runtime_root / "keep.txt").exists()
