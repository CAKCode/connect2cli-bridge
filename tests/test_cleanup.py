import os
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


def test_find_nested_runtime_dirs_rejects_file_runtime_root(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".workspace-bridge-runtime"
    runtime_root.write_text("not a dir", encoding="utf-8")

    try:
        find_nested_runtime_dirs(runtime_root)
    except ValueError as exc:
        assert str(exc) == f"runtime_root is not a directory: {runtime_root.resolve()}"
    else:
        raise AssertionError("expected ValueError")


def test_find_nested_runtime_dirs_keeps_symlink_path_instead_of_target(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".workspace-bridge-runtime"
    workspace_dir = runtime_root / "workspaces" / "team-a" / "users" / "alice" / "workfile"
    nested = workspace_dir / ".workspace-bridge-runtime"
    target = tmp_path / "external-runtime-target"
    target.mkdir(parents=True)
    workspace_dir.mkdir(parents=True, exist_ok=True)
    nested.symlink_to(target, target_is_directory=True)

    matches = find_nested_runtime_dirs(runtime_root)

    assert matches == [nested]


def test_cleanup_nested_runtime_dirs_unlinks_symlink_without_deleting_target(tmp_path: Path) -> None:
    runtime_root = tmp_path / ".workspace-bridge-runtime"
    workspace_dir = runtime_root / "workspaces" / "team-a" / "users" / "alice" / "workfile"
    nested = workspace_dir / ".workspace-bridge-runtime"
    target = tmp_path / "external-runtime-target"
    target.mkdir(parents=True)
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    workspace_dir.mkdir(parents=True, exist_ok=True)
    nested.symlink_to(target, target_is_directory=True)

    removed = cleanup_nested_runtime_dirs(runtime_root)

    assert removed == [nested]
    assert not os.path.lexists(nested)
    assert target.exists()
    assert (target / "keep.txt").exists()
