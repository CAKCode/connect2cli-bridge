from __future__ import annotations

import shutil
from pathlib import Path


def _is_relative_to(path: Path, other: Path) -> bool:
    try:
        path.relative_to(other)
        return True
    except ValueError:
        return False


def find_nested_runtime_dirs(runtime_root: Path | str) -> list[Path]:
    root = Path(runtime_root).expanduser().resolve()
    if root.exists() and not root.is_dir():
        raise ValueError(f"runtime_root is not a directory: {root}")
    workspace_root = root / "workspaces"
    if not workspace_root.exists():
        return []
    matches: list[Path] = []
    for pattern in ("workfile/.workspace-bridge-runtime", "roomfile/.workspace-bridge-runtime", "project/.workspace-bridge-runtime"):
        for path in workspace_root.rglob(pattern):
            candidate = path if path.is_absolute() else path.absolute()
            if candidate == root:
                continue
            if any(_is_relative_to(candidate, existing) for existing in matches):
                continue
            matches.append(candidate)
    matches.sort()
    return matches


def cleanup_nested_runtime_dirs(runtime_root: Path | str) -> list[Path]:
    removed: list[Path] = []
    for path in find_nested_runtime_dirs(runtime_root):
        if path.is_symlink():
            try:
                path.unlink()
            except FileNotFoundError:
                continue
            removed.append(path)
            continue
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            removed.append(path)
    return removed
