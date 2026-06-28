from __future__ import annotations

import shutil
from pathlib import Path


def find_nested_runtime_dirs(runtime_root: Path | str) -> list[Path]:
    root = Path(runtime_root).expanduser().resolve()
    workspace_root = root / "workspaces"
    if not workspace_root.exists():
        return []
    matches: list[Path] = []
    for pattern in ("workfile/.workspace-bridge-runtime", "roomfile/.workspace-bridge-runtime", "project/.workspace-bridge-runtime"):
        for path in workspace_root.rglob(pattern):
            resolved = path.resolve()
            if resolved == root:
                continue
            if any(str(resolved).startswith(f"{existing}{Path('/')}") for existing in matches):
                continue
            matches.append(resolved)
    matches.sort()
    return matches


def cleanup_nested_runtime_dirs(runtime_root: Path | str) -> list[Path]:
    removed: list[Path] = []
    for path in find_nested_runtime_dirs(runtime_root):
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
            removed.append(path)
    return removed
