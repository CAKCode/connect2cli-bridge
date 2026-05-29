from __future__ import annotations

import os
import shutil
from functools import lru_cache
from pathlib import Path


def looks_like_codex_command(command: str | None) -> bool:
    raw = str(command or "").strip()
    if not raw:
        return True
    name = Path(raw).name.lower()
    return name == "codex" or name == "codex.js" or "codex" in name


def _path_entries(path_value: str) -> list[str]:
    return [item for item in str(path_value or "").split(os.pathsep) if item]


def _prepend_path(path_value: str, entry: str) -> str:
    items: list[str] = []
    seen: set[str] = set()
    for value in [entry, *_path_entries(path_value)]:
        normalized = value.rstrip("/\\") or value
        if not value or normalized in seen:
            continue
        seen.add(normalized)
        items.append(value)
    return os.pathsep.join(items)


@lru_cache(maxsize=32)
def resolve_executable(command: str) -> Path | None:
    raw = str(command or "").strip()
    if not raw:
        return None

    if os.path.sep in raw or (os.path.altsep and os.path.altsep in raw):
        candidate = Path(raw).expanduser()
        if candidate.exists():
            return candidate.resolve()
        return None

    resolved = shutil.which(raw)
    if not resolved:
        return None
    return Path(resolved).expanduser().resolve()


@lru_cache(maxsize=32)
def find_bundled_bwrap(command: str) -> Path | None:
    executable = resolve_executable(command)
    if executable is None:
        return None

    patterns = (
        "node_modules/@openai/codex-*/vendor/*/codex-resources/bwrap",
        "@openai/codex-*/vendor/*/codex-resources/bwrap",
        "vendor/*/codex-resources/bwrap",
        "codex-resources/bwrap",
    )
    search_roots: list[Path] = []
    for root in (executable.parent, *executable.parents[:6]):
        resolved_root = root.resolve()
        if resolved_root in search_roots:
            continue
        search_roots.append(resolved_root)

    for root in search_roots:
        for pattern in patterns:
            for candidate in root.glob(pattern):
                if candidate.is_file():
                    return candidate.resolve()
    return None


def prepare_codex_subprocess_env(env: dict[str, str] | None = None, *, command: str | None = None) -> dict[str, str]:
    prepared = dict(os.environ if env is None else env)
    if not looks_like_codex_command(command):
        return prepared

    path_value = prepared.get("PATH") or os.defpath
    if shutil.which("bwrap", path=path_value):
        return prepared

    bundled_bwrap = find_bundled_bwrap(str(command or "codex"))
    if bundled_bwrap is None:
        return prepared

    prepared["PATH"] = _prepend_path(path_value, str(bundled_bwrap.parent))
    return prepared
