from __future__ import annotations

import os
import grp
import pwd
import shutil
from pathlib import Path


def resolve_posix_identity(user: str, group: str | None = None) -> tuple[int, int]:
    user_record = pwd.getpwnam(user)
    if group:
        group_record = grp.getgrnam(group)
        return user_record.pw_uid, group_record.gr_gid
    return user_record.pw_uid, user_record.pw_gid


def normalize_optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null"}:
        return None
    return text


def normalize_optional_path(value: str | None, *, base_dir: Path | None = None) -> str | None:
    text = normalize_optional_text(value)
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = ((base_dir or Path.cwd()) / path).resolve()
    else:
        path = path.resolve()
    return str(path)


def config_agent_run_as_user(config: dict) -> str | None:
    return normalize_optional_text(config.get("agentRunAsUser") or config.get("agent_run_as_user"))


def config_agent_run_as_group(config: dict) -> str | None:
    return normalize_optional_text(config.get("agentRunAsGroup") or config.get("agent_run_as_group"))


def config_agent_runtime_root(config: dict, *, base_dir: Path | None = None) -> Path | None:
    normalized = normalize_optional_path(config.get("agentRuntimeRoot") or config.get("agent_runtime_root"), base_dir=base_dir)
    return Path(normalized).resolve() if normalized else None


def build_setpriv_prefix(uid: int, gid: int) -> list[str]:
    return [
        "setpriv",
        f"--reuid={uid}",
        f"--regid={gid}",
        "--clear-groups",
        "--",
    ]


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _copy_root_claude_state(source_root: Path, target_root: Path) -> None:
    if not source_root.exists():
        return
    ensure_dir(target_root)
    copy_dirs = ("plugins", "settings.json")
    for name in copy_dirs:
        source = source_root / name
        target = target_root / name
        if not source.exists() or target.exists():
            continue
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)


def _chown_tree(path: Path, uid: int, gid: int) -> None:
    for root, dirs, files in os.walk(path):
        try:
            os.chown(root, uid, gid)
        except OSError:
            pass
        for name in dirs:
            try:
                os.chown(os.path.join(root, name), uid, gid)
            except OSError:
                pass
        for name in files:
            try:
                os.chown(os.path.join(root, name), uid, gid)
            except OSError:
                pass


def prepare_claude_runtime_root(
    base_root: Path,
    *,
    session_id: str,
    source_claude_root: Path,
    uid: int,
    gid: int,
) -> dict[str, Path]:
    session_root = ensure_dir(base_root / "sessions" / session_id)
    claude_config_dir = ensure_dir(session_root / ".claude")
    tmp_dir = ensure_dir(session_root / "tmp")
    project_dir = ensure_dir(session_root / "project")
    chatfile_dir = ensure_dir(session_root / "chatfile")
    workfile_dir = ensure_dir(session_root / "workfile")
    roomfile_dir = ensure_dir(session_root / "roomfile")
    codex_home_dir = ensure_dir(session_root / ".bridge-codex-home")
    ensure_dir(codex_home_dir / "sessions")
    ensure_dir(codex_home_dir / "tmp")
    _copy_root_claude_state(source_claude_root, claude_config_dir)
    _chown_tree(session_root, uid, gid)
    return {
        "session_root": session_root,
        "claude_config_dir": claude_config_dir,
        "tmp_dir": tmp_dir,
        "project_dir": project_dir,
        "chatfile_dir": chatfile_dir,
        "workfile_dir": workfile_dir,
        "roomfile_dir": roomfile_dir,
        "codex_home_dir": codex_home_dir,
    }


def apply_claude_runtime_env(
    env: dict[str, str],
    layout: dict[str, Path],
) -> dict[str, str]:
    updated = dict(env)
    updated["CLAUDE_CONFIG_DIR"] = str(layout["claude_config_dir"])
    updated["TMPDIR"] = str(layout["tmp_dir"])
    updated["TMP"] = str(layout["tmp_dir"])
    updated["TEMP"] = str(layout["tmp_dir"])
    updated["CODEX_HOME"] = str(layout["codex_home_dir"])
    updated["WECOM_BRIDGE_CHATFILE_DIR"] = str(layout["chatfile_dir"])
    updated["WECOM_BRIDGE_EXPORT_DIR"] = str(layout["chatfile_dir"])
    return updated
