from __future__ import annotations

import os
import shutil
from pathlib import Path

from .models import DEFAULT_GLOBAL_SKILL_DIR, WorkspaceRef, WorkspaceRuntimeContext
from .skills import resolve_skill_space


DEFAULT_CODEX_HOME = Path(__import__("os").environ.get("CODEX_HOME") or (Path.home() / ".codex")).expanduser().resolve()


def _remove_existing_path(target: Path) -> None:
    if target.is_dir() and not target.is_symlink():
        shutil.rmtree(target, ignore_errors=True)
    else:
        target.unlink(missing_ok=True)


def _copy_child_to_target(source: Path, target: Path, *, replace_existing: bool) -> None:
    if replace_existing and (target.exists() or target.is_symlink()):
        _remove_existing_path(target)
    elif target.exists() or target.is_symlink():
        return

    if source.is_symlink():
        target.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(os.readlink(source), target, target_is_directory=source.is_dir())
    elif source.is_dir():
        shutil.copytree(source, target, symlinks=True)
    else:
        shutil.copy2(source, target, follow_symlinks=False)


def build_session_codex_home(runtime_root: Path | str, session_id: str, workspace_skill_dir: Path) -> Path:
    runtime_root = Path(runtime_root).expanduser().resolve()
    session_home = runtime_root / ".bridge-codex-home" / "sessions" / session_id
    session_home.mkdir(parents=True, exist_ok=True)
    (session_home / "tmp").mkdir(parents=True, exist_ok=True)
    skills_root = session_home / "skills"
    skills_root.mkdir(parents=True, exist_ok=True)
    for child in list(skills_root.iterdir()):
        _remove_existing_path(child)

    allowed_root_files = {"auth.json", "config.toml", "version.json", "installation_id"}
    if DEFAULT_CODEX_HOME.exists() and DEFAULT_CODEX_HOME.is_dir():
        for child in DEFAULT_CODEX_HOME.iterdir():
            if child.name in {"skills", "sessions", "tmp"}:
                continue
            if not child.is_file() or child.name not in allowed_root_files:
                continue
            shutil.copy2(child, session_home / child.name)

    if DEFAULT_GLOBAL_SKILL_DIR.exists() and DEFAULT_GLOBAL_SKILL_DIR.is_dir():
        for child in DEFAULT_GLOBAL_SKILL_DIR.iterdir():
            target = skills_root / child.name
            _copy_child_to_target(child, target, replace_existing=False)
    if workspace_skill_dir.exists() and workspace_skill_dir.is_dir():
        for child in workspace_skill_dir.iterdir():
            target = skills_root / child.name
            _copy_child_to_target(child, target, replace_existing=True)
    return session_home


def resolve_workspace_cwd(workspace: WorkspaceRef) -> Path:
    return workspace.cwd_dir


def build_runtime_context(
    workspace: WorkspaceRef,
    *,
    runtime_root: Path | str,
    session_id: str,
    chatfile_root: Path | str,
    workspace_mode: str = "team",
    cwd_dir: Path | str | None = None,
    skill_dir: Path | str | None = None,
    codex_exec_mode: str = "host",
    agent_backend: str = "codex",
    file_send_roots: tuple[Path, ...] = (),
    max_upload_size: int = 100 * 1024 * 1024,
) -> WorkspaceRuntimeContext:
    chatfile_root = Path(chatfile_root).expanduser().resolve()
    chatfile_dir = chatfile_root / session_id
    chatfile_dir.mkdir(parents=True, exist_ok=True)
    resolved_workspace_mode = str(workspace_mode or "").strip().lower().replace("_", "-") or "team"
    resolved_cwd_dir = Path(cwd_dir).expanduser().resolve() if cwd_dir is not None else workspace.cwd_dir
    resolved_skill_dir = Path(skill_dir).expanduser().resolve() if skill_dir is not None else workspace.skill_dir

    skill_space = resolve_skill_space(DEFAULT_GLOBAL_SKILL_DIR, resolved_skill_dir)
    codex_home_dir = build_session_codex_home(runtime_root, session_id, resolved_skill_dir)
    env = {
        "WECOM_BRIDGE_WORKSPACE_ID": workspace.workspace_id,
        "WECOM_BRIDGE_WORKSPACE_SCOPE": workspace.scope,
        "WECOM_BRIDGE_WORKSPACE_NAMESPACE": workspace.namespace,
        "WECOM_BRIDGE_WORKSPACE_MODE": resolved_workspace_mode,
        "WECOM_BRIDGE_SOURCE_DIR": str(workspace.source_dir),
        "WECOM_BRIDGE_PROJECT_DIR": str(resolved_cwd_dir),
        "WECOM_BRIDGE_WORKDIR_DIR": str(workspace.source_dir),
        "WECOM_BRIDGE_WORKSPACE_SKILL_DIR": str(resolved_skill_dir),
        "WECOM_BRIDGE_CHATFILE_DIR": str(chatfile_dir),
        "WECOM_BRIDGE_EXPORT_DIR": str(chatfile_dir),
        "WECOM_BRIDGE_EXEC_MODE": str(codex_exec_mode).strip().lower() or "host",
        "WECOM_BRIDGE_AGENT_BACKEND": str(agent_backend).strip().lower() or "codex",
        "CODEX_HOME": str(codex_home_dir),
        "TMPDIR": str(chatfile_dir),
        "TMP": str(chatfile_dir),
        "TEMP": str(chatfile_dir),
    }
    if workspace.workfile_dir is not None:
        env["WECOM_BRIDGE_WORKFILE_DIR"] = str(workspace.workfile_dir)
    if workspace.roomfile_dir is not None:
        env["WECOM_BRIDGE_ROOMFILE_DIR"] = str(workspace.roomfile_dir)
    if workspace.owner_user_id:
        env["WECOM_BRIDGE_USER_ID"] = workspace.owner_user_id
    if workspace.owner_room_id:
        env["WECOM_BRIDGE_ROOM_ID"] = workspace.owner_room_id

    allowed_file_roots = [chatfile_dir.resolve()]
    for root in file_send_roots:
        resolved = Path(root).expanduser().resolve()
        if resolved not in allowed_file_roots:
            allowed_file_roots.append(resolved)

    return WorkspaceRuntimeContext(
        workspace=workspace,
        workspace_mode=resolved_workspace_mode,  # type: ignore[arg-type]
        cwd_dir=resolved_cwd_dir,
        chatfile_dir=chatfile_dir,
        export_dir=chatfile_dir,
        workfile_dir=workspace.workfile_dir,
        roomfile_dir=workspace.roomfile_dir,
        allowed_file_roots=tuple(allowed_file_roots),
        max_upload_size=max(1, int(max_upload_size)),
        codex_exec_mode=(str(codex_exec_mode).strip().lower() or "host"),
        effective_skill_names=tuple(sorted(skill_space.effective_skills)),
        env=env,
    )
