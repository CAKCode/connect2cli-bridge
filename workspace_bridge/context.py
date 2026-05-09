from __future__ import annotations

from pathlib import Path

from .models import WorkspaceRef, WorkspaceRuntimeContext
from .skills import resolve_skill_space


def resolve_workspace_cwd(workspace: WorkspaceRef) -> Path:
    if workspace.scope == "user" and workspace.workfile_dir is not None:
        return workspace.project_dir
    if workspace.scope == "room" and workspace.roomfile_dir is not None:
        return workspace.project_dir
    return workspace.project_dir


def build_runtime_context(
    workspace: WorkspaceRef,
    *,
    global_skill_dir: Path | str,
    chatfile_root: Path | str,
) -> WorkspaceRuntimeContext:
    global_skill_dir = Path(global_skill_dir).expanduser().resolve()
    chatfile_root = Path(chatfile_root).expanduser().resolve()
    chatfile_dir = chatfile_root / workspace.workspace_id.replace(":", "__")
    chatfile_dir.mkdir(parents=True, exist_ok=True)

    skill_space = resolve_skill_space(global_skill_dir, workspace.skill_dir)
    env = {
        "WECOM_BRIDGE_WORKSPACE_ID": workspace.workspace_id,
        "WECOM_BRIDGE_WORKSPACE_SCOPE": workspace.scope,
        "WECOM_BRIDGE_SOURCE_DIR": str(workspace.source_dir),
        "WECOM_BRIDGE_PROJECT_DIR": str(workspace.project_dir),
        "WECOM_BRIDGE_WORKSPACE_SKILL_DIR": str(workspace.skill_dir),
        "WECOM_BRIDGE_GLOBAL_SKILL_DIR": str(global_skill_dir),
        "WECOM_BRIDGE_CHATFILE_DIR": str(chatfile_dir),
        "WECOM_BRIDGE_EXPORT_DIR": str(chatfile_dir),
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
    if workspace.workfile_dir is not None:
        allowed_file_roots.append(workspace.workfile_dir.resolve())
    if workspace.roomfile_dir is not None:
        allowed_file_roots.append(workspace.roomfile_dir.resolve())

    return WorkspaceRuntimeContext(
        workspace=workspace,
        project_dir=workspace.project_dir,
        chatfile_dir=chatfile_dir,
        export_dir=chatfile_dir,
        workfile_dir=workspace.workfile_dir,
        roomfile_dir=workspace.roomfile_dir,
        allowed_file_roots=tuple(allowed_file_roots),
        global_skill_dir=global_skill_dir,
        effective_skill_names=tuple(sorted(skill_space.effective_skills)),
        env=env,
    )
