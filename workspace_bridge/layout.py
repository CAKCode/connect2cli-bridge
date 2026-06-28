from __future__ import annotations

import re
from pathlib import Path

from .agent_backends import normalize_agent_backend
from .models import WorkspaceRef


def slugify(value: str, *, fallback: str = "unknown") -> str:
    slug = re.sub(r"[^\w.-]+", "_", str(value or "").strip())
    return slug or fallback


def parse_chat_key(chat_key: str) -> dict[str, str]:
    text = str(chat_key or "").strip()
    if text.startswith("single:"):
        user_id = text.split(":", 1)[1]
        if user_id:
            return {"scope": "user", "user_id": user_id}
    elif text.startswith("group-user:"):
        parts = text.split(":", 2)
        if len(parts) == 3 and parts[1] and parts[2]:
            return {"scope": "user", "room_id": parts[1], "user_id": parts[2]}
    elif text.startswith("group:"):
        room_id = text.split(":", 1)[1]
        if room_id:
            return {"scope": "room", "room_id": room_id}
    raise ValueError(f"invalid chat key: {text}")


def build_workspace_ref(
    runtime_root: Path | str,
    workspace_namespace: str,
    source_dir: Path | str,
    chat_key: str,
    *,
    workspace_mode: str = "team",
    agent_backend: str = "codex",
) -> WorkspaceRef:
    runtime_root = Path(runtime_root).expanduser().resolve()
    source_dir = Path(source_dir).expanduser().resolve()
    namespace = slugify(workspace_namespace)
    mode = str(workspace_mode or "").strip().lower().replace("_", "-")
    if not mode:
        mode = "personal" if normalize_agent_backend(agent_backend) == "claude" else "team"
    if mode not in {"team", "personal"}:
        raise ValueError(f"invalid workspace mode: {workspace_mode}")
    parsed = parse_chat_key(chat_key)

    if parsed["scope"] == "user":
        user_id = parsed["user_id"]
        room_id = parsed.get("room_id")
        root_dir = runtime_root / "workspaces" / namespace / "users" / slugify(user_id)
        workfile_dir = root_dir / "workfile"
        roomfile_dir = (runtime_root / "workspaces" / namespace / "rooms" / slugify(room_id) / "roomfile") if room_id else None
        cwd_dir = workfile_dir if mode == "team" else source_dir
        skill_dir = (workfile_dir / ".codex" / "skills") if mode == "team" else (source_dir / ".codex" / "skills")
        return WorkspaceRef(
            workspace_id=f"user:{namespace}:{slugify(user_id)}",
            scope="user",
            namespace=namespace,
            owner_user_id=user_id,
            owner_room_id=room_id,
            chat_key=chat_key,
            source_dir=source_dir,
            root_dir=root_dir,
            cwd_dir=cwd_dir,
            project_dir=workfile_dir,
            skill_dir=skill_dir,
            state_dir=root_dir / "state",
            workfile_dir=workfile_dir,
            roomfile_dir=roomfile_dir,
            lock_file=runtime_root / "locks" / f"user__{namespace}__{slugify(user_id)}.lock",
            metadata_file=root_dir / "workspace.json",
        )

    room_id = parsed["room_id"]
    root_dir = runtime_root / "workspaces" / namespace / "rooms" / slugify(room_id)
    roomfile_dir = root_dir / "roomfile"
    cwd_dir = roomfile_dir if mode == "team" else source_dir
    skill_dir = (roomfile_dir / ".codex" / "skills") if mode == "team" else (source_dir / ".codex" / "skills")
    return WorkspaceRef(
        workspace_id=f"room:{namespace}:{slugify(room_id)}",
        scope="room",
        namespace=namespace,
        owner_user_id=None,
        owner_room_id=room_id,
        chat_key=chat_key,
        source_dir=source_dir,
        root_dir=root_dir,
        cwd_dir=cwd_dir,
        project_dir=roomfile_dir,
        skill_dir=skill_dir,
        state_dir=root_dir / "state",
        workfile_dir=None,
        roomfile_dir=roomfile_dir,
        lock_file=runtime_root / "locks" / f"room__{namespace}__{slugify(room_id)}.lock",
        metadata_file=root_dir / "workspace.json",
    )


def ensure_workspace_dirs(workspace: WorkspaceRef) -> WorkspaceRef:
    for path in (
        workspace.root_dir,
        workspace.state_dir,
        workspace.lock_file.parent,
        *([workspace.workfile_dir] if workspace.workfile_dir is not None else []),
        *([workspace.roomfile_dir] if workspace.roomfile_dir is not None else []),
    ):
        path.mkdir(parents=True, exist_ok=True)
    if workspace.cwd_dir == workspace.source_dir:
        workspace.skill_dir.mkdir(parents=True, exist_ok=True)
        return workspace
    if workspace.scope == "user":
        assert workspace.workfile_dir is not None
        workspace.workfile_dir.mkdir(parents=True, exist_ok=True)
        room_id = workspace.owner_room_id
        if room_id:
            roomfile_dir = workspace.root_dir.parent.parent / "rooms" / slugify(room_id) / "roomfile"
            roomfile_dir.mkdir(parents=True, exist_ok=True)
    if workspace.scope == "room" and workspace.roomfile_dir is not None:
        workspace.roomfile_dir.mkdir(parents=True, exist_ok=True)
    workspace.skill_dir.mkdir(parents=True, exist_ok=True)
    return workspace


def workspace_migration_marker_file(target_root: Path) -> Path:
    return target_root / ".workspace-layout-migration.json"
