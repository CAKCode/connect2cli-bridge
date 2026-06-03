from __future__ import annotations

import hashlib
import re
from pathlib import Path

from .models import WorkspaceRef


def slugify(value: str, *, fallback: str = "unknown") -> str:
    slug = re.sub(r"[^\w.-]+", "_", str(value or "").strip())
    return slug or fallback


def source_key(source_dir: Path | str) -> str:
    resolved = Path(source_dir).expanduser().resolve()
    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:12]
    return f"src_{digest}"


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


def build_workspace_ref(runtime_root: Path | str, source_dir: Path | str, chat_key: str) -> WorkspaceRef:
    runtime_root = Path(runtime_root).expanduser().resolve()
    source_dir = Path(source_dir).expanduser().resolve()
    parsed = parse_chat_key(chat_key)
    src_key = source_key(source_dir)

    if parsed["scope"] == "user":
        user_id = parsed["user_id"]
        room_id = parsed.get("room_id")
        root_dir = runtime_root / "workspaces" / "users" / slugify(user_id) / src_key
        workspace_id = f"user:{slugify(user_id)}:{src_key}"
        workfile_dir = root_dir / "workfile"
        return WorkspaceRef(
            workspace_id=workspace_id,
            scope="user",
            owner_user_id=user_id,
            owner_room_id=room_id,
            chat_key=chat_key,
            source_dir=source_dir,
            source_key=src_key,
            root_dir=root_dir,
            project_dir=root_dir / "project",
            skill_dir=(root_dir / "project" / ".codex" / "skills"),
            state_dir=root_dir / "state",
            workfile_dir=workfile_dir,
            roomfile_dir=(runtime_root / "workspaces" / "rooms" / slugify(room_id) / src_key / "roomfile") if room_id else None,
            lock_file=runtime_root / "locks" / f"{workspace_id}.lock",
            metadata_file=root_dir / "workspace.json",
        )

    room_id = parsed["room_id"]
    root_dir = runtime_root / "workspaces" / "rooms" / slugify(room_id) / src_key
    workspace_id = f"room:{slugify(room_id)}:{src_key}"
    roomfile_dir = root_dir / "roomfile"
    return WorkspaceRef(
        workspace_id=workspace_id,
        scope="room",
        owner_user_id=None,
        owner_room_id=room_id,
        chat_key=chat_key,
        source_dir=source_dir,
        source_key=src_key,
        root_dir=root_dir,
        project_dir=root_dir / "project",
        skill_dir=(root_dir / "project" / ".codex" / "skills"),
        state_dir=root_dir / "state",
        workfile_dir=None,
        roomfile_dir=roomfile_dir,
        lock_file=runtime_root / "locks" / f"{workspace_id}.lock",
        metadata_file=root_dir / "workspace.json",
    )


def ensure_workspace_dirs(workspace: WorkspaceRef) -> WorkspaceRef:
    for path in (
        workspace.root_dir,
        workspace.project_dir,
        workspace.state_dir,
        workspace.lock_file.parent,
        *([workspace.workfile_dir] if workspace.workfile_dir is not None else []),
        *([workspace.roomfile_dir] if workspace.roomfile_dir is not None else []),
    ):
        path.mkdir(parents=True, exist_ok=True)
    if workspace.scope == "user":
        workfile_dir = workspace.root_dir / "workfile"
        workfile_dir.mkdir(parents=True, exist_ok=True)
        room_id = workspace.owner_room_id
        if room_id:
            roomfile_dir = workspace.lock_file.parent.parent / "workspaces" / "rooms" / slugify(room_id) / workspace.source_key / "roomfile"
            roomfile_dir.mkdir(parents=True, exist_ok=True)
    workspace.skill_dir.mkdir(parents=True, exist_ok=True)
    return workspace
