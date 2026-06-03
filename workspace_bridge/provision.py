from __future__ import annotations

import json
import shutil
import time
from pathlib import Path

from .layout import ensure_workspace_dirs
from .models import ProvisionedWorkspace, WorkspaceRef


def now_ms() -> int:
    return int(time.time() * 1000)


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_json_file(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def detect_source_mode(source_dir: Path) -> str:
    return "git" if (source_dir / ".git").exists() else "copy"


def resolve_git_dir(source_dir: Path) -> Path | None:
    git_marker = source_dir / ".git"
    if git_marker.is_dir():
        return git_marker
    if not git_marker.is_file():
        return None
    try:
        text = git_marker.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    prefix = "gitdir:"
    if not text.lower().startswith(prefix):
        return None
    raw_path = text[len(prefix) :].strip()
    if not raw_path:
        return None
    git_dir = Path(raw_path)
    if not git_dir.is_absolute():
        git_dir = (source_dir / git_dir).resolve()
    return git_dir if git_dir.exists() else None


def read_packed_ref(git_dir: Path, ref_name: str) -> str | None:
    packed_refs = git_dir / "packed-refs"
    if not packed_refs.is_file():
        return None
    try:
        for raw_line in packed_refs.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith(("#", "^")):
                continue
            value, _, name = line.partition(" ")
            if name == ref_name:
                return value.strip() or None
    except Exception:
        return None
    return None


def detect_source_revision(source_dir: Path) -> str | None:
    if detect_source_mode(source_dir) != "git":
        return None
    git_dir = resolve_git_dir(source_dir)
    if git_dir is None:
        return None
    head_file = git_dir / "HEAD"
    try:
        head_text = head_file.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    if head_text.startswith("ref:"):
        ref_name = head_text.split(":", 1)[1].strip()
        if not ref_name:
            return None
        ref_path = git_dir / ref_name
        if ref_path.is_file():
            try:
                return ref_path.read_text(encoding="utf-8").strip() or None
            except Exception:
                return None
        return read_packed_ref(git_dir, ref_name)
    return head_text or None


def load_workspace_metadata(workspace: WorkspaceRef) -> dict | None:
    return read_json_file(workspace.metadata_file)


def should_skip_source_child(workspace: WorkspaceRef, child: Path) -> bool:
    if child.name in {".git", "__pycache__", ".pytest_cache", ".workspace-bridge-runtime"}:
        return True
    try:
        child.resolve().relative_to(workspace.root_dir.resolve())
        return True
    except Exception:
        pass
    return False


def bootstrap_project_dir(workspace: WorkspaceRef) -> None:
    non_codex_entries = [child for child in workspace.project_dir.iterdir() if child.name != ".codex"]
    if non_codex_entries:
        return
    for child in workspace.source_dir.iterdir():
        if should_skip_source_child(workspace, child):
            continue
        target = workspace.project_dir / child.name
        if child.is_dir():
            shutil.copytree(child, target)
        else:
            shutil.copy2(child, target)


def provision_workspace(workspace: WorkspaceRef) -> ProvisionedWorkspace:
    ensure_workspace_dirs(workspace)
    metadata = load_workspace_metadata(workspace)
    bootstrap_project_dir(workspace)
    source_mode = detect_source_mode(workspace.source_dir)
    source_revision = detect_source_revision(workspace.source_dir)
    initialized_at = int(metadata.get("initializedAt")) if metadata and metadata.get("initializedAt") else now_ms()
    updated_at = now_ms()
    payload = {
        "workspaceId": workspace.workspace_id,
        "scope": workspace.scope,
        "ownerUserId": workspace.owner_user_id,
        "ownerRoomId": workspace.owner_room_id,
        "chatKey": workspace.chat_key,
        "sourceDir": str(workspace.source_dir),
        "sourceKey": workspace.source_key,
        "rootDir": str(workspace.root_dir),
        "projectDir": str(workspace.project_dir),
        "skillDir": str(workspace.skill_dir),
        "stateDir": str(workspace.state_dir),
        "projectReady": True,
        "sourceMode": source_mode,
        "sourceRevision": source_revision,
        "initializedAt": initialized_at,
        "updatedAt": updated_at,
    }
    write_json_atomic(workspace.metadata_file, payload)
    return ProvisionedWorkspace(
        workspace=workspace,
        source_mode=source_mode,
        source_revision=source_revision,
        initialized_at=initialized_at,
        updated_at=updated_at,
        project_ready=True,
    )
