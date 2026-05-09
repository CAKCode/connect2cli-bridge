from __future__ import annotations

import json
import shutil
import subprocess
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


def detect_source_revision(source_dir: Path) -> str | None:
    if detect_source_mode(source_dir) != "git":
        return None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=source_dir,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return None
    return result.stdout.strip() or None if result.returncode == 0 else None


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
    if any(workspace.project_dir.iterdir()):
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
