#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from workspace_bridge.layout import build_workspace_ref, ensure_workspace_dirs
from workspace_bridge.models import DEFAULT_GLOBAL_SKILL_DIR
from workspace_bridge.skills import resolve_skill_space


def fail(message: str, code: int = 1) -> None:
    print(message, file=__import__("sys").stderr)
    raise SystemExit(code)


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect workspace and skill resolution.")
    parser.add_argument("--runtime-root", required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--chat-key", required=True)
    parser.add_argument("--workspace-namespace", default="default")
    parser.add_argument("--workspace-mode", choices=("team", "personal"))
    parser.add_argument("--agent-backend", choices=("codex", "claude"), default="codex")
    parser.add_argument("--ensure-dirs", action="store_true")
    args = parser.parse_args()

    try:
        source_dir = Path(args.source_dir).expanduser().resolve()
        if not source_dir.exists():
            fail(f"source-dir does not exist: {source_dir}", 4)
        if not source_dir.is_dir():
            fail(f"source-dir is not a directory: {source_dir}", 4)
        workspace = build_workspace_ref(
            args.runtime_root,
            args.workspace_namespace,
            source_dir,
            args.chat_key,
            workspace_mode=args.workspace_mode,
            agent_backend=args.agent_backend,
        )
        if args.ensure_dirs:
            ensure_workspace_dirs(workspace)
        skill_space = resolve_skill_space(DEFAULT_GLOBAL_SKILL_DIR, workspace.skill_dir)
    except ValueError as exc:
        fail(str(exc), 4)
    except Exception as exc:
        fail(str(exc) or "unexpected error", 1)

    payload = {
        "workspaceId": workspace.workspace_id,
        "scope": workspace.scope,
        "ownerUserId": workspace.owner_user_id,
        "ownerRoomId": workspace.owner_room_id,
        "sourceDir": str(workspace.source_dir),
        "rootDir": str(workspace.root_dir),
        "projectDir": str(workspace.project_dir),
        "cwdDir": str(workspace.cwd_dir),
        "skillDir": str(workspace.skill_dir),
        "stateDir": str(workspace.state_dir),
        "effectiveSkills": {
            name: {"layer": skill.layer, "skillFile": str(skill.skill_file)}
            for name, skill in skill_space.effective_skills.items()
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
