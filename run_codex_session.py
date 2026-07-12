#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from workspace_bridge.prompting import build_prompt
from workspace_bridge.runner import build_runner_invocation, run_invocation
from workspace_bridge.runtime import build_bot_config, prepare_session_run


def fail(message: str, code: int = 1) -> None:
    print(message, file=__import__("sys").stderr)
    raise SystemExit(code)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a workspace-backed Codex session.")
    parser.add_argument("--bot-id", required=True)
    parser.add_argument("--bot-name", required=True)
    parser.add_argument("--runtime-root", required=True)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--chatfile-root", required=True)
    parser.add_argument("--chat-key", required=True)
    parser.add_argument("--message", required=True)
    parser.add_argument("--output-file")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    try:
        source_dir = Path(args.source_dir).expanduser().resolve()
        if not source_dir.exists():
            fail(f"source-dir does not exist: {source_dir}", 4)
        if not source_dir.is_dir():
            fail(f"source-dir is not a directory: {source_dir}", 4)
        bot = build_bot_config(
            bot_id=args.bot_id,
            bot_name=args.bot_name,
            source_dir=source_dir,
            runtime_root=args.runtime_root,
            chatfile_root=args.chatfile_root,
        )
        launch = prepare_session_run(bot, args.chat_key)
        prompt = build_prompt(bot, launch, args.message)
    except ValueError as exc:
        fail(str(exc), 4)
    except Exception as exc:
        fail(str(exc) or "unexpected error", 1)

    if args.dry_run:
        payload = {
            "cwd": str(launch.cwd),
            "sessionId": launch.session.session_id,
            "workspaceId": launch.session.workspace_id,
            "cwdDir": str(launch.runtime_context.cwd_dir),
            "chatfileDir": str(launch.runtime_context.chatfile_dir),
            "effectiveSkills": list(launch.runtime_context.effective_skill_names),
            "prompt": prompt,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if not str(args.output_file or "").strip():
        fail("--output-file is required unless --dry-run is used", 4)
    try:
        invocation = build_runner_invocation(launch, prompt=prompt, output_file=Path(args.output_file).expanduser().resolve())
        result = run_invocation(invocation)
    except Exception as exc:
        fail(str(exc) or "unexpected error", 1)
    print(
        json.dumps(
            {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
