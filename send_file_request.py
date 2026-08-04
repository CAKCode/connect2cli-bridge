#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from os import environ
from pathlib import Path

from workspace_bridge.config import merge_env, require_env
from workspace_bridge.file_send import create_file_send_request
from workspace_bridge.runtime import build_bot_config, prepare_session_run


def load_file_send_request_bot(env_file: Path):
    values = merge_env(environ, env_file=env_file)
    runtime_root = Path(values.get("RUNTIME_ROOT") or ".workspace-bridge-runtime").expanduser().resolve()
    source_dir = Path(require_env(values, "WECOM_BOT_SOURCE_DIR")).expanduser().resolve()
    if not source_dir.exists():
        raise ValueError(f"WECOM_BOT_SOURCE_DIR does not exist: {source_dir}")
    if not source_dir.is_dir():
        raise ValueError(f"WECOM_BOT_SOURCE_DIR is not a directory: {source_dir}")
    chatfile_root = Path(values.get("CHATFILE_ROOT") or (runtime_root / "chatfiles")).expanduser().resolve()
    file_send_roots = tuple(
        Path(item.strip()).expanduser().resolve()
        for item in str(values.get("FILE_SEND_ROOTS") or "").split(",")
        if item.strip()
    )
    max_upload_size = max(1, int(str(values.get("MAX_UPLOAD_SIZE") or str(100 * 1024 * 1024)).strip()))
    return build_bot_config(
        bot_id=require_env(values, "WECOM_BOT_ID"),
        bot_name=str(values.get("WECOM_BOT_NAME") or "default").strip() or "default",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace=(
            str(
                values.get("WORKSPACE_NAMESPACE")
                or values.get("WECOM_BOT_WORKSPACE_NAMESPACE")
                or values.get("WECOM_WORKSPACE_NAMESPACE")
                or require_env(values, "WECOM_BOT_ID")
            ).strip()
        ),
        chatfile_root=chatfile_root,
        workspace_mode=(str(values.get("WECOM_BOT_WORKSPACE_MODE") or values.get("WECOM_WORKSPACE_MODE") or "").strip().lower() or None),
        codex_exec_mode=(str(values.get("CODEX_EXEC_MODE") or "host").strip().lower() or "host"),
        agent_backend=(str(values.get("WECOM_AGENT_BACKEND") or values.get("AGENT_BACKEND") or "codex").strip() or "codex"),
        agent_command=(str(values.get("WECOM_AGENT_COMMAND") or values.get("AGENT_COMMAND") or "").strip() or None),
        agent_run_as_user=None,
        agent_run_as_group=None,
        agent_runtime_root=None,
        file_send_roots=file_send_roots,
        max_upload_size=max_upload_size,
    )


def fail(message: str, code: int = 1) -> None:
    print(message, file=__import__("sys").stderr)
    raise SystemExit(code)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a workspace file for send-back.")
    parser.add_argument("--chat-key", required=True)
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--env-file", default=".env")
    args = parser.parse_args()

    try:
        bot = load_file_send_request_bot(Path(args.env_file).expanduser().resolve())
        launch = prepare_session_run(bot, args.chat_key)
        file_request = create_file_send_request(
            launch.runtime_context,
            session_id=launch.session.session_id,
            chat_key=args.chat_key,
            file_path=args.file_path,
        )
    except (FileNotFoundError, PermissionError, ValueError) as exc:
        fail(str(exc), 4)
    except Exception as exc:
        fail(str(exc) or "unexpected error", 1)
    print(
        json.dumps(
            {
                "sessionId": file_request.session_id,
                "workspaceId": file_request.workspace_id,
                "fileName": file_request.file_name,
                "filePath": str(file_request.file_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
