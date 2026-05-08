#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from workspace_bridge.config import build_bot_from_app_config, load_app_config
from workspace_bridge.file_send import create_file_send_request
from workspace_bridge.runtime import prepare_session_run


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a workspace file for send-back.")
    parser.add_argument("--chat-key", required=True)
    parser.add_argument("--file-path", required=True)
    parser.add_argument("--env-file", default=".env")
    args = parser.parse_args()

    config = load_app_config(env_file=Path(args.env_file).expanduser().resolve())
    bot = build_bot_from_app_config(config)
    launch = prepare_session_run(bot, args.chat_key)
    file_request = create_file_send_request(
        launch.runtime_context,
        session_id=launch.session.session_id,
        chat_key=args.chat_key,
        file_path=args.file_path,
    )
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
