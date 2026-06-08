#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from itertools import count
from pathlib import Path
from urllib.parse import quote

from workspace_bridge.layout import parse_chat_key
from workspace_bridge.template_card_validation import validate_feedback_id, validate_template_card_payload


def resolve_base_queue_root(base_dir: Path | None = None) -> Path:
    root_base_dir = (base_dir or Path(__file__).resolve().parent).resolve()
    raw = str(os.environ.get("LOCAL_SEND_MESSAGE_QUEUE_ROOT") or "").strip()
    if not raw:
        return (root_base_dir / ".local-send-message-queue").resolve()
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (root_base_dir / path).resolve()
    else:
        path = path.resolve()
    return path


BASE_QUEUE_ROOT = resolve_base_queue_root()
DEFAULT_RESULT_TIMEOUT_MS = max(1000, int(os.environ.get("LOCAL_SEND_MESSAGE_RESULT_TIMEOUT_MS", "120000")))
TRANSIENT_RETRY_INTERVAL_SEC = 1.0
DEFAULT_BOT_NAME = str(os.environ.get("WECOM_BRIDGE_BOT_NAME") or "").strip()
DEFAULT_BOT_CONFIG_ID = str(os.environ.get("WECOM_BRIDGE_BOT_CONFIG_ID") or "").strip()
EXPECTED_ARG_KEYS = {
    "session-id",
    "session_id",
    "chat-key",
    "chat_key",
    "bot-name",
    "bot_name",
    "bot-config-id",
    "bot_config_id",
    "timeout-ms",
    "timeout_ms",
    "msgtype",
    "content",
    "mention-user-id",
    "mention_user_id",
    "feedback-id",
    "feedback_id",
    "template-card-file",
    "template_card_file",
}
DASH_PREFIX_VALUE_KEYS = {"content", "template-card-file", "template_card_file"}


def queue_namespace(value: str) -> str:
    return quote(str(value or "").strip(), safe="._-") or "default"


def queue_root_for_target(bot_config_id: str) -> Path:
    target = str(bot_config_id or "").strip()
    if not target:
        return BASE_QUEUE_ROOT
    return BASE_QUEUE_ROOT / "targets" / queue_namespace(target)


def queue_paths_for_target(bot_config_id: str) -> tuple[Path, Path, Path]:
    queue_root = queue_root_for_target(bot_config_id)
    return queue_root, queue_root / "pending", queue_root / "results"


QUEUE_ROOT, PENDING_ROOT, RESULT_ROOT = queue_paths_for_target(DEFAULT_BOT_CONFIG_ID)
REQUEST_COUNTER = count()


def fail(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def is_retryable_bridge_result(result: dict[str, object]) -> bool:
    if result.get("ok"):
        return False
    try:
        status_code = int(result.get("statusCode", 0))
    except (TypeError, ValueError):
        return False
    if status_code != 503:
        return False
    error = str(result.get("error") or "").strip()
    return error == "bot not connected" or error.startswith("bot not running:")


def parse_timeout_ms(raw: str | None) -> int:
    text = str(raw or "").strip()
    if not text:
        return DEFAULT_RESULT_TIMEOUT_MS
    try:
        timeout_ms = int(text)
    except ValueError:
        fail("timeout-ms must be an integer", 2)
    if timeout_ms <= 0:
        fail("timeout-ms must be greater than 0", 2)
    return max(1000, timeout_ms)


def parse_args(argv: list[str]) -> dict[str, str]:
    args: dict[str, str] = {}
    idx = 0
    while idx < len(argv):
        item = argv[idx]
        if not item.startswith("--"):
            idx += 1
            continue
        raw = item[2:]
        if "=" in raw:
            key, value = raw.split("=", 1)
            if key not in EXPECTED_ARG_KEYS:
                fail(f"unknown option: --{key}", 2)
            args[key] = value
            idx += 1
            continue
        key = raw
        if key not in EXPECTED_ARG_KEYS:
            fail(f"unknown option: --{key}", 2)
        if idx + 1 >= len(argv):
            fail(f"missing value for --{key}", 2)
        next_value = argv[idx + 1]
        if next_value.startswith("--"):
            next_key = next_value[2:].split("=", 1)[0]
            if next_key in EXPECTED_ARG_KEYS:
                fail(f"missing value for --{key}", 2)
            if key not in DASH_PREFIX_VALUE_KEYS:
                fail(f"unknown option: {next_value}", 2)
        args[key] = next_value
        idx += 2
    return args


def next_request_id() -> str:
    return f"{int(time.time() * 1000):x}-{os.getpid()}-{next(REQUEST_COUNTER):x}"


def main() -> int:
    args = parse_args(sys.argv[1:])
    session_id = args.get("session-id") or args.get("session_id") or ""
    chat_key = args.get("chat-key") or args.get("chat_key") or ""
    bot_name = args.get("bot-name") or args.get("bot_name") or DEFAULT_BOT_NAME
    bot_config_id = args.get("bot-config-id") or args.get("bot_config_id") or DEFAULT_BOT_CONFIG_ID
    timeout_ms = parse_timeout_ms(args.get("timeout-ms") or args.get("timeout_ms"))
    msgtype = str(args.get("msgtype") or "markdown").strip()
    _queue_root, pending_root, result_root = queue_paths_for_target(bot_config_id)

    if not session_id and not chat_key:
        fail("session-id or chat-key required", 2)
    if chat_key:
        try:
            parse_chat_key(chat_key)
        except ValueError as exc:
            fail(str(exc), 2)

    request: dict[str, object] = {
        "sessionId": session_id or None,
        "chatKey": chat_key or None,
        "botName": bot_name or None,
        "targetConfigId": bot_config_id or None,
        "msgtype": msgtype,
    }

    if msgtype == "markdown":
        content = str(args.get("content") or "").strip()
        if not content:
            fail("content required", 2)
        request["content"] = content
        mention_user_id = str(args.get("mention-user-id") or args.get("mention_user_id") or "").strip()
        feedback_id = str(args.get("feedback-id") or args.get("feedback_id") or "").strip()
        if mention_user_id:
            request["mentionUserId"] = mention_user_id
        if feedback_id:
            try:
                request["feedbackId"] = validate_feedback_id(feedback_id)
            except ValueError as exc:
                fail(str(exc), 2)
    elif msgtype == "template_card":
        card_path = str(args.get("template-card-file") or args.get("template_card_file") or "").strip()
        if not card_path:
            fail("template-card-file required", 2)
        try:
            template_card = json.loads(Path(card_path).expanduser().resolve().read_text(encoding="utf-8"))
        except Exception as exc:
            fail(f"invalid template-card-file: {exc}", 2)
        try:
            template_card = validate_template_card_payload(template_card, require_interaction_task_id=False)
        except ValueError as exc:
            fail(str(exc), 2)
        request["templateCard"] = template_card
        feedback_id = str(args.get("feedback-id") or args.get("feedback_id") or "").strip()
        if feedback_id:
            try:
                request["feedbackId"] = validate_feedback_id(feedback_id)
            except ValueError as exc:
                fail(str(exc), 2)
    else:
        fail(f"unsupported msgtype: {msgtype}", 2)

    ensure_dir(pending_root)
    ensure_dir(result_root)

    deadline = time.time() + (timeout_ms / 1000)
    deadline_ms = int(deadline * 1000)
    request_id = ""
    while time.time() < deadline:
        requested_at = int(time.time() * 1000)
        request_id = next_request_id()
        queue_request = {
            **request,
            "requestId": request_id,
            "requestedAt": requested_at,
            "timeoutMs": max(1000, deadline_ms - requested_at),
            "expiresAt": deadline_ms,
        }

        pending_tmp = pending_root / f"{request_id}.json.tmp"
        pending_file = pending_root / f"{request_id}.json"
        result_file = result_root / f"{request_id}.json"

        pending_tmp.write_text(json.dumps(queue_request, ensure_ascii=False, indent=2), "utf-8")
        pending_tmp.replace(pending_file)

        while time.time() < deadline:
            if result_file.exists():
                try:
                    result = json.loads(result_file.read_text("utf-8"))
                except Exception as exc:
                    fail(f"invalid bridge result: {exc}", 1)
                try:
                    result_file.unlink()
                except FileNotFoundError:
                    pass
                if result.get("ok"):
                    print(json.dumps(result, ensure_ascii=False))
                    return 0
                if is_retryable_bridge_result(result) and time.time() + TRANSIENT_RETRY_INTERVAL_SEC < deadline:
                    time.sleep(TRANSIENT_RETRY_INTERVAL_SEC)
                    break
                fail(
                    result.get("error") or "bridge rejected local message request",
                    4 if 400 <= int(result.get("statusCode", 500)) < 500 else 1,
                )
            time.sleep(0.2)

    fail(f"timeout waiting for bridge result: {request_id}", 3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
