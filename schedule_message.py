#!/usr/bin/env python3

import json
import os
import sys

import bridge


EXPECTED_ARG_KEYS = {
    "session-id",
    "session_id",
    "chat-key",
    "chat_key",
    "reply-req-id",
    "reply_req_id",
    "bot-name",
    "bot_name",
    "bot-config-id",
    "bot_config_id",
    "message",
    "msgtype",
    "template-card-file",
    "template_card_file",
    "run-at",
    "run_at",
    "delay-seconds",
    "delay_seconds",
    "cron",
    "timezone",
    "start-at",
    "start_at",
    "end-at",
    "end_at",
    "max-runs",
    "max_runs",
    "misfire-policy",
    "misfire_policy",
    "concurrency-policy",
    "concurrency_policy",
}
DEFAULT_BOT_NAME = str(os.environ.get("WECOM_BRIDGE_BOT_NAME") or "").strip()
DEFAULT_BOT_CONFIG_ID = str(os.environ.get("WECOM_BRIDGE_BOT_CONFIG_ID") or "").strip()
DASH_PREFIX_VALUE_KEYS = {
    "message",
}


def fail(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


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


def write_one_shot_schedule(data: dict[str, str]) -> dict[str, object]:
    return bridge.submit_schedule_message_request(data, "local-command")


def write_recurring_schedule(data: dict[str, str]) -> dict[str, object]:
    return bridge.submit_schedule_definition_request(data, "local-command")


def main() -> None:
    args = parse_args(sys.argv[1:])
    session_id = args.get("session-id") or args.get("session_id") or ""
    chat_key = args.get("chat-key") or args.get("chat_key") or ""
    reply_req_id = args.get("reply-req-id") or args.get("reply_req_id") or ""
    bot_name = args.get("bot-name") or args.get("bot_name") or DEFAULT_BOT_NAME
    bot_config_id = args.get("bot-config-id") or args.get("bot_config_id") or DEFAULT_BOT_CONFIG_ID
    msgtype = str(args.get("msgtype") or "markdown").strip() or "markdown"
    message = args.get("message") or ""

    if not session_id and not chat_key and not reply_req_id:
        fail("session-id or chat-key or reply-req-id required", 2)
    if msgtype == "markdown" and not message.strip():
        fail("message required", 2)
    if msgtype == "template_card":
        card_path = str(args.get("template-card-file") or args.get("template_card_file") or "").strip()
        if not card_path:
            fail("template-card-file required", 2)
        try:
            template_card = json.loads(__import__("pathlib").Path(card_path).expanduser().resolve().read_text(encoding="utf-8"))
        except Exception as exc:
            fail(f"invalid template-card-file: {exc}", 2)
        try:
            template_card = bridge.validate_template_card_payload(template_card, require_interaction_task_id=False)
        except Exception as exc:
            fail(str(exc), 2)
    elif msgtype != "markdown":
        fail(f"unsupported msgtype: {msgtype}", 2)

    run_at = args.get("run-at") or args.get("run_at") or ""
    delay_seconds = args.get("delay-seconds") or args.get("delay_seconds") or ""
    cron_expr = args.get("cron") or ""

    if run_at and delay_seconds:
        fail("choose exactly one of --run-at or --delay-seconds for one-shot scheduling", 2)

    selector_count = sum(
        1
        for present in (
            bool(run_at or delay_seconds),
            bool(cron_expr),
        )
        if present
    )
    if selector_count != 1:
        fail("choose exactly one of one-shot (--run-at/--delay-seconds) or --cron", 2)

    data = {
        "sessionId": session_id or None,
        "chatKey": chat_key or None,
        "replyReqId": reply_req_id or None,
        "botName": bot_name or None,
        "targetConfigId": bot_config_id or None,
        "message": message.strip() if msgtype == "markdown" else "[template_card scheduled follow-up]",
        "msgtype": msgtype,
    }
    if msgtype == "markdown":
        data["content"] = message.strip()
    else:
        data["templateCard"] = template_card

    try:
        if run_at or delay_seconds:
            data.update({"runAt": run_at or None, "delaySeconds": delay_seconds or None})
            result = write_one_shot_schedule(data)
        elif cron_expr:
            data.update(
                {
                    "mode": "cron",
                    "cron": cron_expr,
                    "timezone": args.get("timezone") or "UTC",
                    "startAt": args.get("start-at") or args.get("start_at") or None,
                    "endAt": args.get("end-at") or args.get("end_at") or None,
                    "maxRuns": args.get("max-runs") or args.get("max_runs") or None,
                    "misfirePolicy": args.get("misfire-policy") or args.get("misfire_policy") or None,
                    "concurrencyPolicy": args.get("concurrency-policy") or args.get("concurrency_policy") or None,
                }
            )
            result = write_recurring_schedule(data)
    except bridge.BridgeError as exc:
        fail(exc.message, 4 if 400 <= exc.status_code < 500 else 1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
