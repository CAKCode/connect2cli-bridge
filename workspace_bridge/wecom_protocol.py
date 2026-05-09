from __future__ import annotations

import itertools
import time
from dataclasses import replace

from .models import BotConfig, WeComTextMessage

_UID_COUNTER = itertools.count()


def uid() -> str:
    return f"{int(time.time() * 1000):x}-{next(_UID_COUNTER):x}"


def chat_key_from_message(payload: dict) -> str:
    body = payload.get("body") or {}
    if body.get("chattype") == "group" and body.get("chatid"):
        return f"group-user:{body['chatid']}:{((body.get('from') or {}).get('userid') or 'unknown')}"
    return f"single:{((body.get('from') or {}).get('userid') or 'unknown')}"


def build_subscribe_payload(bot: BotConfig, *, req_id: str | None = None) -> dict:
    if not bot.bot_secret:
        raise ValueError("bot secret is required for subscribe payload")
    return {
        "cmd": "aibot_subscribe",
        "headers": {"req_id": req_id or uid()},
        "body": {"bot_id": bot.bot_id, "secret": bot.bot_secret},
    }


def build_text_response_payload(req_id: str, session_id: str, content: str, *, final: bool) -> dict:
    return {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": req_id},
        "body": {"msgtype": "stream", "stream": {"id": session_id, "finish": final, "content": content}},
    }


def parse_text_callback(payload: dict) -> WeComTextMessage | None:
    if payload.get("cmd") != "aibot_msg_callback":
        return None
    body = payload.get("body") or {}
    if body.get("msgtype") != "text":
        return None
    return WeComTextMessage(
        req_id=str((payload.get("headers") or {}).get("req_id") or ""),
        chat_key=chat_key_from_message(payload),
        content=str(((body.get("text") or {}).get("content")) or "").strip(),
        raw_payload=payload,
    )


def is_subscribe_ok(payload: dict) -> bool:
    return payload.get("errcode") == 0
