from __future__ import annotations

import json
import time
from pathlib import Path

from .models import BotConfig, WeComTextMessage

WECOM_WS_URL = "wss://openws.work.weixin.qq.com"


def uid() -> str:
    return f"{int(time.time() * 1000):x}"


def payload_req_id(payload: dict) -> str:
    return str(((payload.get("headers") or {}).get("req_id")) or "").strip()


def chat_key_from_message(payload: dict) -> str:
    body = payload.get("body") or {}
    sender = str(((body.get("from") or {}).get("userid")) or "").strip()
    if body.get("chattype") == "group":
        chat_id = str(body.get("chatid") or "").strip()
        if chat_id and sender:
            return f"group-user:{chat_id}:{sender}"
        if chat_id:
            return f"group:{chat_id}"
    if sender:
        return f"single:{sender}"
    raise ValueError("cannot derive chat key from message")


def build_subscribe_payload(bot: BotConfig, *, req_id: str | None = None) -> dict:
    if not bot.bot_secret:
        raise ValueError("bot secret is required for subscribe payload")
    return {
        "cmd": "aibot_subscribe",
        "headers": {"req_id": req_id or uid()},
        "body": {"bot_id": bot.bot_id, "secret": bot.bot_secret},
    }


def build_text_response_payload(req_id: str, session_id: str, content: str, *, final: bool = True) -> dict:
    return {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": req_id},
        "body": {"msgtype": "stream", "stream": {"id": session_id, "finish": final, "content": content}},
    }


def chat_key_to_send_target(chat_key: str) -> tuple[int, str]:
    if chat_key.startswith("group-user:"):
        parts = chat_key.split(":", 2)
        return 2, parts[1]
    prefix, value = chat_key.split(":", 1)
    return (2 if prefix == "group" else 1), value


def build_proactive_text_payload(chat_key: str, content: str) -> dict:
    chat_type, chat_id = chat_key_to_send_target(chat_key)
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {
            "chatid": chat_id,
            "chat_type": chat_type,
            "msgtype": "markdown",
            "markdown": {"content": content},
        },
    }


def parse_text_callback(payload: dict) -> WeComTextMessage | None:
    cmd = str(payload.get("cmd") or "").strip()
    body = payload.get("body") or {}
    msg_type = str(body.get("msgtype") or "").strip()
    if cmd != "aibot_msg_callback" or msg_type != "text":
        return None
    content = str(((body.get("text") or {}).get("content")) or "").strip()
    if not content:
        return None
    return WeComTextMessage(
        req_id=payload_req_id(payload),
        chat_key=chat_key_from_message(payload),
        content=content,
        raw_payload=payload,
    )


def payload_msg_type(payload: dict) -> str:
    return str(((payload.get("body") or {}).get("msgtype")) or "").strip()


def is_subscribe_ok(payload: dict) -> bool:
    return int(payload.get("errcode") or 0) == 0


def encode_ws_payload(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False)
