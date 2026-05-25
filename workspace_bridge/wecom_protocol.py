from __future__ import annotations

import itertools
import os
import re
import time
from dataclasses import replace

from .models import BotConfig, WeComTextMessage

_UID_COUNTER = itertools.count()
TEXT_MENTION_RE = re.compile(r"(?<!\S)@\S+(?:\s+|$)")
LEADING_MENTION_RE = re.compile(r"^\s*@\S+(?:\s+|$)")
MENTION_DELIMITER_CHARS = ",:;，。：；"
PROACTIVE_TEXT_MAX_CHARS = max(256, int(os.environ.get("PROACTIVE_TEXT_MAX_CHARS", "1800")))
STREAM_TEXT_MAX_CHARS = max(256, int(os.environ.get("STREAM_TEXT_MAX_CHARS", "3500")))


def uid() -> str:
    return f"{int(time.time() * 1000):x}-{next(_UID_COUNTER):x}"


def chat_key_from_message(payload: dict) -> str:
    body = payload.get("body") or {}
    if body.get("chattype") == "group" and body.get("chatid"):
        return f"group-user:{body['chatid']}:{((body.get('from') or {}).get('userid') or 'unknown')}"
    return f"single:{((body.get('from') or {}).get('userid') or 'unknown')}"


def chat_key_to_user_id(chat_key: str) -> str | None:
    text = str(chat_key or "").strip()
    if text.startswith("single:"):
        return text.split(":", 1)[1] or None
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        return parts[2] if len(parts) == 3 and parts[2] else None
    return None


def chat_key_to_send_target(chat_key: str) -> tuple[int, str]:
    if chat_key.startswith("group-user:"):
        parts = chat_key.split(":", 2)
        return 2, parts[1]
    chat_type_name, chat_id = chat_key.split(":", 1)
    return (2 if chat_type_name == "group" else 1), chat_id


def format_group_user_mention(user_id: str | None) -> str:
    text = str(user_id or "").strip()
    if not text:
        return ""
    return f"<@{text}>"


def prepend_group_user_mention(content: str, user_id: str | None) -> str:
    mention = format_group_user_mention(user_id)
    text = str(content or "").strip()
    if not mention:
        return text
    if text.startswith(mention):
        return text
    if not text:
        return mention
    return f"{mention}\n{text}"


def limit_proactive_text(content: str) -> str:
    text = str(content or "").strip()
    if len(text) <= PROACTIVE_TEXT_MAX_CHARS:
        return text
    suffix = "...(truncated)"
    return text[: max(0, PROACTIVE_TEXT_MAX_CHARS - len(suffix))].rstrip() + suffix


def split_text_chunks(content: str, *, max_chars: int) -> list[str]:
    text = str(content or "").strip()
    if not text:
        return [""]
    chunks: list[str] = []
    remaining = text
    limit = max(1, int(max_chars))
    while len(remaining) > limit:
        split_at = remaining.rfind("\n", 0, limit + 1)
        if split_at <= 0:
            split_at = remaining.rfind(" ", 0, limit + 1)
        if split_at <= 0:
            split_at = limit
        chunk = remaining[:split_at].rstrip()
        if not chunk:
            chunk = remaining[:limit]
            split_at = len(chunk)
        chunks.append(chunk)
        remaining = remaining[split_at:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def build_proactive_text_payload(chat_key: str, content: str, mention_user_id: str | None = None) -> dict:
    chat_type, chat_id = chat_key_to_send_target(chat_key)
    resolved_mention_user_id = str(mention_user_id or "").strip()
    if not resolved_mention_user_id and chat_key.startswith("group-user:"):
        resolved_mention_user_id = str(chat_key_to_user_id(chat_key) or "").strip()
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {
            "chatid": chat_id,
            "chat_type": chat_type,
            "msgtype": "markdown",
            "markdown": {
                "content": limit_proactive_text(prepend_group_user_mention(content, resolved_mention_user_id))
            },
        },
    }


def build_proactive_text_payloads(chat_key: str, content: str, mention_user_id: str | None = None) -> list[dict]:
    chunks = split_text_chunks(content, max_chars=PROACTIVE_TEXT_MAX_CHARS)
    payloads: list[dict] = []
    for chunk in chunks:
        chat_type, chat_id = chat_key_to_send_target(chat_key)
        resolved_mention_user_id = str(mention_user_id or "").strip()
        if not resolved_mention_user_id and chat_key.startswith("group-user:"):
            resolved_mention_user_id = str(chat_key_to_user_id(chat_key) or "").strip()
        payloads.append(
            {
                "cmd": "aibot_send_msg",
                "headers": {"req_id": uid()},
                "body": {
                    "chatid": chat_id,
                    "chat_type": chat_type,
                    "msgtype": "markdown",
                    "markdown": {"content": prepend_group_user_mention(chunk, resolved_mention_user_id)},
                },
            }
        )
    return payloads


def strip_text_mentions(content: str, bot_name: str | None = None) -> str:
    text = str(content or "")
    normalized_bot_name = str(bot_name or "").strip()
    if not normalized_bot_name:
        return LEADING_MENTION_RE.sub("", text, count=1).strip()
    cursor = text.lstrip()
    bot_pattern = re.compile(rf"@{re.escape(normalized_bot_name)}(?P<suffix>\s+|[{re.escape(MENTION_DELIMITER_CHARS)}]|$)")
    leading_mentions_pattern = re.compile(r"^(?:@[^@\n]+?\s+)*$")
    for bot_match in bot_pattern.finditer(cursor):
        start = bot_match.start()
        if start > 0 and not cursor[start - 1].isspace():
            continue
        prefix = cursor[:start]
        if prefix and not leading_mentions_pattern.fullmatch(prefix):
            continue
        return cursor[bot_match.end() :].lstrip().strip()
    return text.strip()


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


def build_text_response_payloads(req_id: str, session_id: str, content: str, *, final: bool) -> list[dict]:
    chunks = split_text_chunks(content, max_chars=STREAM_TEXT_MAX_CHARS)
    payloads: list[dict] = []
    for index, chunk in enumerate(chunks):
        payloads.append(
            build_text_response_payload(
                req_id,
                session_id,
                chunk,
                final=final and index == len(chunks) - 1,
            )
        )
    return payloads


def parse_text_callback(payload: dict) -> WeComTextMessage | None:
    if payload.get("cmd") != "aibot_msg_callback":
        return None
    body = payload.get("body") or {}
    if body.get("msgtype") != "text":
        return None
    return WeComTextMessage(
        req_id=str((payload.get("headers") or {}).get("req_id") or ""),
        chat_key=chat_key_from_message(payload),
        content=str(((body.get("text") or {}).get("content")) or ""),
        raw_payload=payload,
    )


def is_subscribe_ok(payload: dict) -> bool:
    return payload.get("errcode") == 0
