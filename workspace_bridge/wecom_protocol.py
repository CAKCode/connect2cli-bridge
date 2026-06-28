from __future__ import annotations

import itertools
import os
import re
import time

from .models import (
    BotConfig,
    OutboundMessage,
    TemplateCardUpdateRequest,
    WeComTemplateCardEvent,
    WeComTemplateCardSelection,
    WeComTextMessage,
)
from .template_card_validation import enrich_template_card_for_delivery

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
    return build_proactive_message_payload(
        OutboundMessage(
            chat_key=chat_key,
            msgtype="markdown",
            content=content,
            mention_user_id=mention_user_id,
        )
    )


def _resolve_message_body(message: OutboundMessage) -> dict:
    return _resolve_message_body_with_options(message, truncate_markdown=True)


def _resolve_message_body_with_options(message: OutboundMessage, *, truncate_markdown: bool) -> dict:
    msgtype = str(message.msgtype or "").strip()
    if msgtype == "markdown":
        resolved_mention_user_id = str(message.mention_user_id or "").strip()
        if not resolved_mention_user_id and message.chat_key.startswith("group-user:"):
            resolved_mention_user_id = str(chat_key_to_user_id(message.chat_key) or "").strip()
        markdown_content = prepend_group_user_mention(str(message.content or ""), resolved_mention_user_id)
        if truncate_markdown:
            markdown_content = limit_proactive_text(markdown_content)
        markdown = {
            "content": markdown_content
        }
        if message.feedback_id:
            markdown["feedback"] = {"id": str(message.feedback_id)}
        return {
            "msgtype": "markdown",
            "markdown": markdown,
        }
    if msgtype == "template_card":
        card = enrich_template_card_for_delivery(
            message.chat_key,
            dict(message.template_card or {}),
            unique_token=uid(),
        )
        if message.feedback_id:
            card_feedback = dict(card.get("feedback") or {})
            card_feedback["id"] = str(message.feedback_id)
            card["feedback"] = card_feedback
        return {"msgtype": "template_card", "template_card": card}
    if msgtype in {"file", "image", "voice"}:
        if not message.media_id:
            raise ValueError(f"{msgtype} message requires media_id")
        return {"msgtype": msgtype, msgtype: {"media_id": message.media_id}}
    if msgtype == "video":
        video = dict(message.template_card or {})
        if message.media_id:
            video["media_id"] = message.media_id
        if not video.get("media_id"):
            raise ValueError("video message requires media_id")
        return {"msgtype": "video", "video": video}
    raise ValueError(f"unsupported proactive message type: {msgtype}")


def build_proactive_message_payload(message: OutboundMessage) -> dict:
    return build_proactive_message_payload_with_options(message, truncate_markdown=True)


def build_proactive_message_payload_with_options(message: OutboundMessage, *, truncate_markdown: bool) -> dict:
    chat_type, chat_id = chat_key_to_send_target(message.chat_key)
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {
            "chatid": chat_id,
            "chat_type": chat_type,
            **_resolve_message_body_with_options(message, truncate_markdown=truncate_markdown),
        },
    }


def build_proactive_text_payloads(chat_key: str, content: str, mention_user_id: str | None = None) -> list[dict]:
    return build_proactive_message_payloads(
        OutboundMessage(
            chat_key=chat_key,
            msgtype="markdown",
            content=content,
            mention_user_id=mention_user_id,
        )
    )


def build_proactive_message_payloads(message: OutboundMessage) -> list[dict]:
    if str(message.msgtype or "").strip() != "markdown":
        return [build_proactive_message_payload(message)]
    content = str(message.content or "")
    mention_user_id = message.mention_user_id
    chunks = split_text_chunks(content, max_chars=PROACTIVE_TEXT_MAX_CHARS)
    payloads: list[dict] = []
    for chunk in chunks:
        payloads.append(
            build_proactive_message_payload_with_options(
                OutboundMessage(
                    chat_key=message.chat_key,
                    msgtype="markdown",
                    content=chunk,
                    mention_user_id=mention_user_id,
                    feedback_id=message.feedback_id,
                ),
                truncate_markdown=False,
            )
        )
    return payloads


def resolve_template_card_for_delivery(message: OutboundMessage) -> dict | None:
    if str(message.msgtype or "").strip() != "template_card":
        return None
    body = _resolve_message_body(message)
    return dict(body.get("template_card") or {})


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


def normalize_bridge_command_text(content: str, bot_name: str | None = None) -> str:
    text = strip_text_mentions(content, bot_name)
    if text.startswith("/bridge-"):
        return text
    fallback = LEADING_MENTION_RE.sub("", str(content or ""), count=1).strip()
    return fallback if fallback.startswith("/bridge-") else str(content or "").strip()


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


def build_template_card_update_payload(request: TemplateCardUpdateRequest) -> dict:
    return {
        "cmd": "aibot_respond_update_msg",
        "headers": {"req_id": request.req_id},
        "body": {
            "response_type": "update_template_card",
            "template_card": dict(request.template_card or {}),
        },
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


def extract_response_url(payload: dict) -> str | None:
    body = payload.get("body") or {}
    headers = payload.get("headers") or {}
    for candidate in (
        body.get("response_url"),
        body.get("responseUrl"),
        headers.get("response_url"),
        headers.get("responseUrl"),
        payload.get("response_url"),
        payload.get("responseUrl"),
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return None


def parse_template_card_event(payload: dict) -> WeComTemplateCardEvent | None:
    if payload.get("cmd") != "aibot_event_callback":
        return None
    body = payload.get("body") or {}
    if body.get("msgtype") != "event":
        return None
    event = body.get("event") or {}
    if event.get("eventtype") != "template_card_event":
        return None
    card_event = event.get("template_card_event") or {}
    selected_items = []
    for item in ((card_event.get("selected_items") or {}).get("selected_item") or []):
        option_ids = tuple(str(option_id) for option_id in (((item.get("option_ids") or {}).get("option_id")) or []) if str(option_id))
        selected_items.append(
            WeComTemplateCardSelection(
                question_key=str(item.get("question_key") or ""),
                option_ids=option_ids,
            )
        )
    return WeComTemplateCardEvent(
        req_id=str((payload.get("headers") or {}).get("req_id") or ""),
        chat_key=chat_key_from_message(payload),
        card_type=str(card_event.get("card_type") or ""),
        event_key=str(card_event.get("event_key") or ""),
        task_id=str(card_event.get("task_id") or "") or None,
        selected_items=tuple(selected_items),
        raw_payload=payload,
    )


def is_subscribe_ok(payload: dict) -> bool:
    return payload.get("errcode") == 0
