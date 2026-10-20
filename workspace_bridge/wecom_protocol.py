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
from .template_card_validation import (
    enrich_template_card_for_delivery,
    require_non_empty_string,
    validate_feedback_id,
    validate_template_card_update_payload,
)
from .wecom_text import (
    WECOM_CONTENT_MAX_BYTES,
    split_markdown_chunks,
    split_text_chunks as split_text_chunks,
    truncate_markdown,
)
from .wecom_context import normalize_assistant_reply, message_text

_UID_COUNTER = itertools.count()
TEXT_MENTION_RE = re.compile(r"(?<!\S)@\S+(?:\s+|$)")
LEADING_MENTION_RE = re.compile(r"^\s*@\S+(?:\s+|$)")
MENTION_DELIMITER_CHARS = ",:;，。：；"
PROACTIVE_TEXT_MAX_CHARS = max(256, int(os.environ.get("PROACTIVE_TEXT_MAX_CHARS", "1800")))
STREAM_TEXT_MAX_CHARS = max(256, int(os.environ.get("STREAM_TEXT_MAX_CHARS", "3500")))


def _normalize_optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null"}:
        return None
    return text


def uid() -> str:
    return f"{int(time.time() * 1000):x}-{next(_UID_COUNTER):x}"


def _as_dict(value) -> dict:
    return value if isinstance(value, dict) else {}


def chat_key_from_message(payload: dict) -> str:
    body = _as_dict(payload.get("body"))
    user_id = str(_as_dict(body.get("from")).get("userid") or "").strip()
    if not user_id:
        raise ValueError("message sender userid required")
    if body.get("chattype") == "group":
        chat_id = str(body.get("chatid") or "").strip()
        if not chat_id:
            raise ValueError("group message chatid required")
        return f"group-user:{chat_id}:{user_id}"
    return f"single:{user_id}"


def chat_key_to_user_id(chat_key: str) -> str | None:
    text = str(chat_key or "").strip()
    if text.startswith("single:"):
        return text.split(":", 1)[1] or None
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        return parts[2] if len(parts) == 3 and parts[2] else None
    return None


def chat_key_to_send_target(chat_key: str) -> tuple[int, str]:
    text = str(chat_key or "").strip()
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        if len(parts) == 3 and parts[1] and parts[2]:
            return 2, parts[1]
        raise ValueError(f"invalid chat key: {chat_key}")
    if text.startswith("group:"):
        room_id = text.split(":", 1)[1]
        if room_id:
            return 2, room_id
        raise ValueError(f"invalid chat key: {chat_key}")
    if text.startswith("single:"):
        user_id = text.split(":", 1)[1]
        if user_id:
            return 1, user_id
        raise ValueError(f"invalid chat key: {chat_key}")
    raise ValueError(f"invalid chat key: {chat_key}")


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
    return truncate_markdown(content, max_chars=PROACTIVE_TEXT_MAX_CHARS, suffix="...(truncated)")


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
    body = _resolve_plain_message_body(message, truncate_markdown=truncate_markdown)
    assistant = normalize_assistant_reply(message.to_ai_assistant, proactive=True)
    if assistant:
        if body["msgtype"] not in {"markdown", "file", "image", "video"}:
            raise ValueError("assistant replies do not support this message type")
        body["to_ai_assistant"] = assistant
    return body


def _resolve_plain_message_body(message: OutboundMessage, *, truncate_markdown: bool) -> dict:
    msgtype = str(message.msgtype or "").strip()
    feedback_id = validate_feedback_id(message.feedback_id) if message.feedback_id else None
    if msgtype == "markdown":
        resolved_mention_user_id = _normalize_optional_text(message.mention_user_id) or ""
        if not resolved_mention_user_id and message.chat_key.startswith("group-user:"):
            resolved_mention_user_id = str(chat_key_to_user_id(message.chat_key) or "").strip()
        markdown_content = prepend_group_user_mention(str(message.content or ""), resolved_mention_user_id)
        if truncate_markdown:
            markdown_content = limit_proactive_text(markdown_content)
        markdown = {
            "content": markdown_content
        }
        if feedback_id:
            markdown["feedback"] = {"id": feedback_id}
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
        if feedback_id:
            card_feedback = dict(card.get("feedback") or {})
            card_feedback["id"] = feedback_id
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
    mention_user_id = _normalize_optional_text(message.mention_user_id)
    if not mention_user_id and message.chat_key.startswith("group-user:"):
        mention_user_id = chat_key_to_user_id(message.chat_key)
    mention = format_group_user_mention(mention_user_id)
    mention_size = len((mention + "\n").encode("utf-8")) if mention else 0
    chunks = split_markdown_chunks(
        content,
        max_chars=PROACTIVE_TEXT_MAX_CHARS,
        max_bytes=WECOM_CONTENT_MAX_BYTES - mention_size,
    )
    payloads: list[dict] = []
    for index, chunk in enumerate(chunks):
        assistant = normalize_assistant_reply(message.to_ai_assistant, proactive=True)
        if assistant and index < len(chunks) - 1:
            assistant["reply_status"] = "waiting"
        payloads.append(
            build_proactive_message_payload_with_options(
                OutboundMessage(
                    chat_key=message.chat_key,
                    msgtype="markdown",
                    content=chunk,
                    mention_user_id=mention_user_id,
                    feedback_id=message.feedback_id,
                    to_ai_assistant=assistant,
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
    if text == "/new" or text.startswith(("/bridge-", "/cwd")):
        return text
    fallback = LEADING_MENTION_RE.sub("", str(content or ""), count=1).strip()
    if fallback == "/new" or fallback.startswith(("/bridge-", "/cwd")):
        return fallback
    return str(content or "").strip()


def build_subscribe_payload(bot: BotConfig, *, req_id: str | None = None) -> dict:
    if not bot.bot_secret:
        raise ValueError("bot secret is required for subscribe payload")
    return {
        "cmd": "aibot_subscribe",
        "headers": {"req_id": req_id or uid()},
        "body": {"bot_id": bot.bot_id, "secret": bot.bot_secret},
    }


def build_text_response_payload(req_id: str, session_id: str, content: str, *, final: bool) -> dict:
    req_text = require_non_empty_string(req_id, "reqId")
    session_text = require_non_empty_string(session_id, "sessionId")
    return {
        "cmd": "aibot_respond_msg",
        "headers": {"req_id": req_text},
        "body": {"msgtype": "stream", "stream": {"id": session_text, "finish": final, "content": content}},
    }


def build_template_card_update_payload(request: TemplateCardUpdateRequest) -> dict:
    req_id = require_non_empty_string(request.req_id, "reqId")
    template_card = validate_template_card_update_payload(request.template_card)
    return {
        "cmd": "aibot_respond_update_msg",
        "headers": {"req_id": req_id},
        "body": {
            "response_type": "update_template_card",
            "template_card": dict(template_card),
        },
    }


def stream_text_preview(content: str, *, max_chars: int | None = None) -> str:
    limit = STREAM_TEXT_MAX_CHARS if max_chars is None else max_chars
    return truncate_markdown(content, max_chars=limit, suffix="\n\n（完整内容将在最终回复中显示。）")


def build_text_response_payloads(
    req_id: str, session_id: str, content: str, *, final: bool, max_chars: int | None = None
) -> list[dict]:
    limit = STREAM_TEXT_MAX_CHARS if max_chars is None else max_chars
    if not final:
        # Refreshes replace the existing stream's full content. Only the final
        # reply creates additional messages, so progress never leaves stale streams.
        preview = stream_text_preview(content, max_chars=limit)
        return [build_text_response_payload(req_id, session_id, preview, final=False)]
    chunks = split_markdown_chunks(content, max_chars=limit)
    payloads: list[dict] = []
    for index, chunk in enumerate(chunks):
        payloads.append(
            build_text_response_payload(
                req_id,
                session_id if index == 0 else f"{session_id}-part-{index + 1}",
                chunk,
                final=True,
            )
        )
    return payloads


def parse_text_callback(payload: dict) -> WeComTextMessage | None:
    if payload.get("cmd") != "aibot_msg_callback":
        return None
    body = _as_dict(payload.get("body"))
    if body.get("msgtype") not in {"text", "voice"}:
        return None
    try:
        chat_key = chat_key_from_message(payload)
    except ValueError:
        return None
    return WeComTextMessage(
        req_id=str(_as_dict(payload.get("headers")).get("req_id") or ""),
        chat_key=chat_key,
        content=message_text(body),
        raw_payload=payload,
    )


def extract_response_url(payload: dict) -> str | None:
    body = _as_dict(payload.get("body"))
    headers = _as_dict(payload.get("headers"))
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
    body = _as_dict(payload.get("body"))
    if body.get("msgtype") != "event":
        return None
    event = _as_dict(body.get("event"))
    if event.get("eventtype") != "template_card_event":
        return None
    card_event = _as_dict(event.get("template_card_event"))
    selected_items = []
    for item in (_as_dict(card_event.get("selected_items")).get("selected_item") or []):
        if not isinstance(item, dict):
            continue
        option_ids = tuple(
            str(option_id)
            for option_id in (_as_dict(item.get("option_ids")).get("option_id") or [])
            if str(option_id)
        )
        selected_items.append(
            WeComTemplateCardSelection(
                question_key=str(item.get("question_key") or ""),
                option_ids=option_ids,
            )
        )
    try:
        chat_key = chat_key_from_message(payload)
    except ValueError:
        return None
    return WeComTemplateCardEvent(
        req_id=str(_as_dict(payload.get("headers")).get("req_id") or ""),
        chat_key=chat_key,
        card_type=str(card_event.get("card_type") or ""),
        event_key=str(card_event.get("event_key") or ""),
        task_id=str(card_event.get("task_id") or "") or None,
        selected_items=tuple(selected_items),
        raw_payload=payload,
    )


def is_subscribe_ok(payload: dict) -> bool:
    raw_errcode = payload.get("errcode")
    if raw_errcode in (None, ""):
        return False
    try:
        return int(raw_errcode) == 0
    except (TypeError, ValueError):
        return False
