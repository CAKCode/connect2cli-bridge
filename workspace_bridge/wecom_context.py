from __future__ import annotations

import copy
import hashlib
import time
from dataclasses import dataclass, field


def _object(value) -> dict:
    return value if isinstance(value, dict) else {}


def message_source(body: dict) -> str:
    return str(_object(body.get("compat_info")).get("source") or "user")


def callback_context(frame: dict, chat_key: str, target: tuple[int, str], *, received_at: float) -> dict:
    body = _object(frame.get("body"))
    info = assistant_call_info(body) if frame.get("cmd") == "aibot_msg_callback" else None
    return {
        "chat_key": chat_key,
        "target": target,
        "source": message_source(body),
        "received_at": received_at,
        "assistant_task_id": info["task_id"] if info else None,
        "streams": {},
        "feedback_id": "feedback-" + hashlib.sha256(str(_object(frame.get("headers")).get("req_id") or "").encode()).hexdigest()[:40],
    }


def assistant_call_info(body: dict) -> dict | None:
    compat = _object(body.get("compat_info"))
    if compat.get("source") != "ai_assistant":
        return None
    info = _object(compat.get("from_ai_assistant"))
    task_id = info.get("task_id")
    sub_id, count = info.get("sub_id"), info.get("total_sub_count")
    if not isinstance(task_id, str) or not task_id.strip():
        raise ValueError("assistant call task_id required")
    if type(sub_id) is not int or type(count) is not int or not 1 <= sub_id <= count <= 2147483647:
        raise ValueError("assistant call sub_id and total_sub_count must be valid positive int32 values")
    if body.get("msgtype") not in {"text", "image", "file"}:
        raise ValueError("assistant input must be text, image or file")
    return {"task_id": task_id, "sub_id": sub_id, "total_sub_count": count}


def normalize_assistant_reply(value, *, proactive: bool = False) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("to_ai_assistant must be an object")
    if value.get("reply_status") not in {"waiting", "finished"}:
        raise ValueError("to_ai_assistant.reply_status must be waiting or finished")
    reply = {"reply_status": value["reply_status"]}
    task_id = value.get("task_id")
    if proactive and (not isinstance(task_id, str) or not task_id.strip()):
        raise ValueError("to_ai_assistant.task_id required for proactive replies")
    if isinstance(task_id, str) and task_id.strip():
        reply["task_id"] = task_id
    return reply


def decorate_assistant_replies(payloads: list[dict], context: dict | None, *, final: bool) -> list[dict]:
    task_id = (context or {}).get("assistant_task_id")
    if not task_id:
        return payloads
    result = copy.deepcopy(payloads)
    for index, payload in enumerate(result):
        body = payload.setdefault("body", {})
        if body.get("msgtype") not in {"stream", "markdown", "image", "file", "video"}:
            raise ValueError("assistant replies do not support this message type")
        reply = {"reply_status": "finished" if final and index == len(result) - 1 else "waiting"}
        if payload.get("cmd") == "aibot_send_msg":
            reply["task_id"] = task_id
        body["to_ai_assistant"] = reply
    return result


def decorate_context_replies(payloads: list[dict], context: dict | None, *, final: bool) -> list[dict]:
    result = decorate_assistant_replies(payloads, context, final=final)
    if not context or not context.get("feedback_id"):
        return result
    result = copy.deepcopy(result)
    for payload in result:
        stream = _object(_object(payload.get("body")).get("stream"))
        if stream and stream.get("id") not in context.get("streams", {}):
            stream["feedback"] = {"id": context["feedback_id"]}
    return result


def message_text(body: dict) -> str:
    kind = body.get("msgtype")
    if kind == "text":
        return str(_object(body.get("text")).get("content") or "").strip()
    if kind == "voice":
        voice = _object(body.get("voice"))
        # WeCom currently sends content, but keep the parser tolerant of
        # recognition/transcript aliases used by SDKs and protocol revisions.
        for candidate_field in ("content", "recognition", "transcript", "text"):
            value = str(voice.get(candidate_field) or "").strip()
            if value:
                return value
        return ""
    if kind == "mixed":
        return "\n".join(message_text(_object(item)) for item in _object(body.get("mixed")).get("msg_item", []) if _object(item).get("msgtype") == "text").strip()
    if kind in {"file", "video", "image"}:
        media = _object(body.get(kind))
        return str(media.get("filename") or media.get("file_name") or kind)
    return ""


def text_with_quote(content: str, body: dict) -> str:
    quote = _object(body.get("quote"))
    quoted = message_text(quote)
    if not quoted:
        return content
    return f"用户请求：\n{content}\n\n引用消息：\n{quoted}"


def quoted_media(body: dict) -> list[tuple[str, dict]]:
    quote = _object(body.get("quote"))
    kind = quote.get("msgtype")
    if kind in {"image", "file", "video"} and _object(quote.get(kind)).get("url"):
        return [(kind, quote[kind])]
    if kind == "mixed":
        return [("image", _object(item.get("image"))) for item in _object(quote.get("mixed")).get("msg_item", [])
                if isinstance(item, dict) and item.get("msgtype") == "image" and _object(item.get("image")).get("url")]
    return []


def feedback_event(body: dict) -> dict | None:
    event = _object(body.get("event"))
    if event.get("eventtype") != "feedback_event":
        return None
    feedback = _object(event.get("feedback_event"))
    feedback_id, verdict = feedback.get("id"), feedback.get("type")
    if not isinstance(feedback_id, str) or not 1 <= len(feedback_id.encode("utf-8")) <= 256:
        raise ValueError("feedback id must contain 1 to 256 UTF-8 bytes")
    if type(verdict) is not int or verdict not in {1, 2, 3}:
        raise ValueError("feedback type must be 1, 2 or 3")
    reasons = feedback.get("inaccurate_reason_list") or []
    if not isinstance(reasons, list) or any(type(reason) is not int or reason not in {1, 2, 3, 4} for reason in reasons):
        raise ValueError("invalid negative feedback reasons")
    return {"id": feedback_id, "type": verdict, "content": str(feedback.get("content") or "")[:4000], "inaccurate_reason_list": reasons}


@dataclass
class _AssistantCall:
    count: int
    identity: tuple[str, str]
    created_at: float
    messages: dict[int, dict] = field(default_factory=dict)


class AssistantCallAssembler:
    def __init__(self, *, clock=None):
        self.clock = clock or time.monotonic
        self.pending: dict[str, _AssistantCall] = {}
        self.completed: dict[str, float] = {}

    def add(self, frame: dict) -> list[dict] | None:
        body = _object(frame.get("body"))
        info = assistant_call_info(body)
        if info is None:
            return [frame]
        now = self.clock()
        self.pending = {key: call for key, call in self.pending.items() if now - call.created_at < 300}
        self.completed = {key: stamp for key, stamp in self.completed.items() if now - stamp < 86400}
        task_id = info["task_id"]
        if task_id in self.completed:
            return None
        identity = (str(body.get("chatid") or ""), str(_object(body.get("from")).get("userid") or ""))
        call = self.pending.setdefault(task_id, _AssistantCall(info["total_sub_count"], identity, now))
        if call.count != info["total_sub_count"] or call.identity != identity:
            raise ValueError("inconsistent assistant call identity or total_sub_count")
        call.messages.setdefault(info["sub_id"], copy.deepcopy(frame))
        if len(call.messages) != call.count:
            return None
        self.pending.pop(task_id, None)
        self.completed[task_id] = now
        return [call.messages[index] for index in sorted(call.messages)]

    def expire(self, task_id: str, *, timeout_sec: float = 300) -> list[dict] | None:
        """Remove and return an incomplete call after its collection deadline."""
        call = self.pending.get(task_id)
        if call is None or self.clock() - call.created_at < timeout_sec:
            return None
        self.pending.pop(task_id, None)
        self.completed[task_id] = self.clock()
        return [call.messages[index] for index in sorted(call.messages)]
