from __future__ import annotations

import asyncio
import time
from pathlib import Path

from .models import FileSendRequest, WeComBotRuntime
from .wecom_protocol import uid, chat_key_to_send_target
from .wecom_limits import wait_for_send_slot
from .wecom_context import normalize_assistant_reply
from .wecom_media import native_media_body, upload_media


def build_send_file_payload(chat_key: str, media_id: str) -> dict:
    media_id_text = str(media_id or "").strip()
    if not media_id_text:
        raise ValueError("media_id required")
    chat_type, chat_id = chat_key_to_send_target(chat_key)
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {"chatid": chat_id, "chat_type": chat_type, "msgtype": "file", "file": {"media_id": media_id_text}},
    }


def create_request_future(bot: WeComBotRuntime, req_id: str) -> asyncio.Future:
    if bot.pending_requests is None:
        bot.pending_requests = {}
    req_text = str(req_id or "").strip()
    if not req_text:
        raise ValueError("req_id required")
    existing = bot.pending_requests.get(req_text)
    if existing is not None and not existing.done():
        raise RuntimeError(f"duplicate pending request: {req_text}")
    future = asyncio.get_running_loop().create_future()
    bot.pending_requests[req_text] = future
    return future


def resolve_pending_request(bot: WeComBotRuntime, payload: dict) -> bool:
    headers = payload.get("headers")
    req_id = str(headers.get("req_id") or "").strip() if isinstance(headers, dict) else ""
    if not req_id or not bot.pending_requests:
        return False
    future = bot.pending_requests.pop(req_id, None)
    if not future:
        return False
    if not future.done():
        future.set_result(payload)
    return True


def reject_pending_requests(bot: WeComBotRuntime, message: str) -> None:
    if not bot.pending_requests:
        return
    for req_id, future in list(bot.pending_requests.items()):
        bot.pending_requests.pop(req_id, None)
        if not future.done():
            future.set_exception(RuntimeError(message))


async def ws_send_json(bot: WeComBotRuntime, payload: dict) -> None:
    if bot.ws is None:
        raise RuntimeError("bot websocket not connected")
    if bot.ws_send_lock is None:
        bot.ws_send_lock = asyncio.Lock()
    context = bot.callback_contexts.get(str((payload.get("headers") or {}).get("req_id") or ""), {})
    target = context.get("target")
    if target is None:
        state = bot.reply_states.get(str((payload.get("headers") or {}).get("req_id") or ""))
        if state:
            target = chat_key_to_send_target(state.chat_key)
    await wait_for_send_slot(bot.rate_limiter, payload, callback_target=target, received_at=context.get("received_at"))
    async with bot.ws_send_lock:
        deadline = context.get("received_at", time.monotonic()) + 5 if payload.get("cmd") in {"aibot_respond_update_msg", "aibot_respond_welcome_msg"} else None
        if deadline is None:
            await bot.ws.send_json(payload)
        else:
            await asyncio.wait_for(bot.ws.send_json(payload), max(0, deadline - bot.rate_limiter.clock()))
        stream_id = ((payload.get("body") or {}).get("stream") or {}).get("id")
        if context and stream_id:
            context["streams"].setdefault(stream_id, bot.rate_limiter.clock())


async def send_ws_payload_with_ack(bot: WeComBotRuntime, payload: dict, timeout_sec: int) -> dict:
    req_id = payload["headers"]["req_id"]
    future = create_request_future(bot, req_id)
    try:
        await ws_send_json(bot, payload)
        return await asyncio.wait_for(future, timeout_sec)
    except Exception:
        if bot.pending_requests is not None:
            bot.pending_requests.pop(req_id, None)
        if not future.done():
            future.cancel()
        raise
    finally:
        if bot.pending_requests is not None and bot.pending_requests.get(req_id) is future:
            bot.pending_requests.pop(req_id, None)
        if not future.done():
            future.cancel()


def require_ack_ok(payload: dict, stage: str, *, required_body_key: str | None = None) -> dict:
    raw_errcode = payload.get("errcode", 0)
    try:
        errcode = int(raw_errcode if raw_errcode not in (None, "") else 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{stage} failed: invalid errcode") from exc
    if errcode != 0:
        errmsg = str(payload.get("errmsg") or f"{stage} failed")
        raise RuntimeError(f"{stage} failed: {errmsg}")
    body = payload.get("body")
    if not isinstance(body, dict):
        body = {}
    if required_body_key and not body.get(required_body_key):
        raise RuntimeError(f"{stage} failed: missing {required_body_key}")
    return body


async def upload_and_send_file(bot: WeComBotRuntime, request: FileSendRequest) -> dict:
    assistant = normalize_assistant_reply(request.to_ai_assistant, proactive=True)
    if assistant and request.media_type == "voice":
        raise ValueError("assistant replies do not support voice")
    media_id = await upload_media(
        Path(request.file_path), filename=request.file_name, media_type=request.media_type,
        owner=bot.config.bot_id, destination=request.chat_key,
        cache_root=bot.config.runtime_root / ".wecom-uploads",
        send=lambda payload, timeout: send_ws_payload_with_ack(bot, payload, timeout), uid=uid,
        max_bytes=bot.config.max_upload_size,
    )
    chat_type, chat_id = chat_key_to_send_target(request.chat_key)
    body = {"chatid": chat_id, "chat_type": chat_type,
            **native_media_body(request.media_type, media_id, title=request.title, description=request.description)}
    if assistant:
        body["to_ai_assistant"] = assistant
    payload = {"cmd": "aibot_send_msg", "headers": {"req_id": uid()}, "body": body}
    require_ack_ok(await send_ws_payload_with_ack(bot, payload, 30), "send file")
    return {"ok": True, "mediaId": media_id}
