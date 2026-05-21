from __future__ import annotations

import asyncio
import base64
import hashlib
from pathlib import Path

from .models import FileSendRequest, WeComBotRuntime


def uid() -> str:
    import time
    import itertools

    if not hasattr(uid, "_counter"):
        uid._counter = itertools.count()

    return f"{int(time.time() * 1000):x}-{next(uid._counter):x}"


def chat_key_to_send_target(key: str) -> tuple[int, str]:
    if key.startswith("group-user:"):
        parts = key.split(":", 2)
        return 2, parts[1]
    chat_type_name, chat_id = key.split(":", 1)
    return (2 if chat_type_name == "group" else 1), chat_id


def build_send_file_payload(chat_key: str, media_id: str) -> dict:
    chat_type, chat_id = chat_key_to_send_target(chat_key)
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {"chatid": chat_id, "chat_type": chat_type, "msgtype": "file", "file": {"media_id": media_id}},
    }


def create_request_future(bot: WeComBotRuntime, req_id: str) -> asyncio.Future:
    if bot.pending_requests is None:
        bot.pending_requests = {}
    future = asyncio.get_running_loop().create_future()
    bot.pending_requests[req_id] = future
    return future


def resolve_pending_request(bot: WeComBotRuntime, payload: dict) -> bool:
    req_id = str((payload.get("headers") or {}).get("req_id") or "")
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
    async with bot.ws_send_lock:
        await bot.ws.send_json(payload)


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


def require_ack_ok(payload: dict, stage: str, *, required_body_key: str | None = None) -> dict:
    errcode = int(payload.get("errcode", 0))
    if errcode != 0:
        errmsg = str(payload.get("errmsg") or f"{stage} failed")
        raise RuntimeError(f"{stage} failed: {errmsg}")
    body = payload.get("body") or {}
    if required_body_key and not body.get(required_body_key):
        raise RuntimeError(f"{stage} failed: missing {required_body_key}")
    return body


async def upload_and_send_file(bot: WeComBotRuntime, request: FileSendRequest) -> dict:
    file_bytes = Path(request.file_path).read_bytes()
    file_name = request.file_name
    file_size = len(file_bytes)
    chunk_size = 400 * 1024
    total_chunks = max(1, (file_size + chunk_size - 1) // chunk_size)
    md5 = hashlib.md5(file_bytes).hexdigest()

    init_response = await send_ws_payload_with_ack(
        bot,
        {
            "cmd": "aibot_upload_media_init",
            "headers": {"req_id": uid()},
            "body": {
                "type": "file",
                "filename": file_name,
                "total_size": file_size,
                "total_chunks": total_chunks,
                "md5": md5,
            },
        },
        30,
    )
    upload_id = require_ack_ok(init_response, "upload init", required_body_key="upload_id")["upload_id"]
    for idx in range(total_chunks):
        chunk = file_bytes[idx * chunk_size : (idx + 1) * chunk_size]
        require_ack_ok(
            await send_ws_payload_with_ack(
                bot,
                {
                    "cmd": "aibot_upload_media_chunk",
                    "headers": {"req_id": uid()},
                    "body": {"upload_id": upload_id, "chunk_index": idx, "base64_data": base64.b64encode(chunk).decode("ascii")},
                },
                30,
            ),
            f"upload chunk {idx}",
        )
    finish_response = await send_ws_payload_with_ack(
        bot,
        {
            "cmd": "aibot_upload_media_finish",
            "headers": {"req_id": uid()},
            "body": {"upload_id": upload_id},
        },
        60,
    )
    media_id = require_ack_ok(finish_response, "upload finish", required_body_key="media_id")["media_id"]
    require_ack_ok(await send_ws_payload_with_ack(bot, build_send_file_payload(request.chat_key, media_id), 30), "send file")
    return {"ok": True, "mediaId": media_id}
