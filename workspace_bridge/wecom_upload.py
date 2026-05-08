from __future__ import annotations

import asyncio
import base64
import hashlib
from pathlib import Path

from .models import FileSendRequest, WeComBotRuntime
from .wecom_protocol import payload_req_id, uid

UPLOAD_CHUNK_SIZE = 400 * 1024


def chat_key_to_send_target(chat_key: str) -> tuple[int, str]:
    if chat_key.startswith("group-user:"):
        parts = chat_key.split(":", 2)
        return 2, parts[1]
    prefix, value = chat_key.split(":", 1)
    return (2 if prefix == "group" else 1), value


def create_request_future(bot: WeComBotRuntime, req_id: str) -> asyncio.Future:
    if bot.pending_requests is None:
        bot.pending_requests = {}
    future = asyncio.get_running_loop().create_future()
    bot.pending_requests[req_id] = future
    return future


def resolve_pending_request(bot: WeComBotRuntime, payload: dict) -> bool:
    req_id = payload_req_id(payload)
    if not req_id or not bot.pending_requests:
        return False
    future = bot.pending_requests.pop(req_id, None)
    if future is None or future.done():
        return False
    future.set_result(payload)
    return True


def reject_pending_requests(bot: WeComBotRuntime, message: str) -> None:
    if not bot.pending_requests:
        return
    for req_id, future in list(bot.pending_requests.items()):
        bot.pending_requests.pop(req_id, None)
        if hasattr(future, "done") and not future.done():
            future.set_exception(RuntimeError(message))


async def ws_send_json(bot: WeComBotRuntime, payload: dict) -> None:
    if bot.ws is None:
        raise RuntimeError("bot websocket not connected")
    await bot.ws.send_json(payload)


async def send_ws_payload_with_ack(bot: WeComBotRuntime, payload: dict, timeout_sec: int) -> dict:
    req_id = payload_req_id(payload)
    if not req_id:
        raise ValueError("payload req_id required for ack")
    future = create_request_future(bot, req_id)
    try:
        await ws_send_json(bot, payload)
        return await asyncio.wait_for(future, timeout_sec)
    except Exception:
        if bot.pending_requests is not None:
            bot.pending_requests.pop(req_id, None)
        raise


def build_upload_init_payload(file_name: str, file_size: int, total_chunks: int, md5: str) -> dict:
    return {
        "cmd": "aibot_upload_media_init",
        "headers": {"req_id": uid()},
        "body": {
            "type": "file",
            "filename": file_name,
            "total_size": file_size,
            "total_chunks": total_chunks,
            "md5": md5,
        },
    }


def build_upload_chunk_payload(upload_id: str, chunk_index: int, chunk: bytes) -> dict:
    return {
        "cmd": "aibot_upload_media_chunk",
        "headers": {"req_id": uid()},
        "body": {
            "upload_id": upload_id,
            "chunk_index": chunk_index,
            "base64_data": base64.b64encode(chunk).decode("ascii"),
        },
    }


def build_upload_finish_payload(upload_id: str) -> dict:
    return {
        "cmd": "aibot_upload_media_finish",
        "headers": {"req_id": uid()},
        "body": {"upload_id": upload_id},
    }


def build_send_file_payload(chat_key: str, media_id: str) -> dict:
    chat_type, chat_id = chat_key_to_send_target(chat_key)
    return {
        "cmd": "aibot_send_msg",
        "headers": {"req_id": uid()},
        "body": {"chatid": chat_id, "chat_type": chat_type, "msgtype": "file", "file": {"media_id": media_id}},
    }


async def upload_and_send_file(bot: WeComBotRuntime, request: FileSendRequest) -> dict:
    file_bytes = request.file_path.read_bytes()
    file_size = len(file_bytes)
    total_chunks = max(1, (file_size + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE)
    md5 = hashlib.md5(file_bytes).hexdigest()

    init_response = await send_ws_payload_with_ack(
        bot,
        build_upload_init_payload(request.file_name, file_size, total_chunks, md5),
        30,
    )
    upload_id = str(((init_response.get("body") or {}).get("upload_id")) or "").strip()
    if init_response.get("errcode") != 0 or not upload_id:
        raise RuntimeError(f"upload init failed: {init_response}")

    chunk_futures: list[asyncio.Future] = []
    req_ids: list[str] = []
    for idx in range(total_chunks):
        chunk = file_bytes[idx * UPLOAD_CHUNK_SIZE : (idx + 1) * UPLOAD_CHUNK_SIZE]
        payload = build_upload_chunk_payload(upload_id, idx, chunk)
        req_id = payload_req_id(payload)
        future = create_request_future(bot, req_id)
        try:
            await ws_send_json(bot, payload)
        except Exception:
            if bot.pending_requests is not None:
                bot.pending_requests.pop(req_id, None)
            raise
        chunk_futures.append(future)
        req_ids.append(req_id)

    try:
        responses = await asyncio.wait_for(asyncio.gather(*chunk_futures), max(5, total_chunks * 2))
    except Exception:
        if bot.pending_requests is not None:
            for req_id in req_ids:
                bot.pending_requests.pop(req_id, None)
        raise
    for response in responses:
        if response.get("errcode") not in (None, 0):
            raise RuntimeError(f"upload chunk failed: {response}")

    finish_response = await send_ws_payload_with_ack(bot, build_upload_finish_payload(upload_id), 60)
    media_id = str(((finish_response.get("body") or {}).get("media_id")) or "").strip()
    if finish_response.get("errcode") != 0 or not media_id:
        raise RuntimeError(f"upload finish failed: {finish_response}")

    send_response = await send_ws_payload_with_ack(bot, build_send_file_payload(request.chat_key, media_id), 30)
    if send_response.get("errcode") not in (None, 0):
        raise RuntimeError(f"file send failed: {send_response}")
    return {
        "ok": True,
        "mediaId": media_id,
        "fileName": request.file_name,
        "filePath": str(request.file_path),
    }
