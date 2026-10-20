from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time
from pathlib import Path

from .atomic_io import atomic_write_json, ensure_state_directory
from .wecom_limits import UPLOAD_CHUNK_BYTES, UPLOAD_SESSION_SECONDS, validate_media_upload


class MediaUploadRejected(RuntimeError):
    pass


def _ack_body(response: dict, stage: str, required: str | None = None) -> dict:
    try:
        code = int(response.get("errcode", 0))
    except (TypeError, ValueError) as exc:
        raise MediaUploadRejected(f"{stage} failed: invalid errcode") from exc
    if code:
        raise MediaUploadRejected(f"{stage} failed: {response.get('errmsg') or code}")
    body = response.get("body") if isinstance(response.get("body"), dict) else {}
    if required and not isinstance(body.get(required), str):
        raise MediaUploadRejected(f"{stage} failed: missing {required}")
    if required and not body[required]:
        raise MediaUploadRejected(f"{stage} failed: missing {required}")
    return body


async def upload_media(
    file_path: Path,
    *,
    filename: str,
    media_type: str,
    owner: str,
    destination: str,
    cache_root: Path,
    send,
    uid,
    max_bytes: int | None = None,
    clock=None,
) -> str:
    """Resume ACKed chunks; resend uncertain chunks using the documented idempotency."""
    clock = clock or time.time
    size = file_path.stat().st_size
    validate_media_upload(filename, size, media_type, max_bytes=max_bytes)
    data = file_path.read_bytes()
    total = validate_media_upload(filename, len(data), media_type, max_bytes=max_bytes)
    identity = json.dumps([owner, destination, str(file_path.resolve()), filename, media_type, hashlib.sha256(data).hexdigest()], ensure_ascii=False)
    key = hashlib.sha256(identity.encode()).hexdigest()
    ensure_state_directory(cache_root)
    state_path = cache_root / f"{key}.json"
    state = {}
    try:
        loaded = json.loads(state_path.read_text())
        if isinstance(loaded, dict):
            state = loaded
    except (FileNotFoundError, ValueError, OSError):
        pass
    now = clock()
    if state.get("media_id") and now < float(state.get("media_expires_at") or 0):
        return str(state["media_id"])
    uploaded = state.get("uploaded")
    resumable = (
        isinstance(state.get("upload_id"), str) and state["upload_id"]
        and now < float(state.get("upload_expires_at") or 0)
        and isinstance(uploaded, list)
        and all(type(index) is int and 0 <= index < total for index in uploaded)
    )
    if not resumable:
        response = await send({
            "cmd": "aibot_upload_media_init", "headers": {"req_id": uid()},
            "body": {"type": media_type, "filename": filename, "total_size": len(data), "total_chunks": total,
                     "md5": hashlib.md5(data).hexdigest()},
        }, 30)
        upload_id = _ack_body(response, "upload init", "upload_id")["upload_id"]
        state = {"upload_id": upload_id, "upload_expires_at": clock() + UPLOAD_SESSION_SECONDS, "uploaded": []}
        atomic_write_json(state_path, state)
    confirmed = set(state["uploaded"])
    state_lock = asyncio.Lock()
    concurrency = asyncio.Semaphore(4)

    async def send_chunk(index: int) -> None:
        async with concurrency:
            if clock() >= state["upload_expires_at"]:
                raise MediaUploadRejected("upload session expired")
            chunk = data[index * UPLOAD_CHUNK_BYTES:(index + 1) * UPLOAD_CHUNK_BYTES]
            response = await send({
                "cmd": "aibot_upload_media_chunk", "headers": {"req_id": uid()},
                "body": {"upload_id": state["upload_id"], "chunk_index": index, "base64_data": base64.b64encode(chunk).decode("ascii")},
            }, 30)
            _ack_body(response, f"upload chunk {index}")
            async with state_lock:
                confirmed.add(index)
                state["uploaded"] = sorted(confirmed)
                atomic_write_json(state_path, state)

    tasks = [asyncio.create_task(send_chunk(index)) for index in range(total) if index not in confirmed]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failure = next((result for result in results if isinstance(result, BaseException)), None)
        if failure is not None:
            raise failure
        if clock() >= state["upload_expires_at"]:
            raise MediaUploadRejected("upload session expired")
        response = await send({
            "cmd": "aibot_upload_media_finish", "headers": {"req_id": uid()}, "body": {"upload_id": state["upload_id"]},
        }, 60)
        media_id = _ack_body(response, "upload finish", "media_id")["media_id"]
    except MediaUploadRejected:
        # A rejected/expired session is not reusable; transport failures keep
        # confirmed progress for the next caller, including after process restart.
        state_path.unlink(missing_ok=True)
        raise
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    state.update(media_id=media_id, media_expires_at=clock() + 3 * 86400)
    atomic_write_json(state_path, state)
    return media_id


def native_media_body(media_type: str, media_id: str, *, title: str = "", description: str = "") -> dict:
    media = {"media_id": media_id}
    if media_type == "video":
        if title:
            media["title"] = title.encode("utf-8")[:64].decode("utf-8", "ignore")
        if description:
            media["description"] = description.encode("utf-8")[:512].decode("utf-8", "ignore")
    return {"msgtype": media_type, media_type: media}
