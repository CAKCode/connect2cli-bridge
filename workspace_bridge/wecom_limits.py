from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Awaitable, Callable


MEDIA_MAX_BYTES = {"file": 20 * 1024 * 1024, "image": 10 * 1024 * 1024, "voice": 2 * 1024 * 1024, "video": 10 * 1024 * 1024}
MEDIA_EXTENSIONS = {"image": {".png", ".jpg", ".jpeg", ".gif"}, "voice": {".amr"}, "video": {".mp4"}}
UPLOAD_CHUNK_BYTES = 400 * 1024
UPLOAD_MAX_CHUNKS = 100
UPLOAD_SESSION_SECONDS = 30 * 60


def validate_media_upload(filename: str, size: int, media_type: str = "file", *, max_bytes: int | None = None) -> int:
    from pathlib import Path

    if media_type not in MEDIA_MAX_BYTES:
        raise ValueError("media type must be file, image, voice or video")
    if not filename or len(filename.encode("utf-8")) > 256:
        raise ValueError("media filename must contain 1 to 256 UTF-8 bytes")
    limit = MEDIA_MAX_BYTES[media_type]
    if max_bytes is not None:
        limit = min(limit, max_bytes)
    if size < 5:
        raise ValueError("media file must contain at least 5 bytes")
    if size > limit:
        raise ValueError(f"file too large: {size} bytes (max {limit})")
    allowed = MEDIA_EXTENSIONS.get(media_type)
    if allowed is not None and Path(filename).suffix.lower() not in allowed:
        raise ValueError(f"unsupported {media_type} format")
    chunks = (size + UPLOAD_CHUNK_BYTES - 1) // UPLOAD_CHUNK_BYTES
    if chunks > UPLOAD_MAX_CHUNKS:
        raise ValueError("media upload exceeds 100 chunks")
    return chunks


class WeComRateLimitError(RuntimeError):
    pass


class WeComRateLimiter:
    """Share the official rolling quotas across all members of a conversation."""

    def __init__(self, *, clock: Callable[[], float] | None = None, sleep: Callable[[float], Awaitable[None]] | None = None):
        self.clock = clock or time.monotonic
        self.sleep = sleep or asyncio.sleep
        self._lock = asyncio.Lock()
        self._sent: dict[str, deque[float]] = {}

    async def acquire(self, scope: str, *, deadline: float | None = None) -> None:
        while True:
            async with self._lock:
                now = self.clock()
                if deadline is not None and now >= deadline:
                    raise WeComRateLimitError("callback reply deadline exceeded")
                sent = self._sent.setdefault(scope, deque())
                while sent and sent[0] <= now - 3600:
                    sent.popleft()
                minute = [stamp for stamp in sent if stamp > now - 60]
                delay = max(
                    minute[-30] + 60 - now if len(minute) >= 30 else 0,
                    sent[0] + 3600 - now if len(sent) >= 1000 else 0,
                )
                if delay <= 0:
                    sent.append(now)
                    return
                if deadline is not None and now + delay >= deadline:
                    raise WeComRateLimitError("conversation quota cannot meet callback reply deadline")
            # Do not hold the lock while another conversation can still send.
            await self.sleep(delay)


async def wait_for_send_slot(
    limiter: WeComRateLimiter,
    payload: dict,
    *,
    callback_target: tuple[int, str] | None = None,
    received_at: float | None = None,
    deadline: float | None = None,
) -> None:
    cmd = payload.get("cmd")
    body = payload.get("body") or {}
    if cmd == "aibot_upload_media_init":
        await limiter.acquire("upload")
        return
    if cmd == "aibot_send_msg":
        target = (int(body.get("chat_type") or 0), str(body.get("chatid") or ""))
    elif cmd in {"aibot_respond_msg", "aibot_respond_update_msg", "aibot_respond_welcome_msg"}:
        target = callback_target
    else:
        return
    if cmd in {"aibot_respond_update_msg", "aibot_respond_welcome_msg"} and received_at is not None:
        deadline = min(deadline, received_at + 5) if deadline is not None else received_at + 5
    if target is not None and target[1]:
        await limiter.acquire(f"chat:{target[0]}:{target[1]}", deadline=deadline)
