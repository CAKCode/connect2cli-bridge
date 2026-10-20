from __future__ import annotations

import base64
import re
from pathlib import Path
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import aiohttp


def sanitize_file_name(name: str, fallback: str = "attachment.bin") -> str:
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name or fallback).strip())
    if base in {".", ".."}:
        return fallback
    return base or fallback


def build_nonconflicting_file_path(target_dir: Path, file_name: str) -> Path:
    normalized_name = Path(str(file_name or "")).name or "attachment.bin"
    if normalized_name in {".", ".."}:
        normalized_name = "attachment.bin"
    candidate = target_dir / normalized_name
    if not candidate.exists() and not candidate.is_symlink():
        return candidate
    path = Path(normalized_name)
    stem = path.stem or "attachment"
    suffix = path.suffix
    index = 1
    while True:
        candidate = target_dir / f"{stem}-{index}{suffix}"
        if not candidate.exists() and not candidate.is_symlink():
            return candidate
        index += 1


def extract_mixed_text(mixed: dict) -> str:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    parts = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("msgtype") == "text":
            content = ((item.get("text") or {}).get("content") or "").strip()
            if content:
                parts.append(content)
    return "\n".join(parts)


def extract_mixed_images(mixed: dict) -> list[dict]:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    return [
        item.get("image")
        for item in items
        if (
            isinstance(item, dict)
            and item.get("msgtype") == "image"
            and isinstance(item.get("image"), dict)
            and (
                item["image"].get("url")
                or item["image"].get("download_url")
                or item["image"].get("downloadUrl")
            )
        )
    ]


def decode_aes_key(key: str) -> bytes:
    if len(key) == 32:
        return key.encode("utf-8")
    try:
        data = base64.b64decode(key + "=" * ((4 - len(key) % 4) % 4))
    except Exception as exc:
        raise ValueError("invalid aes key") from exc
    if len(data) not in {16, 24, 32}:
        raise ValueError("invalid aes key")
    return data


def decrypt_media_buffer(data: bytes, key: str) -> bytes:
    decoded = decode_aes_key(key)
    if not data or len(data) % AES.block_size:
        raise ValueError("invalid encrypted media size")
    plaintext = AES.new(decoded, AES.MODE_CBC, iv=decoded[:16]).decrypt(data)
    try:
        return unpad(plaintext, 32)
    except ValueError as exc:
        raise ValueError("invalid encrypted media padding") from exc


def should_fallback_to_curl(exc: Exception) -> bool:
    text = str(exc).lower()
    return "connection reset" in text or "broken pipe" in text or "ssl" in text


async def download_callback_media(directory: Path, kind: str, payload: dict, *, max_bytes: int) -> Path:
    url = payload.get("url")
    if not isinstance(url, str) or not url.startswith(("https://", "http://")):
        raise ValueError("media download URL required")
    key = payload.get("aeskey") or payload.get("aes_key")
    limit = max_bytes + 32 if key else max_bytes
    data = bytearray()
    timeout = aiohttp.ClientTimeout(total=20, sock_connect=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            async for chunk in response.content.iter_chunked(64 * 1024):
                data.extend(chunk)
                if len(data) > limit:
                    raise ValueError("incoming media exceeds size limit")
    plaintext = decrypt_media_buffer(bytes(data), str(key)) if key else bytes(data)
    if len(plaintext) > max_bytes:
        raise ValueError("incoming media exceeds size limit")
    from .atomic_io import ensure_state_directory
    ensure_state_directory(directory)
    fallback = {"image": "image.jpg", "video": "video.mp4"}.get(kind, "attachment.bin")
    filename = sanitize_file_name(str(payload.get("filename") or payload.get("file_name") or fallback), fallback)
    path = build_nonconflicting_file_path(directory, filename)
    path.write_bytes(plaintext)
    path.chmod(0o600)
    return path
