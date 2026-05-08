from __future__ import annotations

import asyncio
import mimetypes
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from Crypto.Cipher import AES

from .runtime import prepare_session_run

MAX_INBOUND_IMAGE_SIZE = 30 * 1024 * 1024
MAX_INBOUND_FILE_SIZE = 100 * 1024 * 1024


def extension_from_url(raw_url: str) -> str:
    try:
        ext = Path(urlparse(raw_url).path).suffix
        return ext if ext and len(ext) <= 10 else ""
    except Exception:
        return ""


def sanitize_file_name(name: str, fallback: str = "attachment.bin") -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(name or fallback).strip())
    return cleaned or fallback


def build_nonconflicting_file_path(target_dir: Path, file_name: str) -> Path:
    candidate = target_dir / file_name
    if not candidate.exists():
        return candidate
    path = Path(file_name)
    stem = path.stem or "attachment"
    suffix = path.suffix
    index = 1
    while True:
        candidate = target_dir / f"{stem}-{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def decode_aes_key(raw: str) -> bytes | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        decoded = text.encode("utf-8")
    except Exception:
        return None
    return decoded if len(decoded) == 32 else None


def decrypt_media_buffer(buffer: bytes, aes_key: str) -> bytes:
    key = decode_aes_key(aes_key)
    if not key:
        raise ValueError("invalid aes key")
    iv = key[:16]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(buffer)
    if not decrypted:
        return decrypted
    pad = decrypted[-1]
    if 1 <= pad <= 32 and decrypted.endswith(bytes([pad]) * pad):
        return decrypted[:-pad]
    return decrypted


async def download_buffer(url: str) -> dict:
    timeout = aiohttp.ClientTimeout(total=20, connect=8)
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.get(url, timeout=timeout, allow_redirects=True) as response:
            if response.status != 200:
                raise RuntimeError(f"media download failed: HTTP {response.status}")
            return {
                "data": await response.read(),
                "contentType": response.headers.get("Content-Type", ""),
            }


async def download_buffer_via_curl(url: str) -> dict:
    tmp_dir = Path(tempfile.mkdtemp(prefix="workspace-bridge-media-"))
    header_file = tmp_dir / "headers.txt"
    body_file = tmp_dir / "body.bin"
    process = await asyncio.create_subprocess_exec(
        "curl",
        "-fsSL",
        "--connect-timeout",
        "8",
        "--max-time",
        "20",
        "-D",
        str(header_file),
        "-o",
        str(body_file),
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    try:
        if process.returncode != 0:
            raise RuntimeError(f"curl media download failed: {(stderr or stdout).decode('utf-8', 'ignore').strip()}")
        header_text = header_file.read_text("utf-8") if header_file.exists() else ""
        content_type = ""
        for line in header_text.splitlines():
            if line.lower().startswith("content-type:"):
                content_type = line.split(":", 1)[1].strip()
        return {
            "data": body_file.read_bytes(),
            "contentType": content_type,
        }
    finally:
        for child in tmp_dir.glob("*"):
            try:
                child.unlink()
            except Exception:
                pass
        try:
            tmp_dir.rmdir()
        except Exception:
            pass


def should_fallback_to_curl(exc: Exception) -> bool:
    message = str(exc)
    return any(token in message.lower() for token in ("timed out", "network", "socket", "refused", "unreachable", "reset"))


async def download_incoming_media(bot, message, kind: str, payload: dict) -> dict:
    url = payload.get("url") or payload.get("download_url") or payload.get("downloadUrl")
    if not url:
        raise ValueError(f"{kind} payload missing url")
    limit = MAX_INBOUND_IMAGE_SIZE if kind == "image" else MAX_INBOUND_FILE_SIZE
    try:
        response = await download_buffer(url)
    except Exception as exc:
        if not should_fallback_to_curl(exc):
            raise
        response = await download_buffer_via_curl(url)
    data = response["data"]
    if len(data) > limit:
        raise ValueError(f"{kind} too large: {len(data)} bytes")
    aes_key = payload.get("aeskey") or payload.get("aes_key")
    if aes_key:
        data = decrypt_media_buffer(data, aes_key)
        if len(data) > limit:
            raise ValueError(f"{kind} too large after decrypt: {len(data)} bytes")
    launch = prepare_session_run(bot, message.chat_key)
    target_dir = launch.runtime_context.chatfile_dir
    ext = extension_from_url(url)
    if not ext:
        guessed = mimetypes.guess_extension(response.get("contentType", "").split(";", 1)[0].strip()) or ""
        ext = guessed
    file_name = sanitize_file_name(payload.get("filename") or payload.get("name") or f"{kind}{ext}", fallback=f"{kind}{ext or '.bin'}")
    target = build_nonconflicting_file_path(target_dir, file_name)
    target.write_bytes(data)
    return {
        "kind": kind,
        "path": str(target),
        "size": len(data),
        "contentType": response.get("contentType", ""),
        "fileName": target.name,
    }


def extract_mixed_text(mixed: dict) -> str:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    parts: list[str] = []
    for item in items:
        if item.get("msgtype") == "text":
            content = str(((item.get("text") or {}).get("content")) or "").strip()
            if content:
                parts.append(content)
    return "\n".join(parts)


def extract_mixed_images(mixed: dict) -> list[dict]:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    return [item.get("image") for item in items if item.get("msgtype") == "image" and isinstance(item.get("image"), dict)]
