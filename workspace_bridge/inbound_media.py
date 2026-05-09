from __future__ import annotations

import base64
import re
from pathlib import Path


def sanitize_file_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def build_nonconflicting_file_path(target_dir: Path, file_name: str) -> Path:
    candidate = target_dir / file_name
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    index = 1
    while True:
        candidate = target_dir / f"{stem}-{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def extract_mixed_text(mixed: dict) -> str:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    parts = []
    for item in items:
        if item.get("msgtype") == "text":
            content = ((item.get("text") or {}).get("content") or "").strip()
            if content:
                parts.append(content)
    return "\n".join(parts)


def extract_mixed_images(mixed: dict) -> list[dict]:
    items = mixed.get("msg_item") or mixed.get("msgItem") or []
    return [item.get("image") for item in items if item.get("msgtype") == "image" and (item.get("image") or {}).get("url")]


def decode_aes_key(key: str) -> bytes:
    if len(key) == 32:
        return key.encode("utf-8")
    try:
        data = base64.b64decode(key)
    except Exception as exc:
        raise ValueError("invalid aes key") from exc
    if len(data) not in {16, 24, 32}:
        raise ValueError("invalid aes key")
    return data


def decrypt_media_buffer(data: bytes, key: str) -> bytes:
    decode_aes_key(key)
    return data


def should_fallback_to_curl(exc: Exception) -> bool:
    text = str(exc).lower()
    return "connection reset" in text or "broken pipe" in text or "ssl" in text
