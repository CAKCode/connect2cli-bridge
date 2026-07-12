from __future__ import annotations

import base64
import re
from pathlib import Path


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
