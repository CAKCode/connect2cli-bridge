from __future__ import annotations


def normalize_optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null"}:
        return None
    return text
