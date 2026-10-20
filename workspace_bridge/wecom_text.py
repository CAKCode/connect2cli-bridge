from __future__ import annotations

import re


# Smart-bot stream and Markdown content limits, checked against official docs
# 101031 and 101463 (updated 2026-10-10). Group-webhook limits are different.
WECOM_CONTENT_MAX_BYTES = 20480
FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})([^\r\n]*)$")


def _take_text(text: str, *, max_chars: int, max_bytes: int) -> str:
    if max_chars <= 0 or max_bytes <= 0:
        raise ValueError("message budget is too small for Markdown formatting")
    prefix = text[:max_chars].encode("utf-8")[:max_bytes].decode("utf-8", "ignore")
    if not prefix:
        raise ValueError("message budget is too small for one UTF-8 character")
    if len(prefix) == len(text):
        return text
    # Keep paragraph boundaries when they are reasonably close to the limit.
    boundary = prefix.rfind("\n\n")
    if boundary >= len(prefix) // 2 and boundary > 0:
        return prefix[:boundary + 2]
    boundary = text.rfind("\n", 0, len(prefix) + 1)
    if boundary < len(prefix) // 2:
        boundary = text.rfind(" ", 0, len(prefix) + 1)
    if boundary >= len(prefix) // 2 and boundary > 0:
        return prefix[:min(boundary + 1, len(prefix))]
    return prefix


def split_text_chunks(
    content: str, *, max_chars: int, max_bytes: int = WECOM_CONTENT_MAX_BYTES
) -> list[str]:
    remaining = str(content or "").strip()
    if not remaining:
        return [""]
    chunks = []
    while remaining:
        chunk = _take_text(remaining, max_chars=max(1, int(max_chars)), max_bytes=max_bytes)
        chunks.append(chunk)
        remaining = remaining[len(chunk):]
    return chunks


def split_markdown_chunks(
    content: str, *, max_chars: int, max_bytes: int = WECOM_CONTENT_MAX_BYTES
) -> list[str]:
    remaining = str(content or "").strip()
    limit = max(1, int(max_chars))
    if len(remaining) <= limit and len(remaining.encode("utf-8")) <= max_bytes:
        return [remaining]
    fences = [match for line in remaining.splitlines() if (match := FENCE_RE.match(line))]
    if not fences:
        return split_text_chunks(remaining, max_chars=limit, max_bytes=max_bytes)

    # Every message must close its code block; the next message reopens it with
    # the same language, preserving indentation and line breaks inside the code.
    closing_reserve = max(len(match.group(1)) for match in fences) + 1
    opening = ""
    marker = ""
    chunks = []
    while remaining:
        prefix = opening
        raw = _take_text(
            remaining,
            max_chars=limit - len(prefix) - closing_reserve,
            max_bytes=max_bytes - len(prefix.encode("utf-8")) - closing_reserve,
        )
        for line in raw.splitlines():
            match = FENCE_RE.match(line)
            if not match:
                continue
            fence, info = match.groups()
            if not marker:
                marker = fence
                opening = line + "\n"
            elif fence[0] == marker[0] and len(fence) >= len(marker) and not info.strip():
                marker = ""
                opening = ""
        closing = ("" if raw.endswith("\n") else "\n") + marker if marker else ""
        chunks.append(prefix + raw + closing)
        remaining = remaining[len(raw):]
    return chunks


def truncate_markdown(
    content: str, *, max_chars: int, suffix: str, max_bytes: int = WECOM_CONTENT_MAX_BYTES
) -> str:
    text = str(content or "").strip()
    if len(text) <= max_chars and len(text.encode("utf-8")) <= max_bytes:
        return text
    chunks = split_markdown_chunks(
        text,
        max_chars=max_chars - len(suffix),
        max_bytes=max_bytes - len(suffix.encode("utf-8")),
    )
    return chunks[0].rstrip() + suffix
