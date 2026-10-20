from __future__ import annotations

from pathlib import Path

from .models import FileSendRequest, WorkspaceRuntimeContext
from .wecom_limits import validate_media_upload


def validate_file_for_send(runtime_context: WorkspaceRuntimeContext, file_path: Path | str, *, media_type: str = "file") -> Path:
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    allowed = False
    for root in runtime_context.allowed_file_roots:
        try:
            resolved.relative_to(root.resolve())
            allowed = True
            break
        except ValueError:
            continue
    if not allowed:
        raise PermissionError("outside allowed roots")
    size = resolved.stat().st_size
    try:
        validate_media_upload(resolved.name, size, media_type, max_bytes=runtime_context.max_upload_size)
    except ValueError as exc:
        if str(exc).startswith("file too large:"):
            raise PermissionError(str(exc)) from exc
        raise
    return resolved


def create_file_send_request(
    runtime_context: WorkspaceRuntimeContext,
    *,
    session_id: str,
    chat_key: str,
    file_path: Path | str,
    media_type: str = "file",
    title: str = "",
    description: str = "",
    to_ai_assistant: dict | None = None,
) -> FileSendRequest:
    resolved = validate_file_for_send(runtime_context, file_path, media_type=media_type)
    return FileSendRequest(
        session_id=session_id,
        chat_key=chat_key,
        workspace_id=runtime_context.workspace.workspace_id,
        file_path=resolved,
        file_name=resolved.name,
        media_type=media_type,
        title=title,
        description=description,
        to_ai_assistant=to_ai_assistant,
    )
