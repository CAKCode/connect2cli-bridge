from __future__ import annotations

from pathlib import Path

from .models import FileSendRequest, WorkspaceRuntimeContext


def validate_file_for_send(runtime_context: WorkspaceRuntimeContext, file_path: Path | str) -> Path:
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    size = resolved.stat().st_size
    if size > runtime_context.max_upload_size:
        raise PermissionError(f"file too large: {size} bytes (max {runtime_context.max_upload_size})")
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
    return resolved


def create_file_send_request(
    runtime_context: WorkspaceRuntimeContext,
    *,
    session_id: str,
    chat_key: str,
    file_path: Path | str,
) -> FileSendRequest:
    resolved = validate_file_for_send(runtime_context, file_path)
    return FileSendRequest(
        session_id=session_id,
        chat_key=chat_key,
        workspace_id=runtime_context.workspace.workspace_id,
        file_path=resolved,
        file_name=resolved.name,
    )
