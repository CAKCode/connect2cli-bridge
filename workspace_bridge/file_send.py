from __future__ import annotations

from pathlib import Path

from .models import FileSendRequest, WorkspaceRuntimeContext

MAX_UPLOAD_SIZE = 100 * 1024 * 1024


def is_path_inside(file_path: Path, root_path: Path) -> bool:
    try:
        file_path.resolve().relative_to(root_path.resolve())
        return True
    except Exception:
        return False


def validate_file_for_send(runtime_context: WorkspaceRuntimeContext, file_path: Path | str) -> Path:
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"file not found: {resolved}")
    if not resolved.is_file():
        raise ValueError(f"not a regular file: {resolved}")
    size = resolved.stat().st_size
    if size > MAX_UPLOAD_SIZE:
        raise ValueError(f"file too large: {size} bytes (max {MAX_UPLOAD_SIZE})")
    if not any(is_path_inside(resolved, root) for root in runtime_context.allowed_file_roots):
        allowed = ", ".join(str(root) for root in runtime_context.allowed_file_roots)
        raise PermissionError(f"filePath is outside allowed roots: {allowed}")
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
