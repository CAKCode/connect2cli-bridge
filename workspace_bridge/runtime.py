from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

from .context import build_runtime_context, resolve_workspace_cwd
from .layout import build_workspace_ref
from .models import BotConfig, CodexLaunchSpec, SessionRecord, SourceConfig
from .provision import provision_workspace
from .workspace_lock import workspace_lock


def now_ms() -> int:
    return int(time.time() * 1000)


def stable_session_id(bot_id: str, chat_key: str) -> str:
    digest = hashlib.sha1(f"{bot_id}\n{chat_key}".encode("utf-8")).hexdigest()[:16]
    return f"session-{digest}"


def make_source_config(source_dir: Path | str) -> SourceConfig:
    resolved = Path(source_dir).expanduser().resolve()
    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:12]
    return SourceConfig(source_id=f"src_{digest}", source_dir=resolved)


def build_bot_config(
    *,
    bot_id: str,
    bot_name: str,
    source_dir: Path | str,
    runtime_root: Path | str,
    global_skill_dir: Path | str,
    chatfile_root: Path | str,
    file_send_roots: tuple[Path, ...] = (),
    max_upload_size: int = 100 * 1024 * 1024,
) -> BotConfig:
    return BotConfig(
        bot_id=str(bot_id).strip(),
        bot_name=str(bot_name).strip(),
        bot_secret=None,
        source=make_source_config(source_dir),
        runtime_root=Path(runtime_root).expanduser().resolve(),
        global_skill_dir=Path(global_skill_dir).expanduser().resolve(),
        chatfile_root=Path(chatfile_root).expanduser().resolve(),
        file_send_roots=tuple(Path(item).expanduser().resolve() for item in file_send_roots),
        max_upload_size=max(1, int(max_upload_size)),
    )


def session_registry_root(runtime_root: Path | str) -> Path:
    return Path(runtime_root).expanduser().resolve() / "sessions"


def session_record_file(runtime_root: Path | str, session_id: str) -> Path:
    return session_registry_root(runtime_root) / f"{session_id}.json"


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_json_file(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def load_session_record(runtime_root: Path | str, session_id: str) -> SessionRecord | None:
    payload = read_json_file(session_record_file(runtime_root, session_id))
    if not payload:
        return None
    return SessionRecord(
        session_id=str(payload["sessionId"]),
        bot_id=str(payload["botId"]),
        bot_name=str(payload["botName"]),
        chat_key=str(payload["chatKey"]),
        workspace_id=str(payload["workspaceId"]),
        workspace_scope=str(payload["workspaceScope"]),
        project_dir=Path(payload["projectDir"]).resolve(),
        chatfile_dir=Path(payload["chatfileDir"]).resolve(),
        workfile_dir=Path(payload["workfileDir"]).resolve() if payload.get("workfileDir") else None,
        roomfile_dir=Path(payload["roomfileDir"]).resolve() if payload.get("roomfileDir") else None,
        created_at=int(payload["createdAt"]),
        updated_at=int(payload["updatedAt"]),
        thread_id=str(payload.get("threadId") or "").strip() or None,
        last_run_at=int(payload["lastRunAt"]) if payload.get("lastRunAt") is not None else None,
    )


def store_session_record(runtime_root: Path | str, session: SessionRecord) -> SessionRecord:
    write_json_atomic(
        session_record_file(runtime_root, session.session_id),
        {
            "sessionId": session.session_id,
            "botId": session.bot_id,
            "botName": session.bot_name,
            "chatKey": session.chat_key,
            "workspaceId": session.workspace_id,
            "workspaceScope": session.workspace_scope,
            "projectDir": str(session.project_dir),
            "chatfileDir": str(session.chatfile_dir),
            "workfileDir": str(session.workfile_dir) if session.workfile_dir else None,
            "roomfileDir": str(session.roomfile_dir) if session.roomfile_dir else None,
            "createdAt": session.created_at,
            "updatedAt": session.updated_at,
            "threadId": session.thread_id,
            "lastRunAt": session.last_run_at,
        },
    )
    return session


def list_session_records(runtime_root: Path | str, bot_id: str) -> list[SessionRecord]:
    root = session_registry_root(runtime_root)
    if not root.exists():
        return []
    records: list[SessionRecord] = []
    for session_file in root.glob("*.json"):
        record = load_session_record(runtime_root, session_file.stem)
        if record is None or record.bot_id != bot_id:
            continue
        records.append(record)
    records.sort(
        key=lambda item: (
            int(item.last_run_at or 0),
            int(item.updated_at),
            int(item.created_at),
            item.session_id,
        ),
        reverse=True,
    )
    return records


def update_session_record(
    runtime_root: Path | str,
    session_id: str,
    updater,
) -> SessionRecord | None:
    current = load_session_record(runtime_root, session_id)
    if current is None:
        return None
    next_record = updater(current)
    if next_record is None:
        return current
    return store_session_record(runtime_root, next_record)


def prepare_session_run(bot: BotConfig, chat_key: str) -> CodexLaunchSpec:
    workspace_ref = build_workspace_ref(bot.runtime_root, bot.source.source_dir, chat_key)
    with workspace_lock(workspace_ref):
        provisioned = provision_workspace(workspace_ref)
        runtime_context = build_runtime_context(
            workspace_ref,
            global_skill_dir=bot.global_skill_dir,
            chatfile_root=bot.chatfile_root,
            file_send_roots=bot.file_send_roots,
            max_upload_size=bot.max_upload_size,
        )
        session_id = stable_session_id(bot.bot_id, chat_key)
        current = load_session_record(bot.runtime_root, session_id)
        created_at = current.created_at if current else now_ms()
        session = SessionRecord(
            session_id=session_id,
            bot_id=bot.bot_id,
            bot_name=bot.bot_name,
            chat_key=chat_key,
            workspace_id=workspace_ref.workspace_id,
            workspace_scope=workspace_ref.scope,
            project_dir=runtime_context.project_dir,
            chatfile_dir=runtime_context.chatfile_dir,
            workfile_dir=runtime_context.workfile_dir,
            roomfile_dir=runtime_context.roomfile_dir,
            created_at=created_at,
            updated_at=now_ms(),
            thread_id=current.thread_id if current else None,
            last_run_at=current.last_run_at if current else None,
        )
        store_session_record(bot.runtime_root, session)
        env = {
            **runtime_context.env,
            "WECOM_BRIDGE_BOT_ID": bot.bot_id,
            "WECOM_BRIDGE_BOT_NAME": bot.bot_name,
            "WECOM_BRIDGE_SESSION_ID": session.session_id,
            "WECOM_BRIDGE_CHAT_KEY": chat_key,
        }
        return CodexLaunchSpec(
            session=session,
            workspace=provisioned,
            runtime_context=runtime_context,
            cwd=resolve_workspace_cwd(workspace_ref),
            env=env,
        )
