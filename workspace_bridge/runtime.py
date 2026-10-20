from __future__ import annotations

import hashlib
import json
import shutil
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import cast
from urllib.parse import quote

from .agent_backends import normalize_agent_backend
from .atomic_io import atomic_write_json, ensure_state_directory
from .context import build_runtime_context, resolve_workspace_cwd
from .layout import build_workspace_ref
from .models import BotConfig, CodexLaunchSpec, SessionRecord, SourceConfig, WorkspaceMode
from .provision import provision_workspace
from .workspace_lock import workspace_lock

MAX_PERSISTED_SESSION_CHAT_ITEMS = 50
VALID_CODEX_EXEC_MODES = {"host", "sandboxed"}


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_optional_int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def stable_session_id(
    bot_id: str,
    chat_key: str,
    source_dir: Path | str,
    workspace_namespace: str | None = None,
    workspace_mode: str | None = None,
) -> str:
    source = Path(source_dir).expanduser().resolve()
    namespace = str(workspace_namespace or bot_id).strip() or str(bot_id).strip()
    mode = str(workspace_mode or "").strip().lower().replace("_", "-") or "team"
    digest = hashlib.sha1(f"{bot_id}\n{chat_key}\n{source}\n{namespace}\n{mode}".encode("utf-8")).hexdigest()[:16]
    return f"session-{digest}"


def make_source_config(source_dir: Path | str) -> SourceConfig:
    resolved = Path(source_dir).expanduser().resolve()
    if not resolved.exists():
        raise ValueError(f"source_dir does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"source_dir is not a directory: {resolved}")
    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:12]
    return SourceConfig(source_id=f"src_{digest}", source_dir=resolved)


def build_bot_config(
    *,
    bot_id: str,
    bot_name: str,
    source_dir: Path | str,
    runtime_root: Path | str,
    workspace_namespace: str | None = None,
    chatfile_root: Path | str,
    workspace_mode: str | None = None,
    codex_exec_mode: str = "host",
    agent_backend: str = "codex",
    agent_command: str | None = None,
    agent_run_as_user: str | None = None,
    agent_run_as_group: str | None = None,
    agent_runtime_root: Path | str | None = None,
    file_send_roots: tuple[Path, ...] = (),
    max_upload_size: int = 100 * 1024 * 1024,
) -> BotConfig:
    def _normalize_optional_text(value: str | None) -> str | None:
        text = str(value or "").strip()
        if not text or text.lower() in {"none", "null"}:
            return None
        return text

    normalized_bot_id = _normalize_optional_text(bot_id)
    if not normalized_bot_id:
        raise ValueError("bot_id required")
    normalized_backend = normalize_agent_backend(agent_backend)
    normalized_workspace_mode = str(workspace_mode or "").strip().lower().replace("_", "-")
    if normalized_workspace_mode in {"none", "null"}:
        normalized_workspace_mode = ""
    if not normalized_workspace_mode:
        normalized_workspace_mode = "personal" if normalized_backend == "claude" else "team"
    if normalized_workspace_mode not in {"team", "personal"}:
        raise ValueError(f"invalid workspace_mode: {workspace_mode}")
    resolved_runtime_root = Path(runtime_root).expanduser().resolve()
    if resolved_runtime_root.exists() and not resolved_runtime_root.is_dir():
        raise ValueError(f"runtime_root is not a directory: {resolved_runtime_root}")
    resolved_chatfile_root = Path(chatfile_root).expanduser().resolve()
    if resolved_chatfile_root.exists() and not resolved_chatfile_root.is_dir():
        raise ValueError(f"chatfile_root is not a directory: {resolved_chatfile_root}")
    resolved_file_send_roots = tuple(Path(item).expanduser().resolve() for item in file_send_roots)
    for root in resolved_file_send_roots:
        if root.exists() and not root.is_dir():
            raise ValueError(f"file_send_root is not a directory: {root}")
    normalized_codex_exec_mode = (_normalize_optional_text(codex_exec_mode) or "host").lower()
    if normalized_codex_exec_mode not in VALID_CODEX_EXEC_MODES:
        raise ValueError(f"invalid codex_exec_mode: {codex_exec_mode}")
    return BotConfig(
        bot_id=normalized_bot_id,
        bot_name=(_normalize_optional_text(bot_name) or "default"),
        bot_secret=None,
        source=make_source_config(source_dir),
        runtime_root=resolved_runtime_root,
        workspace_namespace=(_normalize_optional_text(workspace_namespace) or normalized_bot_id),
        chatfile_root=resolved_chatfile_root,
        workspace_mode=normalized_workspace_mode,
        codex_exec_mode=normalized_codex_exec_mode,
        agent_backend=normalized_backend,
        agent_command=_normalize_optional_text(agent_command),
        agent_run_as_user=None,
        agent_run_as_group=None,
        agent_runtime_root=None,
        file_send_roots=resolved_file_send_roots,
        max_upload_size=max(1, int(max_upload_size)),
        platform="wecom",
    )


def session_registry_root(runtime_root: Path | str) -> Path:
    return ensure_state_directory(Path(runtime_root).expanduser().resolve() / "sessions")


def session_record_file(runtime_root: Path | str, session_id: str) -> Path:
    return session_registry_root(runtime_root) / f"{session_id}.json"


def session_binding_file(runtime_root: Path | str, bot_id: str, chat_key: str) -> Path:
    root = ensure_state_directory(
        Path(runtime_root).expanduser().resolve()
        / "session-bindings"
        / quote(str(bot_id or "").strip(), safe="._-")
    )
    return root / f"{quote(str(chat_key or '').strip(), safe='._-')}.json"


def session_codex_home_root(runtime_root: Path | str) -> Path:
    codex_home_root = ensure_state_directory(Path(runtime_root).expanduser().resolve() / ".bridge-codex-home")
    return ensure_state_directory(codex_home_root / "sessions")


def _state_root(runtime_root: Path | str) -> Path:
    return ensure_state_directory(Path(runtime_root).expanduser().resolve() / "template-card-state")


def _state_file_component(bot_id: str) -> str:
    return quote(str(bot_id or "").strip(), safe="._-") or "default"


def _legacy_state_file(root: Path, bot_id: str, suffix: str) -> Path | None:
    raw_name = f"{str(bot_id or '').strip()}{suffix}"
    if not raw_name:
        return None
    candidate = root / raw_name
    try:
        candidate.resolve().relative_to(root.resolve())
    except Exception:
        return None
    return candidate


def template_card_state_file(runtime_root: Path | str, bot_id: str) -> Path:
    return _state_root(runtime_root) / f"{_state_file_component(bot_id)}.json"


def reply_url_state_file(runtime_root: Path | str, bot_id: str) -> Path:
    return _state_root(runtime_root) / f"{_state_file_component(bot_id)}.reply-urls.json"


def _remove_tree_path(path: Path) -> bool:
    if path.is_symlink():
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        return True
    if not path.exists():
        return False
    __import__("shutil").rmtree(path, ignore_errors=True)
    return not path.exists()


def remove_session_codex_home(runtime_root: Path | str, session_id: str) -> None:
    root = session_codex_home_root(runtime_root) / session_id
    _remove_tree_path(root)


def remove_session_chatfile(chatfile_root: Path | str, session_id: str) -> None:
    root = Path(chatfile_root).expanduser().resolve() / session_id
    _remove_tree_path(root)


def copy_session_resume_state(runtime_root: Path | str, source_session_id: str, target_session_id: str) -> None:
    if source_session_id == target_session_id:
        return
    homes_root = session_codex_home_root(runtime_root)
    source = homes_root / source_session_id / "sessions"
    if not source.exists() or not source.is_dir():
        return
    target = homes_root / target_session_id / "sessions"
    target.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, target, dirs_exist_ok=True, symlinks=True)


def write_json_atomic(path: Path, payload: dict) -> None:
    atomic_write_json(path, payload)


def read_json_file(path: Path) -> dict | None:
    if path.is_symlink():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def normalize_session_chat_items(items, *, limit: int = MAX_PERSISTED_SESSION_CHAT_ITEMS) -> tuple[dict[str, object], ...]:
    if not isinstance(items, (list, tuple)):
        return ()
    normalized: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        text = str(item.get("text") or item.get("content") or "").strip()
        if role not in {"user", "bot"} or not text:
            continue
        current: dict[str, object] = {"role": role, "text": text}
        source = str(item.get("from") or "").strip()
        if source:
            current["from"] = source
        try:
            timestamp = item.get("time")
            if timestamp is not None:
                current["time"] = int(timestamp)
        except (TypeError, ValueError):
            pass
        normalized.append(current)
    if len(normalized) > limit:
        normalized = normalized[-limit:]
    return tuple(normalized)


def path_within_root(path: Path | str, root: Path | str) -> bool:
    try:
        Path(path).expanduser().resolve().relative_to(Path(root).expanduser().resolve())
        return True
    except Exception:
        return False


def resolve_personal_session_cwd(bot: BotConfig, cwd: Path | str | None) -> Path:
    base_dir = bot.source.source_dir.resolve()
    if cwd is None:
        return base_dir
    resolved = Path(cwd).expanduser().resolve()
    if not resolved.exists():
        raise ValueError(f"cwd does not exist: {resolved}")
    if not resolved.is_dir():
        raise ValueError(f"cwd is not a directory: {resolved}")
    if not path_within_root(resolved, base_dir):
        raise ValueError(f"cwd must stay within workDir: {base_dir}")
    return resolved


def resolve_personal_skill_dir(cwd_dir: Path) -> Path:
    return (cwd_dir / ".codex" / "skills").resolve()


def prune_template_card_state(state: dict[str, dict], *, current_ms: int | None = None, ttl_ms: int = 72 * 60 * 60 * 1000) -> dict[str, dict]:
    normalized: dict[str, dict] = {}
    now_value = now_ms() if current_ms is None else int(current_ms)
    for task_id, item in state.items():
        task_text = str(task_id or "").strip()
        if not task_text or not isinstance(item, dict):
            continue
        current = dict(item)
        current.pop("responseCode", None)
        current.pop("responseCodeCapturedAtMs", None)
        current.pop("responseCodeConsumed", None)
        current.pop("responseCodeConsumedAtMs", None)
        sent_at_ms = normalize_optional_int(current.get("sentAtMs"))
        if sent_at_ms is not None:
            if sent_at_ms <= 0 or now_value - sent_at_ms >= int(ttl_ms):
                continue
            current["sentAtMs"] = sent_at_ms
        normalized[task_text] = current
    return normalized


def load_template_card_state(runtime_root: Path | str, bot_id: str) -> dict[str, dict]:
    path = template_card_state_file(runtime_root, bot_id)
    payload = read_json_file(path)
    if not isinstance(payload, dict) and not path.exists():
        legacy = _legacy_state_file(_state_root(runtime_root), bot_id, ".json")
        if legacy is not None and legacy != path:
            payload = read_json_file(legacy)
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict] = {}
    for task_id, item in payload.items():
        task_text = str(task_id or "").strip()
        if not task_text or not isinstance(item, dict):
            continue
        normalized[task_text] = dict(item)
    return prune_template_card_state(normalized)


def store_template_card_state(runtime_root: Path | str, bot_id: str, state: dict[str, dict]) -> None:
    write_json_atomic(template_card_state_file(runtime_root, bot_id), prune_template_card_state(state))


def load_reply_url_state(runtime_root: Path | str, bot_id: str) -> dict[str, dict]:
    path = reply_url_state_file(runtime_root, bot_id)
    payload = read_json_file(path)
    if not isinstance(payload, dict) and not path.exists():
        legacy = _legacy_state_file(_state_root(runtime_root), bot_id, ".reply-urls.json")
        if legacy is not None and legacy != path:
            payload = read_json_file(legacy)
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict] = {}
    for req_id, item in payload.items():
        req_text = str(req_id or "").strip()
        if not req_text or not isinstance(item, dict):
            continue
        response_url = str(item.get("responseUrl") or "").strip()
        if not response_url:
            continue
        captured_at_ms = normalize_optional_int(item.get("capturedAtMs")) or 0
        if captured_at_ms <= 0 or now_ms() - captured_at_ms >= 60 * 60 * 1000:
            continue
        normalized[req_text] = {**dict(item), "responseUrl": response_url}
    return normalized


def store_reply_url_state(runtime_root: Path | str, bot_id: str, state: dict[str, dict]) -> None:
    filtered: dict[str, dict] = {}
    for req_id, item in state.items():
        req_text = str(req_id or "").strip()
        if not req_text or not isinstance(item, dict):
            continue
        response_url = str(item.get("responseUrl") or "").strip()
        if not response_url:
            continue
        captured_at_ms = normalize_optional_int(item.get("capturedAtMs")) or 0
        if captured_at_ms <= 0 or now_ms() - captured_at_ms >= 60 * 60 * 1000:
            continue
        filtered[req_text] = {**dict(item), "responseUrl": response_url}
    write_json_atomic(reply_url_state_file(runtime_root, bot_id), filtered)


def load_session_record(runtime_root: Path | str, session_id: str) -> SessionRecord | None:
    payload = read_json_file(session_record_file(runtime_root, session_id))
    if not payload:
        return None
    try:
        thread_id = str(payload.get("threadId") or "").strip()
        if thread_id.lower() in {"none", "null"}:
            thread_id = ""
        return SessionRecord(
            session_id=str(payload["sessionId"]),
            bot_id=str(payload["botId"]),
            bot_name=str(payload["botName"]),
            chat_key=str(payload["chatKey"]),
            workspace_id=str(payload["workspaceId"]),
            workspace_scope=str(payload["workspaceScope"]),
            workspace_mode=cast(WorkspaceMode, str(payload.get("workspaceMode") or "team")),
            cwd_dir=Path(payload["cwdDir"]).resolve(),
            skill_dir=Path(payload.get("skillDir") or str(Path(payload["cwdDir"]) / ".codex" / "skills")).resolve(),
            chatfile_dir=Path(payload["chatfileDir"]).resolve(),
            workfile_dir=Path(payload["workfileDir"]).resolve() if payload.get("workfileDir") else None,
            roomfile_dir=Path(payload["roomfileDir"]).resolve() if payload.get("roomfileDir") else None,
            created_at=int(payload["createdAt"]),
            updated_at=int(payload["updatedAt"]),
            thread_id=(thread_id or None),
            last_run_at=int(payload["lastRunAt"]) if payload.get("lastRunAt") is not None else None,
            recent_chat=normalize_session_chat_items(payload.get("recentChat")),
        )
    except (KeyError, TypeError, ValueError):
        return None


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
            "workspaceMode": session.workspace_mode,
            "cwdDir": str(session.cwd_dir),
            "skillDir": str(session.skill_dir),
            "chatfileDir": str(session.chatfile_dir),
            "workfileDir": str(session.workfile_dir) if session.workfile_dir else None,
            "roomfileDir": str(session.roomfile_dir) if session.roomfile_dir else None,
            "createdAt": session.created_at,
            "updatedAt": session.updated_at,
            "threadId": session.thread_id,
            "lastRunAt": session.last_run_at,
            "recentChat": [dict(item) for item in session.recent_chat],
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


def current_session_id(bot: BotConfig, chat_key: str) -> str:
    fallback = stable_session_id(
        bot.bot_id,
        chat_key,
        bot.source.source_dir,
        bot.workspace_namespace,
        bot.workspace_mode,
    )
    payload = read_json_file(session_binding_file(bot.runtime_root, bot.bot_id, chat_key))
    session_id = str((payload or {}).get("sessionId") or "").strip()
    if not session_id:
        return fallback
    record = load_session_record(bot.runtime_root, session_id)
    if record is None or record.bot_id != bot.bot_id or record.chat_key != chat_key:
        return fallback
    return session_id


def bind_current_session(bot: BotConfig, chat_key: str, session_id: str) -> None:
    record = load_session_record(bot.runtime_root, session_id)
    if record is None or record.bot_id != bot.bot_id or record.chat_key != chat_key:
        raise ValueError(f"cannot bind invalid session: {session_id}")
    write_json_atomic(
        session_binding_file(bot.runtime_root, bot.bot_id, chat_key),
        {
            "sessionId": session_id,
            "botId": bot.bot_id,
            "chatKey": chat_key,
            "updatedAt": now_ms(),
        },
    )


def cleanup_orphan_session_codex_homes(runtime_root: Path | str) -> int:
    homes_root = session_codex_home_root(runtime_root)
    if not homes_root.exists():
        return 0
    persisted = {path.stem for path in session_registry_root(runtime_root).glob("*.json")}
    removed = 0
    for child in homes_root.iterdir():
        if not child.is_dir() and not child.is_symlink():
            continue
        if child.name in persisted:
            continue
        if _remove_tree_path(child):
            removed += 1
    return removed


def cleanup_stale_session_codex_homes(
    runtime_root: Path | str,
    *,
    current_ms: int,
    ttl_ms: int,
    active_session_ids: set[str] | None = None,
) -> int:
    homes_root = session_codex_home_root(runtime_root)
    if not homes_root.exists():
        return 0
    active = set(active_session_ids or set())
    removed = 0
    for child in homes_root.iterdir():
        if not child.is_dir() and not child.is_symlink():
            continue
        if child.name in active:
            continue
        raw_record = read_json_file(session_record_file(runtime_root, child.name))
        record = load_session_record(runtime_root, child.name)
        if record is None and not isinstance(raw_record, dict):
            continue
        last_seen_ms = int(
            (
                record.last_run_at or record.updated_at or record.created_at
                if record is not None
                else (
                    normalize_optional_int(raw_record.get("lastRunAt"))
                    or normalize_optional_int(raw_record.get("updatedAt"))
                    or normalize_optional_int(raw_record.get("createdAt"))
                    or 0
                )
            )
        )
        if (int(current_ms) - last_seen_ms) <= int(ttl_ms):
            continue
        if _remove_tree_path(child):
            removed += 1
    return removed


def cleanup_outdated_session_artifacts(bot: BotConfig) -> int:
    removed = 0
    chatfile_root = Path(bot.chatfile_root).expanduser().resolve()
    current_records = list_session_records(bot.runtime_root, bot.bot_id)
    for record in current_records:
        expected_workspace = build_workspace_ref(
            bot.runtime_root,
            bot.workspace_namespace,
            bot.source.source_dir,
            record.chat_key,
            workspace_mode=bot.workspace_mode,
            agent_backend=bot.agent_backend,
        )
        if record.workspace_id == expected_workspace.workspace_id and record.workspace_mode == bot.workspace_mode:
            continue
        remove_session_codex_home(bot.runtime_root, record.session_id)
        remove_session_chatfile(chatfile_root, record.session_id)
        session_record_file(bot.runtime_root, record.session_id).unlink(missing_ok=True)
        removed += 1
    return removed


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


def append_session_record_chat_message(runtime_root: Path | str, session_id: str, item: dict[str, object]) -> SessionRecord | None:
    return update_session_record(
        runtime_root,
        session_id,
        lambda current: replace(
            current,
            updated_at=now_ms(),
            recent_chat=normalize_session_chat_items([*current.recent_chat, dict(item)]),
        ),
    )


def _prepare_session_run_for_id(
    bot: BotConfig,
    chat_key: str,
    session_id: str,
    *,
    inherit_from: SessionRecord | None = None,
) -> CodexLaunchSpec:
    workspace_ref = build_workspace_ref(
        bot.runtime_root,
        bot.workspace_namespace,
        bot.source.source_dir,
        chat_key,
        workspace_mode=bot.workspace_mode,
        agent_backend=bot.agent_backend,
    )
    with workspace_lock(workspace_ref):
        provisioned = provision_workspace(workspace_ref)
        current = load_session_record(bot.runtime_root, session_id)
        resolved_cwd_dir = workspace_ref.cwd_dir
        resolved_skill_dir = workspace_ref.skill_dir
        cwd_source = current or inherit_from
        if bot.workspace_mode == "personal" and cwd_source is not None:
            resolved_cwd_dir = resolve_personal_session_cwd(bot, cwd_source.cwd_dir)
            resolved_skill_dir = resolve_personal_skill_dir(resolved_cwd_dir)
            resolved_skill_dir.mkdir(parents=True, exist_ok=True)
        runtime_context = build_runtime_context(
            workspace_ref,
            runtime_root=bot.runtime_root,
            session_id=session_id,
            chatfile_root=bot.chatfile_root,
            workspace_mode=bot.workspace_mode,
            cwd_dir=resolved_cwd_dir,
            skill_dir=resolved_skill_dir,
            codex_exec_mode=bot.codex_exec_mode,
            agent_backend=bot.agent_backend,
            file_send_roots=bot.file_send_roots,
            max_upload_size=bot.max_upload_size,
        )
        created_at = current.created_at if current else now_ms()
        session = SessionRecord(
            session_id=session_id,
            bot_id=bot.bot_id,
            bot_name=bot.bot_name,
            chat_key=chat_key,
            workspace_id=workspace_ref.workspace_id,
            workspace_scope=workspace_ref.scope,
            workspace_mode=cast(WorkspaceMode, bot.workspace_mode),
            cwd_dir=runtime_context.cwd_dir,
            skill_dir=resolved_skill_dir,
            chatfile_dir=runtime_context.chatfile_dir,
            workfile_dir=runtime_context.workfile_dir,
            roomfile_dir=runtime_context.roomfile_dir,
            created_at=created_at,
            updated_at=now_ms(),
            thread_id=current.thread_id if current else None,
            last_run_at=current.last_run_at if current else None,
            recent_chat=current.recent_chat if current else (),
        )
        store_session_record(bot.runtime_root, session)
        env = {
            **runtime_context.env,
            "WECOM_BRIDGE_BOT_ID": bot.bot_id,
            "WECOM_BRIDGE_BOT_NAME": bot.bot_name,
            "WECOM_BRIDGE_SESSION_ID": session.session_id,
            "WECOM_BRIDGE_CHAT_KEY": chat_key,
            "WECOM_BRIDGE_AGENT_BACKEND": bot.agent_backend,
        }
        if bot.agent_command:
            env["WECOM_BRIDGE_AGENT_COMMAND"] = bot.agent_command
        return CodexLaunchSpec(
            session=session,
            workspace=provisioned,
            runtime_context=runtime_context,
            cwd=runtime_context.cwd_dir,
            env=env,
        )


def prepare_session_run(bot: BotConfig, chat_key: str) -> CodexLaunchSpec:
    return _prepare_session_run_for_id(bot, chat_key, current_session_id(bot, chat_key))


def create_new_session_run(bot: BotConfig, chat_key: str) -> CodexLaunchSpec:
    previous_session_id = current_session_id(bot, chat_key)
    previous = load_session_record(bot.runtime_root, previous_session_id)
    session_id = f"session-{uuid.uuid4().hex[:16]}"
    while session_record_file(bot.runtime_root, session_id).exists():
        session_id = f"session-{uuid.uuid4().hex[:16]}"
    launch = _prepare_session_run_for_id(bot, chat_key, session_id, inherit_from=previous)
    try:
        bind_current_session(bot, chat_key, session_id)
    except Exception:
        session_record_file(bot.runtime_root, session_id).unlink(missing_ok=True)
        remove_session_codex_home(bot.runtime_root, session_id)
        remove_session_chatfile(bot.chatfile_root, session_id)
        raise
    return launch
