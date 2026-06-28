from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

from .agent_backends import normalize_agent_backend
from .context import build_runtime_context, resolve_workspace_cwd
from .layout import build_workspace_ref
from .models import BotConfig, CodexLaunchSpec, SessionRecord, SourceConfig
from .provision import provision_workspace
from .workspace_lock import workspace_lock


def now_ms() -> int:
    return int(time.time() * 1000)


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
    normalized_backend = normalize_agent_backend(agent_backend)
    normalized_workspace_mode = str(workspace_mode or "").strip().lower().replace("_", "-")
    if not normalized_workspace_mode:
        normalized_workspace_mode = "personal" if normalized_backend == "claude" else "team"
    if normalized_workspace_mode not in {"team", "personal"}:
        raise ValueError(f"invalid workspace_mode: {workspace_mode}")
    return BotConfig(
        bot_id=str(bot_id).strip(),
        bot_name=str(bot_name).strip(),
        bot_secret=None,
        source=make_source_config(source_dir),
        runtime_root=Path(runtime_root).expanduser().resolve(),
        workspace_namespace=(str(workspace_namespace or bot_id).strip() or str(bot_id).strip()),
        chatfile_root=Path(chatfile_root).expanduser().resolve(),
        workspace_mode=normalized_workspace_mode,
        codex_exec_mode=(str(codex_exec_mode).strip().lower() or "host"),
        agent_backend=normalized_backend,
        agent_command=(str(agent_command).strip() or None),
        agent_run_as_user=(str(agent_run_as_user).strip() or None),
        agent_run_as_group=(str(agent_run_as_group).strip() or None),
        agent_runtime_root=(
            Path(agent_runtime_root).expanduser().resolve() if str(agent_runtime_root or "").strip() else None
        ),
        file_send_roots=tuple(Path(item).expanduser().resolve() for item in file_send_roots),
        max_upload_size=max(1, int(max_upload_size)),
        platform="wecom",
    )


def session_registry_root(runtime_root: Path | str) -> Path:
    return Path(runtime_root).expanduser().resolve() / "sessions"


def session_record_file(runtime_root: Path | str, session_id: str) -> Path:
    return session_registry_root(runtime_root) / f"{session_id}.json"


def session_codex_home_root(runtime_root: Path | str) -> Path:
    return Path(runtime_root).expanduser().resolve() / ".bridge-codex-home" / "sessions"


def template_card_state_file(runtime_root: Path | str, bot_id: str) -> Path:
    return Path(runtime_root).expanduser().resolve() / "template-card-state" / f"{bot_id}.json"


def reply_url_state_file(runtime_root: Path | str, bot_id: str) -> Path:
    return Path(runtime_root).expanduser().resolve() / "template-card-state" / f"{bot_id}.reply-urls.json"


def remove_session_codex_home(runtime_root: Path | str, session_id: str) -> None:
    root = session_codex_home_root(runtime_root) / session_id
    if root.exists():
        __import__("shutil").rmtree(root, ignore_errors=True)


def remove_session_chatfile(chatfile_root: Path | str, session_id: str) -> None:
    root = Path(chatfile_root).expanduser().resolve() / session_id
    if root.exists():
        __import__("shutil").rmtree(root, ignore_errors=True)


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


def prune_template_card_state(state: dict[str, dict], *, current_ms: int | None = None, ttl_ms: int = 72 * 60 * 60 * 1000) -> dict[str, dict]:
    return {str(task_id): dict(item) for task_id, item in state.items() if str(task_id).strip() and isinstance(item, dict)}


def load_template_card_state(runtime_root: Path | str, bot_id: str) -> dict[str, dict]:
    payload = read_json_file(template_card_state_file(runtime_root, bot_id))
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
    payload = read_json_file(reply_url_state_file(runtime_root, bot_id))
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict] = {}
    for req_id, item in payload.items():
        req_text = str(req_id or "").strip()
        if not req_text or not isinstance(item, dict):
            continue
        captured_at_ms = int(item.get("capturedAtMs") or 0)
        if captured_at_ms <= 0 or now_ms() - captured_at_ms >= 60 * 60 * 1000:
            continue
        normalized[req_text] = dict(item)
    return normalized


def store_reply_url_state(runtime_root: Path | str, bot_id: str, state: dict[str, dict]) -> None:
    filtered: dict[str, dict] = {}
    for req_id, item in state.items():
        req_text = str(req_id or "").strip()
        if not req_text or not isinstance(item, dict):
            continue
        captured_at_ms = int(item.get("capturedAtMs") or 0)
        if captured_at_ms <= 0 or now_ms() - captured_at_ms >= 60 * 60 * 1000:
            continue
        filtered[req_text] = dict(item)
    write_json_atomic(reply_url_state_file(runtime_root, bot_id), filtered)


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
        cwd_dir=Path(payload["cwdDir"]).resolve(),
        chatfile_dir=Path(payload["chatfileDir"]).resolve(),
        workfile_dir=Path(payload["workfileDir"]).resolve() if payload.get("workfileDir") else None,
        roomfile_dir=Path(payload["roomfileDir"]).resolve() if payload.get("roomfileDir") else None,
        created_at=int(payload["createdAt"]),
        updated_at=int(payload["updatedAt"]),
        thread_id=None,
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
            "cwdDir": str(session.cwd_dir),
            "chatfileDir": str(session.chatfile_dir),
            "workfileDir": str(session.workfile_dir) if session.workfile_dir else None,
            "roomfileDir": str(session.roomfile_dir) if session.roomfile_dir else None,
            "createdAt": session.created_at,
            "updatedAt": session.updated_at,
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


def cleanup_orphan_session_codex_homes(runtime_root: Path | str) -> int:
    homes_root = session_codex_home_root(runtime_root)
    if not homes_root.exists():
        return 0
    persisted = {path.stem for path in session_registry_root(runtime_root).glob("*.json")}
    removed = 0
    for child in homes_root.iterdir():
        if not child.is_dir():
            continue
        if child.name in persisted:
            continue
        __import__("shutil").rmtree(child, ignore_errors=True)
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
        if not child.is_dir():
            continue
        if child.name in active:
            continue
        record = load_session_record(runtime_root, child.name)
        if record is None:
            continue
        last_seen_ms = int(record.last_run_at or record.updated_at or record.created_at)
        if (int(current_ms) - last_seen_ms) <= int(ttl_ms):
            continue
        __import__("shutil").rmtree(child, ignore_errors=True)
        removed += 1
    return removed


def cleanup_outdated_session_artifacts(bot: BotConfig) -> int:
    removed = 0
    chatfile_root = Path(bot.chatfile_root).expanduser().resolve()
    current_records = list_session_records(bot.runtime_root, bot.bot_id)
    for record in current_records:
        expected_session_id = stable_session_id(
            bot.bot_id,
            record.chat_key,
            bot.source.source_dir,
            bot.workspace_namespace,
            bot.workspace_mode,
        )
        if record.session_id == expected_session_id:
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


def prepare_session_run(bot: BotConfig, chat_key: str) -> CodexLaunchSpec:
    workspace_ref = build_workspace_ref(
        bot.runtime_root,
        bot.workspace_namespace,
        bot.source.source_dir,
        chat_key,
        workspace_mode=bot.workspace_mode,
        agent_backend=bot.agent_backend,
    )
    with workspace_lock(workspace_ref):
        session_id = stable_session_id(
            bot.bot_id,
            chat_key,
            bot.source.source_dir,
            bot.workspace_namespace,
            bot.workspace_mode,
        )
        provisioned = provision_workspace(workspace_ref)
        runtime_context = build_runtime_context(
            workspace_ref,
            runtime_root=bot.runtime_root,
            session_id=session_id,
            chatfile_root=bot.chatfile_root,
            codex_exec_mode=bot.codex_exec_mode,
            agent_backend=bot.agent_backend,
            file_send_roots=bot.file_send_roots,
            max_upload_size=bot.max_upload_size,
        )
        current = load_session_record(bot.runtime_root, session_id)
        created_at = current.created_at if current else now_ms()
        session = SessionRecord(
            session_id=session_id,
            bot_id=bot.bot_id,
            bot_name=bot.bot_name,
            chat_key=chat_key,
            workspace_id=workspace_ref.workspace_id,
            workspace_scope=workspace_ref.scope,
            cwd_dir=runtime_context.cwd_dir,
            chatfile_dir=runtime_context.chatfile_dir,
            workfile_dir=runtime_context.workfile_dir,
            roomfile_dir=runtime_context.roomfile_dir,
            created_at=created_at,
            updated_at=now_ms(),
            thread_id=None,
            last_run_at=current.last_run_at if current else None,
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
        if bot.agent_run_as_user:
            env["WECOM_BRIDGE_AGENT_RUN_AS_USER"] = bot.agent_run_as_user
        if bot.agent_run_as_group:
            env["WECOM_BRIDGE_AGENT_RUN_AS_GROUP"] = bot.agent_run_as_group
        if bot.agent_runtime_root is not None:
            env["WECOM_BRIDGE_AGENT_RUNTIME_ROOT"] = str(bot.agent_runtime_root)
        return CodexLaunchSpec(
            session=session,
            workspace=provisioned,
            runtime_context=runtime_context,
            cwd=resolve_workspace_cwd(workspace_ref),
            env=env,
        )
