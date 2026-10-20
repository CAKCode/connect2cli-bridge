from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path

import aiohttp
from aiohttp import WSMsgType

from . import context as runtime_context
from .async_utils import run_blocking
from .codex_status import collect_codex_status, format_codex_status
from .execution import flush_cached_runtime_payloads, send_or_cache_runtime_payload, session_run_guards, stream_text_message_once
from .layout import build_workspace_ref
from .process_control import terminate_process_tree
from .reply_state import cleanup_reply_state
from .runtime import (
    copy_session_resume_state,
    create_new_session_run,
    current_session_id,
    list_session_records,
    load_session_record,
    path_within_root,
    prepare_session_run,
    remove_session_chatfile,
    remove_session_codex_home,
    resolve_personal_session_cwd,
    resolve_personal_skill_dir,
    update_session_record,
)
from .schedule import advance_schedule_definition_after_success, schedule_processing_root
from .schedule import schedule_definition_processing_marker_file
from .runtime import store_reply_url_state
from .wecom_protocol import (
    build_subscribe_payload,
    build_text_response_payload,
    chat_key_from_message,
    chat_key_to_user_id,
    extract_response_url,
    is_subscribe_ok,
    normalize_bridge_command_text,
    parse_template_card_event,
    parse_text_callback,
    strip_text_mentions,
)
from .wecom_upload import reject_pending_requests, resolve_pending_request, ws_send_json
from .wecom_context import callback_context, feedback_event, message_source, message_text, quoted_media, text_with_quote
from .inbound_media import download_callback_media
from .atomic_io import atomic_write_json
from .models import WeComTextMessage
from .wecom_protocol import chat_key_to_send_target

WECOM_WS = "wss://openws.work.weixin.qq.com"
RESUME_SELECTION_TTL_MS = 5 * 60 * 1000
SESSION_CONTROL_EXIT_TIMEOUT_SEC = 5.0


def _set_wecom_state(runtime, status: str | None, error: str | None = None) -> None:
    runtime.wecom_status = status
    runtime.wecom_last_error = error


def _interrupt_error_suppressed(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    return "terminated" in text or "cancelled" in text or "canceled" in text


def _session_lifecycle_lock(runtime, chat_key: str) -> asyncio.Lock:
    lock = runtime.session_lifecycle_locks.get(chat_key)
    if lock is None:
        lock = asyncio.Lock()
        runtime.session_lifecycle_locks[chat_key] = lock
    return lock


def _request_process_termination(process) -> None:
    if process is None or getattr(process, "returncode", None) is not None:
        return
    terminate = getattr(process, "terminate", None)
    if callable(terminate):
        try:
            terminate()
        except ProcessLookupError:
            pass


async def _confirm_process_exit(process, timeout_sec: float = SESSION_CONTROL_EXIT_TIMEOUT_SEC) -> None:
    if await terminate_process_tree(process, timeout_sec=timeout_sec):
        return
    raise RuntimeError("agent process did not exit; session context was not cleared")


async def _confirm_task_exit(task, timeout_sec: float = SESSION_CONTROL_EXIT_TIMEOUT_SEC) -> None:
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout_sec)
    except asyncio.CancelledError:
        return
    except asyncio.TimeoutError as exc:
        raise RuntimeError("agent task did not exit; session context was not cleared") from exc
    except Exception:
        return


def _runtime_status_session_values(runtime, chat_key: str, session_id: str | None):
    session_record = load_session_record(runtime.config.runtime_root, session_id) if session_id else None
    thread_id = str(
        runtime.session_threads.get(chat_key)
        or (session_record.thread_id if session_record is not None else "")
        or ""
    ).strip() or None
    session_cwd = session_record.cwd_dir if session_record is not None else None
    if session_cwd is not None:
        current_cwd = Path(session_cwd)
    else:
        current_cwd = build_workspace_ref(
            runtime.config.runtime_root,
            runtime.config.workspace_namespace,
            runtime.config.source.source_dir,
            chat_key,
            workspace_mode=runtime.config.workspace_mode,
            agent_backend=runtime.config.agent_backend,
        ).cwd_dir
    return session_record, thread_id, current_cwd


def build_runtime_status_text(runtime, chat_key: str, *, session_id: str | None = None) -> str:
    _session_record, thread_id, current_cwd = _runtime_status_session_values(runtime, chat_key, session_id)
    active_task = runtime.active_message_tasks.get(chat_key)
    active_schedule_task = runtime.active_schedule_tasks.get(chat_key)
    active = (
        chat_key in runtime.active_processes
        or (active_task is not None and not active_task.done())
        or (active_schedule_task is not None and not active_schedule_task.done())
        or chat_key in runtime.active_schedule_runs
    )
    pending_stream_count = 0
    for req_id, payloads in (runtime.pending_streams or {}).items():
        state = runtime.reply_states.get(req_id)
        if state is not None and state.chat_key == chat_key:
            pending_stream_count += 1
    pending_final_count = 0
    for req_id, payloads in (runtime.pending_finals or {}).items():
        state = runtime.reply_states.get(req_id)
        if state is not None and state.chat_key == chat_key:
            pending_final_count += 1
    return "\n".join(
        [
            f"chatKey: {chat_key}",
            f"sessionId: {session_id or '-'}",
            f"threadId: {thread_id or '-'}",
            f"cwd: {current_cwd}",
            f"connected: {'yes' if runtime.connected else 'no'}",
            f"running: {'yes' if active else 'no'}",
            f"pendingStreams: {pending_stream_count}",
            f"pendingFinals: {pending_final_count}",
        ]
    )


def build_runtime_codex_status_text(runtime, chat_key: str, *, session_id: str) -> str:
    _session_record, thread_id, current_cwd = _runtime_status_session_values(runtime, chat_key, session_id)
    session_home = Path(runtime.config.runtime_root) / ".bridge-codex-home" / "sessions" / session_id
    snapshot = collect_codex_status(
        default_codex_home=runtime_context.DEFAULT_CODEX_HOME,
        session_codex_home=session_home,
        directory=current_cwd,
        thread_id=thread_id,
        exec_mode=runtime.config.codex_exec_mode,
        agent_backend=runtime.config.agent_backend,
    )
    return format_codex_status(snapshot)


async def _handle_cwd_command(runtime, parsed, command_session_id: str, command_text: str) -> bool:
    if not command_text.startswith("/cwd"):
        return False
    if runtime.config.workspace_mode != "personal":
        await ws_send_json(
            runtime,
            build_text_response_payload(parsed.req_id, command_session_id, "仅 personal 模式支持 /cwd；team 模式不允许切换。", final=True),
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return True
    active_task = runtime.active_message_tasks.get(parsed.chat_key)
    active_schedule_task = runtime.active_schedule_tasks.get(parsed.chat_key)
    if (
        parsed.chat_key in runtime.active_processes
        or (active_task is not None and not active_task.done())
        or (active_schedule_task is not None and not active_schedule_task.done())
        or parsed.chat_key in runtime.active_schedule_runs
    ):
        await ws_send_json(
            runtime,
            build_text_response_payload(parsed.req_id, command_session_id, "当前有任务在运行，不能切换 cwd。请先等待完成或使用 /bridge-interrupt。", final=True),
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return True
    async with session_run_guards(runtime.config, parsed.chat_key):
        launch = await run_blocking(prepare_session_run, runtime.config, parsed.chat_key)
        current_cwd = launch.cwd.resolve()
        args = command_text.split(None, 1)
        requested = args[1].strip() if len(args) > 1 else ""
        response_text = f"Current cwd: {current_cwd}"
        if requested:
            requested_path = Path(requested).expanduser()
            resolved = (current_cwd / requested_path).resolve() if not requested_path.is_absolute() else requested_path.resolve()
            try:
                next_cwd = resolve_personal_session_cwd(runtime.config, resolved)
            except ValueError as exc:
                response_text = str(exc)
            else:
                if not path_within_root(next_cwd, runtime.config.source.source_dir):
                    response_text = f"cwd must stay within workDir: {runtime.config.source.source_dir}"
                else:
                    changed = next_cwd != current_cwd
                    if changed:
                        runtime.session_threads.pop(parsed.chat_key, None)
                        _clear_resume_selection(runtime, parsed.chat_key)
                    skill_dir = resolve_personal_skill_dir(next_cwd)
                    skill_dir.mkdir(parents=True, exist_ok=True)
                    update_session_record(
                        runtime.config.runtime_root,
                        command_session_id,
                        lambda current: replace(
                            current,
                            workspace_mode="personal",
                            cwd_dir=next_cwd,
                            skill_dir=skill_dir,
                            thread_id=None if changed else current.thread_id,
                            updated_at=int(__import__("time").time() * 1000),
                        ),
                    )
                    response_text = f"cwd changed to {next_cwd}"
    await ws_send_json(
        runtime,
        build_text_response_payload(parsed.req_id, command_session_id, response_text, final=True),
    )
    cleanup_reply_state(runtime, parsed.req_id)
    return True


def _handle_message_task(chat_key: str, task, runtime) -> None:
    runtime.message_tasks.discard(task)
    runtime.suppressed_failure_tasks.discard(task)
    was_active = runtime.active_message_tasks.get(chat_key) is task
    if runtime.active_message_tasks.get(chat_key) is task:
        runtime.active_message_tasks.pop(chat_key, None)
    try:
        task.result()
        if was_active and runtime.last_status == "message_failed":
            runtime.last_status = None
            runtime.last_error = None
    except asyncio.CancelledError:
        return
    except Exception as exc:
        runtime.last_status = "message_failed"
        runtime.last_error = str(exc)


async def _interrupt_active_schedule_run_for_chat(runtime, chat_key: str) -> None:
    active = runtime.active_schedule_runs.get(chat_key)
    schedule_task = runtime.active_schedule_tasks.get(chat_key)
    if active is None and schedule_task is None:
        return
    schedule_id = str(active[0]).strip() if active is not None and len(active) > 0 else ""
    request_id = str(active[1]).strip() if active is not None and len(active) > 1 else ""
    if request_id:
        runtime.suppressed_schedule_cancels.add((chat_key, request_id))
        runtime.terminal_schedule_cancels.add((chat_key, request_id))
    process = runtime.active_processes.get(chat_key)
    _request_process_termination(process)
    try:
        if process is not None:
            await _confirm_process_exit(process)
        await _confirm_task_exit(schedule_task)
    except Exception:
        if request_id:
            runtime.suppressed_schedule_cancels.discard((chat_key, request_id))
            runtime.terminal_schedule_cancels.discard((chat_key, request_id))
        raise
    if runtime.active_processes.get(chat_key) is process:
        runtime.active_processes.pop(chat_key, None)
    if runtime.active_schedule_tasks.get(chat_key) is schedule_task:
        runtime.active_schedule_tasks.pop(chat_key, None)
    runtime.active_schedule_runs.pop(chat_key, None)
    processing_root = schedule_processing_root(runtime.config.runtime_root)
    for path in processing_root.glob("*.json"):
        if schedule_id and path == schedule_definition_processing_marker_file(runtime.config.runtime_root, schedule_id):
            path.unlink(missing_ok=True)
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        payload_request_id = str(payload.get("request_id") or payload.get("requestId") or "").strip()
        if request_id and payload_request_id == request_id:
            path.unlink(missing_ok=True)
    if schedule_id:
        advance_schedule_definition_after_success(runtime.config.runtime_root, schedule_id)
    if request_id:
        cache_key = f"job:{request_id}"
        if runtime.pending_finals is not None:
            runtime.pending_finals.pop(cache_key, None)
        if runtime.pending_streams is not None:
            runtime.pending_streams.pop(cache_key, None)
        cleanup_reply_state(runtime, cache_key)
        runtime.suppressed_schedule_cancels.discard((chat_key, request_id))
        runtime.terminal_schedule_cancels.discard((chat_key, request_id))


async def _quiesce_session_for_context_change(runtime, chat_key: str) -> None:
    await _interrupt_active_schedule_run_for_chat(runtime, chat_key)
    process = runtime.active_processes.get(chat_key)
    active_task = runtime.active_message_tasks.get(chat_key)
    _request_process_termination(process)
    await _confirm_process_exit(process)
    if active_task is not None:
        runtime.suppressed_failure_tasks.add(active_task)
    await _confirm_task_exit(active_task)
    if runtime.active_processes.get(chat_key) is process:
        runtime.active_processes.pop(chat_key, None)
    if runtime.active_message_tasks.get(chat_key) is active_task:
        runtime.active_message_tasks.pop(chat_key, None)


def _clear_session_reply_state(runtime, chat_key: str) -> None:
    req_ids = [req_id for req_id, state in runtime.reply_states.items() if state.chat_key == chat_key]
    for req_id in req_ids:
        cleanup_reply_state(runtime, req_id)
        if runtime.pending_streams is not None:
            runtime.pending_streams.pop(req_id, None)
        if runtime.pending_finals is not None:
            runtime.pending_finals.pop(req_id, None)
    _clear_resume_selection(runtime, chat_key)


async def _start_new_session(runtime, chat_key: str) -> str:
    async with _session_lifecycle_lock(runtime, chat_key):
        await _quiesce_session_for_context_change(runtime, chat_key)

        async with session_run_guards(runtime.config, chat_key):
            previous_session_id = current_session_id(runtime.config, chat_key)
            runtime.session_threads.pop(chat_key, None)
            update_session_record(
                runtime.config.runtime_root,
                previous_session_id,
                lambda current: replace(
                    current,
                    updated_at=int(__import__("time").time() * 1000),
                    last_run_at=int(__import__("time").time() * 1000),
                ),
            )
            launch = await run_blocking(create_new_session_run, runtime.config, chat_key)
            _clear_session_reply_state(runtime, chat_key)
        return launch.session.session_id


async def _reset_session(runtime, chat_key: str) -> str:
    async with _session_lifecycle_lock(runtime, chat_key):
        await _quiesce_session_for_context_change(runtime, chat_key)

        async with session_run_guards(runtime.config, chat_key):
            session_id = current_session_id(runtime.config, chat_key)
            runtime.session_threads.pop(chat_key, None)
            await run_blocking(remove_session_codex_home, runtime.config.runtime_root, session_id)
            await run_blocking(remove_session_chatfile, runtime.config.chatfile_root, session_id)
            update_session_record(
                runtime.config.runtime_root,
                session_id,
                lambda current: replace(
                    current,
                    updated_at=int(__import__("time").time() * 1000),
                    last_run_at=int(__import__("time").time() * 1000),
                    thread_id=None,
                    recent_chat=(),
                ),
            )
            _clear_session_reply_state(runtime, chat_key)
        return session_id


def _chat_key_to_room_id(chat_key: str) -> str | None:
    text = str(chat_key or "").strip()
    if text.startswith("group-user:"):
        parts = text.split(":", 2)
        return parts[1] if len(parts) >= 2 and parts[1] else None
    if text.startswith("group:"):
        return text.split(":", 1)[1] or None
    return None


def _resume_record_is_visible(target_key: str, current_key: str) -> bool:
    if target_key == current_key:
        return True
    target_user_id = chat_key_to_user_id(target_key)
    current_user_id = chat_key_to_user_id(current_key)
    if target_user_id and current_user_id:
        return target_user_id == current_user_id
    if target_key.startswith("group:") and current_key.startswith("group:"):
        return _chat_key_to_room_id(target_key) == _chat_key_to_room_id(current_key)
    return False


def _build_resume_candidates_from_records(records, runtime, chat_key: str) -> list[dict[str, str | int]]:
    candidates: list[dict[str, str | int]] = []
    for record in records:
        thread_id = str(runtime.session_threads.get(record.chat_key) or record.thread_id or "").strip()
        if not thread_id or not _resume_record_is_visible(record.chat_key, chat_key):
            continue
        candidates.append(
            {
                "sessionId": record.session_id,
                "threadId": thread_id,
                "chatKey": record.chat_key,
                "updatedAt": record.updated_at,
                "lastRunAt": int(record.last_run_at or 0),
            }
        )
    return candidates


def _resume_selection_active(runtime, chat_key: str) -> bool:
    candidates = runtime.resume_candidates.get(chat_key) or []
    if not candidates:
        return False
    expires_at = int(runtime.resume_selection_expires_at.get(chat_key) or 0)
    if expires_at <= int(__import__("time").time() * 1000):
        runtime.resume_candidates.pop(chat_key, None)
        runtime.resume_selection_expires_at.pop(chat_key, None)
        return False
    return True


def _clear_resume_selection(runtime, chat_key: str) -> None:
    runtime.resume_candidates.pop(chat_key, None)
    runtime.resume_selection_expires_at.pop(chat_key, None)


def _build_resume_candidates_text(candidates: list[dict[str, str | int]]) -> str:
    lines = ["检测到以下可恢复会话，请回复编号或直接回复 /bridge-resume <sessionId>："]
    for idx, candidate in enumerate(candidates, start=1):
        ts = int(candidate.get("lastRunAt") or candidate.get("updatedAt") or 0)
        ts_text = __import__("time").strftime("%Y-%m-%d %H:%M:%S", __import__("time").localtime(ts / 1000)) if ts else "-"
        lines.append(f"{idx}. {candidate['sessionId']}  {ts_text}  chatKey={candidate['chatKey']}")
    lines.append("回复“取消”可退出恢复选择。")
    return "\n".join(lines)


def _select_resume_candidate(runtime, chat_key: str, token: str) -> dict[str, str | int] | None:
    candidates = runtime.resume_candidates.get(chat_key) or []
    text = str(token or "").strip()
    if text.isdigit():
        index = int(text)
        if 1 <= index <= len(candidates):
            return candidates[index - 1]
        return None
    for candidate in candidates:
        if text == str(candidate.get("sessionId") or ""):
            return candidate
    return None


async def _bind_resume_candidate(runtime, chat_key: str, candidate: dict[str, str | int]) -> str:
    source_chat_key = str(candidate.get("chatKey") or "").strip()
    if not source_chat_key:
        raise RuntimeError("selected session is no longer resumable")
    async with session_run_guards(runtime.config, chat_key, source_chat_key):
        records = await run_blocking(list_session_records, runtime.config.runtime_root, runtime.config.bot_id)
        record = next(
            (
                item for item in records if item.session_id == candidate["sessionId"]
            ),
            None,
        )
        if record is None or record.chat_key != source_chat_key:
            raise RuntimeError("selected session is no longer resumable")
        thread_id = str(runtime.session_threads.get(source_chat_key) or record.thread_id or "").strip()
        if not thread_id:
            raise RuntimeError("selected session is no longer resumable")
        launch = await run_blocking(prepare_session_run, runtime.config, chat_key)
        await run_blocking(
            copy_session_resume_state,
            runtime.config.runtime_root,
            record.session_id,
            launch.session.session_id,
        )
        runtime.session_threads[chat_key] = thread_id
        update_session_record(
            runtime.config.runtime_root,
            launch.session.session_id,
            lambda current: replace(
                current,
                updated_at=int(__import__("time").time() * 1000),
                last_run_at=int(__import__("time").time() * 1000),
                thread_id=thread_id,
                recent_chat=record.recent_chat,
            ),
        )
    _clear_resume_selection(runtime, chat_key)
    return record.session_id


async def _run_message_task(config, runtime, parsed, *, ws=None) -> None:
    try:
        await stream_text_message_once(config, runtime, parsed)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        current_task = asyncio.current_task()
        if (current_task is not None and current_task in runtime.suppressed_failure_tasks) or _interrupt_error_suppressed(exc):
            return
        session_id = current_session_id(runtime.config, parsed.chat_key)
        runtime.last_status = "message_failed"
        runtime.last_error = str(exc)
        async with session_run_guards(runtime.config, parsed.chat_key):
            update_session_record(
                runtime.config.runtime_root,
                session_id,
                lambda current: replace(
                    current,
                    updated_at=int(__import__("time").time() * 1000),
                    last_run_at=int(__import__("time").time() * 1000),
                ),
            )
        if runtime.ws is None and ws is not None:
            original_ws = runtime.ws
            runtime.ws = ws
            try:
                await send_or_cache_runtime_payload(runtime, parsed, session_id, f"执行失败: {exc}", final=True)
            finally:
                runtime.ws = original_ws
        else:
            await send_or_cache_runtime_payload(runtime, parsed, session_id, f"执行失败: {exc}", final=True)
        raise


async def _dispatch_message(config, runtime, parsed, *, ws=None) -> None:
    async with _session_lifecycle_lock(runtime, parsed.chat_key):
        active_task = runtime.active_message_tasks.get(parsed.chat_key)
        active_schedule_task = runtime.active_schedule_tasks.get(parsed.chat_key)
        if (
            (active_task is not None and not active_task.done())
            or (active_schedule_task is not None and not active_schedule_task.done())
            or parsed.chat_key in runtime.active_schedule_runs
        ):
            busy_session_id = current_session_id(runtime.config, parsed.chat_key)
            if runtime.ws is None and ws is not None:
                original_ws = runtime.ws
                runtime.ws = ws
                try:
                    await send_or_cache_runtime_payload(
                        runtime,
                        parsed,
                        busy_session_id,
                        "已有任务在运行，请稍后再试或使用 /bridge-interrupt。",
                        final=True,
                    )
                finally:
                    runtime.ws = original_ws
            else:
                await send_or_cache_runtime_payload(
                    runtime,
                    parsed,
                    busy_session_id,
                    "已有任务在运行，请稍后再试或使用 /bridge-interrupt。",
                    final=True,
                )
            return
        task = asyncio.create_task(_run_message_task(config, runtime, parsed, ws=ws))
        runtime.message_tasks.add(task)
        runtime.active_message_tasks[parsed.chat_key] = task
        task.add_done_callback(lambda completed, chat_key=parsed.chat_key: _handle_message_task(chat_key, completed, runtime))


def _runtime_has_pending_payloads(runtime) -> bool:
    return bool(runtime.pending_streams or runtime.pending_finals)


async def _flush_pending_runtime_payloads_from_inbound(runtime, source: str) -> None:
    if runtime.ws is None or not _runtime_has_pending_payloads(runtime):
        return
    try:
        await flush_cached_runtime_payloads(runtime)
    except Exception as exc:
        runtime.last_error = str(exc)


async def _runtime_download_attachments(config, runtime, frame, media):
    chat_key = chat_key_from_message(frame)
    launch = await run_blocking(prepare_session_run, runtime.config, chat_key)
    notes = []
    for kind, value in media:
        maximum = 30 * 1024 * 1024 if kind == "image" else 100 * 1024 * 1024
        try:
            path = await download_callback_media(launch.runtime_context.chatfile_dir, kind, value, max_bytes=maximum)
            notes.append(f"附件（{kind}）已保存到本地：{path}。需要分析时读取该文件。")
        except Exception as exc:
            runtime.last_error = f"attachment receive failed: {type(exc).__name__}"
            notes.append("附件接收失败，请说明需要重新发送；不能声称已读取成功。")
    return notes


async def handle_wecom_payload(config, runtime, ws, payload, handler, *, received_at=None):
    if not isinstance(payload, dict):
        return
    if resolve_pending_request(runtime, payload):
        await _flush_pending_runtime_payloads_from_inbound(runtime, "resolved_request")
        return
    headers = payload.get("headers")
    req_id = str(headers.get("req_id") or "").strip() if isinstance(headers, dict) else ""
    body = payload.get("body") if isinstance(payload.get("body"), dict) else {}
    if req_id and payload.get("cmd") in {"aibot_msg_callback", "aibot_event_callback"} and (body.get("event") or {}).get("eventtype") != "disconnected_event":
        try:
            chat_key = chat_key_from_message(payload)
            now = runtime.rate_limiter.clock()
            runtime.callback_contexts = {key: value for key, value in runtime.callback_contexts.items() if now - value["received_at"] < 86400}
            runtime.callback_contexts[req_id] = callback_context(payload, chat_key, chat_key_to_send_target(chat_key), received_at=received_at if received_at is not None else now)
        except ValueError as exc:
            runtime.last_error = str(exc)
            return
    response_url = extract_response_url(payload)
    if req_id and response_url:
        try:
            chat_key = chat_key_from_message(payload)
        except ValueError:
            chat_key = None
        if chat_key:
            runtime.reply_urls[req_id] = {
                "responseUrl": response_url,
                "chatKey": str(chat_key or ""),
                "capturedAtMs": int(__import__("time").time() * 1000),
                "consumed": False,
            }
            store_reply_url_state(runtime.config.runtime_root, runtime.config.bot_id, runtime.reply_urls)
    if payload.get("cmd") == "aibot_event_callback":
        try:
            record = feedback_event(body)
        except ValueError as exc:
            runtime.last_error = str(exc)
            return
        if record is not None:
            record = {**record, "user_id": str((body.get("from") or {}).get("userid") or ""), "received_at": int(__import__("time").time() * 1000)}
            runtime.feedback_records[record["id"]] = record
            path = runtime.config.runtime_root / ".wecom-feedback" / "feedback.json"
            atomic_write_json(path, {"records": list(runtime.feedback_records.values())[-1000:]})
            return
    if payload.get("cmd") == "aibot_msg_callback" and message_source(body) == "ai_assistant":
        try:
            frames = runtime.assistant_calls.add(payload)
        except ValueError as exc:
            runtime.last_error = str(exc)
            return
        if frames is None:
            return
        first = frames[0]
        chat_key = chat_key_from_message(first)
        context = "\n\n".join(text_with_quote(message_text(frame["body"]), frame["body"]) for frame in frames if frame["body"]["msgtype"] == "text")
        media = [(frame["body"]["msgtype"], frame["body"][frame["body"]["msgtype"]]) for frame in frames if frame["body"]["msgtype"] in {"image", "file"}]
        media.extend(item for frame in frames for item in quoted_media(frame["body"]))
        notes = await _runtime_download_attachments(config, runtime, first, media)
        if not context.strip():
            context = "本次调用只包含附件，未提供文字指令；请询问需要执行的处理目标。"
        content = "\n\n".join([context, *notes])
        parsed = WeComTextMessage(req_id=first["headers"]["req_id"], chat_key=chat_key, content=content, raw_payload=first)
        await handler(config, runtime, parsed, ws=ws)
        return
    if payload.get("cmd") == "aibot_msg_callback" and body.get("msgtype") in {"image", "file", "video", "mixed"}:
        chat_key = chat_key_from_message(payload)
        kind = body["msgtype"]
        if kind == "mixed":
            items = (body.get("mixed") or {}).get("msg_item") or []
            media = [("image", item["image"]) for item in items if isinstance(item, dict) and item.get("msgtype") == "image" and isinstance(item.get("image"), dict)]
        else:
            media = [(kind, body.get(kind) or {})]
        notes = await _runtime_download_attachments(config, runtime, payload, media)
        raw_text = message_text(body) if kind == "mixed" else ""
        if raw_text:
            content = "\n\n".join([text_with_quote(raw_text, body), *runtime.pending_attachments.pop(chat_key, []), *notes])
            await handler(config, runtime, WeComTextMessage(req_id=req_id, chat_key=chat_key, content=content, raw_payload=payload), ws=ws)
        else:
            runtime.pending_attachments.setdefault(chat_key, []).extend(notes)
            receipt = "附件接收失败，请重新发送。" if any("接收失败" in note for note in notes) else "附件已接收，可以继续提问。"
            notification = WeComTextMessage(req_id=req_id, chat_key=chat_key, content="", raw_payload=payload)
            await send_or_cache_runtime_payload(runtime, notification, current_session_id(runtime.config, chat_key), receipt, final=True)
        return
    if str(payload.get("cmd") or "").strip() == "aibot_event_callback":
        body = payload.get("body")
        body_dict = body if isinstance(body, dict) else {}
        event = body_dict.get("event")
        event_dict = event if isinstance(event, dict) else {}
        event_type = str(event_dict.get("eventtype") or "").strip()
        if event_type == "disconnected_event":
            _set_wecom_state(runtime, "websocket_disconnected_event", "bot disconnected event")
            if runtime.ws is not None and ws is not None and runtime.ws is ws:
                await ws.close()
            return
    card_event = parse_template_card_event(payload)
    if card_event is not None:
        task_id = str(card_event.task_id or "").strip()
        if task_id:
            current = dict(runtime.template_card_delivery_meta.get(task_id) or {})
            template_card = runtime.template_card_payloads.get(task_id) or current.get("templateCard") or {}
            current.update({
                "taskId": task_id,
                "chatKey": card_event.chat_key,
                "templateCard": dict(template_card),
            })
            runtime.template_card_delivery_meta[task_id] = current
        return
    parsed = parse_text_callback(payload)
    if parsed is None:
        if payload.get("errcode") is not None and is_subscribe_ok(payload) and not payload.get("cmd"):
            await _flush_pending_runtime_payloads_from_inbound(runtime, "ack_only")
        return
    text = strip_text_mentions(parsed.content, runtime.config.bot_name)
    command_text = normalize_bridge_command_text(parsed.content, runtime.config.bot_name)
    parsed = type(parsed)(req_id=parsed.req_id, chat_key=parsed.chat_key, content=text, raw_payload=parsed.raw_payload)
    command_session_id = current_session_id(runtime.config, parsed.chat_key)
    if command_text == "/bridge-status":
        bridge_status = build_runtime_status_text(
            runtime,
            parsed.chat_key,
            session_id=command_session_id,
        )
        native_status = await run_blocking(
            build_runtime_codex_status_text,
            runtime,
            parsed.chat_key,
            session_id=command_session_id,
        )
        await ws_send_json(
            runtime,
            build_text_response_payload(
                parsed.req_id,
                command_session_id,
                f"{bridge_status}\n\n{native_status}",
                final=True,
            ),
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if await _handle_cwd_command(runtime, parsed, command_session_id, command_text):
        return
    if command_text == "/bridge-resume":
        records = await run_blocking(list_session_records, runtime.config.runtime_root, runtime.config.bot_id)
        candidates = _build_resume_candidates_from_records(records, runtime, parsed.chat_key)
        if not candidates:
            await ws_send_json(runtime, build_text_response_payload(parsed.req_id, command_session_id, "没有可恢复的会话。", final=True))
            cleanup_reply_state(runtime, parsed.req_id)
            return
        runtime.resume_candidates[parsed.chat_key] = candidates
        runtime.resume_selection_expires_at[parsed.chat_key] = int(__import__("time").time() * 1000) + RESUME_SELECTION_TTL_MS
        await ws_send_json(runtime, build_text_response_payload(parsed.req_id, command_session_id, _build_resume_candidates_text(candidates), final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if command_text.startswith("/bridge-resume "):
        candidate = _select_resume_candidate(runtime, parsed.chat_key, command_text.split(None, 1)[1].strip())
        if candidate is None:
            records = await run_blocking(list_session_records, runtime.config.runtime_root, runtime.config.bot_id)
            candidates = _build_resume_candidates_from_records(records, runtime, parsed.chat_key)
            candidate = next((item for item in candidates if item["sessionId"] == command_text.split(None, 1)[1].strip()), None)
        if candidate is None:
            await ws_send_json(runtime, build_text_response_payload(parsed.req_id, command_session_id, "未找到可恢复会话。", final=True))
            cleanup_reply_state(runtime, parsed.req_id)
            return
        source_session_id = await _bind_resume_candidate(runtime, parsed.chat_key, candidate)
        await ws_send_json(
            runtime,
            build_text_response_payload(parsed.req_id, command_session_id, f"已选择会话 {source_session_id}，接下来会继续该上下文。", final=True)
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if command_text in {"/new", "/bridge-reset"}:
        is_reset = command_text == "/bridge-reset"
        try:
            response_session_id = (
                await _reset_session(runtime, parsed.chat_key)
                if is_reset
                else await _start_new_session(runtime, parsed.chat_key)
            )
        except Exception as exc:
            failure_prefix = "Failed to reset session" if is_reset else "Failed to start a new session"
            await ws_send_json(
                runtime,
                build_text_response_payload(
                    parsed.req_id,
                    command_session_id,
                    f"{failure_prefix}: {exc}",
                    final=True,
                ),
            )
            cleanup_reply_state(runtime, parsed.req_id)
            return
        await ws_send_json(
            runtime,
            build_text_response_payload(
                parsed.req_id,
                response_session_id,
                (
                    "Session reset. Your next message will start a new thread."
                    if is_reset
                    else "New session ready. Your next message will start a new thread."
                ),
                final=True,
            ),
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if command_text == "/bridge-interrupt":
        _clear_resume_selection(runtime, parsed.chat_key)
        try:
            async with _session_lifecycle_lock(runtime, parsed.chat_key):
                await _interrupt_active_schedule_run_for_chat(runtime, parsed.chat_key)
                process = runtime.active_processes.get(parsed.chat_key)
                active_task = runtime.active_message_tasks.get(parsed.chat_key)
                _request_process_termination(process)
                if active_task is not None:
                    runtime.suppressed_failure_tasks.add(active_task)
                await _confirm_process_exit(process)
                await _confirm_task_exit(active_task)
                if runtime.active_processes.get(parsed.chat_key) is process:
                    runtime.active_processes.pop(parsed.chat_key, None)
                if runtime.active_message_tasks.get(parsed.chat_key) is active_task:
                    runtime.active_message_tasks.pop(parsed.chat_key, None)
        except Exception as exc:
            await ws_send_json(
                runtime,
                build_text_response_payload(
                    parsed.req_id,
                    command_session_id,
                    f"Failed to interrupt current task: {exc}",
                    final=True,
                ),
            )
            cleanup_reply_state(runtime, parsed.req_id)
            return
        req_ids = [req_id for req_id, state in runtime.reply_states.items() if state.chat_key == parsed.chat_key]
        for req_id in req_ids:
            cleanup_reply_state(runtime, req_id)
            if runtime.pending_streams is not None:
                runtime.pending_streams.pop(req_id, None)
            if runtime.pending_finals is not None:
                runtime.pending_finals.pop(req_id, None)
        await ws_send_json(runtime, build_text_response_payload(parsed.req_id, command_session_id, "Current task interrupted.", final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if _resume_selection_active(runtime, parsed.chat_key):
        if text in {"取消", "cancel", "Cancel", "CANCEL"}:
            _clear_resume_selection(runtime, parsed.chat_key)
            await ws_send_json(runtime, build_text_response_payload(parsed.req_id, command_session_id, "已取消恢复选择。", final=True))
            cleanup_reply_state(runtime, parsed.req_id)
            return
        candidate = _select_resume_candidate(runtime, parsed.chat_key, text)
        if candidate is None:
            await ws_send_json(
                runtime,
                build_text_response_payload(parsed.req_id, command_session_id, "无效选择，请回复列表编号、sessionId，或回复“取消”。", final=True)
            )
            cleanup_reply_state(runtime, parsed.req_id)
            return
        source_session_id = await _bind_resume_candidate(runtime, parsed.chat_key, candidate)
        await ws_send_json(
            runtime,
            build_text_response_payload(parsed.req_id, command_session_id, f"已选择会话 {source_session_id}，接下来会继续该上下文。", final=True)
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return
    notes = runtime.pending_attachments.pop(parsed.chat_key, [])
    if quoted_media(body):
        notes.extend(await _runtime_download_attachments(config, runtime, payload, quoted_media(body)))
    content = "\n\n".join([text_with_quote(parsed.content, body), *notes])
    parsed = replace(parsed, content=content)
    await handler(config, runtime, parsed, ws=ws)


async def _handle_runtime_frame(config, runtime, ws, payload, received_at=None):
    try:
        await handle_wecom_payload(config, runtime, ws, payload, _dispatch_message, received_at=received_at)
    except Exception as exc:
        runtime.last_error = str(exc)


def _start_runtime_frame(config, runtime, ws, payload):
    task = asyncio.create_task(_handle_runtime_frame(config, runtime, ws, payload, runtime.rate_limiter.clock()))
    runtime.callback_tasks.add(task)
    task.add_done_callback(runtime.callback_tasks.discard)


async def _receive_runtime_frames(config, runtime, ws):
    while True:
        msg = await ws.receive()
        if msg.type == WSMsgType.TEXT:
            payload = json.loads(msg.data)
            body = payload.get("body") if isinstance(payload, dict) and isinstance(payload.get("body"), dict) else {}
            event = body.get("event") if isinstance(body.get("event"), dict) else {}
            if event.get("eventtype") == "disconnected_event":
                await handle_wecom_payload(config, runtime, ws, payload, _dispatch_message)
                return
            _start_runtime_frame(config, runtime, ws, payload)
            await asyncio.sleep(0)
            continue
        if msg.type in {WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.CLOSING}:
            runtime.connected = False
            _set_wecom_state(runtime, "websocket_closed", "bot websocket closed")
            reject_pending_requests(runtime, runtime.wecom_last_error or "bot websocket closed")
            return
        if msg.type == WSMsgType.ERROR:
            runtime.connected = False
            _set_wecom_state(runtime, "websocket_error", str(ws.exception() or "bot websocket error"))
            reject_pending_requests(runtime, runtime.wecom_last_error or "bot websocket error")
            raise RuntimeError(runtime.wecom_last_error or "bot websocket error")


async def run_wecom_runtime_once(config, runtime) -> None:
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=config.wecom_subscribe_timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(WECOM_WS) as ws:
            runtime.ws = ws
            subscribe_payload = build_subscribe_payload(runtime.config)
            await ws_send_json(runtime, subscribe_payload)
            subscribe_msg = await ws.receive()
            if subscribe_msg.type != WSMsgType.TEXT:
                runtime.connected = False
                _set_wecom_state(runtime, "subscribe_failed", f"unexpected subscribe message type: {subscribe_msg.type!s}")
                raise RuntimeError(runtime.wecom_last_error or "subscribe failed")
            subscribe_response = json.loads(subscribe_msg.data)
            if not isinstance(subscribe_response, dict):
                runtime.connected = False
                _set_wecom_state(runtime, "subscribe_failed", "unexpected subscribe payload shape")
                raise RuntimeError(runtime.wecom_last_error or "subscribe failed")
            if resolve_pending_request(runtime, subscribe_response):
                pass
            if not is_subscribe_ok(subscribe_response):
                runtime.connected = False
                _set_wecom_state(runtime, "subscribe_failed", str(subscribe_response.get("errmsg") or "subscribe failed"))
                raise RuntimeError(runtime.wecom_last_error or "subscribe failed")
            runtime.connected = True
            _set_wecom_state(runtime, "subscribe_ok", None)
            receiver = asyncio.create_task(_receive_runtime_frames(config, runtime, ws))
            try:
                await flush_cached_runtime_payloads(runtime)
                await receiver
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if runtime.wecom_status == "subscribe_ok":
                    _set_wecom_state(runtime, "websocket_error", str(exc) or "bot websocket error")
                raise
            finally:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
                runtime.connected = False
                runtime.ws = None
                reject_pending_requests(runtime, runtime.wecom_last_error or "bot websocket closed")


async def run_wecom_runtime(config, runtime) -> None:
    retry_delay_sec = 1
    while True:
        try:
            await run_wecom_runtime_once(config, runtime)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if runtime.wecom_status is None:
                _set_wecom_state(runtime, "connect_failed", str(exc))
            if runtime.wecom_status == "subscribe_failed":
                raise
            await asyncio.sleep(retry_delay_sec)
            continue
        await asyncio.sleep(retry_delay_sec)
