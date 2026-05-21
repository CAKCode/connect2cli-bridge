from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from dataclasses import replace

import aiohttp
from aiohttp import WSMsgType

from .execution import flush_cached_runtime_payloads, send_or_cache_runtime_payload, stream_text_message_once
from .reply_state import cleanup_reply_state
from .runtime import list_session_records, prepare_session_run, update_session_record
from .wecom_protocol import (
    build_proactive_text_payload,
    build_subscribe_payload,
    build_text_response_payload,
    chat_key_to_user_id,
    is_subscribe_ok,
    parse_text_callback,
    strip_text_mentions,
)
from .wecom_upload import reject_pending_requests, resolve_pending_request, ws_send_json

WECOM_WS = "wss://openws.work.weixin.qq.com"
RESUME_SELECTION_TTL_MS = 5 * 60 * 1000


def build_runtime_status_text(runtime, chat_key: str) -> str:
    active = chat_key in runtime.active_processes
    return "\n".join(
        [
            f"chatKey: {chat_key}",
            f"connected: {'yes' if runtime.connected else 'no'}",
            f"running: {'yes' if active else 'no'}",
        ]
    )


def _handle_message_task(chat_key: str, task, runtime) -> None:
    runtime.message_tasks.discard(task)
    if runtime.active_message_tasks.get(chat_key) is task:
        runtime.active_message_tasks.pop(chat_key, None)
    try:
        task.result()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        runtime.last_status = "message_failed"
        runtime.last_error = str(exc)


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


def _build_resume_candidates(runtime, chat_key: str) -> list[dict[str, str | int]]:
    candidates: list[dict[str, str | int]] = []
    for record in list_session_records(runtime.config.runtime_root, runtime.config.bot_id):
        if not record.thread_id or not _resume_record_is_visible(record.chat_key, chat_key):
            continue
        candidates.append(
            {
                "sessionId": record.session_id,
                "threadId": record.thread_id,
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


def _bind_resume_candidate(runtime, chat_key: str, candidate: dict[str, str | int]) -> str:
    record = next(
        (
            item
            for item in list_session_records(runtime.config.runtime_root, runtime.config.bot_id)
            if item.session_id == candidate["sessionId"]
        ),
        None,
    )
    if record is None or not record.thread_id:
        raise RuntimeError("selected session is no longer resumable")
    launch = prepare_session_run(runtime.config, chat_key)
    update_session_record(
        runtime.config.runtime_root,
        launch.session.session_id,
        lambda current: replace(
            current,
            updated_at=int(__import__("time").time() * 1000),
            thread_id=record.thread_id,
            last_run_at=int(__import__("time").time() * 1000),
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
        runtime.last_status = "message_failed"
        runtime.last_error = str(exc)
        if runtime.ws is None and ws is not None:
            original_ws = runtime.ws
            runtime.ws = ws
            try:
                await send_or_cache_runtime_payload(runtime, parsed, "session-error", f"执行失败: {exc}", final=True)
            finally:
                runtime.ws = original_ws
        else:
            await send_or_cache_runtime_payload(runtime, parsed, "session-error", f"执行失败: {exc}", final=True)


async def _dispatch_message(config, runtime, parsed, *, ws=None) -> None:
    active_task = runtime.active_message_tasks.get(parsed.chat_key)
    if active_task is not None and not active_task.done():
        if runtime.ws is None and ws is not None:
            original_ws = runtime.ws
            runtime.ws = ws
            try:
                await send_or_cache_runtime_payload(
                    runtime,
                    parsed,
                    "session-busy",
                    "已有任务在运行，请稍后再试或使用 /bridge-interrupt。",
                    final=True,
                )
            finally:
                runtime.ws = original_ws
        else:
            await send_or_cache_runtime_payload(
                runtime,
                parsed,
                "session-busy",
                "已有任务在运行，请稍后再试或使用 /bridge-interrupt。",
                final=True,
            )
        return
    task = asyncio.create_task(_run_message_task(config, runtime, parsed, ws=ws))
    runtime.message_tasks.add(task)
    runtime.active_message_tasks[parsed.chat_key] = task
    task.add_done_callback(lambda completed, chat_key=parsed.chat_key: _handle_message_task(chat_key, completed, runtime))


async def handle_wecom_payload(config, runtime, ws, payload, handler):
    if resolve_pending_request(runtime, payload):
        return
    parsed = parse_text_callback(payload)
    if parsed is None:
        return
    text = strip_text_mentions(parsed.content, runtime.config.bot_name)
    parsed = type(parsed)(req_id=parsed.req_id, chat_key=parsed.chat_key, content=text, raw_payload=parsed.raw_payload)
    if text == "/bridge-status":
        await ws_send_json(runtime, build_text_response_payload(parsed.req_id, "session-1", build_runtime_status_text(runtime, parsed.chat_key), final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if text == "/bridge-resume":
        candidates = _build_resume_candidates(runtime, parsed.chat_key)
        if not candidates:
            await ws_send_json(runtime, build_text_response_payload(parsed.req_id, "session-1", "没有可恢复的会话。", final=True))
            cleanup_reply_state(runtime, parsed.req_id)
            return
        runtime.resume_candidates[parsed.chat_key] = candidates
        runtime.resume_selection_expires_at[parsed.chat_key] = int(__import__("time").time() * 1000) + RESUME_SELECTION_TTL_MS
        await ws_send_json(runtime, build_text_response_payload(parsed.req_id, "session-1", _build_resume_candidates_text(candidates), final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if text.startswith("/bridge-resume "):
        candidate = _select_resume_candidate(runtime, parsed.chat_key, text.split(None, 1)[1].strip())
        if candidate is None:
            candidates = _build_resume_candidates(runtime, parsed.chat_key)
            candidate = next((item for item in candidates if item["sessionId"] == text.split(None, 1)[1].strip()), None)
        if candidate is None:
            await ws_send_json(runtime, build_text_response_payload(parsed.req_id, "session-1", "未找到可恢复会话。", final=True))
            cleanup_reply_state(runtime, parsed.req_id)
            return
        source_session_id = _bind_resume_candidate(runtime, parsed.chat_key, candidate)
        await ws_send_json(
            runtime,
            build_text_response_payload(parsed.req_id, "session-1", f"已选择会话 {source_session_id}，接下来会继续该上下文。", final=True)
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if text == "/bridge-reset":
        _clear_resume_selection(runtime, parsed.chat_key)
        process = runtime.active_processes.pop(parsed.chat_key, None)
        if process is not None:
            process.terminate()
        active_task = runtime.active_message_tasks.pop(parsed.chat_key, None)
        if active_task is not None:
            active_task.cancel()
        req_ids = [req_id for req_id, state in runtime.reply_states.items() if state.chat_key == parsed.chat_key]
        for req_id in req_ids:
            cleanup_reply_state(runtime, req_id)
            if runtime.pending_streams is not None:
                runtime.pending_streams.pop(req_id, None)
            if runtime.pending_finals is not None:
                runtime.pending_finals.pop(req_id, None)
        await ws_send_json(runtime, build_text_response_payload(parsed.req_id, "session-1", "Session reset.", final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if text == "/bridge-interrupt":
        _clear_resume_selection(runtime, parsed.chat_key)
        process = runtime.active_processes.get(parsed.chat_key)
        if process is not None:
            process.terminate()
        await ws_send_json(runtime, build_text_response_payload(parsed.req_id, "session-1", "Current task interrupted.", final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if _resume_selection_active(runtime, parsed.chat_key):
        if text in {"取消", "cancel", "Cancel", "CANCEL"}:
            _clear_resume_selection(runtime, parsed.chat_key)
            await ws_send_json(runtime, build_text_response_payload(parsed.req_id, "session-1", "已取消恢复选择。", final=True))
            cleanup_reply_state(runtime, parsed.req_id)
            return
        candidate = _select_resume_candidate(runtime, parsed.chat_key, text)
        if candidate is None:
            await ws_send_json(
                runtime,
                build_text_response_payload(parsed.req_id, "session-1", "无效选择，请回复列表编号、sessionId，或回复“取消”。", final=True)
            )
            cleanup_reply_state(runtime, parsed.req_id)
            return
        source_session_id = _bind_resume_candidate(runtime, parsed.chat_key, candidate)
        await ws_send_json(
            runtime,
            build_text_response_payload(parsed.req_id, "session-1", f"已选择会话 {source_session_id}，接下来会继续该上下文。", final=True)
        )
        cleanup_reply_state(runtime, parsed.req_id)
        return
    await handler(config, runtime, parsed, ws=ws)


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
                runtime.last_status = "subscribe_failed"
                runtime.last_error = f"unexpected subscribe message type: {subscribe_msg.type!s}"
                raise RuntimeError(runtime.last_error)
            subscribe_response = json.loads(subscribe_msg.data)
            if resolve_pending_request(runtime, subscribe_response):
                pass
            if not is_subscribe_ok(subscribe_response):
                runtime.connected = False
                runtime.last_status = "subscribe_failed"
                runtime.last_error = str(subscribe_response.get("errmsg") or "subscribe failed")
                raise RuntimeError(runtime.last_error)
            runtime.connected = True
            runtime.last_status = "subscribe_ok"
            runtime.last_error = None
            await flush_cached_runtime_payloads(runtime)
            try:
                while True:
                    msg = await ws.receive()
                    if msg.type == WSMsgType.TEXT:
                        payload = json.loads(msg.data)
                        await handle_wecom_payload(config, runtime, ws, payload, _dispatch_message)
                        continue
                    if msg.type in {WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.CLOSING}:
                        runtime.connected = False
                        runtime.last_status = "websocket_closed"
                        runtime.last_error = "bot websocket closed"
                        reject_pending_requests(runtime, runtime.last_error)
                        return
                    if msg.type == WSMsgType.ERROR:
                        runtime.connected = False
                        runtime.last_status = "websocket_error"
                        runtime.last_error = str(ws.exception() or "bot websocket error")
                        reject_pending_requests(runtime, runtime.last_error)
                        raise RuntimeError(runtime.last_error)
            finally:
                runtime.connected = False
                runtime.ws = None
                reject_pending_requests(runtime, runtime.last_error or "bot websocket closed")


async def run_wecom_runtime(config, runtime) -> None:
    retry_delay_sec = 1
    while True:
        try:
            await run_wecom_runtime_once(config, runtime)
        except asyncio.CancelledError:
            raise
        except Exception:
            if runtime.last_status == "subscribe_failed":
                raise
            await asyncio.sleep(retry_delay_sec)
            continue
        await asyncio.sleep(retry_delay_sec)
