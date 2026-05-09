from __future__ import annotations

import asyncio
import json
from contextlib import suppress

import aiohttp
from aiohttp import WSMsgType

from .execution import flush_cached_runtime_payloads, send_or_cache_runtime_payload, stream_text_message_once
from .reply_state import cleanup_reply_state
from .wecom_protocol import build_subscribe_payload, build_text_response_payload, is_subscribe_ok, parse_text_callback
from .wecom_upload import reject_pending_requests, resolve_pending_request

WECOM_WS = "wss://openws.work.weixin.qq.com"


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
    text = parsed.content
    if text == "/bridge-status":
        await ws.send_json(build_text_response_payload(parsed.req_id, "session-1", build_runtime_status_text(runtime, parsed.chat_key), final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if text == "/bridge-reset":
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
        await ws.send_json(build_text_response_payload(parsed.req_id, "session-1", "Session reset.", final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    if text == "/bridge-interrupt":
        process = runtime.active_processes.get(parsed.chat_key)
        if process is not None:
            process.terminate()
        await ws.send_json(build_text_response_payload(parsed.req_id, "session-1", "Current task interrupted.", final=True))
        cleanup_reply_state(runtime, parsed.req_id)
        return
    await handler(config, runtime, parsed, ws=ws)


async def run_wecom_runtime_once(config, runtime) -> None:
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=config.wecom_subscribe_timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.ws_connect(WECOM_WS) as ws:
            runtime.ws = ws
            subscribe_payload = build_subscribe_payload(runtime.config)
            await ws.send_json(subscribe_payload)
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
