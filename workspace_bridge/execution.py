from __future__ import annotations

import asyncio
import json
from pathlib import Path

from .prompting import build_prompt
from .reply_state import cache_reply_payload, cleanup_reply_state, get_or_create_reply_state, mark_reply_sent
from .runner import build_runner_invocation, run_invocation
from .runtime import prepare_session_run
from .wecom_protocol import build_text_response_payload

STATUS_STREAM_INTERVAL_SEC = 2
_SESSION_RUN_LOCKS: dict[str, asyncio.Lock] = {}


def extract_codex_stdout_text(stdout: str) -> str:
    latest = ""
    for line in str(stdout or "").splitlines():
        try:
            payload = json.loads(line)
        except Exception:
            continue
        item = payload.get("item") or {}
        if payload.get("type") == "item.completed" and item.get("type") in {"agent_message", "agentmessage"}:
            latest = str(item.get("text") or "").strip() or latest
    return latest


def _clear_cached_runtime_payload(runtime, req_id: str, *, final: bool) -> None:
    target = runtime.pending_finals if final else runtime.pending_streams
    if target is not None:
        target.pop(req_id, None)


def _get_session_run_lock(session_id: str) -> asyncio.Lock:
    lock = _SESSION_RUN_LOCKS.get(session_id)
    if lock is None:
        lock = asyncio.Lock()
        _SESSION_RUN_LOCKS[session_id] = lock
    return lock


def _release_session_run_lock(session_id: str, lock: asyncio.Lock) -> None:
    current = _SESSION_RUN_LOCKS.get(session_id)
    if current is lock and not lock.locked():
        _SESSION_RUN_LOCKS.pop(session_id, None)


async def send_or_cache_runtime_payload(runtime, message, session_id: str, content: str, *, final: bool) -> bool:
    payload = build_text_response_payload(message.req_id, session_id, content, final=final)
    state = get_or_create_reply_state(runtime, message.req_id, session_id, message.chat_key)
    if runtime.ws is None:
        cache_reply_payload(state, payload, final=final)
        target = runtime.pending_finals if final else runtime.pending_streams
        if target is not None:
            target[message.req_id] = payload
        return False
    try:
        await runtime.ws.send_json(payload)
    except Exception as exc:
        runtime.last_error = str(exc)
        cache_reply_payload(state, payload, final=final)
        target = runtime.pending_finals if final else runtime.pending_streams
        if target is not None:
            target[message.req_id] = payload
        return False
    _clear_cached_runtime_payload(runtime, message.req_id, final=final)
    if final and runtime.pending_streams is not None:
        runtime.pending_streams.pop(message.req_id, None)
    mark_reply_sent(state, final=final)
    if final:
        cleanup_reply_state(runtime, message.req_id)
    return True


async def flush_cached_runtime_payloads(runtime) -> None:
    if runtime.ws is None:
        return
    for req_id, payload in list((runtime.pending_streams or {}).items()):
        await runtime.ws.send_json(payload)
        state = runtime.reply_states.get(req_id)
        if state is not None:
            state.pending_stream_payload = None
    if runtime.pending_streams is not None:
        runtime.pending_streams.clear()
    for req_id, payload in list((runtime.pending_finals or {}).items()):
        await runtime.ws.send_json(payload)
        state = runtime.reply_states.get(req_id)
        if state is not None:
            state.pending_final_payload = None
            cleanup_reply_state(runtime, req_id)
    if runtime.pending_finals is not None:
        runtime.pending_finals.clear()


async def run_text_message_once(config, bot, message, **kwargs):
    launch = prepare_session_run(bot, message.chat_key)
    prompt = build_prompt(bot, launch, message.content)
    output_file = Path(config.codex_output_root) / f"{launch.session.session_id}.jsonl"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    session_lock = _get_session_run_lock(launch.session.session_id)
    try:
        async with session_lock:
            output_file.unlink(missing_ok=True)
            invocation = build_runner_invocation(launch, prompt=prompt, output_file=output_file, argv_override=kwargs.get("argv_override"))
            result = await asyncio.to_thread(run_invocation, invocation)
            if output_file.exists():
                reply = output_file.read_text(encoding="utf-8").strip()
            else:
                reply = extract_codex_stdout_text(result.stdout) or result.stdout.strip()
    finally:
        _release_session_run_lock(launch.session.session_id, session_lock)
    return launch.session.session_id, reply


async def stream_text_message_once(config, runtime, message, **kwargs):
    launch = prepare_session_run(runtime.config, message.chat_key)
    prompt = build_prompt(runtime.config, launch, message.content)
    output_file = Path(config.codex_output_root) / f"{launch.session.session_id}.jsonl"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    session_lock = _get_session_run_lock(launch.session.session_id)
    try:
        async with session_lock:
            output_file.unlink(missing_ok=True)
            process = await asyncio.create_subprocess_exec(
                *(kwargs.get("argv_override") or ("python", "-c", "print('done')")),
                cwd=launch.cwd,
                env=launch.env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            runtime.active_processes[message.chat_key] = process
            if process.stdin is not None:
                process.stdin.write(prompt.encode("utf-8"))
                await process.stdin.drain()
                process.stdin.close()
            await send_or_cache_runtime_payload(runtime, message, launch.session.session_id, "运行状态：思考中，已运行 0s。", final=False)

            async def ticker() -> None:
                await asyncio.sleep(STATUS_STREAM_INTERVAL_SEC)
                await send_or_cache_runtime_payload(runtime, message, launch.session.session_id, "运行状态：思考中，已运行 1s。", final=False)

            ticker_task = asyncio.create_task(ticker())
            try:
                if hasattr(process, "communicate"):
                    stdout_data, _stderr_data = await process.communicate()
                else:
                    await process.wait()
                    stdout_data = await process.stdout.read() if process.stdout is not None else b""
            finally:
                if getattr(process, "returncode", None) is None and hasattr(process, "terminate"):
                    with __import__("contextlib").suppress(Exception):
                        process.terminate()
                    wait_method = getattr(process, "wait", None)
                    if callable(wait_method):
                        with __import__("contextlib").suppress(Exception):
                            await wait_method()
                runtime.active_processes.pop(message.chat_key, None)
                if not ticker_task.done():
                    ticker_task.cancel()
                    try:
                        await ticker_task
                    except asyncio.CancelledError:
                        pass
            text = (stdout_data or b"").decode("utf-8", "ignore").strip()
            reply = extract_codex_stdout_text(text)
            if output_file.exists():
                reply = output_file.read_text(encoding="utf-8").strip()
            reply = reply or text
            await send_or_cache_runtime_payload(runtime, message, launch.session.session_id, reply, final=True)
    finally:
        _release_session_run_lock(launch.session.session_id, session_lock)
    return launch.session.session_id, reply


async def execute_and_deliver_message(config, runtime, message, **kwargs):
    session_id, reply = await run_text_message_once(config, runtime.config, message, **kwargs)
    await send_or_cache_runtime_payload(runtime, message, session_id, reply, final=True)
    return session_id, reply
