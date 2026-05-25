from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from dataclasses import replace

from .prompting import build_prompt
from .reply_state import cache_reply_payload, cleanup_reply_state, get_or_create_reply_state, mark_reply_sent
from .runner import build_runner_invocation, run_invocation
from .runtime import prepare_session_run, update_session_record
from .wecom_protocol import (
    build_proactive_text_payload,
    build_proactive_text_payloads,
    build_text_response_payload,
    build_text_response_payloads,
)
from .wecom_upload import ws_send_json

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


def extract_codex_thread_id(stdout: str) -> str | None:
    for line in str(stdout or "").splitlines():
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if payload.get("type") == "thread.started":
            thread_id = str(payload.get("thread_id") or "").strip()
            if thread_id:
                return thread_id
    return None


def _clear_cached_runtime_payload(runtime, req_id: str, *, final: bool) -> None:
    if not req_id:
        return
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
    payloads = (
        build_proactive_text_payloads(message.chat_key, content)
        if final and not message.req_id
        else build_text_response_payloads(message.req_id, session_id, content, final=final)
    )
    payload = payloads[-1]
    cache_key = str(message.req_id or ((payload.get("headers") or {}).get("req_id")) or "")
    state = get_or_create_reply_state(runtime, cache_key, session_id, message.chat_key) if cache_key else None
    if runtime.ws is None:
        if final and message.req_id:
            payloads = build_proactive_text_payloads(message.chat_key, content)
            payload = payloads[-1]
        if state is not None:
            cache_reply_payload(state, payload, final=final, payloads=payloads)
        target = runtime.pending_finals if final else runtime.pending_streams
        if target is not None and cache_key:
            target[cache_key] = [dict(item) for item in payloads]
        return False
    try:
        for item in payloads:
            await ws_send_json(runtime, item)
    except Exception as exc:
        runtime.last_error = str(exc)
        if final and message.req_id:
            payloads = build_proactive_text_payloads(message.chat_key, content)
            payload = payloads[-1]
        if state is not None:
            cache_reply_payload(state, payload, final=final, payloads=payloads)
        target = runtime.pending_finals if final else runtime.pending_streams
        if target is not None and cache_key:
            target[cache_key] = [dict(item) for item in payloads]
        return False
    _clear_cached_runtime_payload(runtime, cache_key, final=final)
    if final and runtime.pending_streams is not None and cache_key:
        runtime.pending_streams.pop(cache_key, None)
    if state is not None:
        mark_reply_sent(state, final=final)
    if final and cache_key:
        cleanup_reply_state(runtime, cache_key)
    return True


async def flush_cached_runtime_payloads(runtime) -> None:
    if runtime.ws is None:
        return
    for req_id, payloads in list((runtime.pending_streams or {}).items()):
        sequence = payloads if isinstance(payloads, list) else [payloads]
        for payload in sequence:
            await ws_send_json(runtime, payload)
        state = runtime.reply_states.get(req_id)
        if state is not None:
            state.pending_stream_payload = None
            state.pending_stream_payloads = None
    if runtime.pending_streams is not None:
        runtime.pending_streams.clear()
    for req_id, payloads in list((runtime.pending_finals or {}).items()):
        sequence = payloads if isinstance(payloads, list) else [payloads]
        for payload in sequence:
            await ws_send_json(runtime, payload)
        state = runtime.reply_states.get(req_id)
        if state is not None:
            state.pending_final_payload = None
            state.pending_final_payloads = None
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
            argv_override = kwargs.get("argv_override")
            launch_thread_id = getattr(launch.session, "thread_id", None)
            if argv_override is None and launch_thread_id:
                argv_override = (
                    "codex",
                    "exec",
                    "resume",
                    "--skip-git-repo-check",
                    "--json",
                    "-o",
                    str(output_file),
                    launch_thread_id,
                    "-",
                )
            invocation = build_runner_invocation(launch, prompt=prompt, output_file=output_file, argv_override=argv_override)
            result = await asyncio.to_thread(run_invocation, invocation)
            if output_file.exists():
                reply = output_file.read_text(encoding="utf-8").strip()
            else:
                reply = extract_codex_stdout_text(result.stdout) or result.stdout.strip()
            next_thread_id = extract_codex_thread_id(result.stdout) or launch_thread_id
            update_session_record(
                bot.runtime_root,
                launch.session.session_id,
                lambda current: replace(
                    current,
                    updated_at=int(time.time() * 1000),
                    thread_id=next_thread_id,
                    last_run_at=int(time.time() * 1000),
                ),
            )
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
            argv_override = kwargs.get("argv_override")
            launch_thread_id = getattr(launch.session, "thread_id", None)
            if argv_override is None and launch_thread_id:
                argv_override = (
                    "codex",
                    "exec",
                    "resume",
                    "--skip-git-repo-check",
                    "--json",
                    "-o",
                    str(output_file),
                    launch_thread_id,
                    "-",
                )
            process = await asyncio.create_subprocess_exec(
                *(argv_override or ("python", "-c", "print('done')")),
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
            next_thread_id = extract_codex_thread_id(text) or launch_thread_id
            update_session_record(
                runtime.config.runtime_root,
                launch.session.session_id,
                lambda current: replace(
                    current,
                    updated_at=int(time.time() * 1000),
                    thread_id=next_thread_id,
                    last_run_at=int(time.time() * 1000),
                ),
            )
            await send_or_cache_runtime_payload(runtime, message, launch.session.session_id, reply, final=True)
    finally:
        _release_session_run_lock(launch.session.session_id, session_lock)
    return launch.session.session_id, reply


async def execute_and_deliver_message(config, runtime, message, **kwargs):
    session_id, reply = await run_text_message_once(config, runtime.config, message, **kwargs)
    await send_or_cache_runtime_payload(runtime, message, session_id, reply, final=True)
    return session_id, reply
