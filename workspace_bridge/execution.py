from __future__ import annotations

import asyncio
import os
import re
import time
from pathlib import Path
from dataclasses import replace

from .agent_backends import extract_agent_reply, extract_agent_thread_id
from .agent_runtime import (
    apply_claude_runtime_env,
    build_setpriv_prefix,
    normalize_optional_text,
    prepare_claude_runtime_root,
    resolve_posix_identity,
)
from .async_utils import run_blocking
from .prompting import build_prompt
from .reply_state import cache_reply_payload, cleanup_reply_state, get_or_create_reply_state, mark_reply_sent
from .runner import build_runner_invocation, run_invocation
from .runtime import prepare_session_run, update_session_record
from .schedule import advance_schedule_definition_after_success, schedule_done_root, schedule_failed_root, schedule_pending_root
from .messaging import get_messaging_provider
from .models import OutboundMessage
from .wecom_protocol import build_text_response_payloads
from .wecom_upload import ws_send_json

STATUS_STREAM_INTERVAL_SEC = 2
_SESSION_RUN_LOCKS: dict[str, asyncio.Lock] = {}
_RESUME_STATE_MISSING_RE = re.compile(
    r"no rollout found|no prompt provid\w* via stdin|thread [0-9a-f-]+ not found|failed to record rollout items: thread [0-9a-f-]+ not found|No conversation found with session ID:",
    re.I,
)


def extract_codex_stdout_text(stdout: str) -> str:
    return extract_agent_reply("codex", stdout)


def extract_codex_thread_id(stdout: str) -> str | None:
    return extract_agent_thread_id("codex", stdout)


def resume_state_missing(text: str) -> bool:
    return bool(_RESUME_STATE_MISSING_RE.search(str(text or "")))


def _read_execution_reply(output_file: Path, stdout_text: str, stderr_text: str) -> str:
    if output_file.exists():
        file_text = output_file.read_text(encoding="utf-8").strip()
        if file_text:
            return file_text
    return extract_codex_stdout_text(stdout_text) or stdout_text.strip() or stderr_text.strip() or "(no output)"


def _raise_for_failed_returncode(returncode: int, *, stdout_text: str, stderr_text: str) -> None:
    if int(returncode) == 0:
        return
    detail = str(stderr_text or "").strip() or extract_codex_stdout_text(stdout_text) or str(stdout_text or "").strip()
    if detail:
        raise RuntimeError(f"codex exited with status {returncode}: {detail}")
    raise RuntimeError(f"codex exited with status {returncode}")


def _touch_session_failure(runtime_root: Path | str, session_id: str) -> None:
    now_ms = int(time.time() * 1000)
    update_session_record(
        runtime_root,
        session_id,
        lambda current: replace(
            current,
            updated_at=now_ms,
            last_run_at=now_ms,
        ),
    )


def _resume_fallback_error_text(stderr_text: str, prompt_error: Exception | None = None) -> str:
    parts = [str(stderr_text or "").strip()]
    if prompt_error is not None:
        parts.append(str(prompt_error).strip())
    return "\n".join(part for part in parts if part)


def _agent_backend(config_or_bot) -> str:
    return str(getattr(config_or_bot, "agent_backend", None) or getattr(getattr(config_or_bot, "config", None), "agent_backend", None) or "codex").strip().lower() or "codex"


def _build_compat_history_prompt(bot_or_runtime, launch, message, prompt: str) -> str:
    backend = _agent_backend(bot_or_runtime)
    if backend == "codex":
        return prompt

    history_lines: list[str] = []
    session_history = []
    if hasattr(bot_or_runtime, "sessions"):
        session = getattr(bot_or_runtime, "sessions", {}).get(message.chat_key)
        if session is not None:
            session_history = list(getattr(session, "chat", []) or [])
    if not session_history and hasattr(bot_or_runtime, "active_message_tasks"):
        session_history = []
    if not session_history:
        return prompt

    for item in session_history[-12:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip()
        text = str(item.get("text") or "").strip()
        if role not in {"user", "bot"} or not text:
            continue
        label = "User" if role == "user" else "Assistant"
        history_lines.append(f"{label}: {text}")
    if not history_lines:
        return prompt

    return (
        f"{prompt}\n\n[RecentConversation]\n"
        "Use this recent bridge-local chat history for continuity when native resume is unavailable.\n"
        f"{chr(10).join(history_lines)}\n[/RecentConversation]"
    )


def _backend_supports_native_resume(backend: str) -> bool:
    return backend in {"codex", "claude"}


def _apply_claude_runtime_override(invocation, runtime_root: Path, session_id: str):
    run_as_user = normalize_optional_text(invocation.run_as_user)
    if not run_as_user:
        return invocation
    run_as_group = normalize_optional_text(invocation.run_as_group)
    uid, gid = resolve_posix_identity(run_as_user, run_as_group)
    layout = prepare_claude_runtime_root(
        runtime_root,
        session_id=session_id,
        source_claude_root=Path("/root/.claude"),
        uid=uid,
        gid=gid,
    )
    env = apply_claude_runtime_env(invocation.env, layout)
    cwd = layout["project_dir"]
    return replace(invocation, cwd=cwd, env=env)


def _read_backend_reply(backend: str, output_file: Path, stdout_text: str, stderr_text: str) -> str:
    if output_file.exists():
        file_text = output_file.read_text(encoding="utf-8").strip()
        if file_text:
            return file_text
    return extract_agent_reply(backend, stdout_text) or stdout_text.strip() or stderr_text.strip() or "(no output)"


def _read_backend_thread_id(backend: str, stdout_text: str) -> str | None:
    return extract_agent_thread_id(backend, stdout_text)


def _raise_for_backend_failed_returncode(backend: str, returncode: int, *, stdout_text: str, stderr_text: str) -> None:
    if int(returncode) == 0:
        return
    detail = str(stderr_text or "").strip() or extract_agent_reply(backend, stdout_text) or str(stdout_text or "").strip()
    if detail:
        raise RuntimeError(f"{backend} exited with status {returncode}: {detail}")
    raise RuntimeError(f"{backend} exited with status {returncode}")


def _clear_cached_runtime_payload(runtime, req_id: str, *, final: bool) -> None:
    if not req_id:
        return
    target = runtime.pending_finals if final else runtime.pending_streams
    if target is not None:
        target.pop(req_id, None)


def _store_cached_runtime_payloads(runtime, state, cache_key: str, payloads: list[dict], *, final: bool) -> None:
    if state is not None and payloads:
        cache_reply_payload(state, payloads[-1], final=final, payloads=payloads)
    target = runtime.pending_finals if final else runtime.pending_streams
    if target is not None and cache_key:
        target[cache_key] = [dict(item) for item in payloads]


def _message_cache_key(message, payload: dict) -> str:
    direct_req_id = str(message.req_id or "").strip()
    if direct_req_id:
        return direct_req_id
    raw_payload = getattr(message, "raw_payload", {}) or {}
    override = str(raw_payload.get("deliveryCacheKey") or "").strip()
    if override:
        return override
    return str(((payload.get("headers") or {}).get("req_id")) or "")


def _clear_transient_runtime_error(runtime) -> None:
    if getattr(runtime, "last_status", None) in (None, ""):
        runtime.last_error = None


def _finalize_deferred_job_delivery(runtime, cache_key: str) -> None:
    text = str(cache_key or "").strip()
    if not text.startswith("job:"):
        return
    request_id = text.split(":", 1)[1]
    if not request_id:
        return
    pending_root = schedule_pending_root(runtime.config.runtime_root)
    done_root = schedule_done_root(runtime.config.runtime_root)
    for path in sorted(pending_root.glob(f"*-{request_id}.json")):
        schedule_id = None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            schedule_id = str(payload.get("schedule_id") or payload.get("scheduleId") or "") or None
        except Exception:
            schedule_id = None
        if schedule_id:
            advance_schedule_definition_after_success(runtime.config.runtime_root, schedule_id)
            schedule_failed_root(runtime.config.runtime_root).joinpath(f"{schedule_id}.json").unlink(missing_ok=True)
        path.replace(done_root / path.name)
        if schedule_id:
            (done_root / f"{schedule_id}.json").write_text("{}", encoding="utf-8")
        return


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


def _resolve_launch_thread_id(bot_or_runtime, launch, message) -> str | None:
    if hasattr(bot_or_runtime, "session_threads"):
        thread_id = str(getattr(bot_or_runtime, "session_threads", {}).get(message.chat_key) or "").strip()
        return thread_id or None
    launch_thread_id = getattr(launch.session, "thread_id", None)
    text = str(launch_thread_id or "").strip()
    return text or None


async def send_or_cache_runtime_payload(runtime, message, session_id: str, content: str, *, final: bool) -> bool:
    payloads = (
        get_messaging_provider(runtime.config).build_proactive_payloads(
            OutboundMessage(chat_key=message.chat_key, msgtype="markdown", content=content)
        )
        if final and not message.req_id
        else build_text_response_payloads(message.req_id, session_id, content, final=final)
    )
    payload = payloads[-1]
    cache_key = _message_cache_key(message, payload)
    state = get_or_create_reply_state(runtime, cache_key, session_id, message.chat_key) if cache_key else None
    if runtime.ws is None:
        _store_cached_runtime_payloads(runtime, state, cache_key, [dict(item) for item in payloads], final=final)
        return False
    sequence = [dict(item) for item in payloads]
    try:
        for index, item in enumerate(sequence):
            await ws_send_json(runtime, item)
    except Exception as exc:
        runtime.last_error = str(exc)
        _store_cached_runtime_payloads(runtime, state, cache_key, sequence[index:], final=final)
        return False
    _clear_cached_runtime_payload(runtime, cache_key, final=final)
    if final and runtime.pending_streams is not None and cache_key:
        runtime.pending_streams.pop(cache_key, None)
    if state is not None:
        mark_reply_sent(state, final=final)
    _clear_transient_runtime_error(runtime)
    if final and cache_key:
        cleanup_reply_state(runtime, cache_key)
    return True


async def flush_cached_runtime_payloads(runtime) -> None:
    if runtime.ws is None:
        return
    for req_id, payloads in list((runtime.pending_streams or {}).items()):
        sequence = [dict(item) for item in (payloads if isinstance(payloads, list) else [payloads])]
        state = runtime.reply_states.get(req_id)
        for index, payload in enumerate(sequence):
            try:
                await ws_send_json(runtime, payload)
            except Exception as exc:
                runtime.last_error = str(exc)
                remaining = sequence[index:]
                _store_cached_runtime_payloads(runtime, state, req_id, remaining, final=False)
                raise
        if state is not None:
            mark_reply_sent(state, final=False)
        _clear_transient_runtime_error(runtime)
        if runtime.pending_streams is not None:
            runtime.pending_streams.pop(req_id, None)
    for req_id, payloads in list((runtime.pending_finals or {}).items()):
        sequence = [dict(item) for item in (payloads if isinstance(payloads, list) else [payloads])]
        state = runtime.reply_states.get(req_id)
        for index, payload in enumerate(sequence):
            try:
                await ws_send_json(runtime, payload)
            except Exception as exc:
                runtime.last_error = str(exc)
                remaining = sequence[index:]
                _store_cached_runtime_payloads(runtime, state, req_id, remaining, final=True)
                raise
        if state is not None:
            mark_reply_sent(state, final=True)
            _finalize_deferred_job_delivery(runtime, req_id)
            cleanup_reply_state(runtime, req_id)
        _clear_transient_runtime_error(runtime)
        if runtime.pending_finals is not None:
            runtime.pending_finals.pop(req_id, None)


async def run_text_message_once(config, bot, message, **kwargs):
    launch = await run_blocking(prepare_session_run, bot, message.chat_key)
    backend = _agent_backend(bot)
    prompt = build_prompt(bot, launch, message.content)
    output_file = Path(config.codex_output_root) / f"{launch.session.session_id}.jsonl"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    session_lock = _get_session_run_lock(launch.session.session_id)
    try:
        async with session_lock:
            if hasattr(bot, "active_session_ids"):
                bot.active_session_ids.add(launch.session.session_id)
            output_file.unlink(missing_ok=True)
            argv_override = kwargs.get("argv_override")
            launch_thread_id = kwargs.get("launch_thread_id_override")
            if launch_thread_id is None:
                launch_thread_id = _resolve_launch_thread_id(bot, launch, message)
            allow_fresh_fallback = True
            while True:
                use_native_resume = bool(launch_thread_id) and _backend_supports_native_resume(backend)
                effective_prompt = prompt if use_native_resume else _build_compat_history_prompt(bot, launch, message, prompt)
                invocation_override = argv_override
                attempted_resume = use_native_resume
                invocation = build_runner_invocation(
                    launch,
                    prompt=effective_prompt,
                    output_file=output_file,
                    argv_override=invocation_override,
                    resume=attempted_resume,
                    resume_thread_id=launch_thread_id if attempted_resume else None,
                )
                if backend == "claude":
                    runtime_root_override = Path(
                        launch.env.get("WECOM_BRIDGE_AGENT_RUNTIME_ROOT") or (bot.runtime_root / "claude-runtime")
                    ).expanduser().resolve()
                    invocation = _apply_claude_runtime_override(invocation, runtime_root_override, launch.session.session_id)
                result = await run_blocking(run_invocation, invocation)
                stdout_text = str(result.stdout or "")
                stderr_text = str(result.stderr or "")
                if attempted_resume and allow_fresh_fallback and resume_state_missing(
                    _resume_fallback_error_text(stderr_text)
                ):
                    launch_thread_id = None
                    allow_fresh_fallback = False
                    update_session_record(
                        bot.runtime_root,
                        launch.session.session_id,
                        lambda current: replace(
                            current,
                            updated_at=int(time.time() * 1000),
                        ),
                    )
                    output_file.unlink(missing_ok=True)
                    continue
                try:
                    _raise_for_backend_failed_returncode(
                        backend,
                        int(getattr(result, "returncode", 0) or 0),
                        stdout_text=stdout_text,
                        stderr_text=stderr_text,
                    )
                except Exception:
                    _touch_session_failure(bot.runtime_root, launch.session.session_id)
                    raise
                reply = _read_backend_reply(backend, output_file, stdout_text, stderr_text)
                next_thread_id = _read_backend_thread_id(backend, stdout_text) or launch_thread_id
                thread_state_sink = kwargs.get("thread_state_sink")
                if callable(thread_state_sink):
                    thread_state_sink(message.chat_key, next_thread_id)
                update_session_record(
                    bot.runtime_root,
                    launch.session.session_id,
                    lambda current: replace(
                        current,
                        updated_at=int(time.time() * 1000),
                        last_run_at=int(time.time() * 1000),
                    ),
                )
                break
    finally:
        if hasattr(bot, "active_session_ids"):
            getattr(bot, "active_session_ids").discard(launch.session.session_id)
        _release_session_run_lock(launch.session.session_id, session_lock)
    return launch.session.session_id, reply


async def stream_text_message_once(config, runtime, message, **kwargs):
    launch = await run_blocking(prepare_session_run, runtime.config, message.chat_key)
    backend = _agent_backend(runtime)
    prompt = build_prompt(runtime.config, launch, message.content)
    output_file = Path(config.codex_output_root) / f"{launch.session.session_id}.jsonl"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    session_lock = _get_session_run_lock(launch.session.session_id)
    try:
        async with session_lock:
            runtime.active_session_ids.add(launch.session.session_id)
            output_file.unlink(missing_ok=True)
            argv_override = kwargs.get("argv_override")
            launch_thread_id = _resolve_launch_thread_id(runtime, launch, message)
            allow_fresh_fallback = True
            emit_status_updates = bool(kwargs.get("emit_status_updates", True))
            require_final_delivery = bool(kwargs.get("require_final_delivery", False))
            while True:
                use_native_resume = bool(launch_thread_id) and _backend_supports_native_resume(backend)
                effective_prompt = prompt if use_native_resume else _build_compat_history_prompt(runtime, launch, message, prompt)
                invocation_override = argv_override
                attempted_resume = use_native_resume
                invocation = build_runner_invocation(
                    launch,
                    prompt=effective_prompt,
                    output_file=output_file,
                    argv_override=invocation_override,
                    resume=attempted_resume,
                    resume_thread_id=launch_thread_id if attempted_resume else None,
                )
                if backend == "claude":
                    runtime_root_override = Path(
                        launch.env.get("WECOM_BRIDGE_AGENT_RUNTIME_ROOT") or (runtime.config.runtime_root / "claude-runtime")
                    ).expanduser().resolve()
                    invocation = _apply_claude_runtime_override(invocation, runtime_root_override, launch.session.session_id)
                process = await asyncio.create_subprocess_exec(
                    *invocation.argv,
                    cwd=invocation.cwd,
                    env=invocation.env,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                runtime.active_processes[message.chat_key] = process
                if emit_status_updates:
                    await send_or_cache_runtime_payload(runtime, message, launch.session.session_id, "运行状态：思考中，已运行 0s。", final=False)

                async def ticker() -> None:
                    elapsed = 0
                    while True:
                        await asyncio.sleep(STATUS_STREAM_INTERVAL_SEC)
                        elapsed += STATUS_STREAM_INTERVAL_SEC
                        elapsed_display = max(1, int(elapsed))
                        await send_or_cache_runtime_payload(
                            runtime,
                            message,
                            launch.session.session_id,
                            f"运行状态：思考中，已运行 {elapsed_display}s。",
                            final=False,
                        )

                ticker_task = asyncio.create_task(ticker()) if emit_status_updates else None
                prompt_write_error: Exception | None = None
                stderr_data = b""
                try:
                    if process.stdin is not None:
                        try:
                            process.stdin.write(effective_prompt.encode("utf-8"))
                            await process.stdin.drain()
                        except Exception as exc:
                            prompt_write_error = exc
                        finally:
                            with __import__("contextlib").suppress(Exception):
                                process.stdin.close()
                    if hasattr(process, "communicate"):
                        stdout_data, stderr_data = await process.communicate()
                    else:
                        await process.wait()
                        stdout_data = await process.stdout.read() if process.stdout is not None else b""
                        stderr_data = await process.stderr.read() if process.stderr is not None else b""
                finally:
                    if getattr(process, "returncode", None) is None and hasattr(process, "terminate"):
                        with __import__("contextlib").suppress(Exception):
                            process.terminate()
                        wait_method = getattr(process, "wait", None)
                        if callable(wait_method):
                            with __import__("contextlib").suppress(Exception):
                                await wait_method()
                    runtime.active_processes.pop(message.chat_key, None)
                    if ticker_task is not None and not ticker_task.done():
                        ticker_task.cancel()
                        try:
                            await ticker_task
                        except asyncio.CancelledError:
                            pass
                text = (stdout_data or b"").decode("utf-8", "ignore").strip()
                stderr_text = (stderr_data or b"").decode("utf-8", "ignore").strip()
                if attempted_resume and allow_fresh_fallback and resume_state_missing(
                    _resume_fallback_error_text(stderr_text, prompt_write_error)
                ):
                    launch_thread_id = None
                    runtime.session_threads.pop(message.chat_key, None)
                    allow_fresh_fallback = False
                    update_session_record(
                        runtime.config.runtime_root,
                        launch.session.session_id,
                        lambda current: replace(
                            current,
                            updated_at=int(time.time() * 1000),
                        ),
                    )
                    output_file.unlink(missing_ok=True)
                    continue
                if prompt_write_error is not None:
                    _touch_session_failure(runtime.config.runtime_root, launch.session.session_id)
                    raise prompt_write_error
                try:
                    _raise_for_backend_failed_returncode(
                        backend,
                        int(getattr(process, "returncode", 0) or 0),
                        stdout_text=text,
                        stderr_text=stderr_text,
                    )
                except Exception:
                    _touch_session_failure(runtime.config.runtime_root, launch.session.session_id)
                    raise
                reply = _read_backend_reply(backend, output_file, text, stderr_text)
                next_thread_id = _read_backend_thread_id(backend, text) or launch_thread_id
                if next_thread_id:
                    runtime.session_threads[message.chat_key] = next_thread_id
                else:
                    runtime.session_threads.pop(message.chat_key, None)
                update_session_record(
                    runtime.config.runtime_root,
                    launch.session.session_id,
                    lambda current: replace(
                        current,
                        updated_at=int(time.time() * 1000),
                        last_run_at=int(time.time() * 1000),
                    ),
                )
                delivered = await send_or_cache_runtime_payload(runtime, message, launch.session.session_id, reply, final=True)
                if require_final_delivery and not delivered:
                    raise RuntimeError("final delivery deferred until connection recovers")
                break
    finally:
        runtime.active_session_ids.discard(launch.session.session_id)
        _release_session_run_lock(launch.session.session_id, session_lock)
    return launch.session.session_id, reply


async def execute_and_deliver_message(config, runtime, message, **kwargs):
    session_id, reply = await stream_text_message_once(
        config,
        runtime,
        message,
        emit_status_updates=False,
        require_final_delivery=True,
        **kwargs,
    )
    return session_id, reply
