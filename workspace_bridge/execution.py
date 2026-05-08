from __future__ import annotations

import asyncio
import time
from pathlib import Path

from .config import AppConfig
from .prompting import build_prompt
from .reply_state import (
    cache_reply_payload,
    cleanup_reply_state,
    get_or_create_reply_state,
    iter_cached_reply_payloads,
    mark_proactive_status_sent,
    mark_reply_proactive,
    mark_reply_sent,
    proactive_status_due,
    reply_idle_too_long,
    reply_should_use_proactive,
)
from .runner import build_runner_invocation, run_invocation
from .runtime import prepare_session_run
from .status import build_reply_window_expired_notice, build_status_stream_content, build_thinking_status_text, build_working_status_text
from .wecom_protocol import build_proactive_text_payload, build_text_response_payload


def truncate_reply(text: str, *, limit: int = 4000) -> str:
    cleaned = str(text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    suffix = "\n...(truncated)"
    return cleaned[: max(0, limit - len(suffix))].rstrip() + suffix


def delivery_cache_key(message, session_id: str) -> str:
    req_id = str(getattr(message, "req_id", "") or "").strip()
    if req_id:
        return req_id
    return f"{getattr(message, 'chat_key', '')}:{session_id}"


def build_delivery_payload(message, session_id: str, content: str, *, final: bool) -> dict | None:
    req_id = str(getattr(message, "req_id", "") or "").strip()
    chat_key = str(getattr(message, "chat_key", "") or "").strip()
    if req_id:
        return build_text_response_payload(req_id, session_id, content, final=final)
    if final and chat_key:
        return build_proactive_text_payload(chat_key, content)
    return None


async def send_or_cache_runtime_payload(runtime, message, session_id: str, content: str, *, final: bool) -> bool:
    payload = build_delivery_payload(message, session_id, content, final=final)
    if payload is None:
        return False
    cache_key = delivery_cache_key(message, session_id)
    req_id = str(getattr(message, "req_id", "") or "").strip()
    state = get_or_create_reply_state(runtime, req_id, session_id, str(getattr(message, "chat_key", "") or "")) if req_id else None
    if state and not final and not state.proactive and reply_should_use_proactive(state):
        if runtime.ws is not None and not state.proactive_notice_sent:
            notice_payload = build_text_response_payload(state.req_id, state.session_id, build_reply_window_expired_notice(), final=True)
            try:
                await runtime.ws.send_json(notice_payload)
                state.proactive_notice_sent = True
            except Exception:
                runtime.connected = False
        mark_reply_proactive(state)
    if state and final and reply_should_use_proactive(state):
        mark_reply_proactive(state)
        payload = build_proactive_text_payload(state.chat_key, content)
    if state and state.proactive and not final:
        if not proactive_status_due(state):
            return False
        payload = build_proactive_text_payload(state.chat_key, content)
    if runtime.ws is None:
        if state:
            cache_reply_payload(state, payload, final=final)
        else:
            if final:
                runtime.pending_finals[cache_key] = payload
            else:
                runtime.pending_streams[cache_key] = payload
        return False
    try:
        await runtime.ws.send_json(payload)
        if state:
            if state.proactive and not final:
                mark_proactive_status_sent(state)
            mark_reply_sent(state, final=final)
            if final:
                cleanup_reply_state(runtime, state.req_id)
        else:
            if final:
                runtime.pending_finals.pop(cache_key, None)
            else:
                runtime.pending_streams.pop(cache_key, None)
        return True
    except Exception:
        if state:
            cache_reply_payload(state, payload, final=final)
        else:
            if final:
                runtime.pending_finals[cache_key] = payload
            else:
                runtime.pending_streams[cache_key] = payload
        runtime.connected = False
        return False


async def flush_cached_runtime_payloads(runtime) -> None:
    if runtime.ws is None:
        return
    for req_id, state, payload, final in list(iter_cached_reply_payloads(runtime)):
        try:
            await runtime.ws.send_json(payload)
            if final:
                mark_reply_sent(state, final=True)
                cleanup_reply_state(runtime, req_id)
                runtime.pending_finals.pop(req_id, None)
            else:
                mark_reply_sent(state, final=False)
                runtime.pending_streams.pop(req_id, None)
                if state.proactive:
                    mark_proactive_status_sent(state)
        except Exception:
            runtime.connected = False
            return


async def run_text_message_once(
    config: AppConfig,
    bot,
    message,
    *,
    argv_override: tuple[str, ...] | None = None,
) -> tuple[str, str]:
    launch = prepare_session_run(bot, message.chat_key)
    prompt = build_prompt(bot, launch, message.content)
    output_root = Path(config.codex_output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    output_file = output_root / f"{launch.session.session_id}.jsonl"
    invocation = build_runner_invocation(
        launch,
        prompt=prompt,
        output_file=output_file,
        argv_override=argv_override,
    )
    result = run_invocation(invocation)
    if result.returncode == 0:
        reply = result.stdout.strip() or "(no output)"
    else:
        reply = f"Codex failed ({result.returncode})\n{result.stderr.strip() or result.stdout.strip() or '(no output)'}"
    return launch.session.session_id, truncate_reply(reply)


async def stream_text_message_once(
    config: AppConfig,
    runtime,
    message,
    *,
    argv_override: tuple[str, ...] | None = None,
) -> tuple[str, str]:
    launch = prepare_session_run(runtime.config, message.chat_key)
    prompt = build_prompt(runtime.config, launch, message.content)
    output_root = Path(config.codex_output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    output_file = output_root / f"{launch.session.session_id}.jsonl"

    await send_or_cache_runtime_payload(runtime, message, launch.session.session_id, build_thinking_status_text(0), final=False)

    start = time.monotonic()
    invocation = build_runner_invocation(
        launch,
        prompt=prompt,
        output_file=output_file,
        argv_override=argv_override,
    )
    result = await asyncio.to_thread(run_invocation, invocation)

    elapsed = int(time.monotonic() - start)
    if result.returncode == 0:
        final_reply = result.stdout.strip() or "(no output)"
    else:
        final_reply = f"Codex failed ({result.returncode})\n{result.stderr.strip() or result.stdout.strip() or '(no output)'}"
    final_reply = truncate_reply(final_reply)

    await send_or_cache_runtime_payload(
        runtime,
        message,
        launch.session.session_id,
        build_status_stream_content(build_working_status_text(elapsed), final_reply[:800]),
        final=False,
    )
    return launch.session.session_id, final_reply


async def execute_and_deliver_message(
    config: AppConfig,
    runtime,
    message,
    *,
    argv_override: tuple[str, ...] | None = None,
) -> tuple[str, str]:
    session_id, final_reply = await stream_text_message_once(
        config,
        runtime,
        message,
        argv_override=argv_override,
    )
    await send_or_cache_runtime_payload(runtime, message, session_id, final_reply, final=True)
    return session_id, final_reply
