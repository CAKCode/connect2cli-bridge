from __future__ import annotations

import time

from .models import ReplyState

REPLY_IDLE_FALLBACK_SEC = 240
REPLY_MAX_AGE_FALLBACK_SEC = 540
PROACTIVE_STATUS_INTERVAL_SEC = 120


def get_or_create_reply_state(runtime, req_id: str, session_id: str, chat_key: str) -> ReplyState:
    state = runtime.reply_states.get(req_id)
    if state is not None:
        return state
    now_value = time.time()
    state = ReplyState(
        req_id=req_id,
        session_id=session_id,
        chat_key=chat_key,
        started_at=now_value,
        last_sent_at=now_value,
    )
    runtime.reply_states[req_id] = state
    return state


def cleanup_reply_state(runtime, req_id: str) -> None:
    runtime.reply_states.pop(req_id, None)


def reply_idle_too_long(state: ReplyState) -> bool:
    return (time.time() - state.last_sent_at) >= REPLY_IDLE_FALLBACK_SEC


def reply_age_too_long(state: ReplyState) -> bool:
    return (time.time() - state.started_at) >= REPLY_MAX_AGE_FALLBACK_SEC


def reply_should_use_proactive(state: ReplyState) -> bool:
    return state.proactive or reply_age_too_long(state)


def mark_reply_proactive(state: ReplyState) -> None:
    state.proactive = True


def proactive_status_due(state: ReplyState) -> bool:
    return (time.time() - state.proactive_status_sent_at) >= PROACTIVE_STATUS_INTERVAL_SEC


def mark_proactive_status_sent(state: ReplyState) -> None:
    state.proactive_status_sent_at = time.time()


def cache_reply_payload(state: ReplyState, payload: dict, *, final: bool) -> None:
    if final:
        state.pending_final_payload = dict(payload)
        state.pending_stream_payload = None
    else:
        state.pending_stream_payload = dict(payload)


def mark_reply_sent(state: ReplyState, *, final: bool) -> None:
    state.last_sent_at = time.time()
    if final:
        state.pending_final_payload = None
        state.pending_stream_payload = None
    else:
        state.pending_stream_payload = None


def iter_cached_reply_payloads(runtime):
    for req_id, state in list(runtime.reply_states.items()):
        if state.pending_final_payload is not None:
            yield req_id, state, dict(state.pending_final_payload), True
        elif state.pending_stream_payload is not None:
            yield req_id, state, dict(state.pending_stream_payload), False
