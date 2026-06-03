from __future__ import annotations

import time

from .models import ReplyState, WeComBotRuntime


def get_or_create_reply_state(runtime: WeComBotRuntime, req_id: str, session_id: str, chat_key: str) -> ReplyState:
    state = runtime.reply_states.get(req_id)
    if state is not None:
        return state
    state = ReplyState(req_id=req_id, session_id=session_id, chat_key=chat_key, started_at=time.time(), last_sent_at=time.time())
    runtime.reply_states[req_id] = state
    return state


def cache_reply_payload(state: ReplyState, payload: dict, *, final: bool, payloads: list[dict] | None = None) -> None:
    if final:
        state.pending_final_payload = payload
        state.pending_final_payloads = [dict(item) for item in payloads] if payloads is not None else [dict(payload)]
    else:
        state.pending_stream_payload = payload
        state.pending_stream_payloads = [dict(item) for item in payloads] if payloads is not None else [dict(payload)]


def mark_reply_sent(state: ReplyState, *, final: bool) -> None:
    state.last_sent_at = time.time()
    if final:
        state.pending_final_payload = None
        state.pending_final_payloads = None
    else:
        state.pending_stream_payload = None
        state.pending_stream_payloads = None


def cleanup_reply_state(runtime: WeComBotRuntime, req_id: str) -> None:
    runtime.reply_states.pop(req_id, None)
