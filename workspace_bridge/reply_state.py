from __future__ import annotations

import copy
import time

from .models import ReplyState, WeComBotRuntime


def get_or_create_reply_state(runtime: WeComBotRuntime, req_id: str, session_id: str, chat_key: str) -> ReplyState:
    req_text = str(req_id or "").strip()
    if not req_text:
        raise ValueError("req_id required")
    state = runtime.reply_states.get(req_text)
    if state is not None and state.session_id == session_id and state.chat_key == chat_key:
        return state
    state = ReplyState(req_id=req_text, session_id=session_id, chat_key=chat_key, started_at=time.time(), last_sent_at=time.time())
    runtime.reply_states[req_text] = state
    return state


def cache_reply_payload(state: ReplyState, payload: dict, *, final: bool, payloads: list[dict] | None = None) -> None:
    if final:
        state.pending_final_payload = copy.deepcopy(payload)
        state.pending_final_payloads = [copy.deepcopy(item) for item in payloads] if payloads is not None else [copy.deepcopy(payload)]
    else:
        state.pending_stream_payload = copy.deepcopy(payload)
        state.pending_stream_payloads = [copy.deepcopy(item) for item in payloads] if payloads is not None else [copy.deepcopy(payload)]


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
