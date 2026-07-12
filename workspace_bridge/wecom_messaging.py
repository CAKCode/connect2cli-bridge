from __future__ import annotations

import os
import time

from .models import FileSendRequest, OutboundMessage, TemplateCardUpdateRequest
from .runtime import normalize_optional_int, store_reply_url_state, store_template_card_state
import aiohttp

from .wecom_protocol import build_proactive_message_payload, build_proactive_message_payloads, build_template_card_update_payload, resolve_template_card_for_delivery
from .wecom_upload import require_ack_ok, send_ws_payload_with_ack, upload_and_send_file

PROACTIVE_SEND_ACK_TIMEOUT_SEC = max(1, int(os.environ.get("PROACTIVE_SEND_ACK_TIMEOUT_SEC", "10")))


class WeComMessagingProvider:
    @staticmethod
    def _register_runtime_template_card_state(runtime, chat_key: str, template_card: dict, *, sent_at_ms: int | None = None) -> None:
        task_id = str((template_card or {}).get("task_id") or "").strip()
        if not task_id:
            return
        runtime.template_card_payloads[task_id] = dict(template_card)
        button_texts: dict[str, str] = {}
        for item in template_card.get("button_list") or []:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            text = str(item.get("text") or "").strip()
            if key and text:
                button_texts[key] = text
        if button_texts:
            runtime.template_card_button_texts[task_id] = button_texts
        else:
            runtime.template_card_button_texts.pop(task_id, None)
        runtime.template_card_delivery_meta[task_id] = {
            "taskId": task_id,
            "chatKey": chat_key,
            "templateCard": dict(template_card),
            "sentAtMs": int(sent_at_ms if sent_at_ms is not None else time.time() * 1000),
        }
        store_template_card_state(runtime.config.runtime_root, runtime.config.bot_id, runtime.template_card_delivery_meta)

    @staticmethod
    def _update_runtime_template_card_cache(runtime, task_id: str, template_card: dict) -> None:
        runtime.template_card_payloads[task_id] = dict(template_card)
        button_texts: dict[str, str] = {}
        for item in template_card.get("button_list") or []:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            text = str(item.get("text") or "").strip()
            if key and text:
                button_texts[key] = text
        if button_texts:
            runtime.template_card_button_texts[task_id] = button_texts
        else:
            runtime.template_card_button_texts.pop(task_id, None)

    def build_proactive_payloads(self, message: OutboundMessage) -> list[dict]:
        return build_proactive_message_payloads(message)

    @staticmethod
    def _build_response_url_body(message: OutboundMessage) -> dict:
        return build_proactive_message_payload(message)["body"] | {}

    async def send_via_response_url(self, runtime, *, reply_req_id: str, message: OutboundMessage) -> dict:
        state = dict(runtime.reply_urls.get(reply_req_id) or {})
        if not state:
            raise RuntimeError(f"reply response_url not found or expired: {reply_req_id}")
        response_url = str(state.get("responseUrl") or "").strip()
        if not response_url:
            raise RuntimeError(f"reply response_url not found or expired: {reply_req_id}")
        captured_at_ms = normalize_optional_int(state.get("capturedAtMs")) or 0
        if captured_at_ms <= 0 or int(__import__("time").time() * 1000) - captured_at_ms >= 60 * 60 * 1000:
            raise RuntimeError(f"reply response_url not found or expired: {reply_req_id}")
        if state.get("consumed") is True:
            raise RuntimeError(f"reply response_url not found or expired: {reply_req_id}")
        payload = self._build_response_url_body(message)
        payload.pop("chatid", None)
        payload.pop("chat_type", None)
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(response_url, json=payload) as response:
                if response.status != 200:
                    raise RuntimeError(f"response_url send failed: HTTP {response.status}")
                result = await response.json(content_type=None)
        raw_errcode = result.get("errcode", 0)
        try:
            errcode = int(raw_errcode if raw_errcode not in (None, "") else 0)
        except (TypeError, ValueError) as exc:
            raise RuntimeError("response_url send failed: invalid errcode") from exc
        if errcode != 0:
            raise RuntimeError(f"response_url send failed: {result.get('errcode')} {result.get('errmsg', '')}".strip())
        current_state = runtime.reply_urls.get(reply_req_id)
        if isinstance(current_state, dict):
            current_state["consumed"] = True
            store_reply_url_state(runtime.config.runtime_root, runtime.config.bot_id, runtime.reply_urls)
        delivered_template_card = resolve_template_card_for_delivery(message)
        if delivered_template_card:
            self._register_runtime_template_card_state(runtime, message.chat_key, delivered_template_card)
        return {"ok": True, "response": result, "deliveredTemplateCard": delivered_template_card}

    async def send_proactive_message(self, runtime, message: OutboundMessage) -> dict:
        payloads = self.build_proactive_payloads(message)
        last_response = None
        for index, payload in enumerate(payloads, start=1):
            last_response = await send_ws_payload_with_ack(runtime, payload, PROACTIVE_SEND_ACK_TIMEOUT_SEC)
            require_ack_ok(last_response, f"proactive send {index}")
        delivered_template_card = resolve_template_card_for_delivery(message)
        if delivered_template_card:
            self._register_runtime_template_card_state(
                runtime,
                message.chat_key,
                delivered_template_card,
            )
        return {
            "ok": True,
            "payloadCount": len(payloads),
            "response": last_response or {},
            "deliveredTemplateCard": delivered_template_card,
        }

    async def send_file(self, runtime, request: FileSendRequest) -> dict:
        return await upload_and_send_file(runtime, request)

    async def update_template_card(self, runtime, request: TemplateCardUpdateRequest) -> dict:
        payload = build_template_card_update_payload(request)
        response = await send_ws_payload_with_ack(runtime, payload, PROACTIVE_SEND_ACK_TIMEOUT_SEC)
        require_ack_ok(response, "template card update")
        task_id = str((request.template_card or {}).get("task_id") or "").strip()
        if task_id:
            current = dict(runtime.template_card_delivery_meta.get(task_id) or {})
            if current and str(current.get("chatKey") or "").strip():
                self._register_runtime_template_card_state(
                    runtime,
                    str(current.get("chatKey") or ""),
                    request.template_card,
                    sent_at_ms=int(current.get("sentAtMs") or 0) or None,
                )
            elif task_id in runtime.template_card_payloads:
                self._update_runtime_template_card_cache(runtime, task_id, request.template_card)
        return {"ok": True, "response": response}


__all__ = ["WeComMessagingProvider"]
