from __future__ import annotations

import os

from .models import FileSendRequest, OutboundMessage, TemplateCardUpdateRequest
from .runtime import store_reply_url_state, store_template_card_state
import aiohttp

from .wecom_protocol import build_proactive_message_payloads, build_template_card_update_payload, resolve_template_card_for_delivery
from .wecom_upload import send_ws_payload_with_ack, upload_and_send_file

PROACTIVE_SEND_ACK_TIMEOUT_SEC = max(1, int(os.environ.get("PROACTIVE_SEND_ACK_TIMEOUT_SEC", "10")))


class WeComMessagingProvider:
    @staticmethod
    def _register_runtime_template_card_state(runtime, chat_key: str, template_card: dict) -> None:
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
        runtime.template_card_delivery_meta[task_id] = {
            "taskId": task_id,
            "chatKey": chat_key,
            "templateCard": dict(template_card),
        }
        store_template_card_state(runtime.config.runtime_root, runtime.config.bot_id, runtime.template_card_delivery_meta)

    def build_proactive_payloads(self, message: OutboundMessage) -> list[dict]:
        return build_proactive_message_payloads(message)

    @staticmethod
    def _build_response_url_body(message: OutboundMessage) -> dict:
        return build_proactive_message_payloads(message)[0]["body"] | {}

    async def send_via_response_url(self, runtime, *, reply_req_id: str, message: OutboundMessage) -> dict:
        state = dict(runtime.reply_urls.get(reply_req_id) or {})
        if not state:
            raise RuntimeError(f"reply response_url not found or expired: {reply_req_id}")
        captured_at_ms = int(state.get("capturedAtMs") or 0)
        if captured_at_ms <= 0 or int(__import__("time").time() * 1000) - captured_at_ms >= 60 * 60 * 1000:
            raise RuntimeError(f"reply response_url not found or expired: {reply_req_id}")
        if state.get("consumed") is True:
            raise RuntimeError(f"reply response_url not found or expired: {reply_req_id}")
        payload = self._build_response_url_body(message)
        payload.pop("chatid", None)
        payload.pop("chat_type", None)
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(str(state["responseUrl"]), json=payload) as response:
                if response.status != 200:
                    raise RuntimeError(f"response_url send failed: HTTP {response.status}")
                result = await response.json(content_type=None)
        if int(result.get("errcode", 0)) != 0:
            raise RuntimeError(f"response_url send failed: {result.get('errcode')} {result.get('errmsg', '')}".strip())
        runtime.reply_urls[reply_req_id]["consumed"] = True
        store_reply_url_state(runtime.config.runtime_root, runtime.config.bot_id, runtime.reply_urls)
        delivered_template_card = resolve_template_card_for_delivery(message)
        if delivered_template_card:
            self._register_runtime_template_card_state(runtime, message.chat_key, delivered_template_card)
        return {"ok": True, "response": result, "deliveredTemplateCard": delivered_template_card}

    async def send_proactive_message(self, runtime, message: OutboundMessage) -> dict:
        payloads = self.build_proactive_payloads(message)
        last_response = None
        for payload in payloads:
            last_response = await send_ws_payload_with_ack(runtime, payload, PROACTIVE_SEND_ACK_TIMEOUT_SEC)
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
        return {"ok": True, "response": response}


__all__ = ["WeComMessagingProvider"]
