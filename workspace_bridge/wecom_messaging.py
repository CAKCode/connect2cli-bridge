from __future__ import annotations

import os

from .models import FileSendRequest, OutboundMessage, TemplateCardUpdateRequest
from .wecom_protocol import build_proactive_message_payloads, build_template_card_update_payload
from .wecom_upload import send_ws_payload_with_ack, upload_and_send_file

PROACTIVE_SEND_ACK_TIMEOUT_SEC = max(1, int(os.environ.get("PROACTIVE_SEND_ACK_TIMEOUT_SEC", "10")))


class WeComMessagingProvider:
    def build_proactive_payloads(self, message: OutboundMessage) -> list[dict]:
        return build_proactive_message_payloads(message)

    async def send_proactive_message(self, runtime, message: OutboundMessage) -> dict:
        payloads = self.build_proactive_payloads(message)
        last_response = None
        for payload in payloads:
            last_response = await send_ws_payload_with_ack(runtime, payload, PROACTIVE_SEND_ACK_TIMEOUT_SEC)
        return {"ok": True, "payloadCount": len(payloads), "response": last_response or {}}

    async def send_file(self, runtime, request: FileSendRequest) -> dict:
        return await upload_and_send_file(runtime, request)

    async def update_template_card(self, runtime, request: TemplateCardUpdateRequest) -> dict:
        payload = build_template_card_update_payload(request)
        response = await send_ws_payload_with_ack(runtime, payload, PROACTIVE_SEND_ACK_TIMEOUT_SEC)
        return {"ok": True, "response": response}


__all__ = ["WeComMessagingProvider"]
