from __future__ import annotations

from typing import Protocol

from .models import FileSendRequest, OutboundMessage, TemplateCardUpdateRequest


class MessagingProvider(Protocol):
    def build_proactive_payloads(self, message: OutboundMessage) -> list[dict]:
        ...

    async def send_proactive_message(self, runtime, message: OutboundMessage) -> dict:
        ...

    async def send_file(self, runtime, request: FileSendRequest) -> dict:
        ...

    async def update_template_card(self, runtime, request: TemplateCardUpdateRequest) -> dict:
        ...


def get_messaging_provider(config) -> MessagingProvider:
    platform = str(getattr(config, "platform", "wecom") or "wecom").strip().lower()
    if platform == "wecom":
        from .wecom_messaging import WeComMessagingProvider

        return WeComMessagingProvider()
    raise ValueError(f"unsupported messaging platform: {platform}")
