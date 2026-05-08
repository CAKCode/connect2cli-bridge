from __future__ import annotations

import asyncio
import json
from typing import Awaitable, Callable

import aiohttp
from aiohttp import WSMsgType

from .config import AppConfig, build_bot_from_app_config
from .execution import execute_and_deliver_message, flush_cached_runtime_payloads
from .inbound_media import download_incoming_media, extract_mixed_images, extract_mixed_text
from .logging_utils import get_logger
from .models import WeComBotRuntime
from .wecom_protocol import WECOM_WS_URL, build_subscribe_payload, build_text_response_payload, is_subscribe_ok, parse_text_callback, payload_msg_type
from .wecom_upload import create_request_future, reject_pending_requests, resolve_pending_request

LOG = get_logger("workspace_bridge.wecom")


async def default_text_handler(bot, message) -> tuple[str, str]:
    raise RuntimeError("default_text_handler requires an app config-aware wrapper")


async def ws_send_json(ws: aiohttp.ClientWebSocketResponse, payload: dict) -> None:
    await ws.send_json(payload)


async def flush_pending_payloads(bot: WeComBotRuntime) -> None:
    if bot.ws is None:
        return
    for payload in list((bot.pending_streams or {}).values()):
        await bot.ws.send_json(payload)
    for payload in list((bot.pending_finals or {}).values()):
        await bot.ws.send_json(payload)
    if bot.pending_streams is not None:
        bot.pending_streams.clear()
    if bot.pending_finals is not None:
        bot.pending_finals.clear()
    await flush_cached_runtime_payloads(bot)


async def handle_wecom_payload(
    config: AppConfig,
    bot: WeComBotRuntime,
    ws: aiohttp.ClientWebSocketResponse,
    data: dict,
    handler: Callable[[AppConfig, object, object], Awaitable[tuple[str, str]]],
) -> None:
    if resolve_pending_request(bot, data):
        return
    LOG.info(
        "wecom prelude/unsolicited message botId=%s cmd=%s body_keys=%s",
        bot.config.bot_id,
        data.get("cmd"),
        sorted((data.get("body") or {}).keys()),
    )
    message = parse_text_callback(data)
    if message:
        LOG.info("wecom recv text chatKey=%s req_id=%s", message.chat_key, message.req_id)
        await handler(config, bot, message)
        return
    msg_type = payload_msg_type(data)
    if msg_type == "image":
        parsed = type(
            "InboundMessage",
            (),
            {
                "req_id": str(((data.get("headers") or {}).get("req_id")) or "").strip(),
                "chat_key": parse_text_callback({"cmd": "aibot_msg_callback", "headers": data.get("headers") or {}, "body": {"msgtype": "text", "text": {"content": "."}, **(data.get("body") or {})}}).chat_key,
            },
        )()
        try:
            media = await download_incoming_media(bot.config, parsed, "image", (data.get("body") or {}).get("image") or {})
            LOG.info("wecom recv image chatKey=%s file=%s", parsed.chat_key, media["fileName"])
            await ws_send_json(ws, build_text_response_payload(parsed.req_id, uid(), f"Received image: {media['fileName']}", final=True))
        except Exception as exc:
            LOG.exception("wecom image handling failed chatKey=%s", parsed.chat_key)
            await ws_send_json(ws, build_text_response_payload(parsed.req_id, uid(), f"Receive image failed: {exc}", final=True))
        return
    if msg_type == "file":
        parsed = type(
            "InboundMessage",
            (),
            {
                "req_id": str(((data.get("headers") or {}).get("req_id")) or "").strip(),
                "chat_key": parse_text_callback({"cmd": "aibot_msg_callback", "headers": data.get("headers") or {}, "body": {"msgtype": "text", "text": {"content": "."}, **(data.get("body") or {})}}).chat_key,
            },
        )()
        try:
            media = await download_incoming_media(bot.config, parsed, "file", (data.get("body") or {}).get("file") or {})
            LOG.info("wecom recv file chatKey=%s file=%s", parsed.chat_key, media["fileName"])
            await ws_send_json(ws, build_text_response_payload(parsed.req_id, uid(), f"Received file: {media['fileName']}", final=True))
        except Exception as exc:
            LOG.exception("wecom file handling failed chatKey=%s", parsed.chat_key)
            await ws_send_json(ws, build_text_response_payload(parsed.req_id, uid(), f"Receive file failed: {exc}", final=True))
        return
    if msg_type == "mixed":
        body = data.get("body") or {}
        mixed = body.get("mixed") or {}
        parsed = type(
            "InboundMessage",
            (),
            {
                "req_id": str(((data.get("headers") or {}).get("req_id")) or "").strip(),
                "chat_key": parse_text_callback({"cmd": "aibot_msg_callback", "headers": data.get("headers") or {}, "body": {"msgtype": "text", "text": {"content": "."}, **body}}).chat_key,
            },
        )()
        saved_files = []
        for image in extract_mixed_images(mixed):
            try:
                media = await download_incoming_media(bot.config, parsed, "image", image or {})
                saved_files.append(media["fileName"])
            except Exception:
                continue
        mixed_text = extract_mixed_text(mixed)
        response_lines = []
        if saved_files:
            response_lines.append("Received mixed images: " + ", ".join(saved_files))
        if mixed_text:
            response_lines.append("Mixed text captured.")
            text_message = type("MixedTextMessage", (), {"req_id": parsed.req_id, "chat_key": parsed.chat_key, "content": mixed_text, "raw_payload": data})()
            _session_id, _content = await handler(config, bot, text_message)
            if response_lines and bot.ws is not None:
                await ws_send_json(ws, build_text_response_payload(parsed.req_id, uid(), "\n".join(response_lines), final=False))
        elif response_lines:
            await ws_send_json(ws, build_text_response_payload(parsed.req_id, uid(), "\n".join(response_lines), final=True))
        return
    if msg_type:
        LOG.info("wecom recv unsupported msgtype=%s", msg_type)


async def ws_reader_loop(
    config: AppConfig,
    bot: WeComBotRuntime,
    ws: aiohttp.ClientWebSocketResponse,
    handler: Callable[[AppConfig, object, object], Awaitable[tuple[str, str]]],
) -> None:
    async for raw_msg in ws:
        if raw_msg.type == WSMsgType.TEXT:
            data = json.loads(raw_msg.data)
            await handle_wecom_payload(config, bot, ws, data, handler)
        elif raw_msg.type == WSMsgType.ERROR:
            raise RuntimeError(f"websocket error: {ws.exception()}")
        elif raw_msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
            break


async def run_wecom_ws_once(
    config: AppConfig,
    runtime: WeComBotRuntime | None = None,
    *,
    handler: Callable[[AppConfig, object, object], Awaitable[tuple[str, str]]] = execute_and_deliver_message,
) -> None:
    bot = runtime or WeComBotRuntime(config=build_bot_from_app_config(config), pending_requests={}, pending_streams={}, pending_finals={})
    bot.last_status = "starting"
    backoff_sec = 1
    while True:
        try:
            LOG.info("wecom connecting botId=%s ws=%s", bot.config.bot_id, WECOM_WS_URL)
            async with aiohttp.ClientSession(trust_env=True) as session:
                async with session.ws_connect(WECOM_WS_URL, heartbeat=None, autoping=True) as ws:
                    bot.ws = ws
                    bot.connected = False
                    bot.last_status = "connecting"
                    bot.last_error = None
                    reader_task = asyncio.create_task(ws_reader_loop(config, bot, ws, handler))
                    sub_payload = build_subscribe_payload(bot.config)
                    sub_req_id = str(((sub_payload.get("headers") or {}).get("req_id")) or "")
                    sub_future = create_request_future(bot, sub_req_id)
                    LOG.info(
                        "wecom subscribe request botId=%s secret_len=%s req_id=%s",
                        bot.config.bot_id,
                        len(bot.config.bot_secret or ""),
                        sub_req_id,
                    )
                    await ws_send_json(ws, sub_payload)
                    try:
                        subscribe_response = await asyncio.wait_for(sub_future, timeout=config.wecom_subscribe_timeout_sec)
                    except asyncio.TimeoutError as exc:
                        reader_task.cancel()
                        try:
                            await reader_task
                        except asyncio.CancelledError:
                            pass
                        bot.connected = False
                        bot.last_status = "subscribe_timeout"
                        bot.last_error = f"subscribe timeout after {config.wecom_subscribe_timeout_sec}s"
                        LOG.error("wecom subscribe timeout botId=%s timeout=%ss", bot.config.bot_id, config.wecom_subscribe_timeout_sec)
                        raise RuntimeError(bot.last_error) from exc
                    LOG.info(
                        "wecom subscribe response botId=%s errcode=%s errmsg=%s",
                        bot.config.bot_id,
                        subscribe_response.get("errcode"),
                        subscribe_response.get("errmsg"),
                    )
                    if not subscribe_response or not is_subscribe_ok(subscribe_response):
                        reader_task.cancel()
                        try:
                            await reader_task
                        except asyncio.CancelledError:
                            pass
                        bot.connected = False
                        bot.last_status = "subscribe_failed"
                        bot.last_error = json.dumps(subscribe_response or {"error": "no subscribe response"}, ensure_ascii=False)
                        LOG.error("wecom subscribe failed botId=%s detail=%s", bot.config.bot_id, bot.last_error)
                        raise RuntimeError(f"subscribe failed: {bot.last_error}")
                    bot.connected = True
                    bot.last_status = "running"
                    LOG.info("wecom connected botId=%s", bot.config.bot_id)
                    await flush_pending_payloads(bot)
                    backoff_sec = 1
                    await reader_task
        except asyncio.CancelledError:
            bot.connected = False
            bot.ws = None
            bot.last_status = "stopped"
            LOG.info("wecom runner stopped botId=%s", bot.config.bot_id)
            reject_pending_requests(bot, "bot websocket closed")
            raise
        except Exception as exc:
            bot.connected = False
            bot.ws = None
            bot.last_status = "reconnecting"
            bot.last_error = str(exc)
            LOG.exception("wecom runtime error botId=%s", bot.config.bot_id)
            reject_pending_requests(bot, "bot websocket closed")
            await asyncio.sleep(backoff_sec)
            LOG.info("wecom reconnecting in %ss botId=%s", backoff_sec, bot.config.bot_id)
            backoff_sec = min(backoff_sec * 2, 30)
