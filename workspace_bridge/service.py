from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import hashlib
from json import JSONDecodeError
import uuid

from aiohttp import web

from .async_utils import run_blocking
from .config import AppConfig, build_bot_from_app_config, load_app_config
from .file_send import create_file_send_request
from .layout import build_workspace_ref, parse_chat_key
from .messaging import get_messaging_provider
from .models import OutboundMessage, TemplateCardUpdateRequest, WeComBotRuntime
from .reply_state import cleanup_reply_state
from .runtime import (
    cleanup_outdated_session_artifacts,
    cleanup_orphan_session_codex_homes,
    cleanup_stale_session_codex_homes,
    load_reply_url_state,
    load_template_card_state,
    stable_session_id,
)
from .schedule_runtime import process_due_schedules_once, process_scheduled_jobs_once
from .template_card_validation import validate_feedback_id, validate_template_card_payload, validate_template_card_update_payload
from .wecom_runtime import run_wecom_runtime
from .wecom_upload import reject_pending_requests

APP_CONFIG_KEY = web.AppKey("config", object)
APP_BOT_KEY = web.AppKey("bot", object)
APP_WECOM_RUNTIME_KEY = web.AppKey("wecom_runtime", object)
APP_WECOM_TASK_KEY = web.AppKey("wecom_task", object)
APP_SCHEDULE_TASK_KEY = web.AppKey("schedule_task", object)
SESSION_HOME_TTL_MS = 30 * 60 * 1000


def _clear_deferred_schedule_cache(runtime: WeComBotRuntime, runtime_root, schedule_id: str) -> None:
    from .schedule import schedule_pending_root, schedule_processing_root

    request_ids: set[str] = set()
    for root in (schedule_pending_root(runtime_root), schedule_processing_root(runtime_root)):
        for path in root.glob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(payload.get("schedule_id") or payload.get("scheduleId") or "") != schedule_id:
                continue
            request_id = str(payload.get("request_id") or payload.get("requestId") or "").strip()
            if request_id:
                request_ids.add(request_id)
    for request_id in request_ids:
        cache_key = f"job:{request_id}"
        if runtime.pending_finals is not None:
            runtime.pending_finals.pop(cache_key, None)
        if runtime.pending_streams is not None:
            runtime.pending_streams.pop(cache_key, None)
        cleanup_reply_state(runtime, cache_key)


async def _interrupt_active_schedule_run(runtime: WeComBotRuntime, schedule_id: str, chat_key: str) -> None:
    active = runtime.active_schedule_runs.get(chat_key)
    if active is None or active[0] != schedule_id:
        return
    request_id = active[1]
    runtime.suppressed_schedule_cancels.add((chat_key, request_id))
    runtime.terminal_schedule_cancels.add((chat_key, request_id))
    schedule_task = runtime.active_schedule_tasks.pop(chat_key, None)
    process = runtime.active_processes.get(chat_key)
    if schedule_task is not None:
        with suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
            schedule_task.cancel()
            await asyncio.wait_for(schedule_task, timeout=1.0)
    if process is not None:
        process.terminate()
    if process is not None:
        wait_method = getattr(process, "wait", None)
        if callable(wait_method):
            with suppress(asyncio.TimeoutError, Exception):
                await asyncio.wait_for(wait_method(), timeout=1.0)
        runtime.active_processes.pop(chat_key, None)
    runtime.active_schedule_runs.pop(chat_key, None)
    cache_key = f"job:{request_id}"
    if runtime.pending_finals is not None:
        runtime.pending_finals.pop(cache_key, None)
    if runtime.pending_streams is not None:
        runtime.pending_streams.pop(cache_key, None)
    cleanup_reply_state(runtime, cache_key)
    runtime.suppressed_schedule_cancels.discard((chat_key, request_id))
    runtime.terminal_schedule_cancels.discard((chat_key, request_id))


async def api_health(request: web.Request) -> web.Response:
    config = request.app[APP_CONFIG_KEY]
    runtime = request.app[APP_WECOM_RUNTIME_KEY]
    wecom_task = request.app[APP_WECOM_TASK_KEY]
    schedule_task = request.app[APP_SCHEDULE_TASK_KEY]
    wecom_task_done = bool(wecom_task.done()) if wecom_task is not None else None
    schedule_task_done = bool(schedule_task.done()) if schedule_task is not None else None
    health_ok = True
    if config.wecom_enabled:
        wecom_status = str(runtime.wecom_status or "").strip()
        health_ok = (
            wecom_task is not None
            and not bool(wecom_task_done)
            and schedule_task is not None
            and not bool(schedule_task_done)
            and wecom_status not in {"subscribe_failed", "connect_failed", "websocket_closed", "websocket_error", "websocket_disconnected_event"}
            and runtime.last_status != "schedule_failed"
            and (bool(runtime.connected) or wecom_status in {"", "subscribe_ok"})
        )
    return web.json_response(
        {
            "ok": health_ok,
            "botId": config.bot_id,
            "wecomEnabled": config.wecom_enabled,
            "wecomConnected": bool(runtime.connected),
            "wecomStatus": runtime.wecom_status,
            "wecomLastError": runtime.wecom_last_error,
            "runtimeStatus": runtime.last_status,
            "runtimeLastError": runtime.last_error,
            "wecomTaskPresent": wecom_task is not None,
            "wecomTaskDone": wecom_task_done,
            "scheduleTaskPresent": schedule_task is not None,
            "scheduleTaskDone": schedule_task_done,
            "pendingRequests": len(runtime.pending_requests or {}),
            "pendingStreams": len(runtime.pending_streams or {}),
            "pendingFinals": len(runtime.pending_finals or {}),
            "replyStates": len(runtime.reply_states),
        },
        status=200 if health_ok else 503,
    )


async def api_prepare(request) -> web.Response:
    bot = request.app[APP_BOT_KEY]
    try:
        data = await request.json()
    except (JSONDecodeError, UnicodeDecodeError) as exc:
        raise web.HTTPBadRequest(text="request body must be valid JSON") from exc
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="request body must be a JSON object")
    chat_key = str(data.get("chatKey") or "").strip()
    message = str(data.get("message") or "").strip()
    if not chat_key:
        raise web.HTTPBadRequest(text="chatKey required")
    if not message:
        raise web.HTTPBadRequest(text="message required")
    from .runtime import prepare_session_run
    from .prompting import build_prompt

    try:
        launch = await run_blocking(prepare_session_run, bot, chat_key)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    prompt = build_prompt(bot, launch, message)
    return web.json_response(
        {
            "ok": True,
            "workspaceId": launch.session.workspace_id,
            "workspaceScope": launch.session.workspace_scope,
            "cwd": str(launch.cwd),
            "workfileDir": str(launch.runtime_context.workfile_dir) if launch.runtime_context.workfile_dir else None,
            "roomfileDir": str(launch.runtime_context.roomfile_dir) if launch.runtime_context.roomfile_dir else None,
            "ownerUserId": launch.workspace.workspace.owner_user_id,
            "ownerRoomId": launch.workspace.workspace.owner_room_id,
            "prompt": prompt,
            "sessionId": launch.session.session_id,
        }
    )


async def api_send_file(request) -> web.Response:
    bot = request.app[APP_BOT_KEY]
    runtime = request.app[APP_WECOM_RUNTIME_KEY]
    try:
        data = await request.json()
    except (JSONDecodeError, UnicodeDecodeError) as exc:
        raise web.HTTPBadRequest(text="request body must be valid JSON") from exc
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="request body must be a JSON object")
    chat_key = str(data.get("chatKey") or "").strip()
    file_path = str(data.get("filePath") or "").strip()
    if not chat_key:
        raise web.HTTPBadRequest(text="chatKey required")
    if not file_path:
        raise web.HTTPBadRequest(text="filePath required")
    from .runtime import prepare_session_run

    # This endpoint performs an immediate WeCom upload/send and returns the
    # final transport result; it is not the queue-based bridge.py endpoint.
    try:
        launch = await run_blocking(prepare_session_run, bot, chat_key)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    try:
        file_request = create_file_send_request(
            launch.runtime_context,
            session_id=launch.session.session_id,
            chat_key=chat_key,
            file_path=file_path,
        )
    except FileNotFoundError as exc:
        raise web.HTTPNotFound(text=str(exc)) from exc
    except PermissionError as exc:
        text = str(exc)
        if text.startswith("file too large:"):
            actual_size = 0
            try:
                actual_size = __import__("pathlib").Path(data["filePath"]).expanduser().resolve().stat().st_size
            except Exception:
                pass
            raise web.HTTPRequestEntityTooLarge(
                max_size=launch.runtime_context.max_upload_size,
                actual_size=actual_size,
                text=text,
            ) from exc
        raise web.HTTPForbidden(text=text) from exc
    if runtime.ws is None or not runtime.connected:
        raise web.HTTPServiceUnavailable(text="bot not connected")
    try:
        result = await get_messaging_provider(bot).send_file(runtime, file_request)
    except asyncio.TimeoutError as exc:
        raise web.HTTPGatewayTimeout(text="file send timed out") from exc
    except RuntimeError as exc:
        message = str(exc) or "file send failed"
        if "not connected" in message:
            raise web.HTTPServiceUnavailable(text=message) from exc
        raise web.HTTPBadGateway(text=message) from exc
    except Exception as exc:
        message = str(exc) or "file send failed"
        raise web.HTTPBadGateway(text=message) from exc
    return web.json_response(
        {
            "ok": True,
            "sessionId": file_request.session_id,
            "chatKey": file_request.chat_key,
            "fileName": file_request.file_name,
            "workspaceId": file_request.workspace_id,
            "mediaId": result["mediaId"],
            "message": f"sent {file_request.file_name}",
        }
    )


async def api_send_message(request) -> web.Response:
    bot = request.app[APP_BOT_KEY]
    runtime = request.app[APP_WECOM_RUNTIME_KEY]
    try:
        data = await request.json()
    except (JSONDecodeError, UnicodeDecodeError) as exc:
        raise web.HTTPBadRequest(text="request body must be valid JSON") from exc
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="request body must be a JSON object")
    chat_key = str(data.get("chatKey") or "").strip()
    reply_req_id = str(data.get("replyReqId") or data.get("reply_req_id") or "").strip()
    msgtype = str(data.get("msgtype") or "").strip()
    if not chat_key and not reply_req_id:
        raise web.HTTPBadRequest(text="chatKey required")
    if chat_key:
        try:
            parse_chat_key(chat_key)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
    if not msgtype:
        raise web.HTTPBadRequest(text="msgtype required")
    if msgtype == "markdown":
        content = str(data.get("content") or "").strip()
        if not content:
            raise web.HTTPBadRequest(text="content required")
        feedback_id = str(data.get("feedbackId") or data.get("feedback_id") or "").strip() or None
        if feedback_id is not None:
            try:
                feedback_id = validate_feedback_id(feedback_id)
            except ValueError as exc:
                raise web.HTTPBadRequest(text=str(exc)) from exc
        message = OutboundMessage(
            chat_key=chat_key or str((runtime.reply_urls.get(reply_req_id) or {}).get("chatKey") or ""),
            msgtype="markdown",
            content=content,
            mention_user_id=str(data.get("mentionUserId") or data.get("mention_user_id") or "").strip() or None,
            feedback_id=feedback_id,
        )
    elif msgtype == "template_card":
        template_card = data["templateCard"] if "templateCard" in data else data.get("template_card")
        try:
            template_card = validate_template_card_payload(template_card, require_interaction_task_id=False)
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        feedback_id = str(data.get("feedbackId") or data.get("feedback_id") or "").strip() or None
        if feedback_id is not None:
            try:
                feedback_id = validate_feedback_id(feedback_id)
            except ValueError as exc:
                raise web.HTTPBadRequest(text=str(exc)) from exc
        message = OutboundMessage(
            chat_key=chat_key or str((runtime.reply_urls.get(reply_req_id) or {}).get("chatKey") or ""),
            msgtype="template_card",
            template_card=template_card,
            feedback_id=feedback_id,
        )
    else:
        raise web.HTTPBadRequest(text=f"unsupported msgtype: {msgtype}")
    if runtime.ws is None or not runtime.connected:
        raise web.HTTPServiceUnavailable(text="bot not connected")
    try:
        if reply_req_id:
            result = await get_messaging_provider(bot).send_via_response_url(runtime, reply_req_id=reply_req_id, message=message)
        else:
            result = await get_messaging_provider(bot).send_proactive_message(runtime, message)
    except asyncio.TimeoutError as exc:
        raise web.HTTPGatewayTimeout(text="message send timed out") from exc
    except RuntimeError as exc:
        message_text = str(exc) or "message send failed"
        if "not connected" in message_text:
            raise web.HTTPServiceUnavailable(text=message_text) from exc
        raise web.HTTPBadGateway(text=message_text) from exc
    except Exception as exc:
        raise web.HTTPBadGateway(text=str(exc) or "message send failed") from exc
    response_payload = {
        "ok": True,
        "chatKey": chat_key,
        "msgtype": msgtype,
        "payloadCount": int(result.get("payloadCount") or 1),
    }
    if msgtype == "template_card":
        delivered_template_card = dict(result.get("deliveredTemplateCard") or {})
        response_payload["cardType"] = str((template_card or {}).get("card_type") or "")
        task_id = str(delivered_template_card.get("task_id") or "").strip()
        if task_id:
            response_payload["taskId"] = task_id
    return web.json_response(response_payload)


async def api_update_template_card(request) -> web.Response:
    bot = request.app[APP_BOT_KEY]
    runtime = request.app[APP_WECOM_RUNTIME_KEY]
    try:
        data = await request.json()
    except (JSONDecodeError, UnicodeDecodeError) as exc:
        raise web.HTTPBadRequest(text="request body must be valid JSON") from exc
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="request body must be a JSON object")
    req_id = str(data.get("reqId") or data.get("req_id") or "").strip()
    template_card = data["templateCard"] if "templateCard" in data else data.get("template_card")
    if not req_id:
        raise web.HTTPBadRequest(text="reqId required")
    if not isinstance(template_card, dict):
        raise web.HTTPBadRequest(text="templateCard must be a JSON object")
    try:
        template_card = validate_template_card_update_payload(template_card)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    card_type = str(template_card.get("card_type") or "").strip()
    if runtime.ws is None or not runtime.connected:
        raise web.HTTPServiceUnavailable(text="bot not connected")
    update_request = TemplateCardUpdateRequest(req_id=req_id, template_card=template_card)
    try:
        await get_messaging_provider(bot).update_template_card(runtime, update_request)
    except asyncio.TimeoutError as exc:
        raise web.HTTPGatewayTimeout(text="template card update timed out") from exc
    except RuntimeError as exc:
        message_text = str(exc) or "template card update failed"
        if "not connected" in message_text:
            raise web.HTTPServiceUnavailable(text=message_text) from exc
        raise web.HTTPBadGateway(text=message_text) from exc
    except Exception as exc:
        raise web.HTTPBadGateway(text=str(exc) or "template card update failed") from exc
    return web.json_response({"ok": True, "reqId": req_id, "cardType": card_type})


async def api_list_schedules(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import list_schedule_definitions
    bot_id = request.app[APP_CONFIG_KEY].bot_id
    source_dir = request.app[APP_CONFIG_KEY].source_dir

    definitions = [
        {
            "scheduleId": item.schedule_id,
            "chatKey": item.chat_key,
            "sessionId": stable_session_id(
                bot_id,
                item.chat_key,
                request.app[APP_CONFIG_KEY].source_dir,
                request.app[APP_CONFIG_KEY].workspace_namespace,
                request.app[APP_CONFIG_KEY].workspace_mode,
            ),
            "workspaceId": build_workspace_ref(
                request.app[APP_CONFIG_KEY].runtime_root,
                request.app[APP_CONFIG_KEY].workspace_namespace,
                source_dir,
                item.chat_key,
                workspace_mode=request.app[APP_CONFIG_KEY].workspace_mode,
                agent_backend=request.app[APP_CONFIG_KEY].agent_backend,
            ).workspace_id,
            "message": item.message,
            "cron": item.cron,
            "timezone": item.timezone_name,
            "nextRunAt": item.next_run_at,
            "enabled": item.enabled,
            "maxRuns": item.max_runs,
            "runCount": item.run_count,
            "misfirePolicy": item.misfire_policy,
            "concurrencyPolicy": item.concurrency_policy,
        }
        for item in list_schedule_definitions(request.app[APP_CONFIG_KEY].runtime_root)
    ]
    return web.json_response(definitions)


async def api_create_schedule(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import create_schedule_definition

    try:
        data = await request.json()
    except (JSONDecodeError, UnicodeDecodeError) as exc:
        raise web.HTTPBadRequest(text="request body must be valid JSON") from exc
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text="request body must be a JSON object")
    chat_key = str(data.get("chatKey") or "").strip()
    message = str(data.get("message") or "").strip()
    cron = str(data.get("cron") or "").strip()
    timezone_name = str(data.get("timezone") or "UTC").strip() or "UTC"
    misfire_policy = str(data.get("misfirePolicy") or data.get("misfire_policy") or "fire_once_now").strip() or "fire_once_now"
    concurrency_policy = str(data.get("concurrencyPolicy") or data.get("concurrency_policy") or "skip_if_running").strip() or "skip_if_running"
    max_runs = data.get("maxRuns") or data.get("max_runs")
    if not chat_key:
        raise web.HTTPBadRequest(text="chatKey required")
    if not message:
        raise web.HTTPBadRequest(text="message required")
    if not cron:
        raise web.HTTPBadRequest(text="cron required")
    schedule_seed = "\n".join((chat_key, message, cron, timezone_name, str(max_runs or ""), misfire_policy, concurrency_policy))
    schedule_id = f"schedule-{hashlib.sha1(schedule_seed.encode('utf-8')).hexdigest()[:8]}-{uuid.uuid4().hex[:8]}"
    try:
        definition = create_schedule_definition(
            request.app[APP_CONFIG_KEY].runtime_root,
            schedule_id=schedule_id,
            chat_key=chat_key,
            message=message,
            cron=cron,
            timezone_name=timezone_name,
            max_runs=max_runs,
            misfire_policy=misfire_policy,
            concurrency_policy=concurrency_policy,
        )
    except ValueError as exc:
        raise web.HTTPBadRequest(text=str(exc)) from exc
    return web.json_response(
        {
            "ok": True,
            "scheduleId": definition.schedule_id,
            "chatKey": definition.chat_key,
            "sessionId": stable_session_id(
                request.app[APP_CONFIG_KEY].bot_id,
                definition.chat_key,
                request.app[APP_CONFIG_KEY].source_dir,
                request.app[APP_CONFIG_KEY].workspace_namespace,
                request.app[APP_CONFIG_KEY].workspace_mode,
            ),
            "workspaceId": build_workspace_ref(
                request.app[APP_CONFIG_KEY].runtime_root,
                request.app[APP_CONFIG_KEY].workspace_namespace,
                request.app[APP_CONFIG_KEY].source_dir,
                definition.chat_key,
                workspace_mode=request.app[APP_CONFIG_KEY].workspace_mode,
                agent_backend=request.app[APP_CONFIG_KEY].agent_backend,
            ).workspace_id,
            "message": definition.message,
            "cron": definition.cron,
            "timezone": definition.timezone_name,
            "nextRunAt": definition.next_run_at,
            "enabled": definition.enabled,
            "maxRuns": definition.max_runs,
            "runCount": definition.run_count,
            "misfirePolicy": definition.misfire_policy,
            "concurrencyPolicy": definition.concurrency_policy,
        }
    )


async def api_get_schedule(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import read_schedule_definition

    definition = read_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, request.match_info["schedule_id"])
    if definition is None:
        raise web.HTTPNotFound(text="schedule not found")
    return web.json_response(
        {
            "scheduleId": definition.schedule_id,
            "chatKey": definition.chat_key,
            "sessionId": stable_session_id(
                request.app[APP_CONFIG_KEY].bot_id,
                definition.chat_key,
                request.app[APP_CONFIG_KEY].source_dir,
                request.app[APP_CONFIG_KEY].workspace_namespace,
                request.app[APP_CONFIG_KEY].workspace_mode,
            ),
            "workspaceId": build_workspace_ref(
                request.app[APP_CONFIG_KEY].runtime_root,
                request.app[APP_CONFIG_KEY].workspace_namespace,
                request.app[APP_CONFIG_KEY].source_dir,
                definition.chat_key,
                workspace_mode=request.app[APP_CONFIG_KEY].workspace_mode,
                agent_backend=request.app[APP_CONFIG_KEY].agent_backend,
            ).workspace_id,
            "message": definition.message,
            "cron": definition.cron,
            "timezone": definition.timezone_name,
            "nextRunAt": definition.next_run_at,
            "enabled": definition.enabled,
            "maxRuns": definition.max_runs,
            "runCount": definition.run_count,
            "misfirePolicy": definition.misfire_policy,
            "concurrencyPolicy": definition.concurrency_policy,
        }
    )


async def api_pause_schedule(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import pause_schedule_definition, read_schedule_definition

    schedule_id = request.match_info["schedule_id"]
    definition = read_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, schedule_id)
    if definition is None:
        raise web.HTTPNotFound(text="schedule not found")
    await _interrupt_active_schedule_run(
        request.app[APP_WECOM_RUNTIME_KEY],
        schedule_id,
        definition.chat_key,
    )
    _clear_deferred_schedule_cache(
        request.app[APP_WECOM_RUNTIME_KEY],
        request.app[APP_CONFIG_KEY].runtime_root,
        schedule_id,
    )
    try:
        definition = pause_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, schedule_id)
    except FileNotFoundError as exc:
        raise web.HTTPNotFound(text="schedule not found") from exc
    return web.json_response({"scheduleId": definition.schedule_id, "enabled": definition.enabled})


async def api_resume_schedule(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import resume_schedule_definition

    try:
        definition = resume_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, request.match_info["schedule_id"])
    except FileNotFoundError as exc:
        raise web.HTTPNotFound(text="schedule not found") from exc
    return web.json_response({"scheduleId": definition.schedule_id, "enabled": definition.enabled})


async def api_delete_schedule(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import delete_schedule_definition

    schedule_id = request.match_info["schedule_id"]
    from .schedule import read_schedule_definition

    definition = read_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, schedule_id)
    if definition is None:
        raise web.HTTPNotFound(text="schedule not found")
    await _interrupt_active_schedule_run(
        request.app[APP_WECOM_RUNTIME_KEY],
        schedule_id,
        definition.chat_key,
    )
    _clear_deferred_schedule_cache(
        request.app[APP_WECOM_RUNTIME_KEY],
        request.app[APP_CONFIG_KEY].runtime_root,
        schedule_id,
    )
    delete_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, schedule_id)
    return web.json_response({"ok": True})


def create_app(config: AppConfig) -> web.Application:
    app = web.Application()
    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    runtime.template_card_delivery_meta = load_template_card_state(config.runtime_root, config.bot_id)
    runtime.reply_urls = load_reply_url_state(config.runtime_root, config.bot_id)
    for item in runtime.template_card_delivery_meta.values():
        template_card = item.get("templateCard")
        if not isinstance(template_card, dict):
            continue
        task_id = str(template_card.get("task_id") or "").strip()
        if not task_id:
            continue
        runtime.template_card_payloads[task_id] = dict(template_card)
        button_texts: dict[str, str] = {}
        for button in template_card.get("button_list") or []:
            if not isinstance(button, dict):
                continue
            key = str(button.get("key") or "").strip()
            text = str(button.get("text") or "").strip()
            if key and text:
                button_texts[key] = text
        if button_texts:
            runtime.template_card_button_texts[task_id] = button_texts
    app[APP_CONFIG_KEY] = config
    app[APP_BOT_KEY] = bot
    app[APP_WECOM_RUNTIME_KEY] = runtime
    app[APP_WECOM_TASK_KEY] = None
    app[APP_SCHEDULE_TASK_KEY] = None
    app.router.add_get("/", api_health)
    app.router.add_get("/healthz", api_health)
    app.router.add_post("/api/prepare", api_prepare)
    app.router.add_post("/api/send-message", api_send_message)
    app.router.add_post("/api/template-card/update", api_update_template_card)
    app.router.add_post("/api/send-file", api_send_file)
    app.router.add_get("/api/schedules", api_list_schedules)
    app.router.add_post("/api/schedules", api_create_schedule)
    app.router.add_get("/api/schedules/{schedule_id}", api_get_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/pause", api_pause_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/resume", api_resume_schedule)
    app.router.add_delete("/api/schedules/{schedule_id}", api_delete_schedule)

    async def on_startup(app_: web.Application) -> None:
        cleanup_outdated_session_artifacts(bot)
        cleanup_orphan_session_codex_homes(config.runtime_root)
        cleanup_stale_session_codex_homes(
            config.runtime_root,
            current_ms=int(__import__("time").time() * 1000),
            ttl_ms=SESSION_HOME_TTL_MS,
            active_session_ids=set(app_[APP_WECOM_RUNTIME_KEY].active_session_ids),
        )

        async def schedule_loop() -> None:
            while True:
                runtime = app_[APP_WECOM_RUNTIME_KEY]
                if runtime.connected:
                    schedule_failed = False
                    try:
                        await process_due_schedules_once(config, runtime)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        schedule_failed = True
                        runtime.last_status = "schedule_failed"
                        runtime.last_error = str(exc)
                    try:
                        await process_scheduled_jobs_once(config, runtime)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        schedule_failed = True
                        runtime.last_status = "schedule_failed"
                        runtime.last_error = str(exc)
                    if not schedule_failed and runtime.last_status == "schedule_failed":
                        runtime.last_status = None
                        runtime.last_error = None
                await asyncio.sleep(config.schedule_poll_ms / 1000)

        if config.wecom_enabled:
            app_[APP_WECOM_TASK_KEY] = asyncio.create_task(run_wecom_runtime(config, app_[APP_WECOM_RUNTIME_KEY]))
            app_[APP_SCHEDULE_TASK_KEY] = asyncio.create_task(schedule_loop())
        else:
            app_[APP_WECOM_TASK_KEY] = None
            app_[APP_SCHEDULE_TASK_KEY] = None

    async def on_cleanup(app_: web.Application) -> None:
        schedule_task = app_[APP_SCHEDULE_TASK_KEY]
        if schedule_task is not None:
            schedule_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await schedule_task
        app_[APP_SCHEDULE_TASK_KEY] = None
        task = app_[APP_WECOM_TASK_KEY]
        if task is not None:
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        runtime_ = app_[APP_WECOM_RUNTIME_KEY]
        for process in list(runtime_.active_processes.values()):
            with suppress(Exception):
                process.terminate()
        for chat_key, active in list(runtime_.active_schedule_runs.items()):
            if isinstance(active, tuple) and len(active) == 2:
                runtime_.suppressed_schedule_cancels.add((chat_key, active[1]))
        for schedule_task_ in list(runtime_.active_schedule_tasks.values()):
            schedule_task_.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await schedule_task_
        for message_task in list(runtime_.message_tasks):
            message_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await message_task
        runtime_.connected = False
        runtime_.ws = None
        runtime_.wecom_status = None
        runtime_.wecom_last_error = None
        runtime_.last_status = None
        runtime_.last_error = None
        reject_pending_requests(runtime_, "service shutting down")
        runtime_.pending_streams = {}
        runtime_.pending_finals = {}
        runtime_.reply_states = {}
        runtime_.active_processes = {}
        runtime_.active_message_tasks = {}
        runtime_.message_tasks = set()
        runtime_.active_session_ids = set()
        runtime_.session_threads = {}
        runtime_.active_schedule_tasks = {}
        runtime_.active_schedule_runs = {}
        runtime_.suppressed_schedule_cancels = set()
        runtime_.terminal_schedule_cancels = set()
        runtime_.suppressed_failure_tasks = set()
        runtime_.resume_candidates = {}
        runtime_.resume_selection_expires_at = {}
        app_[APP_WECOM_TASK_KEY] = None

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def load_app(argv, *, env_file=None):
    config = load_app_config(env_file=env_file)
    return create_app(config)
