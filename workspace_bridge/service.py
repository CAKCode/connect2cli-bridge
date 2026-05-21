from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import hashlib
import uuid

from aiohttp import web

from .config import AppConfig, build_bot_from_app_config, load_app_config
from .file_send import create_file_send_request
from .models import WeComBotRuntime, WeComTextMessage
from .schedule_runtime import process_due_schedules_once, process_scheduled_jobs_once
from .wecom_runtime import run_wecom_runtime

APP_CONFIG_KEY = web.AppKey("config", object)
APP_BOT_KEY = web.AppKey("bot", object)
APP_WECOM_RUNTIME_KEY = web.AppKey("wecom_runtime", object)
APP_WECOM_TASK_KEY = web.AppKey("wecom_task", object)
APP_SCHEDULE_TASK_KEY = web.AppKey("schedule_task", object)


async def api_health(request: web.Request) -> web.Response:
    config = request.app[APP_CONFIG_KEY]
    runtime = request.app[APP_WECOM_RUNTIME_KEY]
    schedule_task = request.app[APP_SCHEDULE_TASK_KEY]
    return web.json_response(
        {
            "ok": True,
            "botId": config.bot_id,
            "wecomEnabled": config.wecom_enabled,
            "wecomConnected": bool(runtime.connected),
            "wecomStatus": runtime.last_status,
            "wecomLastError": runtime.last_error,
            "wecomTaskPresent": request.app[APP_WECOM_TASK_KEY] is not None,
            "wecomTaskDone": bool(request.app[APP_WECOM_TASK_KEY].done()) if request.app[APP_WECOM_TASK_KEY] is not None else None,
            "scheduleTaskPresent": schedule_task is not None,
            "scheduleTaskDone": bool(schedule_task.done()) if schedule_task is not None else None,
            "pendingRequests": len(runtime.pending_requests or {}),
            "pendingStreams": len(runtime.pending_streams or {}),
            "pendingFinals": len(runtime.pending_finals or {}),
            "replyStates": len(runtime.reply_states),
        }
    )


async def api_prepare(request) -> web.Response:
    bot = request.app[APP_BOT_KEY]
    data = await request.json()
    from .runtime import prepare_session_run
    from .prompting import build_prompt

    launch = prepare_session_run(bot, data["chatKey"])
    prompt = build_prompt(bot, launch, data["message"])
    return web.json_response(
        {
            "ok": True,
            "workspaceId": launch.session.workspace_id,
            "cwd": str(launch.cwd),
            "workfileDir": str(launch.runtime_context.workfile_dir) if launch.runtime_context.workfile_dir else None,
            "prompt": prompt,
            "sessionId": launch.session.session_id,
        }
    )


async def api_send_file(request) -> web.Response:
    bot = request.app[APP_BOT_KEY]
    data = await request.json()
    from .runtime import prepare_session_run

    launch = prepare_session_run(bot, data["chatKey"])
    file_request = create_file_send_request(
        launch.runtime_context,
        session_id=launch.session.session_id,
        chat_key=data["chatKey"],
        file_path=data["filePath"],
    )
    return web.json_response({"ok": True, "fileName": file_request.file_name, "workspaceId": file_request.workspace_id})


async def api_list_schedules(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import list_schedule_definitions

    definitions = [
        {
            "scheduleId": item.schedule_id,
            "chatKey": item.chat_key,
            "message": item.message,
            "cron": item.cron,
            "timezone": item.timezone_name,
            "nextRunAt": item.next_run_at,
            "enabled": item.enabled,
        }
        for item in list_schedule_definitions(request.app[APP_CONFIG_KEY].runtime_root)
    ]
    return web.json_response(definitions)


async def api_create_schedule(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import create_schedule_definition

    data = await request.json()
    schedule_seed = "\n".join((str(data["chatKey"]), str(data["message"]), str(data["cron"])))
    schedule_id = f"schedule-{hashlib.sha1(schedule_seed.encode('utf-8')).hexdigest()[:8]}-{uuid.uuid4().hex[:8]}"
    definition = create_schedule_definition(
        request.app[APP_CONFIG_KEY].runtime_root,
        schedule_id=schedule_id,
        chat_key=data["chatKey"],
        message=data["message"],
        cron=data["cron"],
        timezone_name="UTC",
    )
    return web.json_response({"ok": True, "scheduleId": definition.schedule_id})


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
            "message": definition.message,
            "cron": definition.cron,
            "timezone": definition.timezone_name,
            "nextRunAt": definition.next_run_at,
            "enabled": definition.enabled,
        }
    )


async def api_pause_schedule(request) -> web.Response:
    if not request.app[APP_CONFIG_KEY].wecom_enabled:
        raise web.HTTPServiceUnavailable(text="schedules require wecom runtime")
    from .schedule import pause_schedule_definition

    try:
        definition = pause_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, request.match_info["schedule_id"])
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

    if read_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, schedule_id) is None:
        raise web.HTTPNotFound(text="schedule not found")
    delete_schedule_definition(request.app[APP_CONFIG_KEY].runtime_root, schedule_id)
    return web.json_response({"ok": True})


def create_app(config: AppConfig) -> web.Application:
    app = web.Application()
    bot = build_bot_from_app_config(config)
    runtime = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    app[APP_CONFIG_KEY] = config
    app[APP_BOT_KEY] = bot
    app[APP_WECOM_RUNTIME_KEY] = runtime
    app[APP_WECOM_TASK_KEY] = None
    app[APP_SCHEDULE_TASK_KEY] = None
    app.router.add_get("/", api_health)
    app.router.add_post("/api/prepare", api_prepare)
    app.router.add_post("/api/send-file", api_send_file)
    app.router.add_get("/api/schedules", api_list_schedules)
    app.router.add_post("/api/schedules", api_create_schedule)
    app.router.add_get("/api/schedules/{schedule_id}", api_get_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/pause", api_pause_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/resume", api_resume_schedule)
    app.router.add_delete("/api/schedules/{schedule_id}", api_delete_schedule)

    async def on_startup(app_: web.Application) -> None:
        async def schedule_loop() -> None:
            while True:
                runtime = app_[APP_WECOM_RUNTIME_KEY]
                if not config.wecom_enabled or runtime.connected:
                    await process_due_schedules_once(config, runtime)
                    await process_scheduled_jobs_once(config, runtime)
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
        for message_task in list(runtime_.message_tasks):
            message_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await message_task
        app_[APP_WECOM_TASK_KEY] = None

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def load_app(argv, *, env_file=None):
    config = load_app_config(env_file=env_file)
    return create_app(config)
