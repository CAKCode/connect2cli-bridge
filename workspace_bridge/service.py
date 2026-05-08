from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from aiohttp import web

from .config import AppConfig, build_bot_from_app_config, load_app_config
from .file_send import create_file_send_request
from .models import WeComBotRuntime
from .prompting import build_prompt
from .runtime import prepare_session_run
from .schedule import (
    create_one_shot_schedule,
    create_schedule_definition,
    delete_schedule_definition,
    list_schedule_definitions,
    pause_schedule_definition,
    read_schedule_definition,
    resume_schedule_definition,
)
from .schedule_runtime import process_due_schedules_once, schedule_loop, uid
from .wecom_runtime import run_wecom_ws_once
from .wecom_upload import upload_and_send_file

APP_CONFIG_KEY = web.AppKey("config", AppConfig)
APP_BOT_KEY = web.AppKey("bot", object)
APP_WECOM_TASK_KEY = web.AppKey("wecom_task", object)
APP_WECOM_RUNTIME_KEY = web.AppKey("wecom_runtime", object)
APP_SCHEDULE_TASK_KEY = web.AppKey("schedule_task", object)
APP_SCHEDULE_STOP_KEY = web.AppKey("schedule_stop", object)


def configure_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(logging.INFO)
        logging.getLogger("workspace_bridge").setLevel(logging.INFO)
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def create_app(config: AppConfig) -> web.Application:
    bot = build_bot_from_app_config(config)
    app = web.Application()
    app[APP_CONFIG_KEY] = config
    app[APP_BOT_KEY] = bot
    app[APP_WECOM_TASK_KEY] = None
    app[APP_WECOM_RUNTIME_KEY] = WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    app[APP_SCHEDULE_TASK_KEY] = None
    app[APP_SCHEDULE_STOP_KEY] = asyncio.Event()

    async def health(_: web.Request) -> web.Response:
        runtime = app[APP_WECOM_RUNTIME_KEY]
        task = app[APP_WECOM_TASK_KEY]
        return web.json_response(
            {
                "ok": True,
                "bind": f"{config.bind_host}:{config.bind_port}",
                "botId": config.bot_id,
                "botName": config.bot_name,
                "sourceDir": str(config.source_dir),
                "runtimeRoot": str(config.runtime_root),
                "wecomEnabled": config.wecom_enabled,
                "wecomConnected": bool(runtime and runtime.connected),
                "wecomStatus": runtime.last_status if runtime else None,
                "wecomLastError": runtime.last_error if runtime else None,
                "wecomTaskPresent": task is not None,
                "wecomTaskDone": bool(task.done()) if task is not None else None,
                "pendingRequests": len(runtime.pending_requests or {}) if runtime else 0,
                "pendingStreams": len(runtime.pending_streams or {}) if runtime else 0,
                "pendingFinals": len(runtime.pending_finals or {}) if runtime else 0,
                "replyStates": len(runtime.reply_states or {}) if runtime else 0,
            }
        )

    async def prepare_session(request: web.Request) -> web.Response:
        payload = await request.json()
        chat_key = str(payload.get("chatKey") or "").strip()
        message = str(payload.get("message") or "").strip()
        if not chat_key:
            raise web.HTTPBadRequest(text="chatKey required")
        bot = request.app[APP_BOT_KEY]
        launch = prepare_session_run(bot, chat_key)
        prompt = build_prompt(bot, launch, message)
        return web.json_response(
            {
                "ok": True,
                "sessionId": launch.session.session_id,
                "workspaceId": launch.session.workspace_id,
                "cwd": str(launch.cwd),
                "chatfileDir": str(launch.runtime_context.chatfile_dir),
                "effectiveSkills": list(launch.runtime_context.effective_skill_names),
                "prompt": prompt,
            }
        )

    async def send_file(request: web.Request) -> web.Response:
        payload = await request.json()
        chat_key = str(payload.get("chatKey") or "").strip()
        file_path = str(payload.get("filePath") or "").strip()
        if not chat_key:
            raise web.HTTPBadRequest(text="chatKey required")
        if not file_path:
            raise web.HTTPBadRequest(text="filePath required")
        bot = request.app[APP_BOT_KEY]
        launch = prepare_session_run(bot, chat_key)
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
            raise web.HTTPForbidden(text=str(exc)) from exc
        except ValueError as exc:
            raise web.HTTPBadRequest(text=str(exc)) from exc
        wecom_runtime = request.app[APP_WECOM_RUNTIME_KEY]
        if wecom_runtime is not None and wecom_runtime.ws is not None:
            result = await upload_and_send_file(wecom_runtime, file_request)
            return web.json_response({**result, "sessionId": file_request.session_id, "workspaceId": file_request.workspace_id})
        return web.json_response(
            {
                "ok": True,
                "sessionId": file_request.session_id,
                "workspaceId": file_request.workspace_id,
                "fileName": file_request.file_name,
                "filePath": str(file_request.file_path),
                "message": "validated for send-back; upload transport not connected",
            }
        )

    async def create_schedule(request: web.Request) -> web.Response:
        payload = await request.json()
        chat_key = str(payload.get("chatKey") or "").strip()
        message = str(payload.get("message") or "").strip()
        cron = str(payload.get("cron") or "").strip()
        run_at = payload.get("runAt")
        if not chat_key or not message:
            raise web.HTTPBadRequest(text="chatKey and message required")
        schedule_id = uid()
        if cron:
            definition = create_schedule_definition(
                config.runtime_root,
                schedule_id=schedule_id,
                chat_key=chat_key,
                message=message,
                cron=cron,
                timezone_name=str(payload.get("timezone") or "UTC").strip() or "UTC",
                max_runs=int(payload["maxRuns"]) if payload.get("maxRuns") is not None else None,
                misfire_policy=str(payload.get("misfirePolicy") or "fire_once_now").strip() or "fire_once_now",
                concurrency_policy=str(payload.get("concurrencyPolicy") or "skip_if_running").strip() or "skip_if_running",
            )
        else:
            if run_at is None:
                raise web.HTTPBadRequest(text="cron or runAt required")
            definition = create_one_shot_schedule(
                config.runtime_root,
                schedule_id=schedule_id,
                chat_key=chat_key,
                message=message,
                run_at_ms=int(run_at),
            )
        return web.json_response({"ok": True, "scheduleId": definition.schedule_id, "nextRunAt": definition.next_run_at})

    async def list_schedules(_: web.Request) -> web.Response:
        return web.json_response(
            [
                {
                    "scheduleId": item.schedule_id,
                    "chatKey": item.chat_key,
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
                for item in list_schedule_definitions(config.runtime_root)
            ]
        )

    async def get_schedule(request: web.Request) -> web.Response:
        schedule_id = request.match_info["schedule_id"]
        definition = read_schedule_definition(config.runtime_root, schedule_id)
        if definition is None:
            raise web.HTTPNotFound(text=f"schedule not found: {schedule_id}")
        return web.json_response(
            {
                "scheduleId": definition.schedule_id,
                "chatKey": definition.chat_key,
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

    async def pause_schedule(request: web.Request) -> web.Response:
        schedule_id = request.match_info["schedule_id"]
        try:
            definition = pause_schedule_definition(config.runtime_root, schedule_id)
        except FileNotFoundError as exc:
            raise web.HTTPNotFound(text=str(exc)) from exc
        return web.json_response({"ok": True, "scheduleId": definition.schedule_id, "enabled": definition.enabled})

    async def resume_schedule(request: web.Request) -> web.Response:
        schedule_id = request.match_info["schedule_id"]
        try:
            definition = resume_schedule_definition(config.runtime_root, schedule_id)
        except FileNotFoundError as exc:
            raise web.HTTPNotFound(text=str(exc)) from exc
        return web.json_response({"ok": True, "scheduleId": definition.schedule_id, "enabled": definition.enabled, "nextRunAt": definition.next_run_at})

    async def delete_schedule(request: web.Request) -> web.Response:
        schedule_id = request.match_info["schedule_id"]
        try:
            delete_schedule_definition(config.runtime_root, schedule_id)
        except FileNotFoundError as exc:
            raise web.HTTPNotFound(text=str(exc)) from exc
        return web.json_response({"ok": True, "scheduleId": schedule_id})

    async def run_schedules_once(_: web.Request) -> web.Response:
        executed = await process_due_schedules_once(config)
        return web.json_response({"ok": True, "executed": executed})

    app.router.add_get("/", health)
    app.router.add_post("/api/prepare-session", prepare_session)
    app.router.add_post("/api/send-file", send_file)
    app.router.add_get("/api/schedules", list_schedules)
    app.router.add_post("/api/schedules", create_schedule)
    app.router.add_get("/api/schedules/{schedule_id}", get_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/pause", pause_schedule)
    app.router.add_post("/api/schedules/{schedule_id}/resume", resume_schedule)
    app.router.add_delete("/api/schedules/{schedule_id}", delete_schedule)
    app.router.add_post("/api/schedules/run-once", run_schedules_once)

    async def on_startup(app: web.Application) -> None:
        app[APP_SCHEDULE_STOP_KEY].clear()
        app[APP_SCHEDULE_TASK_KEY] = asyncio.create_task(schedule_loop(config, stop_event=app[APP_SCHEDULE_STOP_KEY]))
        if not config.wecom_enabled:
            return
        app[APP_WECOM_TASK_KEY] = asyncio.create_task(run_wecom_ws_once(config, runtime=app[APP_WECOM_RUNTIME_KEY]))

    async def on_cleanup(app: web.Application) -> None:
        app[APP_SCHEDULE_STOP_KEY].set()
        schedule_task = app[APP_SCHEDULE_TASK_KEY]
        if schedule_task is not None:
            await schedule_task
            app[APP_SCHEDULE_TASK_KEY] = None
        task = app[APP_WECOM_TASK_KEY]
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        app[APP_WECOM_TASK_KEY] = None

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


def load_app(argv: list[str] | None = None, *, env_file: Path | None = None) -> web.Application:
    _ = argv
    configure_logging()
    return create_app(load_app_config(env_file=env_file))
