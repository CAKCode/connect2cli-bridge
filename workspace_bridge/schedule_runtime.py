from __future__ import annotations

import asyncio
import time

from .config import AppConfig, build_bot_from_app_config
from .execution import run_text_message_once
from .schedule import (
    build_scheduled_job,
    compute_next_cron_run_on_or_after,
    due_schedule_definitions,
    finalize_scheduled_job,
    iter_due_scheduled_job_files,
    now_ms,
    read_scheduled_job,
    schedule_definition_file,
    schedule_pending_root,
    schedule_processing_root,
    write_json_atomic,
    write_scheduled_job,
)


def uid() -> str:
    return f"{int(time.time() * 1000):x}"


def schedule_has_processing_job(config: AppConfig, schedule_id: str) -> bool:
    root = schedule_processing_root(config.runtime_root)
    if not root.exists():
        return False
    for item in root.glob("*.json"):
        job = read_scheduled_job(item)
        if job and job.schedule_id == schedule_id:
            return True
    return False


async def process_due_schedules_once(config: AppConfig) -> list[str]:
    for definition in due_schedule_definitions(config.runtime_root):
        if definition.misfire_policy == "skip_missed":
            grace_ms = max(1000, config.schedule_poll_ms * 2)
            if now_ms() - definition.next_run_at > grace_ms:
                enabled = False if definition.max_runs == 1 else definition.enabled
                next_run_at = 0
                if enabled:
                    next_run_at = compute_next_cron_run_on_or_after(
                        definition.cron,
                        definition.timezone_name,
                        max(now_ms() + 1, definition.next_run_at + 1),
                    )
                next_payload = {
                    "scheduleId": definition.schedule_id,
                    "chatKey": definition.chat_key,
                    "message": definition.message,
                    "cron": definition.cron,
                    "timezone": definition.timezone_name,
                    "nextRunAt": next_run_at,
                    "enabled": enabled,
                    "maxRuns": definition.max_runs,
                    "runCount": definition.run_count,
                    "misfirePolicy": definition.misfire_policy,
                    "concurrencyPolicy": definition.concurrency_policy,
                }
                write_json_atomic(schedule_definition_file(config.runtime_root, definition.schedule_id), next_payload)
                continue
        if definition.concurrency_policy == "skip_if_running" and schedule_has_processing_job(config, definition.schedule_id):
            next_payload = {
                "scheduleId": definition.schedule_id,
                "chatKey": definition.chat_key,
                "message": definition.message,
                "cron": definition.cron,
                "timezone": definition.timezone_name,
                "nextRunAt": compute_next_cron_run_on_or_after(
                    definition.cron,
                    definition.timezone_name,
                    max(now_ms() + 1, definition.next_run_at + 1),
                ),
                "enabled": definition.enabled,
                "maxRuns": definition.max_runs,
                "runCount": definition.run_count,
                "misfirePolicy": definition.misfire_policy,
                "concurrencyPolicy": definition.concurrency_policy,
            }
            write_json_atomic(schedule_definition_file(config.runtime_root, definition.schedule_id), next_payload)
            continue
        job = build_scheduled_job(definition, definition.next_run_at)
        pending_root = schedule_pending_root(config.runtime_root)
        pending_root.mkdir(parents=True, exist_ok=True)
        write_scheduled_job(pending_root / f"{job.run_at:013d}-{job.request_id}.json", job)
        enabled = False if definition.max_runs == 1 else definition.enabled
        next_run_at = 0
        if enabled:
            next_run_at = compute_next_cron_run_on_or_after(
                definition.cron,
                definition.timezone_name,
                max(now_ms() + 1, definition.next_run_at + 1),
            )
        next_payload = {
            "scheduleId": definition.schedule_id,
            "chatKey": definition.chat_key,
            "message": definition.message,
            "cron": definition.cron,
            "timezone": definition.timezone_name,
            "nextRunAt": next_run_at,
            "enabled": enabled,
            "maxRuns": definition.max_runs,
            "runCount": definition.run_count + 1,
            "misfirePolicy": definition.misfire_policy,
            "concurrencyPolicy": definition.concurrency_policy,
        }
        write_json_atomic(schedule_definition_file(config.runtime_root, definition.schedule_id), next_payload)
    return await process_scheduled_jobs_once(config)


async def process_scheduled_jobs_once(config: AppConfig) -> list[str]:
    bot = build_bot_from_app_config(config)
    executed: list[str] = []
    processing_root = schedule_processing_root(config.runtime_root)
    processing_root.mkdir(parents=True, exist_ok=True)
    for job_file in iter_due_scheduled_job_files(config.runtime_root):
        job = read_scheduled_job(job_file)
        if job is None:
            finalize_scheduled_job(config.runtime_root, job_file, ok=False)
            continue
        if job_file.parent != processing_root:
            target = processing_root / job_file.name
            try:
                job_file.replace(target)
            except Exception:
                continue
            job_file = target
        try:
            await run_text_message_once(
                config,
                bot,
                type("ScheduleMessage", (), {"chat_key": job.chat_key, "content": job.message, "req_id": uid(), "raw_payload": {}})(),
            )
        except Exception:
            finalize_scheduled_job(config.runtime_root, job_file, ok=False)
            continue
        finalize_scheduled_job(config.runtime_root, job_file, ok=True)
        executed.append(job.schedule_id)
    return executed


async def schedule_loop(config: AppConfig, *, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        await process_due_schedules_once(config)
        try:
            await asyncio.wait_for(stop_event.wait(), config.schedule_poll_ms / 1000)
        except asyncio.TimeoutError:
            continue
