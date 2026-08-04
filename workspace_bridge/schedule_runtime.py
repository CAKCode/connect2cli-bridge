from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from .atomic_io import atomic_write_json, atomic_write_text, exclusive_create_json
from .config import build_bot_from_app_config
from .models import WeComBotRuntime, WeComTextMessage
from .schedule import (
    ScheduleDefinition,
    ScheduledJob,
    advance_schedule_definition_after_success,
    compute_next_cron_run_on_or_after,
    due_schedule_definitions,
    parse_scheduled_job_payload,
    read_schedule_definition,
    schedule_definition_processing_marker_file,
    schedule_done_marker_file,
    schedule_failed_marker_file,
    schedule_done_root,
    schedule_failed_root,
    schedule_pending_root,
    schedule_processing_root,
    scheduled_job_file_name,
    write_scheduled_job,
    write_schedule_definition,
)
from .execution import execute_and_deliver_message

SCHEDULE_ORPHAN_TTL_MS = 60_000
SCHEDULE_DEFINITION_POLL_MS = 1_000


def _schedule_has_processing_work(config, schedule_id: str) -> bool:
    for path in schedule_processing_root(config.runtime_root).glob("*.json"):
        if path.is_symlink():
            path.unlink(missing_ok=True)
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("schedule_id") or payload.get("scheduleId") or "") == schedule_id:
            return True
    return False


def _schedule_has_pending_work(config, schedule_id: str) -> bool:
    for path in schedule_pending_root(config.runtime_root).glob("*.json"):
        if path.is_symlink():
            path.unlink(missing_ok=True)
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("schedule_id") or payload.get("scheduleId") or "") == schedule_id:
            return True
    return False


def _scheduled_job_file_name(job: ScheduledJob) -> str:
    return scheduled_job_file_name(job.run_at, job.request_id)


def _build_scheduled_job_for_definition(definition: ScheduleDefinition, *, now_ms: int) -> ScheduledJob:
    run_at = int(definition.next_run_at)
    request_id = f"{definition.schedule_id}-{run_at}"
    return ScheduledJob(
        request_id=request_id,
        schedule_id=definition.schedule_id,
        chat_key=definition.chat_key,
        message=definition.message,
        run_at=run_at,
        created_at=now_ms,
        bot_id=definition.bot_id,
    )


def _schedule_marker_path(config, schedule_id: str) -> Path:
    return schedule_definition_processing_marker_file(config.runtime_root, schedule_id)


def _schedule_failed_marker_path(config, schedule_id: str) -> Path:
    return schedule_failed_marker_file(config.runtime_root, schedule_id)


def _acquire_schedule_processing_marker(config, schedule_id: str) -> Path | None:
    marker_path = _schedule_marker_path(config, schedule_id)
    if marker_path.is_symlink():
        marker_path.unlink(missing_ok=True)
    if not exclusive_create_json(
        marker_path,
        {"scheduleId": schedule_id, "claimedAt": int(time.time() * 1000)},
    ):
        return None
    return marker_path


def _write_marker_payload(path: Path, payload: dict | str) -> None:
    if isinstance(payload, dict):
        atomic_write_json(path, payload)
    else:
        atomic_write_text(path, str(payload))


def _write_schedule_failure_marker(config, definition: ScheduleDefinition, exc: Exception) -> None:
    _write_marker_payload(
        _schedule_failed_marker_path(config, definition.schedule_id),
        {
            "scheduleId": definition.schedule_id,
            "chatKey": definition.chat_key,
            "message": definition.message,
            "failedAt": int(time.time() * 1000),
            "error": str(exc),
        },
    )


def _write_scheduled_job_failure_marker(config, job: ScheduledJob, exc: Exception) -> None:
    _write_marker_payload(
        schedule_failed_root(config.runtime_root) / scheduled_job_file_name(job.run_at, job.request_id),
        {
            "requestId": job.request_id,
            "scheduleId": job.schedule_id,
            "chatKey": job.chat_key,
            "message": job.message,
            "runAt": job.run_at,
            "createdAt": job.created_at,
            "failedAt": int(time.time() * 1000),
            "error": str(exc),
        },
    )


def _is_delivery_deferred_error(exc: Exception) -> bool:
    return "final delivery deferred until connection recovers" in str(exc or "")


def _schedule_should_requeue_after_external_cancel(config, schedule_id: str) -> bool:
    definition = read_schedule_definition(config.runtime_root, schedule_id)
    return definition is not None and bool(definition.enabled)


def _claim_pending_job(config, path: Path) -> Path | None:
    if path.is_symlink():
        path.unlink(missing_ok=True)
        return None
    processing_path = schedule_processing_root(config.runtime_root) / f"{path.stem}.{int(time.time() * 1000)}.processing.json"
    try:
        path.replace(processing_path)
    except FileNotFoundError:
        return None
    return processing_path


def _original_pending_name_from_processing(path: Path) -> str:
    name = path.name
    suffix = ".processing.json"
    if not name.endswith(suffix):
        return path.name.split(".", 1)[0] + ".json"
    claimed = name[: -len(suffix)]
    base = claimed.rsplit(".", 1)[0] if "." in claimed else claimed
    return f"{base}.json"


def _cleanup_orphaned_processing_files(config, *, now_ms: int, active_schedule_runs: dict[str, tuple[str, str]] | None = None) -> None:
    processing_root = schedule_processing_root(config.runtime_root)
    active_runs = dict(active_schedule_runs or {})
    active_schedule_ids = {schedule_id for schedule_id, _request_id in active_runs.values()}

    def belongs_to_other_bot(payload: dict | None) -> bool:
        if not isinstance(payload, dict):
            return False
        current_bot_id = str(getattr(config, "bot_id", "") or "").strip()
        payload_bot_id = str(payload.get("bot_id") or payload.get("botId") or "").strip()
        if payload_bot_id:
            return current_bot_id != payload_bot_id
        schedule_id = str(payload.get("scheduleId") or payload.get("schedule_id") or "").strip()
        if not schedule_id:
            return False
        definition = read_schedule_definition(config.runtime_root, schedule_id)
        if definition is None or definition.bot_id is None:
            return False
        return definition.bot_id != current_bot_id

    for path in processing_root.glob("*.json"):
        if path.is_symlink():
            path.unlink(missing_ok=True)
            continue
        try:
            mtime_ms = int(path.stat().st_mtime * 1000)
        except FileNotFoundError:
            continue
        if now_ms - mtime_ms <= SCHEDULE_ORPHAN_TTL_MS:
            continue
        if path.name.startswith("definition-"):
            payload = None
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                payload = None
            if belongs_to_other_bot(payload):
                continue
            schedule_id = str((payload or {}).get("scheduleId") or (payload or {}).get("schedule_id") or "").strip()
            if schedule_id and schedule_id in active_schedule_ids:
                continue
            path.unlink(missing_ok=True)
            continue
        payload = None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = None
        if belongs_to_other_bot(payload):
            continue
        schedule_id = str((payload or {}).get("schedule_id") or (payload or {}).get("scheduleId") or "").strip()
        request_id = str((payload or {}).get("request_id") or (payload or {}).get("requestId") or "").strip()
        chat_key = str((payload or {}).get("chat_key") or (payload or {}).get("chatKey") or "").strip()
        if chat_key and schedule_id and request_id and active_runs.get(chat_key) == (schedule_id, request_id):
            continue
        original_name = _original_pending_name_from_processing(path)
        target = schedule_pending_root(config.runtime_root) / original_name
        if target.is_symlink():
            target.unlink(missing_ok=True)
        elif target.exists():
            path.unlink(missing_ok=True)
            continue
        try:
            path.replace(target)
        except FileNotFoundError:
            continue


def _should_skip_due_to_misfire(config, definition, *, now_ms: int) -> bool:
    if definition.misfire_policy != "skip_missed":
        return False
    grace_ms = max(
        1_000,
        int(config.schedule_poll_ms) * 2 if getattr(config, "schedule_poll_ms", None) else SCHEDULE_DEFINITION_POLL_MS * 2,
    )
    return bool(definition.next_run_at) and now_ms - int(definition.next_run_at) > grace_ms


async def process_due_schedules_once(config, runtime: WeComBotRuntime | None = None) -> list[str]:
    executed: list[str] = []
    now_ms = int(time.time() * 1000)
    _cleanup_orphaned_processing_files(config, now_ms=now_ms, active_schedule_runs=(runtime.active_schedule_runs if runtime is not None else None))
    bot = build_bot_from_app_config(config)
    delivery_runtime = runtime or WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    if runtime is not None and not bool(runtime.connected):
        return executed
    for definition in due_schedule_definitions(config.runtime_root, current_ms=now_ms, bot_id=bot.bot_id):
        if _should_skip_due_to_misfire(config, definition, now_ms=now_ms):
            next_run_count = definition.run_count
            if definition.cron:
                next_run_at = compute_next_cron_run_on_or_after(
                    definition.cron,
                    definition.timezone_name or "UTC",
                    now_ms + 1,
                )
                write_schedule_definition(
                    config.runtime_root,
                    ScheduleDefinition(
                        **{
                            **definition.__dict__,
                            "next_run_at": next_run_at,
                            "run_count": next_run_count,
                        }
                    ),
                )
            else:
                write_schedule_definition(
                    config.runtime_root,
                    ScheduleDefinition(
                        **{
                            **definition.__dict__,
                            "enabled": False,
                            "next_run_at": 0,
                            "run_count": next_run_count,
                        }
                    ),
                )
            continue
        if definition.concurrency_policy == "skip_if_running" and (
            _schedule_has_processing_work(config, definition.schedule_id)
            or _schedule_has_pending_work(config, definition.schedule_id)
        ):
            continue
        marker_path = _acquire_schedule_processing_marker(config, definition.schedule_id)
        if marker_path is None:
            continue
        scheduled_job = _build_scheduled_job_for_definition(definition, now_ms=now_ms)
        run_task = None
        if runtime is not None:
            runtime.active_schedule_runs[definition.chat_key] = (definition.schedule_id, scheduled_job.request_id)
        try:
            if runtime is not None:
                run_task = asyncio.create_task(
                    execute_and_deliver_message(
                        config,
                        delivery_runtime,
                        WeComTextMessage(
                            req_id="",
                            chat_key=definition.chat_key,
                            content=definition.message,
                            raw_payload={"deliveryCacheKey": f"job:{scheduled_job.request_id}"},
                        ),
                    )
                )
                runtime.active_schedule_tasks[definition.chat_key] = run_task
                await run_task
            else:
                await execute_and_deliver_message(
                    config,
                    delivery_runtime,
                    WeComTextMessage(
                        req_id="",
                        chat_key=definition.chat_key,
                        content=definition.message,
                        raw_payload={"deliveryCacheKey": f"job:{scheduled_job.request_id}"},
                    ),
                )
            executed.append(definition.schedule_id)
            advance_schedule_definition_after_success(config.runtime_root, definition.schedule_id)
            _write_marker_payload(schedule_done_marker_file(config.runtime_root, definition.schedule_id), "{}")
            _schedule_failed_marker_path(config, definition.schedule_id).unlink(missing_ok=True)
        except asyncio.CancelledError:
            if runtime is not None and (definition.chat_key, scheduled_job.request_id) in runtime.terminal_schedule_cancels:
                runtime.terminal_schedule_cancels.discard((definition.chat_key, scheduled_job.request_id))
                runtime.suppressed_schedule_cancels.discard((definition.chat_key, scheduled_job.request_id))
                continue
            if runtime is not None and (definition.chat_key, scheduled_job.request_id) in runtime.suppressed_schedule_cancels:
                runtime.suppressed_schedule_cancels.discard((definition.chat_key, scheduled_job.request_id))
                if _schedule_should_requeue_after_external_cancel(config, definition.schedule_id):
                    write_scheduled_job(
                        schedule_pending_root(config.runtime_root) / _scheduled_job_file_name(scheduled_job),
                        scheduled_job,
                    )
                continue
            if runtime is not None and run_task is not None and runtime.active_schedule_tasks.get(definition.chat_key) is not run_task:
                continue
            raise
        except Exception as exc:
            if runtime is not None and run_task is not None and runtime.active_schedule_tasks.get(definition.chat_key) is not run_task:
                continue
            if _is_delivery_deferred_error(exc):
                write_scheduled_job(
                    schedule_pending_root(config.runtime_root) / _scheduled_job_file_name(scheduled_job),
                    scheduled_job,
                )
                _schedule_failed_marker_path(config, definition.schedule_id).unlink(missing_ok=True)
                schedule_done_marker_file(config.runtime_root, definition.schedule_id).unlink(missing_ok=True)
                continue
            _write_schedule_failure_marker(config, definition, exc)
            schedule_done_marker_file(config.runtime_root, definition.schedule_id).unlink(missing_ok=True)
            if definition.cron:
                next_run_at = compute_next_cron_run_on_or_after(
                    definition.cron,
                    definition.timezone_name or "UTC",
                    int(time.time() * 1000) + 1,
                )
                write_schedule_definition(
                    config.runtime_root,
                    ScheduleDefinition(
                        **{
                            **definition.__dict__,
                            "next_run_at": next_run_at,
                        }
                    ),
                )
            continue
        finally:
            if runtime is not None:
                runtime.suppressed_schedule_cancels.discard((definition.chat_key, scheduled_job.request_id))
                runtime.terminal_schedule_cancels.discard((definition.chat_key, scheduled_job.request_id))
            if runtime is not None and run_task is not None and runtime.active_schedule_tasks.get(definition.chat_key) is run_task:
                runtime.active_schedule_tasks.pop(definition.chat_key, None)
            if runtime is not None and runtime.active_schedule_runs.get(definition.chat_key) == (definition.schedule_id, scheduled_job.request_id):
                runtime.active_schedule_runs.pop(definition.chat_key, None)
            marker_path.unlink(missing_ok=True)
    return executed


async def process_scheduled_jobs_once(config, runtime: WeComBotRuntime | None = None) -> list[str]:
    executed: list[str] = []
    _cleanup_orphaned_processing_files(
        config,
        now_ms=int(time.time() * 1000),
        active_schedule_runs=(runtime.active_schedule_runs if runtime is not None else None),
    )
    bot = build_bot_from_app_config(config)
    delivery_runtime = runtime or WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    if runtime is not None and not bool(runtime.connected):
        return executed
    for path in sorted(schedule_pending_root(config.runtime_root).glob("*.json")):
        try:
            preview_payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            preview_payload = None
        preview = parse_scheduled_job_payload(preview_payload)
        if preview is None:
            path.unlink(missing_ok=True)
            continue
        if preview.bot_id not in {None, bot.bot_id}:
            continue
        processing_path = _claim_pending_job(config, path)
        if processing_path is None:
            continue
        job: ScheduledJob | None = None
        active_run_key: tuple[str, str] | None = None
        run_task = None
        try:
            payload = json.loads(processing_path.read_text(encoding="utf-8"))
            job = parse_scheduled_job_payload(payload)
            if job is None:
                processing_path.unlink(missing_ok=True)
                continue
            if job.bot_id not in {None, bot.bot_id}:
                processing_path.replace(schedule_pending_root(config.runtime_root) / path.name)
                continue
            if runtime is not None and (runtime.pending_finals or {}).get(f"job:{job.request_id}"):
                processing_path.replace(schedule_pending_root(config.runtime_root) / path.name)
                continue
            if runtime is not None:
                active_run_key = (job.schedule_id, job.request_id)
                runtime.active_schedule_runs[job.chat_key] = active_run_key
                run_task = asyncio.create_task(
                    execute_and_deliver_message(
                        config,
                        delivery_runtime,
                        WeComTextMessage(
                            req_id="",
                            chat_key=job.chat_key,
                            content=job.message,
                            raw_payload={"deliveryCacheKey": f"job:{job.request_id}"},
                        ),
                    )
                )
                runtime.active_schedule_tasks[job.chat_key] = run_task
                await run_task
            else:
                await execute_and_deliver_message(
                    config,
                    delivery_runtime,
                    WeComTextMessage(
                        req_id="",
                        chat_key=job.chat_key,
                        content=job.message,
                        raw_payload={"deliveryCacheKey": f"job:{job.request_id}"},
                    ),
                )
        except asyncio.CancelledError:
            if runtime is not None and job is not None and (job.chat_key, job.request_id) in runtime.terminal_schedule_cancels:
                runtime.terminal_schedule_cancels.discard((job.chat_key, job.request_id))
                runtime.suppressed_schedule_cancels.discard((job.chat_key, job.request_id))
                processing_path.unlink(missing_ok=True)
                continue
            if runtime is not None and job is not None and (job.chat_key, job.request_id) in runtime.suppressed_schedule_cancels:
                runtime.suppressed_schedule_cancels.discard((job.chat_key, job.request_id))
                if _schedule_should_requeue_after_external_cancel(config, job.schedule_id):
                    target = schedule_pending_root(config.runtime_root) / path.name
                    processing_path.replace(target)
                else:
                    processing_path.unlink(missing_ok=True)
                continue
            if runtime is not None and job is not None and run_task is not None and runtime.active_schedule_tasks.get(job.chat_key) is not run_task:
                continue
            raise
        except Exception as exc:
            if runtime is not None and job is not None and run_task is not None and runtime.active_schedule_tasks.get(job.chat_key) is not run_task:
                continue
            if job is not None:
                if _is_delivery_deferred_error(exc):
                    target = schedule_pending_root(config.runtime_root) / path.name
                    processing_path.replace(target)
                    schedule_done_marker_file(config.runtime_root, job.schedule_id).unlink(missing_ok=True)
                    continue
                _write_scheduled_job_failure_marker(config, job, exc)
                schedule_done_marker_file(config.runtime_root, job.schedule_id).unlink(missing_ok=True)
                processing_path.unlink(missing_ok=True)
            else:
                target = schedule_failed_root(config.runtime_root) / path.name
                processing_path.replace(target)
            continue
        else:
            executed.append(job.request_id)
            target = schedule_done_root(config.runtime_root) / path.name
            advance_schedule_definition_after_success(config.runtime_root, job.schedule_id)
            schedule_failed_marker_file(config.runtime_root, job.schedule_id).unlink(missing_ok=True)
            processing_path.replace(target)
            _write_marker_payload(schedule_done_marker_file(config.runtime_root, job.schedule_id), "{}")
        finally:
            if runtime is not None and job is not None:
                runtime.suppressed_schedule_cancels.discard((job.chat_key, job.request_id))
                runtime.terminal_schedule_cancels.discard((job.chat_key, job.request_id))
            if runtime is not None and job is not None and run_task is not None and runtime.active_schedule_tasks.get(job.chat_key) is run_task:
                runtime.active_schedule_tasks.pop(job.chat_key, None)
            if runtime is not None and job is not None and active_run_key is not None and runtime.active_schedule_runs.get(job.chat_key) == active_run_key:
                runtime.active_schedule_runs.pop(job.chat_key, None)
    return executed
