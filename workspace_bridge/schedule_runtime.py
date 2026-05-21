from __future__ import annotations

import json
import time
from pathlib import Path

from .config import build_bot_from_app_config
from .models import WeComBotRuntime, WeComTextMessage
from .schedule import (
    ScheduleDefinition,
    ScheduledJob,
    compute_next_cron_run_on_or_after,
    due_schedule_definitions,
    schedule_done_root,
    schedule_failed_root,
    schedule_pending_root,
    schedule_processing_root,
    write_schedule_definition,
)
from .execution import execute_and_deliver_message

SCHEDULE_ORPHAN_TTL_MS = 60_000
SCHEDULE_DEFINITION_POLL_MS = 1_000


def _schedule_has_processing_work(config, schedule_id: str) -> bool:
    for path in schedule_processing_root(config.runtime_root).glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("schedule_id") or payload.get("scheduleId") or "") == schedule_id:
            return True
    return False


def _schedule_marker_path(config, schedule_id: str) -> Path:
    return schedule_processing_root(config.runtime_root) / f"definition-{schedule_id}.json"


def _acquire_schedule_processing_marker(config, schedule_id: str) -> Path | None:
    marker_path = _schedule_marker_path(config, schedule_id)
    try:
        with marker_path.open("x", encoding="utf-8") as handle:
            json.dump({"scheduleId": schedule_id, "claimedAt": int(time.time() * 1000)}, handle, ensure_ascii=False)
    except FileExistsError:
        return None
    return marker_path


def _claim_pending_job(config, path: Path) -> Path | None:
    processing_path = schedule_processing_root(config.runtime_root) / f"{path.stem}.{int(time.time() * 1000)}.processing.json"
    try:
        path.replace(processing_path)
    except FileNotFoundError:
        return None
    return processing_path


def _cleanup_orphaned_processing_files(config, *, now_ms: int) -> None:
    processing_root = schedule_processing_root(config.runtime_root)
    for path in processing_root.glob("*.json"):
        try:
            mtime_ms = int(path.stat().st_mtime * 1000)
        except FileNotFoundError:
            continue
        if now_ms - mtime_ms <= SCHEDULE_ORPHAN_TTL_MS:
            continue
        if path.name.startswith("definition-"):
            path.unlink(missing_ok=True)
            continue
        original_name = path.name.split(".", 1)[0] + ".json"
        target = schedule_pending_root(config.runtime_root) / original_name
        if target.exists():
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
    _cleanup_orphaned_processing_files(config, now_ms=now_ms)
    bot = build_bot_from_app_config(config)
    delivery_runtime = runtime or WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    for definition in due_schedule_definitions(config.runtime_root, current_ms=now_ms):
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
        if definition.concurrency_policy == "skip_if_running" and _schedule_has_processing_work(config, definition.schedule_id):
            continue
        marker_path = _acquire_schedule_processing_marker(config, definition.schedule_id)
        if marker_path is None:
            continue
        try:
            await execute_and_deliver_message(
                config,
                delivery_runtime,
                WeComTextMessage(req_id="", chat_key=definition.chat_key, content=definition.message, raw_payload={}),
            )
            executed.append(definition.schedule_id)
            next_run_count = definition.run_count + 1
            if definition.max_runs is not None and next_run_count >= definition.max_runs:
                next_definition = ScheduleDefinition(
                    **{
                        **definition.__dict__,
                        "enabled": False,
                        "next_run_at": 0,
                        "run_count": next_run_count,
                    }
                )
            elif definition.cron:
                next_run_at = compute_next_cron_run_on_or_after(
                    definition.cron,
                    definition.timezone_name or "UTC",
                    int(time.time() * 1000) + 1,
                )
                next_definition = ScheduleDefinition(
                    **{
                        **definition.__dict__,
                        "next_run_at": next_run_at,
                        "run_count": next_run_count,
                    }
                )
            else:
                next_definition = ScheduleDefinition(
                    **{
                        **definition.__dict__,
                        "enabled": False,
                        "next_run_at": 0,
                        "run_count": next_run_count,
                    }
                )
            write_schedule_definition(config.runtime_root, next_definition)
            (schedule_done_root(config.runtime_root) / f"{definition.schedule_id}.json").write_text("{}", encoding="utf-8")
        except Exception:
            continue
        finally:
            marker_path.unlink(missing_ok=True)
    return executed


async def process_scheduled_jobs_once(config, runtime: WeComBotRuntime | None = None) -> list[str]:
    executed: list[str] = []
    _cleanup_orphaned_processing_files(config, now_ms=int(time.time() * 1000))
    bot = build_bot_from_app_config(config)
    delivery_runtime = runtime or WeComBotRuntime(config=bot, pending_requests={}, pending_streams={}, pending_finals={})
    for path in sorted(schedule_pending_root(config.runtime_root).glob("*.json")):
        processing_path = _claim_pending_job(config, path)
        if processing_path is None:
            continue
        try:
            payload = __import__("json").loads(processing_path.read_text(encoding="utf-8"))
            job = ScheduledJob(**payload)
            await execute_and_deliver_message(
                config,
                delivery_runtime,
                WeComTextMessage(req_id="", chat_key=job.chat_key, content=job.message, raw_payload={}),
            )
        except Exception:
            target = schedule_failed_root(config.runtime_root) / path.name
            processing_path.replace(target)
            continue
        executed.append(job.request_id)
        target = schedule_done_root(config.runtime_root) / path.name
        processing_path.replace(target)
    return executed
