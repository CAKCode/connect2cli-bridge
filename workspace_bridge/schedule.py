from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def now_ms() -> int:
    return int(time.time() * 1000)


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_json_file(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


@dataclass(frozen=True)
class ScheduleDefinition:
    schedule_id: str
    chat_key: str
    message: str
    cron: str
    timezone_name: str
    next_run_at: int
    enabled: bool = True
    max_runs: int | None = None
    run_count: int = 0
    misfire_policy: str = "fire_once_now"
    concurrency_policy: str = "skip_if_running"


@dataclass(frozen=True)
class ScheduledJob:
    request_id: str
    schedule_id: str
    chat_key: str
    message: str
    run_at: int
    created_at: int


def schedule_root(runtime_root: Path | str) -> Path:
    return Path(runtime_root).expanduser().resolve() / "schedules"


def schedule_definition_file(runtime_root: Path | str, schedule_id: str) -> Path:
    return schedule_root(runtime_root) / "definitions" / f"{schedule_id}.json"


def schedule_pending_root(runtime_root: Path | str) -> Path:
    return schedule_root(runtime_root) / "pending"


def schedule_processing_root(runtime_root: Path | str) -> Path:
    return schedule_root(runtime_root) / "processing"


def schedule_done_root(runtime_root: Path | str) -> Path:
    return schedule_root(runtime_root) / "done"


def schedule_failed_root(runtime_root: Path | str) -> Path:
    return schedule_root(runtime_root) / "failed"


def list_schedule_definitions(runtime_root: Path | str) -> list[ScheduleDefinition]:
    root = schedule_root(runtime_root) / "definitions"
    if not root.exists():
        return []
    items: list[ScheduleDefinition] = []
    for item in sorted(root.glob("*.json")):
        payload = read_json_file(item)
        if not payload:
            continue
        items.append(
            ScheduleDefinition(
                schedule_id=str(payload["scheduleId"]),
                chat_key=str(payload["chatKey"]),
                message=str(payload["message"]),
                cron=str(payload["cron"]),
                timezone_name=str(payload["timezone"]),
                next_run_at=int(payload["nextRunAt"]),
                enabled=bool(payload.get("enabled", True)),
                max_runs=int(payload["maxRuns"]) if payload.get("maxRuns") is not None else None,
                run_count=int(payload.get("runCount") or 0),
                misfire_policy=str(payload.get("misfirePolicy") or "fire_once_now"),
                concurrency_policy=str(payload.get("concurrencyPolicy") or "skip_if_running"),
            )
        )
    return items


def parse_cron_expression(expr: str) -> tuple[set[int], set[int], set[int], set[int], set[int]]:
    parts = str(expr or "").strip().split()
    if len(parts) != 5:
        raise ValueError("cron must contain exactly 5 fields")

    def parse_field(token: str, minimum: int, maximum: int) -> set[int]:
        if token == "*":
            return set(range(minimum, maximum + 1))
        values: set[int] = set()
        for item in token.split(","):
            values.add(int(item))
        for value in values:
            if value < minimum or value > maximum:
                raise ValueError(f"cron field out of range: {token}")
        return values

    return (
        parse_field(parts[0], 0, 59),
        parse_field(parts[1], 0, 23),
        parse_field(parts[2], 1, 31),
        parse_field(parts[3], 1, 12),
        parse_field(parts[4], 0, 6),
    )


def cron_datetime_matches(dt: datetime, spec: tuple[set[int], set[int], set[int], set[int], set[int]]) -> bool:
    minutes, hours, month_days, months, weekdays = spec
    return (
        dt.minute in minutes
        and dt.hour in hours
        and dt.day in month_days
        and dt.month in months
        and dt.weekday() in weekdays
    )


def compute_next_cron_run_on_or_after(expr: str, timezone_name: str, earliest_ms: int) -> int:
    spec = parse_cron_expression(expr)
    tzinfo = ZoneInfo(timezone_name)
    earliest = datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc).astimezone(tzinfo)
    candidate = earliest.replace(second=0, microsecond=0)
    if earliest.second or earliest.microsecond:
        candidate += timedelta(minutes=1)
    limit = candidate + timedelta(days=366)
    while candidate <= limit:
        if cron_datetime_matches(candidate, spec):
            return int(candidate.astimezone(timezone.utc).timestamp() * 1000)
        candidate += timedelta(minutes=1)
    raise ValueError("cron has no matching run time within 1 year")


def cron_expression_for_timestamp_ms(timestamp_ms: int, timezone_name: str = "UTC") -> str:
    tzinfo = ZoneInfo(timezone_name)
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(tzinfo)
    return f"{dt.minute} {dt.hour} {dt.day} {dt.month} {dt.weekday()}"


def create_schedule_definition(
    runtime_root: Path | str,
    *,
    schedule_id: str,
    chat_key: str,
    message: str,
    cron: str,
    timezone_name: str = "UTC",
    max_runs: int | None = None,
    misfire_policy: str = "fire_once_now",
    concurrency_policy: str = "skip_if_running",
) -> ScheduleDefinition:
    next_run_at = compute_next_cron_run_on_or_after(cron, timezone_name, now_ms())
    definition = ScheduleDefinition(
        schedule_id=schedule_id,
        chat_key=chat_key,
        message=message,
        cron=cron,
        timezone_name=timezone_name,
        next_run_at=next_run_at,
        max_runs=max_runs,
        misfire_policy=misfire_policy,
        concurrency_policy=concurrency_policy,
    )
    write_json_atomic(
        schedule_definition_file(runtime_root, schedule_id),
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
        },
    )
    return definition


def create_one_shot_schedule(
    runtime_root: Path | str,
    *,
    schedule_id: str,
    chat_key: str,
    message: str,
    run_at_ms: int,
) -> ScheduleDefinition:
    normalized = ((run_at_ms + 59999) // 60000) * 60000
    cron = cron_expression_for_timestamp_ms(normalized, "UTC")
    definition = ScheduleDefinition(
        schedule_id=schedule_id,
        chat_key=chat_key,
        message=message,
        cron=cron,
        timezone_name="UTC",
        next_run_at=normalized,
        max_runs=1,
        misfire_policy="fire_once_now",
        concurrency_policy="skip_if_running",
    )
    write_json_atomic(
        schedule_definition_file(runtime_root, schedule_id),
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
        },
    )
    return definition


def due_schedule_definitions(runtime_root: Path | str, current_ms: int | None = None) -> list[ScheduleDefinition]:
    now_value = current_ms if current_ms is not None else now_ms()
    return [item for item in list_schedule_definitions(runtime_root) if item.enabled and item.next_run_at <= now_value]


def build_scheduled_job(definition: ScheduleDefinition, run_at_ms: int) -> ScheduledJob:
    return ScheduledJob(
        request_id=f"{run_at_ms:013d}-{definition.schedule_id}",
        schedule_id=definition.schedule_id,
        chat_key=definition.chat_key,
        message=definition.message,
        run_at=run_at_ms,
        created_at=now_ms(),
    )


def scheduled_job_file(root: Path, job: ScheduledJob) -> Path:
    return root / f"{job.run_at:013d}-{job.request_id}.json"


def write_scheduled_job(path: Path, job: ScheduledJob) -> ScheduledJob:
    write_json_atomic(
        path,
        {
            "requestId": job.request_id,
            "scheduleId": job.schedule_id,
            "chatKey": job.chat_key,
            "message": job.message,
            "runAt": job.run_at,
            "createdAt": job.created_at,
        },
    )
    return job


def read_scheduled_job(path: Path) -> ScheduledJob | None:
    payload = read_json_file(path)
    if not payload:
        return None
    return ScheduledJob(
        request_id=str(payload["requestId"]),
        schedule_id=str(payload["scheduleId"]),
        chat_key=str(payload["chatKey"]),
        message=str(payload["message"]),
        run_at=int(payload["runAt"]),
        created_at=int(payload["createdAt"]),
    )


def iter_due_scheduled_job_files(runtime_root: Path | str, current_ms: int | None = None) -> list[Path]:
    now_value = current_ms if current_ms is not None else now_ms()
    result: list[Path] = []
    for root in (schedule_processing_root(runtime_root), schedule_pending_root(runtime_root)):
        if not root.exists():
            continue
        for item in sorted(root.glob("*.json")):
            job = read_scheduled_job(item)
            if job is None:
                result.append(item)
                continue
            if root == schedule_processing_root(runtime_root) or job.run_at <= now_value:
                result.append(item)
    return result


def finalize_scheduled_job(runtime_root: Path | str, path: Path, *, ok: bool) -> None:
    target_root = schedule_done_root(runtime_root) if ok else schedule_failed_root(runtime_root)
    target_root.mkdir(parents=True, exist_ok=True)
    try:
        path.replace(target_root / path.name)
    except Exception:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def read_schedule_definition(runtime_root: Path | str, schedule_id: str) -> ScheduleDefinition | None:
    payload = read_json_file(schedule_definition_file(runtime_root, schedule_id))
    if not payload:
        return None
    return ScheduleDefinition(
        schedule_id=str(payload["scheduleId"]),
        chat_key=str(payload["chatKey"]),
        message=str(payload["message"]),
        cron=str(payload["cron"]),
        timezone_name=str(payload["timezone"]),
        next_run_at=int(payload["nextRunAt"]),
        enabled=bool(payload.get("enabled", True)),
        max_runs=int(payload["maxRuns"]) if payload.get("maxRuns") is not None else None,
        run_count=int(payload.get("runCount") or 0),
        misfire_policy=str(payload.get("misfirePolicy") or "fire_once_now"),
        concurrency_policy=str(payload.get("concurrencyPolicy") or "skip_if_running"),
    )


def write_schedule_definition(runtime_root: Path | str, definition: ScheduleDefinition) -> ScheduleDefinition:
    write_json_atomic(
        schedule_definition_file(runtime_root, definition.schedule_id),
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
        },
    )
    return definition


def pause_schedule_definition(runtime_root: Path | str, schedule_id: str) -> ScheduleDefinition:
    current = read_schedule_definition(runtime_root, schedule_id)
    if current is None:
        raise FileNotFoundError(f"schedule not found: {schedule_id}")
    return write_schedule_definition(
        runtime_root,
        ScheduleDefinition(
            schedule_id=current.schedule_id,
            chat_key=current.chat_key,
            message=current.message,
            cron=current.cron,
            timezone_name=current.timezone_name,
            next_run_at=current.next_run_at,
            enabled=False,
            max_runs=current.max_runs,
            run_count=current.run_count,
            misfire_policy=current.misfire_policy,
            concurrency_policy=current.concurrency_policy,
        ),
    )


def resume_schedule_definition(runtime_root: Path | str, schedule_id: str) -> ScheduleDefinition:
    current = read_schedule_definition(runtime_root, schedule_id)
    if current is None:
        raise FileNotFoundError(f"schedule not found: {schedule_id}")
    next_run_at = compute_next_cron_run_on_or_after(current.cron, current.timezone_name, now_ms())
    return write_schedule_definition(
        runtime_root,
        ScheduleDefinition(
            schedule_id=current.schedule_id,
            chat_key=current.chat_key,
            message=current.message,
            cron=current.cron,
            timezone_name=current.timezone_name,
            next_run_at=next_run_at,
            enabled=True,
            max_runs=current.max_runs,
            run_count=current.run_count,
            misfire_policy=current.misfire_policy,
            concurrency_policy=current.concurrency_policy,
        ),
    )


def delete_schedule_definition(runtime_root: Path | str, schedule_id: str) -> None:
    path = schedule_definition_file(runtime_root, schedule_id)
    if not path.exists():
        raise FileNotFoundError(f"schedule not found: {schedule_id}")
    path.unlink()
