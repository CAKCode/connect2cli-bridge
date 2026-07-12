from __future__ import annotations

import json
import time
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .layout import parse_chat_key
from .models import ScheduleDefinition, ScheduledJob

CRON_MONTH_NAMES = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
CRON_WEEKDAY_NAMES = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}

VALID_MISFIRE_POLICIES = {"fire_once_now", "skip_missed"}
VALID_CONCURRENCY_POLICIES = {"skip_if_running"}


def _parse_optional_bool(value, *, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean: {value}")


def _schedule_root(runtime_root: Path | str) -> Path:
    return Path(runtime_root).expanduser().resolve() / "schedules"


def _path_component(value: str) -> str:
    return quote(str(value or "").strip(), safe="._-")


def schedule_pending_root(runtime_root: Path | str) -> Path:
    root = _schedule_root(runtime_root) / "pending"
    root.mkdir(parents=True, exist_ok=True)
    return root


def schedule_processing_root(runtime_root: Path | str) -> Path:
    root = _schedule_root(runtime_root) / "processing"
    root.mkdir(parents=True, exist_ok=True)
    return root


def schedule_done_root(runtime_root: Path | str) -> Path:
    root = _schedule_root(runtime_root) / "done"
    root.mkdir(parents=True, exist_ok=True)
    return root


def schedule_failed_root(runtime_root: Path | str) -> Path:
    root = _schedule_root(runtime_root) / "failed"
    root.mkdir(parents=True, exist_ok=True)
    return root


def schedule_definition_root(runtime_root: Path | str) -> Path:
    root = _schedule_root(runtime_root) / "definitions"
    root.mkdir(parents=True, exist_ok=True)
    return root


def schedule_definition_file(runtime_root: Path | str, schedule_id: str) -> Path:
    return schedule_definition_root(runtime_root) / f"{_path_component(schedule_id)}.json"


def schedule_failed_marker_file(runtime_root: Path | str, schedule_id: str) -> Path:
    return schedule_failed_root(runtime_root) / f"{_path_component(schedule_id)}.json"


def schedule_done_marker_file(runtime_root: Path | str, schedule_id: str) -> Path:
    return schedule_done_root(runtime_root) / f"{_path_component(schedule_id)}.json"


def schedule_processing_marker_file(runtime_root: Path | str, schedule_id: str) -> Path:
    return schedule_processing_root(runtime_root) / f"definition-{_path_component(schedule_id)}.json"


def schedule_definition_processing_marker_file(runtime_root: Path | str, schedule_id: str) -> Path:
    return schedule_processing_marker_file(runtime_root, schedule_id)


def scheduled_job_file_name(run_at: int, request_id: str) -> str:
    return f"{int(run_at):013d}-{_path_component(request_id)}.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _read_json(path: Path) -> dict | None:
    if path.is_symlink():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def resolve_schedule_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid timezone: {name}") from exc


def validate_schedule_misfire_policy(policy: str) -> str:
    normalized = str(policy or "").strip() or "fire_once_now"
    if normalized not in VALID_MISFIRE_POLICIES:
        raise ValueError(f"invalid misfirePolicy: {normalized}")
    return normalized


def validate_schedule_concurrency_policy(policy: str) -> str:
    normalized = str(policy or "").strip() or "skip_if_running"
    if normalized not in VALID_CONCURRENCY_POLICIES:
        raise ValueError(f"invalid concurrencyPolicy: {normalized}")
    return normalized


def parse_cron_atom(token: str, names: dict[str, int], minimum: int, maximum: int, label: str) -> int:
    upper = token.strip().upper()
    if upper in names:
        value = names[upper]
    else:
        try:
            value = int(token)
        except ValueError as exc:
            raise ValueError(f"invalid {label} cron field: {token}") from exc
    if label == "day-of-week" and value == 7:
        value = 0
    if value < minimum or value > maximum:
        raise ValueError(f"{label} cron field out of range: {token}")
    return value


def parse_cron_field(
    expr: str,
    minimum: int,
    maximum: int,
    label: str,
    *,
    names: dict[str, int] | None = None,
    allow_question: bool = False,
) -> tuple[set[int], bool]:
    text = expr.strip()
    if allow_question and text == "?":
        text = "*"
    if not text:
        raise ValueError(f"empty {label} cron field")
    wildcard = text == "*"
    values: set[int] = set()
    name_map = names or {}

    for part in text.split(","):
        item = part.strip()
        if not item:
            raise ValueError(f"invalid {label} cron field")
        step = 1
        if "/" in item:
            base, step_text = item.split("/", 1)
            try:
                step = int(step_text)
            except ValueError as exc:
                raise ValueError(f"invalid {label} cron step: {item}") from exc
            if step <= 0:
                raise ValueError(f"invalid {label} cron step: {item}")
        else:
            base = item

        if base == "*":
            start = minimum
            end = maximum
        elif "-" in base:
            start_text, end_text = base.split("-", 1)
            start = parse_cron_atom(start_text, name_map, minimum, maximum, label)
            end = parse_cron_atom(end_text, name_map, minimum, maximum, label)
            if end < start:
                raise ValueError(f"invalid {label} cron range: {item}")
        else:
            start = parse_cron_atom(base, name_map, minimum, maximum, label)
            end = start

        for value in range(start, end + 1, step):
            if label == "day-of-week" and value == 7:
                value = 0
            values.add(value)

    return values, wildcard


def parse_cron_expression(expr: str) -> dict[str, object]:
    parts = [part for part in str(expr or "").split() if part]
    if len(parts) != 5:
        raise ValueError("cron must contain exactly 5 fields")
    minutes, _ = parse_cron_field(parts[0], 0, 59, "minute")
    hours, _ = parse_cron_field(parts[1], 0, 23, "hour")
    month_days, month_days_any = parse_cron_field(parts[2], 1, 31, "day-of-month", allow_question=True)
    months, _ = parse_cron_field(parts[3], 1, 12, "month", names=CRON_MONTH_NAMES)
    weekdays, weekdays_any = parse_cron_field(parts[4], 0, 7, "day-of-week", names=CRON_WEEKDAY_NAMES, allow_question=True)
    return {
        "minutes": minutes,
        "hours": hours,
        "monthDays": month_days,
        "monthDaysAny": month_days_any,
        "months": months,
        "weekdays": weekdays,
        "weekdaysAny": weekdays_any,
    }


def cron_datetime_matches(dt: datetime, spec: dict[str, object]) -> bool:
    cron_weekday = (dt.weekday() + 1) % 7
    month_day_match = dt.day in spec["monthDays"]
    weekday_match = cron_weekday in spec["weekdays"]
    if spec["monthDaysAny"] and spec["weekdaysAny"]:
        day_match = True
    elif spec["monthDaysAny"]:
        day_match = weekday_match
    elif spec["weekdaysAny"]:
        day_match = month_day_match
    else:
        day_match = month_day_match or weekday_match
    return (
        dt.minute in spec["minutes"]
        and dt.hour in spec["hours"]
        and dt.month in spec["months"]
        and day_match
    )


def compute_next_cron_run_on_or_after(cron: str, timezone_name: str, current_ms: int) -> int:
    spec = parse_cron_expression(cron)
    tzinfo = resolve_schedule_timezone(timezone_name)
    earliest = datetime.fromtimestamp(current_ms / 1000, tz=timezone.utc).astimezone(tzinfo)
    candidate = earliest.replace(second=0, microsecond=0)
    if earliest.second or earliest.microsecond:
        candidate += timedelta(minutes=1)
    limit = candidate + timedelta(days=366 * 5)
    while candidate <= limit:
        if cron_datetime_matches(candidate, spec):
            return int(candidate.astimezone(timezone.utc).timestamp() * 1000)
        candidate += timedelta(minutes=1)
    raise ValueError("cron has no matching run time within 5 years")


def _parse_schedule_definition_payload(payload: dict | None) -> ScheduleDefinition | None:
    if not isinstance(payload, dict):
        return None
    try:
        schedule_id = str(payload["schedule_id"] or "").strip()
        chat_key = str(payload["chat_key"] or "").strip()
        message = str(payload["message"] or "").strip()
        cron_raw = payload.get("cron")
        cron = None if cron_raw in (None, "") else str(cron_raw).strip() or None
        timezone_raw = payload.get("timezone_name")
        timezone_name = None if timezone_raw in (None, "") else str(timezone_raw).strip() or None
        next_run_at = int(payload["next_run_at"])
        enabled = _parse_optional_bool(payload.get("enabled", True), default=True)
        max_runs_raw = payload.get("max_runs")
        max_runs = None if max_runs_raw in (None, "") else int(max_runs_raw)
        run_count = int(payload.get("run_count", 0))
        misfire_policy = str(payload.get("misfire_policy") or "fire_once_now").strip() or "fire_once_now"
        concurrency_policy = str(payload.get("concurrency_policy") or "skip_if_running").strip() or "skip_if_running"
        run_at_raw = payload.get("run_at_ms")
        run_at_ms = None if run_at_raw in (None, "") else int(run_at_raw)
        bot_id_raw = payload.get("bot_id")
        bot_id = None if bot_id_raw in (None, "") else str(bot_id_raw).strip() or None
    except (KeyError, TypeError, ValueError):
        return None
    if not schedule_id or not message:
        return None
    try:
        parse_chat_key(chat_key)
        validate_schedule_misfire_policy(misfire_policy)
        validate_schedule_concurrency_policy(concurrency_policy)
    except ValueError:
        return None
    return ScheduleDefinition(
        schedule_id=schedule_id,
        chat_key=chat_key,
        message=message,
        cron=cron,
        timezone_name=timezone_name,
        next_run_at=next_run_at,
        enabled=enabled,
        max_runs=max_runs,
        run_count=run_count,
        misfire_policy=misfire_policy,
        concurrency_policy=concurrency_policy,
        run_at_ms=run_at_ms,
        bot_id=bot_id,
    )


def parse_scheduled_job_payload(payload: dict | None) -> ScheduledJob | None:
    if not isinstance(payload, dict):
        return None
    try:
        request_id = str(payload["request_id"] or "").strip()
        schedule_id = str(payload["schedule_id"] or "").strip()
        chat_key = str(payload["chat_key"] or "").strip()
        message = str(payload["message"] or "").strip()
        run_at = int(payload["run_at"])
        created_at = int(payload["created_at"])
        bot_id_raw = payload.get("bot_id")
        bot_id = None if bot_id_raw in (None, "") else str(bot_id_raw).strip() or None
    except (KeyError, TypeError, ValueError):
        return None
    if not request_id or not schedule_id or not message:
        return None
    try:
        parse_chat_key(chat_key)
    except ValueError:
        return None
    return ScheduledJob(
        request_id=request_id,
        schedule_id=schedule_id,
        chat_key=chat_key,
        message=message,
        run_at=run_at,
        created_at=created_at,
        bot_id=bot_id,
    )


def write_schedule_definition(runtime_root: Path | str, definition: ScheduleDefinition) -> ScheduleDefinition:
    _write_json(schedule_definition_file(runtime_root, definition.schedule_id), asdict(definition))
    return definition


def read_schedule_definition(runtime_root: Path | str, schedule_id: str) -> ScheduleDefinition | None:
    payload = _read_json(schedule_definition_file(runtime_root, schedule_id))
    return _parse_schedule_definition_payload(payload)


def list_schedule_definitions(runtime_root: Path | str, *, bot_id: str | None = None) -> list[ScheduleDefinition]:
    items = []
    for path in sorted(schedule_definition_root(runtime_root).glob("*.json")):
        definition = _parse_schedule_definition_payload(_read_json(path))
        if definition is None:
            continue
        if bot_id is not None and definition.bot_id not in {None, bot_id}:
            continue
        items.append(definition)
    return items


def advance_schedule_definition_after_success(
    runtime_root: Path | str,
    schedule_id: str,
    *,
    current_ms: int | None = None,
) -> ScheduleDefinition | None:
    definition = read_schedule_definition(runtime_root, schedule_id)
    if definition is None:
        return None
    now_ms = int(time.time() * 1000) if current_ms is None else int(current_ms)
    next_run_count = definition.run_count + 1
    if definition.max_runs is not None and next_run_count >= definition.max_runs:
        next_definition = replace(
            definition,
            enabled=False,
            next_run_at=0,
            run_count=next_run_count,
        )
    elif definition.cron:
        next_run_at = compute_next_cron_run_on_or_after(
            definition.cron,
            definition.timezone_name or "UTC",
            now_ms + 1,
        )
        next_definition = replace(
            definition,
            next_run_at=next_run_at,
            run_count=next_run_count,
        )
    else:
        next_definition = replace(
            definition,
            enabled=False,
            next_run_at=0,
            run_count=next_run_count,
        )
    return write_schedule_definition(runtime_root, next_definition)


def create_schedule_definition(
    runtime_root: Path | str,
    *,
    schedule_id: str,
    chat_key: str,
    message: str,
    cron: str,
    timezone_name: str,
    max_runs: int | None = None,
    misfire_policy: str = "fire_once_now",
    concurrency_policy: str = "skip_if_running",
    bot_id: str | None = None,
) -> ScheduleDefinition:
    parse_chat_key(chat_key)
    normalized_max_runs = None if max_runs in (None, "") else int(max_runs)
    if normalized_max_runs is not None and normalized_max_runs <= 0:
        raise ValueError("maxRuns must be > 0")
    normalized_misfire_policy = validate_schedule_misfire_policy(misfire_policy)
    normalized_concurrency_policy = validate_schedule_concurrency_policy(concurrency_policy)
    definition = ScheduleDefinition(
        schedule_id=schedule_id,
        chat_key=chat_key,
        message=message,
        cron=cron,
        timezone_name=timezone_name,
        next_run_at=compute_next_cron_run_on_or_after(cron, timezone_name, int(time.time() * 1000) + 1),
        enabled=True,
        max_runs=normalized_max_runs,
        run_count=0,
        misfire_policy=normalized_misfire_policy,
        concurrency_policy=normalized_concurrency_policy,
        bot_id=bot_id,
    )
    return write_schedule_definition(runtime_root, definition)


def create_one_shot_schedule(
    runtime_root: Path | str,
    *,
    schedule_id: str,
    chat_key: str,
    message: str,
    run_at_ms: int,
    bot_id: str | None = None,
) -> ScheduleDefinition:
    parse_chat_key(chat_key)
    definition = ScheduleDefinition(
        schedule_id=schedule_id,
        chat_key=chat_key,
        message=message,
        cron=None,
        timezone_name=None,
        next_run_at=run_at_ms,
        enabled=True,
        max_runs=1,
        run_count=0,
        misfire_policy="fire_once_now",
        concurrency_policy="skip_if_running",
        run_at_ms=run_at_ms,
        bot_id=bot_id,
    )
    return write_schedule_definition(runtime_root, definition)


def due_schedule_definitions(runtime_root: Path | str, *, current_ms: int, bot_id: str | None = None) -> list[ScheduleDefinition]:
    return [item for item in list_schedule_definitions(runtime_root, bot_id=bot_id) if item.enabled and item.next_run_at <= current_ms]


def pause_schedule_definition(runtime_root: Path | str, schedule_id: str) -> ScheduleDefinition:
    definition = read_schedule_definition(runtime_root, schedule_id)
    if definition is None:
        raise FileNotFoundError(schedule_id)
    for root in (schedule_pending_root(runtime_root), schedule_processing_root(runtime_root)):
        for path in root.glob("*.json"):
            if path.is_symlink():
                path.unlink(missing_ok=True)
                continue
            payload = _read_json(path)
            if not payload or str(payload.get("schedule_id") or payload.get("scheduleId") or "") != schedule_id:
                continue
            path.replace(schedule_failed_root(runtime_root) / path.name)
    schedule_definition_processing_marker_file(runtime_root, schedule_id).unlink(missing_ok=True)
    return write_schedule_definition(runtime_root, replace(definition, enabled=False))


def resume_schedule_definition(runtime_root: Path | str, schedule_id: str) -> ScheduleDefinition:
    definition = read_schedule_definition(runtime_root, schedule_id)
    if definition is None:
        raise FileNotFoundError(schedule_id)
    if definition.cron:
        next_run_at = compute_next_cron_run_on_or_after(definition.cron, definition.timezone_name or "UTC", int(time.time() * 1000))
    else:
        next_run_at = max(int(time.time() * 1000), int(definition.run_at_ms or 0))
    return write_schedule_definition(runtime_root, replace(definition, enabled=True, next_run_at=next_run_at))


def delete_schedule_definition(runtime_root: Path | str, schedule_id: str) -> None:
    for root in (
        schedule_pending_root(runtime_root),
        schedule_processing_root(runtime_root),
        schedule_failed_root(runtime_root),
        schedule_done_root(runtime_root),
    ):
        for path in root.glob("*.json"):
            if path.is_symlink():
                path.unlink(missing_ok=True)
                continue
            payload = _read_json(path)
            if not payload or str(payload.get("schedule_id") or payload.get("scheduleId") or "") != schedule_id:
                continue
            path.unlink(missing_ok=True)
    schedule_definition_processing_marker_file(runtime_root, schedule_id).unlink(missing_ok=True)
    schedule_failed_marker_file(runtime_root, schedule_id).unlink(missing_ok=True)
    schedule_done_marker_file(runtime_root, schedule_id).unlink(missing_ok=True)
    schedule_definition_file(runtime_root, schedule_id).unlink(missing_ok=True)


def write_scheduled_job(path: Path, job: ScheduledJob) -> None:
    _write_json(path, asdict(job))
