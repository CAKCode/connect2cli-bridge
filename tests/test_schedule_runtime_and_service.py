import asyncio
import json

import pytest
from workspace_bridge.config import build_bot_from_app_config, load_app_config
from workspace_bridge.models import WeComBotRuntime
from workspace_bridge.schedule import (
    ScheduledJob,
    create_one_shot_schedule,
    schedule_done_root,
    schedule_failed_root,
    schedule_pending_root,
    schedule_processing_root,
    write_scheduled_job,
)
from workspace_bridge.runtime import stable_session_id
from workspace_bridge.schedule_runtime import process_due_schedules_once, process_scheduled_jobs_once
from workspace_bridge.service import APP_WECOM_RUNTIME_KEY, create_app
from aiohttp import web


def write_secret(path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def make_config(tmp_path):
    secret_file = tmp_path / ".secrets" / "bot.secret"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    write_secret(secret_file, "secret-value\n")
    return load_app_config(
        {
            "RUNTIME_ROOT": str(tmp_path / "runtime"),
            "WECOM_BOT_NAME": "default",
            "WECOM_BOT_ID": "bot-1",
            "WECOM_BOT_SECRET_FILE": str(secret_file),
            "WECOM_BOT_SOURCE_DIR": str(source_dir),
            "SCHEDULE_POLL_MS": "1000",
        }
    )


async def test_service_create_schedule_rejects_invalid_json_missing_fields_and_bad_cron(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules")

    class BadJsonRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            raise json.JSONDecodeError("bad", "{", 1)

    class JsonRequest:
        def __init__(self, app, payload):
            self.app = app
            self._payload = payload

        async def json(self):
            return self._payload

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(BadJsonRequest(app))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "request body must be valid JSON"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "chatKey required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "single:alice"}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "message required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "single:alice", "message": "hello"}))
    assert excinfo.value.status == 400
    assert excinfo.value.text == "cron required"

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "single:alice", "message": "hello", "cron": "bad cron"}))
    assert excinfo.value.status == 400
    assert "cron must contain exactly 5 fields" in excinfo.value.text

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(JsonRequest(app, {"chatKey": "invalid", "message": "hello", "cron": "0 9 * * *"}))
    assert excinfo.value.status == 400
    assert "invalid chat key" in excinfo.value.text

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(
            JsonRequest(
                app,
                {"chatKey": "single:alice", "message": "hello", "cron": "0 9 * * *", "misfirePolicy": "bad"},
            )
        )
    assert excinfo.value.status == 400
    assert "invalid misfirePolicy" in excinfo.value.text

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(
            JsonRequest(
                app,
                {"chatKey": "single:alice", "message": "hello", "cron": "0 9 * * *", "concurrencyPolicy": "bad"},
            )
        )
    assert excinfo.value.status == 400
    assert "invalid concurrencyPolicy" in excinfo.value.text


async def test_process_due_schedules_once_executes_due_schedule(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    calls = []

    async def fake_execute_and_deliver_message(_config, _runtime, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == ["schedule-1"]
    assert calls == [("single:alice", "hello")]
    done_files = list(schedule_done_root(config.runtime_root).glob("*.json"))
    assert len(done_files) == 1


async def test_process_due_schedules_once_skips_execution_when_runtime_disconnected(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    runtime = WeComBotRuntime(
        config=build_bot_from_app_config(config),
        pending_requests={},
        pending_streams={},
        pending_finals={},
    )
    runtime.connected = False
    calls = []

    async def fake_execute_and_deliver_message(_config, _runtime, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config, runtime)

    assert executed == []
    assert calls == []


async def test_process_due_schedules_once_records_failed_definition_delivery(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    (schedule_done_root(config.runtime_root) / "schedule-1.json").write_text("{}", encoding="utf-8")

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("delivery failed")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == []
    failed_files = list(schedule_failed_root(config.runtime_root).glob("*.json"))
    assert len(failed_files) == 1
    payload = json.loads(failed_files[0].read_text(encoding="utf-8"))
    assert payload["scheduleId"] == "schedule-1"
    assert payload["error"] == "delivery failed"
    assert not (schedule_done_root(config.runtime_root) / "schedule-1.json").exists()
    from workspace_bridge.schedule import read_schedule_definition

    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.enabled is True
    assert updated.next_run_at == 0


async def test_process_due_schedules_once_does_not_advance_when_final_delivery_is_deferred(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("final delivery deferred until connection recovers")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == []
    assert not list(schedule_failed_root(config.runtime_root).glob("*.json"))
    pending_files = list(schedule_pending_root(config.runtime_root).glob("*.json"))
    assert len(pending_files) == 1
    from workspace_bridge.schedule import read_schedule_definition

    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.enabled is True
    assert updated.run_count == 0
    assert updated.next_run_at == 0


async def test_process_due_schedules_once_reschedules_failed_cron_delivery(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.schedule import create_schedule_definition, read_schedule_definition

    create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="*/5 * * * *",
        timezone_name="UTC",
    )
    definition = read_schedule_definition(config.runtime_root, "schedule-1")
    assert definition is not None
    from workspace_bridge.schedule import write_schedule_definition

    write_schedule_definition(config.runtime_root, definition.__class__(**{**definition.__dict__, "next_run_at": 0}))

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("delivery failed")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == []
    failed_files = list(schedule_failed_root(config.runtime_root).glob("*.json"))
    assert len(failed_files) == 1
    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.next_run_at > 0


async def test_process_due_schedules_once_skips_missed_when_policy_demands(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.schedule import write_schedule_definition, ScheduleDefinition

    write_schedule_definition(
        config.runtime_root,
        ScheduleDefinition(
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            cron="0 9 * * *",
            timezone_name="UTC",
            next_run_at=int(__import__("time").time() * 1000) - 10_000,
            enabled=True,
            max_runs=1,
            run_count=0,
            misfire_policy="skip_missed",
            concurrency_policy="skip_if_running",
        ),
    )
    calls = []

    async def fake_execute_and_deliver_message(_config, _runtime, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == []
    assert calls == []
    from workspace_bridge.schedule import read_schedule_definition

    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.next_run_at > 0


async def test_process_scheduled_jobs_once_moves_failed_jobs_to_failed(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    job = ScheduledJob(
        request_id="job-1",
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at=0,
        created_at=0,
    )
    write_scheduled_job(pending_root / "0000000000000-job-1.json", job)

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == []
    failed_files = list(schedule_failed_root(config.runtime_root).glob("*.json"))
    assert len(failed_files) == 1
    payload = json.loads(failed_files[0].read_text(encoding="utf-8"))
    assert payload["requestId"] == "job-1"
    assert payload["scheduleId"] == "schedule-1"
    assert payload["chatKey"] == "single:alice"
    assert payload["error"] == "boom"


async def test_process_scheduled_jobs_once_claims_pending_job_before_run(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    job = ScheduledJob(
        request_id="job-1",
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at=0,
        created_at=0,
    )
    write_scheduled_job(pending_root / "0000000000000-job-1.json", job)
    processing_files_seen = []

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        processing_files_seen.extend(sorted((config.runtime_root / "schedules" / "processing").glob("*.json")))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == ["job-1"]
    assert processing_files_seen
    assert not list(schedule_pending_root(config.runtime_root).glob("*.json"))


async def test_process_scheduled_jobs_once_moves_delivery_failures_to_failed(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    job = ScheduledJob(
        request_id="job-1",
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at=0,
        created_at=0,
    )
    write_scheduled_job(pending_root / "0000000000000-job-1.json", job)

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("delivery failed")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == []
    failed_files = list(schedule_failed_root(config.runtime_root).glob("*.json"))
    assert len(failed_files) == 1
    payload = json.loads(failed_files[0].read_text(encoding="utf-8"))
    assert payload["requestId"] == "job-1"
    assert payload["scheduleId"] == "schedule-1"
    assert payload["chatKey"] == "single:alice"
    assert payload["error"] == "delivery failed"


async def test_process_scheduled_jobs_once_requeues_when_final_delivery_is_deferred(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    job = ScheduledJob(
        request_id="job-1",
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at=0,
        created_at=0,
    )
    write_scheduled_job(pending_root / "0000000000000-job-1.json", job)

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("final delivery deferred until connection recovers")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == []
    assert not list(schedule_failed_root(config.runtime_root).glob("*.json"))
    pending_files = list(schedule_pending_root(config.runtime_root).glob("*.json"))
    assert len(pending_files) == 1
    assert pending_files[0].name == "0000000000000-job-1.json"


async def test_process_scheduled_jobs_once_skips_rerun_while_deferred_final_is_pending(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    runtime = WeComBotRuntime(
        config=build_bot_from_app_config(config),
        pending_requests={},
        pending_streams={},
        pending_finals={"job:job-1": [{"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}]},
    )
    runtime.connected = True
    calls = []

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        calls.append("executed")
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config, runtime)

    assert executed == []
    assert calls == []
    pending_files = list(schedule_pending_root(config.runtime_root).glob("*.json"))
    assert len(pending_files) == 1
    assert "single:alice" not in runtime.active_schedule_runs


async def test_process_scheduled_jobs_once_clears_active_run_after_failure(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    runtime = WeComBotRuntime(
        config=build_bot_from_app_config(config),
        pending_requests={},
        pending_streams={},
        pending_finals={},
    )
    runtime.connected = True

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("delivery failed")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config, runtime)

    assert executed == []
    assert "single:alice" not in runtime.active_schedule_runs


async def test_process_scheduled_jobs_once_treats_externally_cancelled_schedule_task_as_local_cancel(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    runtime = WeComBotRuntime(
        config=build_bot_from_app_config(config),
        pending_requests={},
        pending_streams={},
        pending_finals={},
    )
    runtime.connected = True
    runtime.suppressed_schedule_cancels.add(("single:alice", "job-1"))

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config, runtime)

    assert executed == []
    assert "single:alice" not in runtime.suppressed_schedule_cancels
    assert "single:alice" not in runtime.active_schedule_runs


async def test_process_scheduled_jobs_once_does_not_requeue_after_delete_cancel(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.schedule import create_schedule_definition, delete_schedule_definition

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    delete_schedule_definition(config.runtime_root, definition.schedule_id)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    runtime = WeComBotRuntime(
        config=build_bot_from_app_config(config),
        pending_requests={},
        pending_streams={},
        pending_finals={},
    )
    runtime.connected = True
    runtime.suppressed_schedule_cancels.add(("single:alice", "job-1"))

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config, runtime)

    assert executed == []
    assert list(schedule_pending_root(config.runtime_root).glob("*.json")) == []
    assert ("single:alice", "job-1") not in runtime.terminal_schedule_cancels


async def test_process_due_schedules_once_treats_externally_cancelled_schedule_task_as_local_cancel(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    runtime = WeComBotRuntime(
        config=build_bot_from_app_config(config),
        pending_requests={},
        pending_streams={},
        pending_finals={},
    )
    runtime.connected = True
    runtime.suppressed_schedule_cancels.add(("single:alice", "schedule-1-0"))

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config, runtime)

    assert executed == []
    pending_files = list(schedule_pending_root(config.runtime_root).glob("*.json"))
    assert len(pending_files) == 1
    assert "single:alice" not in runtime.active_schedule_runs
    assert ("single:alice", "schedule-1-0") not in runtime.suppressed_schedule_cancels
    assert ("single:alice", "schedule-1-0") not in runtime.terminal_schedule_cancels


async def test_process_due_schedules_once_does_not_requeue_after_pause_cancel(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    from workspace_bridge.schedule import pause_schedule_definition, read_schedule_definition

    pause_schedule_definition(config.runtime_root, "schedule-1")
    runtime = WeComBotRuntime(
        config=build_bot_from_app_config(config),
        pending_requests={},
        pending_streams={},
        pending_finals={},
    )
    runtime.connected = True
    runtime.suppressed_schedule_cancels.add(("single:alice", "schedule-1-0"))

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config, runtime)

    assert executed == []
    assert list(schedule_pending_root(config.runtime_root).glob("*.json")) == []
    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.enabled is False
    assert ("single:alice", "schedule-1-0") not in runtime.terminal_schedule_cancels


async def test_process_scheduled_jobs_once_reclaims_orphaned_processing_job(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    processing_root = config.runtime_root / "schedules" / "processing"
    processing_root.mkdir(parents=True, exist_ok=True)
    job = ScheduledJob(
        request_id="job-1",
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at=0,
        created_at=0,
    )
    orphan = processing_root / "0000000000000-job-1.123.processing.json"
    write_scheduled_job(orphan, job)
    old = __import__("time").time() - 120
    __import__("os").utime(orphan, (old, old))

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == ["job-1"]
    assert list(schedule_done_root(config.runtime_root).glob("*.json"))


async def test_process_scheduled_jobs_once_success_advances_schedule_definition(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == ["job-1"]
    from workspace_bridge.schedule import read_schedule_definition

    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.enabled is False
    assert updated.run_count == 1
    assert updated.next_run_at == 0
    assert (schedule_done_root(config.runtime_root) / "schedule-1.json").exists()


async def test_process_scheduled_jobs_once_success_clears_stale_schedule_failed_marker(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    failed_marker = schedule_failed_root(config.runtime_root) / "schedule-1.json"
    failed_marker.write_text("{}", encoding="utf-8")

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == ["job-1"]
    assert failed_marker.exists() is False


async def test_process_scheduled_jobs_once_failure_clears_stale_schedule_done_marker(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    done_marker = schedule_done_root(config.runtime_root) / "schedule-1.json"
    done_marker.write_text("{}", encoding="utf-8")

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        raise RuntimeError("delivery failed")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_scheduled_jobs_once(config)

    assert executed == []
    assert done_marker.exists() is False


async def test_process_scheduled_jobs_once_advances_schedule_before_removing_processing_job(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    pending_path = pending_root / "0000000000000-job-1.json"
    write_scheduled_job(
        pending_path,
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    calls = []

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        return "session-1", "done"

    from workspace_bridge import schedule_runtime as schedule_runtime_module

    original_advance = schedule_runtime_module.advance_schedule_definition_after_success

    def fake_advance(runtime_root, schedule_id, *, current_ms=None):
        processing_files = list((config.runtime_root / "schedules" / "processing").glob("*.json"))
        calls.append((schedule_id, bool(processing_files)))
        return original_advance(runtime_root, schedule_id, current_ms=current_ms)

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    monkeypatch.setattr(schedule_runtime_module, "advance_schedule_definition_after_success", fake_advance)
    executed = await process_scheduled_jobs_once(config)

    assert executed == ["job-1"]
    assert calls == [("schedule-1", True)]


async def test_process_due_schedules_once_reclaims_orphaned_definition_marker(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )
    processing_root = config.runtime_root / "schedules" / "processing"
    processing_root.mkdir(parents=True, exist_ok=True)
    marker = processing_root / "definition-schedule-1.json"
    marker.write_text("{}", encoding="utf-8")
    old = __import__("time").time() - 120
    __import__("os").utime(marker, (old, old))

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == ["schedule-1"]


async def test_process_due_schedules_once_respects_skip_if_running(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.schedule import ScheduleDefinition, write_schedule_definition
    processing_root = config.runtime_root / "schedules" / "processing"
    processing_root.mkdir(parents=True, exist_ok=True)
    job = ScheduledJob(
        request_id="job-1",
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at=0,
        created_at=0,
    )
    write_scheduled_job(processing_root / "0000000000000-job-1.json", job)
    write_schedule_definition(
        config.runtime_root,
        ScheduleDefinition(
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            cron="0 9 * * *",
            timezone_name="UTC",
            next_run_at=0,
            enabled=True,
            max_runs=None,
            run_count=0,
            misfire_policy="fire_once_now",
            concurrency_policy="skip_if_running",
        ),
    )
    calls = []

    async def fake_execute_and_deliver_message(_config, _runtime, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == []
    assert calls == []
    from workspace_bridge.schedule import read_schedule_definition

    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.next_run_at == 0


async def test_process_due_schedules_once_respects_skip_if_running_with_pending_job(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.schedule import ScheduleDefinition, write_schedule_definition, read_schedule_definition

    pending_root = schedule_pending_root(config.runtime_root)
    pending_root.mkdir(parents=True, exist_ok=True)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    write_schedule_definition(
        config.runtime_root,
        ScheduleDefinition(
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            cron="0 9 * * *",
            timezone_name="UTC",
            next_run_at=0,
            enabled=True,
            max_runs=None,
            run_count=0,
            misfire_policy="fire_once_now",
            concurrency_policy="skip_if_running",
        ),
    )
    calls = []

    async def fake_execute_and_deliver_message(_config, _runtime, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == []
    assert calls == []
    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.next_run_at == 0


async def test_process_due_schedules_once_advances_recurring_schedule(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    from workspace_bridge.schedule import ScheduleDefinition, write_schedule_definition, read_schedule_definition

    write_schedule_definition(
        config.runtime_root,
        ScheduleDefinition(
            schedule_id="schedule-1",
            chat_key="single:alice",
            message="hello",
            cron="0 9 * * *",
            timezone_name="UTC",
            next_run_at=0,
            enabled=True,
            max_runs=None,
            run_count=0,
            misfire_policy="fire_once_now",
            concurrency_policy="skip_if_running",
        ),
    )

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == ["schedule-1"]
    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.next_run_at > 0


async def test_process_due_schedules_once_disables_one_shot_after_run(tmp_path, monkeypatch) -> None:
    config = make_config(tmp_path)
    create_one_shot_schedule(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )

    async def fake_execute_and_deliver_message(_config, _runtime, _message, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.execute_and_deliver_message", fake_execute_and_deliver_message)
    executed = await process_due_schedules_once(config)

    assert executed == ["schedule-1"]
    from workspace_bridge.schedule import read_schedule_definition

    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.enabled is False
    assert updated.run_count == 1
    assert updated.next_run_at == 0


async def test_service_schedule_endpoints_create_and_list(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)

    class CreateRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {
                "chatKey": "single:alice",
                "message": "hello",
                "cron": "0 9 * * *",
                "timezone": "Asia/Shanghai",
                "maxRuns": 3,
                "misfirePolicy": "skip_missed",
                "concurrencyPolicy": "skip_if_running",
            }

    create_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules")
    create_response = await create_route.handler(CreateRequest(app))
    create_payload = json.loads(create_response.text)
    assert create_payload["ok"] is True
    assert create_payload["scheduleId"].startswith("schedule-")
    assert create_payload["chatKey"] == "single:alice"
    assert create_payload["sessionId"] == stable_session_id(config.bot_id, "single:alice", config.source_dir)
    assert create_payload["workspaceId"].startswith("user:")
    assert create_payload["timezone"] == "Asia/Shanghai"
    assert create_payload["maxRuns"] == 3
    assert create_payload["misfirePolicy"] == "skip_missed"
    assert create_payload["concurrencyPolicy"] == "skip_if_running"

    list_route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/api/schedules")
    list_response = await list_route.handler(type("Req", (), {"app": app})())
    list_payload = json.loads(list_response.text)
    assert len(list_payload) == 1
    assert list_payload[0]["chatKey"] == "single:alice"
    assert list_payload[0]["sessionId"] == stable_session_id(config.bot_id, "single:alice", config.source_dir)
    assert list_payload[0]["workspaceId"].startswith("user:")
    assert list_payload[0]["timezone"] == "Asia/Shanghai"
    assert list_payload[0]["maxRuns"] == 3
    assert list_payload[0]["misfirePolicy"] == "skip_missed"
    assert list_payload[0]["concurrencyPolicy"] == "skip_if_running"


async def test_service_pause_schedule_clears_deferred_job_cache(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    from workspace_bridge.schedule import create_schedule_definition

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    pending_root = schedule_pending_root(config.runtime_root)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    runtime.pending_finals["job:job-1"] = [{"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}]
    runtime.reply_states["job:job-1"] = type("State", (), {"chat_key": "single:alice"})()

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    response = await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": definition.schedule_id}})())
    payload = json.loads(response.text)

    assert payload["enabled"] is False
    assert "job:job-1" not in runtime.pending_finals
    assert "job:job-1" not in runtime.reply_states


async def test_service_delete_schedule_clears_deferred_job_cache(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    from workspace_bridge.schedule import create_schedule_definition

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    pending_root = schedule_pending_root(config.runtime_root)
    write_scheduled_job(
        pending_root / "0000000000000-job-1.json",
        ScheduledJob(
            request_id="job-1",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    runtime.pending_finals["job:job-1"] = [{"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}]
    runtime.reply_states["job:job-1"] = type("State", (), {"chat_key": "single:alice"})()

    route = next(route for route in app.router.routes() if route.method == "DELETE" and route.resource.canonical == "/api/schedules/{schedule_id}")
    response = await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": definition.schedule_id}})())
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert "job:job-1" not in runtime.pending_finals
    assert "job:job-1" not in runtime.reply_states


async def test_service_pause_schedule_interrupts_active_schedule_run(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    from workspace_bridge.schedule import create_schedule_definition

    class FakeProcess:
        def __init__(self) -> None:
            self.terminated = False

        def terminate(self) -> None:
            self.terminated = True

        async def wait(self) -> None:
            return None

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    task = asyncio.create_task(asyncio.sleep(3600))
    process = FakeProcess()
    runtime.active_schedule_tasks["single:alice"] = task
    runtime.active_processes["single:alice"] = process
    runtime.active_schedule_runs["single:alice"] = (definition.schedule_id, "job-1")
    runtime.pending_finals["job:job-1"] = [{"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}]

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    response = await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": definition.schedule_id}})())
    payload = json.loads(response.text)

    assert payload["enabled"] is False
    assert process.terminated is True
    assert task.cancelled() is True
    assert "single:alice" not in runtime.active_schedule_runs
    assert "job:job-1" not in runtime.pending_finals


async def test_service_delete_schedule_interrupts_active_schedule_run(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    from workspace_bridge.schedule import create_schedule_definition

    class FakeProcess:
        def __init__(self) -> None:
            self.terminated = False

        def terminate(self) -> None:
            self.terminated = True

        async def wait(self) -> None:
            return None

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    task = asyncio.create_task(asyncio.sleep(3600))
    process = FakeProcess()
    runtime.active_schedule_tasks["single:alice"] = task
    runtime.active_processes["single:alice"] = process
    runtime.active_schedule_runs["single:alice"] = (definition.schedule_id, "job-1")
    runtime.pending_finals["job:job-1"] = [{"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}]

    route = next(route for route in app.router.routes() if route.method == "DELETE" and route.resource.canonical == "/api/schedules/{schedule_id}")
    response = await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": definition.schedule_id}})())
    payload = json.loads(response.text)

    assert payload["ok"] is True
    assert process.terminated is True
    assert task.cancelled() is True
    assert "single:alice" not in runtime.active_schedule_runs
    assert "job:job-1" not in runtime.pending_finals


async def test_service_pause_schedule_cancels_active_schedule_task_before_process_exists(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    from workspace_bridge.schedule import create_schedule_definition

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    task = asyncio.create_task(asyncio.sleep(3600))
    runtime.active_schedule_tasks["single:alice"] = task
    runtime.active_schedule_runs["single:alice"] = (definition.schedule_id, "job-1")

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    response = await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": definition.schedule_id}})())
    payload = json.loads(response.text)

    assert payload["enabled"] is False
    assert task.cancelled() is True
    assert "single:alice" not in runtime.active_schedule_tasks
    assert "single:alice" not in runtime.active_schedule_runs
    assert ("single:alice", "job-1") not in runtime.suppressed_schedule_cancels


async def test_service_pause_schedule_does_not_hang_on_slow_schedule_task_cancel(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    from workspace_bridge.schedule import create_schedule_definition

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )

    async def slow_cancel():
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await asyncio.sleep(3600)
            raise

    task = asyncio.create_task(slow_cancel())
    runtime.active_schedule_tasks["single:alice"] = task
    runtime.active_schedule_runs["single:alice"] = (definition.schedule_id, "job-1")

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    response = await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": definition.schedule_id}})())
    payload = json.loads(response.text)

    assert payload["enabled"] is False
    assert "single:alice" not in runtime.active_schedule_tasks
    assert "single:alice" not in runtime.active_schedule_runs
    task.cancel()
    with __import__("contextlib").suppress(asyncio.CancelledError):
        await task


async def test_service_pause_schedule_does_not_cancel_unrelated_message_task(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    from workspace_bridge.schedule import create_schedule_definition

    definition = create_schedule_definition(
        config.runtime_root,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    schedule_task = asyncio.create_task(asyncio.sleep(3600))
    message_task = asyncio.create_task(asyncio.sleep(3600))
    runtime.active_schedule_tasks["single:alice"] = schedule_task
    runtime.active_schedule_runs["single:alice"] = (definition.schedule_id, "job-1")
    runtime.active_message_tasks["single:alice"] = message_task

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    response = await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": definition.schedule_id}})())
    payload = json.loads(response.text)

    assert payload["enabled"] is False
    assert schedule_task.cancelled() is True
    assert message_task.cancelled() is False
    message_task.cancel()
    with __import__("contextlib").suppress(asyncio.CancelledError):
        await message_task


async def test_service_pause_schedule_not_found_does_not_clear_runtime_cache(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)
    runtime = app[APP_WECOM_RUNTIME_KEY]
    runtime.pending_finals["job:job-1"] = [{"headers": {"req_id": "generated"}, "body": {"msgtype": "stream"}}]
    runtime.reply_states["job:job-1"] = type("State", (), {"chat_key": "single:alice"})()

    route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")

    with pytest.raises(web.HTTPException) as excinfo:
        await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": "missing"}})())

    assert excinfo.value.status == 404
    assert "job:job-1" in runtime.pending_finals
    assert "job:job-1" in runtime.reply_states


async def test_service_schedule_endpoint_uses_distinct_ids_for_distinct_payloads(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)

    class CreateRequest:
        def __init__(self, app, payload):
            self.app = app
            self._payload = payload

        async def json(self):
            return self._payload

    create_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules")
    first = json.loads(
        (
            await create_route.handler(
                CreateRequest(app, {"chatKey": "single:alice", "message": "hello", "cron": "0 9 * * *", "timezone": "UTC"})
            )
        ).text
    )
    second = json.loads(
        (
            await create_route.handler(
                CreateRequest(app, {"chatKey": "single:bob__", "message": "hello", "cron": "0 9 * * *", "timezone": "Asia/Shanghai"})
            )
        ).text
    )

    assert first["scheduleId"] != second["scheduleId"]


async def test_service_schedule_endpoint_allows_duplicate_payload_as_distinct_schedule(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)

    class CreateRequest:
        def __init__(self, app, payload):
            self.app = app
            self._payload = payload

        async def json(self):
            return self._payload

    payload = {"chatKey": "single:alice", "message": "hello", "cron": "0 9 * * *"}
    create_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules")
    first = json.loads((await create_route.handler(CreateRequest(app, payload))).text)
    second = json.loads((await create_route.handler(CreateRequest(app, payload))).text)

    assert first["scheduleId"] != second["scheduleId"]


async def test_service_schedule_management_endpoints(tmp_path) -> None:
    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)

    class CreateRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "message": "hello", "cron": "0 9 * * *"}

    create_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules")
    create_response = await create_route.handler(CreateRequest(app))
    created = json.loads(create_response.text)
    schedule_id = created["scheduleId"]

    get_route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/api/schedules/{schedule_id}")
    get_response = await get_route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": schedule_id}})())
    get_payload = json.loads(get_response.text)
    assert get_payload["scheduleId"] == schedule_id
    assert get_payload["sessionId"] == stable_session_id(config.bot_id, "single:alice", config.source_dir)
    assert get_payload["workspaceId"].startswith("user:")
    assert get_payload["maxRuns"] is None
    assert get_payload["misfirePolicy"] == "fire_once_now"
    assert get_payload["concurrencyPolicy"] == "skip_if_running"

    pause_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    pause_response = await pause_route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": schedule_id}})())
    assert json.loads(pause_response.text)["enabled"] is False

    resume_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/resume")
    resume_response = await resume_route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": schedule_id}})())
    assert json.loads(resume_response.text)["enabled"] is True

    delete_route = next(route for route in app.router.routes() if route.method == "DELETE" and route.resource.canonical == "/api/schedules/{schedule_id}")
    delete_response = await delete_route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": schedule_id}})())
    assert json.loads(delete_response.text)["ok"] is True


async def test_service_schedule_endpoints_return_not_found_for_missing_id(tmp_path) -> None:
    from aiohttp import web

    config = make_config(tmp_path)
    config = config.__class__(**{**config.__dict__, "wecom_enabled": True})
    app = create_app(config)

    get_route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/api/schedules/{schedule_id}")
    pause_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    resume_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/resume")
    delete_route = next(route for route in app.router.routes() if route.method == "DELETE" and route.resource.canonical == "/api/schedules/{schedule_id}")

    for route in (get_route, pause_route, resume_route, delete_route):
        try:
            await route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": "missing"}})())
        except web.HTTPNotFound:
            pass
        else:
            raise AssertionError("expected HTTPNotFound")


async def test_service_schedule_endpoints_require_wecom_runtime(tmp_path) -> None:
    from aiohttp import web
    from workspace_bridge.service import create_app

    config = make_config(tmp_path)
    app = create_app(config)

    class CreateRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "message": "hello", "cron": "0 9 * * *"}

    targets = [
        next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/api/schedules"),
        next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules"),
        next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/api/schedules/{schedule_id}"),
        next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause"),
        next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/resume"),
        next(route for route in app.router.routes() if route.method == "DELETE" and route.resource.canonical == "/api/schedules/{schedule_id}"),
    ]

    requests = [
        type("Req", (), {"app": app})(),
        CreateRequest(app),
        type("Req", (), {"app": app, "match_info": {"schedule_id": "x"}})(),
        type("Req", (), {"app": app, "match_info": {"schedule_id": "x"}})(),
        type("Req", (), {"app": app, "match_info": {"schedule_id": "x"}})(),
        type("Req", (), {"app": app, "match_info": {"schedule_id": "x"}})(),
    ]

    for route, req in zip(targets, requests):
        try:
            await route.handler(req)
        except web.HTTPServiceUnavailable:
            pass
        else:
            raise AssertionError("expected HTTPServiceUnavailable")
