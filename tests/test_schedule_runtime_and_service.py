import json

from workspace_bridge.config import load_app_config
from workspace_bridge.schedule import (
    ScheduledJob,
    create_one_shot_schedule,
    schedule_done_root,
    schedule_failed_root,
    schedule_pending_root,
    write_scheduled_job,
)
from workspace_bridge.schedule_runtime import process_due_schedules_once, process_scheduled_jobs_once
from workspace_bridge.service import create_app


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

    async def fake_run_text_message_once(_config, _bot, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.run_text_message_once", fake_run_text_message_once)
    executed = await process_due_schedules_once(config)

    assert executed == ["schedule-1"]
    assert calls == [("single:alice", "hello")]
    done_files = list(schedule_done_root(config.runtime_root).glob("*.json"))
    assert len(done_files) == 1


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
            next_run_at=0,
            enabled=True,
            max_runs=1,
            run_count=0,
            misfire_policy="skip_missed",
            concurrency_policy="skip_if_running",
        ),
    )
    calls = []

    async def fake_run_text_message_once(_config, _bot, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.run_text_message_once", fake_run_text_message_once)
    executed = await process_due_schedules_once(config)

    assert executed == []
    assert calls == []


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

    async def fake_run_text_message_once(_config, _bot, _message, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("workspace_bridge.schedule_runtime.run_text_message_once", fake_run_text_message_once)
    executed = await process_scheduled_jobs_once(config)

    assert executed == []
    failed_files = list(schedule_failed_root(config.runtime_root).glob("*.json"))
    assert len(failed_files) == 1


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

    async def fake_run_text_message_once(_config, _bot, message, **_kwargs):
        calls.append((message.chat_key, message.content))
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.run_text_message_once", fake_run_text_message_once)
    executed = await process_due_schedules_once(config)

    assert executed == ["schedule-1"]
    assert calls == [("single:alice", "hello")]
    from workspace_bridge.schedule import read_schedule_definition

    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.next_run_at > 0


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
            concurrency_policy="enqueue",
        ),
    )

    async def fake_run_text_message_once(_config, _bot, _message, **_kwargs):
        return "session-1", "done"

    monkeypatch.setattr("workspace_bridge.schedule_runtime.run_text_message_once", fake_run_text_message_once)
    executed = await process_due_schedules_once(config)

    assert executed == ["schedule-1"]
    updated = read_schedule_definition(config.runtime_root, "schedule-1")
    assert updated is not None
    assert updated.next_run_at > 0


async def test_service_schedule_endpoints_create_and_list(tmp_path) -> None:
    config = make_config(tmp_path)
    app = create_app(config)

    class CreateRequest:
        def __init__(self, app):
            self.app = app

        async def json(self):
            return {"chatKey": "single:alice", "message": "hello", "cron": "0 9 * * *"}

    create_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules")
    create_response = await create_route.handler(CreateRequest(app))
    create_payload = json.loads(create_response.text)
    assert create_payload["ok"] is True

    list_route = next(route for route in app.router.routes() if route.method == "GET" and route.resource.canonical == "/api/schedules")
    list_response = await list_route.handler(type("Req", (), {"app": app})())
    list_payload = json.loads(list_response.text)
    assert len(list_payload) == 1
    assert list_payload[0]["chatKey"] == "single:alice"


async def test_service_schedule_management_endpoints(tmp_path) -> None:
    config = make_config(tmp_path)
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
    assert json.loads(get_response.text)["scheduleId"] == schedule_id

    pause_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/pause")
    pause_response = await pause_route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": schedule_id}})())
    assert json.loads(pause_response.text)["enabled"] is False

    resume_route = next(route for route in app.router.routes() if route.method == "POST" and route.resource.canonical == "/api/schedules/{schedule_id}/resume")
    resume_response = await resume_route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": schedule_id}})())
    assert json.loads(resume_response.text)["enabled"] is True

    delete_route = next(route for route in app.router.routes() if route.method == "DELETE" and route.resource.canonical == "/api/schedules/{schedule_id}")
    delete_response = await delete_route.handler(type("Req", (), {"app": app, "match_info": {"schedule_id": schedule_id}})())
    assert json.loads(delete_response.text)["ok"] is True
