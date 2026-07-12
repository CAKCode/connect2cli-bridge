import json

from workspace_bridge.schedule import (
    compute_next_cron_run_on_or_after,
    create_one_shot_schedule,
    create_schedule_definition,
    delete_schedule_definition,
    due_schedule_definitions,
    schedule_done_root,
    schedule_definition_root,
    schedule_definition_processing_marker_file,
    list_schedule_definitions,
    pause_schedule_definition,
    read_schedule_definition,
    resume_schedule_definition,
    schedule_failed_root,
    schedule_pending_root,
    schedule_processing_root,
    write_scheduled_job,
)
from workspace_bridge.models import ScheduledJob


def test_create_schedule_definition_persists_definition(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )

    assert definition.schedule_id == "schedule-1"
    stored = list_schedule_definitions(tmp_path)
    assert len(stored) == 1
    assert stored[0].chat_key == "single:alice"
    assert due_schedule_definitions(tmp_path, current_ms=int(__import__("time").time() * 1000)) == []


def test_create_schedule_definition_escapes_schedule_id_in_filesystem(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="folder/evil",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )

    stored = read_schedule_definition(tmp_path, "folder/evil")
    definition_files = list(schedule_definition_root(tmp_path).glob("*.json"))

    assert definition.schedule_id == "folder/evil"
    assert stored is not None
    assert stored.schedule_id == "folder/evil"
    assert len(definition_files) == 1
    assert not (schedule_definition_root(tmp_path) / "folder").exists()


def test_read_schedule_definition_returns_none_for_malformed_payload(tmp_path) -> None:
    (schedule_definition_root(tmp_path) / "broken.json").write_text('{"schedule_id":"broken"}', encoding="utf-8")

    assert read_schedule_definition(tmp_path, "broken") is None


def test_read_schedule_definition_returns_none_for_symlinked_definition(tmp_path) -> None:
    external = tmp_path / "external-definition.json"
    external.write_text(
        '{"schedule_id":"schedule-1","chat_key":"single:alice","message":"hello","cron":"0 9 * * *","timezone_name":"UTC","next_run_at":1,"enabled":true,"max_runs":1,"run_count":0,"misfire_policy":"fire_once_now","concurrency_policy":"skip_if_running"}',
        encoding="utf-8",
    )
    (schedule_definition_root(tmp_path) / "schedule-1.json").symlink_to(external)

    assert read_schedule_definition(tmp_path, "schedule-1") is None


def test_list_schedule_definitions_skips_malformed_payload(tmp_path) -> None:
    create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    (schedule_definition_root(tmp_path) / "broken.json").write_text('{"schedule_id":"broken"}', encoding="utf-8")

    stored = list_schedule_definitions(tmp_path)

    assert len(stored) == 1
    assert stored[0].schedule_id == "schedule-1"


def test_list_schedule_definitions_skips_symlinked_payload(tmp_path) -> None:
    create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    external = tmp_path / "external-definition.json"
    external.write_text(
        '{"schedule_id":"schedule-2","chat_key":"single:alice","message":"hello","cron":"0 9 * * *","timezone_name":"UTC","next_run_at":1,"enabled":true,"max_runs":1,"run_count":0,"misfire_policy":"fire_once_now","concurrency_policy":"skip_if_running"}',
        encoding="utf-8",
    )
    (schedule_definition_root(tmp_path) / "schedule-2.json").symlink_to(external)

    stored = list_schedule_definitions(tmp_path)

    assert len(stored) == 1
    assert stored[0].schedule_id == "schedule-1"


def test_schedule_read_json_ignores_non_object_json(tmp_path) -> None:
    (schedule_definition_root(tmp_path) / "array.json").write_text("[]", encoding="utf-8")

    assert read_schedule_definition(tmp_path, "array") is None


def test_create_one_shot_schedule_marks_due_when_past_now(tmp_path) -> None:
    definition = create_one_shot_schedule(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=0,
    )

    due = due_schedule_definitions(tmp_path, current_ms=definition.next_run_at)
    assert [item.schedule_id for item in due] == ["schedule-1"]


def test_compute_next_cron_run_on_or_after_returns_future_timestamp() -> None:
    next_run_at = compute_next_cron_run_on_or_after("0 9 * * *", "UTC", 0)

    assert next_run_at > 0


def test_pause_resume_and_delete_schedule_definition(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )

    paused = pause_schedule_definition(tmp_path, definition.schedule_id)
    assert paused.enabled is False

    resumed = resume_schedule_definition(tmp_path, definition.schedule_id)
    assert resumed.enabled is True
    assert resumed.next_run_at > 0

    delete_schedule_definition(tmp_path, definition.schedule_id)
    assert read_schedule_definition(tmp_path, definition.schedule_id) is None


def test_pause_schedule_definition_moves_pending_jobs_to_failed(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    pending_path = schedule_pending_root(tmp_path) / "0000000000000-job-1.json"
    write_scheduled_job(
        pending_path,
        ScheduledJob(
            request_id="job-1",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )

    pause_schedule_definition(tmp_path, definition.schedule_id)

    assert not pending_path.exists()
    assert (schedule_failed_root(tmp_path) / "0000000000000-job-1.json").exists()


def test_pause_schedule_definition_clears_processing_marker(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    marker = schedule_definition_processing_marker_file(tmp_path, definition.schedule_id)
    marker.write_text("{}", encoding="utf-8")

    pause_schedule_definition(tmp_path, definition.schedule_id)

    assert not marker.exists()


def test_pause_schedule_definition_removes_symlinked_pending_job_without_touching_target(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    external_job = tmp_path / "external-pending.json"
    external_job.write_text(
        json.dumps(
            {
                "requestId": "job-1",
                "scheduleId": definition.schedule_id,
                "chatKey": "single:alice",
                "message": "hello",
                "runAt": 0,
                "createdAt": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    pending_path = schedule_pending_root(tmp_path) / "0000000000000-job-1.json"
    pending_path.symlink_to(external_job)

    pause_schedule_definition(tmp_path, definition.schedule_id)

    assert not pending_path.exists()
    assert not __import__("os").path.lexists(pending_path)
    assert external_job.exists()


def test_delete_schedule_definition_clears_pending_and_processing_jobs(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    pending_path = schedule_pending_root(tmp_path) / "0000000000000-job-1.json"
    processing_path = schedule_processing_root(tmp_path) / "0000000000000-job-2.processing.json"
    write_scheduled_job(
        pending_path,
        ScheduledJob(
            request_id="job-1",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    write_scheduled_job(
        processing_path,
        ScheduledJob(
            request_id="job-2",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )

    delete_schedule_definition(tmp_path, definition.schedule_id)

    assert not pending_path.exists()
    assert not processing_path.exists()
    assert read_schedule_definition(tmp_path, definition.schedule_id) is None


def test_delete_schedule_definition_clears_processing_marker(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    marker = schedule_definition_processing_marker_file(tmp_path, definition.schedule_id)
    marker.write_text("{}", encoding="utf-8")

    delete_schedule_definition(tmp_path, definition.schedule_id)

    assert not marker.exists()


def test_delete_schedule_definition_removes_symlinked_done_job_without_touching_target(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    external_job = tmp_path / "external-done.json"
    external_job.write_text(
        json.dumps(
            {
                "requestId": "job-2",
                "scheduleId": definition.schedule_id,
                "chatKey": "single:alice",
                "message": "hello",
                "runAt": 0,
                "createdAt": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    done_job = schedule_done_root(tmp_path) / "0000000000000-job-2.json"
    done_job.symlink_to(external_job)

    delete_schedule_definition(tmp_path, definition.schedule_id)

    assert not done_job.exists()
    assert not __import__("os").path.lexists(done_job)
    assert external_job.exists()


def test_delete_schedule_definition_clears_failed_and_done_job_artifacts(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
    )
    failed_job = schedule_failed_root(tmp_path) / "0000000000000-job-1.json"
    done_job = schedule_done_root(tmp_path) / "0000000000000-job-2.json"
    write_scheduled_job(
        failed_job,
        ScheduledJob(
            request_id="job-1",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )
    write_scheduled_job(
        done_job,
        ScheduledJob(
            request_id="job-2",
            schedule_id=definition.schedule_id,
            chat_key="single:alice",
            message="hello",
            run_at=0,
            created_at=0,
        ),
    )

    delete_schedule_definition(tmp_path, definition.schedule_id)

    assert not failed_job.exists()
    assert not done_job.exists()


def test_resume_one_shot_schedule_preserves_one_shot_timing(tmp_path) -> None:
    definition = create_one_shot_schedule(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        run_at_ms=1234,
    )

    paused = pause_schedule_definition(tmp_path, definition.schedule_id)
    resumed = resume_schedule_definition(tmp_path, paused.schedule_id)

    assert resumed.cron is None
    assert resumed.next_run_at >= 1234


def test_create_schedule_definition_persists_misfire_and_supported_concurrency(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
        max_runs=3,
        misfire_policy="skip_missed",
        concurrency_policy="skip_if_running",
    )

    assert definition.max_runs == 3
    assert definition.misfire_policy == "skip_missed"
    assert definition.concurrency_policy == "skip_if_running"


def test_create_schedule_definition_rejects_invalid_chat_key(tmp_path) -> None:
    try:
        create_schedule_definition(
            tmp_path,
            schedule_id="schedule-1",
            chat_key="invalid",
            message="hello",
            cron="0 9 * * *",
            timezone_name="UTC",
        )
    except ValueError as exc:
        assert "invalid chat key" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_create_schedule_definition_rejects_invalid_policies(tmp_path) -> None:
    for misfire_policy, concurrency_policy in (
        ("bad", "skip_if_running"),
        ("fire_once_now", "bad"),
        ("fire_once_now", "enqueue"),
    ):
        try:
            create_schedule_definition(
                tmp_path,
                schedule_id="schedule-1",
                chat_key="single:alice",
                message="hello",
                cron="0 9 * * *",
                timezone_name="UTC",
                misfire_policy=misfire_policy,
                concurrency_policy=concurrency_policy,
            )
        except ValueError as exc:
            assert "invalid" in str(exc)
        else:
            raise AssertionError("expected ValueError")
