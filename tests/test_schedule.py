from workspace_bridge.schedule import (
    compute_next_cron_run_on_or_after,
    create_one_shot_schedule,
    create_schedule_definition,
    delete_schedule_definition,
    due_schedule_definitions,
    list_schedule_definitions,
    pause_schedule_definition,
    read_schedule_definition,
    resume_schedule_definition,
)


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


def test_create_schedule_definition_persists_misfire_and_concurrency(tmp_path) -> None:
    definition = create_schedule_definition(
        tmp_path,
        schedule_id="schedule-1",
        chat_key="single:alice",
        message="hello",
        cron="0 9 * * *",
        timezone_name="UTC",
        misfire_policy="skip_missed",
        concurrency_policy="enqueue",
    )

    assert definition.misfire_policy == "skip_missed"
    assert definition.concurrency_policy == "enqueue"
