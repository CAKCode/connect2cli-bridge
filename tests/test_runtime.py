from pathlib import Path

from workspace_bridge.runtime import (
    build_bot_config,
    cleanup_orphan_session_codex_homes,
    cleanup_stale_session_codex_homes,
    list_session_records,
    load_session_record,
    prepare_session_run,
    session_codex_home_root,
    stable_session_id,
)


def write_skill(root: Path, name: str, body: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body, encoding="utf-8")


def test_stable_session_id_is_deterministic() -> None:
    first = stable_session_id("bot-1", "single:alice")
    second = stable_session_id("bot-1", "single:alice")

    assert first == second


def test_prepare_session_run_builds_launch_spec_and_persists_session(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    write_skill(global_skill_dir, "deploy", "# deploy")
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
    )

    launch = prepare_session_run(bot, "single:alice")

    assert launch.cwd == launch.runtime_context.project_dir
    assert launch.session.bot_id == "bot-1"
    assert launch.session.bot_name == "codex"
    assert launch.session.chat_key == "single:alice"
    assert launch.env["WECOM_BRIDGE_SESSION_ID"] == launch.session.session_id
    assert launch.env["WECOM_BRIDGE_PROJECT_DIR"] == str(launch.cwd)
    assert launch.env["WECOM_BRIDGE_EXEC_MODE"] == "host"
    assert launch.env["CODEX_HOME"].startswith(str(runtime_root / ".bridge-codex-home" / "sessions"))
    assert launch.session.workfile_dir == launch.runtime_context.workfile_dir
    assert launch.runtime_context.codex_exec_mode == "host"
    assert launch.runtime_context.effective_skill_names == ("deploy",)

    stored = load_session_record(runtime_root, launch.session.session_id)
    assert stored is not None
    assert stored.workspace_id == launch.session.workspace_id
    assert stored.project_dir == launch.cwd
    assert stored.workfile_dir == launch.runtime_context.workfile_dir


def test_prepare_session_run_session_codex_home_copies_only_lightweight_root_state(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    codex_home = tmp_path / ".codex"
    codex_home.mkdir()
    (codex_home / "installation_id").write_text("install-1", encoding="utf-8")
    (codex_home / "logs_2.sqlite").write_text("ignore-me", encoding="utf-8")
    monkeypatch.setattr("workspace_bridge.context.DEFAULT_CODEX_HOME", codex_home)

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
    )

    launch = prepare_session_run(bot, "single:alice")
    session_home = Path(launch.env["CODEX_HOME"])

    assert (session_home / "installation_id").read_text(encoding="utf-8") == "install-1"
    assert (session_home / "logs_2.sqlite").exists() is False


def test_prepare_session_run_reuses_stable_session_id_for_same_chat(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
    )

    first = prepare_session_run(bot, "group-user:room-1:alice")
    second = prepare_session_run(bot, "group-user:room-1:alice")

    assert first.session.session_id == second.session.session_id
    assert first.session.workspace_id == second.session.workspace_id
    assert first.cwd == second.cwd


def test_prepare_session_run_uses_workspace_skills_over_global(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    write_skill(global_skill_dir, "deploy", "# global deploy")
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    write_skill(first_launch.workspace.workspace.skill_dir, "deploy", "# workspace deploy")
    second_launch = prepare_session_run(bot, "single:alice")

    assert "deploy" in second_launch.runtime_context.effective_skill_names


def test_prepare_session_run_rebuilds_session_skill_dir_without_stale_entries(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    write_skill(global_skill_dir, "deploy", "# global deploy")
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    session_home = Path(first_launch.env["CODEX_HOME"])
    stale_skill = session_home / "skills" / "stale-skill"
    stale_skill.mkdir(parents=True, exist_ok=True)
    (stale_skill / "SKILL.md").write_text("---\nname: stale\n---\nstale\n", encoding="utf-8")

    second_launch = prepare_session_run(bot, "single:alice")

    assert (Path(second_launch.env["CODEX_HOME"]) / "skills" / "stale-skill").exists() is False


def test_list_session_records_returns_latest_first_without_restoring_thread_info(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
    )

    first = prepare_session_run(bot, "single:alice")
    second = prepare_session_run(bot, "single:bob")
    from workspace_bridge.runtime import store_session_record
    from dataclasses import replace

    store_session_record(runtime_root, replace(first.session, thread_id="thread-a", last_run_at=1000, updated_at=1000))
    store_session_record(runtime_root, replace(second.session, thread_id="thread-b", last_run_at=2000, updated_at=2000))

    records = list_session_records(runtime_root, "bot-1")

    assert [item.session_id for item in records[:2]] == [second.session.session_id, first.session.session_id]
    assert records[0].thread_id is None

def test_prepare_session_run_preserves_host_exec_mode(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        global_skill_dir=global_skill_dir,
        chatfile_root=chatfile_root,
        codex_exec_mode="host",
    )

    launch = prepare_session_run(bot, "single:alice")

    assert launch.runtime_context.codex_exec_mode == "host"
    assert launch.env["WECOM_BRIDGE_EXEC_MODE"] == "host"


def test_cleanup_orphan_session_codex_homes_removes_unregistered_runtime_dirs(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    orphan = homes_root / "session-orphan"
    live = homes_root / "session-live"
    orphan.mkdir(parents=True, exist_ok=True)
    live.mkdir(parents=True, exist_ok=True)
    sessions_root = runtime_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    (sessions_root / "session-live.json").write_text('{"sessionId":"session-live"}', encoding="utf-8")

    removed = cleanup_orphan_session_codex_homes(runtime_root)

    assert removed == 1
    assert orphan.exists() is False
    assert live.exists() is True


def test_cleanup_stale_session_codex_homes_removes_only_expired_dirs(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    stale = homes_root / "session-stale"
    fresh = homes_root / "session-fresh"
    stale.mkdir(parents=True, exist_ok=True)
    fresh.mkdir(parents=True, exist_ok=True)
    sessions_root = runtime_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    now_ms = 1_000_000
    old_ms = now_ms - (30 * 60 * 1000) - 1
    (sessions_root / "session-stale.json").write_text(
        '{"sessionId":"session-stale","botId":"bot-1","botName":"default","chatKey":"single:stale","workspaceId":"w1","workspaceScope":"user","projectDir":"/tmp/p1","chatfileDir":"/tmp/c1","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (old_ms, old_ms, old_ms),
        encoding="utf-8",
    )
    (sessions_root / "session-fresh.json").write_text(
        '{"sessionId":"session-fresh","botId":"bot-1","botName":"default","chatKey":"single:fresh","workspaceId":"w2","workspaceScope":"user","projectDir":"/tmp/p2","chatfileDir":"/tmp/c2","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (now_ms, now_ms, now_ms),
        encoding="utf-8",
    )

    removed = cleanup_stale_session_codex_homes(runtime_root, current_ms=now_ms, ttl_ms=30 * 60 * 1000)

    assert removed == 0
    assert stale.exists() is True
    assert fresh.exists() is True


def test_cleanup_stale_session_codex_homes_keeps_active_session_dirs(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    active = homes_root / "session-active"
    active.mkdir(parents=True, exist_ok=True)
    sessions_root = runtime_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    now_ms = 1_000_000
    old_ms = now_ms - (30 * 60 * 1000) - 1
    (sessions_root / "session-active.json").write_text(
        '{"sessionId":"session-active","botId":"bot-1","botName":"default","chatKey":"single:active","workspaceId":"w1","workspaceScope":"user","projectDir":"/tmp/p1","chatfileDir":"/tmp/c1","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (old_ms, old_ms, old_ms),
        encoding="utf-8",
    )

    removed = cleanup_stale_session_codex_homes(
        runtime_root,
        current_ms=now_ms,
        ttl_ms=30 * 60 * 1000,
        active_session_ids={"session-active"},
    )

    assert removed == 0
    assert active.exists() is True
