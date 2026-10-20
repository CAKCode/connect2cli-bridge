import os
import json
from pathlib import Path

import workspace_bridge.context as context_module
import workspace_bridge.models as models_module
from workspace_bridge.runtime import (
    append_session_record_chat_message,
    build_bot_config,
    cleanup_outdated_session_artifacts,
    cleanup_orphan_session_codex_homes,
    cleanup_stale_session_codex_homes,
    create_new_session_run,
    current_session_id,
    list_session_records,
    load_session_record,
    load_reply_url_state,
    load_template_card_state,
    prepare_session_run,
    remove_session_codex_home,
    reply_url_state_file,
    store_session_record,
    store_reply_url_state,
    store_template_card_state,
    session_codex_home_root,
    stable_session_id,
    template_card_state_file,
)


def write_skill(root: Path, name: str, body: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body, encoding="utf-8")


def test_stable_session_id_is_deterministic() -> None:
    first = stable_session_id("bot-1", "single:alice", "/tmp/repo", "team-a", "team")
    second = stable_session_id("bot-1", "single:alice", "/tmp/repo", "team-a", "team")

    assert first == second


def test_stable_session_id_changes_when_source_dir_changes() -> None:
    first = stable_session_id("bot-1", "single:alice", "/tmp/repo-a", "team-a", "team")
    second = stable_session_id("bot-1", "single:alice", "/tmp/repo-b", "team-a", "team")

    assert first != second


def test_stable_session_id_changes_when_workspace_identity_changes() -> None:
    first = stable_session_id("bot-1", "single:alice", "/tmp/repo", "team-a", "team")
    second = stable_session_id("bot-1", "single:alice", "/tmp/repo", "team-b", "team")
    third = stable_session_id("bot-1", "single:alice", "/tmp/repo", "team-a", "personal")

    assert first != second
    assert first != third


def test_create_new_session_run_persists_new_binding_and_keeps_previous_record(tmp_path: Path) -> None:
    from dataclasses import replace

    source_dir = tmp_path / "repo"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        workspace_namespace="team-a",
        chatfile_root=tmp_path / "chatfiles",
        workspace_mode="team",
    )
    previous = prepare_session_run(bot, "single:alice")
    store_session_record(
        bot.runtime_root,
        replace(
            previous.session,
            thread_id="thread-old",
            recent_chat=({"role": "user", "text": "keep-chat"},),
        ),
    )

    current = create_new_session_run(bot, "single:alice")

    assert current.session.session_id != previous.session.session_id
    assert current_session_id(bot, "single:alice") == current.session.session_id
    assert prepare_session_run(bot, "single:alice").session.session_id == current.session.session_id
    assert current.session.thread_id is None
    assert current.session.recent_chat == ()
    previous_record = load_session_record(bot.runtime_root, previous.session.session_id)
    assert previous_record is not None
    assert previous_record.thread_id == "thread-old"
    assert previous_record.recent_chat == ({"role": "user", "text": "keep-chat"},)
    assert cleanup_outdated_session_artifacts(bot) == 0


def test_build_bot_config_rejects_missing_source_dir(tmp_path: Path) -> None:
    missing_source_dir = tmp_path / "missing-repo"

    try:
        build_bot_config(
            bot_id="bot-1",
            bot_name="codex",
            source_dir=missing_source_dir,
            runtime_root=tmp_path / "runtime",
            chatfile_root=tmp_path / "chatfiles",
        )
    except ValueError as exc:
        assert str(exc) == f"source_dir does not exist: {missing_source_dir.resolve()}"
    else:
        raise AssertionError("expected ValueError")


def test_build_bot_config_rejects_non_directory_source_dir(tmp_path: Path) -> None:
    source_file = tmp_path / "repo.txt"
    source_file.write_text("not a dir", encoding="utf-8")

    try:
        build_bot_config(
            bot_id="bot-1",
            bot_name="codex",
            source_dir=source_file,
            runtime_root=tmp_path / "runtime",
            chatfile_root=tmp_path / "chatfiles",
        )
    except ValueError as exc:
        assert str(exc) == f"source_dir is not a directory: {source_file.resolve()}"
    else:
        raise AssertionError("expected ValueError")


def test_build_bot_config_rejects_file_runtime_root(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime.txt"
    source_dir.mkdir()
    runtime_root.write_text("not a dir", encoding="utf-8")

    try:
        build_bot_config(
            bot_id="bot-1",
            bot_name="codex",
            source_dir=source_dir,
            runtime_root=runtime_root,
            chatfile_root=tmp_path / "chatfiles",
        )
    except ValueError as exc:
        assert str(exc) == f"runtime_root is not a directory: {runtime_root.resolve()}"
    else:
        raise AssertionError("expected ValueError")


def test_build_bot_config_rejects_file_chatfile_root(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    chatfile_root = tmp_path / "chatfiles.txt"
    source_dir.mkdir()
    chatfile_root.write_text("not a dir", encoding="utf-8")

    try:
        build_bot_config(
            bot_id="bot-1",
            bot_name="codex",
            source_dir=source_dir,
            runtime_root=tmp_path / "runtime",
            chatfile_root=chatfile_root,
        )
    except ValueError as exc:
        assert str(exc) == f"chatfile_root is not a directory: {chatfile_root.resolve()}"
    else:
        raise AssertionError("expected ValueError")


def test_build_bot_config_ignores_file_legacy_agent_runtime_root(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    agent_runtime_root = tmp_path / "claude-runtime.txt"
    source_dir.mkdir()
    agent_runtime_root.write_text("not a dir", encoding="utf-8")

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        agent_runtime_root=agent_runtime_root,
    )

    assert bot.agent_runtime_root is None


def test_build_bot_config_treats_null_agent_runtime_root_as_unset(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        agent_runtime_root="null",
    )

    assert bot.agent_runtime_root is None


def test_build_bot_config_rejects_null_bot_id(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    try:
        build_bot_config(
            bot_id="null",
            bot_name="codex",
            source_dir=source_dir,
            runtime_root=tmp_path / "runtime",
            chatfile_root=tmp_path / "chatfiles",
        )
    except ValueError as exc:
        assert str(exc) == "bot_id required"
    else:
        raise AssertionError("expected ValueError")


def test_build_bot_config_treats_null_agent_run_as_values_as_unset(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        agent_run_as_user="null",
        agent_run_as_group="none",
    )

    assert bot.agent_run_as_user is None
    assert bot.agent_run_as_group is None


def test_build_bot_config_treats_null_agent_command_as_unset(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        agent_command="null",
    )

    assert bot.agent_command is None


def test_build_bot_config_treats_null_agent_backend_as_default_codex(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        agent_backend="null",
    )

    assert bot.agent_backend == "codex"
    assert bot.workspace_mode == "team"


def test_build_bot_config_treats_null_workspace_namespace_as_bot_id(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        workspace_namespace="null",
        chatfile_root=tmp_path / "chatfiles",
    )

    assert bot.workspace_namespace == "bot-1"


def test_build_bot_config_treats_null_bot_name_as_default(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="null",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
    )

    assert bot.bot_name == "default"


def test_build_bot_config_treats_null_codex_exec_mode_as_host(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        codex_exec_mode="null",
    )

    assert bot.codex_exec_mode == "host"


def test_build_bot_config_rejects_invalid_codex_exec_mode(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    try:
        build_bot_config(
            bot_id="bot-1",
            bot_name="codex",
            source_dir=source_dir,
            runtime_root=tmp_path / "runtime",
            chatfile_root=tmp_path / "chatfiles",
            codex_exec_mode="weird",
        )
    except ValueError as exc:
        assert str(exc) == "invalid codex_exec_mode: weird"
    else:
        raise AssertionError("expected ValueError")


def test_build_bot_config_rejects_file_send_root_that_is_not_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    file_send_root = tmp_path / "allowed.txt"
    source_dir.mkdir()
    file_send_root.write_text("not a dir", encoding="utf-8")

    try:
        build_bot_config(
            bot_id="bot-1",
            bot_name="codex",
            source_dir=source_dir,
            runtime_root=tmp_path / "runtime",
            chatfile_root=tmp_path / "chatfiles",
            file_send_roots=(file_send_root,),
        )
    except ValueError as exc:
        assert str(exc) == f"file_send_root is not a directory: {file_send_root.resolve()}"
    else:
        raise AssertionError("expected ValueError")


def test_build_bot_config_treats_null_workspace_mode_as_backend_default(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    codex_bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        workspace_mode="null",
        agent_backend="codex",
    )
    claude_bot = build_bot_config(
        bot_id="bot-2",
        bot_name="claude",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime-2",
        chatfile_root=tmp_path / "chatfiles-2",
        workspace_mode="null",
        agent_backend="claude",
    )

    assert codex_bot.workspace_mode == "team"
    assert claude_bot.workspace_mode == "personal"


def test_prepare_session_run_builds_launch_spec_and_persists_session(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    launch = prepare_session_run(bot, "single:alice")
    write_skill(launch.workspace.workspace.skill_dir, "deploy", "# deploy")
    launch = prepare_session_run(bot, "single:alice")

    assert launch.cwd == launch.runtime_context.cwd_dir
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
    assert bot.workspace_mode == "team"

    stored = load_session_record(runtime_root, launch.session.session_id)
    assert stored is not None
    assert stored.workspace_id == launch.session.workspace_id
    assert stored.cwd_dir == launch.cwd
    assert stored.workfile_dir == launch.runtime_context.workfile_dir
    assert (runtime_root / "sessions").stat().st_mode & 0o777 == 0o700
    assert (runtime_root / "sessions" / f"{launch.session.session_id}.json").stat().st_mode & 0o777 == 0o600
    assert Path(launch.env["CODEX_HOME"]).stat().st_mode & 0o777 == 0o700
    assert launch.runtime_context.chatfile_dir.stat().st_mode & 0o777 == 0o700


def test_prepare_session_run_session_codex_home_copies_only_lightweight_root_state(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
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
        chatfile_root=chatfile_root,
    )

    launch = prepare_session_run(bot, "single:alice")
    session_home = Path(launch.env["CODEX_HOME"])

    assert (session_home / "installation_id").read_text(encoding="utf-8") == "install-1"
    assert (session_home / "logs_2.sqlite").exists() is False
    assert session_home.stat().st_mode & 0o777 == 0o700
    assert (session_home / "installation_id").stat().st_mode & 0o777 == 0o600


def test_prepare_session_run_reuses_stable_session_id_for_same_chat(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    first = prepare_session_run(bot, "group-user:room-1:alice")
    second = prepare_session_run(bot, "group-user:room-1:alice")

    assert first.session.session_id == second.session.session_id
    assert first.session.workspace_id == second.session.workspace_id
    assert first.cwd == second.cwd


def test_prepare_session_run_session_codex_home_prefers_workspace_skill_contents_on_name_conflict(
    tmp_path: Path, monkeypatch
) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    write_skill(global_skill_dir, "deploy", "# global deploy")
    write_skill(global_skill_dir, "global-only", "# global only")
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    write_skill(first_launch.workspace.workspace.skill_dir, "deploy", "# workspace deploy")
    second_launch = prepare_session_run(bot, "single:alice")
    session_home = Path(second_launch.env["CODEX_HOME"])

    assert (session_home / "skills" / "deploy" / "SKILL.md").read_text(encoding="utf-8") == "# workspace deploy"
    assert (session_home / "skills" / "global-only" / "SKILL.md").read_text(encoding="utf-8") == "# global only"


def test_prepare_session_run_preserves_symlinked_global_skill(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    external_target = tmp_path / "external-global-skill"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    external_target.mkdir()
    (external_target / "SKILL.md").write_text("# linked global", encoding="utf-8")
    (global_skill_dir / "linked-skill").symlink_to(external_target, target_is_directory=True)
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    launch = prepare_session_run(bot, "single:alice")
    linked_skill = Path(launch.env["CODEX_HOME"]) / "skills" / "linked-skill"

    assert linked_skill.is_symlink()
    assert linked_skill.resolve() == external_target.resolve()
    assert os.path.lexists(linked_skill)
    assert (linked_skill / "SKILL.md").read_text(encoding="utf-8") == "# linked global"


def test_prepare_session_run_tolerates_non_directory_global_skill_root(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_file = tmp_path / "global-skills.txt"
    source_dir.mkdir()
    global_skill_file.write_text("not a dir", encoding="utf-8")
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_file.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_file.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    launch = prepare_session_run(bot, "single:alice")

    assert launch.runtime_context.effective_skill_names == ()
    assert (Path(launch.env["CODEX_HOME"]) / "skills").is_dir()


def test_prepare_session_run_workspace_skill_replaces_broken_global_symlink(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    missing_target = tmp_path / "missing-global-skill"
    (global_skill_dir / "deploy").symlink_to(missing_target, target_is_directory=True)
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    write_skill(first_launch.workspace.workspace.skill_dir, "deploy", "# workspace deploy")
    second_launch = prepare_session_run(bot, "single:alice")
    deploy_skill = Path(second_launch.env["CODEX_HOME"]) / "skills" / "deploy"

    assert deploy_skill.is_dir()
    assert not deploy_skill.is_symlink()
    assert (deploy_skill / "SKILL.md").read_text(encoding="utf-8") == "# workspace deploy"


def test_prepare_session_run_uses_personal_session_cwd_override_within_workdir(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    nested_dir = source_dir / "services" / "api"
    nested_dir.mkdir(parents=True)
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="claude",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
        workspace_mode="personal",
        agent_backend="claude",
    )

    launch = prepare_session_run(bot, "single:alice")
    store_session_record(runtime_root, launch.session.__class__(**{**launch.session.__dict__, "cwd_dir": nested_dir.resolve(), "skill_dir": (nested_dir / ".codex" / "skills").resolve()}))

    updated = prepare_session_run(bot, "single:alice")

    assert updated.cwd == nested_dir.resolve()
    assert updated.runtime_context.cwd_dir == nested_dir.resolve()
    assert updated.env["WECOM_BRIDGE_PROJECT_DIR"] == str(nested_dir.resolve())
    assert updated.env["WECOM_BRIDGE_WORKSPACE_SKILL_DIR"] == str((nested_dir / ".codex" / "skills").resolve())


def test_append_session_record_chat_message_persists_recent_chat(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    launch = prepare_session_run(bot, "single:alice")
    append_session_record_chat_message(runtime_root, launch.session.session_id, {"role": "user", "text": "hello", "time": 1})
    append_session_record_chat_message(runtime_root, launch.session.session_id, {"role": "bot", "text": "world", "time": 2})

    stored = load_session_record(runtime_root, launch.session.session_id)

    assert stored is not None
    assert tuple(item["text"] for item in stored.recent_chat) == ("hello", "world")


def test_state_files_escape_bot_id_path_components(tmp_path: Path) -> None:
    bot_id = "folder/evil"

    store_template_card_state(tmp_path, bot_id, {"task-1": {"taskId": "task-1"}})
    store_reply_url_state(
        tmp_path,
        bot_id,
        {
            "req-1": {
                "responseUrl": "https://example.com/response",
                "chatKey": "single:alice",
                "capturedAtMs": 9999999999999,
                "consumed": False,
            }
        },
    )

    assert template_card_state_file(tmp_path, bot_id).exists() is True
    assert reply_url_state_file(tmp_path, bot_id).exists() is True
    assert not (tmp_path / "template-card-state" / "folder").exists()
    assert load_template_card_state(tmp_path, bot_id)["task-1"]["taskId"] == "task-1"
    assert load_reply_url_state(tmp_path, bot_id)["req-1"]["responseUrl"] == "https://example.com/response"
    assert (tmp_path / "template-card-state").stat().st_mode & 0o777 == 0o700
    assert template_card_state_file(tmp_path, bot_id).stat().st_mode & 0o777 == 0o600
    assert reply_url_state_file(tmp_path, bot_id).stat().st_mode & 0o777 == 0o600


def test_state_loaders_ignore_symlink_files(tmp_path: Path) -> None:
    external = tmp_path / "external.json"
    external.write_text(
        json.dumps(
            {
                "task-1": {"taskId": "task-1"},
                "req-1": {"responseUrl": "https://example.test", "capturedAtMs": 9999999999999},
            }
        ),
        encoding="utf-8",
    )
    template_path = template_card_state_file(tmp_path, "bot-1")
    reply_path = reply_url_state_file(tmp_path, "bot-1")
    template_path.symlink_to(external)
    reply_path.symlink_to(external)

    assert load_template_card_state(tmp_path, "bot-1") == {}
    assert load_reply_url_state(tmp_path, "bot-1") == {}


def test_store_reply_url_state_ignores_invalid_captured_at_values(tmp_path: Path) -> None:
    store_reply_url_state(
        tmp_path,
        "bot-1",
        {
            "req-bad": {
                "responseUrl": "https://example.com/bad",
                "chatKey": "single:alice",
                "capturedAtMs": "bad-value",
                "consumed": False,
            },
            "req-good": {
                "responseUrl": "https://example.com/good",
                "chatKey": "single:bob",
                "capturedAtMs": 9999999999999,
                "consumed": False,
            },
        },
    )

    loaded = load_reply_url_state(tmp_path, "bot-1")

    assert "req-bad" not in loaded
    assert loaded["req-good"]["responseUrl"] == "https://example.com/good"


def test_store_reply_url_state_ignores_missing_response_url(tmp_path: Path) -> None:
    store_reply_url_state(
        tmp_path,
        "bot-1",
        {
            "req-bad": {
                "chatKey": "single:alice",
                "capturedAtMs": 9999999999999,
                "consumed": False,
            },
            "req-good": {
                "responseUrl": "https://example.com/good",
                "chatKey": "single:bob",
                "capturedAtMs": 9999999999999,
                "consumed": False,
            },
        },
    )

    loaded = load_reply_url_state(tmp_path, "bot-1")

    assert "req-bad" not in loaded
    assert loaded["req-good"]["responseUrl"] == "https://example.com/good"


def test_load_session_record_returns_none_for_malformed_payload(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    sessions_root = runtime_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    (sessions_root / "broken.json").write_text('{"sessionId":"broken"}', encoding="utf-8")

    assert load_session_record(runtime_root, "broken") is None


def test_load_session_record_treats_null_thread_id_as_none(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    sessions_root = runtime_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    (sessions_root / "session-1.json").write_text(
        json.dumps(
            {
                "sessionId": "session-1",
                "botId": "bot-1",
                "botName": "default",
                "chatKey": "single:alice",
                "workspaceId": "w1",
                "workspaceScope": "user",
                "workspaceMode": "team",
                "cwdDir": str((tmp_path / "cwd").resolve()),
                "chatfileDir": str((tmp_path / "chatfiles").resolve()),
                "createdAt": 1,
                "updatedAt": 2,
                "threadId": "null",
            }
        ),
        encoding="utf-8",
    )

    record = load_session_record(runtime_root, "session-1")

    assert record is not None
    assert record.thread_id is None


def test_store_template_card_state_prunes_stale_entries(tmp_path: Path) -> None:
    current_ms = int(__import__("time").time() * 1000)
    store_template_card_state(
        tmp_path,
        "bot-1",
        {
            "task-stale": {
                "taskId": "task-stale",
                "chatKey": "single:alice",
                "templateCard": {"task_id": "task-stale"},
                "sentAtMs": current_ms - (73 * 60 * 60 * 1000),
            },
            "task-fresh": {
                "taskId": "task-fresh",
                "chatKey": "single:bob",
                "templateCard": {"task_id": "task-fresh"},
                "sentAtMs": current_ms,
            },
        },
    )

    loaded = load_template_card_state(tmp_path, "bot-1")

    assert "task-stale" not in loaded
    assert loaded["task-fresh"]["templateCard"]["task_id"] == "task-fresh"


def test_prepare_session_run_rejects_personal_session_cwd_outside_workdir(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    outside_dir = tmp_path / "outside"
    source_dir.mkdir()
    outside_dir.mkdir()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="claude",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
        workspace_mode="personal",
        agent_backend="claude",
    )

    launch = prepare_session_run(bot, "single:alice")
    store_session_record(runtime_root, launch.session.__class__(**{**launch.session.__dict__, "cwd_dir": outside_dir.resolve(), "skill_dir": (outside_dir / ".codex" / "skills").resolve()}))

    try:
        prepare_session_run(bot, "single:alice")
    except ValueError as exc:
        assert "cwd must stay within workDir" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_prepare_session_run_changes_session_id_when_workspace_identity_changes(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    team_bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace="team-a",
        chatfile_root=chatfile_root,
        workspace_mode="team",
    )
    personal_bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace="team-a",
        chatfile_root=chatfile_root,
        workspace_mode="personal",
    )
    other_namespace_bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace="team-b",
        chatfile_root=chatfile_root,
        workspace_mode="team",
    )

    team_launch = prepare_session_run(team_bot, "single:alice")
    personal_launch = prepare_session_run(personal_bot, "single:alice")
    other_namespace_launch = prepare_session_run(other_namespace_bot, "single:alice")

    assert team_launch.session.session_id != personal_launch.session.session_id
    assert team_launch.session.session_id != other_namespace_launch.session.session_id


def test_cleanup_outdated_session_artifacts_removes_records_and_runtime_for_old_workspace_identity(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    old_bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace="team-a",
        chatfile_root=chatfile_root,
        workspace_mode="team",
    )
    new_bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace="team-a",
        chatfile_root=chatfile_root,
        workspace_mode="personal",
    )

    old_launch = prepare_session_run(old_bot, "single:alice")
    old_chatfile = old_launch.runtime_context.chatfile_dir
    old_chatfile.mkdir(parents=True, exist_ok=True)
    (old_chatfile / "artifact.txt").write_text("artifact", encoding="utf-8")
    old_codex_home = session_codex_home_root(runtime_root) / old_launch.session.session_id
    old_codex_home.mkdir(parents=True, exist_ok=True)
    (old_codex_home / "state.txt").write_text("state", encoding="utf-8")

    removed = cleanup_outdated_session_artifacts(new_bot)

    assert removed == 1
    assert load_session_record(runtime_root, old_launch.session.session_id) is None
    assert not old_chatfile.exists()
    assert not old_codex_home.exists()


def test_prepare_session_run_reuses_personal_workfile_across_bots_in_same_namespace(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    first_bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex-a",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace="team-a",
        chatfile_root=chatfile_root,
    )
    second_bot = build_bot_config(
        bot_id="bot-2",
        bot_name="codex-b",
        source_dir=source_dir,
        runtime_root=runtime_root,
        workspace_namespace="team-a",
        chatfile_root=chatfile_root,
    )

    first = prepare_session_run(first_bot, "single:alice")
    second = prepare_session_run(second_bot, "single:alice")

    assert first.runtime_context.workfile_dir == second.runtime_context.workfile_dir
    assert first.runtime_context.chatfile_dir != second.runtime_context.chatfile_dir


def test_prepare_session_run_uses_workspace_skills(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    write_skill(first_launch.workspace.workspace.skill_dir, "deploy", "# workspace deploy")
    second_launch = prepare_session_run(bot, "single:alice")

    assert "deploy" in second_launch.runtime_context.effective_skill_names


def test_prepare_session_run_rebuilds_session_skill_dir_without_stale_entries(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    write_skill(first_launch.workspace.workspace.skill_dir, "deploy", "# workspace deploy")
    first_launch = prepare_session_run(bot, "single:alice")
    session_home = Path(first_launch.env["CODEX_HOME"])
    stale_skill = session_home / "skills" / "stale-skill"
    stale_skill.mkdir(parents=True, exist_ok=True)
    (stale_skill / "SKILL.md").write_text("---\nname: stale\n---\nstale\n", encoding="utf-8")

    second_launch = prepare_session_run(bot, "single:alice")

    assert (Path(second_launch.env["CODEX_HOME"]) / "skills" / "stale-skill").exists() is False


def test_prepare_session_run_workspace_symlinked_skill_replaces_global_dir(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    external_target = tmp_path / "external-workspace-skill"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    external_target.mkdir()
    write_skill(global_skill_dir, "deploy", "# global deploy")
    (external_target / "SKILL.md").write_text("# linked workspace", encoding="utf-8")
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    deploy_skill = first_launch.workspace.workspace.skill_dir / "deploy"
    if deploy_skill.exists() or deploy_skill.is_symlink():
        if deploy_skill.is_dir() and not deploy_skill.is_symlink():
            __import__("shutil").rmtree(deploy_skill, ignore_errors=True)
        else:
            deploy_skill.unlink(missing_ok=True)
    deploy_skill.symlink_to(external_target, target_is_directory=True)

    second_launch = prepare_session_run(bot, "single:alice")
    linked_skill = Path(second_launch.env["CODEX_HOME"]) / "skills" / "deploy"

    assert linked_skill.is_symlink()
    assert linked_skill.resolve() == external_target.resolve()
    assert os.path.lexists(linked_skill)
    assert (linked_skill / "SKILL.md").read_text(encoding="utf-8") == "# linked workspace"


def test_prepare_session_run_unlinks_stale_symlinked_skill_without_deleting_target(tmp_path: Path, monkeypatch) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )

    first_launch = prepare_session_run(bot, "single:alice")
    session_home = Path(first_launch.env["CODEX_HOME"])
    stale_skill = session_home / "skills" / "stale-skill"
    external_target = tmp_path / "external-stale-skill"
    external_target.mkdir(parents=True, exist_ok=True)
    (external_target / "SKILL.md").write_text("# stale", encoding="utf-8")
    stale_skill.symlink_to(external_target, target_is_directory=True)

    second_launch = prepare_session_run(bot, "single:alice")

    assert not os.path.lexists(Path(second_launch.env["CODEX_HOME"]) / "skills" / "stale-skill")
    assert external_target.exists()
    assert (external_target / "SKILL.md").exists()


def test_list_session_records_returns_latest_first_without_restoring_thread_info(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
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
    assert records[0].thread_id == "thread-b"

def test_prepare_session_run_preserves_host_exec_mode(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
        codex_exec_mode="host",
    )

    launch = prepare_session_run(bot, "single:alice")

    assert launch.runtime_context.codex_exec_mode == "host"
    assert launch.env["WECOM_BRIDGE_EXEC_MODE"] == "host"


def test_prepare_session_run_defaults_claude_to_personal_workspace_mode(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="claude",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
        agent_backend="claude",
    )

    launch = prepare_session_run(bot, "single:alice")

    assert bot.workspace_mode == "personal"
    assert launch.cwd == source_dir.resolve()
    assert launch.runtime_context.workfile_dir is not None
    assert launch.env["WECOM_BRIDGE_WORKSPACE_MODE"] == "personal"
    assert launch.env["WECOM_BRIDGE_WORKDIR_DIR"] == str(source_dir.resolve())


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


def test_remove_session_codex_home_unlinks_symlink_without_deleting_target(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    session_home = homes_root / "session-link"
    target = tmp_path / "external-session-home"
    target.mkdir(parents=True, exist_ok=True)
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    homes_root.mkdir(parents=True, exist_ok=True)
    session_home.symlink_to(target, target_is_directory=True)

    remove_session_codex_home(runtime_root, "session-link")

    assert not os.path.lexists(session_home)
    assert target.exists()
    assert (target / "keep.txt").exists()


def test_cleanup_orphan_session_codex_homes_unlinks_symlinked_orphan_home(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    orphan = homes_root / "session-orphan"
    target = tmp_path / "external-orphan-home"
    target.mkdir(parents=True, exist_ok=True)
    (target / "keep.txt").write_text("keep", encoding="utf-8")
    homes_root.mkdir(parents=True, exist_ok=True)
    orphan.symlink_to(target, target_is_directory=True)

    removed = cleanup_orphan_session_codex_homes(runtime_root)

    assert removed == 1
    assert not os.path.lexists(orphan)
    assert target.exists()
    assert (target / "keep.txt").exists()


def test_cleanup_orphan_session_codex_homes_removes_broken_symlink_orphan_home(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    orphan = homes_root / "session-orphan"
    target = tmp_path / "missing-orphan-home"
    homes_root.mkdir(parents=True, exist_ok=True)
    orphan.symlink_to(target, target_is_directory=True)

    removed = cleanup_orphan_session_codex_homes(runtime_root)

    assert removed == 1
    assert not os.path.lexists(orphan)


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
        '{"sessionId":"session-stale","botId":"bot-1","botName":"default","chatKey":"single:stale","workspaceId":"w1","workspaceScope":"user","cwdDir":"/tmp/p1","chatfileDir":"/tmp/c1","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (old_ms, old_ms, old_ms),
        encoding="utf-8",
    )
    (sessions_root / "session-fresh.json").write_text(
        '{"sessionId":"session-fresh","botId":"bot-1","botName":"default","chatKey":"single:fresh","workspaceId":"w2","workspaceScope":"user","cwdDir":"/tmp/p2","chatfileDir":"/tmp/c2","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (now_ms, now_ms, now_ms),
        encoding="utf-8",
    )

    removed = cleanup_stale_session_codex_homes(runtime_root, current_ms=now_ms, ttl_ms=30 * 60 * 1000)

    assert removed == 1
    assert stale.exists() is False
    assert fresh.exists() is True


def test_cleanup_stale_session_codex_homes_removes_broken_symlink_home(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    stale = homes_root / "session-stale"
    target = tmp_path / "missing-stale-home"
    homes_root.mkdir(parents=True, exist_ok=True)
    stale.symlink_to(target, target_is_directory=True)
    sessions_root = runtime_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    now_ms = 1_000_000
    old_ms = now_ms - (30 * 60 * 1000) - 1
    (sessions_root / "session-stale.json").write_text(
        '{"sessionId":"session-stale","botId":"bot-1","botName":"default","chatKey":"single:stale","workspaceId":"w1","workspaceScope":"user","cwdDir":"/tmp/p1","chatfileDir":"/tmp/c1","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (old_ms, old_ms, old_ms),
        encoding="utf-8",
    )

    removed = cleanup_stale_session_codex_homes(runtime_root, current_ms=now_ms, ttl_ms=30 * 60 * 1000)

    assert removed == 1
    assert not os.path.lexists(stale)


def test_cleanup_stale_session_codex_homes_removes_malformed_expired_record_home(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    homes_root = session_codex_home_root(runtime_root)
    stale = homes_root / "session-stale"
    stale.mkdir(parents=True, exist_ok=True)
    sessions_root = runtime_root / "sessions"
    sessions_root.mkdir(parents=True, exist_ok=True)
    now_ms = 1_000_000
    old_ms = now_ms - (30 * 60 * 1000) - 1
    (sessions_root / "session-stale.json").write_text(
        '{"sessionId":"session-stale","botId":"bot-1","chatKey":"single:stale","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (old_ms, old_ms, old_ms),
        encoding="utf-8",
    )

    removed = cleanup_stale_session_codex_homes(runtime_root, current_ms=now_ms, ttl_ms=30 * 60 * 1000)

    assert removed == 1
    assert stale.exists() is False


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
        '{"sessionId":"session-active","botId":"bot-1","botName":"default","chatKey":"single:active","workspaceId":"w1","workspaceScope":"user","cwdDir":"/tmp/p1","chatfileDir":"/tmp/c1","createdAt":%d,"updatedAt":%d,"lastRunAt":%d}' % (old_ms, old_ms, old_ms),
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
