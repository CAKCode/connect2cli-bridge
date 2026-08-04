import json
import os
import subprocess
import sys
from pathlib import Path

import workspace_bridge.agent_backends as agent_backends_module
import workspace_bridge.codex_runtime as codex_runtime
import workspace_bridge.context as context_module
import workspace_bridge.models as models_module
import workspace_bridge.prompting as prompting_module
from workspace_bridge.agent_backends import parse_command_to_argv, resolve_agent_command
from workspace_bridge.prompting import build_bridge_context, build_prompt
from workspace_bridge.runner import build_runner_invocation, run_invocation
from workspace_bridge.runtime import build_bot_config, prepare_session_run


def write_skill(root: Path, name: str, body: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body, encoding="utf-8")


def prepare_launch(tmp_path: Path):
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir(exist_ok=True)
    models_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    context_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    prompting_module.DEFAULT_GLOBAL_SKILL_DIR = global_skill_dir.resolve()
    write_skill(global_skill_dir, "global-only", "# global")
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="codex",
        source_dir=source_dir,
        runtime_root=runtime_root,
        chatfile_root=chatfile_root,
    )
    launch = prepare_session_run(bot, "group-user:room-1:alice")
    write_skill(launch.workspace.workspace.skill_dir, "deploy", "# deploy")
    return bot, prepare_session_run(bot, "group-user:room-1:alice")


def make_fake_codex_install(tmp_path: Path) -> tuple[Path, Path]:
    codex_exec = tmp_path / "fake-codex" / "bin" / "codex.js"
    bwrap = (
        tmp_path
        / "fake-codex"
        / "node_modules"
        / "@openai"
        / "codex-linux-x64"
        / "vendor"
        / "x86_64-unknown-linux-musl"
        / "codex-resources"
        / "bwrap"
    )
    codex_exec.parent.mkdir(parents=True, exist_ok=True)
    bwrap.parent.mkdir(parents=True, exist_ok=True)
    codex_exec.write_text("#!/usr/bin/env node\n", encoding="utf-8")
    bwrap.write_text("#!/bin/sh\n", encoding="utf-8")
    return codex_exec, bwrap


def test_build_bridge_context_mentions_project_dir_and_skills(tmp_path: Path) -> None:
    bot, launch = prepare_launch(tmp_path)

    context = build_bridge_context(bot, launch)

    assert "executionMode: host" in context
    assert "workspaceMode: team" in context
    assert "PROJECT_DIR:" in context
    assert "WORKDIR_DIR:" in context
    assert "CWD_DIR:" in context
    assert "SOURCE_DIR:" in context
    assert "WORKSPACE_SKILL_DIR:" in context
    assert "HOME_CODEX_SKILLS_DIR:" in context
    assert "effectiveSkills: deploy" in context
    assert "localSendFileCommand:" in context
    assert "localSendMessageCommand:" in context
    assert "localScheduleMessageCommand:" in context
    assert "allowedFileSendRoots:" in context
    assert "Run in CWD_DIR." in context
    assert "Use localSendFileCommand for file replies." in context


def test_build_bridge_context_mentions_sandbox_constraints(tmp_path: Path) -> None:
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
        codex_exec_mode="sandboxed",
    )
    launch = prepare_session_run(bot, "single:alice")

    context = build_bridge_context(bot, launch)

    assert "executionMode: sandboxed" in context
    assert "Local network access is blocked inside the Codex sandbox for this bridge." in context


def test_build_prompt_wraps_context_and_user_request(tmp_path: Path) -> None:
    bot, launch = prepare_launch(tmp_path)

    prompt = build_prompt(bot, launch, "请检查这个项目")

    assert "[BridgeContext]" in prompt
    assert "User request:" in prompt
    assert "请检查这个项目" in prompt


def test_build_runner_invocation_uses_launch_cwd_and_env(tmp_path: Path) -> None:
    bot, launch = prepare_launch(tmp_path)
    prompt = build_prompt(bot, launch, "hello")

    invocation = build_runner_invocation(
        launch,
        prompt=prompt,
        output_file=tmp_path / "out.jsonl",
        argv_override=(sys.executable, "-c", "print('ok')"),
    )

    assert invocation.cwd == launch.cwd
    assert invocation.env["WECOM_BRIDGE_PROJECT_DIR"] == str(launch.cwd)
    assert invocation.env["WECOM_BRIDGE_WORKSPACE_MODE"] == "team"
    assert invocation.prompt == prompt


def test_build_runner_invocation_uses_source_dir_for_personal_workspace_mode(tmp_path: Path) -> None:
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

    invocation = build_runner_invocation(
        launch,
        prompt="hello",
        output_file=tmp_path / "out.jsonl",
        argv_override=(sys.executable, "-c", "print('ok')"),
    )

    assert launch.cwd == source_dir.resolve()
    assert invocation.cwd == source_dir.resolve()
    assert invocation.env["WECOM_BRIDGE_PROJECT_DIR"] == str(source_dir.resolve())
    assert invocation.env["WECOM_BRIDGE_WORKDIR_DIR"] == str(source_dir.resolve())
    assert invocation.env["WECOM_BRIDGE_WORKSPACE_MODE"] == "personal"


def test_build_runner_invocation_default_codex_argv_reads_prompt_from_stdin(tmp_path: Path) -> None:
    bot, launch = prepare_launch(tmp_path)
    prompt = build_prompt(bot, launch, "hello")

    invocation = build_runner_invocation(
        launch,
        prompt=prompt,
        output_file=tmp_path / "out.jsonl",
    )

    assert invocation.argv[0:2] == ("codex", "exec")
    assert invocation.argv[-1] == "-"
    assert "--dangerously-bypass-approvals-and-sandbox" in invocation.argv


def test_build_runner_invocation_supports_claude_backend(tmp_path: Path) -> None:
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

    invocation = build_runner_invocation(
        launch,
        prompt="hello",
        output_file=tmp_path / "out.jsonl",
    )

    assert invocation.argv[0:2] == ("claude", "-p")
    assert "--verbose" in invocation.argv
    assert invocation.argv[invocation.argv.index("--permission-mode") + 1] == "bypassPermissions"
    assert "--output-format" in invocation.argv
    assert "stream-json" in invocation.argv
    assert "--image" not in invocation.argv


def test_build_runner_invocation_supports_claude_resume_session_id(tmp_path: Path) -> None:
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

    invocation = build_runner_invocation(
        launch,
        prompt="hello",
        output_file=tmp_path / "out.jsonl",
        resume=True,
        resume_thread_id="550e8400-e29b-41d4-a716-446655440000",
    )

    assert invocation.argv[0:2] == ("claude", "-p")
    assert "--resume" in invocation.argv
    assert "550e8400-e29b-41d4-a716-446655440000" in invocation.argv


def test_build_runner_invocation_ignores_legacy_claude_run_as_identity(tmp_path: Path) -> None:
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
        agent_run_as_user="nobody",
        agent_run_as_group="nogroup",
        agent_runtime_root=tmp_path / "claude-runtime",
    )
    launch = prepare_session_run(bot, "single:alice")

    invocation = build_runner_invocation(
        launch,
        prompt="hello",
        output_file=tmp_path / "out.jsonl",
    )

    assert invocation.run_as_user is None
    assert invocation.run_as_group is None
    assert invocation.argv[0] == "claude"


def test_build_runner_invocation_does_not_duplicate_claude_permission_mode(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    bot = build_bot_config(
        bot_id="bot-1",
        bot_name="claude",
        source_dir=source_dir,
        runtime_root=tmp_path / "runtime",
        chatfile_root=tmp_path / "chatfiles",
        agent_backend="claude",
        agent_command="claude --permission-mode bypassPermissions",
    )
    launch = prepare_session_run(bot, "single:alice")

    invocation = build_runner_invocation(
        launch,
        prompt="hello",
        output_file=tmp_path / "out.jsonl",
    )

    assert invocation.argv.count("--permission-mode") == 1
    assert invocation.argv[invocation.argv.index("--permission-mode") + 1] == "bypassPermissions"


def test_build_runner_invocation_does_not_apply_run_as_to_codex(tmp_path: Path) -> None:
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
        agent_backend="codex",
        agent_run_as_user="nobody",
        agent_run_as_group="nogroup",
    )
    launch = prepare_session_run(bot, "single:alice")

    invocation = build_runner_invocation(
        launch,
        prompt="hello",
        output_file=tmp_path / "out.jsonl",
    )
    assert invocation.run_as_user is None
    assert invocation.run_as_group is None
    assert invocation.argv[0] != "setpriv"


def test_build_runner_invocation_respects_code_command_override(tmp_path: Path, monkeypatch) -> None:
    bot, launch = prepare_launch(tmp_path)
    prompt = build_prompt(bot, launch, "hello")

    monkeypatch.setenv("CODEX_COMMAND", "/opt/codex/bin/codex --profile automation")

    invocation = build_runner_invocation(
        launch,
        prompt=prompt,
        output_file=tmp_path / "out.jsonl",
    )

    assert invocation.argv[0:4] == ("/opt/codex/bin/codex", "--profile", "automation", "exec")


def test_parse_command_to_argv_tolerates_unbalanced_quotes() -> None:
    assert parse_command_to_argv('codex --profile "unterminated') == ('codex --profile "unterminated',)


def test_resolve_agent_command_tolerates_unbalanced_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CODEX_COMMAND", 'codex --profile "unterminated')

    argv = resolve_agent_command("codex")

    assert argv == ('codex --profile "unterminated',)


def test_build_runner_invocation_uses_host_exec_mode_when_configured(tmp_path: Path) -> None:
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
        codex_exec_mode="host",
    )
    launch = prepare_session_run(bot, "single:alice")

    invocation = build_runner_invocation(
        launch,
        prompt="hello",
        output_file=tmp_path / "out.jsonl",
    )

    assert "--dangerously-bypass-approvals-and-sandbox" in invocation.argv
    assert "--full-auto" not in invocation.argv
    assert launch.runtime_context.codex_exec_mode == "host"


def test_build_runner_invocation_prepends_bundled_bwrap_to_path(tmp_path: Path, monkeypatch) -> None:
    bot, launch = prepare_launch(tmp_path)
    prompt = build_prompt(bot, launch, "hello")
    codex_exec, bwrap = make_fake_codex_install(tmp_path)

    codex_runtime.resolve_executable.cache_clear()
    codex_runtime.find_bundled_bwrap.cache_clear()

    def fake_which(name: str, path: str | None = None) -> str | None:
        if name == "codex":
            return str(codex_exec)
        if name == "bwrap":
            return None
        return None

    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setattr(codex_runtime.shutil, "which", fake_which)

    invocation = build_runner_invocation(
        launch,
        prompt=prompt,
        output_file=tmp_path / "out.jsonl",
    )

    assert invocation.env["PATH"].split(os.pathsep)[0] == str(bwrap.parent)


def test_codex_runtime_prepare_env_does_not_treat_non_codex_command_as_codex(tmp_path: Path, monkeypatch) -> None:
    codex_exec, bwrap = make_fake_codex_install(tmp_path)
    codex_runtime.resolve_executable.cache_clear()
    codex_runtime.find_bundled_bwrap.cache_clear()

    def fake_which(name: str, path: str | None = None) -> str | None:
        if name == "bwrap":
            return None
        if name == "notcodex":
            return str(codex_exec)
        return None

    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setattr(codex_runtime.shutil, "which", fake_which)

    env = codex_runtime.prepare_codex_subprocess_env({"PATH": "/usr/bin"}, command="notcodex")

    assert env["PATH"] == "/usr/bin"


def test_agent_backends_prepare_env_does_not_treat_non_codex_command_as_codex(tmp_path: Path, monkeypatch) -> None:
    codex_exec, bwrap = make_fake_codex_install(tmp_path)

    def fake_which(name: str, path: str | None = None) -> str | None:
        if name == "bwrap":
            return None
        if name == "notcodex":
            return str(codex_exec)
        return None

    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setattr(agent_backends_module.shutil, "which", fake_which)

    env = agent_backends_module.prepare_subprocess_env({"PATH": "/usr/bin"}, backend="codex", command="notcodex")

    assert env["PATH"] == "/usr/bin"


def test_codex_runtime_resolve_executable_rejects_directory_command(tmp_path: Path) -> None:
    command_dir = tmp_path / "fake-codex-dir"
    command_dir.mkdir()

    codex_runtime.resolve_executable.cache_clear()
    try:
        assert codex_runtime.resolve_executable(str(command_dir)) is None
    finally:
        codex_runtime.resolve_executable.cache_clear()


def test_agent_backends_resolve_executable_rejects_directory_command(tmp_path: Path) -> None:
    command_dir = tmp_path / "fake-codex-dir"
    command_dir.mkdir()

    assert agent_backends_module.resolve_executable(str(command_dir)) is None


def test_run_invocation_executes_override_process(tmp_path: Path) -> None:
    bot, launch = prepare_launch(tmp_path)
    invocation = build_runner_invocation(
        launch,
        prompt="hello runner",
        output_file=tmp_path / "out.jsonl",
        argv_override=(sys.executable, "-c", "import sys; print(sys.stdin.read())"),
    )

    result = run_invocation(invocation)

    assert result.returncode == 0
    assert "hello runner" in result.stdout


def test_run_codex_session_cli_dry_run_outputs_launch_payload(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    output_file = tmp_path / "out.jsonl"
    source_dir.mkdir()
    script = Path(__file__).resolve().parent.parent / "run_codex_session.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--bot-id",
            "bot-1",
            "--bot-name",
            "codex",
            "--runtime-root",
            str(runtime_root),
            "--source-dir",
            str(source_dir),
            "--chatfile-root",
            str(chatfile_root),
            "--chat-key",
            "single:alice",
            "--message",
            "hello",
            "--output-file",
            str(output_file),
            "--dry-run",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    payload = json.loads(result.stdout)
    assert payload["cwd"].endswith("/workfile")
    assert payload["workspaceId"].startswith("user:")
    assert "prompt" in payload


def test_run_codex_session_cli_reports_invalid_chat_key_cleanly(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    output_file = tmp_path / "out.jsonl"
    source_dir.mkdir()
    script = Path(__file__).resolve().parent.parent / "run_codex_session.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--bot-id",
            "bot-1",
            "--bot-name",
            "codex",
            "--runtime-root",
            str(runtime_root),
            "--source-dir",
            str(source_dir),
            "--chatfile-root",
            str(chatfile_root),
            "--chat-key",
            "invalid",
            "--message",
            "hello",
            "--output-file",
            str(output_file),
        ],
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    assert result.returncode == 4
    assert "invalid chat key" in result.stderr
    assert "Traceback" not in result.stderr


def test_run_codex_session_cli_dry_run_does_not_require_output_file(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    script = Path(__file__).resolve().parent.parent / "run_codex_session.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--bot-id",
            "bot-1",
            "--bot-name",
            "codex",
            "--runtime-root",
            str(runtime_root),
            "--source-dir",
            str(source_dir),
            "--chatfile-root",
            str(chatfile_root),
            "--chat-key",
            "single:alice",
            "--message",
            "hello",
            "--dry-run",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    payload = json.loads(result.stdout)
    assert payload["workspaceId"].startswith("user:")
    assert "prompt" in payload


def test_run_codex_session_cli_requires_output_file_without_dry_run(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    script = Path(__file__).resolve().parent.parent / "run_codex_session.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--bot-id",
            "bot-1",
            "--bot-name",
            "codex",
            "--runtime-root",
            str(runtime_root),
            "--source-dir",
            str(source_dir),
            "--chatfile-root",
            str(chatfile_root),
            "--chat-key",
            "single:alice",
            "--message",
            "hello",
        ],
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    assert result.returncode == 4
    assert "--output-file is required" in result.stderr


def test_run_codex_session_cli_rejects_missing_source_dir_cleanly(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    chatfile_root = tmp_path / "chatfiles"
    missing_source_dir = tmp_path / "missing-repo"
    script = Path(__file__).resolve().parent.parent / "run_codex_session.py"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--bot-id",
            "bot-1",
            "--bot-name",
            "codex",
            "--runtime-root",
            str(runtime_root),
            "--source-dir",
            str(missing_source_dir),
            "--chatfile-root",
            str(chatfile_root),
            "--chat-key",
            "single:alice",
            "--message",
            "hello",
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    assert result.returncode == 4
    assert f"source-dir does not exist: {missing_source_dir.resolve()}" in result.stderr
    assert "Traceback" not in result.stderr
    assert "Traceback" not in result.stderr
