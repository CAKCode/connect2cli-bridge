import json
import subprocess
import sys
from pathlib import Path

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
    return bot, prepare_session_run(bot, "group-user:room-1:alice")


def test_build_bridge_context_mentions_project_dir_and_skills(tmp_path: Path) -> None:
    bot, launch = prepare_launch(tmp_path)

    context = build_bridge_context(bot, launch)

    assert "PROJECT_DIR:" in context
    assert "SOURCE_DIR:" in context
    assert "WORKSPACE_SKILL_DIR:" in context
    assert "WORKFILE_DIR:" in context
    assert "ROOMFILE_DIR:" in context
    assert "effectiveSkills: deploy" in context
    assert "cwd=PROJECT_DIR" in context


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
    assert invocation.prompt == prompt


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
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    output_file = tmp_path / "out.jsonl"
    source_dir.mkdir()
    global_skill_dir.mkdir()
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
            "--global-skills-root",
            str(global_skill_dir),
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
    assert payload["cwd"].endswith("/project")
    assert payload["workspaceId"].startswith("user:")
    assert "prompt" in payload
