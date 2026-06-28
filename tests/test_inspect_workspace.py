import json
import os
import subprocess
import sys
from pathlib import Path

from workspace_bridge.layout import build_workspace_ref


def write_skill(root: Path, name: str, body: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body, encoding="utf-8")


def test_inspect_workspace_outputs_workspace_and_effective_skills(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    home_dir = tmp_path / "home"
    source_dir.mkdir()
    workspace = build_workspace_ref(runtime_root, "default", source_dir, "single:alice")
    skills_root = workspace.skill_dir
    skills_root.mkdir(parents=True, exist_ok=True)
    global_skills_root = home_dir / ".codex" / "skills"
    global_skills_root.mkdir(parents=True, exist_ok=True)
    write_skill(global_skills_root, "global-only", "# global only")
    write_skill(skills_root, "deploy", "# workspace deploy")

    script = Path(__file__).resolve().parent.parent / "inspect_workspace.py"
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--runtime-root",
            str(runtime_root),
            "--source-dir",
            str(source_dir),
            "--chat-key",
            "single:alice",
            "--ensure-dirs",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=script.parent,
        env={**os.environ, "HOME": str(home_dir)},
    )

    payload = json.loads(result.stdout)
    assert payload["scope"] == "user"
    assert payload["ownerUserId"] == "alice"
    assert payload["cwdDir"].endswith("/workfile")
    assert payload["projectDir"].endswith("/workfile")
    assert payload["skillDir"].endswith("/workfile/.codex/skills")
    assert payload["effectiveSkills"]["deploy"]["layer"] == "workspace"
    assert payload["effectiveSkills"]["global-only"]["layer"] == "global"


def test_inspect_workspace_supports_personal_workspace_mode(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    home_dir = tmp_path / "home"
    source_dir.mkdir()
    global_skills_root = home_dir / ".codex" / "skills"
    global_skills_root.mkdir(parents=True, exist_ok=True)

    script = Path(__file__).resolve().parent.parent / "inspect_workspace.py"
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--runtime-root",
            str(runtime_root),
            "--source-dir",
            str(source_dir),
            "--chat-key",
            "single:alice",
            "--workspace-mode",
            "personal",
            "--ensure-dirs",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=script.parent,
        env={**os.environ, "HOME": str(home_dir)},
    )

    payload = json.loads(result.stdout)
    assert payload["cwdDir"] == str(source_dir.resolve())
    assert payload["projectDir"].endswith("/workfile")
    assert payload["skillDir"] == str((source_dir / ".codex" / "skills").resolve())
