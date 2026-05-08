import json
import subprocess
import sys
from pathlib import Path


def write_skill(root: Path, name: str, body: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body, encoding="utf-8")


def test_inspect_workspace_outputs_workspace_and_effective_skills(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    global_skills_root = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skills_root.mkdir()
    write_skill(global_skills_root, "deploy", "# global deploy")

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
            "--global-skills-root",
            str(global_skills_root),
            "--ensure-dirs",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=script.parent,
    )

    payload = json.loads(result.stdout)
    assert payload["scope"] == "user"
    assert payload["ownerUserId"] == "alice"
    assert payload["projectDir"].endswith("/project")
    assert payload["effectiveSkills"]["deploy"]["layer"] == "global"
