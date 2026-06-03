from pathlib import Path

from workspace_bridge.models import DEFAULT_GLOBAL_SKILL_DIR
from workspace_bridge.skills import discover_skills, resolve_skill_space


def write_skill(root: Path, name: str, body: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body, encoding="utf-8")


def test_discover_skills_only_includes_directories_with_skill_md(tmp_path: Path) -> None:
    root = tmp_path / "skills"
    root.mkdir()
    write_skill(root, "deploy", "# deploy")
    (root / "notes").mkdir()
    (root / "README.md").write_text("ignore", encoding="utf-8")

    skills = discover_skills(root, layer_name="workspace")

    assert list(skills) == ["deploy"]
    assert skills["deploy"].layer == "workspace"


def test_workspace_skill_overrides_global_skill_of_same_name(tmp_path: Path) -> None:
    global_root = tmp_path / "global"
    workspace_root = tmp_path / "workspace"
    global_root.mkdir()
    workspace_root.mkdir()

    write_skill(global_root, "deploy", "# global deploy")
    write_skill(global_root, "lint", "# global lint")
    write_skill(workspace_root, "deploy", "# workspace deploy")
    write_skill(workspace_root, "workspace-only", "# workspace only")

    resolved = resolve_skill_space(global_root, workspace_root)

    assert len(resolved.layers) == 2
    assert resolved.effective_skills["deploy"].layer == "workspace"
    assert resolved.effective_skills["lint"].layer == "global"
    assert resolved.effective_skills["workspace-only"].layer == "workspace"


def test_resolve_skill_space_handles_missing_roots(tmp_path: Path) -> None:
    resolved = resolve_skill_space(tmp_path / "missing-global", tmp_path / "missing-workspace")

    assert len(resolved.layers) == 2
    assert resolved.effective_skills == {}
