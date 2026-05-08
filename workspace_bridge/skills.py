from __future__ import annotations

from pathlib import Path

from .models import ResolvedSkillSpace, SkillDefinition, SkillLayer


def discover_skills(root_dir: Path | str, *, layer_name: str) -> dict[str, SkillDefinition]:
    root = Path(root_dir).expanduser().resolve()
    if not root.exists():
        return {}

    discovered: dict[str, SkillDefinition] = {}
    for child in sorted(root.iterdir(), key=lambda item: item.name):
        if not child.is_dir():
            continue
        skill_file = child / "SKILL.md"
        if not skill_file.is_file():
            continue
        discovered[child.name] = SkillDefinition(
            name=child.name,
            layer=layer_name,
            root_dir=child,
            skill_file=skill_file,
        )
    return discovered


def resolve_skill_space(global_root: Path | str, workspace_root: Path | str) -> ResolvedSkillSpace:
    global_root = Path(global_root).expanduser().resolve()
    workspace_root = Path(workspace_root).expanduser().resolve()

    global_skills = discover_skills(global_root, layer_name="global")
    workspace_skills = discover_skills(workspace_root, layer_name="workspace")

    effective = dict(global_skills)
    effective.update(workspace_skills)

    layers = (
        SkillLayer(name="global", root_dir=global_root, skills=global_skills),
        SkillLayer(name="workspace", root_dir=workspace_root, skills=workspace_skills),
    )
    return ResolvedSkillSpace(layers=layers, effective_skills=effective)
