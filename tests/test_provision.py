from pathlib import Path

from workspace_bridge.context import build_runtime_context
from workspace_bridge.layout import build_workspace_ref
from workspace_bridge.provision import load_workspace_metadata, provision_workspace
from workspace_bridge.workspace_lock import workspace_lock


def write_skill(root: Path, name: str, body: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(body, encoding="utf-8")


def test_provision_workspace_bootstraps_project_and_metadata(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    (source_dir / ".git").mkdir()
    workspace = build_workspace_ref(runtime_root, source_dir, "single:alice")

    provisioned = provision_workspace(workspace)

    assert provisioned.project_ready is True
    assert (workspace.project_dir / "README.md").read_text(encoding="utf-8") == "repo"
    metadata = load_workspace_metadata(workspace)
    assert metadata is not None
    assert metadata["workspaceId"] == workspace.workspace_id
    assert metadata["projectReady"] is True
    assert metadata["sourceMode"] == "git"


def test_provision_workspace_keeps_existing_project_files(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    workspace = build_workspace_ref(runtime_root, source_dir, "single:alice")
    workspace.project_dir.mkdir(parents=True, exist_ok=True)
    (workspace.project_dir / "README.md").write_text("custom", encoding="utf-8")

    provision_workspace(workspace)

    assert (workspace.project_dir / "README.md").read_text(encoding="utf-8") == "custom"


def test_workspace_lock_creates_lock_file(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    workspace = build_workspace_ref(runtime_root, source_dir, "single:alice")

    with workspace_lock(workspace):
        assert workspace.lock_file.exists()


def test_build_runtime_context_uses_workspace_skill_precedence(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    global_skill_dir = tmp_path / "global-skills"
    chatfile_root = tmp_path / "chatfiles"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    workspace = build_workspace_ref(runtime_root, source_dir, "group-user:room-1:alice")
    provision_workspace(workspace)
    write_skill(global_skill_dir, "deploy", "# global")
    write_skill(workspace.skill_dir, "deploy", "# workspace")
    write_skill(workspace.skill_dir, "lint", "# workspace lint")

    context = build_runtime_context(workspace, global_skill_dir=global_skill_dir, chatfile_root=chatfile_root)

    assert context.project_dir == workspace.project_dir
    assert context.chatfile_dir.is_dir()
    assert context.export_dir == context.chatfile_dir
    assert context.effective_skill_names == ("deploy", "lint")
    assert context.env["WECOM_BRIDGE_PROJECT_DIR"] == str(workspace.project_dir)
    assert context.env["WECOM_BRIDGE_USER_ID"] == "alice"
    assert context.env["WECOM_BRIDGE_ROOM_ID"] == "room-1"
