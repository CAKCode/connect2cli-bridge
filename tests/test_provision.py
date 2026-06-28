from pathlib import Path

import workspace_bridge.context as context_module
import workspace_bridge.models as models_module
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
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice")

    provisioned = provision_workspace(workspace)

    assert provisioned.project_ready is True
    assert (workspace.cwd_dir / "README.md").read_text(encoding="utf-8") == "repo"
    metadata = load_workspace_metadata(workspace)
    assert metadata is not None
    assert metadata["workspaceId"] == workspace.workspace_id
    assert metadata["projectReady"] is True
    assert metadata["sourceMode"] == "git"


def test_provision_workspace_bootstraps_project_even_with_skill_dir_precreated(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice")
    workspace.cwd_dir.mkdir(parents=True, exist_ok=True)
    workspace.skill_dir.mkdir(parents=True, exist_ok=True)

    provision_workspace(workspace)

    assert (workspace.cwd_dir / "README.md").read_text(encoding="utf-8") == "repo"


def test_provision_workspace_keeps_existing_project_files(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice")
    workspace.cwd_dir.mkdir(parents=True, exist_ok=True)
    (workspace.cwd_dir / "README.md").write_text("custom", encoding="utf-8")

    provision_workspace(workspace)

    assert (workspace.cwd_dir / "README.md").read_text(encoding="utf-8") == "custom"


def test_provision_workspace_skips_runtime_root_copy_to_avoid_recursive_project(tmp_path: Path) -> None:
    runtime_root = tmp_path / "repo" / ".workspace-bridge-runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    runtime_root.mkdir(parents=True)
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    nested_runtime_file = runtime_root / "marker.txt"
    nested_runtime_file.write_text("runtime", encoding="utf-8")
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice")

    provision_workspace(workspace)

    assert (workspace.cwd_dir / "README.md").read_text(encoding="utf-8") == "repo"
    assert not (workspace.cwd_dir / ".workspace-bridge-runtime").exists()


def test_provision_workspace_refreshes_source_revision_when_head_changes(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    git_dir = source_dir / ".git"
    refs_dir = git_dir / "refs" / "heads"
    refs_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    (git_dir / "HEAD").write_text("ref: refs/heads/main\n", encoding="utf-8")
    main_ref = refs_dir / "main"
    main_ref.write_text("rev-1\n", encoding="utf-8")
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice")

    first = provision_workspace(workspace)
    main_ref.write_text("rev-2\n", encoding="utf-8")
    second = provision_workspace(workspace)

    assert first.source_revision == "rev-1"
    assert second.source_revision == "rev-2"


def test_workspace_lock_creates_lock_file(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice")

    with workspace_lock(workspace):
        assert workspace.lock_file.exists()


def test_build_runtime_context_uses_workspace_skills(tmp_path: Path, monkeypatch) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    chatfile_root = tmp_path / "chatfiles"
    global_skill_dir = tmp_path / "global-skills"
    source_dir.mkdir()
    global_skill_dir.mkdir()
    monkeypatch.setattr(models_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    monkeypatch.setattr(context_module, "DEFAULT_GLOBAL_SKILL_DIR", global_skill_dir.resolve())
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "group-user:room-1:alice")
    provision_workspace(workspace)
    write_skill(workspace.skill_dir, "deploy", "# workspace")
    write_skill(workspace.skill_dir, "lint", "# workspace lint")

    context = build_runtime_context(
        workspace,
        runtime_root=runtime_root,
        session_id="session-1",
        chatfile_root=chatfile_root,
    )

    assert context.cwd_dir == workspace.cwd_dir
    assert context.chatfile_dir.is_dir()
    assert context.export_dir == context.chatfile_dir
    assert context.workfile_dir == workspace.workfile_dir
    assert context.roomfile_dir == workspace.roomfile_dir
    assert context.allowed_file_roots == (context.chatfile_dir.resolve(),)
    assert context.effective_skill_names == ("deploy", "lint")
    assert context.env["WECOM_BRIDGE_PROJECT_DIR"] == str(workspace.cwd_dir)
    assert context.env["WECOM_BRIDGE_WORKSPACE_MODE"] == "team"
    assert context.env["WECOM_BRIDGE_WORKFILE_DIR"] == str(workspace.workfile_dir)
    assert context.env["WECOM_BRIDGE_ROOMFILE_DIR"] == str(workspace.roomfile_dir)
    assert context.env["WECOM_BRIDGE_USER_ID"] == "alice"
    assert context.env["WECOM_BRIDGE_ROOM_ID"] == "room-1"


def test_provision_workspace_migrates_legacy_workfile_without_overwrite(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    legacy = runtime_root / "workspace" / "team-a" / "users" / "alice" / "workfile"
    legacy.mkdir(parents=True, exist_ok=True)
    (legacy / "from-legacy.txt").write_text("legacy", encoding="utf-8")
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice")
    workspace.workfile_dir.mkdir(parents=True, exist_ok=True)
    (workspace.workfile_dir / "keep.txt").write_text("new", encoding="utf-8")

    provision_workspace(workspace)

    assert (workspace.workfile_dir / "from-legacy.txt").read_text(encoding="utf-8") == "legacy"
    assert (workspace.workfile_dir / "keep.txt").read_text(encoding="utf-8") == "new"
    assert (workspace.workfile_dir / ".workspace-layout-migration.json").exists()


def test_provision_workspace_skips_bootstrap_and_legacy_migration_in_personal_mode(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    (source_dir / "README.md").write_text("repo", encoding="utf-8")
    legacy = runtime_root / "workspace" / "team-a" / "users" / "alice" / "workfile"
    legacy.mkdir(parents=True, exist_ok=True)
    (legacy / "from-legacy.txt").write_text("legacy", encoding="utf-8")
    workspace = build_workspace_ref(runtime_root, "team-a", source_dir, "single:alice", workspace_mode="personal")

    provision_workspace(workspace)

    assert workspace.cwd_dir == source_dir.resolve()
    assert (source_dir / "README.md").read_text(encoding="utf-8") == "repo"
    assert not (workspace.project_dir / "from-legacy.txt").exists()
