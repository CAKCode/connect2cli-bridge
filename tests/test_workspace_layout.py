from pathlib import Path

import pytest

from workspace_bridge.layout import build_workspace_ref, ensure_workspace_dirs, parse_chat_key, source_key


def test_parse_chat_key_accepts_single_chat() -> None:
    parsed = parse_chat_key("single:alice")

    assert parsed == {"scope": "user", "user_id": "alice"}


def test_parse_chat_key_accepts_group_user_chat() -> None:
    parsed = parse_chat_key("group-user:room-1:alice")

    assert parsed == {"scope": "user", "room_id": "room-1", "user_id": "alice"}


def test_parse_chat_key_accepts_group_chat() -> None:
    parsed = parse_chat_key("group:room-1")

    assert parsed == {"scope": "room", "room_id": "room-1"}


@pytest.mark.parametrize("chat_key", ["", "single:", "group:", "group-user:room-only", "invalid"])
def test_parse_chat_key_rejects_invalid_values(chat_key: str) -> None:
    with pytest.raises(ValueError, match="invalid chat key"):
        parse_chat_key(chat_key)


def test_source_key_is_stable_for_same_path(tmp_path: Path) -> None:
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    assert source_key(source_dir) == source_key(str(source_dir))


def test_single_and_group_user_share_same_user_workspace(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    single_workspace = build_workspace_ref(runtime_root, source_dir, "single:alice")
    group_user_workspace = build_workspace_ref(runtime_root, source_dir, "group-user:room-1:alice")

    assert single_workspace.scope == "user"
    assert group_user_workspace.scope == "user"
    assert single_workspace.root_dir == group_user_workspace.root_dir
    assert single_workspace.project_dir == group_user_workspace.project_dir
    assert group_user_workspace.owner_room_id == "room-1"


def test_group_chat_uses_room_workspace(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()

    workspace = build_workspace_ref(runtime_root, source_dir, "group:room-1")

    assert workspace.scope == "room"
    assert workspace.owner_user_id is None
    assert workspace.owner_room_id == "room-1"
    assert workspace.root_dir == runtime_root / "workspaces" / "rooms" / "room-1" / workspace.source_key


def test_ensure_workspace_dirs_creates_expected_directories(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    source_dir = tmp_path / "repo"
    source_dir.mkdir()
    workspace = build_workspace_ref(runtime_root, source_dir, "single:alice")

    ensure_workspace_dirs(workspace)

    assert workspace.root_dir.is_dir()
    assert workspace.project_dir.is_dir()
    assert workspace.skill_dir.is_dir()
    assert workspace.state_dir.is_dir()
    assert workspace.lock_file.parent.is_dir()
