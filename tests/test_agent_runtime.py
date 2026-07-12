import os
from pathlib import Path

import workspace_bridge.agent_runtime as agent_runtime_module
from workspace_bridge.agent_runtime import prepare_claude_runtime_root


def test_prepare_claude_runtime_root_preserves_symlinked_plugins_dir(tmp_path: Path) -> None:
    base_root = tmp_path / "claude-runtime"
    source_root = tmp_path / "source-claude"
    external_plugins = tmp_path / "external-plugins"
    external_plugins.mkdir(parents=True, exist_ok=True)
    (external_plugins / "plugin.json").write_text("{}", encoding="utf-8")
    source_root.mkdir(parents=True, exist_ok=True)
    (source_root / "plugins").symlink_to(external_plugins, target_is_directory=True)

    layout = prepare_claude_runtime_root(
        base_root,
        session_id="session-1",
        source_claude_root=source_root,
        uid=os.getuid(),
        gid=os.getgid(),
    )

    plugins_dir = layout["claude_config_dir"] / "plugins"
    assert plugins_dir.is_symlink()
    assert plugins_dir.resolve() == external_plugins.resolve()
    assert os.path.lexists(plugins_dir)
    assert (plugins_dir / "plugin.json").read_text(encoding="utf-8") == "{}"


def test_prepare_claude_runtime_root_preserves_symlinked_settings_file(tmp_path: Path) -> None:
    base_root = tmp_path / "claude-runtime"
    source_root = tmp_path / "source-claude"
    external_settings = tmp_path / "external-settings.json"
    external_settings.write_text('{"model":"claude"}', encoding="utf-8")
    source_root.mkdir(parents=True, exist_ok=True)
    (source_root / "settings.json").symlink_to(external_settings)

    layout = prepare_claude_runtime_root(
        base_root,
        session_id="session-1",
        source_claude_root=source_root,
        uid=os.getuid(),
        gid=os.getgid(),
    )

    settings_file = layout["claude_config_dir"] / "settings.json"
    assert settings_file.is_symlink()
    assert settings_file.resolve() == external_settings.resolve()
    assert os.path.lexists(settings_file)
    assert settings_file.read_text(encoding="utf-8") == '{"model":"claude"}'


def test_prepare_claude_runtime_root_chowns_symlink_entries_without_following_targets(tmp_path: Path, monkeypatch) -> None:
    base_root = tmp_path / "claude-runtime"
    source_root = tmp_path / "source-claude"
    external_plugins = tmp_path / "external-plugins"
    external_settings = tmp_path / "external-settings.json"
    external_plugins.mkdir(parents=True, exist_ok=True)
    external_settings.write_text('{"model":"claude"}', encoding="utf-8")
    source_root.mkdir(parents=True, exist_ok=True)
    (source_root / "plugins").symlink_to(external_plugins, target_is_directory=True)
    (source_root / "settings.json").symlink_to(external_settings)
    calls: list[tuple[str, int, int, bool]] = []

    def fake_chown(path, uid, gid, *, follow_symlinks=True):
        calls.append((os.fspath(path), uid, gid, follow_symlinks))

    monkeypatch.setattr(agent_runtime_module.os, "chown", fake_chown)

    layout = prepare_claude_runtime_root(
        base_root,
        session_id="session-1",
        source_claude_root=source_root,
        uid=123,
        gid=456,
    )

    plugins_path = os.fspath(layout["claude_config_dir"] / "plugins")
    settings_path = os.fspath(layout["claude_config_dir"] / "settings.json")
    assert (plugins_path, 123, 456, False) in calls
    assert (settings_path, 123, 456, False) in calls


def test_prepare_claude_runtime_root_replaces_broken_symlinked_settings_file(tmp_path: Path) -> None:
    base_root = tmp_path / "claude-runtime"
    source_root = tmp_path / "source-claude"
    source_root.mkdir(parents=True, exist_ok=True)
    (source_root / "settings.json").write_text('{"model":"claude"}', encoding="utf-8")
    stale_target = tmp_path / "missing-settings.json"
    stale_link = base_root / "sessions" / "session-1" / ".claude" / "settings.json"
    stale_link.parent.mkdir(parents=True, exist_ok=True)
    stale_link.symlink_to(stale_target)

    layout = prepare_claude_runtime_root(
        base_root,
        session_id="session-1",
        source_claude_root=source_root,
        uid=os.getuid(),
        gid=os.getgid(),
    )

    settings_file = layout["claude_config_dir"] / "settings.json"
    assert settings_file.is_file()
    assert not settings_file.is_symlink()
    assert settings_file.read_text(encoding="utf-8") == '{"model":"claude"}'


def test_prepare_claude_runtime_root_replaces_broken_symlinked_plugins_dir(tmp_path: Path) -> None:
    base_root = tmp_path / "claude-runtime"
    source_root = tmp_path / "source-claude"
    source_root.mkdir(parents=True, exist_ok=True)
    (source_root / "plugins").mkdir()
    (source_root / "plugins" / "plugin.json").write_text("{}", encoding="utf-8")
    stale_target = tmp_path / "missing-plugins"
    stale_link = base_root / "sessions" / "session-1" / ".claude" / "plugins"
    stale_link.parent.mkdir(parents=True, exist_ok=True)
    stale_link.symlink_to(stale_target, target_is_directory=True)

    layout = prepare_claude_runtime_root(
        base_root,
        session_id="session-1",
        source_claude_root=source_root,
        uid=os.getuid(),
        gid=os.getgid(),
    )

    plugins_dir = layout["claude_config_dir"] / "plugins"
    assert plugins_dir.is_dir()
    assert not plugins_dir.is_symlink()
    assert (plugins_dir / "plugin.json").read_text(encoding="utf-8") == "{}"
