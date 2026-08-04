from __future__ import annotations

import json
import importlib.util
from pathlib import Path
import threading

import pytest

from workspace_bridge.atomic_io import (
    atomic_write_json,
    atomic_write_text,
    ensure_state_directory,
    exclusive_create_json,
    open_state_lock_file,
)


def test_atomic_write_json_creates_private_regular_file(tmp_path: Path) -> None:
    target = tmp_path / "state" / "payload.json"

    atomic_write_json(target, {"responseUrl": "credential"})

    assert json.loads(target.read_text(encoding="utf-8")) == {"responseUrl": "credential"}
    assert target.is_file() and not target.is_symlink()
    assert target.stat().st_mode & 0o777 == 0o600
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_atomic_write_text_replaces_target_symlink_without_touching_victim(tmp_path: Path) -> None:
    victim = tmp_path / "victim.txt"
    victim.write_text("keep", encoding="utf-8")
    target = tmp_path / "state.txt"
    target.symlink_to(victim)

    atomic_write_text(target, "new")

    assert victim.read_text(encoding="utf-8") == "keep"
    assert target.read_text(encoding="utf-8") == "new"
    assert not target.is_symlink()
    assert target.stat().st_mode & 0o777 == 0o600


def test_atomic_write_rejects_symlink_parent(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(OSError, match="parent must not be a symlink"):
        atomic_write_text(linked_parent / "state.txt", "new")

    assert list(real_parent.iterdir()) == []


def test_ensure_state_directory_preserves_existing_mode(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o755)

    result = ensure_state_directory(state_root)

    assert result == state_root
    assert state_root.stat().st_mode & 0o777 == 0o755


def test_ensure_state_directory_rejects_symlink(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(OSError, match="must not be a symlink"):
        ensure_state_directory(linked_parent)


def test_open_state_lock_file_replaces_symlink_without_touching_victim(tmp_path: Path) -> None:
    victim = tmp_path / "victim.lock"
    victim.write_text("keep", encoding="utf-8")
    lock_path = tmp_path / "locks" / "state.lock"
    lock_path.parent.mkdir()
    lock_path.symlink_to(victim)

    with open_state_lock_file(lock_path) as handle:
        handle.write("locked")
        handle.flush()

    assert victim.read_text(encoding="utf-8") == "keep"
    assert lock_path.is_file() and not lock_path.is_symlink()
    assert lock_path.read_text(encoding="utf-8") == "locked"
    assert lock_path.stat().st_mode & 0o777 == 0o600


def test_open_state_lock_file_rejects_symlink_parent(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(OSError, match="parent must not be a symlink"):
        open_state_lock_file(linked_parent / "state.lock")

    assert list(real_parent.iterdir()) == []


def test_exclusive_create_json_is_private_and_does_not_replace_existing(tmp_path: Path) -> None:
    target = tmp_path / "claims" / "schedule.json"

    assert exclusive_create_json(target, {"owner": "first"}) is True
    assert exclusive_create_json(target, {"owner": "second"}) is False

    assert json.loads(target.read_text(encoding="utf-8")) == {"owner": "first"}
    assert target.stat().st_mode & 0o777 == 0o600


def test_state_io_inherits_group_collaboration_from_setgid_root(tmp_path: Path) -> None:
    shared_root = tmp_path / "shared"
    shared_root.mkdir()
    shared_root.chmod(0o2775)

    state_dir = ensure_state_directory(shared_root / "sessions")
    state_file = state_dir / "session.json"
    lock_file = state_dir / "session.lock"
    claim_file = state_dir / "claim.json"
    atomic_write_json(state_file, {"state": "ready"})
    with open_state_lock_file(lock_file):
        pass
    assert exclusive_create_json(claim_file, {"owner": "first"}) is True

    assert state_dir.stat().st_mode & 0o7777 == 0o2770
    assert state_file.stat().st_mode & 0o777 == 0o660
    assert lock_file.stat().st_mode & 0o777 == 0o660
    assert claim_file.stat().st_mode & 0o777 == 0o660


def test_cloudflared_share_state_is_private_and_atomic(tmp_path: Path) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    state_dir = tmp_path / ".cloudflared_share"
    module.ensure_private_dir(state_dir)
    state_path = state_dir / "state.json"
    victim = tmp_path / "victim.json"
    victim.write_text("keep", encoding="utf-8")
    state_path.symlink_to(victim)

    module.save_state(state_path, {"base_url": "https://example.test"})

    assert victim.read_text(encoding="utf-8") == "keep"
    assert json.loads(state_path.read_text(encoding="utf-8")) == {"base_url": "https://example.test"}
    assert state_dir.stat().st_mode & 0o777 == 0o700
    assert state_path.stat().st_mode & 0o777 == 0o600
    assert list(state_dir.glob(".state.json.*.tmp")) == []


def test_cloudflared_share_cleans_started_processes_when_healthcheck_fails(tmp_path: Path, monkeypatch) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_cleanup_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    class FakeProcess:
        def __init__(self, pid: int) -> None:
            self.pid = pid

    http_proc = FakeProcess(101)
    tunnel_proc = FakeProcess(202)
    terminated = []
    source = tmp_path / "report.txt"
    source.write_text("report", encoding="utf-8")
    export_dir = tmp_path / "export"
    monkeypatch.setattr(module, "start_http_server", lambda *_args: (http_proc, 18280, tmp_path / "http.log"))
    monkeypatch.setattr(
        module,
        "start_cloudflared",
        lambda *_args: (tunnel_proc, "https://example.trycloudflare.com", tmp_path / "tunnel.log"),
    )
    monkeypatch.setattr(module, "healthcheck", lambda *_args: (_ for _ in ()).throw(RuntimeError("unhealthy")))
    monkeypatch.setattr(module, "terminate_process", lambda proc: terminated.append(proc.pid))

    with pytest.raises(RuntimeError, match="unhealthy"):
        module.ensure_share(source, export_dir, tmp_path / "tools", 60)

    assert terminated == [202, 101]
    assert not (export_dir / module.STATE_DIRNAME / "state.json").exists()


def test_cloudflared_share_state_lock_is_private_and_exclusive(tmp_path: Path) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_lock_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    state_dir = tmp_path / ".cloudflared_share"
    module.ensure_private_dir(state_dir)
    second_entered = threading.Event()

    def take_second_lock() -> None:
        with module.share_state_lock(state_dir):
            second_entered.set()

    with module.share_state_lock(state_dir):
        thread = threading.Thread(target=take_second_lock)
        thread.start()
        assert second_entered.wait(timeout=0.1) is False

    thread.join(timeout=2)
    assert thread.is_alive() is False
    assert second_entered.is_set()
    assert (state_dir / "state.lock").stat().st_mode & 0o777 == 0o600


def test_cloudflared_share_never_signals_non_positive_pid(tmp_path: Path, monkeypatch) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_pid_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    calls = []
    monkeypatch.setattr(module.os, "kill", lambda *args: calls.append(args))

    module.stop_pid(0)
    module.stop_pid(-1)

    assert calls == []


def test_cloudflared_start_reads_only_current_generation_log(tmp_path: Path, monkeypatch) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_log_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    state_dir = tmp_path / "state"
    tools_dir = tmp_path / "tools"
    state_dir.mkdir()
    tools_dir.mkdir()
    (tools_dir / "cloudflared").touch()
    (state_dir / "cloudflared-old.log").write_text(
        "https://stale.trycloudflare.com\n",
        encoding="utf-8",
    )

    class FakeProcess:
        pid = 321

        def __init__(self, log_path: Path) -> None:
            self.log_path = log_path
            self.wrote_url = False

        def poll(self):
            if not self.wrote_url:
                self.log_path.write_text("https://fresh.trycloudflare.com\n", encoding="utf-8")
                self.wrote_url = True
            return None

    def fake_popen(*_args, **_kwargs):
        return FakeProcess(state_dir / "cloudflared-fresh.log")

    monkeypatch.setattr(module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(module.time, "sleep", lambda *_args: None)

    _proc, url, log_path = module.start_cloudflared(18280, tools_dir, state_dir, "fresh")

    assert url == "https://fresh.trycloudflare.com"
    assert log_path.name == "cloudflared-fresh.log"


def test_cloudflared_share_log_rejects_symlink(tmp_path: Path) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_log_symlink_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    victim = tmp_path / "victim.log"
    victim.write_text("keep", encoding="utf-8")
    log_path = tmp_path / "share.log"
    log_path.symlink_to(victim)

    with pytest.raises(OSError):
        module.open_private_append_log(log_path)

    assert victim.read_text(encoding="utf-8") == "keep"


def test_cloudflared_share_serves_isolated_file_root_and_starts_expiry_reaper(tmp_path: Path, monkeypatch) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_isolation_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    class FakeProcess:
        def __init__(self, pid: int) -> None:
            self.pid = pid

    roots = []
    source = tmp_path / "report.txt"
    source.write_text("report", encoding="utf-8")
    export_dir = tmp_path / "export"
    monkeypatch.setattr(
        module,
        "start_http_server",
        lambda root, *_args: (roots.append(root) or FakeProcess(101), 18280, tmp_path / "http.log"),
    )
    monkeypatch.setattr(
        module,
        "start_cloudflared",
        lambda *_args: (FakeProcess(202), "https://fresh.trycloudflare.com", tmp_path / "tunnel.log"),
    )
    monkeypatch.setattr(module, "start_expiry_reaper", lambda *_args: FakeProcess(303))
    monkeypatch.setattr(module, "healthcheck", lambda *_args: None)
    monkeypatch.setattr(module, "cleanup_legacy_share_processes", lambda *_args: None)

    url = module.ensure_share(source, export_dir, tmp_path / "tools", 60)

    state_dir = export_dir / module.STATE_DIRNAME
    public_dir = state_dir / module.PUBLIC_DIRNAME
    state = json.loads((state_dir / "state.json").read_text(encoding="utf-8"))
    assert url == "https://fresh.trycloudflare.com/report.txt"
    assert roots == [public_dir]
    assert (public_dir / "report.txt").read_text(encoding="utf-8") == "report"
    assert not (public_dir / "state.json").exists()
    assert state["serve_dir"] == str(public_dir)
    assert state["reaper_pid"] == 303


def test_cloudflared_expiry_reaper_stops_current_generation_and_removes_state(tmp_path: Path, monkeypatch) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_expiry_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    state_dir = tmp_path / ".cloudflared_share"
    serve_dir = state_dir / module.PUBLIC_DIRNAME
    tools_dir = tmp_path / "tools"
    module.ensure_private_dir(state_dir)
    module.ensure_private_dir(serve_dir)
    tools_dir.mkdir()
    generation = "generation-1"
    module.save_state(
        state_dir / "state.json",
        {
            "generation": generation,
            "serve_dir": str(serve_dir),
            "tools_dir": str(tools_dir),
            "expires_at": module.time.time() - 1,
        },
    )
    stopped = []
    monkeypatch.setattr(module, "stop_state_processes", lambda state, **_kwargs: stopped.append(state["generation"]))

    assert module.expire_share_state(state_dir, generation) == 0
    assert stopped == [generation]
    assert not (state_dir / "state.json").exists()


def test_cloudflared_http_handler_disables_directory_listing(tmp_path: Path) -> None:
    script_path = (
        Path(__file__).resolve().parent.parent
        / "relate-skills"
        / "wecom-file-reply"
        / "scripts"
        / "share_via_cloudflared.py"
    )
    spec = importlib.util.spec_from_file_location("share_via_cloudflared_handler_test", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    handler = object.__new__(module.FileOnlyRequestHandler)
    errors = []
    handler.send_error = lambda code, message: errors.append((code, message))

    assert handler.list_directory(str(tmp_path)) is None
    assert errors == [(404, "File not found")]
