from __future__ import annotations

import argparse
import os
import shlex
import stat
import subprocess
from pathlib import Path

import pytest

import check_production_readiness as probe


def valid_args(**overrides):
    values = {
        "duration_sec": 1.0,
        "concurrency": 2,
        "startup_timeout_sec": 5.0,
        "recovery_timeout_sec": 5.0,
        "request_timeout_sec": 1.0,
        "sample_interval_sec": 0.1,
        "min_requests": 1,
        "max_rss_mib": 128.0,
        "max_p95_ms": 1000.0,
        "max_descendants": 2,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"duration_sec": 0}, "duration-sec"),
        ({"concurrency": 0}, "concurrency"),
        ({"request_timeout_sec": 0}, "timeouts"),
        ({"min_requests": 0}, "thresholds"),
        ({"max_descendants": -1}, "thresholds"),
    ],
)
def test_validate_args_rejects_invalid_values(overrides, message):
    with pytest.raises(ValueError, match=message):
        probe.validate_args(valid_args(**overrides))


def test_positive_int_file_rejects_unsafe_pid_values(tmp_path):
    pid_file = tmp_path / "bridge.pid"
    for value in ("", "0", "-1", "123 extra", "abc"):
        pid_file.write_text(value, encoding="utf-8")
        with pytest.raises(ValueError, match="invalid PID file"):
            probe.positive_int_file(pid_file)

    pid_file.write_text("123\n", encoding="utf-8")
    assert probe.positive_int_file(pid_file) == 123


def test_is_owned_process_requires_script_and_control_root(tmp_path, monkeypatch):
    control_root = tmp_path / "control"
    control_root.mkdir()
    monkeypatch.setattr(probe, "process_argv", lambda _pid: [str(probe.REPO_ROOT / "bridge.py")])
    monkeypatch.setattr(
        probe,
        "process_environment",
        lambda _pid: {"BRIDGE_CONTROL_ROOT": str(control_root)},
    )

    assert probe.is_owned_process(123, kind="bridge", control_root=control_root)
    assert not probe.is_owned_process(123, kind="watchdog", control_root=control_root)
    assert not probe.is_owned_process(123, kind="bridge", control_root=tmp_path / "other-control")


def test_signal_owned_process_uses_pidfd_and_rechecks_ownership(tmp_path, monkeypatch):
    control_root = tmp_path / "control"
    control_root.mkdir()
    calls = []
    monkeypatch.setattr(probe.os, "pidfd_open", lambda pid, flags: calls.append(("open", pid, flags)) or 44)
    monkeypatch.setattr(probe.os, "close", lambda fd: calls.append(("close", fd)))
    monkeypatch.setattr(
        probe.signal,
        "pidfd_send_signal",
        lambda fd, sig: calls.append(("signal", fd, sig)),
    )
    monkeypatch.setattr(probe, "is_owned_process", lambda *_args, **_kwargs: False)

    assert probe.signal_owned_process(123, probe.signal.SIGKILL, kind="bridge", control_root=control_root) is False
    assert calls == [("open", 123, 0), ("close", 44)]

    calls.clear()
    monkeypatch.setattr(probe, "is_owned_process", lambda *_args, **_kwargs: True)
    assert probe.signal_owned_process(123, probe.signal.SIGKILL, kind="bridge", control_root=control_root) is True
    assert calls == [("open", 123, 0), ("signal", 44, probe.signal.SIGKILL), ("close", 44)]


def test_filesystem_queue_depth_counts_only_pending_and_processing(tmp_path):
    shared_root = tmp_path / "shared"
    runtime_root = tmp_path / "runtime"
    paths = [
        shared_root / ".scheduled-messages" / "pending" / "schedule.json",
        runtime_root / ".local-file-send-queue" / "processing" / "file.json",
        runtime_root / ".local-send-message-queue" / "targets" / "bot-1" / "pending" / "message.json",
    ]
    ignored = runtime_root / ".local-send-message-queue" / "done" / "done.json"
    for path in [*paths, ignored]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")

    assert probe.filesystem_queue_depth(shared_root, runtime_root) == 3


def test_summarize_resources_reports_peaks_and_cpu_percent(monkeypatch):
    monkeypatch.setattr(probe.os, "sysconf", lambda _name: 100)
    summary = probe.summarize_resources(
        [
            {"atMs": 1000, "rssKiB": 100, "cpuTicks": 10, "descendantCount": 0, "filesystemQueueDepth": 0},
            {"atMs": 2000, "rssKiB": 150, "cpuTicks": 60, "descendantCount": 2, "filesystemQueueDepth": 1},
        ]
    )

    assert summary == {
        "sampleCount": 2,
        "maxRssKiB": 150,
        "maxCpuPercent": 50.0,
        "maxDescendants": 2,
        "maxQueueDepth": 1,
    }


def test_sanitized_launcher_environment_removes_live_bridge_configuration(tmp_path, monkeypatch):
    monkeypatch.setenv("BRIDGE_TOKEN", "live-token")
    monkeypatch.setenv("WECOM_BOT_SECRET_FILE", "/live/secret")
    monkeypatch.setenv("LOCAL_SEND_MESSAGE_QUEUE_ROOT", "/live/queue")
    env_file = tmp_path / "probe.env"

    environment = probe.sanitized_launcher_environment(env_file)

    assert environment["BRIDGE_ENV_FILE"] == str(env_file)
    assert environment["NO_PROXY"] == "127.0.0.1,localhost"
    assert "BRIDGE_TOKEN" not in environment
    assert "WECOM_BOT_SECRET_FILE" not in environment
    assert "LOCAL_SEND_MESSAGE_QUEUE_ROOT" not in environment


def test_write_env_file_quotes_values_and_sets_private_mode(tmp_path):
    env_file = tmp_path / "probe.env"
    probe.write_env_file(env_file, {"VALUE": "path with spaces/it's-safe"})

    result = subprocess.run(
        ["sh", "-c", '. "$1"; printf "%s" "$VALUE"', "sh", str(env_file)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout == "path with spaces/it's-safe"
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600


def test_run_probe_cleans_pid_files_when_launcher_times_out(monkeypatch):
    terminated: list[tuple[int, str]] = []
    alive = {(111, "watchdog"), (222, "bridge")}

    def fake_run(*_args, **kwargs):
        env_file = Path(kwargs["env"]["BRIDGE_ENV_FILE"])
        values = {}
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, raw = line.split("=", 1)
            values[key] = shlex.split(raw)[0]
        control_root = Path(values["BRIDGE_CONTROL_ROOT"])
        (control_root / ".bridge.guard.pid").write_text("111\n", encoding="utf-8")
        (control_root / ".bridge.pid").write_text("222\n", encoding="utf-8")
        raise subprocess.TimeoutExpired(["sh", "start.sh"], timeout=1)

    def fake_owned(pid, *, kind, control_root, repo_root=probe.REPO_ROOT):
        del control_root, repo_root
        return (pid, kind) in alive

    def fake_terminate(pid, *, kind, control_root, timeout_sec=12):
        del control_root, timeout_sec
        terminated.append((pid, kind))
        alive.discard((pid, kind))
        return True

    monkeypatch.setattr(probe, "choose_free_port", lambda: 19493)
    monkeypatch.setattr(probe.subprocess, "run", fake_run)
    monkeypatch.setattr(probe, "is_owned_process", fake_owned)
    monkeypatch.setattr(probe, "terminate_owned_process", fake_terminate)

    result = probe.run_probe(valid_args())

    assert result["pass"] is False
    assert result["cleanup"] == {"ok": True, "remainingOwnedProcesses": []}
    assert terminated == [(111, "watchdog"), (222, "bridge")]
