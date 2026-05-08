from __future__ import annotations

import errno
from pathlib import Path

from workspace_bridge import port_check
from workspace_bridge.port_check import PortOwner, PortProbeResult, find_port_owners


def test_probe_port_reports_address_in_use(monkeypatch) -> None:
    def fake_bind(_host: str, _port: int) -> None:
        raise OSError(errno.EADDRINUSE, "Address already in use")

    monkeypatch.setattr(port_check, "bind_host_port", fake_bind)
    monkeypatch.setattr(
        port_check,
        "find_port_owners",
        lambda _host, _port: (
            PortOwner(pid=321, listener="127.0.0.1:6288", cwd="/srv/bridge", cmdline="python3 -m aiohttp.web"),
        ),
    )

    result = port_check.probe_port("127.0.0.1", 6288)

    assert result.status == "in_use"
    assert result.owners[0].pid == 321


def test_probe_port_reports_non_bind_errors(monkeypatch) -> None:
    def fake_bind(_host: str, _port: int) -> None:
        raise PermissionError(errno.EPERM, "Operation not permitted")

    monkeypatch.setattr(port_check, "bind_host_port", fake_bind)

    result = port_check.probe_port("127.0.0.1", 6288)

    assert result.status == "error"
    assert result.error_errno == errno.EPERM


def test_find_port_owners_matches_wildcard_listener(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    (proc_root / "net" / "tcp").write_text(
        "\n".join(
            [
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
                "   0: 00000000:1890 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 456 1 0000000000000000 100 0 0 10 0",
            ]
        ),
        encoding="utf-8",
    )
    fd_dir = proc_root / "321" / "fd"
    fd_dir.mkdir(parents=True)
    (proc_root / "321" / "cmdline").write_bytes(b"python3\0-m\0aiohttp.web\0")
    (proc_root / "321" / "cwd").symlink_to("/srv/bridge")
    (fd_dir / "9").symlink_to("socket:[456]")

    owners = find_port_owners("127.0.0.1", 6288, proc_root=str(proc_root))

    assert len(owners) == 1
    assert owners[0].pid == 321
    assert owners[0].listener == "0.0.0.0:6288"


def test_main_describe_prints_owner_details(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        port_check,
        "probe_port",
        lambda host, port: PortProbeResult(
            status="in_use",
            host=host,
            port=port,
            owners=(
                PortOwner(pid=321, listener="127.0.0.1:6288", cwd="/srv/bridge", cmdline="python3 -m aiohttp.web"),
            ),
        ),
    )

    exit_code = port_check.main(["--describe", "127.0.0.1", "6288"])

    assert exit_code == 1
    output = capsys.readouterr().out
    assert "port 127.0.0.1:6288 is already in use" in output
    assert "pid=321" in output
