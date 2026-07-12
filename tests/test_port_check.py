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


def test_bind_host_port_uses_getaddrinfo_for_localhost(monkeypatch) -> None:
    calls = {}

    class FakeSocket:
        def __init__(self, family, socktype, proto) -> None:
            calls["socket"] = (family, socktype, proto)
            self.bound = None

        def bind(self, sockaddr) -> None:
            calls["bind"] = sockaddr

        def close(self) -> None:
            calls["closed"] = True

    monkeypatch.setattr(
        port_check.socket,
        "getaddrinfo",
        lambda host, port, type: [(port_check.socket.AF_INET6, port_check.socket.SOCK_STREAM, 0, "", ("::1", port, 0, 0))],
    )
    monkeypatch.setattr(port_check.socket, "socket", FakeSocket)

    port_check.bind_host_port("localhost", 6288)

    assert calls["socket"][0] == port_check.socket.AF_INET6
    assert calls["bind"] == ("::1", 6288, 0, 0)
    assert calls["closed"] is True


def test_bind_host_port_strips_ipv6_brackets(monkeypatch) -> None:
    captured = {}

    class FakeSocket:
        def __init__(self, family, socktype, proto) -> None:
            return None

        def bind(self, sockaddr) -> None:
            captured["sockaddr"] = sockaddr

        def close(self) -> None:
            return None

    def fake_getaddrinfo(host, port, type):
        captured["host"] = host
        return [(port_check.socket.AF_INET6, port_check.socket.SOCK_STREAM, 0, "", ("::1", port, 0, 0))]

    monkeypatch.setattr(port_check.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(port_check.socket, "socket", FakeSocket)

    port_check.bind_host_port("[::1]", 6288)

    assert captured["host"] == "::1"
    assert captured["sockaddr"] == ("::1", 6288, 0, 0)


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


def test_find_port_owners_tolerates_unreadable_proc_net_tcp_file(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    (proc_root / "net" / "tcp").mkdir()

    owners = find_port_owners("127.0.0.1", 6288, proc_root=str(proc_root))

    assert owners == []


def test_find_port_owners_filters_out_unrelated_host_listener(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    (proc_root / "net" / "tcp").write_text(
        "\n".join(
            [
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
                "   0: 0200007F:1890 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 456 1 0000000000000000 100 0 0 10 0",
                "   1: 0300007F:1890 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 789 1 0000000000000000 100 0 0 10 0",
            ]
        ),
        encoding="utf-8",
    )
    fd_dir_a = proc_root / "321" / "fd"
    fd_dir_b = proc_root / "322" / "fd"
    fd_dir_a.mkdir(parents=True)
    fd_dir_b.mkdir(parents=True)
    (proc_root / "321" / "cmdline").write_bytes(b"python3\0a.py\0")
    (proc_root / "321" / "cwd").symlink_to("/srv/a")
    (proc_root / "322" / "cmdline").write_bytes(b"python3\0b.py\0")
    (proc_root / "322" / "cwd").symlink_to("/srv/b")
    (fd_dir_a / "9").symlink_to("socket:[456]")
    (fd_dir_b / "9").symlink_to("socket:[789]")

    owners = find_port_owners("127.0.0.2", 6288, proc_root=str(proc_root))

    assert len(owners) == 1
    assert owners[0].pid == 321
    assert owners[0].listener == "127.0.0.2:6288"


def test_find_port_owners_for_localhost_matches_only_resolved_hosts(tmp_path: Path, monkeypatch) -> None:
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    (proc_root / "net" / "tcp").write_text(
        "\n".join(
            [
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
                "   0: 0100007F:1890 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 456 1 0000000000000000 100 0 0 10 0",
                "   1: 0200007F:1890 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 789 1 0000000000000000 100 0 0 10 0",
            ]
        ),
        encoding="utf-8",
    )
    fd_dir_a = proc_root / "321" / "fd"
    fd_dir_b = proc_root / "322" / "fd"
    fd_dir_a.mkdir(parents=True)
    fd_dir_b.mkdir(parents=True)
    (proc_root / "321" / "cmdline").write_bytes(b"python3\0a.py\0")
    (proc_root / "321" / "cwd").symlink_to("/srv/a")
    (proc_root / "322" / "cmdline").write_bytes(b"python3\0b.py\0")
    (proc_root / "322" / "cwd").symlink_to("/srv/b")
    (fd_dir_a / "9").symlink_to("socket:[456]")
    (fd_dir_b / "9").symlink_to("socket:[789]")

    monkeypatch.setattr(
        port_check.socket,
        "getaddrinfo",
        lambda host, _port, type: [(port_check.socket.AF_INET, port_check.socket.SOCK_STREAM, 0, "", ("127.0.0.1", 6288))],
    )

    owners = find_port_owners("localhost", 6288, proc_root=str(proc_root))

    assert len(owners) == 1
    assert owners[0].pid == 321
    assert owners[0].listener == "127.0.0.1:6288"


def test_find_port_owners_deduplicates_multiple_fds_for_same_socket(tmp_path: Path) -> None:
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
    (fd_dir / "10").symlink_to("socket:[456]")

    owners = find_port_owners("127.0.0.1", 6288, proc_root=str(proc_root))

    assert len(owners) == 1
    assert owners[0].pid == 321


def test_find_port_owners_ignores_non_listening_sockets(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    (proc_root / "net" / "tcp").write_text(
        "\n".join(
            [
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
                "   0: 0100007F:1890 0200007F:0050 01 00000000:00000000 00:00000000 00000000 0 0 456 1 0000000000000000 100 0 0 10 0",
            ]
        ),
        encoding="utf-8",
    )
    fd_dir = proc_root / "321" / "fd"
    fd_dir.mkdir(parents=True)
    (proc_root / "321" / "cmdline").write_bytes(b"python3\0client.py\0")
    (proc_root / "321" / "cwd").symlink_to("/srv/bridge")
    (fd_dir / "9").symlink_to("socket:[456]")

    owners = find_port_owners("127.0.0.1", 6288, proc_root=str(proc_root))

    assert owners == []


def test_find_port_owners_matches_ipv6_listener(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    (proc_root / "net").mkdir(parents=True)
    (proc_root / "net" / "tcp6").write_text(
        "\n".join(
            [
                "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode",
                "   0: 00000000000000000000000001000000:1890 00000000000000000000000000000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 456 1 0000000000000000 100 0 0 10 0",
            ]
        ),
        encoding="utf-8",
    )
    fd_dir = proc_root / "321" / "fd"
    fd_dir.mkdir(parents=True)
    (proc_root / "321" / "cmdline").write_bytes(b"python3\0-m\0aiohttp.web\0")
    (proc_root / "321" / "cwd").symlink_to("/srv/bridge")
    (fd_dir / "9").symlink_to("socket:[456]")

    owners = find_port_owners("::1", 6288, proc_root=str(proc_root))

    assert len(owners) == 1
    assert owners[0].pid == 321
    assert owners[0].listener == "[::1]:6288"


def test_find_port_owners_tolerates_unreadable_cwd_entry(tmp_path: Path) -> None:
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
    (proc_root / "321" / "cwd").write_text("not-a-symlink", encoding="utf-8")
    (fd_dir / "9").symlink_to("socket:[456]")

    owners = find_port_owners("127.0.0.1", 6288, proc_root=str(proc_root))

    assert len(owners) == 1
    assert owners[0].cwd == ""
    assert owners[0].cmdline == "python3 -m aiohttp.web"


def test_find_port_owners_tolerates_unreadable_cmdline_entry(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    cwd_target = tmp_path / "srv" / "bridge"
    cwd_target.mkdir(parents=True)
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
    (proc_root / "321" / "cmdline").mkdir()
    (proc_root / "321" / "cwd").symlink_to(cwd_target)
    (fd_dir / "9").symlink_to("socket:[456]")

    owners = find_port_owners("127.0.0.1", 6288, proc_root=str(proc_root))

    assert len(owners) == 1
    assert owners[0].cwd == str(cwd_target)
    assert owners[0].cmdline == ""


def test_find_port_owners_returns_empty_when_proc_root_missing(tmp_path: Path) -> None:
    owners = find_port_owners("127.0.0.1", 6288, proc_root=str(tmp_path / "missing-proc"))

    assert owners == []


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


def test_main_describe_returns_nonzero_on_probe_error(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        port_check,
        "probe_port",
        lambda host, port: PortProbeResult(
            status="error",
            host=host,
            port=port,
            error_errno=errno.EPERM,
        ),
    )

    exit_code = port_check.main(["--describe", "127.0.0.1", "6288"])

    assert exit_code == 2
    output = capsys.readouterr().out
    assert "port 127.0.0.1:6288 probe failed errno=" in output


def test_main_returns_usage_error_for_invalid_args(capsys) -> None:
    exit_code = port_check.main([])

    assert exit_code == 2
    assert "usage: --describe <host> <port>" in capsys.readouterr().out


def test_main_returns_usage_error_for_invalid_port(capsys) -> None:
    exit_code = port_check.main(["--describe", "127.0.0.1", "not-a-port"])

    assert exit_code == 2
    assert "invalid port: not-a-port" in capsys.readouterr().out


def test_main_returns_usage_error_for_out_of_range_port(capsys) -> None:
    exit_code = port_check.main(["--describe", "127.0.0.1", "65536"])

    assert exit_code == 2
    assert "invalid port: 65536" in capsys.readouterr().out
