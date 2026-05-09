from __future__ import annotations

import errno
import os
import socket
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PortOwner:
    pid: int
    listener: str
    cwd: str
    cmdline: str


@dataclass(frozen=True)
class PortProbeResult:
    status: str
    host: str
    port: int
    owners: tuple[PortOwner, ...] = field(default_factory=tuple)
    error_errno: int | None = None


def bind_host_port(host: str, port: int) -> None:
    family = socket.AF_INET6 if ":" in host and host != "localhost" else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.bind((host, port))
    finally:
        sock.close()


def _parse_proc_net_tcp(path: Path) -> dict[str, str]:
    listeners: dict[str, str] = {}
    if not path.exists():
        return listeners
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        parts = line.split()
        if len(parts) < 10:
            continue
        local_address = parts[1]
        inode = parts[9]
        listeners[inode] = local_address
    return listeners


def _decode_listener(local_address: str) -> str:
    host_hex, port_hex = local_address.split(":")
    port = int(port_hex, 16)
    if host_hex == "00000000":
        host = "0.0.0.0"
    else:
        host = socket.inet_ntoa(bytes.fromhex(host_hex)[::-1])
    return f"{host}:{port}"


def find_port_owners(host: str, port: int, *, proc_root: str = "/proc") -> list[PortOwner]:
    proc_root_path = Path(proc_root)
    listeners = _parse_proc_net_tcp(proc_root_path / "net" / "tcp")
    owners: list[PortOwner] = []
    for entry in proc_root_path.iterdir():
        if not entry.name.isdigit():
            continue
        fd_dir = entry / "fd"
        if not fd_dir.exists():
            continue
        for fd in fd_dir.iterdir():
            try:
                target = os.readlink(fd)
            except OSError:
                continue
            if not target.startswith("socket:["):
                continue
            inode = target[8:-1]
            local_address = listeners.get(inode)
            if not local_address:
                continue
            listener = _decode_listener(local_address)
            listener_host, listener_port = listener.rsplit(":", 1)
            if int(listener_port) != port:
                continue
            cwd = os.readlink(entry / "cwd") if (entry / "cwd").exists() else ""
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "ignore").strip() if (entry / "cmdline").exists() else ""
            owners.append(PortOwner(pid=int(entry.name), listener=listener, cwd=cwd, cmdline=cmdline))
    return owners


def probe_port(host: str, port: int) -> PortProbeResult:
    try:
        bind_host_port(host, port)
    except OSError as exc:
        if exc.errno == errno.EADDRINUSE:
            return PortProbeResult(status="in_use", host=host, port=port, owners=tuple(find_port_owners(host, port)))
        return PortProbeResult(status="error", host=host, port=port, error_errno=exc.errno)
    return PortProbeResult(status="available", host=host, port=port)


def main(argv: list[str]) -> int:
    if len(argv) >= 3 and argv[0] == "--describe":
        host, port = argv[1], int(argv[2])
        result = probe_port(host, port)
        if result.status == "in_use":
            print(f"port {host}:{port} is already in use")
            for owner in result.owners:
                print(f"pid={owner.pid} listener={owner.listener} cwd={owner.cwd} cmdline={owner.cmdline}")
            return 1
        return 0
    return 0
