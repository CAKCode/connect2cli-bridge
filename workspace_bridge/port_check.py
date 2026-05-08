from __future__ import annotations

import argparse
import errno
import os
import socket
from dataclasses import dataclass
from pathlib import Path


LISTEN_STATE = "0A"


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
    owners: tuple[PortOwner, ...] = ()
    error_errno: int | None = None
    error_message: str | None = None


def socket_family_for_host(host: str) -> int:
    return socket.AF_INET6 if ":" in host and host != "localhost" else socket.AF_INET


def normalize_host(host: str) -> str:
    return "127.0.0.1" if host == "localhost" else host


def bind_host_port(host: str, port: int) -> None:
    family = socket_family_for_host(host)
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
    finally:
        sock.close()


def decode_ipv4(hex_value: str) -> str:
    raw = bytes.fromhex(hex_value)
    return socket.inet_ntop(socket.AF_INET, raw[::-1])


def decode_ipv6(hex_value: str) -> str:
    raw = bytes.fromhex(hex_value)
    try:
        return socket.inet_ntop(socket.AF_INET6, raw)
    except OSError:
        words = [raw[index : index + 4][::-1] for index in range(0, len(raw), 4)]
        return socket.inet_ntop(socket.AF_INET6, b"".join(words))


def iter_listeners(table_path: Path, family: int) -> tuple[tuple[str, int, str], ...]:
    listeners: list[tuple[str, int, str]] = []
    if not table_path.exists():
        return ()
    with table_path.open(encoding="utf-8") as handle:
        next(handle, None)
        for line in handle:
            parts = line.split()
            if len(parts) < 10 or parts[3] != LISTEN_STATE:
                continue
            try:
                address_hex, port_hex = parts[1].split(":")
                listener = decode_ipv4(address_hex) if family == socket.AF_INET else decode_ipv6(address_hex)
                listeners.append((listener, int(port_hex, 16), parts[9]))
            except (OSError, ValueError):
                continue
    return tuple(listeners)


def listener_conflicts(target_host: str, listener_host: str) -> bool:
    normalized_target = normalize_host(target_host)
    if ":" in normalized_target:
        if normalized_target == "::":
            return True
        return listener_host in {normalized_target, "::"}
    if normalized_target == "0.0.0.0":
        return True
    return listener_host in {normalized_target, "0.0.0.0"}


def find_port_owners(host: str, port: int, *, proc_root: str = "/proc") -> tuple[PortOwner, ...]:
    proc_path = Path(proc_root)
    family = socket_family_for_host(host)
    tables = [("net/tcp", socket.AF_INET)] if family == socket.AF_INET else [("net/tcp6", socket.AF_INET6)]
    matches: dict[str, str] = {}
    for relative_path, table_family in tables:
        for listener_host, listener_port, inode in iter_listeners(proc_path / relative_path, table_family):
            if listener_port != port:
                continue
            if listener_conflicts(host, listener_host):
                matches[inode] = f"{listener_host}:{listener_port}"
    if not matches:
        return ()

    owners: list[PortOwner] = []
    for entry in sorted(proc_path.iterdir(), key=lambda path: path.name):
        if not entry.name.isdigit():
            continue
        fd_dir = entry / "fd"
        if not fd_dir.exists():
            continue
        listener = None
        try:
            fds = sorted(fd_dir.iterdir(), key=lambda path: path.name)
        except OSError:
            continue
        for fd_path in fds:
            try:
                link_target = os.readlink(fd_path)
            except OSError:
                continue
            if not link_target.startswith("socket:["):
                continue
            inode = link_target[8:-1]
            listener = matches.get(inode)
            if listener:
                break
        if not listener:
            continue
        try:
            cwd = os.readlink(entry / "cwd")
        except OSError:
            cwd = ""
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode().strip()
        except OSError:
            cmdline = ""
        owners.append(PortOwner(pid=int(entry.name), listener=listener, cwd=cwd, cmdline=cmdline))
    return tuple(owners)


def probe_port(host: str, port: int) -> PortProbeResult:
    try:
        bind_host_port(host, port)
    except OSError as exc:
        if exc.errno == errno.EADDRINUSE:
            return PortProbeResult(
                status="in_use",
                host=host,
                port=port,
                owners=find_port_owners(host, port),
                error_errno=exc.errno,
                error_message=str(exc),
            )
        return PortProbeResult(
            status="error",
            host=host,
            port=port,
            error_errno=exc.errno,
            error_message=str(exc),
        )
    return PortProbeResult(status="available", host=host, port=port)


def format_probe_result(result: PortProbeResult) -> str:
    if result.status == "available":
        return f"port {result.host}:{result.port} is available"
    if result.status == "error":
        if result.error_errno is not None:
            return f"port probe failed for {result.host}:{result.port}: [Errno {result.error_errno}] {result.error_message}"
        return f"port probe failed for {result.host}:{result.port}: {result.error_message}"
    if not result.owners:
        return f"port {result.host}:{result.port} is already in use"
    lines = [f"port {result.host}:{result.port} is already in use"]
    for owner in result.owners:
        details = [f"pid={owner.pid}", f"listener={owner.listener}"]
        if owner.cwd:
            details.append(f"cwd={owner.cwd}")
        if owner.cmdline:
            details.append(f"cmd={owner.cmdline}")
        lines.append(" ".join(details))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe whether a TCP port can be bound.")
    parser.add_argument("--describe", action="store_true", help="print a human-readable result")
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    args = parser.parse_args(argv)

    result = probe_port(args.host, args.port)
    if args.describe or result.status != "available":
        print(format_probe_result(result))
    if result.status == "available":
        return 0
    if result.status == "in_use":
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
