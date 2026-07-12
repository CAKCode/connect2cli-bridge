from __future__ import annotations

import errno
import ipaddress
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


def _normalize_bind_host(host: str) -> str:
    text = str(host or "").strip()
    if text.startswith("[") and text.endswith("]"):
        return text[1:-1]
    return text


def bind_host_port(host: str, port: int) -> None:
    normalized_host = _normalize_bind_host(host)
    addrinfos = socket.getaddrinfo(normalized_host, port, type=socket.SOCK_STREAM)
    last_error: OSError | None = None
    for family, socktype, proto, _canonname, sockaddr in addrinfos:
        sock = socket.socket(family, socktype, proto)
        try:
            sock.bind(sockaddr)
            return
        except OSError as exc:
            last_error = exc
        finally:
            sock.close()
    if last_error is not None:
        raise last_error


def _parse_proc_net_tcp(path: Path) -> dict[str, str]:
    listeners: dict[str, str] = {}
    if not path.exists():
        return listeners
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return listeners
    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 10:
            continue
        local_address = parts[1]
        state = parts[3]
        if state != "0A":
            continue
        inode = parts[9]
        listeners[inode] = local_address
    return listeners


def _decode_listener(local_address: str) -> str:
    host_hex, port_hex = local_address.split(":")
    port = int(port_hex, 16)
    if len(host_hex) == 8:
        if host_hex == "00000000":
            host = "0.0.0.0"
        else:
            host = socket.inet_ntoa(bytes.fromhex(host_hex)[::-1])
        return f"{host}:{port}"
    if len(host_hex) == 32:
        if host_hex == "0" * 32:
            host = "::"
        else:
            raw = bytes.fromhex(host_hex)
            words = [raw[index : index + 4][::-1] for index in range(0, len(raw), 4)]
            host = str(ipaddress.IPv6Address(b"".join(words)))
        return f"[{host}]:{port}"
    return f"{host_hex}:{port}"


def _listener_matches_host(probe_host: str, listener_host: str) -> bool:
    normalized_probe = _normalize_bind_host(probe_host)
    normalized_listener = listener_host[1:-1] if listener_host.startswith("[") and listener_host.endswith("]") else listener_host
    try:
        probe_ip = ipaddress.ip_address(normalized_probe)
        listener_ip = ipaddress.ip_address(normalized_listener)
    except ValueError:
        resolved_probe_ips: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
        try:
            addrinfos = socket.getaddrinfo(normalized_probe, None, type=socket.SOCK_STREAM)
        except OSError:
            return normalized_probe == normalized_listener
        for _family, _socktype, _proto, _canonname, sockaddr in addrinfos:
            try:
                resolved_probe_ips.append(ipaddress.ip_address(sockaddr[0]))
            except ValueError:
                continue
        try:
            listener_ip = ipaddress.ip_address(normalized_listener)
        except ValueError:
            return normalized_probe == normalized_listener
        return any(
            listener_ip.version == resolved_probe_ip.version
            and (
                listener_ip == resolved_probe_ip
                or (listener_ip.version == 4 and str(listener_ip) == "0.0.0.0")
                or (listener_ip.version == 6 and str(listener_ip) == "::")
            )
            for resolved_probe_ip in resolved_probe_ips
        )
    if probe_ip.version != listener_ip.version:
        return False
    if probe_ip.version == 4:
        if normalized_probe == "0.0.0.0":
            return True
        return normalized_listener in {normalized_probe, "0.0.0.0"}
    if normalized_probe == "::":
        return True
    return normalized_listener in {normalized_probe, "::"}


def find_port_owners(host: str, port: int, *, proc_root: str = "/proc") -> list[PortOwner]:
    proc_root_path = Path(proc_root)
    listeners = {
        **_parse_proc_net_tcp(proc_root_path / "net" / "tcp"),
        **_parse_proc_net_tcp(proc_root_path / "net" / "tcp6"),
    }
    owners: list[PortOwner] = []
    seen: set[tuple[str, str]] = set()
    try:
        proc_entries = list(proc_root_path.iterdir())
    except OSError:
        return owners
    for entry in proc_entries:
        if not entry.name.isdigit():
            continue
        fd_dir = entry / "fd"
        if not fd_dir.exists():
            continue
        try:
            fd_entries = list(fd_dir.iterdir())
        except OSError:
            continue
        for fd in fd_entries:
            try:
                target = os.readlink(fd)
            except OSError:
                continue
            if not target.startswith("socket:["):
                continue
            inode = target[8:-1]
            owner_key = (entry.name, inode)
            if owner_key in seen:
                continue
            local_address = listeners.get(inode)
            if not local_address:
                continue
            listener = _decode_listener(local_address)
            listener_host, listener_port = listener.rsplit(":", 1)
            if int(listener_port) != port:
                continue
            if not _listener_matches_host(host, listener_host):
                continue
            try:
                cwd = os.readlink(entry / "cwd") if (entry / "cwd").exists() else ""
            except OSError:
                cwd = ""
            try:
                cmdline = (
                    (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "ignore").strip()
                    if (entry / "cmdline").exists()
                    else ""
                )
            except OSError:
                cmdline = ""
            seen.add(owner_key)
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
    if len(argv) != 3 or argv[0] != "--describe":
        print("usage: --describe <host> <port>")
        return 2
    host = argv[1]
    try:
        port = int(argv[2])
    except ValueError:
        print(f"invalid port: {argv[2]}")
        return 2
    if not 1 <= port <= 65535:
        print(f"invalid port: {argv[2]}")
        return 2
    result = probe_port(host, port)
    if result.status == "in_use":
        print(f"port {host}:{port} is already in use")
        for owner in result.owners:
            print(f"pid={owner.pid} listener={owner.listener} cwd={owner.cwd} cmdline={owner.cmdline}")
        return 1
    if result.status == "error":
        suffix = f" errno={result.error_errno}" if result.error_errno is not None else ""
        print(f"port {host}:{port} probe failed{suffix}")
        return 2
    return 0
