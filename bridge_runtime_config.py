from __future__ import annotations

import argparse
import os
import sys
from typing import Mapping


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 9299


def parse_port(raw: str, *, source: str) -> int:
    try:
        port = int(raw)
    except ValueError as exc:
        raise ValueError(f"{source} must be an integer port, got {raw!r}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"{source} must be between 1 and 65535, got {port}")
    return port


def parse_bridge_bind(raw: str) -> tuple[str, int]:
    value = raw.strip()
    if not value:
        raise ValueError("BRIDGE_BIND cannot be empty")

    if value.startswith("["):
        end = value.find("]")
        if end == -1 or end + 1 >= len(value) or value[end + 1] != ":":
            raise ValueError("BRIDGE_BIND must use [ipv6-host]:port for IPv6 addresses")
        host = value[1:end].strip()
        port_text = value[end + 2 :].strip()
    else:
        host, separator, port_text = value.rpartition(":")
        if not separator or not host or not port_text:
            raise ValueError("BRIDGE_BIND must use host:port")
        if ":" in host:
            raise ValueError("BRIDGE_BIND must use [ipv6-host]:port for IPv6 addresses")
        host = host.strip()
        port_text = port_text.strip()

    if not host:
        raise ValueError("BRIDGE_BIND host cannot be empty")
    return host, parse_port(port_text, source="BRIDGE_BIND")


def resolve_host_port(env: Mapping[str, str] | None = None) -> tuple[str, int]:
    values = os.environ if env is None else env

    bind = str(values.get("BRIDGE_BIND") or "").strip()
    if bind:
        return parse_bridge_bind(bind)

    host = (
        str(values.get("BRIDGE_HOST") or "").strip()
        or str(values.get("HOST") or "").strip()
        or DEFAULT_HOST
    )
    port_raw = (
        str(values.get("BRIDGE_PORT") or "").strip()
        or str(values.get("PORT") or "").strip()
        or str(DEFAULT_PORT)
    )
    return host, parse_port(port_raw, source="BRIDGE_PORT/PORT")


def build_bridge_api_base(host: str, port: int) -> str:
    url_host = host
    if ":" in url_host and not url_host.startswith("["):
        url_host = f"[{url_host}]"
    return f"http://{url_host}:{port}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Resolve bridge host/port configuration.")
    parser.add_argument(
        "--print-host-port",
        action="store_true",
        help="Print resolved host and port on separate lines for shell scripts.",
    )
    args = parser.parse_args()

    try:
        host, port = resolve_host_port()
    except ValueError as exc:
        print(f"[CONFIG] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    if args.print_host_port:
        print(host)
        print(port)
        return

    print(build_bridge_api_base(host, port))


if __name__ == "__main__":
    main()
