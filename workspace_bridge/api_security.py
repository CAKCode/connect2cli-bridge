from __future__ import annotations

import base64
from collections.abc import Mapping
from dataclasses import dataclass
import hmac
import ipaddress


@dataclass(frozen=True)
class ApiAccessDecision:
    allowed: bool
    status: int = 200
    message: str = ""


def extract_bearer_token(headers: Mapping[str, str]) -> str:
    authorization = str(headers.get("Authorization") or "").strip()
    scheme, separator, credentials = authorization.partition(" ")
    if separator and scheme.casefold() == "bearer":
        return credentials.strip()
    return str(headers.get("X-Bridge-Token") or "").strip()


def extract_basic_credentials(headers: Mapping[str, str]) -> str:
    authorization = str(headers.get("Authorization") or "").strip()
    scheme, separator, encoded = authorization.partition(" ")
    if not separator or scheme.casefold() != "basic":
        return ""
    encoded = encoded.strip()
    if not encoded:
        return ""
    try:
        return base64.b64decode(encoded, validate=True).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return ""


def secure_text_equal(candidate: str, expected: str) -> bool:
    return hmac.compare_digest(
        str(candidate or "").encode("utf-8", errors="surrogatepass"),
        str(expected or "").encode("utf-8", errors="surrogatepass"),
    )


def api_auth_enabled(token: str, basic_auth: str) -> bool:
    return bool(str(token or "").strip() or str(basic_auth or "").strip())


def api_auth_mode_label(token: str, basic_auth: str) -> str:
    if token and basic_auth:
        return "token or basic auth required"
    if basic_auth:
        return "basic auth required"
    if token:
        return "token required"
    return "localhost only"


def is_loopback_address(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    if normalized == "localhost":
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return address.ipv4_mapped.is_loopback
    return address.is_loopback


def authorize_api_access(
    headers: Mapping[str, str],
    remote: str | None,
    *,
    token: str,
    basic_auth: str,
) -> ApiAccessDecision:
    if api_auth_enabled(token, basic_auth):
        if token and secure_text_equal(extract_bearer_token(headers), token):
            return ApiAccessDecision(allowed=True)
        if basic_auth and secure_text_equal(extract_basic_credentials(headers), basic_auth):
            return ApiAccessDecision(allowed=True)
        return ApiAccessDecision(
            allowed=False,
            status=401,
            message=f"API auth required ({api_auth_mode_label(token, basic_auth)})",
        )
    if is_loopback_address(remote):
        return ApiAccessDecision(allowed=True)
    return ApiAccessDecision(
        allowed=False,
        status=403,
        message="API is limited to localhost unless BRIDGE_TOKEN or BRIDGE_BASIC_AUTH is configured",
    )
