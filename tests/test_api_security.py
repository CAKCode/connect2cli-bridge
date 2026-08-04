from __future__ import annotations

import base64

import pytest

from workspace_bridge.api_security import authorize_api_access, is_loopback_address


@pytest.mark.parametrize(
    "address",
    ["127.0.0.1", "127.10.20.30", "::1", "::ffff:127.0.0.1", "localhost", "LOCALHOST"],
)
def test_is_loopback_address_accepts_local_addresses(address: str) -> None:
    assert is_loopback_address(address) is True


@pytest.mark.parametrize("address", [None, "", "0.0.0.0", "192.0.2.10", "example.test"])
def test_is_loopback_address_rejects_non_local_addresses(address: str | None) -> None:
    assert is_loopback_address(address) is False


def test_authorize_api_access_accepts_case_insensitive_bearer_scheme() -> None:
    decision = authorize_api_access(
        {"Authorization": "bearer expected-token"},
        "192.0.2.10",
        token="expected-token",
        basic_auth="",
    )

    assert decision.allowed is True


def test_authorize_api_access_accepts_case_insensitive_basic_scheme() -> None:
    credentials = "user:\u5bc6\u7801"
    encoded = base64.b64encode(credentials.encode("utf-8")).decode("ascii")

    decision = authorize_api_access(
        {"Authorization": f"basic {encoded}"},
        "192.0.2.10",
        token="",
        basic_auth=credentials,
    )

    assert decision.allowed is True


def test_authorize_api_access_rejects_invalid_credentials() -> None:
    decision = authorize_api_access(
        {"Authorization": "Bearer wrong-token"},
        "127.0.0.1",
        token="expected-token",
        basic_auth="",
    )

    assert decision.allowed is False
    assert decision.status == 401


def test_authorize_api_access_rejects_malformed_basic_header() -> None:
    decision = authorize_api_access(
        {"Authorization": "Basic not-base64"},
        "192.0.2.10",
        token="",
        basic_auth="user:password",
    )

    assert decision.allowed is False
    assert decision.status == 401


def test_authorize_api_access_without_credentials_allows_only_loopback() -> None:
    local = authorize_api_access({}, "127.20.30.40", token="", basic_auth="")
    remote = authorize_api_access({}, "192.0.2.10", token="", basic_auth="")

    assert local.allowed is True
    assert remote.allowed is False
    assert remote.status == 403
