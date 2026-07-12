from workspace_bridge.messaging import get_messaging_provider
from workspace_bridge.wecom_messaging import WeComMessagingProvider


class Config:
    def __init__(self, platform):
        self.platform = platform


def test_get_messaging_provider_defaults_blank_platform_to_wecom() -> None:
    provider = get_messaging_provider(Config("   "))

    assert isinstance(provider, WeComMessagingProvider)


def test_get_messaging_provider_rejects_unknown_platform() -> None:
    try:
        get_messaging_provider(Config("other"))
    except ValueError as exc:
        assert str(exc) == "unsupported messaging platform: other"
    else:
        raise AssertionError("expected ValueError")
