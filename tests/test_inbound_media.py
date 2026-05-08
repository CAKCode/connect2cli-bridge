from pathlib import Path

from workspace_bridge.inbound_media import (
    build_nonconflicting_file_path,
    decode_aes_key,
    decrypt_media_buffer,
    extract_mixed_images,
    extract_mixed_text,
    sanitize_file_name,
    should_fallback_to_curl,
)


def test_sanitize_file_name_replaces_unsafe_characters() -> None:
    assert sanitize_file_name("report 1?.txt") == "report_1_.txt"


def test_build_nonconflicting_file_path_keeps_both_files(tmp_path: Path) -> None:
    first = tmp_path / "report.txt"
    first.write_text("a", encoding="utf-8")

    second = build_nonconflicting_file_path(tmp_path, "report.txt")

    assert second.name == "report-1.txt"


def test_extract_mixed_text_and_images() -> None:
    mixed = {
        "msg_item": [
            {"msgtype": "text", "text": {"content": "hello"}},
            {"msgtype": "image", "image": {"url": "https://example.test/image.png"}},
        ]
    }

    assert extract_mixed_text(mixed) == "hello"
    assert extract_mixed_images(mixed) == [{"url": "https://example.test/image.png"}]


def test_decode_aes_key_accepts_32_byte_text() -> None:
    assert decode_aes_key("12345678901234567890123456789012") == b"12345678901234567890123456789012"


def test_should_fallback_to_curl_matches_network_errors() -> None:
    assert should_fallback_to_curl(RuntimeError("connection reset by peer")) is True
    assert should_fallback_to_curl(RuntimeError("permission denied")) is False


def test_decrypt_media_buffer_rejects_invalid_key() -> None:
    try:
        decrypt_media_buffer(b"abc", "short")
    except ValueError as exc:
        assert "invalid aes key" in str(exc)
    else:
        raise AssertionError("expected ValueError")
