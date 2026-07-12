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


def test_sanitize_file_name_falls_back_for_empty_input() -> None:
    assert sanitize_file_name("") == "attachment.bin"


def test_sanitize_file_name_rejects_dot_and_dotdot() -> None:
    assert sanitize_file_name(".") == "attachment.bin"
    assert sanitize_file_name("..") == "attachment.bin"


def test_build_nonconflicting_file_path_keeps_both_files(tmp_path: Path) -> None:
    first = tmp_path / "report.txt"
    first.write_text("a", encoding="utf-8")

    second = build_nonconflicting_file_path(tmp_path, "report.txt")

    assert second.name == "report-1.txt"


def test_build_nonconflicting_file_path_uses_attachment_stem_for_empty_name(tmp_path: Path) -> None:
    placeholder = tmp_path / "attachment.bin"
    placeholder.write_text("a", encoding="utf-8")

    second = build_nonconflicting_file_path(tmp_path, "attachment.bin")

    assert second.name == "attachment-1.bin"


def test_build_nonconflicting_file_path_skips_broken_symlink_name(tmp_path: Path) -> None:
    (tmp_path / "report.txt").symlink_to(tmp_path / "missing-report.txt")

    second = build_nonconflicting_file_path(tmp_path, "report.txt")

    assert second.name == "report-1.txt"


def test_build_nonconflicting_file_path_skips_valid_symlink_name(tmp_path: Path) -> None:
    external = tmp_path / "external-report.txt"
    external.write_text("a", encoding="utf-8")
    (tmp_path / "report.txt").symlink_to(external)

    second = build_nonconflicting_file_path(tmp_path, "report.txt")

    assert second.name == "report-1.txt"


def test_build_nonconflicting_file_path_strips_path_segments(tmp_path: Path) -> None:
    candidate = build_nonconflicting_file_path(tmp_path, "../escape.txt")

    assert candidate == tmp_path / "escape.txt"


def test_extract_mixed_text_and_images() -> None:
    mixed = {
        "msg_item": [
            {"msgtype": "text", "text": {"content": "hello"}},
            {"msgtype": "image", "image": {"url": "https://example.test/image.png"}},
        ]
    }

    assert extract_mixed_text(mixed) == "hello"
    assert extract_mixed_images(mixed) == [{"url": "https://example.test/image.png"}]


def test_extract_mixed_text_and_images_ignore_malformed_items() -> None:
    mixed = {
        "msg_item": [
            None,
            {"msgtype": "text", "text": {"content": "hello"}},
            {"msgtype": "image", "image": "bad"},
            {"msgtype": "image", "image": {"url": "https://example.test/image.png"}},
        ]
    }

    assert extract_mixed_text(mixed) == "hello"
    assert extract_mixed_images(mixed) == [{"url": "https://example.test/image.png"}]


def test_extract_mixed_images_accepts_download_url_variants() -> None:
    mixed = {
        "msg_item": [
            {"msgtype": "image", "image": {"download_url": "https://example.test/a.png"}},
            {"msgtype": "image", "image": {"downloadUrl": "https://example.test/b.png"}},
        ]
    }

    assert extract_mixed_images(mixed) == [
        {"download_url": "https://example.test/a.png"},
        {"downloadUrl": "https://example.test/b.png"},
    ]


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
