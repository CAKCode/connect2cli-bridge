import asyncio
import base64
import json
from itertools import count

import pytest
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from workspace_bridge.inbound_media import decrypt_media_buffer
from workspace_bridge.wecom_limits import UPLOAD_CHUNK_BYTES
from workspace_bridge.wecom_media import native_media_body, upload_media


async def test_interrupted_upload_resumes_only_unconfirmed_chunks_after_restart(tmp_path):
    path = tmp_path / "report.bin"
    path.write_bytes(b"x" * (UPLOAD_CHUNK_BYTES * 2 + 5))
    seen = []
    fail = True
    sequence = count()

    async def send(payload, _timeout):
        seen.append(payload)
        if payload["cmd"] == "aibot_upload_media_init":
            return {"errcode": 0, "body": {"upload_id": "upload-1"}}
        if payload["cmd"] == "aibot_upload_media_chunk":
            if fail and payload["body"]["chunk_index"] == 1:
                raise asyncio.TimeoutError("uncertain chunk ACK")
            return {"errcode": 0}
        return {"errcode": 0, "body": {"media_id": "media-1"}}

    arguments = dict(filename=path.name, media_type="file", owner="bot", destination="room", cache_root=tmp_path / "cache",
                     send=send, uid=lambda: str(next(sequence)), clock=lambda: 0)
    with pytest.raises(asyncio.TimeoutError):
        await upload_media(path, **arguments)
    state = json.loads(next((tmp_path / "cache").glob("*.json")).read_text())
    assert state["uploaded"] == [0, 2]
    seen.clear()
    fail = False
    result = await upload_media(path, **arguments)
    assert result == "media-1"
    assert not any(payload["cmd"] == "aibot_upload_media_init" for payload in seen)
    assert [payload["body"]["chunk_index"] for payload in seen if payload["cmd"] == "aibot_upload_media_chunk"] == [1]


async def test_expired_upload_session_is_replaced(tmp_path):
    path = tmp_path / "report.txt"
    path.write_bytes(b"hello")
    clock = 0
    attempts = 0
    sequence = count()
    seen = []

    async def send(payload, _timeout):
        nonlocal attempts
        seen.append(payload)
        if payload["cmd"] == "aibot_upload_media_init":
            attempts += 1
            return {"errcode": 0, "body": {"upload_id": f"upload-{attempts}"}}
        if payload["cmd"] == "aibot_upload_media_chunk":
            if attempts == 1:
                raise ConnectionError("disconnected")
            return {"errcode": 0}
        return {"errcode": 0, "body": {"media_id": "media-2"}}

    arguments = dict(filename=path.name, media_type="file", owner="bot", destination="room", cache_root=tmp_path / "cache",
                     send=send, uid=lambda: str(next(sequence)), clock=lambda: clock)
    with pytest.raises(ConnectionError):
        await upload_media(path, **arguments)
    clock = 1801
    seen.clear()
    assert await upload_media(path, **arguments) == "media-2"
    assert seen[0]["cmd"] == "aibot_upload_media_init"
    assert all(payload["body"]["upload_id"] == "upload-2" for payload in seen if "upload_id" in payload["body"])


async def test_oversized_file_is_rejected_without_network_or_full_file_read(tmp_path):
    path = tmp_path / "large.bin"
    with path.open("wb") as stream:
        stream.truncate(20 * 1024 * 1024 + 1)

    async def send(*_args):
        pytest.fail("invalid media must not reach the network")

    with pytest.raises(ValueError, match="too large"):
        await upload_media(path, filename=path.name, media_type="file", owner="bot", destination="room",
                           cache_root=tmp_path / "cache", send=send, uid=lambda: "req")
    assert not (tmp_path / "cache").exists()


def test_video_labels_keep_valid_utf8_within_official_byte_limits():
    body = native_media_body("video", "media", title="录像😀" * 30, description="描述😀" * 200)
    assert len(body["video"]["title"].encode()) <= 64
    assert len(body["video"]["description"].encode()) <= 512


def test_callback_media_decrypts_aes256_and_32_byte_pkcs7_padding():
    key = b"0123456789abcdef0123456789abcdef"
    original = b"actual image or video contents" * 3
    encrypted = AES.new(key, AES.MODE_CBC, iv=key[:16]).encrypt(pad(original, 32))
    encoded_key = base64.b64encode(key).decode().rstrip("=")
    assert decrypt_media_buffer(encrypted, encoded_key) == original
    with pytest.raises(ValueError, match="size"):
        decrypt_media_buffer(encrypted[:-1], encoded_key)
