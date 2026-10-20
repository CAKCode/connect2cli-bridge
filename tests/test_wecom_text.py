import pytest

from workspace_bridge.wecom_text import split_markdown_chunks, split_text_chunks


def test_chunking_preserves_paragraphs_and_code_indentation() -> None:
    content = "概要\n\n  第一行\n    第二行\n\n结论  保留空格"
    chunks = split_text_chunks(content, max_chars=12)

    assert "".join(chunks) == content
    assert all(len(chunk) <= 12 for chunk in chunks)


@pytest.mark.parametrize("fence", ["```", "````", "~~~"])
def test_each_split_code_block_renders_all_original_code(fence: str) -> None:
    code = "    print('结果😀')\n" * 50
    chunks = split_markdown_chunks(f"{fence}python\n{code}{fence}", max_chars=80, max_bytes=128)
    rendered_code = []
    for chunk in chunks:
        lines = chunk.splitlines(keepends=True)
        assert lines[0].strip() == f"{fence}python"
        assert lines[-1].strip() == fence
        assert len(chunk) <= 80
        assert len(chunk.encode("utf-8")) <= 128
        rendered_code.extend(lines[1:-1])

    assert "".join(rendered_code) == code


def test_chinese_and_emoji_chunks_stay_inside_official_byte_limit() -> None:
    content = "中文😀" * 7000
    chunks = split_markdown_chunks(content, max_chars=50000)

    assert "".join(chunks) == content
    assert all(len(chunk.encode("utf-8")) <= 20480 for chunk in chunks)


def test_short_markdown_is_sent_without_rewriting_supported_syntax() -> None:
    content = "**测试完成**\n\n- 通过：3\n- 报告：[查看](https://example.com/report)"

    assert split_markdown_chunks(content, max_chars=3500) == [content]
