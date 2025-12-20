from workers.publisher.rss_builder import (
    sanitize_xml_string,
    looks_like_html,
    looks_like_markdown,
    strip_markdown,
    markdown_to_html,
    create_raw_rss,
)


def test_sanitize_xml_strips_control_chars():
    assert sanitize_xml_string("\x01a\x7fb") == "ab"


def test_html_and_markdown_heuristics():
    assert looks_like_html("<p>hi</p>") is True
    assert looks_like_html("no tags here") is False
    assert looks_like_markdown("[link](http://example.com)") is True
    assert looks_like_markdown("") is False


def test_strip_and_markdown_to_html():
    text = "**bold** and [link](http://example.com)"
    assert strip_markdown(text) == "bold and link"
    html = markdown_to_html(text)
    assert "<strong>bold</strong>" in html


def test_create_raw_rss_generates_feed():
    items = [
        {"id": "1", "title": "Hello", "url": "http://example.com/1", "date": 1, "body": "**bold**"},
        {"id": "2", "title": "World", "url": "http://example.com/2", "date": 2, "body": "plain"},
    ]

    xml = create_raw_rss("https://base", "slug", "Feed Title", items)

    assert "Feed Title" in xml
    assert "http://example.com/1" in xml
    assert "<item>" in xml
