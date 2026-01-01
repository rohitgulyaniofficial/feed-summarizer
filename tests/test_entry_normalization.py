
from workers.fetcher.content import normalize_entry_identity


def test_normalize_entry_identity_basic():
    title, url, guid = normalize_entry_identity(
        "  Example Title  ",
        "https://example.com/a/very/long/path" + "?" + "x" * 2050,
        "guid-value" * 20,
    )
    assert title == "Example Title"
    assert len(url) == 2048
    assert url.startswith("https://example.com/a/very/long/path?")
    assert len(guid) == 64


def test_normalize_entry_identity_defaults():
    title, url, guid = normalize_entry_identity(None, None, None)
    assert title == "No Title"
    assert url == ""
    assert guid == ""


def test_normalize_entry_identity_blank_title():
    title, url, guid = normalize_entry_identity("   ", " http://example.com ", " abc ")
    assert title == "No Title"
    assert url == "http://example.com"
    assert guid == "abc"
