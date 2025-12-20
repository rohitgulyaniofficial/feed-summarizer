from config import config
from workers.fetcher.proxy import resolve_proxy_url


def test_proxy_true_uses_global_proxy(monkeypatch):
    monkeypatch.setattr(config, "PROXY_URL", "http://proxy.example:3128")

    proxy_url = resolve_proxy_url("example", {"proxy": True}, set(), set())

    assert proxy_url == "http://proxy.example:3128"


def test_proxy_string_overrides_global(monkeypatch):
    monkeypatch.setattr(config, "PROXY_URL", "http://global:8080")

    proxy_url = resolve_proxy_url("example", {"proxy": "http://custom:9000"}, set(), set())

    assert proxy_url == "http://custom:9000"


def test_proxy_true_without_global_returns_none(monkeypatch):
    monkeypatch.setattr(config, "PROXY_URL", None)

    proxy_url = resolve_proxy_url("missing-global", {"proxy": True}, set(), set())

    assert proxy_url is None
