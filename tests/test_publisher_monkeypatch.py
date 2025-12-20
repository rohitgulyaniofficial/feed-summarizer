import pytest
import workers.publisher as pkg

from workers.publisher import core as publisher_core


class Dummy:
    async def __call__(self, *args, **kwargs):  # pragma: no cover - trivial
        return "dummy"


def test_ai_chat_completion_monkeypatch(monkeypatch):
    dummy = Dummy()
    # set attribute on module to be picked by _get_ai_chat_completion

    monkeypatch.setattr(pkg, "ai_chat_completion", dummy, raising=False)

    fn = publisher_core._get_ai_chat_completion()

    assert fn is dummy


def test_ai_chat_completion_fallback(monkeypatch):
    monkeypatch.delattr(pkg, "ai_chat_completion", raising=False)

    fn = publisher_core._get_ai_chat_completion()

    assert fn is publisher_core.default_ai_chat_completion
