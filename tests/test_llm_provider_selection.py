import pytest

from config import config
from services.llm_client import chat_completion, validate_llm_configuration


class _CaptureClient:
    def __init__(self):
        self.last_kwargs = None

        outer = self

        class _Completions:
            async def create(self, **kwargs):
                outer.last_kwargs = kwargs

                class _Message:
                    content = "ok"

                class _Choice:
                    message = _Message()
                    finish_reason = "stop"

                class _Resp:
                    choices = [_Choice()]

                return _Resp()

        class _Chat:
            completions = _Completions()

        self.chat = _Chat()


def test_validate_llm_configuration_github_models_missing_fields(monkeypatch):
    monkeypatch.setattr(config, "LLM_PROVIDER", "github_models", raising=False)
    monkeypatch.setattr(config, "LLM_API_KEY", "", raising=False)
    monkeypatch.setattr(config, "LLM_MODEL", "", raising=False)

    errors = validate_llm_configuration()
    assert any("LLM_API_KEY" in e for e in errors)
    assert any("LLM_MODEL" in e for e in errors)


@pytest.mark.asyncio
async def test_chat_completion_uses_model_for_github_models(monkeypatch):
    monkeypatch.setattr(config, "LLM_PROVIDER", "github_models", raising=False)
    monkeypatch.setattr(config, "LLM_API_KEY", "github_pat_xxxxxxxxxxxxxxxxxxxx", raising=False)
    monkeypatch.setattr(config, "LLM_MODEL", "gpt-4.1-mini", raising=False)

    fake = _CaptureClient()
    result = await chat_completion(
        messages=[{"role": "user", "content": "test"}],
        purpose="provider_selection",
        retries=0,
        client_override=fake,
    )

    assert result == "ok"
    assert fake.last_kwargs is not None
    assert fake.last_kwargs.get("model") == "gpt-4.1-mini"


@pytest.mark.asyncio
async def test_chat_completion_uses_deployment_for_azure(monkeypatch):
    monkeypatch.setattr(config, "LLM_PROVIDER", "azure", raising=False)
    monkeypatch.setattr(config, "DEPLOYMENT_NAME", "gpt-5-mini", raising=False)

    fake = _CaptureClient()
    result = await chat_completion(
        messages=[{"role": "user", "content": "test"}],
        purpose="provider_selection",
        retries=0,
        client_override=fake,
    )

    assert result == "ok"
    assert fake.last_kwargs is not None
    assert fake.last_kwargs.get("model") == "gpt-5-mini"
