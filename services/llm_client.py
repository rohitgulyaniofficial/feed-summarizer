#!/usr/bin/env python3
"""Async LLM helper providing `chat_completion` with retries and provider routing.

Supported providers:
- azure (default, backward-compatible)
- github_models (OpenAI-compatible endpoint + PAT)
"""

from __future__ import annotations

from asyncio import sleep
from typing import Any, Callable, Dict, List, Optional

from config import config, get_logger


def _is_token_limit_error(error_obj: Any, exc: Exception) -> bool:
    """Return True if the exception represents a context length/token limit error."""
    try:
        if isinstance(error_obj, dict):
            code = error_obj.get("code")
            inner = (
                (error_obj.get("innererror") or {}).get("code")
                if isinstance(error_obj.get("innererror"), dict)
                else None
            )
            message = (error_obj.get("message") or "").lower()
            if code == "context_length_exceeded" or inner == "context_length_exceeded":
                return True
            if "context length" in message or "tokens" in message:
                return True

        msg = str(exc).lower()
        if "context_length_exceeded" in msg or "input tokens exceed" in msg or "context length" in msg:
            return True
    except Exception:
        return False
    return False


class ContentFilterError(Exception):
    """Raised when provider content filtering blocks a response."""

    def __init__(self, message: str = "Content filtered by LLM provider", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.details = details or {}


class TokenLimitError(Exception):
    """Raised when input tokens exceed the model context window."""

    def __init__(self, message: str = "Token limit exceeded", details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.details = details or {}


try:  # Guarded import (dependency declared in requirements.txt)
    from openai import AsyncAzureOpenAI, AsyncOpenAI, OpenAIError
except ImportError:  # pragma: no cover
    AsyncAzureOpenAI = None  # type: ignore
    AsyncOpenAI = None  # type: ignore
    OpenAIError = Exception  # type: ignore


logger = get_logger("llm_client")

_clients: Dict[str, Any] = {}


def _normalized_provider() -> str:
    provider = str(getattr(config, "LLM_PROVIDER", "azure") or "azure").strip().lower()
    if provider not in {"azure", "github_models"}:
        return "azure"
    return provider


def _get_effective_llm_model(provider: Optional[str] = None) -> Optional[str]:
    p = provider or _normalized_provider()
    if p == "azure":
        return (getattr(config, "DEPLOYMENT_NAME", None) or "").strip() or None
    return (getattr(config, "LLM_MODEL", None) or "").strip() or None


def _get_effective_llm_api_key(provider: Optional[str] = None) -> Optional[str]:
    p = provider or _normalized_provider()
    if p == "azure":
        return (getattr(config, "OPENAI_API_KEY", None) or "").strip() or None
    generic = (getattr(config, "LLM_API_KEY", None) or "").strip() or None
    if generic:
        return generic
    return None


def _get_effective_llm_base_url(provider: Optional[str] = None) -> Optional[str]:
    p = provider or _normalized_provider()
    if p != "github_models":
        return None
    custom = (getattr(config, "LLM_BASE_URL", None) or "").strip()
    if custom:
        return custom.rstrip("/")
    return "https://models.inference.ai.azure.com"


def validate_llm_configuration(provider: Optional[str] = None) -> List[str]:
    """Validate active provider config. Returns a list of human-readable errors."""
    p = provider or _normalized_provider()
    errors: List[str] = []

    if p == "azure":
        endpoint = (getattr(config, "AZURE_ENDPOINT", None) or "").strip()
        key = (getattr(config, "OPENAI_API_KEY", None) or "").strip()
        deployment = (getattr(config, "DEPLOYMENT_NAME", None) or "").strip()
        api_version = (getattr(config, "OPENAI_API_VERSION", None) or "").strip()

        if not endpoint:
            errors.append("AZURE_ENDPOINT environment variable not set")
        if not key:
            errors.append("OPENAI_API_KEY environment variable not set")
        elif len(key) < 20:
            errors.append("OPENAI_API_KEY appears to be invalid (too short)")
        if not deployment:
            errors.append("DEPLOYMENT_NAME environment variable not set")
        if not api_version:
            errors.append("OPENAI_API_VERSION environment variable not set")
        return errors

    if p == "github_models":
        key = _get_effective_llm_api_key("github_models")
        model = _get_effective_llm_model("github_models")
        if not key:
            errors.append("LLM_API_KEY environment variable not set for github_models provider")
        elif len(key) < 20:
            errors.append("LLM_API_KEY appears to be invalid (too short)")
        if not model:
            errors.append("LLM_MODEL environment variable not set for github_models provider")
        return errors

    errors.append(f"Unsupported LLM_PROVIDER '{p}'")
    return errors


def is_llm_enabled(provider: Optional[str] = None) -> bool:
    return len(validate_llm_configuration(provider)) == 0


def _cache_key_for_provider(provider: str) -> str:
    if provider == "azure":
        endpoint = str(getattr(config, "AZURE_ENDPOINT", "") or "")
        version = str(getattr(config, "OPENAI_API_VERSION", "") or "")
        deployment = str(getattr(config, "DEPLOYMENT_NAME", "") or "")
        return f"azure::{endpoint}::{version}::{deployment}"
    base_url = _get_effective_llm_base_url(provider) or ""
    model = _get_effective_llm_model(provider) or ""
    return f"github_models::{base_url}::{model}"


def _get_client(provider: Optional[str] = None) -> Optional[Any]:
    """Instantiate and cache provider client when configuration is valid."""
    p = provider or _normalized_provider()
    if not is_llm_enabled(p):
        logger.debug("Missing %s LLM config; client will not initialize", p)
        return None

    cache_key = _cache_key_for_provider(p)
    if cache_key in _clients:
        return _clients[cache_key]

    if p == "azure":
        if not AsyncAzureOpenAI:
            return None
        endpoint = str(getattr(config, "AZURE_ENDPOINT", "") or "")
        endpoint = f"https://{endpoint}" if endpoint and not endpoint.startswith("http") else endpoint
        _clients[cache_key] = AsyncAzureOpenAI(
            api_key=_get_effective_llm_api_key("azure"),
            api_version=getattr(config, "OPENAI_API_VERSION", None),
            azure_endpoint=endpoint,
        )
        return _clients[cache_key]

    if p == "github_models":
        if not AsyncOpenAI:
            return None
        _clients[cache_key] = AsyncOpenAI(
            api_key=_get_effective_llm_api_key("github_models"),
            base_url=_get_effective_llm_base_url("github_models"),
        )
        return _clients[cache_key]

    return None


def _extract_model_name(provider: str) -> Optional[str]:
    return _get_effective_llm_model(provider)


async def chat_completion(
    messages: Optional[List[Dict[str, str]]] = None,
    *,
    purpose: str = "generic",
    retries: Optional[int] = None,
    postprocess: Optional[Callable[[str], str]] = None,
    client_override: Optional[Any] = None,
) -> Optional[str]:
    """Execute a provider chat completion. Raises `ContentFilterError` on policy violations."""
    if messages is None:
        logger.error("chat_completion called without messages list")
        return None

    provider = _normalized_provider()
    client = client_override or _get_client(provider)
    if client is None:
        logger.warning("LLM client unavailable for provider=%s; skipping %s", provider, purpose)
        return None

    remaining = retries if retries is not None else config.SUMMARIZER_MAX_RETRIES
    attempt = 0
    model_name = _extract_model_name(provider)
    if not model_name:
        if client_override is not None:
            model_name = "test-model"
        else:
            logger.warning("Model name missing for provider=%s; skipping %s", provider, purpose)
            return None

    while attempt <= remaining:
        params: Dict[str, Any] = {
            "model": model_name,
            "messages": messages,
        }
        try:
            resp = await client.chat.completions.create(**params)
            choices = getattr(resp, "choices", None) or []
            if not choices:
                logger.error("No choices in %s response: %s", purpose, resp)
                return None

            def _extract_text(choice: Any) -> str:
                message = getattr(choice, "message", {}) or {}
                if isinstance(message, dict) and message.get("refusal"):
                    return ""
                content = getattr(message, "content", None) if not isinstance(message, dict) else message.get("content")
                if isinstance(content, str):
                    return content.strip()
                if isinstance(content, list):
                    texts: List[str] = []
                    for part in content:
                        if isinstance(part, dict):
                            ptype = part.get("type")
                            txt = part.get("text")
                            if isinstance(txt, str) and txt.strip():
                                texts.append(txt.strip())
                            elif ptype not in ("text", "output_text", None):
                                logger.debug("Ignoring non-text part type=%s keys=%s", ptype, list(part.keys()))
                    return "\n".join(texts).strip()
                return ""

            fragments: List[str] = []
            refusal_detected = False
            for ch in choices:
                msg_obj = getattr(ch, "message", {}) or {}
                refusal_flag = msg_obj.get("refusal") if isinstance(msg_obj, dict) else getattr(msg_obj, "refusal", None)
                if refusal_flag:
                    refusal_detected = True
                    logger.warning("Refusal detected in %s response: %s", purpose, refusal_flag)
                txt = _extract_text(ch)
                if txt:
                    fragments.append(txt)
            if refusal_detected and not fragments:
                logger.warning("All choices refused for %s; returning None", purpose)
                return None

            raw = "\n".join(fragments).strip()
            if not raw:
                finish_reasons = {
                    getattr(c, "finish_reason", None) for c in choices if getattr(c, "finish_reason", None)
                }
                if "length" in finish_reasons:
                    try:
                        logger.warning(
                            "Truncated output with empty content (%s); placeholder will be returned. choices=%s",
                            purpose,
                            [
                                {
                                    "finish_reason": getattr(c, "finish_reason", None),
                                    "has_message": bool(getattr(c, "message", None)),
                                    "message_content_type": type(
                                        getattr(getattr(c, "message", None), "content", None)
                                    ).__name__,
                                    "message_content_repr": repr(
                                        getattr(getattr(c, "message", None), "content", None)
                                    )[:300],
                                }
                                for c in choices
                            ],
                        )
                    except Exception:
                        logger.warning("Truncated output with empty content (%s); returning placeholder", purpose)
                    raw = "[Truncated output: no content returned]"
                else:
                    logger.error(
                        "Empty content in %s response despite choices (finish_reasons=%s)",
                        purpose,
                        finish_reasons,
                    )
                    return None
            return postprocess(raw) if postprocess else raw
        except OpenAIError as e:
            err_body = getattr(e, "body", {}) or {}
            error_obj = err_body.get("error") if isinstance(err_body, dict) else None
            code = (error_obj or {}).get("code") if isinstance(error_obj, dict) else None
            inner_code = (error_obj or {}).get("innererror", {}).get("code") if isinstance(error_obj, dict) else None

            if code == "content_filter" or inner_code == "ResponsibleAIPolicyViolation":
                raise ContentFilterError(message=(error_obj or {}).get("message", "Content filtered"), details=error_obj or {})
            if _is_token_limit_error(error_obj, e):
                logger.info("Detected token limit error - raising TokenLimitError to trigger batch splitting")
                raise TokenLimitError(message=(error_obj or {}).get("message", "Token limit exceeded"), details=error_obj or {})

            attempt += 1
            if attempt > remaining:
                logger.error("%s request failed after %d retries: %s", purpose, remaining, e)
                return None
            delay = config.SUMMARIZER_RETRY_DELAY_BASE * (2 ** (attempt - 1))
            logger.warning(
                "%s transient LLM error (provider=%s): %s. Backoff %ss (attempt %d/%d)",
                purpose,
                provider,
                e,
                delay,
                attempt,
                remaining,
            )
            await sleep(delay)
        except Exception as e:
            body = getattr(e, "body", {}) or {}
            error_obj = body.get("error") if isinstance(body, dict) else None
            if isinstance(error_obj, dict):
                code = error_obj.get("code")
                inner_code = (
                    (error_obj.get("innererror") or {}).get("code")
                    if isinstance(error_obj.get("innererror"), dict)
                    else None
                )

                if code == "content_filter" or inner_code == "ResponsibleAIPolicyViolation":
                    raise ContentFilterError(message=error_obj.get("message", "Content filtered"), details=error_obj)
                if _is_token_limit_error(error_obj, e):
                    logger.info("Detected token limit error in generic handler - raising TokenLimitError")
                    raise TokenLimitError(message=error_obj.get("message", "Token limit exceeded"), details=error_obj)

            attempt += 1
            if attempt > remaining:
                logger.error("%s unexpected failure after %d retries: %s", purpose, remaining, e)
                return None
            delay = config.SUMMARIZER_RETRY_DELAY_BASE * (2 ** (attempt - 1))
            logger.warning(
                "%s unexpected error (provider=%s): %s. Backoff %ss (attempt %d/%d)",
                purpose,
                provider,
                e,
                delay,
                attempt,
                remaining,
            )
            await sleep(delay)

    return None


__all__ = [
    "chat_completion",
    "ContentFilterError",
    "TokenLimitError",
    "is_llm_enabled",
    "validate_llm_configuration",
]
