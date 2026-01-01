"""AI introduction and title helpers shared across publisher pipelines."""
from typing import Any, Dict, List, Optional, Tuple

from aiohttp import ClientSession

from config import config, get_logger

logger = get_logger("publisher.titles")


async def generate_intro_and_title(
    *,
    markdown_bulletin: str,
    summaries: List[Dict[str, Any]],
    group_name: str,
    session_key: str,
    enable_intro: bool,
    prompts: Dict[str, Any],
    generate_ai_introduction,
    generate_ai_title,
    ai_chat_completion_fn,
    generate_title_from_introduction,
    generate_title: bool = True,
) -> Tuple[Optional[str], Optional[str]]:
    """Generate AI introduction/title with shared fallbacks.

    Title generation can be disabled (e.g., RSS path). When enabled, retries an
    alternate condensed prompt if the primary title is empty, then heuristically
    falls back to concatenated item titles, then derives from introduction.
    """
    introduction: Optional[str] = None
    ai_title: Optional[str] = None

    if enable_intro and config.AZURE_ENDPOINT and config.OPENAI_API_KEY:
        try:
            introduction = await generate_ai_introduction(markdown_bulletin)
            if introduction:
                logger.info("Generated AI introduction (%d chars)", len(introduction))
            else:
                logger.warning("AI introduction generation returned empty string")
        except Exception as exc:
            logger.error("Error generating AI introduction: %s", exc)

    if generate_title and config.AZURE_ENDPOINT and config.OPENAI_API_KEY:
        try:
            ai_title = await generate_ai_title(markdown_bulletin)
            if ai_title:
                logger.info("Generated AI title: '%s'", ai_title[:120])
            else:
                logger.warning("Primary AI title attempt empty; attempting fallback")
                try:
                    condensed_titles = [
                        (s.get("item_title") or s.get("title", "")).strip()
                        for s in summaries
                        if (s.get("item_title") or s.get("title"))
                    ]
                    condensed = "\n".join(condensed_titles[:8])
                    if condensed:
                        alt_prompt = (
                            "Generate a concise bulletin title summarizing these article titles:\n" + condensed
                        )
                        alt_messages = []
                        system_prompt = prompts.get("title_system") or prompts.get("system_title") or ""
                        if system_prompt:
                            alt_messages.append({"role": "system", "content": system_prompt})
                        alt_messages.append({"role": "user", "content": alt_prompt})
                        alt_title = await ai_chat_completion_fn(
                            alt_messages,
                            purpose="title_alt",
                            postprocess=lambda r: r.splitlines()[0].strip(),
                        )
                        if alt_title:
                            ai_title = alt_title
                except Exception as exc:
                    logger.debug("Alternative AI title attempt failed: %s", exc)
        except Exception as exc:
            logger.error("Error generating AI title: %s", exc)

    if generate_title and not ai_title:
        try:
            concat_titles = [
                (s.get("item_title") or s.get("title", "")).strip()
                for s in summaries
                if (s.get("item_title") or s.get("title"))
            ]
            if concat_titles:
                heuristic = ", ".join(concat_titles[:5])[:120]
                ai_title = heuristic.rstrip(" ,")
        except Exception:
            pass
        if not ai_title:
            ai_title = generate_title_from_introduction(introduction or "", group_name, session_key)

    return introduction, ai_title


async def with_session_intro_and_title(
    *,
    markdown_bulletin: str,
    summaries: List[Dict[str, Any]],
    group_name: str,
    session_key: str,
    enable_intro: bool,
    prompts: Dict[str, Any],
    generate_ai_introduction,
    generate_ai_title,
    ai_chat_completion_fn,
    generate_title_from_introduction,
    generate_title: bool = True,
) -> Tuple[Optional[str], Optional[str]]:
    """Wrapper that manages a ClientSession for intro/title generation."""
    async with ClientSession() as session:
        def bound_intro(mb):
            return generate_ai_introduction(mb, session)
        
        def bound_title(mb):
            return generate_ai_title(mb, session)
        
        return await generate_intro_and_title(
            markdown_bulletin=markdown_bulletin,
            summaries=summaries,
            group_name=group_name,
            session_key=session_key,
            enable_intro=enable_intro,
            prompts=prompts,
            generate_ai_introduction=bound_intro,
            generate_ai_title=bound_title,
            ai_chat_completion_fn=ai_chat_completion_fn,
            generate_title_from_introduction=generate_title_from_introduction,
            generate_title=generate_title,
        )


__all__ = [
    "generate_intro_and_title",
    "with_session_intro_and_title",
]
