"""AI prompt helpers for publisher."""
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timezone

from aiohttp import ClientSession

from config import get_logger
from workers.publisher.merge import collect_summary_links

logger = get_logger("publisher.prompts")


def generate_markdown_bulletin(summaries: List[Dict[str, Any]]) -> str:
    """Generate a markdown bulletin grouped by topic for AI introduction/title prompts."""
    topics: Dict[str, List[Dict[str, Any]]] = {}
    for summary in summaries:
        topic = summary.get("topic", "General") or "General"
        topics.setdefault(topic, []).append(summary)

    markdown_lines: List[str] = []
    for topic, items in topics.items():
        markdown_lines.append(f"\n## {topic}\n")
        for item in items:
            title = item.get("item_title") or item.get("title") or "Untitled"
            links = collect_summary_links(item)
            if not links:
                markdown_lines.append(f"- {title}")
                continue
            if len(links) == 1:
                markdown_lines.append(f"- {title} ([link]({links[0]['url']}))")
                continue
            link_parts = []
            for idx, link in enumerate(links, start=1):
                label = f"{idx}"
                link_parts.append(f"[{label}]({link['url']})")
            markdown_lines.append(f"- {title} ({', '.join(link_parts)})")
    return "\n".join(markdown_lines)


async def generate_ai_introduction(
    markdown_bulletin: str,
    prompts: Dict[str, str],
    session: ClientSession,
    chat_completion_fn: Callable[..., Any],
) -> Optional[str]:
    """Generate an AI introduction with post-processing to 1–2 concise sentences."""
    intro_prompt = prompts.get("intro", "") if isinstance(prompts, dict) else ""
    if not intro_prompt:
        logger.error("No 'intro' prompt found in configuration")
        return None
    try:
        formatted_prompt = intro_prompt.format(body=markdown_bulletin)
    except Exception as exc:
        logger.error("Error formatting intro prompt: %s", exc)
        return None

    def _postprocess(raw: str) -> str:
        text = " ".join(raw.split())
        parts = text.split(". ")
        if len(parts) >= 2:
            intro = parts[0].rstrip(". ") + ". " + parts[1].rstrip(". ")
        else:
            intro = parts[0].rstrip(". ")
        words = intro.split()
        if len(words) > 60:
            intro = " ".join(words[:60]).rstrip(".,;:! ") + "."
        return intro

    messages = [{"role": "user", "content": formatted_prompt}]
    return await chat_completion_fn(messages, purpose="intro", postprocess=_postprocess)


async def generate_ai_title(
    markdown_bulletin: str,
    prompts: Dict[str, str],
    session: ClientSession,
    chat_completion_fn: Callable[..., Any],
) -> Optional[str]:
    """Generate a concise AI title for the bulletin."""
    title_prompt = prompts.get("title", "") if isinstance(prompts, dict) else ""
    if not title_prompt:
        logger.warning("No 'title' prompt found in configuration; cannot generate AI titles")
        return None
    try:
        formatted_prompt = title_prompt.format(body=markdown_bulletin)
    except Exception as exc:
        logger.error("Error formatting title prompt: %s", exc)
        return None

    def _postprocess(raw: str) -> str:
        title = raw.splitlines()[0].strip()
        if title.endswith((".", "!", "?", ":", ";")):
            title = title.rstrip(".!?:;").strip()
        words = title.split()
        if len(words) > 12:
            title = " ".join(words[:12])
        if title:
            logger.info("Generated AI title: '%s'", title[:120])
        else:
            logger.warning("AI title generation returned an empty string after post-processing")
        return title

    system_prompt = prompts.get("title_system") or prompts.get("system_title") or ""
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": formatted_prompt})
    result = await chat_completion_fn(messages, purpose="title", postprocess=_postprocess)
    if result:
        return result

    try:
        trimmed = "\n".join(markdown_bulletin.splitlines()[:50])
        if len(trimmed) < len(markdown_bulletin):
            logger.debug(
                "Retrying AI title with trimmed bulletin context (%d -> %d chars)",
                len(markdown_bulletin),
                len(trimmed),
            )
            retry_prompt = title_prompt.format(body=trimmed)
            retry_messages = []
            if system_prompt:
                retry_messages.append({"role": "system", "content": system_prompt})
            retry_messages.append({"role": "user", "content": retry_prompt})
            result_retry = await chat_completion_fn(
                retry_messages,
                purpose="title_retry",
                postprocess=_postprocess,
            )
            if result_retry:
                return result_retry
    except Exception as exc:
        logger.debug("AI title retry failed: %s", exc)
    return None


def generate_title_from_introduction(introduction: str, group_name: str, session_key: str) -> str:
    """Generate a descriptive title from the AI introduction or session key."""
    if not introduction or not introduction.strip():
        try:
            if session_key.count("-") == 4:
                bulletin_time = datetime.strptime(session_key[:16], "%Y-%m-%d-%H-%M").replace(
                    tzinfo=timezone.utc
                )
            elif session_key.count("-") >= 5:
                base_time_str = "-".join(session_key.split("-")[:5])
                bulletin_time = datetime.strptime(base_time_str, "%Y-%m-%d-%H-%M").replace(
                    tzinfo=timezone.utc
                )
                chunk_number = session_key.split("-")[-1]
                return f"{group_name.title()} Bulletin #{chunk_number} - {bulletin_time.strftime('%Y-%m-%d %H:%M UTC')}"
            else:
                bulletin_time = datetime.strptime(session_key, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)

            date_str = bulletin_time.strftime("%Y-%m-%d")
            time_str = bulletin_time.strftime("%H:%M")
            return f"{group_name.title()} Bulletin - {date_str} {time_str} UTC"
        except ValueError:
            return f"{group_name.title()} News Bulletin"

    intro_clean = introduction.strip()
    first_sentence = intro_clean.split(".")[0]
    if len(first_sentence) > 80:
        short_intro = intro_clean[:50]
        last_space = short_intro.rfind(" ")
        if last_space > 20:
            short_intro = short_intro[:last_space]
        title_base = short_intro
    else:
        title_base = first_sentence

    title_base = title_base.replace("\n", " ").strip()
    if not title_base.endswith("."):
        title_base += "..."

    return f"{title_base}"


__all__ = [
    "generate_markdown_bulletin",
    "generate_ai_introduction",
    "generate_ai_title",
    "generate_title_from_introduction",
]
