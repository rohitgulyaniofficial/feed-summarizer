
from config import config
from workers.publisher.html_renderer import generate_bulletin_html


def test_generate_bulletin_html_renders_title_and_summary():
    summaries = [
        {
            "topic": "tech",
            "summary_text": "S1",
        }
    ]

    html = generate_bulletin_html(
        summary_group="group1",
        feed_slugs=["f1"],
        summaries=summaries,
        introduction="Intro text",
        title_text="Custom Title",
    )

    assert "Custom Title" in html
    assert "Intro text" in html
    assert "S1" in html


def test_generate_bulletin_html_uses_feed_label(monkeypatch):
    monkeypatch.setattr(config, "FEED_LABELS", {"f1": "Feed One"}, raising=False)
    summaries = [
        {
            "topic": "tech",
            "summary_text": "S1",
            "feed_slug": "f1",
        }
    ]

    html = generate_bulletin_html(
        summary_group="group1",
        feed_slugs=["f1"],
        summaries=summaries,
        introduction="Intro text",
        title_text="Custom Title",
    )

    assert "Source: Feed One" in html


def test_generate_bulletin_html_falls_back_to_slug(monkeypatch):
    monkeypatch.setattr(config, "FEED_LABELS", {}, raising=False)
    summaries = [
        {
            "topic": "tech",
            "summary_text": "S1",
            "feed_slug": "f2",
        }
    ]

    html = generate_bulletin_html(
        summary_group="group1",
        feed_slugs=["f2"],
        summaries=summaries,
        title_text="Custom Title",
    )

    assert "Source: f2" in html
