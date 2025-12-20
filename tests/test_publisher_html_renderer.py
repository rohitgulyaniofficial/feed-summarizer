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
