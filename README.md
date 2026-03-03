# Feed Summarizer (Fork)

This repository is a fork of the original `feed-summarizer` project by **Ricardo "rcarmo" Carmo**.

- Original repository: `https://github.com/rcarmo/feed-summarizer`
- Original README: `https://github.com/rcarmo/feed-summarizer/blob/main/README.md`

## Attribution and AI Notice

- The original architecture and project direction come from the upstream project by `rcarmo`.
- The fork-specific changes in this repository are intentionally documented as **AI-generated changes**.
- In other words: modifications introduced in this fork were produced through AI-assisted development workflows.

## What This Project Does

`feed-summarizer` is an async pipeline that:

- fetches RSS/Atom sources,
- stores processing state in SQLite,
- summarizes/group items into bulletin sessions,
- publishes static HTML and RSS outputs under `public/`.

Output can be consumed locally, on private static hosting (for example over Tailscale), or via GitHub Pages.

## Fork-Specific Changes

This fork adds and/or changes the following:

1. LLM provider abstraction
- Added provider-aware LLM routing with support for `azure` and `github_models`.
- Added `LLM_PROVIDER`, `LLM_MODEL`, `LLM_BASE_URL`, and `LLM_API_KEY` configuration.

2. GitHub Models integration
- Enabled summarization/title/intro flows to run using GitHub Models endpoint.
- Replaced Azure-only feature checks with provider-aware `llm_enabled` gating.

3. Static publishing improvements
- Fixed index link behavior for local/static use to avoid `example.com` redirects.
- Ensured feeds index only shows bulletin links when bulletin files exist.

4. GitHub Pages deployment workflow
- Added `.github/workflows/publish-pages.yml`.
- Added scheduled + manual dispatch deployment modes.
- Added `feeds.db` cache restore/save to preserve generator continuity between runs.
- Added support for private feed config via Actions secrets:
  - `FEEDS_YAML` (raw YAML)
  - `FEEDS_YAML_B64` (base64 fallback)

5. Tooling and runtime standardization
- Migrated project workflow to `uv` (`uv sync`, `uv run`, `uv add`, `uv lock`).
- Added `.python-version`, `pyproject.toml` project metadata/dependencies, and `uv.lock`.
- Added `AGENTS.md` to enforce `uv`-based development commands.

6. Docs and test updates
- Added `ENV-MIGRATION.md` and extended config/publishing docs.
- Added GitHub Pages setup doc: `docs/GITHUB_PAGES.md`.
- Added provider-selection tests and updated test compatibility paths.

## Quickstart (uv)

```bash
uv sync
cp feeds.yaml.example feeds.yaml
uv run python main.py run --no-azure
uv run python -m http.server 8000 -d public
```

Then open `http://127.0.0.1:8000/`.

## Documentation

- `docs/CONFIGURATION.md`
- `docs/RUNNING.md`
- `docs/PUBLISHING.md`
- `docs/GITHUB_PAGES.md`
- `ENV-MIGRATION.md`
- `PLAN.md`

## License

This fork remains under the same project license (`LICENSE`). Please also review upstream licensing and attribution in the original repository.
