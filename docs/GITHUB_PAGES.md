# GitHub Pages Deployment

This repository can publish generated static output (`public/`) to GitHub Pages using Actions.

## What gets deployed

- `public/index.html`
- `public/bulletins/*.html`
- `public/feeds/*.xml`
- `public/feeds/index.html`

SQLite (`feeds.db`) is not hosted on Pages. It remains on the runner and is cached between workflow runs.

## Workflow

Workflow file: `.github/workflows/publish-pages.yml`

Triggers:
- Scheduled every 6 hours
- Manual dispatch (`full` or `publish_only`)

Modes:
- `full`: `uv run python main.py run --no-azure`
- `publish_only`: `uv run python main.py publish --no-azure`

## Repository setup

1. In your repo, open **Settings → Pages**.
2. Under **Build and deployment**, set source to **GitHub Actions**.
3. In **Settings → Secrets and variables → Actions**, add:
   - `LLM_API_KEY` = token used for GitHub Models API.
   - Optional: `FEEDS_YAML_B64` = base64-encoded `feeds.yaml` for private feed config.

The workflow sets:
- `LLM_PROVIDER=github_models`
- `LLM_MODEL=gpt-4.1-mini`
- `RSS_BASE_URL=https://<owner>.github.io/<repo>`

If you want another model, edit `LLM_MODEL` in the workflow.

Feed config precedence in workflow:
1. `FEEDS_YAML_B64` secret (decoded into `feeds.yaml`)
2. Repository `feeds.yaml` (if committed)
3. Fallback `feeds.yaml.example`

## Notes on state

- The workflow restores/saves `feeds.db` via `actions/cache`.
- Cold starts are expected when cache is missing/expired.
- Keeping cache improves dedupe, recurring-story tracking, and fetch continuity.
