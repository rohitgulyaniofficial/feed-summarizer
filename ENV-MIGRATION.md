# ENV Migration Guide

This guide provides copy/paste environment setups for three operating modes:

1. Existing Azure OpenAI mode (current behavior)
2. New GitHub Models mode (planned provider switch)
3. Local static publishing plus optional GitHub Pages distribution

Use this during migration to avoid mixed/partial configuration.

---

## 1) Azure OpenAI Mode (Current-Compatible)

Use this if you want the existing behavior unchanged.

Required:
- `AZURE_ENDPOINT`
- `OPENAI_API_KEY`
- `DEPLOYMENT_NAME`
- `OPENAI_API_VERSION`

Recommended:
- `LLM_PROVIDER=azure` (explicitly pins provider)

Example `.env`:

```dotenv
# Provider selection (explicit)
LLM_PROVIDER=azure

# Azure OpenAI
AZURE_ENDPOINT=your-resource-name.openai.azure.com
OPENAI_API_KEY=your-azure-openai-key
DEPLOYMENT_NAME=gpt-4o-mini
OPENAI_API_VERSION=2024-10-21

# Core app
DATABASE_PATH=feeds.db
RSS_BASE_URL=http://localhost:8000
PUBLIC_DIR=public

# Scheduler
SCHEDULER_TIMEZONE=UTC
SCHEDULER_RUN_IMMEDIATELY=false
```

Notes:
- `AZURE_ENDPOINT` in this repo is normalized and can be with or without `https://`.
- Keep Azure upload vars unset if running local-only publishing.

---

## 2) GitHub Models Mode (Planned)

Use this to run LLM calls against GitHub Models API instead of Azure OpenAI.

Required (planned):
- `LLM_PROVIDER=github_models`
- `LLM_API_KEY=<github_pat_with_models:read>`
- `LLM_MODEL=<model-id>`

Optional:
- `LLM_BASE_URL` (default expected: `https://models.inference.ai.azure.com`)

Example `.env`:

```dotenv
# Provider selection
LLM_PROVIDER=github_models

# GitHub Models auth/model
LLM_API_KEY=github_pat_xxxxxxxxxxxxxxxxxxxx
LLM_MODEL=gpt-4.1-mini
# Optional override; leave unset unless needed
# LLM_BASE_URL=https://models.inference.ai.azure.com

# Core app
DATABASE_PATH=feeds.db
RSS_BASE_URL=http://localhost:8000
PUBLIC_DIR=public

# Scheduler
SCHEDULER_TIMEZONE=UTC
SCHEDULER_RUN_IMMEDIATELY=false
```

Token setup checklist:
1. Create a GitHub PAT.
2. Grant `models:read` scope.
3. Store in `LLM_API_KEY` (or in your secrets file).

Model checklist:
1. Confirm model id is available to your account in GitHub Models.
2. Start with a compact model for summaries to reduce quota pressure.

Rate-limit reality:
- Free API usage is rate-limited (requests/min, requests/day, token caps).
- Treat this as experimentation/low-throughput unless you move to paid limits.

---

## 3) Local Static Publishing (No Azure Blob)

If Azure upload is not configured, outputs are still generated locally.

Primary outputs:
- `public/index.html`
- `public/bulletins/*.html`
- `public/feeds/*.xml`

Example `.env` additions:

```dotenv
PUBLIC_DIR=public
RSS_BASE_URL=http://localhost:8000

# Ensure Azure upload is effectively disabled
AZURE_STORAGE_ACCOUNT=
AZURE_STORAGE_KEY=
AZURE_STORAGE_CONTAINER=$web
AZURE_UPLOAD_SYNC_DELETE=false
```

Run locally:

```bash
python main.py run --no-azure
python -m http.server 8000 -d public
```

Consumption:
- Browser: `http://localhost:8000/`
- RSS reader: subscribe to `http://localhost:8000/feeds/<group>.xml`

---

## 4) Tailscale Hosting Pattern

Use one always-on node as generator + static host.

Suggested flow:
1. Run scheduler on the node (`main.py scheduled`).
2. Serve `public/` via static HTTP server.
3. Access over Tailscale MagicDNS/IP.
4. Set `RSS_BASE_URL` to the Tailscale-served URL.

Example:

```dotenv
RSS_BASE_URL=http://your-node-name.tailnet.ts.net:8000
```

---

## 5) GitHub Pages Distribution Pattern

GitHub Pages is static-only and cannot host SQLite.

Correct architecture:
1. Keep `feeds.db` on the generator host (local box/VM/runner cache).
2. Run pipeline there.
3. Publish only `public/` contents to `gh-pages`.

What SQLite is for:
- feed fetch history/cache headers,
- dedupe/merge fingerprints,
- publication state,
- recurring coverage detection history.

Without persistent SQLite, pipeline still runs but continuity is degraded.

---

## 6) Secrets File Mapping (Optional)

If using `SECRETS_FILE`, map environment vars there instead of `.env`.

Azure-style secrets example:

```yaml
LLM_PROVIDER: azure
AZURE_ENDPOINT: your-resource-name.openai.azure.com
OPENAI_API_KEY: your-azure-openai-key
DEPLOYMENT_NAME: gpt-4o-mini
OPENAI_API_VERSION: "2024-10-21"
```

GitHub Models-style secrets example:

```yaml
LLM_PROVIDER: github_models
LLM_API_KEY: github_pat_xxxxxxxxxxxxxxxxxxxx
LLM_MODEL: gpt-4.1-mini
# LLM_BASE_URL: https://models.inference.ai.azure.com
```

---

## 7) Migration Checklist

1. Choose provider (`azure` or `github_models`).
2. Remove stale vars from the other provider to avoid confusion.
3. Run summarizer-only test command first.
4. Run full local pipeline with `--no-azure`.
5. Confirm generated files under `public/`.
6. Confirm RSS URLs resolve correctly based on `RSS_BASE_URL`.

Quick validation commands:

```bash
python main.py summarizer
python main.py publish --no-azure
python -m http.server 8000 -d public
```

---

## 8) Troubleshooting

- **LLM init fails:** confirm required vars for selected provider are set and non-empty.
- **No summaries generated:** verify provider selection, model id, and API key scope.
- **RSS links point to wrong domain:** fix `RSS_BASE_URL`.
- **Repeated/duplicate behavior after restart:** ensure `feeds.db` is persisted between runs.

---

## 9) Recommended Defaults

For stable personal use:
- Persistent local SQLite (`feeds.db` on disk)
- `--no-azure` unless you explicitly want blob upload
- Conservative model + schedule to stay within free-tier limits
- Static serving over Tailscale for private multi-device access
