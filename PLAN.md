# PLAN: Add GitHub Models Provider + Static Hosting Workflow

## Goal

Enable this codebase to:

1. Run and publish locally (or to GitHub Pages/Tailscale/static server) without requiring Azure Blob.
2. Use GitHub Models (via PAT + API) as an alternative to Azure OpenAI for summarization/title/intro generation.

---

## Scope

### In scope
- Add provider-agnostic LLM configuration and runtime selection.
- Implement a GitHub Models provider path in `services/llm_client.py`.
- Replace Azure-specific capability checks with provider-aware checks.
- Update docs and examples for local/static + GitHub Models usage.
- Add tests for provider selection and failure modes.

### Out of scope
- Replacing SQLite with a different database.
- Changing dedupe/merge algorithm behavior.
- Building a new deployment platform/toolchain from scratch.
- Migrating to paid GitHub Models billing setup (we support free tier constraints and provider config only).

---

## Current State (as observed)

- LLM calls are Azure-only in `services/llm_client.py`.
- Summarizer startup validation in `workers/summarizer/core.py` enforces Azure env vars (`AZURE_ENDPOINT`, `OPENAI_API_KEY`, `DEPLOYMENT_NAME`, `OPENAI_API_VERSION`).
- Publisher intro/title gating also uses Azure-specific checks in a few places.
- Static publishing already works locally under `public/`; Azure upload is optional and separate.
- SQLite (`feeds.db`) is stateful pipeline storage (fetch history, summaries, publication state, similarity/merge history), not frontend runtime storage.

---

## Design Overview

## 1) Introduce provider selection

Add a provider switch in config with backward-compatible defaults.

Proposed env vars:
- `LLM_PROVIDER` = `azure` (default) or `github_models`
- `LLM_MODEL` = model identifier (required for `github_models`; optional alias for Azure deployment name)
- `LLM_BASE_URL` = optional override for OpenAI-compatible endpoint
- `LLM_API_KEY` = generic key (maps to PAT for GitHub Models; optional fallback to existing keys)
- Keep existing Azure vars for Azure mode:
  - `AZURE_ENDPOINT`
  - `OPENAI_API_KEY`
  - `DEPLOYMENT_NAME`
  - `OPENAI_API_VERSION`

Backward compatibility:
- If `LLM_PROVIDER` is missing, default to `azure`.
- Existing Azure configs continue to work unchanged.

## 2) Provider-agnostic LLM client

Refactor `services/llm_client.py`:
- Build a single `chat_completion(...)` interface that routes by provider.
- Keep robust error parsing (content filter / truncation / malformed JSON resilience).
- For `github_models`:
  - Use OpenAI-compatible Chat Completions endpoint.
  - Base URL default: `https://models.inference.ai.azure.com` (unless overridden by `LLM_BASE_URL`).
  - Auth with PAT (`models:read`) from `LLM_API_KEY` (or explicit GitHub token env fallback).
  - Model from `LLM_MODEL`.
- For `azure`:
  - Preserve existing behavior using deployment + API version.

Implementation preference:
- Keep the external function signature unchanged so summarizer/publisher code does not need broad refactors.
- Encapsulate provider-specific client init in internal helper(s), cached per provider.

## 3) Replace Azure-specific “LLM enabled” checks

Update call sites that currently do:
- `if config.AZURE_ENDPOINT and config.OPENAI_API_KEY: ...`

Replace with:
- provider-aware helper (e.g., `is_llm_enabled()` in `services/llm_client.py` or `config.py`) so title/intro generation and summarizer validation are aligned.

Files to update:
- `workers/summarizer/core.py`
- `workers/publisher/core.py`
- `workers/publisher/bulletin_processor.py`
- `workers/publisher/titles.py` (if applicable after grep pass)

## 4) Validation model

Add centralized validation routine:
- `validate_llm_config(provider) -> list[str] errors`

Rules:
- `azure`: requires endpoint, key, deployment name, api version.
- `github_models`: requires PAT + model; optional base URL with sane default.
- Return actionable error messages with exact missing env var names.

Use this in summarizer startup and any place that currently logs Azure-only config errors.

---

## Detailed Step-by-Step Implementation Plan

## Phase A — Baseline and safety checks
1. Confirm all current tests pass on baseline branch.
2. Capture grep snapshot of Azure-specific checks to ensure complete replacement.
3. Identify all call paths using `chat_completion` (summaries, merge synthesis, intro/title generation).

Deliverable:
- List of all touched files and expected behavior unchanged for Azure mode.

## Phase B — Config layer extension
1. Add new config fields in `config.py`:
   - `LLM_PROVIDER`, `LLM_MODEL`, `LLM_BASE_URL`, `LLM_API_KEY`.
2. Normalize `LLM_PROVIDER` to lowercase and validate enum (`azure`, `github_models`).
3. Add helper(s):
   - `get_effective_llm_provider()`
   - `get_effective_llm_api_key()`
   - `llm_config_summary()` (redacted)
4. Keep existing config summary keys and append non-breaking provider fields.

Deliverable:
- Backward-compatible config object with new provider settings.

## Phase C — LLM client refactor
1. Refactor `services/llm_client.py` into provider-aware routing.
2. Add provider-specific client init:
   - Azure client init (existing behavior).
   - GitHub Models init (OpenAI-compatible base URL + PAT + model id).
3. Keep retry/timeouts/rate-limit handling compatible with existing summarizer usage.
4. Ensure response parsing preserves current JSON extraction robustness.

Deliverable:
- Existing callers keep using `chat_completion(...)` with no signature changes.

## Phase D — Replace capability checks across workers
1. Update summarizer validation to call provider-aware validator.
2. Update publisher intro/title gates to use generic `llm_enabled`.
3. Ensure behavior when LLM is disabled remains graceful (fallback title generation etc.).

Deliverable:
- No Azure literals in functional gating logic except inside provider-specific branches.

## Phase E — Docs and examples
1. Update `README.md` (or equivalent primary docs):
   - Local/static consumption flow.
   - GitHub Models provider setup.
2. Update `docs/CONFIGURATION.md`:
   - New env vars and precedence rules.
3. Update `feeds.yaml.example` only if needed (likely minimal/no changes).
4. Update secrets/env example file(s):
   - PAT with `models:read`.
   - Example model ids.
5. Add note about GitHub Models free-tier limits (RPM/RPD/token caps).

Deliverable:
- Clear setup instructions for both Azure and GitHub Models.

## Phase F — Tests
1. Unit tests for provider config validation:
   - missing vars produce clear errors.
2. Unit tests for provider routing in `llm_client`:
   - Azure path selected when `LLM_PROVIDER=azure`.
   - GitHub Models path selected when `LLM_PROVIDER=github_models`.
3. Tests for publisher/summarizer gating:
   - LLM enabled/disabled behavior by provider config.
4. Keep existing Azure tests passing without env changes.

Deliverable:
- Green test suite with new provider coverage.

## Phase G — End-to-end verification
1. Run summarizer-only with `LLM_PROVIDER=github_models` using mocked responses/tests first.
2. Run local pipeline with `--no-azure`; verify outputs:
   - `public/index.html`
   - `public/bulletins/*.html`
   - `public/feeds/*.xml`
3. Validate no regressions in Azure mode (config parity check).

Deliverable:
- Manual verification checklist completed.

## Phase H — GitHub Pages publishing workflow
1. Add a GitHub Actions workflow at `.github/workflows/publish-pages.yml` that:
   - runs on `workflow_dispatch` and scheduled cron,
   - checks out the repo,
   - installs `uv` and syncs dependencies with `uv sync`,
   - runs the pipeline with `uv run python main.py run --no-azure`,
   - uploads `public/` as a GitHub Pages artifact,
   - deploys using `actions/deploy-pages`.
2. Add Actions permissions for Pages deployment in workflow:
   - `pages: write`
   - `id-token: write`
   - `contents: read`
3. Configure runtime env/secrets for the workflow:
   - `LLM_PROVIDER=github_models`
   - `LLM_MODEL=<model-id>`
   - `LLM_API_KEY=${{ secrets.LLM_API_KEY }}`
   - `RSS_BASE_URL=https://<user-or-org>.github.io/<repo>`
4. Preserve SQLite continuity between workflow runs:
   - cache `feeds.db` with `actions/cache` keyed by branch + date window,
   - restore before run and save after run,
   - document cache fallback behavior (cold start is acceptable).
5. Add optional manual fallback workflow mode:
   - separate job/flag to publish existing `public/` without running summarizer.

Deliverable:
- Pages deployment works from Actions and serves generated `public/` content.
- Feed links/self URLs match the GitHub Pages URL.
- `feeds.db` state survives across most runs via cache.

---

## Local + GitHub Pages Operating Model

## Recommended production-lite workflow
1. Keep persistent SQLite on generator host (local mini-PC, VM, or CI cache strategy).
2. Run scheduled pipeline there.
3. Publish only static output (`public/`) to:
   - local static server,
   - Tailscale-served host,
   - GitHub Pages branch (`gh-pages`) via sync action/script.

## Why SQLite stays off GitHub Pages
- GitHub Pages is static-only. SQLite is runtime state for generation, not serving.
- Generated artifacts in `public/` are all that readers need.

---

## Risks and Mitigations

- **Risk:** GitHub Models rate limits are stricter than Azure.
  - **Mitigation:** Keep batching conservative; add retry/backoff clarity; document limits.
- **Risk:** Model output shape differences could affect JSON parsing.
  - **Mitigation:** Preserve robust extraction/validation and add provider-specific parsing tests.
- **Risk:** Hidden Azure assumptions in scattered code paths.
  - **Mitigation:** grep-driven replacement and focused tests on gating logic.
- **Risk:** Free-tier instability/public preview behavior.
  - **Mitigation:** Keep provider switch easy; Azure path remains fallback.

---

## Acceptance Criteria

1. `LLM_PROVIDER=github_models` works end-to-end for summarization and optional title/intro generation.
2. `LLM_PROVIDER=azure` works exactly as before (no breaking changes).
3. Pipeline runs with `--no-azure` and produces complete local static outputs.
4. Docs clearly explain:
   - static hosting model,
   - SQLite role,
   - GitHub Models setup and limitations.
5. Tests pass, including new provider-routing/validation tests.
6. GitHub Pages deployment publishes `public/` and serves:
   - `index.html`
   - `feeds/index.html`
   - `feeds/*.xml`
7. `RSS_BASE_URL` used in CI matches published Pages URL (no `example.com` links).
8. Workflow docs include required repo settings and secret names.

---

## Proposed File Touch List (Expected)

- `config.py`
- `services/llm_client.py`
- `workers/summarizer/core.py`
- `workers/publisher/core.py`
- `workers/publisher/bulletin_processor.py`
- `workers/publisher/titles.py` (if check remains)
- `docs/CONFIGURATION.md`
- `README.md` (or equivalent top-level usage guide)
- optional: `.env.example` / `secrets.yaml.example`
- tests:
  - new/updated files under `tests/` for provider routing + validation
- `.github/workflows/publish-pages.yml`
- optional: `docs/GITHUB_PAGES.md`

---

## Implementation Order (Commit-friendly)

1. Config + validation helpers.
2. LLM client provider routing.
3. Worker gating updates.
4. Tests for config/client/gating.
5. Docs and examples.
6. Final regression run.
7. Add GitHub Pages workflow + docs.
8. Run manual workflow dispatch and verify live Pages output.

---

## Notes for Implementation Session

- Preserve API signatures where possible.
- Avoid broad refactors unrelated to provider support.
- Keep logging redacted for API keys/PAT.
- Prefer explicit, actionable error messages for misconfiguration.
