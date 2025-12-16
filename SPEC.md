# Feed Summarizer Specification

## Overview

The Feed Summarizer is an asynchronous RSS/Atom processing system that fetches feeds, generates AI summaries, and publishes both HTML bulletins and RSS feeds. It uses Azure OpenAI for summarization and can upload outputs to Azure Blob Storage. The system runs as a background service with a smart scheduler and a SQLite-backed persistence layer.

Key design tenets:

- Async-first using asyncio/aiohttp
- Minimal dependencies (see requirements.txt) and functional-style modules
- Robust error handling and observability
- Efficient I/O (HTTP conditional requests, atomic writes, selective uploads)

## Key features

- Orchestrated pipeline coordinated by `main.py`
- Async fetching, summarization, and publishing
- Configurable fetch intervals and per-feed schedules
- Decorator-based OpenTelemetry tracing with Azure Application Insights export
- Thread-safe SQLite operations via a database queue and WAL
- Comprehensive error handling and content sanitization
- Reader mode extraction for selected feeds; HTML-to-Markdown storage
- HTTP conditional requests and exponential backoff retries
- AI-powered summarization and topic grouping via Azure OpenAI
- Session-based bulletins with cached introductions
- Multi-format publishing (HTML + RSS)
- Azure Blob upload with hash-based deduplication (optional)
- Per-feed scoped publishing (only affected groups update)
- Passthrough raw feeds for selected source slugs
- Content filter resilience (detect and bisect 400 policy failures)

## Architecture

### Modules

- `fetcher.py`: Fetch feeds with conditional requests and store items
- `summarizer.py`: Generate summaries using Azure OpenAI; handles content filters and bisection
- `publisher.py`: Publish HTML bulletins and RSS feeds; introduction caching; per-feed scoping; Azure upload
- `models.py`: Database access helpers and queue; bulletin/session management
- `scheduler.py`: Global and per-feed schedule/interval handling
- `azure_storage.py`: Minimal Azure Blob Storage REST client used by the uploader
- `config.py`: Configuration loading, environment handling, endpoint normalization
- `utils.py`: Common helpers (sanitization, atomic writes, hashing)

### Workflow

1. Fetch: `fetcher.py` pulls new items from `feeds.yaml` sources using conditional HTTP and stores them in SQLite.
2. Summarize: `summarizer.py` builds prompts, calls Azure OpenAI via `llm_client.py`, and stores summaries/topics.
3. Publish HTML/RSS: `publisher.py` creates session bulletins per group with AI introductions and generates RSS feeds.
4. Passthrough: `publisher.py` optionally emits raw passthrough feeds for selected slugs.
5. Upload: `publisher.py` (via `AzureStorageUploader` and `azure_storage.py`) can push changed files to Azure Blob Storage.

### Fetching details

- Validates and sends If-Modified-Since/If-None-Match based on stored `etag` and `last_modified`.
- Handles 304 efficiently and uses error-count-based backoff on failures.
- Supports optional proxy configuration from `feeds.yaml` (including Tor via `tor` service).
- Reader mode extraction for select feeds; HTML sanitized and stored as Markdown.

### Summarization details

- Queues unsummarized items, trims inputs, constructs prompts.
- Calls Azure OpenAI asynchronously via `llm_client.chat_completion`.
- Detects content filter / policy refusals and returns `None` or raises `ContentFilterError` as appropriate.
- Recursively bisects batches on filter failures to salvage non-offending items; logs skipped IDs.
- Stores summaries with topics; marks items as summarized.

### Publishing details

- Session bulletins keyed to a time window; large sessions can be split for readability.
- AI introductions generated once per session and cached for reuse.
- Per-feed scoped publishing: in per-feed runs, only groups intersecting changed slugs are rebuilt.
- Diagnostics in logs: per-topic and per-feed counts; warns on unusually small bulletins.

### Similarity matching and merge behavior

The publisher can merge multiple summaries that represent the same underlying story.
This is primarily intended to reduce duplicates across sources when generating bulletins and RSS feeds.

Important scope note:

- Merging is performed in the publishing phase by `publisher.py` over the in-memory list of summaries being rendered.
- In the default flow, this means merging is scoped to the current bulletin/session being built (not a global, cross-session dedupe).

#### Primary signal: SimHash (64-bit, Hamming distance)

- Each summary can carry a dedicated merge fingerprint stored in SQLite as `summaries.merge_simhash`.
- The merge fingerprint is computed over a stable, reproducible input: `title + "\n" + summary_text`.
- The publisher prefers the stored `merge_simhash` when available and falls back to computing it on the fly (and finally to the legacy `simhash`).

The merge decision uses:

- A configurable maximum Hamming distance, `SIMHASH_HAMMING_THRESHOLD`.
- Additional conservative guardrails to reduce false positives:
  - merge when there is strong token overlap in titles, or
  - strong overlap in summary text, or
  - a single shared “high-signal” title token (e.g., long token or token with digits).

This design keeps SimHash fast and compact (O(1) fingerprinting; cheap distance checks), while using overlap checks to avoid accidental merges.

#### Secondary signal: optional BM25/FTS5 fallback

SQLite FTS5 can be used as a bounded fallback when SimHash is missing or only slightly above the threshold.

- The schema includes an optional FTS5 table `summary_fts(title, summary_text, topic UNINDEXED)` with `rowid = summaries.id`.
- When enabled, the publisher precomputes mutual BM25 “ratios” among the current candidate set:
  - Query the index for top-k matches.
  - Compute `ratio = abs(candidate_score) / abs(self_score)`.
  - Require mutual agreement (A ranks B strongly and B ranks A strongly).

This is controlled by environment variables:

- `BM25_MERGE_ENABLED`: enable/disable BM25 fallback.
- `BM25_MERGE_RATIO_THRESHOLD`: minimum mutual ratio in [0,1] (default 0.80).
- `BM25_MERGE_MAX_EXTRA_DISTANCE`: allow BM25 to merge beyond the SimHash threshold by this many bits (default 6).
- `BM25_MERGE_MAX_QUERY_TOKENS`: cap tokens used in BM25 queries (default 8).

#### Producing the merged text (LLM-assisted)

When a cluster is formed, the publisher can optionally generate a single merged summary text via the `similar_merge` prompt.

- Input: the summaries in the cluster, each with an ID.
- Output requirement: JSON array with EXACTLY ONE element: `{ "summary": string, "ids": [ ... ] }`.
- The publisher validates the parsed output and prefers a merged summary whose `ids` best cover the full expected cluster; partial coverage is logged and the publisher falls back to a concatenated textual summary.

This keeps the merge step robust to occasional model formatting issues.

#### Comparison: SimHash vs BM25 (FTS5)

Both matchers operate over the same conceptual input (`title + "\n" + summary_text`) but behave differently:

| Aspect | SimHash | BM25 (SQLite FTS5)
| --- | --- | --- |
| Core idea | Fixed-length 64-bit fingerprint over token shingles | Ranking score over an inverted index of normalized tokens |
| Similarity signal | Hamming distance between fingerprints | Relative BM25 score (normalized into a 0–1 ratio vs the query’s self-score) |
| Strengths | Very fast; strong on near-identical wording | More tolerant to paraphrases and reordered sentences |
| Costs | O(1) per summary + cheap pairwise distance checks | Requires FTS5 index maintenance and per-query top-k lookup |
| Thresholding | Single integer cutoff (0–64) via `SIMHASH_HAMMING_THRESHOLD` | Ratio cutoff via `BM25_MERGE_RATIO_THRESHOLD` + mutual agreement |
| Common failure modes | Collisions on short/boilerplate text; weak on paraphrases | Matches boosted by shared boilerplate unless queries are constrained |
| Intended role here | Primary, fast merge signal | Optional, bounded fallback for “almost matches” |

#### Recommended strategy (as implemented)

- Persist a stable merge fingerprint (`summaries.merge_simhash`) computed from `title + "\n" + summary_text` to make merge behavior reproducible.
- Use SimHash as the primary merge gate (configurable by `SIMHASH_HAMMING_THRESHOLD`).
- Apply conservative token-overlap guardrails (titles/summary token overlap and a high-signal token escape hatch) to reduce accidental merges.
- Use BM25 only as a bounded fallback when enabled, and only within a limited extra-distance window (`BM25_MERGE_MAX_EXTRA_DISTANCE`).

Note: topic is deliberately *not* used as a hard merge gate, because upstream classification can be wrong.

#### BM25/FTS5 implementation notes

- The database includes an optional FTS5 virtual table `summary_fts` (created best-effort; some SQLite builds may not ship FTS5).
- Index maintenance is best-effort on summary writes: `models.py` upserts into `summary_fts` in the same persistence path as `summaries`.
- For existing databases, [tools/fts_backfill.py](tools/fts_backfill.py) can populate `summary_fts` from historical summaries.

## Scheduled operation

Define times in `feeds.yaml` (legacy list format):

```yaml
schedule:
  - time: "06:30"
  - time: "12:30"
  - time: "20:30"
```

Or using the newer mapping form with timezone:

```yaml
schedule:
  timezone: Europe/Lisbon
  times:
    - "06:30"
    - "12:30"
    - "20:30"
```

Per-feed schedules and intervals:

- Per-feed schedules: `feeds.<slug>.schedule: [{ time: "HH:MM" }, ...]`
- Per-feed intervals: `feeds.<slug>.interval_minutes` (alias: `refresh_interval_minutes`)
- Scheduler computes due feeds from `last_fetched` and runs per-feed pipelines; publishers respect per-feed scoping

Publishing cadence and timing:

- Global runs (top-level `schedule:` entries) execute the full pipeline: fetch → summarize → publish → upload.
- Per-feed runs (triggered by per-feed schedules or interval checks) also execute the full pipeline but scoped to selected slugs.
- If `SCHEDULER_RUN_IMMEDIATELY=true` (or `FORCE_REFRESH_FEEDS=true`), the scheduler performs a warm run on startup; in practice this runs the same pipeline, and publishing may occur immediately depending on configuration.

Run scheduler:

```bash
python main.py scheduled
python main.py schedule-status
python main.py status
```

## Mastodon list feeds

Configure a Mastodon list as a feed (no extra deps):

```yaml
feeds:
  masto_lobsters:
    type: mastodon
    url: https://mastodon.social/api/v1/timelines/list/46540
    title: Lobsters on Mastodon
    token_env: MASTODON_TOKEN   # or token: "..."
    limit: 40                    # optional; defaults to 40
    summarize: false             # default for mastodon feeds; set true to opt-in
```

Notes:

- Fetcher uses the token to call Mastodon and stores statuses as items
- HTML includes boosts, replies, CWs, attachments, and counters
- Mastodon feeds are excluded from summarization by default

## Hiding groups from index pages

Hide specific summary groups from the HTML and RSS index pages without disabling generation:

```yaml
summaries:
  technews:
    feeds: "teksapo, pplware"
    hidden: true          # hide from index pages (aliases: visible: false, hide_from_index: true)
```

Notes:

- Affects only `public/feeds/index.html` and `public/bulletins/index.html`
- Group pages like `public/bulletins/<group>.html` and `public/feeds/<group>.xml` are still generated

## Passthrough (raw) feeds

Publish raw, non-summarized RSS for selected slugs:

```yaml
passthrough:
  - masto_lobsters    # simple list
  raw_news:
    limit: 50
    title: "Raw News"
```

Outputs are written to `public/feeds/raw/<slug>.xml` and linked from the RSS index.

## Configuration

Environment variables (subset):

- DATABASE_PATH: path to SQLite file (default: `feeds.db`).
- FETCH_INTERVAL_MINUTES: base interval fallback for fetcher (default: `30`).
- ENTRY_EXPIRATION_DAYS: retention for items (default: `365`).
- AZURE_ENDPOINT: Azure OpenAI endpoint host (preferred) or full https URL; normalized internally.
- OPENAI_API_KEY: Azure OpenAI API key.
- DEPLOYMENT_NAME: model deployment (default: `gpt-4o-mini`).
- OPENAI_API_VERSION: API version (taken from env; see README for current default).
- AZURE_STORAGE_ACCOUNT / AZURE_STORAGE_KEY / AZURE_STORAGE_CONTAINER (optional): Azure Blob Storage configuration.
- RSS_BASE_URL: base URL for RSS links.
- SCHEDULER_TIMEZONE: scheduler timezone override (environment wins over `schedule.timezone` when both are set).
- SCHEDULER_RUN_IMMEDIATELY: if true, run the pipeline once immediately on boot before entering the schedule loop.
- FORCE_REFRESH_FEEDS: legacy flag; treated similarly to `SCHEDULER_RUN_IMMEDIATELY` for an immediate run.

Similarity/merge tuning:

- SIMHASH_HAMMING_THRESHOLD: Max Hamming distance (0–64) for merge clustering (0 disables merging). Default is conservative in code; deployments may override (e.g., `kata-compose.yaml` sets 24).
- BM25_MERGE_ENABLED: Enable BM25/FTS5 merge fallback.
- BM25_MERGE_RATIO_THRESHOLD: Minimum mutual BM25 ratio.
- BM25_MERGE_MAX_EXTRA_DISTANCE: Extra distance window beyond SimHash threshold where BM25 may still merge.
- BM25_MERGE_MAX_QUERY_TOKENS: Token cap for BM25 query construction.

`feeds.yaml` thresholds complement the environment toggles:

- `thresholds.time_window_hours`: ignore items older than this many hours when summarizing (default `48`).
- `thresholds.retention_days`: keep published bulletins/feeds for this many days (default `7`).
- `thresholds.initial_fetch_items`: on a brand-new feed, accept up to N most recent entries even if outside the time window (default `10`; set `0` to disable the bootstrap).

## Database

SQLite with WAL via `models.py` queue. Core tables:

- feeds: sources and HTTP state
- items: fetched entries (Markdown)
- summaries: AI summaries with topics and timestamps
- bulletins: group + session records with intro, counts, and feed_slug set
- bulletin_summaries: join table for bulletin membership

See `schema.sql` for the full DDL.

## References

- HTTP conditional request best practices: [http://rachelbythebay.com/w/2023/01/18/http/](http://rachelbythebay.com/w/2023/01/18/http/)
- Feed reader behavior best practices: [http://rachelbythebay.com/w/2024/05/27/feed/](http://rachelbythebay.com/w/2024/05/27/feed/)
- Feed reader scoring criteria: [http://rachelbythebay.com/w/2024/05/30/fs/](http://rachelbythebay.com/w/2024/05/30/fs/)
- Azure OpenAI API reference: [https://learn.microsoft.com/azure/ai-services/openai/reference](https://learn.microsoft.com/azure/ai-services/openai/reference)
- Azure Blob Storage REST API: [https://docs.microsoft.com/rest/api/storageservices/blob-service-rest-api](https://docs.microsoft.com/rest/api/storageservices/blob-service-rest-api)
