# Configuration & Environment

This document is the detailed reference for configuring Feed Summarizer.

## Core files

- `feeds.yaml` (sources, groups, schedule, passthrough) – start from `feeds.yaml.example`.
- `prompt.yaml` (prompt templates for summarization & bulletins).
- `secrets.yaml` (or `.env`) for credentials; example in `secrets.yaml.example`.

## Configuration precedence

1. `secrets.yaml` (if configured via `SECRETS_FILE`)
2. Process environment
3. `.env` (if present)

Notes:

- `secrets.yaml` supports either a top-level mapping or nesting under `environment:`.
- `AZURE_ENDPOINT` is normalized (scheme/trailing slashes removed).
- `.env` is loaded without overriding existing environment variables.

## Essential environment variables

Variable | Purpose | Notes
---------|---------|------
`AZURE_ENDPOINT` | Azure OpenAI endpoint host (no scheme) | Auto-normalized (strip `https://`)
`OPENAI_API_KEY` | Azure OpenAI API key | Required for summaries
`DEPLOYMENT_NAME` | Azure OpenAI deployment name | Required for summaries
`OPENAI_API_VERSION` | Azure OpenAI API version | Required for summaries
`RSS_BASE_URL` | Public base URL for generated links | Affects GUID/self links
`DATABASE_PATH` | SQLite path | Default: `feeds.db`
`PUBLIC_DIR` | Output directory root | Default: `$DATA_PATH/public` (where `DATA_PATH` defaults to the repo root)
`AZURE_STORAGE_ACCOUNT` | Blob storage account | Optional (enables upload)
`AZURE_STORAGE_KEY` | Blob storage key | Optional (enables upload)
`AZURE_STORAGE_CONTAINER` | Target container | Default: `$web` (static website)
`AZURE_UPLOAD_SYNC_DELETE` | Delete remote orphans on upload | Default: `false` (danger when true)
`FETCH_INTERVAL_MINUTES` | Base interval fallback | Default: `30`
`SCHEDULER_TIMEZONE` | Override schedule TZ if not in `feeds.yaml` | Default: `UTC`
`SCHEDULER_RUN_IMMEDIATELY` | Run once on boot before schedule loop | Default: `false`

## Retention & batching controls

Variable | Purpose | Default
---------|---------|--------
`MAX_ITEMS_PER_FEED` | Per-feed physical retention cap | `400`
`SUMMARY_WINDOW_ITEMS` | Unsummarized items per feed per summarizer pass | `50`
`BULLETIN_SUMMARY_LIMIT` | Summaries per HTML bulletin chunk | `100`
`BULLETIN_PER_FEED_LIMIT` | Per-feed cap per chunk (auto-reduced if many feeds) | `40`
`BULLETIN_MAX_CHUNKS` | Backlog chunks processed per run | `5`

## Merge tuning (SimHash + optional BM25/FTS5)

Variable | Purpose | Default
---------|---------|------
`SIMHASH_HAMMING_THRESHOLD` | Max Hamming distance (0–64); set `0` to disable merging | `24`
`SIMHASH_MERGE_LINKAGE` | Merge linkage strategy (`complete` or `single`) | `complete`
`BM25_MERGE_ENABLED` | Enable BM25/FTS5 merge fallback | `true`
`BM25_MERGE_RATIO_THRESHOLD` | Minimum mutual BM25 ratio to accept a merge | `0.80`
`BM25_MERGE_MAX_EXTRA_DISTANCE` | Allow BM25 to merge beyond SimHash threshold | `16`
`BM25_MERGE_MAX_QUERY_TOKENS` | Cap tokens used in BM25 queries | `8`

See [MERGE_TUNING.md](MERGE_TUNING.md) for the operational details and diagnostic tools.

## Telemetry

Variable | Purpose | Notes
---------|---------|------
`DISABLE_TELEMETRY` | Disable tracing/log export | Set `true` to opt out
`OTEL_SERVICE_NAME` | Service name override | Optional
`OTEL_ENVIRONMENT` | Environment tag | Optional
`APPLICATIONINSIGHTS_CONNECTION_STRING` | Azure Monitor exporter connection string | Enables export

## Scheduler configuration (`feeds.yaml`)

Newer mapping form with timezone:

```yaml
schedule:
  timezone: Europe/Lisbon
  times:
    - "06:30"
    - "12:30"
    - "20:30"
```

If both `schedule.timezone` and `SCHEDULER_TIMEZONE` are set, the environment variable wins.
