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

| Variable                    | Purpose                                     | Notes                                                                      |
| --------------------------- | ------------------------------------------- | -------------------------------------------------------------------------- |
| `AZURE_ENDPOINT`            | Azure OpenAI endpoint host (no scheme)      | Auto-normalized (strip `https://`)                                         |
| `OPENAI_API_KEY`            | Azure OpenAI API key                        | Required for summaries                                                     |
| `DEPLOYMENT_NAME`           | Azure OpenAI deployment name                | Required for summaries                                                     |
| `OPENAI_API_VERSION`        | Azure OpenAI API version                    | Required for summaries                                                     |
| `RSS_BASE_URL`              | Public base URL for generated links         | Affects GUID/self links                                                    |
| `DATABASE_PATH`             | SQLite path                                 | Default: `feeds.db`                                                        |
| `PUBLIC_DIR`                | Output directory root                       | Default: `$DATA_PATH/public` (where `DATA_PATH` defaults to the repo root) |
| `AZURE_STORAGE_ACCOUNT`     | Blob storage account                        | Optional (enables upload)                                                  |
| `AZURE_STORAGE_KEY`         | Blob storage key                            | Optional (enables upload)                                                  |
| `AZURE_STORAGE_CONTAINER`   | Target container                            | Default: `$web` (static website)                                           |
| `AZURE_UPLOAD_SYNC_DELETE`  | Delete remote orphans on upload             | Default: `false` (danger when true)                                        |
| `FETCH_INTERVAL_MINUTES`    | Base interval fallback                      | Default: `30`                                                              |
| `SCHEDULER_TIMEZONE`        | Override schedule TZ if not in `feeds.yaml` | Default: `UTC`                                                             |
| `SCHEDULER_RUN_IMMEDIATELY` | Run once on boot before schedule loop       | Default: `false`                                                           |

## Retention & batching controls

| Variable                  | Purpose                                             | Default |
| ------------------------- | --------------------------------------------------- | ------- |
| `MAX_ITEMS_PER_FEED`      | Per-feed physical retention cap                     | `400`   |
| `SUMMARY_WINDOW_ITEMS`    | Unsummarized items per feed per summarizer pass     | `50`    |
| `BULLETIN_SUMMARY_LIMIT`  | Summaries per HTML bulletin chunk                   | `100`   |
| `BULLETIN_PER_FEED_LIMIT` | Per-feed cap per chunk (auto-reduced if many feeds) | `40`    |
| `BULLETIN_MAX_CHUNKS`     | Backlog chunks processed per run                    | `5`     |

## Merge tuning (SimHash + optional BM25/FTS5)

| Variable                        | Purpose                                                 | Default    |
| ------------------------------- | ------------------------------------------------------- | ---------- |
| `SIMHASH_HAMMING_THRESHOLD`     | Max Hamming distance (0–64); set `0` to disable merging | `24`       |
| `SIMHASH_MERGE_LINKAGE`         | Merge linkage strategy (`complete` or `single`)         | `complete` |
| `STOPWORD_LOCALES`              | Language codes for stopwords (comma-separated)          | `en,pt`    |
| `BM25_MERGE_ENABLED`            | Enable BM25/FTS5 merge fallback                         | `true`     |
| `BM25_MERGE_RATIO_THRESHOLD`    | Minimum mutual BM25 ratio to accept a merge             | `0.80`     |
| `BM25_MERGE_MAX_EXTRA_DISTANCE` | Allow BM25 to merge beyond SimHash threshold            | `16`       |
| `BM25_MERGE_MAX_QUERY_TOKENS`   | Cap tokens used in BM25 queries                         | `8`        |

### Stopword Configuration

The `STOPWORD_LOCALES` setting controls which languages are used for stopword filtering in both:
- **Token overlap guardrails** (merge_policy.py) - prevents false positives in similarity detection
- **SimHash fingerprinting** (simhash.py) - filters common words before computing content fingerprints

Use standard ISO 639-1 language codes (e.g., `en` for English, `pt` for Portuguese, `es` for Spanish, `fr` for French). Multiple locales can be specified as a comma-separated list. The stopwords library supports 50+ languages.

Examples:
```bash
# English and Portuguese (default)
STOPWORD_LOCALES=en,pt

# Add Spanish and French
STOPWORD_LOCALES=en,pt,es,fr

# English only
STOPWORD_LOCALES=en
```

**Note**: Changing stopword locales will affect similarity matching. After changing this setting, consider recomputing SimHash values using `python -m tools.backfill_simhash --db feeds.db` to ensure consistency. For production deployments, the system will automatically recompute SimHash values once upon startup after upgrading to use multilingual stopwords (tracked via the `migration_log` table).

See [MERGE_TUNING.md](MERGE_TUNING.md) for the operational details and diagnostic tools.

## Recurring coverage detection

| Variable                       | Purpose                                 | Default              |
| ------------------------------ | --------------------------------------- | -------------------- |
| `RECURRING_COVERAGE_DAYS_BACK` | Days to look back for recurring stories | `7`                  |
| `RECURRING_COVERAGE_TOPIC`     | Topic name for recurring news items     | `Recurring Coverage` |

When enabled (via `SIMHASH_HAMMING_THRESHOLD` > 0), the publisher checks each summary against ALL previously published summaries from the past `RECURRING_COVERAGE_DAYS_BACK` days across all feeds and groups. Summaries that match past news (using the same SimHash similarity logic) are reassigned to the recurring coverage topic, which appears at the end of bulletins. This helps identify stories that are receiving continued coverage across your entire news aggregation, regardless of which specific bulletin group is being built.

## Telemetry

| Variable                                | Purpose                                  | Notes                 |
| --------------------------------------- | ---------------------------------------- | --------------------- |
| `DISABLE_TELEMETRY`                     | Disable tracing/log export               | Set `true` to opt out |
| `OTEL_SERVICE_NAME`                     | Service name override                    | Optional              |
| `OTEL_ENVIRONMENT`                      | Environment tag                          | Optional              |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | Azure Monitor exporter connection string | Enables export        |

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
