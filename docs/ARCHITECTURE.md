# Architecture (High-Level)

This page contains the longer-form architecture notes that were previously in the top-level README.

## Modules

Module | Responsibility
-------|----------------
`workers/fetcher/` | Async retrieval, conditional GET headers, reader mode, error/backoff tracking.
`workers/summarizer/` | Batches new items, calls Azure OpenAI, retry & bisection for filtered content.
`workers/publisher/` | HTML bulletin + RSS feed generation, passthrough feeds, Azure upload, index pages.
`workers/scheduler/` | Time‑zone aware smart scheduling + status reporting.
`models/` | Async database queue (SQLite WAL) + safe operation batching + schema migrations.
`config.py` | Centralized env/YAML/secrets loading + validation + normalization.
`services/telemetry.py` | OpenTelemetry initialization & instrumentation (`aiohttp`, sqlite, logging spans).
`main.py` | Orchestrator & CLI entry point; composes pipeline steps.

## Processing flow

```text
feeds.yaml -> fetcher -> items (SQLite) -> summarizer -> summaries -> publisher -> public/{bulletins,feeds}
                               ^                                           |
                               |_______ backoff + error counts ____________|
```

## Database migrations

The system includes an automatic migration framework that runs on startup to upgrade the database schema and data as needed. Migrations are tracked in the `migration_log` table to ensure idempotency.

**Current migrations:**

1. **Column additions**: `published_date`, `simhash`, `merge_simhash` columns added to `summaries` table
2. **Indexes**: `idx_summaries_simhash`, `idx_summaries_merge_simhash` for performance
3. **FTS5 table**: `summary_fts` for full-text search and BM25 matching
4. **Bulletins tables**: `bulletins`, `bulletin_summaries`, `bulletin_entries` for persistence
5. **SimHash recomputation**: One-time migration (`recompute_merge_simhash_multilingual`) that updates all `merge_simhash` values to use the new multilingual stopwords from stopwordsiso library

The SimHash recomputation migration runs automatically on the first startup after upgrading to multilingual stopwords support. It only runs once and is skipped on subsequent startups. This ensures older deployments automatically upgrade their similarity fingerprints without manual intervention.
