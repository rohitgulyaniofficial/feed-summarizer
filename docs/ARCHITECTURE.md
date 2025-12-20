# Architecture (High-Level)

This page contains the longer-form architecture notes that were previously in the top-level README.

## Modules

Module | Responsibility
-------|----------------
`workers/fetcher/` | Async retrieval, conditional GET headers, reader mode, error/backoff tracking.
`workers/summarizer/` | Batches new items, calls Azure OpenAI, retry & bisection for filtered content.
`workers/publisher/` | HTML bulletin + RSS feed generation, passthrough feeds, Azure upload, index pages.
`workers/scheduler/` | Time‑zone aware smart scheduling + status reporting.
`models/` | Async database queue (SQLite WAL) + safe operation batching.
`config.py` | Centralized env/YAML/secrets loading + validation + normalization.
`services/telemetry.py` | OpenTelemetry initialization & instrumentation (`aiohttp`, sqlite, logging spans).
`main.py` | Orchestrator & CLI entry point; composes pipeline steps.

## Processing flow

```text
feeds.yaml -> fetcher -> items (SQLite) -> summarizer -> summaries -> publisher -> public/{bulletins,feeds}
                               ^                                           |
                               |_______ backoff + error counts ____________|
```
