# Publishing Outputs

## Output paths

Path | Description
----|-------------
`public/bulletins/*.html` | Per-group HTML bulletins (recent sessions, optional AI intro)
`public/feeds/*.xml` | Per-group summarized RSS feeds
`public/feeds/raw/*.xml` | Raw passthrough feeds (only for slugs listed under `passthrough:`)
`public/index.html` | Landing page / directory index

## Retention & grouping

- Summary groups come from `summaries:` in `feeds.yaml`; each group renders one HTML bulletin (latest chunk) plus an RSS feed keyed to bulletin sessions stored in SQLite.
- The publisher drains the newest unpublished summaries per group, merges near-duplicates, and chunks backlog work with `BULLETIN_SUMMARY_LIMIT`, `BULLETIN_PER_FEED_LIMIT`, and `BULLETIN_MAX_CHUNKS` so noisy feeds cannot starve others.
- `thresholds.retention_days` (default 7) gates how many days of bulletin sessions are kept in RSS outputs and indexes; older sessions fall out of the feeds even though their rows stay in the database.

For deeper tuning, see RETENTION.md and MERGE_TUNING.md.

## Azure Upload

- Set `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_KEY`, and optionally `AZURE_STORAGE_CONTAINER` (default `$web` for static sites); `AZURE_UPLOAD_SYNC_DELETE=true` or `--sync-delete` removes remote files missing locally.
- With storage configured, `python main.py run` uploads after publishing unless `--no-azure` is set; `python main.py upload` runs an upload-only sync of the current `PUBLIC_DIR` tree.
- The uploader caches blob metadata and skips unchanged files using MD5/size/mtime comparisons; uploads land under `feeds/`, `bulletins/`, and `index.html` inside the target container.

## Preview status charts locally

Render the current status feed charts to PNG files for quick visual tweaks:

```bash
python tools/render_status_charts.py --output-dir /tmp/status_charts
```

By default charts are written under `PUBLIC_DIR/feeds/status_charts`.
