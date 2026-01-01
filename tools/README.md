# Tools Directory

Utility scripts for maintenance, analysis, and reporting. Run any script via `python -m tools.<script>` from the repo root.

## Conventions

- **Arguments**: use `--db` for database path (default `feeds.db`), `--verbose/-v` for debug output, `--quiet/-q` to suppress non-error output. Time windows use `--days` or `--hours`; merge thresholds use `--threshold`.
- **Outputs**: scripts print plain text tables or progress logs to stdout; errors go to stderr and exit non-zero. Backfill/recompute tools log progress per batch.
- **Safety**: recompute/backfill tools are idempotent; `backfill_simhash` supports `--dry-run`.

## Analysis Tools (`report_*`)

- `report_merge.py`: simulate publisher merge logic (SimHash + guardrails, optional BM25) over recent bulletins; supports keyword queries and clustering views.
- `report_threshold.py`: comprehensive threshold analysis with parallel processing and rich UI; suggests SimHash thresholds across risk profiles.
- `report_recurring.py`: simulate recurring coverage detection; tests how different thresholds perform across bulletins and time periods.

## Migration/Backfill Tools (`backfill_*`)

- `backfill_simhash.py`: recompute `merge_simhash` with current multilingual SimHash; optional `--dry-run`.
- `backfill_fts.py`: create/populate `summary_fts` for BM25 fallback (best-effort FTS5).
- `backfill_bulletins.py`: backfill `bulletin_entries` for legacy bulletins.

## Visualization Tools (`render_*`)

- `render_charts.py`: render status feed charts to SVG files.

## Shared Modules

- `common.py`: shared helpers for DB validation, logging setup, table printing, and progress tracking.
- `standard_args.py`: shared argparse helpers for db/time/threshold/verbosity/output/samples; use in new tools.
- `merge_env.py`: shared environment helpers (HASHED_COSINE\*, BM25 toggles) used by report_merge.
- `data_loaders.py`: shared SQL queries for loading summaries and bulletins.

## Recommendations for new/updated tools

- Reuse `standard_args` for common flags and `common.setup_script_logging` for logging levels.
- Emit concise tables (see `common.print_table`) or short JSON/CSV when needed; keep stdout for data and stderr for errors.
- Validate DB path early and fail fast with clear messaging.
- Import functions from `utils/` rather than reimplementing logic (e.g., use `title_token_set_from_text` not `.split()`).
