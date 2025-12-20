# Dedupe & Merge Tuning (SimHash + optional BM25/FTS5)

This project attempts to merge near-duplicate summaries when generating bulletins/feeds.

## What gets merged (scope)

- Merging happens during publishing over the in-memory set of summaries being rendered.
- In the default flow this is scoped to the current bulletin/session being built (not global cross-session dedupe).
- Topic is not a merge veto: a mis-filed topic can still merge if the text strongly matches.

## Primary signal: SimHash

- Each summary can carry a dedicated merge fingerprint in SQLite as `summaries.merge_simhash`.
- The merge fingerprint is computed over a stable input: `title + "\n" + summary_text`.
- Two summaries are candidates when their fingerprints are within `SIMHASH_HAMMING_THRESHOLD` bits.

Operational knobs:

- `SIMHASH_HAMMING_THRESHOLD` (0–64): set `0` to disable merging.
- `SIMHASH_MERGE_LINKAGE`: `complete` (more conservative) vs `single` (can over-merge transitively).

## Secondary signal: BM25/FTS5 fallback (optional)

When enabled, SQLite FTS5 BM25 scoring can act as a bounded fallback to merge textually identical stories that fall outside the strict SimHash threshold.

- BM25 only applies if FTS is available and the `summary_fts` table exists.
- On older databases, `summary_fts` may exist but be empty; run the backfill tool once.

Knobs:

- `BM25_MERGE_ENABLED`
- `BM25_MERGE_RATIO_THRESHOLD`
- `BM25_MERGE_MAX_EXTRA_DISTANCE`
- `BM25_MERGE_MAX_QUERY_TOKENS`

## Hashed cosine (optional confirmation gate)

If enabled, merges require cosine similarity above the configured minimum in addition to the SimHash/BM25 criteria.

- `HASHED_COSINE_ENABLED`
- `HASHED_COSINE_MIN_SIM`
- `HASHED_COSINE_MAX_TOKENS`
- `HASHED_COSINE_BUCKETS`

## Diagnostics

- `tools/merge_report.py` prints both SimHash and BM25 decisions (when enabled) for a keyword across recent bulletins.
  - Example: `python3 tools/merge_report.py --hours 96 --threshold 24 --query Cloudflare`

## FTS backfill (one-time, for existing DBs)

- `tools/fts_backfill.py` can populate `summary_fts` from historical rows so BM25 works immediately.
  - Example: `python3 tools/fts_backfill.py --db /data/feeds.db`
