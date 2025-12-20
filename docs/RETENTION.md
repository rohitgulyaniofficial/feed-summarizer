# Age Window & Retention

Three complementary controls govern how long items stick around and which are summarized:

1. Time Window (`feeds.yaml`: `thresholds.time_window_hours`)
   - Unsummarized items older than this window are ignored when building prompts.
   - Default: 48h. Raise temporarily for historical backfill.

2. Count-based physical retention (`MAX_ITEMS_PER_FEED`)
   - After each fetch, the newest N items per feed are kept (default 400).
   - Older items beyond that per-feed cap are pruned.
   - This prevents date-less feeds from re-surfacing old entries as “new” after day-based purges.

3. Summary window (`SUMMARY_WINDOW_ITEMS`)
   - At most the newest N unsummarized items per feed are considered in a single summarizer pass (default 50).
   - Larger backlogs are processed gradually across runs.

Optional long-term aging still applies via `ENTRY_EXPIRATION_DAYS` (default 365, fetcher maintenance) to trim truly old data if you run this for months.

## `feeds.yaml` thresholds

```yaml
thresholds:
  time_window_hours: 48
  retention_days: 7
  initial_fetch_items: 10
```

- `thresholds.retention_days` gates how many days of bulletin sessions are emitted in the summary RSS feeds and indexes.
- `thresholds.initial_fetch_items` bootstraps brand-new feeds by allowing up to N most recent entries even if outside the time window (set `0` to disable).

## Operational guidance

- If some feeds are extremely high volume, lower `MAX_ITEMS_PER_FEED` (e.g. 200) for faster turnover.
- To accelerate clearing a backlog, temporarily raise `SUMMARY_WINDOW_ITEMS` (e.g. to 80) then revert to keep prompts smaller.
- For historical bulk summarization, raise `time_window_hours` first; the count cap ensures the DB won’t grow without bound.
- High-fanout bulletin groups can drain backlog fairly via `BULLETIN_SUMMARY_LIMIT`, `BULLETIN_PER_FEED_LIMIT`, and `BULLETIN_MAX_CHUNKS`.

## Edge cases

- Feeds with no reliable pub dates fall back to ingestion timestamp; count-based retention ensures they stay recorded and don’t churn.
- If Azure content filtering splits batches, the bisect logic still respects the summary window.
