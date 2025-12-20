# Running

## CLI modes

Mode | Command | What it does
-----|---------|-------------
One-shot full pipeline | `python main.py run` | Fetch → Summarize → Publish (HTML+RSS+passthrough) → Azure upload (if configured)
Scheduled (smart) | `python main.py scheduled` | Run continuously at times declared under `schedule:` in `feeds.yaml`
Show schedule | `python main.py schedule-status` | Print parsed schedule (with timezone)
Status snapshot | `python main.py status` | DB counts + output presence
Fetch only | `python main.py fetcher` | Just ingest feeds (no summarization/publish)
Summarize only | `python main.py summarizer` | Summarize new items (no publish)
Upload existing output | `python main.py upload` | Sync current `public/` tree to Azure only

## Useful flags

- `--no-publish` (with `run`) skip HTML/RSS generation.
- `--no-azure` disable Azure upload for that invocation.
- `--sync-delete` remove remote blobs not present locally (use cautiously).

## Scheduling

```yaml
schedule:
  timezone: Europe/Lisbon
  times:
    - "06:30"
    - "12:30"
    - "20:30"
```

If both `schedule.timezone` and `SCHEDULER_TIMEZONE` are set, the environment variable wins.
