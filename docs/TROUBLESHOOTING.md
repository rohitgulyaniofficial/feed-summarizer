# Troubleshooting

Symptom | Cause | Fix
--------|-------|----
Empty summaries | Missing / bad Azure config | Check endpoint host (no scheme), key, deployment
Few bulletin items | Items filtered / no new content | Verify fetcher logs & summary successes
Broken feed links | Wrong `RSS_BASE_URL` | Set correct public domain before publishing
Slow summarization | Rate limit / large content | Adjust `SUMMARIZER_REQUESTS_PER_MINUTE` / enable reader mode selectively
Missing Azure upload | Vars unset | Provide storage account + key or run without upload
Telemetry missing | Disabled or no exporter | Remove `DISABLE_TELEMETRY` / set connection string
Empty summaries with token usage | New structured response format returned parts list | Ensure you pulled the latest `services/llm_client.py` parser
