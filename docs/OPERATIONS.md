# Operations, correctness and acceptance

## Required deployment checks

1. Send 1,000 known events plus exact-ID duplicates; verify silver unique IDs and no double-counted revenue.
2. Send malformed JSON, missing event IDs, negative amounts, non-USD currency and timestamps >5 minutes ahead of ingestion. Verify quarantine and reason.
3. Send an event one minute late while the stream continues. Verify inclusion. Send events older than the watermark and inspect Spark dropped-by-watermark metrics. The local reference test is not evidence of streaming lateness behavior.
4. Send view/cart/checkout/purchase in arrival order different from event-time order. Verify final funnel stage 4. Send purchase-before-view and verify it does not count as an ordered conversion.
5. Verify session split at a >=30-minute gap, closure after watermark, and that product updates continue while sessions remain unfinalized.
6. Kill and restart each stream with the SAME checkpoint. Verify aggregates and counts do not grow from replay.
7. Interrupt export before and after MERGE; verify cursor and target commit together. Confirm no overlapping export jobs.
8. Modify a product window with a within-watermark late event. Confirm CDF export and dbt incremental update replace the old count, rather than add it.
9. Test analyst permissions: no bronze/silver raw access, email masked in explicitly granted contact lookup, gold readable. Snowflake reader cannot write and sees no raw email/user identifiers.
10. Open Tableau template, refresh against Snowflake, reconcile totals to dbt, publish and verify as a reader.

## Failure handling

| Symptom | Action |
|---|---|
| Producer throttled | Inspect per-shard writes, increase shards or reduce rate; producer retries only failed entries and exits on exhausted retries |
| Kinesis lag | Inspect iterator age, Spark input/processed rates, CPU, state memory and partition skew |
| Stateful query OOM | Find very active users, partition imbalance and long sessions; reduce synthetic rate before tuning cluster/state store |
| Late records dropped | Inspect watermark and clock quality; widen watermark only with planned state/checkpoint migration |
| Gold table missing | Start upstream jobs in order and wait for first session finalization before enabling exports |
| CDF version expired | Stop exports; archive cursor, rebuild target from a fixed Delta snapshot and reset cursor together; do not silently skip missing versions |
| Job schema changes | Version event contract; plan state migration and replay from bronze into new checkpoint/table names |
| Raw data expired | Kinesis retention is 48h; bronze retention must cover recovery needs; do not VACUUM away needed CDF history |
| Staging table left over | Failed stage write can leave `RAW.STAGE_*`; inspect active jobs before dropping orphaned tables |
| dbt source stale | Check producer, stream, export and scheduler; idle demo traffic also triggers freshness alerts |

Deletes are not propagated by this export. To remove data: stop writers/export, perform a controlled gold and Snowflake rebuild, recreate cursor at matching Delta version, full-refresh dbt and resume. This is a demo with no real personal data; a production retention/deletion policy must include bronze, quarantine, checkpoints, contact lookup, Snowflake, backups and Tableau extracts.

## Reproducible 5K/sec benchmark

Warm the system at 100/sec, then 1K/sec; run 5K/sec for at least five minutes. Save producer output and CloudWatch screenshots for IncomingRecords, IncomingBytes, throttles and iterator age. Save Spark progress JSON (`numInputRows`, `processedRowsPerSecond`, `stateOperators`, event-time watermark), cluster size/runtime, shard count, payload bytes, partition distribution and Snowflake export timing. Reconcile sent acknowledgements with bronze and deduplicated silver records over a complete interval. Report p50/p95 ingestion-to-gold and gold-to-dashboard latency separately from session finalization latency. Check all throttles/retries and compute total cost.

No benchmark results are included because this environment has not deployed the cloud pipeline.
