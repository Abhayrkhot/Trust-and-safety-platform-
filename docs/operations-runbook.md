# Operations Runbook

## Preflight

1. Run `./verify-all.sh` on the exact revision being deployed.
2. Publish `conf/safety-rules.json` through the normal configuration release process.
3. Confirm Kafka topic, Redis, ClickHouse, and Prometheus connectivity from the Flink runtime.
4. Confirm the checkpoint filesystem plugin and credentials for the configured URI.

## Required production settings

- `SAFETY_ENVIRONMENT=production`
- `SAFETY_CHECKPOINT_URI`: absolute durable URI such as an installation-supported S3 or HDFS path
- `SAFETY_RULES_PATH`: mounted strict JSON rule configuration
- `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`
- `SAFETY_QUARANTINE_TOPIC`: dedicated restricted-access topic; defaults to `<input-topic>.quarantine`

Optional tuning settings are `SAFETY_CHECKPOINT_INTERVAL_MS`, `SAFETY_CHECKPOINT_TIMEOUT_MS`, `SAFETY_CHECKPOINT_MIN_PAUSE_MS`, `SAFETY_RESTART_ATTEMPTS`, `SAFETY_RESTART_DELAY_MS`, and `SAFETY_MAX_HISTORY_EVENTS_PER_ACTOR`. Values are parsed and cross-validated at startup. The history cap defaults to 100,000 events per actor and must be at least every configured rule's `minimum_events`; tune it from measured per-key rates and window sizes, not from the default alone. Production mode fails closed without an explicit absolute checkpoint URI; operators must ensure that the selected backend is durable for their deployment.

## Recovery contract

- Checkpoints use exactly-once mode, one concurrent checkpoint, zero tolerated checkpoint failures, and externalized retention on cancellation.
- Stable Flink operator UIDs protect state mapping across compatible job revisions.
- Kafka offsets and Flink keyed state restore from the same checkpoint boundary.
- Redis, ClickHouse, and Kafka quarantine remain at-least-once sinks. Redis rejects stale event-time updates atomically; ClickHouse collapses replayed stable signal IDs in `FINAL` reads; quarantine replays keep the same source-coordinate key.
- A successful checkpoint does not make arbitrary application upgrades state-compatible. Validate serializer and operator-UID compatibility before restoring a new revision.

## Controlled failure drill

Set `SAFETY_FAIL_AFTER_EVENTS` to a positive record count only in an approved drill environment. The operator fails only execution attempt zero, so a replacement TaskManager/JVM does not repeat the injected failure. Startup rejects a drill when restart attempts are disabled. Remove the setting after the drill.

Expected evidence is a failed initial execution attempt, a successful restart, at least one completed checkpoint, and complete unique output. `FailureRecoveryIT` exercises the focused restart invariant; `EndToEndPipelineIT` injects the same attempt-aware failure into a real Kafka workload and asserts post-restart convergence across Redis, ClickHouse, and Kafka quarantine.

## Observability

The `trust_safety` metric group exports accepted/quarantined ingestion counts, input count/rate, duplicate count, emitted-signal count, event-time lag, processing-latency histogram data, per-actor history-size samples, expired-history counts, beyond-retention late events, and state-capacity breaches/evictions. A capacity breach emits the replay-stable `__state_capacity__` risk signal at score 100 because configured rules may undercount after eviction; operators must investigate instead of treating it as a normal policy match. Event-time timers reclaim idle-key history after the longest rule window. Deduplication state remains time-bounded by its processing-time TTL rather than count-bounded. The Prometheus reporter configuration is in `conf/flink-conf.yaml`. Alert thresholds must be derived from deployment measurements; this repository does not invent production thresholds.

## Quarantine response

1. Restrict topic access because payload previews may contain sensitive producer data.
2. Group records by `failure_reason`, producer identity, and source coordinate; never replay quarantine JSON directly into the input topic.
3. Decode and correct the original payload offline, assign a new event ID when semantics changed, and publish through the normal validated producer path.
4. Use `payload_sha256`, `original_payload_bytes`, and `payload_truncated` to distinguish identical poison inputs and determine whether the bounded preview is complete.
5. Alert on a measured quarantine-rate baseline, not a threshold invented in this repository.

## Incident checks

1. Inspect the most recent completed checkpoint and restart attempt before changing state.
2. Check Kafka consumer lag and watermark idleness independently; low throughput can be legitimate idle input.
3. Check Redis and ClickHouse health separately because either synchronous sink can backpressure the job.
4. Check quarantine-topic write health and `quarantined_records_total`; a blocked quarantine sink backpressures ingestion by design so poison records are not silently discarded.
5. Preserve the checkpoint/savepoint and raw event sample before changing rules or replaying data.
6. Record any benchmark or recovery result with revision, dataset, environment, and raw output in the evidence ledger.
