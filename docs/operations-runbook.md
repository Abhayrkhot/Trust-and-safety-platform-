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

Optional tuning settings are `SAFETY_CHECKPOINT_INTERVAL_MS`, `SAFETY_CHECKPOINT_TIMEOUT_MS`, `SAFETY_CHECKPOINT_MIN_PAUSE_MS`, `SAFETY_RESTART_ATTEMPTS`, and `SAFETY_RESTART_DELAY_MS`. Values are parsed and cross-validated at startup. Production mode fails closed without an explicit absolute checkpoint URI; operators must ensure that the selected backend is durable for their deployment.

## Recovery contract

- Checkpoints use exactly-once mode, one concurrent checkpoint, zero tolerated checkpoint failures, and externalized retention on cancellation.
- Stable Flink operator UIDs protect state mapping across compatible job revisions.
- Kafka offsets and Flink keyed state restore from the same checkpoint boundary.
- Redis and ClickHouse remain at-least-once sinks. Redis rejects stale event-time updates atomically; ClickHouse collapses replayed stable signal IDs in `FINAL` reads.
- A successful checkpoint does not make arbitrary application upgrades state-compatible. Validate serializer and operator-UID compatibility before restoring a new revision.

## Controlled failure drill

Set `SAFETY_FAIL_AFTER_EVENTS` to a positive record count only in an approved drill environment. The operator fails only execution attempt zero, so a replacement TaskManager/JVM does not repeat the injected failure. Startup rejects a drill when restart attempts are disabled. Remove the setting after the drill.

Expected evidence is a failed initial execution attempt, a successful restart, and complete unique output. `FailureRecoveryIT` exercises that invariant in the clean gate.

## Observability

The `trust_safety` metric group exports input count/rate, duplicate count, emitted-signal count, event-time lag, and processing-latency histogram data. The Prometheus reporter configuration is in `conf/flink-conf.yaml`. Alert thresholds must be derived from deployment measurements; this repository does not invent production thresholds.

## Incident checks

1. Inspect the most recent completed checkpoint and restart attempt before changing state.
2. Check Kafka consumer lag and watermark idleness independently; low throughput can be legitimate idle input.
3. Check Redis and ClickHouse health separately because either synchronous sink can backpressure the job.
4. Preserve the checkpoint/savepoint and raw event sample before changing rules or replaying data.
5. Record any benchmark or recovery result with revision, dataset, environment, and raw output in the evidence ledger.
