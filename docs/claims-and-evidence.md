# Claims and Evidence

This file is the source of truth for public claims. A claim stays unverified until its named command succeeds in a clean checkout.

| Claim | Evidence | Status |
|---|---|---|
| V1 safety events are strictly validated and unknown schema versions are rejected. | `SafetyEventJsonTest`; `./verify-all.sh` | Verified 2026-08-18 |
| Kafka records use the strict V1 decoder; malformed records fail deserialization and therefore are not acknowledged. | `SafetyEventJsonTest`; `./verify-all.sh` | Verified 2026-08-18 |
| Event-time processing uses bounded out-of-orderness and idleness detection. | `SafetyStreamJobTest`; `./verify-all.sh` | Verified 2026-08-18 |
| Duplicate `event_id` values are suppressed per actor with state TTL and restored from checkpoints. | `SafetyProcessorTest`; `./verify-all.sh` | Verified 2026-08-18 |
| Rules are configuration-driven and emit explainable risk signals from keyed rolling state without losing newer history on late arrivals. | `SafetyProcessorTest`; `./verify-all.sh` | Verified 2026-08-18 |
| Redis hot state atomically preserves the newest event-time signal, is replay-idempotent, and has a configured TTL. | `RedisHotStateStoreIT`; `./verify-all.sh` | Verified 2026-08-18 |
| ClickHouse persists historical risk signals and collapses replayed stable signal IDs in `FINAL` reads. | `ClickHouseHistoricalStoreIT`; `./verify-all.sh` | Verified 2026-08-18 |
| Both external sinks provide at-least-once delivery under Flink replay; no exactly-once sink claim is made. | `RiskSignalSinkTest`, sink implementation; `./verify-all.sh` | Verified 2026-08-18 |
| An opt-in fail-once operator causes a real local Flink restart and the bounded job completes with all 100 unique records. | `FailureRecoveryIT`; `./verify-all.sh` | Verified 2026-08-18 |
| Flink metrics expose input throughput/count, duplicates, emitted signals, event-time lag, and processing-latency histogram quantiles suitable for Prometheus export. | `SafetyMetricsTest`, `conf/flink-conf.yaml`; `./verify-all.sh` | Verified 2026-08-18 |
| A serial 1 ms/event sink applies measurable backpressure and the job still drains exactly 1,000 unique signals. | `BackpressureIT`; `./verify-all.sh` | Verified 2026-08-18 |
| A 22,000-input soak (20,000 unique plus 2,000 duplicates, with deterministic out-of-order arrivals) completes within 30 seconds and produces exactly 20,000 unique signals. | `SoakIT`; `./verify-all.sh` | Verified 2026-08-18 |
| The embedded, in-memory-source/discard-sink load harness processed 50,000 events in 1.383560 seconds (36,138.65 events/s) on the recorded local machine. This is not Kafka-to-database throughput. | `scripts/run-local-load-benchmark.sh 50000`; `docs/results/local-load-2026-08-18.json` | Measured 2026-08-18 |
| A finite Kafka stream runs through the production Flink graph and writes one deduplicated risk signal to both Redis and ClickHouse with matching rule/count fields. | `EndToEndPipelineIT`; `./verify-all.sh` | Verified 2026-08-18 |
| Strict V1 and V2 schemas coexist; V1 events normalize default tenant/trace values while V2 requires tenant identity and supports trace correlation. | schema files, `SafetyEventJsonTest`, `SchemaContractTest`; `./verify-all.sh` | Verified 2026-08-18 |
| Safety rules load from strict external JSON, reject duplicate/unknown/invalid configuration, and filter by event type and attributes. | `RuleConfigLoaderTest`, `SafetyProcessorTest`; `./verify-all.sh` | Verified 2026-08-18 |
## Verification runs

- 2026-08-18 Phase 1: `./verify-all.sh` — PASS in the full Phase 1–2 workspace. Phase-specific branch verification is recorded in its PR. This is correctness evidence only, not a performance measurement.
- 2026-08-18 Phase 2 development gate: `./verify-all.sh` — PASS; 11 tests, 0 failures, 0 errors, 0 skipped after Phase 2 test expansion. Final isolated branch result is recorded in its PR.
- 2026-08-18 Phase 3: `./verify-all.sh` — PASS; 13 unit tests plus 2 real-container integration tests, 0 failures, 0 errors, 0 skipped; Redis 8.2 and ClickHouse 25.8 containers. No performance measurement.
- 2026-08-18 Phase 4: `./verify-all.sh` — PASS; 16 unit tests plus 3 integration tests, 0 failures, 0 errors, 0 skipped; includes an embedded Flink fail/restart/completion run and both database containers. No performance measurement.
- 2026-08-18 Phase 5: `./verify-all.sh` — PASS; 17 unit tests plus 5 integration tests, 0 failures, 0 errors, 0 skipped. Soak completed in 1.349 seconds and slow-sink test in 1.770 seconds in this run; these durations are test evidence, not production performance claims.
- 2026-08-18 local load harness: 50,000 events, 500 actors, parallelism 4, 1.383560 seconds, 36,138.65 events/s; Java 26.0.2, macOS arm64, 10 available processors. Embedded collection source and discard sink only.
- 2026-08-18 Phase 6: `./verify-all.sh` — PASS; 17 unit tests plus 6 integration tests, 0 failures, 0 errors, 0 skipped. The end-to-end test used Apache Kafka 3.8, Redis 8.2, ClickHouse 25.8, the production Flink evaluation graph, and exact assertions in both stores.
- 2026-08-18 Phase 7: `./verify-all.sh` — PASS; 25 unit/contract tests plus 6 integration tests, 0 failures, 0 errors, 0 skipped. Published Draft 2020-12 V1/V2 schemas are validated by an independent JSON Schema engine and cross-version rejection tests.

## Baseline (2026-08-18)

The workspace was empty and was not a Git repository. There was no existing build or test suite to run. Maven 3.9.16 and Docker 28.1.1 were present; Maven ran on JDK 26.0.2. No performance measurements have been made.

## Measurements

None. Performance numbers must include the command, source revision, hardware, dataset, warm-up, sample count, and raw output.
