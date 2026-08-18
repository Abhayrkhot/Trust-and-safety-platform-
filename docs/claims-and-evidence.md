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
## Verification runs

- 2026-08-18 Phase 1: `./verify-all.sh` — PASS in the full Phase 1–2 workspace. Phase-specific branch verification is recorded in its PR. This is correctness evidence only, not a performance measurement.
- 2026-08-18 Phase 2 development gate: `./verify-all.sh` — PASS; 11 tests, 0 failures, 0 errors, 0 skipped after Phase 2 test expansion. Final isolated branch result is recorded in its PR.
- 2026-08-18 Phase 3: `./verify-all.sh` — PASS; 13 unit tests plus 2 real-container integration tests, 0 failures, 0 errors, 0 skipped; Redis 8.2 and ClickHouse 25.8 containers. No performance measurement.

## Baseline (2026-08-18)

The workspace was empty and was not a Git repository. There was no existing build or test suite to run. Maven 3.9.16 and Docker 28.1.1 were present; Maven ran on JDK 26.0.2. No performance measurements have been made.

## Measurements

None. Performance numbers must include the command, source revision, hardware, dataset, warm-up, sample count, and raw output.
