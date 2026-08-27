# Delivery Phases

## Phase 1 — Contracts and ingestion

- Versioned safety-event schema and strict JSON codec
- Kafka source with committed offsets and strict malformed-record rejection
- Validation tests for required fields, enum values, versions, and timestamps

## Phase 2 — Stateful event-time evaluation

- Bounded-out-of-orderness watermarks with idle partitions
- Keyed TTL `event_id` deduplication
- Rolling actor event/severity counts
- Configurable rules producing explainable risk signals
- Checkpoint configuration and deterministic operator tests

## Phase 3 — Serving stores

- Redis hot state rejects stale event-time updates and applies TTL atomically
- ClickHouse historical storage uses stable signal IDs for replay deduplication
- Container-backed integration tests exercise both real databases

## Phase 4 — Recovery and observability

- Opt-in deterministic fail-once injection and an embedded recovery integration test
- Prometheus reporter configuration
- Throughput, duplicates, risk-signal, event-time-lag, and processing-latency distribution metrics

## Phase 5 — Load, backpressure, and soak

- Slow-sink backpressure test with an exact completion oracle
- Time-bounded 20,000-unique-event soak with duplicates and out-of-order arrivals
- Repeatable local benchmark command that emits machine context and raw results
- GitHub Actions clean verification on every PR

## Phase 6 — Full infrastructure path

- Finite-offset Kafka source drives the real Flink evaluation graph
- The same run asserts exact Redis hot state and ClickHouse historical rows
- Duplicate Kafka event IDs are verified across the complete path

## Phase 7 — Schema evolution and policy configuration

- Strict V1 and V2 JSON schemas; V1 normalizes into the current internal model
- V2 adds required tenant identity and optional trace correlation without breaking V1 producers
- Rules load from validated JSON and support event-type plus attribute predicates

## Phase 8 — Quality and security gates

- Deterministic Java/config/document formatting enforced in `verify`
- Maximum-effort SpotBugs analysis
- Combined unit and integration line-coverage minimum
- Runnable shaded application JAR with verified entry point
- GitHub dependency-review and CodeQL workflows

## Phase 9 — Operational recovery hardening

- Validated runtime configuration with production fail-closed checkpoint storage
- Durable externalized-checkpoint settings and bounded restart policy
- Execution-attempt-aware failure injection that survives JVM replacement
- Recovery and incident-response runbook with explicit delivery semantics

## Phase 10 — Supply-chain hardening

- Patched direct dependencies with Jackson family alignment through its BOM
- Verified CycloneDX application SBOM containing direct and transitive runtime dependencies
- Weekly automated Maven and GitHub Actions dependency update PRs
- GitHub vulnerability alerts and moderate-or-higher dependency-review enforcement

## Phase 11 — Poison-event quarantine

- Typed classification for null, malformed, unsupported-version, and contract-invalid Kafka records
- Versioned quarantine schema with stable source-coordinate identity and Kafka origin metadata
- Bounded payload preview plus complete-payload SHA-256 evidence
- At-least-once Kafka quarantine sink and accepted/quarantined Prometheus counters
- Exact real-Kafka integration assertions across quarantine, Redis, and ClickHouse

## Phase 12 — Full-infrastructure recovery and replay

- Real Kafka workload with 3,002 valid records and 6 poison records
- Deterministic failure on initial execution attempt after valid record 1,500
- Local filesystem checkpoints with an asserted successful-checkpoint accumulator
- Exact post-restart convergence assertions in Redis and ClickHouse
- Exact distinct quarantine identity assertions with honest at-least-once duplicate handling

## Phase 13 — Compiler and API-drift hygiene

- Java compiler warnings fail both production and test compilation
- Deprecated Flink and Jackson APIs migrated to their supported replacements
- Test harnesses close explicitly so interrupt-capable cleanup is visible
- Formatting recursively covers Java, Markdown, JSON, and YAML while excluding generated output

## Phase 14 — Bounded keyed-history lifecycle

- Configurable per-actor rolling-history cap cross-validated against rule thresholds
- Deterministic oldest-event-time eviction under out-of-order arrival
- Replay-stable operational risk signal and dedicated capacity metrics on breach
- Event-time timers reclaim idle history and survive checkpoint/restore
- Explicit telemetry for expired history and arrivals beyond the retention horizon

## Phase 15 — Kafka consumer-offset lag evidence

- Standard Flink Kafka-source `pendingRecords` gauge exported through the Prometheus reporter
- Real Kafka contract test proves exact nonzero lag behind the head offset
- Drain assertion proves the same live connector gauge returns to zero
- Operations guidance separates offset lag, event-time lag, and watermark idleness

## Phase 16 — Multi-topic concurrent streams

- Validated comma-separated Kafka topic configuration with no blank or duplicate topics
- Content, activity, and moderation topics consumed by one parallel Kafka source
- Actor-keyed rule state combines events across topic/partition boundaries
- Exact Redis and ClickHouse assertions for the cross-stream risk signal
