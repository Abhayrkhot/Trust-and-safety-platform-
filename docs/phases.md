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

## Later phases (not yet claimed)

## Phase 4 — Recovery and observability

- Opt-in deterministic fail-once injection and an embedded recovery integration test
- Prometheus reporter configuration
- Throughput, duplicates, risk-signal, event-time-lag, and processing-latency distribution metrics

## Phase 5 — Load, backpressure, and soak

- Slow-sink backpressure test with an exact completion oracle
- Time-bounded 20,000-unique-event soak with duplicates and out-of-order arrivals
- Repeatable local benchmark command that emits machine context and raw results
- GitHub Actions clean verification on every PR
