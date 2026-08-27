# Real-Time Trust & Safety Event Processing

An evidence-first Java platform that consumes versioned safety events from Kafka, evaluates stateful rules in Apache Flink, serves current risk state from Redis, and stores replay-safe history in ClickHouse.

```text
Kafka -> strict V1/V2 decode -> event-time watermarks -> keyed TTL dedup
      -> rolling actor state -> configurable safety rules -> risk signals
      -> Redis hot state + ClickHouse history + Prometheus metrics
```

The implementation includes checkpoint restore tests, a real Kafka-to-databases integration path, deterministic failure injection, load/backpressure/soak gates, strict schema contracts, and security/coverage CI. Every public claim is tied to reproducible evidence in [docs/claims-and-evidence.md](docs/claims-and-evidence.md).

Delivery semantics are explicit: Flink state and Kafka offsets use exactly-once checkpoints; the synchronous external sinks are at-least-once and make replay idempotent with stable signal IDs and event-time ordering. No end-to-end exactly-once or production throughput claim is made.

## Verify

Requires JDK 17, Maven 3.9+, Docker, and enough local resources for Kafka, Redis, and ClickHouse test containers.

```bash
./verify-all.sh
```

## Run

```bash
mvn package
export SAFETY_ENVIRONMENT=local
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD='your-password'
export SAFETY_RULES_PATH=conf/safety-rules.json
java -jar target/safety-stream-0.1.0-SNAPSHOT-app.jar \
  localhost:9092 safety-events safety-platform-v1 redis://localhost:6379 \
  jdbc:clickhouse:http://localhost:8123/default
```

For non-local runs, set `SAFETY_ENVIRONMENT=production` and an absolute `SAFETY_CHECKPOINT_URI` backed by storage supported by the Flink installation. Production mode refuses to start without that URI. Checkpoint intervals, timeouts, restart limits, and the opt-in failure drill are validated before the job graph starts; see [docs/operations-runbook.md](docs/operations-runbook.md).
