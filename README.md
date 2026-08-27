# Real-Time Trust & Safety Event Processing

An evidence-first reference platform for processing versioned safety events with Kafka and Apache Flink. Current implementation covers Phases 1–2: strict JSON ingestion, event-time watermarks, keyed TTL deduplication, rolling actor aggregates, and configurable risk rules.

No throughput or latency claims are made yet. See [docs/claims-and-evidence.md](docs/claims-and-evidence.md).

## Verify

Requires JDK 17+ and Maven 3.9+.

```bash
./verify-all.sh
```

## Run

```bash
mvn package
java -cp target/safety-stream-0.1.0-SNAPSHOT.jar dev.trustsafety.SafetyStreamJob \
  localhost:9092 safety-events safety-platform-v1 redis://localhost:6379 \
  jdbc:clickhouse:http://localhost:8123/default
```
