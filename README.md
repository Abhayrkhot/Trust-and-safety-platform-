# Real-Time Trust & Safety Event Processing

An evidence-first Java platform that consumes versioned safety events from Kafka, evaluates stateful rules in Apache Flink, serves current risk state from Redis, and stores replay-safe history in ClickHouse.

```text
Kafka -> strict V1/V2 decode -> valid -> event-time watermarks -> keyed TTL dedup
      -> rolling actor state -> configurable safety rules -> risk signals
      -> Redis hot state + ClickHouse history + Prometheus metrics
                              \-> poison -> versioned Kafka quarantine topic
```

The implementation includes checkpoint restore tests, a real Kafka-to-databases recovery/replay drill, deterministic failure injection, verified Kafka consumer-offset lag, a two-TaskManager execution-placement proof, load/backpressure/soak gates, strict schema contracts, warnings-as-errors compilation, and security/coverage CI. Every public claim is tied to reproducible evidence in [docs/claims-and-evidence.md](docs/claims-and-evidence.md).

The clean build also emits a schema-validated CycloneDX runtime SBOM at `target/bom.json`. Dependabot covers Maven and GitHub Actions, while dependency review rejects newly introduced moderate-or-higher vulnerabilities.

The revision-matched local warm-query benchmark in [docs/results/serving-query-2026-08-18.json](docs/results/serving-query-2026-08-18.json) measured Redis p95 at 0.433 ms and ClickHouse p95 at 1.609 ms across 300 response-validated queries per store. These are local warm-container round trips, not production or cold-query latency.

The latest separately scoped real-path load benchmark in [docs/results/end-to-end-load-paired-2026-08-18.json](docs/results/end-to-end-load-paired-2026-08-18.json) used two excluded warm-up pairs followed by ten measured, alternating 30,000/60,000-event pairs. Median raw rates were 7,273.446 and 7,206.415 events/s, with CVs of 13.697% and 17.627%. Every run traversed four real Kafka topics, the production Flink graph at parallelism four, and synchronous Redis and ClickHouse sinks. Both stores were read back and every actor's event count, severity sum, risk score, rule, triggering event, and stable signal ID were validated; observed-store checksums matched independently generated expected checksums.

This is local preloaded-backlog evidence, not production capacity. Producer time was excluded and Flink startup was included. The paired fixed-startup decomposition is retained as a falsifiable diagnostic, not a result: five of ten estimates produced impossible negative startup values, so the model's stability assumption did not hold in this run. Earlier 10,000- and 60,000-event raw artifacts remain available as historical evidence.

Producer-controlled invalid records are classified and quarantined rather than dropped or allowed to block valid traffic. Quarantine records retain Kafka origin, a bounded base64 payload preview, full-payload SHA-256, and a stable source-coordinate key; set `SAFETY_QUARANTINE_TOPIC` to override the default `<first-configured-input-topic>.quarantine` topic.

Per-actor rolling history is reclaimed by event-time timers and capped by `SAFETY_MAX_HISTORY_EVENTS_PER_ACTOR` (default 100,000). A breach evicts the oldest event-time entries, increments explicit capacity metrics, and emits a replay-stable `__state_capacity__` operational risk signal rather than silently pretending configured rule counts remain complete.

Delivery semantics are explicit: Flink state and Kafka offsets use exactly-once checkpoints; Redis, ClickHouse, and Kafka quarantine are at-least-once sinks. Serving-store replays are idempotent through stable signal IDs and event-time ordering, while quarantine replays remain identifiable through the stable source-coordinate key. No end-to-end exactly-once or production throughput claim is made.

## Verify

Requires JDK 17, Maven 3.9+, Docker, and enough local resources for Kafka, Redis, and ClickHouse test containers.

```bash
./verify-all.sh
```

To reproduce the separately scoped serving-query measurement:

```bash
./scripts/run-serving-query-benchmark.sh 300 50
```

To reproduce the real Kafka-to-serving-stores load measurement:

```bash
./scripts/run-end-to-end-load-benchmark.sh 60000 10 6000 2
```

## Run

```bash
mvn package
export SAFETY_ENVIRONMENT=local
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD='your-password'
export SAFETY_RULES_PATH=conf/safety-rules.json
java -jar target/safety-stream-0.1.0-SNAPSHOT-app.jar \
  localhost:9092 content-events,activity-events,moderation-events safety-platform-v1 redis://localhost:6379 \
  jdbc:clickhouse:http://localhost:8123/default
```

For non-local runs, set `SAFETY_ENVIRONMENT=production` and an absolute `SAFETY_CHECKPOINT_URI` backed by storage supported by the Flink installation. Production mode refuses to start without that URI. Checkpoint intervals, timeouts, restart limits, and the opt-in failure drill are validated before the job graph starts; see [docs/operations-runbook.md](docs/operations-runbook.md).
