# Real-Time Trust & Safety Event Processing

An evidence-first reference platform for processing versioned safety events with Kafka and Apache Flink. Phase 1 implements strict, versioned JSON contracts and Kafka record deserialization. Stateful event-time evaluation follows in Phase 2.

No throughput or latency claims are made yet. See [docs/claims-and-evidence.md](docs/claims-and-evidence.md).

## Verify

Requires JDK 17+ and Maven 3.9+.

```bash
./verify-all.sh
```
