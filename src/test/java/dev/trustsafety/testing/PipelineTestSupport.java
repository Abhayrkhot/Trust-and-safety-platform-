package dev.trustsafety.testing;

import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.processing.SafetyProcessor;
import dev.trustsafety.rules.RuleConfig;
import dev.trustsafety.sink.RiskSignalSink;
import dev.trustsafety.sink.RiskSignalStore;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

final class PipelineTestSupport {
  private PipelineTestSupport() {}

  static SafetyEvent event(long sequence, int actors) {
    long logical = sequence;
    long occurred = 1_700_000_000_000L + logical * 10;
    if (sequence % 17 == 0) occurred -= 250; // deterministic out-of-order arrivals
    return new SafetyEvent(
        1,
        "event-" + logical,
        Instant.ofEpochMilli(occurred),
        Instant.ofEpochMilli(occurred),
        "actor-" + (logical % actors),
        "content-" + logical,
        SafetyEvent.EventType.POLICY_MATCH,
        (int) (logical % 100),
        Map.of("source", "test"));
  }

  static void build(
      StreamExecutionEnvironment env,
      List<SafetyEvent> input,
      RiskSignalStoreFactory factory,
      int sinkParallelism) {
    env.fromCollection(input)
        .keyBy(SafetyEvent::actorId)
        .process(
            new SafetyProcessor(
                List.of(new RuleConfig("all-events", Duration.ofHours(1).toMillis(), 1, 0, 10)),
                Duration.ofHours(2).toMillis()))
        .sinkTo(new RiskSignalSink(factory::create))
        .setParallelism(sinkParallelism);
  }

  @FunctionalInterface
  interface RiskSignalStoreFactory extends java.io.Serializable {
    RiskSignalStore create();
  }

  static class ExactRecordingStore implements RiskSignalStore {
    static final Set<String> SIGNAL_IDS = ConcurrentHashMap.newKeySet();
    static final AtomicLong WRITES = new AtomicLong();

    static void reset() {
      SIGNAL_IDS.clear();
      WRITES.set(0);
    }

    public void write(RiskSignal signal) {
      WRITES.incrementAndGet();
      SIGNAL_IDS.add(signal.signalId());
    }

    public void close() {}
  }
}
