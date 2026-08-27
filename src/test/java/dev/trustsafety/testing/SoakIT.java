package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import dev.trustsafety.model.SafetyEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class SoakIT {
  @Test
  void mixedDuplicateAndOutOfOrderStreamHasExactOracleAndCompletesWithinBudget() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(30),
        () -> {
          int unique = 120_000;
          var input = new ArrayList<SafetyEvent>(132_000);
          for (int i = 0; i < unique; i++) {
            SafetyEvent event = PipelineTestSupport.event(i, 100);
            input.add(event);
            if (i % 10 == 0) input.add(event);
          }
          executeAndAssert(input, unique, "uniform-132k-soak-it");
        });
  }

  @Test
  void hotKeySkewAndChangingDuplicateRatiosPreserveTheExactUniqueOracle() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(30),
        () -> {
          int unique = 30_000;
          var input = new ArrayList<SafetyEvent>(36_000);
          for (int i = 0; i < unique; i++) {
            String actor = i < 27_000 ? "hot-" + (i % 10) : "cold-" + (i % 1_000);
            long eventTime = 1_700_000_000_000L + i * 10L - (i % 17 == 0 ? 250 : 0);
            SafetyEvent event = shapedEvent("skew-" + i, actor, eventTime, i % 100);
            input.add(event);
            if (i >= 10_000 && i < 20_000 && i % 10 == 0) input.add(event);
            if (i >= 20_000 && i % 2 == 0) input.add(event);
          }
          executeAndAssert(input, unique, "skew-and-duplicate-matrix-soak-it");
        });
  }

  @Test
  void arrivalsBeyondEachActorsRetentionCutoffAreIgnoredUnderLoad() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(30),
        () -> {
          int actors = 100;
          int recentPerActor = 20;
          int stalePerActor = 10;
          long recentBase = 1_700_010_800_000L;
          var input = new ArrayList<SafetyEvent>(actors * (recentPerActor + stalePerActor));
          for (int event = 0; event < recentPerActor; event++)
            for (int actor = 0; actor < actors; actor++)
              input.add(
                  shapedEvent(
                      "recent-" + actor + "-" + event,
                      "retention-" + actor,
                      recentBase + event,
                      1));
          for (int event = 0; event < stalePerActor; event++)
            for (int actor = 0; actor < actors; actor++)
              input.add(
                  shapedEvent(
                      "stale-" + actor + "-" + event,
                      "retention-" + actor,
                      recentBase - Duration.ofHours(3).toMillis() + event,
                      1));
          executeAndAssert(input, actors * recentPerActor, "retention-boundary-soak-it");
        });
  }

  private static void executeAndAssert(List<SafetyEvent> input, int expectedSignals, String jobName)
      throws Exception {
    PipelineTestSupport.ExactRecordingStore.reset();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    PipelineTestSupport.build(env, input, PipelineTestSupport.ExactRecordingStore::new, 2);
    env.execute(jobName);
    assertThat(PipelineTestSupport.ExactRecordingStore.WRITES.get()).isEqualTo(expectedSignals);
    assertThat(PipelineTestSupport.ExactRecordingStore.SIGNAL_IDS).hasSize(expectedSignals);
  }

  private static SafetyEvent shapedEvent(
      String eventId, String actorId, long eventTimeMillis, int severity) {
    Instant timestamp = Instant.ofEpochMilli(eventTimeMillis);
    return new SafetyEvent(
        1,
        eventId,
        timestamp,
        timestamp,
        actorId,
        null,
        SafetyEvent.EventType.POLICY_MATCH,
        severity,
        Map.of("source", "adversarial-soak"));
  }
}
