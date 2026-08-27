package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import dev.trustsafety.model.SafetyEvent;
import java.time.Duration;
import java.util.ArrayList;
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
          PipelineTestSupport.ExactRecordingStore.reset();
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(4);
          PipelineTestSupport.build(env, input, PipelineTestSupport.ExactRecordingStore::new, 2);
          env.execute("non-vacuous-soak-it");
          assertThat(PipelineTestSupport.ExactRecordingStore.WRITES.get()).isEqualTo(unique);
          assertThat(PipelineTestSupport.ExactRecordingStore.SIGNAL_IDS).hasSize(unique);
        });
  }
}
