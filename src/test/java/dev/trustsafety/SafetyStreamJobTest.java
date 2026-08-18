package dev.trustsafety;

import static org.assertj.core.api.Assertions.assertThat;

import dev.trustsafety.config.RuntimeConfig;
import dev.trustsafety.model.SafetyEvent;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class SafetyStreamJobTest {
  @Test
  void watermarkUsesOccurredAt() {
    var e =
        new SafetyEvent(
            1,
            "e",
            Instant.ofEpochMilli(1234),
            Instant.ofEpochMilli(9999),
            "a",
            null,
            SafetyEvent.EventType.USER_BLOCK,
            1,
            Map.of());
    var assigner = SafetyStreamJob.watermarkStrategy().createTimestampAssigner(() -> null);
    assertThat(assigner.extractTimestamp(e, 0)).isEqualTo(1234);
  }

  @Test
  void configuresExternalizedCheckpointsAndBoundedRestarts() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    RuntimeConfig config =
        new RuntimeConfig(
            RuntimeConfig.Environment.PRODUCTION,
            Path.of("conf/safety-rules.json"),
            Optional.of(URI.create("file:///tmp/safety-checkpoints")),
            Duration.ofSeconds(20),
            Duration.ofSeconds(90),
            Duration.ofSeconds(4),
            4,
            Duration.ofSeconds(3),
            Optional.empty());

    SafetyStreamJob.configureReliability(env, config);

    CheckpointConfig checkpoints = env.getCheckpointConfig();
    assertThat(checkpoints.isCheckpointingEnabled()).isTrue();
    assertThat(checkpoints.getCheckpointInterval()).isEqualTo(20_000);
    assertThat(checkpoints.getCheckpointTimeout()).isEqualTo(90_000);
    assertThat(checkpoints.getMinPauseBetweenCheckpoints()).isEqualTo(4_000);
    assertThat(checkpoints.getMaxConcurrentCheckpoints()).isOne();
    assertThat(checkpoints.getTolerableCheckpointFailureNumber()).isZero();
    assertThat(checkpoints.getExternalizedCheckpointCleanup())
        .isEqualTo(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    assertThat(checkpoints.getCheckpointStorage())
        .isInstanceOfSatisfying(
            FileSystemCheckpointStorage.class,
            storage ->
                assertThat(storage.getCheckpointPath().toString())
                    .isEqualTo("file:/tmp/safety-checkpoints"));
    assertThat(env.getConfig().getRestartStrategy())
        .isInstanceOfSatisfying(
            RestartStrategies.FixedDelayRestartStrategyConfiguration.class,
            restart -> {
              assertThat(restart.getRestartAttempts()).isEqualTo(4);
              assertThat(restart.getDurationBetweenAttempts()).isEqualTo(Duration.ofSeconds(3));
            });
  }
}
