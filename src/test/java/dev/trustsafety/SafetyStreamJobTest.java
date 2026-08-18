package dev.trustsafety;

import static org.assertj.core.api.Assertions.assertThat;

import dev.trustsafety.config.RuntimeConfig;
import dev.trustsafety.model.IngestedSafetyRecord;
import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.rules.RuleConfig;
import dev.trustsafety.serde.SafetyEventDeserializer;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
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
    assertThat(checkpoints.getExternalizedCheckpointRetention())
        .isEqualTo(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    assertThat(env.getConfiguration().get(CheckpointingOptions.CHECKPOINTS_DIRECTORY).toString())
        .isEqualTo("file:///tmp/safety-checkpoints");
    assertThat(env.getConfiguration().get(RestartStrategyOptions.RESTART_STRATEGY))
        .isEqualTo("fixed-delay");
    assertThat(
            env.getConfiguration()
                .get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS))
        .isEqualTo(4);
    assertThat(
            env.getConfiguration().get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY))
        .isEqualTo(Duration.ofSeconds(3));
  }

  @Test
  void preservesStableOperatorUidsAcrossTheIngestionGraph() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    KafkaSource<IngestedSafetyRecord> source =
        KafkaSource.<IngestedSafetyRecord>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("events")
            .setGroupId("uid-test")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new SafetyEventDeserializer())
            .build();
    SafetyStreamJob.buildEvaluationPipeline(
        env, source, java.util.List.of(new RuleConfig("uid-rule", 1_000, 1, 1, 1)), ignored -> {});

    assertThat(env.getStreamGraph().getStreamNodes())
        .extracting(org.apache.flink.streaming.api.graph.StreamNode::getTransformationUID)
        .contains(
            "safety-events-kafka-v1",
            "validate-and-route-v1",
            "safety-event-watermarks-v1",
            "safety-rules-v1");
  }
}
