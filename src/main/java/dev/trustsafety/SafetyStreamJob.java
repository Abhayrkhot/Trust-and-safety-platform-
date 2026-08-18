package dev.trustsafety;

import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.processing.SafetyProcessor;
import dev.trustsafety.rules.RuleConfig;
import dev.trustsafety.serde.SafetyEventDeserializer;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class SafetyStreamJob {
  private SafetyStreamJob() {}
  public static WatermarkStrategy<SafetyEvent> watermarkStrategy() {
    return WatermarkStrategy.<SafetyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((event, ignored) -> event.occurredAt().toEpochMilli()).withIdleness(Duration.ofMinutes(1));
  }
  public static void main(String[] args) throws Exception {
    if(args.length!=3) throw new IllegalArgumentException("usage: <bootstrap-servers> <topic> <group-id>");
    StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE); env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5_000));
    KafkaSource<SafetyEvent> source=KafkaSource.<SafetyEvent>builder().setBootstrapServers(args[0]).setTopics(args[1]).setGroupId(args[2])
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setDeserializer(new SafetyEventDeserializer()).build();
    List<RuleConfig> rules=List.of(new RuleConfig("burst-severity-v1",60_000,3,120,80));
    env.fromSource(source,watermarkStrategy(),"safety-events-kafka").uid("safety-events-kafka-v1")
        .keyBy(SafetyEvent::actorId).process(new SafetyProcessor(rules,Duration.ofHours(24).toMillis()))
        .uid("safety-rules-v1").print();
    env.execute("trust-and-safety-event-processing");
  }
}
