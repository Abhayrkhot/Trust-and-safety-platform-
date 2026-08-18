package dev.trustsafety;

import dev.trustsafety.config.RuntimeConfig;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.processing.SafetyProcessor;
import dev.trustsafety.rules.RuleConfig;
import dev.trustsafety.rules.RuleConfigLoader;
import dev.trustsafety.serde.SafetyEventDeserializer;
import dev.trustsafety.sink.ClickHouseHistoricalStore;
import dev.trustsafety.sink.RedisHotStateStore;
import dev.trustsafety.sink.RiskSignalSink;
import dev.trustsafety.testing.FailureInjector;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class SafetyStreamJob {
  private static final String USAGE =
      "usage: <bootstrap-servers> <topic> <group-id> <redis-uri> <clickhouse-jdbc-url>";

  private SafetyStreamJob() {}

  public static WatermarkStrategy<SafetyEvent> watermarkStrategy() {
    return WatermarkStrategy.<SafetyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((event, ignored) -> event.occurredAt().toEpochMilli())
        .withIdleness(Duration.ofMinutes(1));
  }

  public static DataStream<RiskSignal> buildEvaluationPipeline(
      StreamExecutionEnvironment env, KafkaSource<SafetyEvent> source, List<RuleConfig> rules) {
    return buildEvaluationPipeline(env, source, rules, null);
  }

  public static DataStream<RiskSignal> buildEvaluationPipeline(
      StreamExecutionEnvironment env,
      KafkaSource<SafetyEvent> source,
      List<RuleConfig> rules,
      Long failAfterEvents) {
    var events =
        env.fromSource(source, watermarkStrategy(), "safety-events-kafka")
            .uid("safety-events-kafka-v1");
    if (failAfterEvents != null)
      events =
          events
              .map(new FailureInjector<>("configured-drill", failAfterEvents))
              .name("failure-injection")
              .uid("failure-injection-v1");
    return events
        .keyBy(SafetyEvent::actorId)
        .process(new SafetyProcessor(rules, Duration.ofHours(24).toMillis()))
        .uid("safety-rules-v1");
  }

  public static void configureReliability(StreamExecutionEnvironment env, RuntimeConfig config) {
    env.enableCheckpointing(config.checkpointInterval().toMillis(), CheckpointingMode.EXACTLY_ONCE);
    CheckpointConfig checkpoints = env.getCheckpointConfig();
    checkpoints.setCheckpointTimeout(config.checkpointTimeout().toMillis());
    checkpoints.setMinPauseBetweenCheckpoints(config.checkpointMinPause().toMillis());
    checkpoints.setMaxConcurrentCheckpoints(1);
    checkpoints.setTolerableCheckpointFailureNumber(0);
    checkpoints.enableExternalizedCheckpoints(
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    config.checkpointUri().ifPresent(checkpoints::setCheckpointStorage);
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(
            config.restartAttempts(), config.restartDelay().toMillis()));
  }

  public static void attachServingSinks(
      DataStream<RiskSignal> signals, String redisUri, String clickHouseJdbcUrl) {
    attachServingSinks(signals, redisUri, clickHouseJdbcUrl, "default", "");
  }

  public static void attachServingSinks(
      DataStream<RiskSignal> signals,
      String redisUri,
      String clickHouseJdbcUrl,
      String clickHouseUser,
      String clickHousePassword) {
    signals
        .sinkTo(new RiskSignalSink(() -> new RedisHotStateStore(redisUri, Duration.ofHours(24))))
        .uid("redis-hot-state-v1");
    signals
        .sinkTo(
            new RiskSignalSink(
                () ->
                    new ClickHouseHistoricalStore(
                        clickHouseJdbcUrl, clickHouseUser, clickHousePassword)))
        .uid("clickhouse-history-v1");
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 1 && "--help".equals(args[0])) {
      System.out.println(USAGE);
      return;
    }
    if (args.length != 5) throw new IllegalArgumentException(USAGE);
    RuntimeConfig config = RuntimeConfig.fromEnvironment(System.getenv());
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureReliability(env, config);
    KafkaSource<SafetyEvent> source =
        KafkaSource.<SafetyEvent>builder()
            .setBootstrapServers(args[0])
            .setTopics(args[1])
            .setGroupId(args[2])
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setDeserializer(new SafetyEventDeserializer())
            .build();
    List<RuleConfig> rules = RuleConfigLoader.load(config.rulesPath());
    var signals =
        buildEvaluationPipeline(env, source, rules, config.failureAfterEvents().orElse(null));
    attachServingSinks(
        signals,
        args[3],
        args[4],
        System.getenv().getOrDefault("CLICKHOUSE_USER", "default"),
        System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", ""));
    env.execute("trust-and-safety-event-processing");
  }
}
