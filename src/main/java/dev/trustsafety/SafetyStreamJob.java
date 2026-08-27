package dev.trustsafety;

import dev.trustsafety.config.RuntimeConfig;
import dev.trustsafety.model.IngestedSafetyRecord;
import dev.trustsafety.model.QuarantinedEvent;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.processing.IngestionRouter;
import dev.trustsafety.processing.SafetyProcessor;
import dev.trustsafety.rules.RuleConfig;
import dev.trustsafety.rules.RuleConfigLoader;
import dev.trustsafety.serde.SafetyEventDeserializer;
import dev.trustsafety.sink.ClickHouseHistoricalStore;
import dev.trustsafety.sink.QuarantineKafkaSerializationSchema;
import dev.trustsafety.sink.RedisHotStateStore;
import dev.trustsafety.sink.RiskSignalSink;
import dev.trustsafety.testing.FailureInjector;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
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
      StreamExecutionEnvironment env,
      KafkaSource<IngestedSafetyRecord> source,
      List<RuleConfig> rules,
      Consumer<DataStream<QuarantinedEvent>> quarantineAttacher) {
    return buildEvaluationPipeline(env, source, rules, null, quarantineAttacher);
  }

  public static DataStream<RiskSignal> buildEvaluationPipeline(
      StreamExecutionEnvironment env,
      KafkaSource<IngestedSafetyRecord> source,
      List<RuleConfig> rules,
      Long failAfterEvents,
      Consumer<DataStream<QuarantinedEvent>> quarantineAttacher) {
    Objects.requireNonNull(quarantineAttacher, "quarantineAttacher");
    var ingested =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "safety-events-kafka")
            .uid("safety-events-kafka-v1");
    var routed =
        ingested
            .process(new IngestionRouter())
            .name("validate-and-route")
            .uid("validate-and-route-v1");
    DataStream<QuarantinedEvent> quarantined = routed.getSideOutput(IngestionRouter.QUARANTINE);
    quarantineAttacher.accept(quarantined);
    DataStream<SafetyEvent> events =
        routed
            .assignTimestampsAndWatermarks(watermarkStrategy())
            .name("safety-event-watermarks")
            .uid("safety-event-watermarks-v1");
    if (failAfterEvents != null) {
      events =
          events
              .map(new FailureInjector<>("configured-drill", failAfterEvents))
              .name("failure-injection")
              .uid("failure-injection-v1");
    }
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
    checkpoints.setExternalizedCheckpointRetention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    config
        .checkpointUri()
        .ifPresent(
            uri -> {
              Configuration storage = new Configuration();
              storage.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, uri.toString());
              env.configure(storage);
            });
    Configuration restart = new Configuration();
    restart.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    restart.set(
        RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, config.restartAttempts());
    restart.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, config.restartDelay());
    env.configure(restart);
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

  public static void attachQuarantineSink(
      DataStream<QuarantinedEvent> quarantined, String bootstrapServers, String quarantineTopic) {
    KafkaSink<QuarantinedEvent> sink =
        KafkaSink.<QuarantinedEvent>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(new QuarantineKafkaSerializationSchema(quarantineTopic))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    quarantined.sinkTo(sink).name("quarantine-kafka").uid("quarantine-kafka-v1");
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
    KafkaSource<IngestedSafetyRecord> source =
        KafkaSource.<IngestedSafetyRecord>builder()
            .setBootstrapServers(args[0])
            .setTopics(args[1])
            .setGroupId(args[2])
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setDeserializer(new SafetyEventDeserializer())
            .build();
    List<RuleConfig> rules = RuleConfigLoader.load(config.rulesPath());
    var signals =
        buildEvaluationPipeline(
            env,
            source,
            rules,
            config.failureAfterEvents().orElse(null),
            quarantined ->
                attachQuarantineSink(
                    quarantined,
                    args[0],
                    System.getenv()
                        .getOrDefault("SAFETY_QUARANTINE_TOPIC", args[1] + ".quarantine")));
    attachServingSinks(
        signals,
        args[3],
        args[4],
        System.getenv().getOrDefault("CLICKHOUSE_USER", "default"),
        System.getenv().getOrDefault("CLICKHOUSE_PASSWORD", ""));
    env.execute("trust-and-safety-event-processing");
  }
}
