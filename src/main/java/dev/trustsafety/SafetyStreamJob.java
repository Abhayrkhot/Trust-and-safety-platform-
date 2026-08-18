package dev.trustsafety;

import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.processing.SafetyProcessor;
import dev.trustsafety.rules.RuleConfig;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public final class SafetyStreamJob {
  private SafetyStreamJob() {}
  public static WatermarkStrategy<SafetyEvent> watermarkStrategy() {
    return WatermarkStrategy.<SafetyEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner((event, ignored) -> event.occurredAt().toEpochMilli()).withIdleness(Duration.ofMinutes(1));
  }
  public static DataStream<RiskSignal> buildEvaluationPipeline(StreamExecutionEnvironment env,KafkaSource<SafetyEvent> source,List<RuleConfig> rules) {
    var events=env.fromSource(source,watermarkStrategy(),"safety-events-kafka").uid("safety-events-kafka-v1");
    String failAfter=System.getenv("SAFETY_FAIL_AFTER_EVENTS");
    if(failAfter!=null&&!failAfter.isBlank()) events=events.map(new FailureInjector<>("configured-drill",Long.parseLong(failAfter))).name("failure-injection").uid("failure-injection-v1");
    return events.keyBy(SafetyEvent::actorId).process(new SafetyProcessor(rules,Duration.ofHours(24).toMillis())).uid("safety-rules-v1");
  }
  public static void attachServingSinks(DataStream<RiskSignal> signals,String redisUri,String clickHouseJdbcUrl) {
    attachServingSinks(signals,redisUri,clickHouseJdbcUrl,"default","");
  }
  public static void attachServingSinks(DataStream<RiskSignal> signals,String redisUri,String clickHouseJdbcUrl,String clickHouseUser,String clickHousePassword) {
    signals.sinkTo(new RiskSignalSink(() -> new RedisHotStateStore(redisUri,Duration.ofHours(24)))).uid("redis-hot-state-v1");
    signals.sinkTo(new RiskSignalSink(() -> new ClickHouseHistoricalStore(clickHouseJdbcUrl,clickHouseUser,clickHousePassword))).uid("clickhouse-history-v1");
  }
  public static void main(String[] args) throws Exception {
    if(args.length!=5) throw new IllegalArgumentException("usage: <bootstrap-servers> <topic> <group-id> <redis-uri> <clickhouse-jdbc-url>");
    StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE); env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5_000));
    KafkaSource<SafetyEvent> source=KafkaSource.<SafetyEvent>builder().setBootstrapServers(args[0]).setTopics(args[1]).setGroupId(args[2])
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setDeserializer(new SafetyEventDeserializer()).build();
    List<RuleConfig> rules=List.of(new RuleConfig("burst-severity-v1",60_000,3,120,80));
    var signals=buildEvaluationPipeline(env,source,rules);attachServingSinks(signals,args[3],args[4],System.getenv().getOrDefault("CLICKHOUSE_USER","default"),System.getenv().getOrDefault("CLICKHOUSE_PASSWORD",""));
    env.execute("trust-and-safety-event-processing");
  }
}
