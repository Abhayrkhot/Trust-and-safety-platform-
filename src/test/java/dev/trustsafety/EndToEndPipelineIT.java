package dev.trustsafety;

import static org.assertj.core.api.Assertions.assertThat;

import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.rules.RuleConfig;
import dev.trustsafety.serde.SafetyEventDeserializer;
import dev.trustsafety.sink.RedisHotStateStore;
import io.lettuce.core.RedisClient;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class EndToEndPipelineIT {
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

  @Container
  static final ClickHouseContainer CLICKHOUSE =
      new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:25.8-alpine"));

  @Test
  void kafkaRecordsFlowThroughFlinkIntoBothServingStoresWithDeduplication() throws Exception {
    String topic = "safety-e2e";
    produce(topic, event("e1", 40, 0), event("e1", 40, 0), event("e2", 40, 1), event("e3", 40, 2));
    KafkaSource<SafetyEvent> source =
        KafkaSource.<SafetyEvent>builder()
            .setBootstrapServers(KAFKA.getBootstrapServers())
            .setTopics(topic)
            .setGroupId("e2e-group")
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
            .setBounded(OffsetsInitializer.latest())
            .setDeserializer(new SafetyEventDeserializer())
            .build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    var signals =
        SafetyStreamJob.buildEvaluationPipeline(
            env, source, List.of(new RuleConfig("e2e-rule", 60_000, 3, 120, 90)));
    String redisUri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    SafetyStreamJob.attachServingSinks(
        signals,
        redisUri,
        CLICKHOUSE.getJdbcUrl(),
        CLICKHOUSE.getUsername(),
        CLICKHOUSE.getPassword());
    env.execute("kafka-flink-serving-e2e");

    RedisClient redis = RedisClient.create(redisUri);
    try (var connection = redis.connect()) {
      assertThat(connection.sync().hget(RedisHotStateStore.key("actor-e2e"), "payload"))
          .contains("e2e-rule")
          .contains("\"risk_score\":90")
          .contains("\"observed_event_count\":3");
    } finally {
      redis.shutdown();
    }
    try (var connection =
            DriverManager.getConnection(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT count(),any(rule_id),any(observed_event_count) FROM risk_signals FINAL")) {
      assertThat(rows.next()).isTrue();
      assertThat(rows.getLong(1)).isEqualTo(1);
      assertThat(rows.getString(2)).isEqualTo("e2e-rule");
      assertThat(rows.getLong(3)).isEqualTo(3);
    }
  }

  private static void produce(String topic, String... payloads) {
    Properties p = new Properties();
    p.put("bootstrap.servers", KAFKA.getBootstrapServers());
    p.put("key.serializer", ByteArraySerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    try (var producer = new KafkaProducer<byte[], byte[]>(p)) {
      for (String payload : payloads)
        producer.send(new ProducerRecord<>(topic, null, payload.getBytes(StandardCharsets.UTF_8)));
      producer.flush();
    }
  }

  private static String event(String id, int severity, int seconds) {
    return "{\"schema_version\":1,\"event_id\":\""
        + id
        + "\",\"occurred_at\":\"2026-08-18T12:00:0"
        + seconds
        + "Z\",\"ingested_at\":\"2026-08-18T12:00:0"
        + seconds
        + "Z\",\"actor_id\":\"actor-e2e\",\"event_type\":\"POLICY_MATCH\",\"severity\":"
        + severity
        + "}";
  }
}
