package dev.trustsafety;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.trustsafety.config.RuntimeConfig;
import dev.trustsafety.model.IngestedSafetyRecord;
import dev.trustsafety.model.QuarantinedEvent;
import dev.trustsafety.rules.RuleConfig;
import dev.trustsafety.serde.QuarantinedEventJson;
import dev.trustsafety.serde.SafetyEventDecodingException.Reason;
import dev.trustsafety.serde.SafetyEventDeserializer;
import dev.trustsafety.sink.RedisHotStateStore;
import dev.trustsafety.testing.FailureInjector;
import io.lettuce.core.RedisClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
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
  void validAndPoisonKafkaRecordsReachTheirExactDestinations() throws Exception {
    String topic = "safety-e2e";
    String quarantineTopic = "safety-e2e-quarantine";
    produce(
        topic,
        event("e1", 40, 0),
        "not-json",
        event("e1", 40, 0),
        "{\"schema_version\":99}",
        event("e2", 40, 1),
        event("e-invalid", 40, 1).replace("}", ",\"unexpected\":true}"),
        event("e3", 40, 2));
    KafkaSource<IngestedSafetyRecord> source =
        KafkaSource.<IngestedSafetyRecord>builder()
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
            env,
            source,
            List.of(new RuleConfig("e2e-rule", 60_000, 3, 120, 90)),
            quarantined ->
                SafetyStreamJob.attachQuarantineSink(
                    quarantined, KAFKA.getBootstrapServers(), quarantineTopic));
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
                "SELECT count(),any(rule_id),any(observed_event_count) FROM risk_signals FINAL WHERE rule_id='e2e-rule'")) {
      assertThat(rows.next()).isTrue();
      assertThat(rows.getLong(1)).isEqualTo(1);
      assertThat(rows.getString(2)).isEqualTo("e2e-rule");
      assertThat(rows.getLong(3)).isEqualTo(3);
    }

    List<QuarantinedEvent> quarantined = consumeQuarantine(quarantineTopic, 3);
    assertThat(quarantined).extracting(QuarantinedEvent::sourceOffset).containsExactly(1L, 3L, 5L);
    assertThat(quarantined)
        .extracting(QuarantinedEvent::failureReason)
        .containsExactly(
            Reason.MALFORMED_JSON, Reason.UNSUPPORTED_SCHEMA_VERSION, Reason.CONTRACT_VIOLATION);
    assertThat(quarantined)
        .allSatisfy(
            record -> {
              assertThat(record.sourceTopic()).isEqualTo(topic);
              assertThat(record.sourcePartition()).isZero();
              assertThat(record.quarantineId()).isEqualTo(topic + ":0:" + record.sourceOffset());
              assertThat(record.payloadSha256()).hasSize(64);
            });
  }

  @Test
  @Timeout(60)
  void injectedRestartConvergesAllExternalStoresWithCheckpointEvidence(
      @TempDir Path checkpointDirectory) throws Exception {
    String topic = "safety-recovery";
    String quarantineTopic = "safety-recovery-quarantine";
    List<String> payloads = new ArrayList<>();
    Set<Long> poisonOffsets = new LinkedHashSet<>();
    for (int i = 0; i < 3_000; i++) {
      if (i % 500 == 0) {
        poisonOffsets.add((long) payloads.size());
        payloads.add("not-json-" + i);
      }
      payloads.add(eventForActor("filler-" + i, "actor-" + i, i, 1));
    }
    payloads.add(eventForActor("recovery-1", "actor-recovery", 3_001, 40));
    payloads.add(eventForActor("recovery-2", "actor-recovery", 3_002, 40));
    produce(topic, payloads.toArray(String[]::new));

    KafkaSource<IngestedSafetyRecord> source = source(topic, "recovery-group");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    RuntimeConfig config =
        new RuntimeConfig(
            RuntimeConfig.Environment.LOCAL,
            Path.of("conf/safety-rules.json"),
            java.util.Optional.of(checkpointDirectory.toUri()),
            Duration.ofMillis(25),
            Duration.ofSeconds(10),
            Duration.ZERO,
            1,
            Duration.ZERO,
            100_000,
            java.util.Optional.of(1_500L));
    SafetyStreamJob.configureReliability(env, config);
    var signals =
        SafetyStreamJob.buildEvaluationPipeline(
            env,
            source,
            List.of(new RuleConfig("recovery-rule", 60_000, 2, 80, 95)),
            config.failureAfterEvents().orElseThrow(),
            quarantined ->
                SafetyStreamJob.attachQuarantineSink(
                    quarantined, KAFKA.getBootstrapServers(), quarantineTopic));
    String redisUri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    SafetyStreamJob.attachServingSinks(
        signals,
        redisUri,
        CLICKHOUSE.getJdbcUrl(),
        CLICKHOUSE.getUsername(),
        CLICKHOUSE.getPassword());

    var result = env.execute("full-infrastructure-recovery");
    Long completedCheckpoints =
        result.getAccumulatorResult(FailureInjector.checkpointAccumulator("configured-drill"));
    assertThat(completedCheckpoints).isPositive();

    RedisClient redis = RedisClient.create(redisUri);
    try (var connection = redis.connect()) {
      assertThat(connection.sync().hget(RedisHotStateStore.key("actor-recovery"), "payload"))
          .contains("recovery-rule")
          .contains("\"observed_event_count\":2")
          .contains("\"risk_score\":95");
    } finally {
      redis.shutdown();
    }
    try (var connection =
            DriverManager.getConnection(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT count() FROM risk_signals FINAL WHERE rule_id='recovery-rule'")) {
      assertThat(rows.next()).isTrue();
      assertThat(rows.getLong(1)).isOne();
    }

    List<QuarantinedEvent> quarantined =
        consumeQuarantineAtLeast(quarantineTopic, poisonOffsets.size());
    assertThat(quarantined)
        .allSatisfy(record -> assertThat(record.failureReason()).isEqualTo(Reason.MALFORMED_JSON));
    assertThat(
            quarantined.stream()
                .map(QuarantinedEvent::sourceOffset)
                .collect(java.util.stream.Collectors.toSet()))
        .containsExactlyInAnyOrderElementsOf(poisonOffsets);
    assertThat(quarantined.stream().map(QuarantinedEvent::quarantineId).distinct().count())
        .isEqualTo(poisonOffsets.size());
  }

  @Test
  void parallelTopicsConvergeIntoOneKeyedActorRiskSignal() throws Exception {
    List<String> topics =
        List.of("content-events-e2e", "activity-events-e2e", "moderation-events-e2e");
    produce(topics.get(0), eventForActor("content-1", "actor-cross-stream", 1, 40));
    produce(topics.get(1), eventForActor("activity-1", "actor-cross-stream", 2, 40));
    produce(topics.get(2), eventForActor("moderation-1", "actor-cross-stream", 3, 40));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);
    var signals =
        SafetyStreamJob.buildEvaluationPipeline(
            env,
            source(topics, "cross-stream-group"),
            List.of(new RuleConfig("cross-stream-rule", 60_000, 3, 120, 97)),
            ignored -> {});
    String redisUri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    SafetyStreamJob.attachServingSinks(
        signals,
        redisUri,
        CLICKHOUSE.getJdbcUrl(),
        CLICKHOUSE.getUsername(),
        CLICKHOUSE.getPassword());

    env.execute("multi-topic-cross-stream-e2e");

    RedisClient redis = RedisClient.create(redisUri);
    try (var connection = redis.connect()) {
      assertThat(connection.sync().hget(RedisHotStateStore.key("actor-cross-stream"), "payload"))
          .contains("cross-stream-rule")
          .contains("\"observed_event_count\":3")
          .contains("\"risk_score\":97");
    } finally {
      redis.shutdown();
    }
    try (var connection =
            DriverManager.getConnection(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT count(),any(observed_event_count) FROM risk_signals FINAL WHERE rule_id='cross-stream-rule'")) {
      assertThat(rows.next()).isTrue();
      assertThat(rows.getLong(1)).isOne();
      assertThat(rows.getLong(2)).isEqualTo(3);
    }
  }

  @Test
  @Timeout(90)
  void benchmarksBackloggedKafkaThroughFlinkIntoBothServingStores() throws Exception {
    int events = positiveProperty("endToEndBenchmarkEvents", 10_000);
    int actors = positiveProperty("endToEndBenchmarkActors", 1_000);
    int iterations = positiveProperty("endToEndBenchmarkIterations", 1);
    if (events % actors != 0)
      throw new IllegalArgumentException("end-to-end benchmark events must be divisible by actors");
    int eventsPerActor = events / actors;
    List<Map<String, Object>> trials = new ArrayList<>();
    double[] throughput = new double[iterations];

    for (int trial = 0; trial < iterations; trial++) {
      List<String> topics =
          List.of(
              "load-content-" + trial,
              "load-activity-" + trial,
              "load-moderation-" + trial,
              "load-enforcement-" + trial);
      String ruleId = "load-rule-" + trial;
      produceLoad(topics, trial, events, actors);
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(4);
      var signals =
          SafetyStreamJob.buildEvaluationPipeline(
              env,
              source(topics, "load-group-" + trial),
              List.of(new RuleConfig(ruleId, 60_000, eventsPerActor, eventsPerActor, 90)),
              ignored -> {});
      String redisUri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
      SafetyStreamJob.attachServingSinks(
          signals,
          redisUri,
          CLICKHOUSE.getJdbcUrl(),
          CLICKHOUSE.getUsername(),
          CLICKHOUSE.getPassword());

      long started = System.nanoTime();
      env.execute("end-to-end-load-" + trial);
      long elapsedNanos = System.nanoTime() - started;
      throughput[trial] = events / (elapsedNanos / 1_000_000_000.0);

      RedisClient redis = RedisClient.create(redisUri);
      try (var connection = redis.connect()) {
        assertThat(connection.sync().keys("safety:risk:actor:load-" + trial + "-*"))
            .hasSize(actors);
      } finally {
        redis.shutdown();
      }
      try (var connection =
              DriverManager.getConnection(
                  CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
          var statement = connection.createStatement();
          var rows =
              statement.executeQuery(
                  "SELECT count() FROM risk_signals FINAL WHERE rule_id='" + ruleId + "'")) {
        assertThat(rows.next()).isTrue();
        assertThat(rows.getLong(1)).isEqualTo(actors);
      }

      Map<String, Object> measured = new LinkedHashMap<>();
      measured.put("trial", trial + 1);
      measured.put("elapsed_seconds", round(elapsedNanos / 1_000_000_000.0));
      measured.put("events_per_second", round(throughput[trial]));
      measured.put("redis_actor_keys", actors);
      measured.put("clickhouse_final_rows", actors);
      trials.add(measured);
    }

    double[] sorted = throughput.clone();
    java.util.Arrays.sort(sorted);
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("benchmark", "backlogged-kafka-to-serving-stores");
    result.put("revision", System.getProperty("endToEndBenchmarkRevision", "unspecified"));
    result.put("events_per_trial", events);
    result.put("actors", actors);
    result.put("events_per_actor", eventsPerActor);
    result.put("kafka_topics", 4);
    result.put("flink_parallelism", 4);
    result.put("risk_signals_per_trial", actors);
    result.put("iterations", iterations);
    result.put("trials", trials);
    result.put("median_events_per_second", round(percentile(sorted, 0.50)));
    result.put("redis_image", "redis:8.2-alpine");
    result.put("clickhouse_image", "clickhouse/clickhouse-server:25.8-alpine");
    result.put("kafka_image", "apache/kafka-native:3.8.0");
    result.put("java_version", System.getProperty("java.version"));
    result.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
    result.put("available_processors", Runtime.getRuntime().availableProcessors());
    result.put(
        "scope",
        "local preloaded Kafka backlog through Flink and synchronous Redis/ClickHouse sinks; producer time excluded, job startup included");
    Path output = Path.of("target", "benchmark-results", "end-to-end-load.json");
    Files.createDirectories(output.getParent());
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), result);
    System.out.println(new ObjectMapper().writeValueAsString(result));
  }

  private static List<QuarantinedEvent> consumeQuarantine(String topic, int expected)
      throws Exception {
    Properties p = new Properties();
    p.put("bootstrap.servers", KAFKA.getBootstrapServers());
    p.put("group.id", "quarantine-verifier");
    p.put("auto.offset.reset", "earliest");
    p.put("enable.auto.commit", "false");
    p.put("key.deserializer", ByteArrayDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    List<QuarantinedEvent> output = new ArrayList<>();
    try (var consumer = new KafkaConsumer<byte[], byte[]>(p)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (output.size() < expected && System.nanoTime() < deadline) {
        consumer
            .poll(Duration.ofMillis(250))
            .forEach(
                record -> {
                  try {
                    QuarantinedEvent decoded = QuarantinedEventJson.decode(record.value());
                    assertThat(new String(record.key(), StandardCharsets.UTF_8))
                        .isEqualTo(decoded.quarantineId());
                    output.add(decoded);
                  } catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                  }
                });
      }
      consumer.poll(Duration.ofMillis(500)).forEach(ignored -> output.add(null));
    }
    assertThat(output).hasSize(expected).doesNotContainNull();
    output.sort(Comparator.comparingLong(QuarantinedEvent::sourceOffset));
    return output;
  }

  private static List<QuarantinedEvent> consumeQuarantineAtLeast(String topic, int expectedDistinct)
      throws Exception {
    Properties p = consumerProperties("recovery-quarantine-verifier");
    List<QuarantinedEvent> output = new ArrayList<>();
    try (var consumer = new KafkaConsumer<byte[], byte[]>(p)) {
      consumer.subscribe(List.of(topic));
      long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
      while (distinctQuarantineIds(output) < expectedDistinct && System.nanoTime() < deadline)
        decode(consumer.poll(Duration.ofMillis(250)), output);
      decode(consumer.poll(Duration.ofSeconds(1)), output);
    }
    assertThat(distinctQuarantineIds(output)).isEqualTo(expectedDistinct);
    return output;
  }

  private static void decode(
      org.apache.kafka.clients.consumer.ConsumerRecords<byte[], byte[]> records,
      List<QuarantinedEvent> output) {
    records.forEach(
        record -> {
          try {
            QuarantinedEvent decoded = QuarantinedEventJson.decode(record.value());
            assertThat(new String(record.key(), StandardCharsets.UTF_8))
                .isEqualTo(decoded.quarantineId());
            output.add(decoded);
          } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
          }
        });
  }

  private static long distinctQuarantineIds(List<QuarantinedEvent> records) {
    return records.stream().map(QuarantinedEvent::quarantineId).distinct().count();
  }

  private static Properties consumerProperties(String groupId) {
    Properties p = new Properties();
    p.put("bootstrap.servers", KAFKA.getBootstrapServers());
    p.put("group.id", groupId);
    p.put("auto.offset.reset", "earliest");
    p.put("enable.auto.commit", "false");
    p.put("key.deserializer", ByteArrayDeserializer.class.getName());
    p.put("value.deserializer", ByteArrayDeserializer.class.getName());
    return p;
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

  private static void produceLoad(List<String> topics, int trial, int events, int actors) {
    Properties p = new Properties();
    p.put("bootstrap.servers", KAFKA.getBootstrapServers());
    p.put("key.serializer", ByteArraySerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    try (var producer = new KafkaProducer<byte[], byte[]>(p)) {
      for (int i = 0; i < events; i++) {
        String actor = "load-" + trial + "-" + (i % actors);
        String payload = eventForActor("load-" + trial + "-" + i, actor, i, 1);
        producer.send(
            new ProducerRecord<>(
                topics.get(i % topics.size()),
                actor.getBytes(StandardCharsets.UTF_8),
                payload.getBytes(StandardCharsets.UTF_8)));
      }
      producer.flush();
    }
  }

  private static KafkaSource<IngestedSafetyRecord> source(String topic, String groupId) {
    return source(List.of(topic), groupId);
  }

  private static KafkaSource<IngestedSafetyRecord> source(List<String> topics, String groupId) {
    return KafkaSource.<IngestedSafetyRecord>builder()
        .setBootstrapServers(KAFKA.getBootstrapServers())
        .setTopics(topics)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setBounded(OffsetsInitializer.latest())
        .setDeserializer(new SafetyEventDeserializer())
        .build();
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

  private static String eventForActor(String id, String actor, int millis, int severity) {
    String timestamp = java.time.Instant.ofEpochMilli(1_787_054_400_000L + millis).toString();
    return "{\"schema_version\":1,\"event_id\":\""
        + id
        + "\",\"occurred_at\":\""
        + timestamp
        + "\",\"ingested_at\":\""
        + timestamp
        + "\",\"actor_id\":\""
        + actor
        + "\",\"event_type\":\"POLICY_MATCH\",\"severity\":"
        + severity
        + "}";
  }

  private static int positiveProperty(String name, int defaultValue) {
    int value = Integer.parseInt(System.getProperty(name, Integer.toString(defaultValue)));
    if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }

  private static double percentile(double[] sorted, double quantile) {
    int index = Math.max(0, (int) Math.ceil(quantile * sorted.length) - 1);
    return sorted[index];
  }

  private static double round(double value) {
    return Math.round(value * 1_000.0) / 1_000.0;
  }
}
