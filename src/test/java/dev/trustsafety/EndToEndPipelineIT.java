package dev.trustsafety;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.Admin;
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
  private static final ObjectMapper JSON = new ObjectMapper();

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
  @Timeout(600)
  void benchmarksBackloggedKafkaThroughFlinkIntoBothServingStores() throws Exception {
    int largeEvents = positiveProperty("endToEndBenchmarkEvents", 60_000);
    int largeActors = positiveProperty("endToEndBenchmarkActors", 6_000);
    int warmupPairs = nonNegativeProperty("endToEndBenchmarkWarmupPairs", 0);
    int measuredPairs = positiveProperty("endToEndBenchmarkMeasuredPairs", 1);
    if (largeEvents % 2 != 0 || largeActors % 2 != 0)
      throw new IllegalArgumentException("paired benchmark events and actors must be even");
    int smallEvents = largeEvents / 2;
    int smallActors = largeActors / 2;
    if (largeEvents % largeActors != 0 || smallEvents % smallActors != 0)
      throw new IllegalArgumentException(
          "benchmark events must be divisible by actors at both sizes");
    int eventsPerActor = largeEvents / largeActors;
    List<LoadRun> allRuns = new ArrayList<>();
    List<LoadRun> measuredSmall = new ArrayList<>();
    List<LoadRun> measuredLarge = new ArrayList<>();

    int totalPairs = warmupPairs + measuredPairs;
    for (int pair = 0; pair < totalPairs; pair++) {
      boolean warmup = pair < warmupPairs;
      int measuredPair = warmup ? 0 : pair - warmupPairs + 1;
      boolean smallFirst = pair % 2 == 0;
      int[] eventOrder =
          smallFirst ? new int[] {smallEvents, largeEvents} : new int[] {largeEvents, smallEvents};
      int[] actorOrder =
          smallFirst ? new int[] {smallActors, largeActors} : new int[] {largeActors, smallActors};
      for (int position = 0; position < eventOrder.length; position++) {
        String runId =
            (warmup ? "warmup-" + (pair + 1) : "measure-" + measuredPair)
                + "-"
                + eventOrder[position];
        LoadRun run =
            executeObservedLoadRun(
                runId,
                pair + 1,
                measuredPair,
                warmup,
                position + 1,
                eventOrder[position],
                actorOrder[position],
                eventsPerActor);
        allRuns.add(run);
        if (!warmup) {
          if (run.events() == smallEvents) measuredSmall.add(run);
          else measuredLarge.add(run);
        }
      }
    }

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("benchmark", "paired-backlogged-kafka-to-serving-stores");
    result.put("revision", System.getProperty("endToEndBenchmarkRevision", "unspecified"));
    result.put("small_events", smallEvents);
    result.put("large_events", largeEvents);
    result.put("small_actors", smallActors);
    result.put("large_actors", largeActors);
    result.put("events_per_actor", eventsPerActor);
    result.put("kafka_topics", 4);
    result.put("flink_parallelism", 4);
    result.put("warmup_pairs_excluded", warmupPairs);
    result.put("measured_pairs", measuredPairs);
    result.put("alternating_size_order", true);
    result.put("runs", allRuns.stream().map(LoadRun::asMap).toList());
    Map<String, Object> sizeStatistics = new LinkedHashMap<>();
    sizeStatistics.put(Integer.toString(smallEvents), runStatistics(measuredSmall));
    sizeStatistics.put(Integer.toString(largeEvents), runStatistics(measuredLarge));
    result.put("measured_size_statistics", sizeStatistics);
    result.put(
        "startup_decomposition",
        startupDecomposition(smallEvents, largeEvents, measuredSmall, measuredLarge));
    result.put(
        "post_run_cleanup",
        "run-scoped Redis keys and Kafka topics deleted and ClickHouse benchmark table truncated after oracle validation; cleanup excluded from timed span");
    result.put("redis_image", "redis:8.2-alpine");
    result.put("clickhouse_image", "clickhouse/clickhouse-server:25.8-alpine");
    result.put("kafka_image", "apache/kafka-native:3.8.0");
    result.put("java_version", System.getProperty("java.version"));
    result.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
    result.put("available_processors", Runtime.getRuntime().availableProcessors());
    result.put(
        "scope",
        "local preloaded Kafka backlogs through Flink and synchronous Redis/ClickHouse sinks; producer time excluded; raw elapsed time includes job startup; decomposition assumes elapsed(N)=startup+N/rate");
    Path output = Path.of("target", "benchmark-results", "end-to-end-load.json");
    Files.createDirectories(output.getParent());
    JSON.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), result);
    System.out.println(JSON.writeValueAsString(result));
  }

  private static LoadRun executeObservedLoadRun(
      String runId,
      int pair,
      int measuredPair,
      boolean warmup,
      int orderInPair,
      int events,
      int actors,
      int eventsPerActor)
      throws Exception {
    List<String> topics =
        List.of(
            "load-content-" + runId,
            "load-activity-" + runId,
            "load-moderation-" + runId,
            "load-enforcement-" + runId);
    String ruleId = "load-rule-" + runId;
    produceLoad(topics, runId, events, actors);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(4);
    var signals =
        SafetyStreamJob.buildEvaluationPipeline(
            env,
            source(topics, "load-group-" + runId),
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
    env.execute("end-to-end-load-" + runId);
    long elapsedNanos = System.nanoTime() - started;
    double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
    String expectedChecksum = expectedResultChecksum(runId, ruleId, events, actors, eventsPerActor);
    StoreObservation redis = observeRedis(redisUri, runId, ruleId, actors, eventsPerActor);
    StoreObservation clickHouse = observeClickHouse(runId, ruleId, actors, eventsPerActor);
    assertObservedStore(redis, actors, expectedChecksum);
    assertObservedStore(clickHouse, actors, expectedChecksum);
    assertThat(redis.checksumSha256()).isEqualTo(clickHouse.checksumSha256());
    cleanupLoadRun(redisUri, topics, runId);
    return new LoadRun(
        runId,
        pair,
        measuredPair,
        warmup,
        orderInPair,
        events,
        actors,
        elapsedSeconds,
        events / elapsedSeconds,
        expectedChecksum,
        redis,
        clickHouse);
  }

  private static void cleanupLoadRun(String redisUri, List<String> topics, String runId)
      throws Exception {
    RedisClient redis = RedisClient.create(redisUri);
    try (var connection = redis.connect()) {
      List<String> keys = connection.sync().keys("safety:risk:actor:load-" + runId + "-*");
      if (!keys.isEmpty()) connection.sync().del(keys.toArray(String[]::new));
    } finally {
      redis.shutdown();
    }
    try (var connection =
            DriverManager.getConnection(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
        var statement = connection.createStatement()) {
      statement.execute("TRUNCATE TABLE risk_signals");
    }
    Properties adminProperties = new Properties();
    adminProperties.put("bootstrap.servers", KAFKA.getBootstrapServers());
    try (Admin admin = Admin.create(adminProperties)) {
      admin.deleteTopics(topics).all().get();
    }
  }

  private static StoreObservation observeRedis(
      String redisUri, String runId, String ruleId, int actors, int eventsPerActor)
      throws Exception {
    RedisClient redis = RedisClient.create(redisUri);
    try (var connection = redis.connect()) {
      var commands = connection.sync();
      List<String> keys = commands.keys("safety:risk:actor:load-" + runId + "-*");
      Set<String> distinctActors = new LinkedHashSet<>();
      List<String> canonicalRecords = new ArrayList<>();
      int payloads = 0;
      int mismatches = 0;
      for (String key : keys) {
        String payload = commands.hget(key, "payload");
        if (payload == null) {
          mismatches++;
          continue;
        }
        payloads++;
        JsonNode value = JSON.readTree(payload);
        String actorId = value.path("actor_id").asText();
        distinctActors.add(actorId);
        ResultFields actual =
            new ResultFields(
                value.path("signal_id").asText(),
                actorId,
                value.path("triggering_event_id").asText(),
                value.path("rule_id").asText(),
                value.path("risk_score").asInt(),
                value.path("observed_event_count").asLong(),
                value.path("observed_severity_sum").asLong());
        ResultFields expected = expectedResult(runId, ruleId, actorId, actors, eventsPerActor);
        if (!actual.equals(expected)) mismatches++;
        canonicalRecords.add(actual.canonical());
      }
      return new StoreObservation(
          keys.size(), distinctActors.size(), payloads, mismatches, checksum(canonicalRecords));
    } finally {
      redis.shutdown();
    }
  }

  private static StoreObservation observeClickHouse(
      String runId, String ruleId, int actors, int eventsPerActor) throws Exception {
    Set<String> distinctActors = new LinkedHashSet<>();
    List<String> canonicalRecords = new ArrayList<>();
    int rowsObserved = 0;
    int mismatches = 0;
    try (var connection =
            DriverManager.getConnection(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT signal_id,actor_id,triggering_event_id,rule_id,risk_score,observed_event_count,observed_severity_sum FROM risk_signals FINAL WHERE rule_id='"
                    + ruleId
                    + "'")) {
      while (rows.next()) {
        rowsObserved++;
        ResultFields actual =
            new ResultFields(
                rows.getString(1),
                rows.getString(2),
                rows.getString(3),
                rows.getString(4),
                rows.getInt(5),
                rows.getLong(6),
                rows.getLong(7));
        distinctActors.add(actual.actorId());
        ResultFields expected =
            expectedResult(runId, ruleId, actual.actorId(), actors, eventsPerActor);
        if (!actual.equals(expected)) mismatches++;
        canonicalRecords.add(actual.canonical());
      }
    }
    return new StoreObservation(
        rowsObserved, distinctActors.size(), rowsObserved, mismatches, checksum(canonicalRecords));
  }

  private static ResultFields expectedResult(
      String runId, String ruleId, String actorId, int actors, int eventsPerActor) {
    String actorPrefix = "load-" + runId + "-";
    if (!actorId.startsWith(actorPrefix)) return ResultFields.invalid(actorId);
    int actorIndex;
    try {
      actorIndex = Integer.parseInt(actorId.substring(actorPrefix.length()));
    } catch (NumberFormatException ignored) {
      return ResultFields.invalid(actorId);
    }
    if (actorIndex < 0 || actorIndex >= actors) return ResultFields.invalid(actorId);
    String triggeringEventId = "load-" + runId + "-" + (actorIndex + (eventsPerActor - 1) * actors);
    String signalId =
        UUID.nameUUIDFromBytes((triggeringEventId + ":" + ruleId).getBytes(StandardCharsets.UTF_8))
            .toString();
    return new ResultFields(
        signalId, actorId, triggeringEventId, ruleId, 90, eventsPerActor, eventsPerActor);
  }

  private static String expectedResultChecksum(
      String runId, String ruleId, int events, int actors, int eventsPerActor) {
    assertThat(events).isEqualTo(actors * eventsPerActor);
    List<String> records = new ArrayList<>(actors);
    for (int actor = 0; actor < actors; actor++)
      records.add(
          expectedResult(runId, ruleId, "load-" + runId + "-" + actor, actors, eventsPerActor)
              .canonical());
    return checksum(records);
  }

  private static void assertObservedStore(
      StoreObservation observation, int actors, String expectedChecksum) {
    assertThat(observation.records()).isEqualTo(actors);
    assertThat(observation.distinctActors()).isEqualTo(actors);
    assertThat(observation.payloads()).isEqualTo(actors);
    assertThat(observation.recordsWithFieldMismatch()).isZero();
    assertThat(observation.checksumSha256()).isEqualTo(expectedChecksum);
  }

  private static String checksum(List<String> records) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      records.stream()
          .sorted()
          .forEach(record -> digest.update((record + "\n").getBytes(StandardCharsets.UTF_8)));
      return HexFormat.of().formatHex(digest.digest());
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 unavailable", impossible);
    }
  }

  private static Map<String, Object> runStatistics(List<LoadRun> runs) {
    Map<String, Object> statistics = new LinkedHashMap<>();
    statistics.put(
        "elapsed_seconds", statistics(runs.stream().map(LoadRun::elapsedSeconds).toList()));
    statistics.put(
        "events_per_second", statistics(runs.stream().map(LoadRun::eventsPerSecond).toList()));
    Map<String, Object> orderStatistics = new LinkedHashMap<>();
    for (int order = 1; order <= 2; order++) {
      int position = order;
      orderStatistics.put(
          Integer.toString(order),
          statisticsOrNull(
              runs.stream()
                  .filter(run -> run.orderInPair() == position)
                  .map(LoadRun::eventsPerSecond)
                  .toList()));
    }
    statistics.put("events_per_second_by_order_in_pair", orderStatistics);
    statistics.put("sequence_drift", sequenceDrift(runs));
    return statistics;
  }

  private static Map<String, Object> sequenceDrift(List<LoadRun> runs) {
    List<LoadRun> ordered =
        runs.stream().sorted(Comparator.comparingInt(LoadRun::measuredPair)).toList();
    int split = ordered.size() / 2;
    Map<String, Object> result = new LinkedHashMap<>();
    if (split == 0) {
      result.put("n", ordered.size());
      return result;
    }
    double firstHalfMean =
        ordered.subList(0, split).stream()
            .mapToDouble(LoadRun::eventsPerSecond)
            .average()
            .orElseThrow();
    double secondHalfMean =
        ordered.subList(split, ordered.size()).stream()
            .mapToDouble(LoadRun::eventsPerSecond)
            .average()
            .orElseThrow();
    double meanPair = ordered.stream().mapToInt(LoadRun::measuredPair).average().orElseThrow();
    double meanThroughput =
        ordered.stream().mapToDouble(LoadRun::eventsPerSecond).average().orElseThrow();
    double numerator =
        ordered.stream()
            .mapToDouble(
                run -> (run.measuredPair() - meanPair) * (run.eventsPerSecond() - meanThroughput))
            .sum();
    double denominator =
        ordered.stream()
            .mapToDouble(run -> (run.measuredPair() - meanPair) * (run.measuredPair() - meanPair))
            .sum();
    result.put("n", ordered.size());
    result.put("first_half_mean_events_per_second", round(firstHalfMean));
    result.put("second_half_mean_events_per_second", round(secondHalfMean));
    result.put(
        "second_half_vs_first_half_percent",
        round(firstHalfMean == 0 ? 0 : (secondHalfMean - firstHalfMean) / firstHalfMean * 100));
    result.put(
        "linear_slope_events_per_second_per_measured_pair",
        round(denominator == 0 ? 0 : numerator / denominator));
    return result;
  }

  private static Map<String, Object> startupDecomposition(
      int smallEvents, int largeEvents, List<LoadRun> smallRuns, List<LoadRun> largeRuns) {
    Map<String, LoadRun> smallByPair =
        smallRuns.stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    run -> Integer.toString(run.measuredPair()), run -> run));
    Map<String, LoadRun> largeByPair =
        largeRuns.stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    run -> Integer.toString(run.measuredPair()), run -> run));
    List<Double> pairedRates = new ArrayList<>();
    List<Double> pairedStartups = new ArrayList<>();
    List<Map<String, Object>> pairs = new ArrayList<>();
    int negativeStartupEstimates = 0;
    for (int pair = 1; pair <= smallRuns.size(); pair++) {
      LoadRun small = smallByPair.get(Integer.toString(pair));
      LoadRun large = largeByPair.get(Integer.toString(pair));
      double deltaSeconds = large.elapsedSeconds() - small.elapsedSeconds();
      Map<String, Object> observation = new LinkedHashMap<>();
      observation.put("measured_pair", pair);
      observation.put("small_elapsed_seconds", round(small.elapsedSeconds()));
      observation.put("large_elapsed_seconds", round(large.elapsedSeconds()));
      observation.put("valid_positive_elapsed_delta", deltaSeconds > 0);
      if (deltaSeconds > 0) {
        double rate = (largeEvents - smallEvents) / deltaSeconds;
        double startup = small.elapsedSeconds() - smallEvents / rate;
        pairedRates.add(rate);
        pairedStartups.add(startup);
        if (startup < 0) negativeStartupEstimates++;
        observation.put("estimated_steady_state_events_per_second", round(rate));
        observation.put("estimated_startup_seconds", round(startup));
      }
      pairs.add(observation);
    }
    double medianSmall = median(smallRuns.stream().map(LoadRun::elapsedSeconds).toList());
    double medianLarge = median(largeRuns.stream().map(LoadRun::elapsedSeconds).toList());
    double medianDelta = medianLarge - medianSmall;
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("model", "elapsed(N)=startup+N/rate");
    result.put("assumption", "startup and steady-state rate are stable across paired sizes");
    result.put("small_median_elapsed_seconds", round(medianSmall));
    result.put("large_median_elapsed_seconds", round(medianLarge));
    result.put("valid_positive_median_elapsed_delta", medianDelta > 0);
    if (medianDelta > 0) {
      double medianRate = (largeEvents - smallEvents) / medianDelta;
      result.put("median_based_steady_state_events_per_second", round(medianRate));
      result.put("median_based_startup_seconds", round(medianSmall - smallEvents / medianRate));
    }
    result.put("paired_estimates", pairs);
    result.put("negative_startup_estimates", negativeStartupEstimates);
    result.put("necessary_nonnegative_startup_condition_met", negativeStartupEstimates == 0);
    result.put(
        "interpretation",
        negativeStartupEstimates == 0
            ? "model remains an estimate and requires stationarity review"
            : "diagnostic only: negative startup estimates contradict the fixed-startup model under these run conditions");
    result.put("valid_paired_rate_statistics", statisticsOrNull(pairedRates));
    result.put("valid_paired_startup_statistics", statisticsOrNull(pairedStartups));
    return result;
  }

  private static Map<String, Object> statisticsOrNull(List<Double> values) {
    return values.isEmpty() ? Map.of("n", 0) : statistics(values);
  }

  private static Map<String, Object> statistics(List<Double> values) {
    double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
    double mean = Arrays.stream(sorted).average().orElseThrow();
    double squared = Arrays.stream(sorted).map(value -> (value - mean) * (value - mean)).sum();
    double sampleStandardDeviation =
        sorted.length > 1 ? Math.sqrt(squared / (sorted.length - 1)) : 0;
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("n", sorted.length);
    result.put("mean", round(mean));
    result.put("median", round(median(Arrays.stream(sorted).boxed().toList())));
    result.put("sample_standard_deviation", round(sampleStandardDeviation));
    boolean coefficientOfVariationApplicable = mean > 0;
    result.put("coefficient_of_variation_applicable", coefficientOfVariationApplicable);
    result.put(
        "coefficient_of_variation_percent",
        coefficientOfVariationApplicable ? round(sampleStandardDeviation / mean * 100) : null);
    result.put("min", round(sorted[0]));
    result.put("max", round(sorted[sorted.length - 1]));
    return result;
  }

  private static double median(List<Double> values) {
    double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
    int middle = sorted.length / 2;
    return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
  }

  private record ResultFields(
      String signalId,
      String actorId,
      String triggeringEventId,
      String ruleId,
      int riskScore,
      long observedEventCount,
      long observedSeveritySum) {
    private static ResultFields invalid(String actorId) {
      return new ResultFields("", actorId, "", "", -1, -1, -1);
    }

    private String canonical() {
      return String.join(
          "|",
          signalId,
          actorId,
          triggeringEventId,
          ruleId,
          Integer.toString(riskScore),
          Long.toString(observedEventCount),
          Long.toString(observedSeveritySum));
    }
  }

  private record StoreObservation(
      int records,
      int distinctActors,
      int payloads,
      int recordsWithFieldMismatch,
      String checksumSha256) {
    private Map<String, Object> asMap() {
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("observed_records", records);
      result.put("observed_distinct_actors", distinctActors);
      result.put("observed_payloads", payloads);
      result.put("observed_records_with_field_mismatch", recordsWithFieldMismatch);
      result.put("observed_checksum_sha256", checksumSha256);
      return result;
    }
  }

  private record LoadRun(
      String runId,
      int pair,
      int measuredPair,
      boolean warmup,
      int orderInPair,
      int events,
      int actors,
      double elapsedSeconds,
      double eventsPerSecond,
      String expectedChecksumSha256,
      StoreObservation redis,
      StoreObservation clickHouse) {
    private Map<String, Object> asMap() {
      Map<String, Object> result = new LinkedHashMap<>();
      result.put("run_id", runId);
      result.put("pair", pair);
      result.put("measured_pair", warmup ? null : measuredPair);
      result.put("warmup_excluded", warmup);
      result.put("order_in_pair", orderInPair);
      result.put("events", events);
      result.put("actors", actors);
      result.put("elapsed_seconds", round(elapsedSeconds));
      result.put("events_per_second", round(eventsPerSecond));
      result.put("expected_result_checksum_sha256", expectedChecksumSha256);
      result.put("redis_observation", redis.asMap());
      result.put("clickhouse_observation", clickHouse.asMap());
      return result;
    }
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

  private static void produceLoad(List<String> topics, String runId, int events, int actors) {
    Properties p = new Properties();
    p.put("bootstrap.servers", KAFKA.getBootstrapServers());
    p.put("key.serializer", ByteArraySerializer.class.getName());
    p.put("value.serializer", ByteArraySerializer.class.getName());
    try (var producer = new KafkaProducer<byte[], byte[]>(p)) {
      for (int i = 0; i < events; i++) {
        String actor = "load-" + runId + "-" + (i % actors);
        String payload = eventForActor("load-" + runId + "-" + i, actor, i, 1);
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

  private static int nonNegativeProperty(String name, int defaultValue) {
    int value = Integer.parseInt(System.getProperty(name, Integer.toString(defaultValue)));
    if (value < 0) throw new IllegalArgumentException(name + " must not be negative");
    return value;
  }

  private static double round(double value) {
    return Math.round(value * 1_000.0) / 1_000.0;
  }
}
