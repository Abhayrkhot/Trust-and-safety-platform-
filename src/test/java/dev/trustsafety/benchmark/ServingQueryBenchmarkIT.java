package dev.trustsafety.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.sink.ClickHouseHistoricalStore;
import dev.trustsafety.sink.RedisHotStateStore;
import io.lettuce.core.RedisClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ServingQueryBenchmarkIT {
  private static final int ACTORS = 200;
  private static final int SIGNALS_PER_ACTOR = 10;

  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

  @Container
  static final ClickHouseContainer CLICKHOUSE =
      new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:25.8-alpine"));

  @Test
  @Timeout(90)
  void measuresCorrectWarmServingQueriesWithoutLatencyThreshold() throws Exception {
    int warmups = positiveProperty("servingBenchmarkWarmups", 50);
    int samples = positiveProperty("servingBenchmarkSamples", 300);
    String redisUri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    loadDataset(redisUri);

    long[] redisNanos = new long[samples];
    long[] clickHouseNanos = new long[samples];
    RedisClient redis = RedisClient.create(redisUri);
    try (var redisConnection = redis.connect();
        var clickHouseConnection =
            DriverManager.getConnection(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
        var query =
            clickHouseConnection.prepareStatement(
                "SELECT signal_id,risk_score FROM risk_signals WHERE actor_id=? ORDER BY emitted_at DESC LIMIT 10")) {
      for (int i = 0; i < warmups; i++) {
        assertRedisQuery(
            redisConnection.sync().hget(RedisHotStateStore.key("actor-100"), "payload"));
        assertClickHouseQuery(query);
      }
      for (int i = 0; i < samples; i++) {
        long started = System.nanoTime();
        String payload =
            redisConnection.sync().hget(RedisHotStateStore.key("actor-100"), "payload");
        assertRedisQuery(payload);
        redisNanos[i] = System.nanoTime() - started;

        started = System.nanoTime();
        assertClickHouseQuery(query);
        clickHouseNanos[i] = System.nanoTime() - started;
      }
    } finally {
      redis.shutdown();
    }

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("benchmark", "warm-serving-query-latency");
    result.put("revision", System.getProperty("servingBenchmarkRevision", "unspecified"));
    result.put("actors", ACTORS);
    result.put("signals_per_actor", SIGNALS_PER_ACTOR);
    result.put("total_signals", ACTORS * SIGNALS_PER_ACTOR);
    result.put("warmup_queries_per_store", warmups);
    result.put("measured_queries_per_store", samples);
    result.put("redis_image", "redis:8.2-alpine");
    result.put("clickhouse_image", "clickhouse/clickhouse-server:25.8-alpine");
    result.put("redis_ms", summary(redisNanos));
    result.put("clickhouse_ms", summary(clickHouseNanos));
    result.put("java_version", System.getProperty("java.version"));
    result.put("os", System.getProperty("os.name") + " " + System.getProperty("os.arch"));
    result.put("available_processors", Runtime.getRuntime().availableProcessors());
    result.put("scope", "local warm cache/container round trips with response validation");
    Path output = Path.of("target", "benchmark-results", "serving-query-latency.json");
    Files.createDirectories(output.getParent());
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(output.toFile(), result);
    System.out.println(new ObjectMapper().writeValueAsString(result));
  }

  private static void loadDataset(String redisUri) throws Exception {
    try (var redis = new RedisHotStateStore(redisUri, Duration.ofHours(1));
        var clickHouse =
            new ClickHouseHistoricalStore(
                CLICKHOUSE.getJdbcUrl(), CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword())) {
      for (int actor = 0; actor < ACTORS; actor++) {
        for (int sequence = 0; sequence < SIGNALS_PER_ACTOR; sequence++) {
          RiskSignal signal = signal(actor, sequence);
          redis.write(signal);
          clickHouse.write(signal);
        }
      }
    }
  }

  private static RiskSignal signal(int actor, int sequence) {
    String actorId = "actor-" + actor;
    return new RiskSignal(
        "signal-" + actor + "-" + sequence,
        actorId,
        "event-" + actor + "-" + sequence,
        "benchmark-rule",
        80,
        "benchmark",
        sequence + 1L,
        (sequence + 1L) * 10,
        Instant.ofEpochMilli(1_700_000_000_000L + sequence));
  }

  private static void assertRedisQuery(String payload) {
    assertThat(payload).contains("signal-100-9").contains("\"actor_id\":\"actor-100\"");
  }

  private static void assertClickHouseQuery(PreparedStatement query) throws Exception {
    query.setString(1, "actor-100");
    int rows = 0;
    try (var result = query.executeQuery()) {
      while (result.next()) {
        assertThat(result.getString(1)).startsWith("signal-100-");
        assertThat(result.getInt(2)).isEqualTo(80);
        rows++;
      }
    }
    assertThat(rows).isEqualTo(SIGNALS_PER_ACTOR);
  }

  private static Map<String, Double> summary(long[] nanos) {
    long[] sorted = nanos.clone();
    Arrays.sort(sorted);
    Map<String, Double> values = new LinkedHashMap<>();
    values.put("p50", millis(percentile(sorted, 0.50)));
    values.put("p95", millis(percentile(sorted, 0.95)));
    values.put("p99", millis(percentile(sorted, 0.99)));
    values.put("max", millis(sorted[sorted.length - 1]));
    return values;
  }

  private static long percentile(long[] sorted, double quantile) {
    int index = Math.max(0, (int) Math.ceil(quantile * sorted.length) - 1);
    return sorted[index];
  }

  private static double millis(long nanos) {
    return Math.round(nanos / 1_000.0) / 1_000.0;
  }

  private static int positiveProperty(String name, int defaultValue) {
    int value = Integer.parseInt(System.getProperty(name, Integer.toString(defaultValue)));
    if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
    return value;
  }
}
