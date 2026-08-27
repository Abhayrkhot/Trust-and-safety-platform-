package dev.trustsafety.sink;

import static org.assertj.core.api.Assertions.assertThat;

import io.lettuce.core.RedisClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class RedisHotStateStoreIT {
  @Container
  static final GenericContainer<?> REDIS =
      new GenericContainer<>(DockerImageName.parse("redis:8.2-alpine")).withExposedPorts(6379);

  @Test
  void newerSignalWinsReplayIsIdempotentAndTtlIsApplied() throws Exception {
    String uri = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    var newer = RiskSignalSinkTest.signal("newer", 2_000);
    var stale = RiskSignalSinkTest.signal("stale", 1_000);
    try (var store = new RedisHotStateStore(uri, Duration.ofMinutes(5))) {
      store.write(newer);
      store.write(newer);
      store.write(stale);
    }
    RedisClient client = RedisClient.create(uri);
    try (var connection = client.connect()) {
      String key = RedisHotStateStore.key("actor");
      assertThat(connection.sync().hget(key, "payload"))
          .contains("newer")
          .contains("\"triggering_event_id\":\"event\"")
          .doesNotContain("stale");
      assertThat(connection.sync().pttl(key)).isBetween(1L, 300_000L);
    } finally {
      client.shutdown();
    }
  }
}
