package dev.trustsafety.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.trustsafety.model.RiskSignal;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** Maintains the newest event-time risk state for each actor with an atomic TTL update. */
public final class RedisHotStateStore implements RiskSignalStore {
  private static final String SCRIPT = """
      local current = redis.call('HGET', KEYS[1], 'emitted_at_ms')
      if current and tonumber(current) > tonumber(ARGV[1]) then return 0 end
      redis.call('HSET', KEYS[1], 'emitted_at_ms', ARGV[1], 'payload', ARGV[2])
      redis.call('PEXPIRE', KEYS[1], ARGV[3])
      return 1
      """;
  private static final ObjectMapper JSON = new ObjectMapper();
  private final RedisClient client; private final StatefulRedisConnection<String,String> connection; private final long ttlMillis;

  public RedisHotStateStore(String redisUri, Duration ttl) {
    if (ttl == null || ttl.isZero() || ttl.isNegative()) throw new IllegalArgumentException("Redis TTL must be positive");
    client=RedisClient.create(redisUri); connection=client.connect(); ttlMillis=ttl.toMillis();
  }
  @Override public void write(RiskSignal signal) throws Exception {
    Map<String,Object> value=new LinkedHashMap<>(); value.put("signal_id",signal.signalId()); value.put("actor_id",signal.actorId());
    value.put("rule_id",signal.ruleId()); value.put("risk_score",signal.riskScore()); value.put("reason",signal.reason());
    value.put("observed_event_count",signal.observedEventCount()); value.put("observed_severity_sum",signal.observedSeveritySum());
    value.put("emitted_at",signal.emittedAt().toString());
    connection.sync().eval(SCRIPT, ScriptOutputType.INTEGER, new String[]{key(signal.actorId())},
        Long.toString(signal.emittedAt().toEpochMilli()),JSON.writeValueAsString(value),Long.toString(ttlMillis));
  }
  public static String key(String actorId) { return "safety:risk:actor:"+actorId; }
  @Override public void close() { connection.close(); client.shutdown(); }
}
