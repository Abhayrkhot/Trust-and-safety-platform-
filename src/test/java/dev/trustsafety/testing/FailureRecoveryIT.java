package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

class FailureRecoveryIT {
  @Test
  void boundedJobRestartsAfterInjectedFailureAndReachesCompleteUniqueResult() throws Exception {
    String id = "recovery-it";
    RecordingSink.reset();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    Configuration restart = new Configuration();
    restart.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    restart.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    restart.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, java.time.Duration.ZERO);
    env.configure(restart);
    env.fromSequence(1, 100).map(new FailureInjector<Long>(id, 50)).addSink(new RecordingSink());
    env.execute("failure-recovery-it");
    assertThat(RecordingSink.VALUES)
        .containsExactlyInAnyOrderElementsOf(
            java.util.stream.LongStream.rangeClosed(1, 100).boxed().toList());
  }

  @SuppressWarnings("deprecation")
  static final class RecordingSink extends RichSinkFunction<Long> {
    private static final long serialVersionUID = 1L;

    static final Set<Long> VALUES = ConcurrentHashMap.newKeySet();

    static void reset() {
      VALUES.clear();
    }

    @Override
    public void invoke(Long value, SinkFunction.Context ignored) {
      VALUES.add(value);
    }
  }
}
