package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

class FailureRecoveryIT {
  @Test void boundedJobRestartsAfterInjectedFailureAndReachesCompleteUniqueResult() throws Exception {
    String id="recovery-it";FailureInjector.reset(id);RecordingSink.reset();
    StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();env.setParallelism(1);env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1,0));
    env.fromSequence(1,100).map(new FailureInjector<Long>(id,50)).addSink(new RecordingSink());env.execute("failure-recovery-it");
    assertThat(RecordingSink.VALUES).containsExactlyInAnyOrderElementsOf(java.util.stream.LongStream.rangeClosed(1,100).boxed().toList());
  }
  @SuppressWarnings("deprecation") static final class RecordingSink extends RichSinkFunction<Long>{static final Set<Long> VALUES=ConcurrentHashMap.newKeySet();static void reset(){VALUES.clear();}@Override public void invoke(Long value,SinkFunction.Context ignored){VALUES.add(value);}}
}
