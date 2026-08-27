package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.assertThat;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.sink.RiskSignalStore;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class BackpressureIT {
  @Test void slowSinkBackpressuresAndDrainsEveryUniqueSignal() throws Exception {
    int events=1_000;SlowStore.reset();StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();env.setParallelism(2);
    var input=LongStream.range(0,events).mapToObj(i->PipelineTestSupport.event(i,50)).toList();
    PipelineTestSupport.build(env,input,SlowStore::new,1);long started=System.nanoTime();env.execute("slow-sink-backpressure-it");long elapsedMillis=(System.nanoTime()-started)/1_000_000;
    assertThat(SlowStore.WRITES.get()).isEqualTo(events);assertThat(SlowStore.SIGNAL_IDS).hasSize(events);
    assertThat(elapsedMillis).as("a 1 ms serial sink must exert measurable downstream pressure").isGreaterThanOrEqualTo(800);
  }
  static final class SlowStore extends PipelineTestSupport.ExactRecordingStore {
    @Override public void write(RiskSignal signal){try{Thread.sleep(1);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new RuntimeException(e);}super.write(signal);}
  }
}
