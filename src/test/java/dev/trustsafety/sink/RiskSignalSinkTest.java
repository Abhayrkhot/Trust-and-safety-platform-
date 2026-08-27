package dev.trustsafety.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.trustsafety.model.RiskSignal;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class RiskSignalSinkTest {
  @Test
  void writesFlushesAndCloses() throws Exception {
    var store = new RecordingStore();
    var writer = new RiskSignalSink.Writer(store);
    var signal = signal("s1", 1);
    writer.write(signal, null);
    writer.flush(false);
    writer.close();
    assertThat(store.values).containsExactly(signal);
    assertThat(store.flushed).isTrue();
    assertThat(store.closed).isTrue();
  }

  @Test
  void convertsStoreFailureToIOException() {
    var writer =
        new RiskSignalSink.Writer(
            new RecordingStore() {
              @Override
              public void write(RiskSignal ignored) {
                throw new IllegalStateException("down");
              }
            });
    assertThatThrownBy(() -> writer.write(signal("s", 1), null))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("write failed")
        .hasRootCauseMessage("down");
  }

  private static class RecordingStore implements RiskSignalStore {
    final ArrayList<RiskSignal> values = new ArrayList<>();
    boolean flushed, closed;

    public void write(RiskSignal s) {
      values.add(s);
    }

    public void flush() {
      flushed = true;
    }

    public void close() {
      closed = true;
    }
  }

  static RiskSignal signal(String id, long millis) {
    return new RiskSignal(
        id, "actor", "event", "rule", 80, "reason", 3, 120, Instant.ofEpochMilli(millis));
  }
}
