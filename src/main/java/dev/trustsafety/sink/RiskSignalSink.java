package dev.trustsafety.sink;

import dev.trustsafety.model.RiskSignal;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/** A synchronous at-least-once sink. Stores must make replay idempotent. */
public final class RiskSignalSink implements Sink<RiskSignal> {
  private final RiskSignalStoreFactory factory;
  public RiskSignalSink(RiskSignalStoreFactory factory) { this.factory = factory; }

  @SuppressWarnings("deprecation")
  public SinkWriter<RiskSignal> createWriter(Sink.InitContext context) throws IOException { return newWriter(); }
  @Override public SinkWriter<RiskSignal> createWriter(WriterInitContext context) throws IOException { return newWriter(); }

  private SinkWriter<RiskSignal> newWriter() throws IOException {
    try { return new Writer(factory.create()); }
    catch (Exception e) { throw new IOException("failed to create risk signal store", e); }
  }

  static final class Writer implements SinkWriter<RiskSignal> {
    private final RiskSignalStore store;
    Writer(RiskSignalStore store) { this.store = store; }
    @Override public void write(RiskSignal signal, Context context) throws IOException {
      try { store.write(signal); } catch (Exception e) { throw new IOException("risk signal write failed", e); }
    }
    @Override public void flush(boolean endOfInput) throws IOException {
      try { store.flush(); } catch (Exception e) { throw new IOException("risk signal flush failed", e); }
    }
    @Override public void close() throws Exception { store.close(); }
  }
}
