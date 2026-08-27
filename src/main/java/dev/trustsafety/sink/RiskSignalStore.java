package dev.trustsafety.sink;

import dev.trustsafety.model.RiskSignal;

public interface RiskSignalStore extends AutoCloseable {
  void write(RiskSignal signal) throws Exception;
  default void flush() throws Exception {}
  @Override void close() throws Exception;
}
