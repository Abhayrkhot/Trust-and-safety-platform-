package dev.trustsafety.sink;

import dev.trustsafety.model.RiskSignal;
import java.io.IOException;

public interface RiskSignalStore extends AutoCloseable {
  void write(RiskSignal signal) throws IOException;

  default void flush() throws IOException {}

  @Override
  void close() throws IOException;
}
