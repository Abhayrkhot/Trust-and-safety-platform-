package dev.trustsafety.sink;

import java.io.Serializable;

@FunctionalInterface
public interface RiskSignalStoreFactory extends Serializable {
  RiskSignalStore create() throws Exception;
}
