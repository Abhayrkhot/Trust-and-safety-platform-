package dev.trustsafety.testing;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

/** Opt-in deterministic fail-once operator for recovery tests and staging drills. */
public final class FailureInjector<T> extends RichMapFunction<T, T> {
  private static final long serialVersionUID = 1L;

  private static final Set<String> FIRED = ConcurrentHashMap.newKeySet();
  private final String injectionId;
  private final long failAfter;
  private long seen;

  public FailureInjector(String injectionId, long failAfter) {
    if (injectionId == null || injectionId.isBlank() || failAfter <= 0)
      throw new IllegalArgumentException("invalid failure injection configuration");
    this.injectionId = injectionId;
    this.failAfter = failAfter;
  }

  @Override
  public void open(OpenContext ignored) {
    seen = 0;
  }

  @Override
  public T map(T value) {
    seen++;
    if (seen == failAfter && FIRED.add(injectionId))
      throw new InjectedFailureException(injectionId, failAfter);
    return value;
  }

  public static void reset(String injectionId) {
    FIRED.remove(injectionId);
  }

  public static final class InjectedFailureException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InjectedFailureException(String id, long after) {
      super("injected failure '" + id + "' after " + after + " records");
    }
  }
}
