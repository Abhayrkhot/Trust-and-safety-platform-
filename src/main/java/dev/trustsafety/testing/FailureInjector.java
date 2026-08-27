package dev.trustsafety.testing;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;

/** Opt-in deterministic fail-once operator for recovery tests and staging drills. */
public final class FailureInjector<T> extends RichMapFunction<T, T> implements CheckpointListener {
  private static final long serialVersionUID = 1L;

  private final String injectionId;
  private final long failAfter;
  private long seen;
  private int attemptNumber;
  private LongCounter completedCheckpoints = new LongCounter();

  public FailureInjector(String injectionId, long failAfter) {
    if (injectionId == null || injectionId.isBlank() || failAfter <= 0)
      throw new IllegalArgumentException("invalid failure injection configuration");
    this.injectionId = injectionId;
    this.failAfter = failAfter;
  }

  @Override
  public void open(OpenContext ignored) {
    seen = 0;
    attemptNumber = getRuntimeContext().getTaskInfo().getAttemptNumber();
    completedCheckpoints = getRuntimeContext().getLongCounter(checkpointAccumulator(injectionId));
  }

  @Override
  public T map(T value) {
    seen++;
    if (shouldInject(seen, failAfter, attemptNumber))
      throw new InjectedFailureException(injectionId, failAfter);
    return value;
  }

  static boolean shouldInject(long seen, long failAfter, int attemptNumber) {
    return attemptNumber == 0 && seen == failAfter;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    completedCheckpoints.add(1);
  }

  public static String checkpointAccumulator(String injectionId) {
    return "failure_injector." + injectionId + ".completed_checkpoints";
  }

  public static final class InjectedFailureException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public InjectedFailureException(String id, long after) {
      super("injected failure '" + id + "' after " + after + " records");
    }
  }
}
