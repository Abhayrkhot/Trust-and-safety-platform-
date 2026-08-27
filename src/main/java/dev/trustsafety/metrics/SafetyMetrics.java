package dev.trustsafety.metrics;

import com.codahale.metrics.SlidingWindowReservoir;
import dev.trustsafety.model.SafetyEvent;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;

/** Central metric contract exported by Flink reporters, including Prometheus. */
public final class SafetyMetrics {
  private final Counter events, duplicates, signals, historyExpired, lateBeyondHistory;
  private final Counter stateCapacityBreaches, stateCapacityEvictions;
  private final Meter throughput;
  private final Histogram latency, historySize;
  private final AtomicLong eventTimeLagMillis = new AtomicLong();

  public SafetyMetrics(MetricGroup root) {
    MetricGroup group = root.addGroup("trust_safety");
    events = group.counter("events_total");
    duplicates = group.counter("duplicates_total");
    signals = group.counter("risk_signals_total");
    historyExpired = group.counter("history_expired_events_total");
    lateBeyondHistory = group.counter("late_events_beyond_history_total");
    stateCapacityBreaches = group.counter("state_capacity_breaches_total");
    stateCapacityEvictions = group.counter("state_capacity_evictions_total");
    throughput = group.meter("events_per_second", new MeterView(events, 60));
    latency =
        group.histogram(
            "processing_latency_ms",
            new DropwizardHistogramWrapper(
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(10_000))));
    historySize =
        group.histogram(
            "history_events_per_actor",
            new DropwizardHistogramWrapper(
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(10_000))));
    group.gauge("event_time_lag_ms", eventTimeLagMillis::get);
  }

  public void onEvent(SafetyEvent event, long processingTimeMillis) {
    throughput.markEvent();
    latency.update(Math.max(0, processingTimeMillis - event.ingestedAt().toEpochMilli()));
    eventTimeLagMillis.set(Math.max(0, processingTimeMillis - event.occurredAt().toEpochMilli()));
  }

  public void onDuplicate() {
    duplicates.inc();
  }

  public void onSignal() {
    signals.inc();
  }

  public void onHistoryExpired(long count) {
    historyExpired.inc(count);
  }

  public void onLateBeyondHistory() {
    lateBeyondHistory.inc();
  }

  public void onStateCapacityBreach(long evicted) {
    stateCapacityBreaches.inc();
    stateCapacityEvictions.inc(evicted);
  }

  public void onHistorySize(int size) {
    historySize.update(size);
  }
}
