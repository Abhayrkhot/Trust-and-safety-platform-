package dev.trustsafety.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import dev.trustsafety.model.SafetyEvent;
import java.time.Instant;
import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.testutils.MetricListener;
import org.junit.jupiter.api.Test;

class SafetyMetricsTest {
  @Test void registersAndUpdatesEveryMetricIncludingQuantiles(){
    MetricListener listener=new MetricListener();SafetyMetrics metrics=new SafetyMetrics(listener.getMetricGroup());
    metrics.onEvent(event(900,800),1_000);metrics.onEvent(event(950,900),1_000);metrics.onDuplicate();metrics.onSignal();
    assertThat(listener.getCounter("trust_safety","events_total").map(Counter::getCount)).contains(2L);
    assertThat(listener.getCounter("trust_safety","duplicates_total").map(Counter::getCount)).contains(1L);
    assertThat(listener.getCounter("trust_safety","risk_signals_total").map(Counter::getCount)).contains(1L);
    Histogram histogram=listener.getHistogram("trust_safety","processing_latency_ms").orElseThrow();assertThat(histogram.getCount()).isEqualTo(2);assertThat(histogram.getStatistics().getQuantile(.50)).isBetween(100.0,200.0);assertThat(histogram.getStatistics().getQuantile(.95)).isBetween(100.0,200.0);assertThat(histogram.getStatistics().getQuantile(.99)).isBetween(100.0,200.0);
    Gauge<?> lag=listener.getGauge("trust_safety","event_time_lag_ms").orElseThrow();assertThat(lag.getValue()).isEqualTo(50L);
    assertThat(listener.getMeter("trust_safety","events_per_second")).isPresent();
  }
  private static SafetyEvent event(long occurred,long ingested){return new SafetyEvent(1,"e"+occurred,Instant.ofEpochMilli(occurred),Instant.ofEpochMilli(ingested),"a",null,SafetyEvent.EventType.CONTENT_REPORT,1,Map.of());}
}
