package dev.trustsafety.processing;

import dev.trustsafety.metrics.SafetyMetrics;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.rules.RuleConfig;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class SafetyProcessor extends KeyedProcessFunction<String, SafetyEvent, RiskSignal> {
  private static final long serialVersionUID = 1L;

  private final List<RuleConfig> rules;
  private final long dedupTtlMillis;
  private final long maxWindowMillis;
  private transient MapState<String, Boolean> seen;
  private transient ListState<SafetyEvent> history;
  private transient ValueState<Long> maxEventTime;
  private transient SafetyMetrics metrics;

  public SafetyProcessor(List<RuleConfig> rules, long dedupTtlMillis) {
    if (rules == null || rules.isEmpty())
      throw new IllegalArgumentException("at least one rule is required");
    if (dedupTtlMillis <= 0) throw new IllegalArgumentException("dedup TTL must be positive");
    this.rules = List.copyOf(rules);
    this.dedupTtlMillis = dedupTtlMillis;
    this.maxWindowMillis = rules.stream().mapToLong(RuleConfig::windowMillis).max().orElseThrow();
  }

  @Override
  public void open(Configuration ignored) {
    MapStateDescriptor<String, Boolean> descriptor =
        new MapStateDescriptor<>("seen-event-ids", String.class, Boolean.class);
    descriptor.enableTimeToLive(
        StateTtlConfig.newBuilder(Time.milliseconds(dedupTtlMillis))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build());
    seen = getRuntimeContext().getMapState(descriptor);
    history =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>("actor-history", TypeInformation.of(SafetyEvent.class)));
    maxEventTime =
        getRuntimeContext().getState(new ValueStateDescriptor<>("max-event-time", Long.class));
    metrics = new SafetyMetrics(getRuntimeContext().getMetricGroup());
  }

  @Override
  public void processElement(SafetyEvent event, Context ctx, Collector<RiskSignal> out)
      throws Exception {
    metrics.onEvent(event, ctx.timerService().currentProcessingTime());
    if (seen.contains(event.eventId())) {
      metrics.onDuplicate();
      return;
    }
    seen.put(event.eventId(), true);
    long eventTime = event.occurredAt().toEpochMilli();
    Long previousMax = maxEventTime.value();
    long windowEnd = previousMax == null ? eventTime : Math.max(previousMax, eventTime);
    maxEventTime.update(windowEnd);
    long cutoff = windowEnd - maxWindowMillis;
    List<SafetyEvent> retained = new ArrayList<>();
    for (SafetyEvent prior : history.get())
      if (prior.occurredAt().toEpochMilli() >= cutoff
          && prior.occurredAt().toEpochMilli() <= windowEnd) retained.add(prior);
    if (eventTime >= cutoff) retained.add(event);
    history.update(retained);
    for (RuleConfig rule : rules) {
      if (!rule.matches(event)) continue;
      long ruleCutoff = windowEnd - rule.windowMillis();
      long count = 0, severity = 0;
      for (SafetyEvent candidate : retained)
        if (rule.matches(candidate)
            && candidate.occurredAt().toEpochMilli() >= ruleCutoff
            && candidate.occurredAt().toEpochMilli() <= windowEnd) {
          count++;
          severity += candidate.severity();
        }
      if (count >= rule.minimumEvents() && severity >= rule.minimumSeveritySum()) {
        metrics.onSignal();
        out.collect(
            new RiskSignal(
                UUID.nameUUIDFromBytes(
                        (event.eventId() + ":" + rule.ruleId())
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .toString(),
                event.actorId(),
                event.eventId(),
                rule.ruleId(),
                rule.riskScore(),
                "events=" + count + ", severity_sum=" + severity,
                count,
                severity,
                Instant.ofEpochMilli(windowEnd)));
      }
    }
  }
}
