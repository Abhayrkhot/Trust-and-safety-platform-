package dev.trustsafety.processing;

import dev.trustsafety.metrics.SafetyMetrics;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.rules.RuleConfig;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public final class SafetyProcessor extends KeyedProcessFunction<String, SafetyEvent, RiskSignal> {
  private static final long serialVersionUID = 1L;
  public static final int DEFAULT_MAX_HISTORY_EVENTS_PER_ACTOR = 100_000;
  public static final String STATE_CAPACITY_RULE_ID = "__state_capacity__";
  private static final Comparator<SafetyEvent> EVENT_ORDER =
      Comparator.comparing(SafetyEvent::occurredAt).thenComparing(SafetyEvent::eventId);

  private final List<RuleConfig> rules;
  private final long dedupTtlMillis;
  private final long maxWindowMillis;
  private final int maxHistoryEventsPerActor;
  private transient MapState<String, Boolean> seen;
  private transient ListState<SafetyEvent> history;
  private transient ValueState<Long> maxEventTime;
  private transient ValueState<Long> cleanupTimer;
  private transient SafetyMetrics metrics;

  public SafetyProcessor(List<RuleConfig> rules, long dedupTtlMillis) {
    this(rules, dedupTtlMillis, DEFAULT_MAX_HISTORY_EVENTS_PER_ACTOR);
  }

  public SafetyProcessor(
      List<RuleConfig> rules, long dedupTtlMillis, int maxHistoryEventsPerActor) {
    if (rules == null || rules.isEmpty())
      throw new IllegalArgumentException("at least one rule is required");
    if (dedupTtlMillis <= 0) throw new IllegalArgumentException("dedup TTL must be positive");
    if (maxHistoryEventsPerActor <= 0)
      throw new IllegalArgumentException("max history events per actor must be positive");
    if (rules.stream().anyMatch(rule -> rule.minimumEvents() > maxHistoryEventsPerActor))
      throw new IllegalArgumentException(
          "max history events per actor must satisfy every rule minimumEvents threshold");
    if (rules.stream().anyMatch(rule -> STATE_CAPACITY_RULE_ID.equals(rule.ruleId())))
      throw new IllegalArgumentException("ruleId " + STATE_CAPACITY_RULE_ID + " is reserved");
    this.rules = List.copyOf(rules);
    this.dedupTtlMillis = dedupTtlMillis;
    this.maxHistoryEventsPerActor = maxHistoryEventsPerActor;
    this.maxWindowMillis = rules.stream().mapToLong(RuleConfig::windowMillis).max().orElseThrow();
  }

  @Override
  public void open(Configuration ignored) {
    MapStateDescriptor<String, Boolean> descriptor =
        new MapStateDescriptor<>("seen-event-ids", String.class, Boolean.class);
    descriptor.enableTimeToLive(
        StateTtlConfig.newBuilder(java.time.Duration.ofMillis(dedupTtlMillis))
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
    cleanupTimer =
        getRuntimeContext()
            .getState(new ValueStateDescriptor<>("history-cleanup-timer", Long.class));
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
    long cutoff = subtractSaturated(windowEnd, maxWindowMillis);
    List<SafetyEvent> retained = new ArrayList<>();
    long expired = 0;
    for (SafetyEvent prior : history.get()) {
      if (prior.occurredAt().toEpochMilli() >= cutoff
          && prior.occurredAt().toEpochMilli() <= windowEnd) {
        retained.add(prior);
      } else {
        expired++;
      }
    }
    metrics.onHistoryExpired(expired);
    if (eventTime < cutoff) {
      metrics.onLateBeyondHistory();
      history.update(retained);
      metrics.onHistorySize(retained.size());
      scheduleCleanup(retained, ctx.timerService());
      return;
    }
    retained.add(event);
    retained.sort(EVENT_ORDER);
    int evicted = Math.max(0, retained.size() - maxHistoryEventsPerActor);
    if (evicted > 0) {
      retained.subList(0, evicted).clear();
      metrics.onStateCapacityBreach(evicted);
      metrics.onSignal();
      out.collect(capacitySignal(event, retained, windowEnd, evicted));
    }
    history.update(retained);
    metrics.onHistorySize(retained.size());
    scheduleCleanup(retained, ctx.timerService());
    for (RuleConfig rule : rules) {
      if (!rule.matches(event)) continue;
      long ruleCutoff = subtractSaturated(windowEnd, rule.windowMillis());
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
                stableSignalId(event.eventId(), rule.ruleId()),
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

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<RiskSignal> out)
      throws Exception {
    Long scheduled = cleanupTimer.value();
    if (scheduled == null || scheduled != timestamp) return;
    long cutoff = subtractSaturated(timestamp, maxWindowMillis);
    List<SafetyEvent> retained = new ArrayList<>();
    long expired = 0;
    for (SafetyEvent event : history.get()) {
      if (event.occurredAt().toEpochMilli() >= cutoff) {
        retained.add(event);
      } else {
        expired++;
      }
    }
    metrics.onHistoryExpired(expired);
    metrics.onHistorySize(retained.size());
    if (retained.isEmpty()) history.clear();
    else history.update(retained);
    cleanupTimer.clear();
    scheduleCleanup(retained, ctx.timerService());
  }

  private RiskSignal capacitySignal(
      SafetyEvent triggeringEvent, List<SafetyEvent> retained, long windowEnd, int evicted) {
    long severity = retained.stream().mapToLong(SafetyEvent::severity).sum();
    return new RiskSignal(
        stableSignalId(triggeringEvent.eventId(), STATE_CAPACITY_RULE_ID),
        triggeringEvent.actorId(),
        triggeringEvent.eventId(),
        STATE_CAPACITY_RULE_ID,
        100,
        "history_capacity="
            + maxHistoryEventsPerActor
            + ", evicted="
            + evicted
            + ", operator_action_required=true",
        retained.size(),
        severity,
        Instant.ofEpochMilli(windowEnd));
  }

  private void scheduleCleanup(List<SafetyEvent> retained, TimerService timers) throws IOException {
    Long previous = cleanupTimer.value();
    if (retained.isEmpty()) {
      if (previous != null) timers.deleteEventTimeTimer(previous);
      cleanupTimer.clear();
      return;
    }
    long earliest = expirationTime(retained.get(0).occurredAt().toEpochMilli());
    if (previous != null && previous == earliest) return;
    if (previous != null) timers.deleteEventTimeTimer(previous);
    timers.registerEventTimeTimer(earliest);
    cleanupTimer.update(earliest);
  }

  private long expirationTime(long eventTime) {
    if (eventTime > Long.MAX_VALUE - maxWindowMillis - 1) return Long.MAX_VALUE;
    return eventTime + maxWindowMillis + 1;
  }

  private static long subtractSaturated(long value, long amount) {
    return value < Long.MIN_VALUE + amount ? Long.MIN_VALUE : value - amount;
  }

  private static String stableSignalId(String eventId, String ruleId) {
    return UUID.nameUUIDFromBytes(
            (eventId + ":" + ruleId).getBytes(java.nio.charset.StandardCharsets.UTF_8))
        .toString();
  }
}
