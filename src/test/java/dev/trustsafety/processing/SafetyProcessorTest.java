package dev.trustsafety.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.rules.RuleConfig;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class SafetyProcessorTest {
  @Test
  void rejectsInvalidCapacityAndReservedRuleId() {
    assertThatThrownBy(
            () -> new SafetyProcessor(List.of(new RuleConfig("r1", 1_000, 1, 0, 50)), 10_000, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("max history");
    assertThatThrownBy(
            () -> new SafetyProcessor(List.of(new RuleConfig("r1", 1_000, 3, 0, 50)), 10_000, 2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("minimumEvents");
    assertThatThrownBy(
            () ->
                new SafetyProcessor(
                    List.of(
                        new RuleConfig(SafetyProcessor.STATE_CAPACITY_RULE_ID, 1_000, 1, 0, 50)),
                    10_000,
                    10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("reserved");
  }

  @Test
  void deduplicatesAndEmitsOnThreshold() throws Exception {
    var processor =
        new SafetyProcessor(List.of(new RuleConfig("r1", 60_000, 3, 100, 75)), 86_400_000);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(processor), SafetyEvent::actorId, Types.STRING);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(event("e1", 0, 40)));
      harness.processElement(new StreamRecord<>(event("e1", 0, 40)));
      harness.processElement(new StreamRecord<>(event("e2", 1_000, 30)));
      assertThat(harness.extractOutputValues()).isEmpty();
      harness.processElement(new StreamRecord<>(event("e3", 2_000, 35)));
      var output = harness.extractOutputValues();
      assertThat(output).hasSize(1);
      assertThat(output.get(0).observedEventCount()).isEqualTo(3);
      assertThat(output.get(0).observedSeveritySum()).isEqualTo(105);
    } finally {
      harness.close();
    }
  }

  @Test
  void excludesEventsOutsideWindow() throws Exception {
    var processor = new SafetyProcessor(List.of(new RuleConfig("r1", 1_000, 2, 1, 50)), 10_000);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(processor), SafetyEvent::actorId, Types.STRING);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(event("old", 0, 10)));
      harness.processElement(new StreamRecord<>(event("new", 2_000, 10)));
      assertThat(harness.extractOutputValues()).isEmpty();
    } finally {
      harness.close();
    }
  }

  @Test
  void checkpointRestorePreservesDedupState() throws Exception {
    var rule = List.of(new RuleConfig("r1", 60_000, 2, 1, 50));
    OperatorSubtaskState snapshot;
    var first =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new SafetyProcessor(rule, 10_000)),
            SafetyEvent::actorId,
            Types.STRING);
    try {
      first.open();
      first.processElement(new StreamRecord<>(event("e1", 0, 10)));
      snapshot = first.snapshot(1, 1);
    } finally {
      first.close();
    }
    var restored =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new SafetyProcessor(rule, 10_000)),
            SafetyEvent::actorId,
            Types.STRING);
    try {
      restored.initializeState(snapshot);
      restored.open();
      assertThat(restored.numEventTimeTimers()).isOne();
      restored.processElement(new StreamRecord<>(event("e1", 0, 10)));
      restored.processElement(new StreamRecord<>(event("e2", 1_000, 10)));
      assertThat(restored.extractOutputValues()).hasSize(1);
    } finally {
      restored.close();
    }
  }

  @Test
  void lateEventDoesNotEraseNewerHistory() throws Exception {
    var processor = new SafetyProcessor(List.of(new RuleConfig("r1", 10_000, 3, 1, 50)), 20_000);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(processor), SafetyEvent::actorId, Types.STRING);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(event("newer", 10_000, 10)));
      harness.processElement(new StreamRecord<>(event("late", 5_000, 10)));
      harness.processElement(new StreamRecord<>(event("newest", 11_000, 10)));
      assertThat(harness.extractOutputValues()).hasSize(1);
    } finally {
      harness.close();
    }
  }

  @Test
  void eventTimeTimerReclaimsIdleHistory() throws Exception {
    var processor = new SafetyProcessor(List.of(new RuleConfig("r1", 1_000, 2, 1, 50)), 10_000);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(processor), SafetyEvent::actorId, Types.STRING);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(event("e1", 0, 10)));
      int stateEntriesBeforeCleanup = harness.numKeyedStateEntries();
      assertThat(harness.numEventTimeTimers()).isOne();

      harness.processWatermark(1_001);

      assertThat(harness.numEventTimeTimers()).isZero();
      assertThat(harness.numKeyedStateEntries()).isLessThan(stateEntriesBeforeCleanup);
    } finally {
      harness.close();
    }
  }

  @Test
  void capsHotActorHistoryByEventTimeAndEmitsOperationalRiskSignal() throws Exception {
    var processor =
        new SafetyProcessor(List.of(new RuleConfig("r1", 60_000, 2, 1_000, 50)), 120_000, 2);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(processor), SafetyEvent::actorId, Types.STRING);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(event("e1", 10, 10)));
      harness.processElement(new StreamRecord<>(event("e2", 20, 20)));
      harness.processElement(new StreamRecord<>(event("e3", 5, 30)));

      assertThat(harness.extractOutputValues())
          .singleElement()
          .satisfies(
              signal -> {
                assertThat(signal.ruleId()).isEqualTo(SafetyProcessor.STATE_CAPACITY_RULE_ID);
                assertThat(signal.triggeringEventId()).isEqualTo("e3");
                assertThat(signal.riskScore()).isEqualTo(100);
                assertThat(signal.observedEventCount()).isEqualTo(2);
                assertThat(signal.observedSeveritySum()).isEqualTo(30);
                assertThat(signal.reason())
                    .contains("history_capacity=2")
                    .contains("operator_action_required=true");
              });
    } finally {
      harness.close();
    }
  }

  @Test
  void doesNotEmitRuleSignalForEventOlderThanRetentionHorizon() throws Exception {
    var processor = new SafetyProcessor(List.of(new RuleConfig("r1", 1_000, 1, 0, 50)), 10_000);
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(processor), SafetyEvent::actorId, Types.STRING);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(event("new", 10_000, 10)));
      harness.getOutput().clear();

      harness.processElement(new StreamRecord<>(event("too-late", 8_999, 10)));

      assertThat(harness.extractOutputValues()).isEmpty();
    } finally {
      harness.close();
    }
  }

  @Test
  void ruleFiltersEventTypeAndRequiredAttributes() throws Exception {
    var rule =
        new RuleConfig(
            "filtered",
            60_000,
            2,
            1,
            50,
            java.util.Set.of(SafetyEvent.EventType.CONTENT_REPORT),
            Map.of("region", "us"));
    var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            new KeyedProcessOperator<>(new SafetyProcessor(List.of(rule), 10_000)),
            SafetyEvent::actorId,
            Types.STRING);
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(event("wrong-type", 0, 10)));
      harness.processElement(
          new StreamRecord<>(
              new SafetyEvent(
                  1,
                  "match-1",
                  Instant.ofEpochMilli(1),
                  Instant.ofEpochMilli(1),
                  "actor",
                  null,
                  SafetyEvent.EventType.CONTENT_REPORT,
                  10,
                  Map.of("region", "us"))));
      harness.processElement(
          new StreamRecord<>(
              new SafetyEvent(
                  1,
                  "wrong-attribute",
                  Instant.ofEpochMilli(2),
                  Instant.ofEpochMilli(2),
                  "actor",
                  null,
                  SafetyEvent.EventType.CONTENT_REPORT,
                  10,
                  Map.of("region", "eu"))));
      assertThat(harness.extractOutputValues()).isEmpty();
      harness.processElement(
          new StreamRecord<>(
              new SafetyEvent(
                  1,
                  "match-2",
                  Instant.ofEpochMilli(3),
                  Instant.ofEpochMilli(3),
                  "actor",
                  null,
                  SafetyEvent.EventType.CONTENT_REPORT,
                  10,
                  Map.of("region", "us"))));
      assertThat(harness.extractOutputValues()).hasSize(1);
    } finally {
      harness.close();
    }
  }

  private static SafetyEvent event(String id, long millis, int severity) {
    return new SafetyEvent(
        1,
        id,
        Instant.ofEpochMilli(millis),
        Instant.ofEpochMilli(millis),
        "actor",
        null,
        SafetyEvent.EventType.POLICY_MATCH,
        severity,
        Map.of());
  }
}
