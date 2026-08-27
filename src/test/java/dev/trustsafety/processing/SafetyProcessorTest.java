package dev.trustsafety.processing;

import static org.assertj.core.api.Assertions.assertThat;

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
