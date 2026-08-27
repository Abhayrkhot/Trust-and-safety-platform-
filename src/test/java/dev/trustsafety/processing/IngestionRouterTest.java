package dev.trustsafety.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.trustsafety.model.IngestedSafetyRecord;
import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.serde.QuarantinedEventJsonTest;
import java.time.Instant;
import java.util.Map;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class IngestionRouterTest {
  @Test
  void routesAcceptedAndQuarantinedOutcomesWithoutDroppingEither() throws Exception {
    SafetyEvent event = event();
    var quarantine = QuarantinedEventJsonTest.event();
    var harness =
        new OneInputStreamOperatorTestHarness<IngestedSafetyRecord, SafetyEvent>(
            new ProcessOperator<>(new IngestionRouter()));
    try {
      harness.open();
      harness.processElement(new StreamRecord<>(IngestedSafetyRecord.accepted(event)));
      harness.processElement(new StreamRecord<>(IngestedSafetyRecord.rejected(quarantine)));
      assertThat(harness.extractOutputValues()).containsExactly(event);
      assertThat(harness.getSideOutput(IngestionRouter.QUARANTINE))
          .extracting(StreamRecord::getValue)
          .containsExactly(quarantine);
    } finally {
      harness.close();
    }
  }

  @Test
  void requiresExactlyOneOutcome() {
    assertThatThrownBy(() -> new IngestedSafetyRecord(null, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new IngestedSafetyRecord(event(), QuarantinedEventJsonTest.event()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static SafetyEvent event() {
    return new SafetyEvent(
        1,
        "event-1",
        Instant.EPOCH,
        Instant.EPOCH,
        "actor-1",
        null,
        SafetyEvent.EventType.CONTENT_REPORT,
        10,
        Map.of());
  }
}
