package dev.trustsafety.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.trustsafety.model.SafetyEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class SafetyEventDeserializerTest {
  private final SafetyEventDeserializer deserializer = new SafetyEventDeserializer();

  @Test
  void decodesKafkaRecordValue() throws Exception {
    var output = new ArrayList<SafetyEvent>();
    deserializer.deserialize(record(validJson()), collector(output));
    assertThat(output).singleElement().extracting(SafetyEvent::eventId).isEqualTo("evt-1");
  }

  @Test
  void rejectsMalformedKafkaRecordWithoutEmitting() {
    var output = new ArrayList<SafetyEvent>();
    assertThatThrownBy(() -> deserializer.deserialize(record("not-json"), collector(output)))
        .isInstanceOf(java.io.IOException.class);
    assertThat(output).isEmpty();
  }

  private static ConsumerRecord<byte[], byte[]> record(String value) {
    return new ConsumerRecord<>(
        "safety-events", 0, 1L, null, value.getBytes(StandardCharsets.UTF_8));
  }

  private static Collector<SafetyEvent> collector(ArrayList<SafetyEvent> output) {
    return new Collector<>() {
      @Override
      public void collect(SafetyEvent event) {
        output.add(event);
      }

      @Override
      public void close() {}
    };
  }

  private static String validJson() {
    return """
        {"schema_version":1,"event_id":"evt-1","occurred_at":"2026-08-18T12:00:00Z","ingested_at":"2026-08-18T12:00:01Z","actor_id":"user-7","event_type":"CONTENT_REPORT","severity":45}
        """;
  }
}
