package dev.trustsafety.serde;

import static org.assertj.core.api.Assertions.assertThat;

import dev.trustsafety.model.IngestedSafetyRecord;
import dev.trustsafety.serde.SafetyEventDecodingException.Reason;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class SafetyEventDeserializerTest {
  private final SafetyEventDeserializer deserializer = new SafetyEventDeserializer();

  @Test
  void decodesKafkaRecordValue() throws Exception {
    var output = new ArrayList<IngestedSafetyRecord>();
    deserializer.deserialize(record(validJson()), collector(output));
    assertThat(output)
        .singleElement()
        .satisfies(
            result -> {
              assertThat(result.accepted()).isTrue();
              assertThat(result.event().eventId()).isEqualTo("evt-1");
              assertThat(result.quarantine()).isNull();
            });
  }

  @Test
  void quarantinesMalformedKafkaRecordWithStableEvidence() throws Exception {
    var output = new ArrayList<IngestedSafetyRecord>();
    deserializer.deserialize(record("not-json"), collector(output));
    assertThat(output)
        .singleElement()
        .satisfies(
            result -> {
              assertThat(result.accepted()).isFalse();
              assertThat(result.event()).isNull();
              assertThat(result.quarantine().quarantineId()).isEqualTo("safety-events:0:1");
              assertThat(result.quarantine().failureReason()).isEqualTo(Reason.MALFORMED_JSON);
              assertThat(result.quarantine().originalPayloadBytes()).isEqualTo(8);
              assertThat(result.quarantine().payloadTruncated()).isFalse();
              assertThat(result.quarantine().payloadBase64()).isEqualTo("bm90LWpzb24=");
              assertThat(result.quarantine().payloadSha256())
                  .isEqualTo("0c21a879c732a67910d80988df4919d794f6a070aab610ef865032a28046b021");
            });
  }

  @Test
  void boundsQuarantinePayloadButHashesTheCompletePoisonRecord() throws Exception {
    byte[] payload = new byte[SafetyEventDeserializer.MAX_QUARANTINE_PAYLOAD_BYTES + 1];
    java.util.Arrays.fill(payload, (byte) 'x');
    var output = new ArrayList<IngestedSafetyRecord>();
    deserializer.deserialize(
        new ConsumerRecord<>("safety-events", 1, 9L, null, payload), collector(output));
    var quarantine = output.get(0).quarantine();
    assertThat(quarantine.originalPayloadBytes()).isEqualTo(payload.length);
    assertThat(quarantine.payloadTruncated()).isTrue();
    assertThat(java.util.Base64.getDecoder().decode(quarantine.payloadBase64()))
        .hasSize(SafetyEventDeserializer.MAX_QUARANTINE_PAYLOAD_BYTES);
    assertThat(quarantine.payloadSha256())
        .isEqualTo("9408817bc040328d1c403ff97e94cea3dfd3a361b2fb7ecc7af75ff3a5157139");
  }

  @Test
  void classifiesNullAndUnsupportedPayloadsWithoutHidingThem() throws Exception {
    var output = new ArrayList<IngestedSafetyRecord>();
    deserializer.deserialize(record(null), collector(output));
    deserializer.deserialize(record("{\"schema_version\":7}"), collector(output));
    assertThat(output)
        .extracting(result -> result.quarantine().failureReason())
        .containsExactly(Reason.NULL_PAYLOAD, Reason.UNSUPPORTED_SCHEMA_VERSION);
    assertThat(output.get(0).quarantine().payloadBase64()).isEmpty();
  }

  private static ConsumerRecord<byte[], byte[]> record(String value) {
    return new ConsumerRecord<>(
        "safety-events",
        0,
        1L,
        null,
        value == null ? null : value.getBytes(StandardCharsets.UTF_8));
  }

  private static Collector<IngestedSafetyRecord> collector(ArrayList<IngestedSafetyRecord> output) {
    return new Collector<>() {
      @Override
      public void collect(IngestedSafetyRecord event) {
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
