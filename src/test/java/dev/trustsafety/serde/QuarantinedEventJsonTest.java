package dev.trustsafety.serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.trustsafety.model.QuarantinedEvent;
import dev.trustsafety.serde.SafetyEventDecodingException.Reason;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class QuarantinedEventJsonTest {
  @Test
  void roundTripsEveryEvidenceField() throws Exception {
    QuarantinedEvent event = event();
    byte[] encoded = QuarantinedEventJson.encode(event);
    assertThat(QuarantinedEventJson.decode(encoded)).isEqualTo(event);
  }

  @Test
  void rejectsUnknownFieldsAndTrailingContent() {
    String valid = new String(uncheckedEncode(event()), StandardCharsets.UTF_8);
    assertThatThrownBy(
            () ->
                QuarantinedEventJson.decode(
                    valid.replace("}", ",\"unknown\":true}").getBytes(StandardCharsets.UTF_8)))
        .isInstanceOf(java.io.IOException.class)
        .hasMessageContaining("unknown field");
    assertThatThrownBy(
            () -> QuarantinedEventJson.decode((valid + "{}").getBytes(StandardCharsets.UTF_8)))
        .isInstanceOf(java.io.IOException.class);
  }

  @Test
  void rejectsIntegrityMetadataThatDoesNotMatchThePayload() {
    String valid = new String(uncheckedEncode(event()), StandardCharsets.UTF_8);
    assertThatThrownBy(
            () ->
                QuarantinedEventJson.decode(
                    valid
                        .replace("\"original_payload_bytes\":2", "\"original_payload_bytes\":3")
                        .getBytes(StandardCharsets.UTF_8)))
        .isInstanceOf(java.io.IOException.class)
        .hasMessageContaining("preview length");
    assertThatThrownBy(
            () ->
                QuarantinedEventJson.decode(
                    valid
                        .replace("\"payload_sha256\":\"", "\"payload_sha256\":\"z")
                        .getBytes(StandardCharsets.UTF_8)))
        .isInstanceOf(java.io.IOException.class)
        .hasMessageContaining("lowercase hex");
  }

  private static byte[] uncheckedEncode(QuarantinedEvent event) {
    try {
      return QuarantinedEventJson.encode(event);
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static QuarantinedEvent event() {
    return new QuarantinedEvent(
        1,
        "events:2:19",
        "events",
        2,
        19,
        1234,
        Reason.CONTRACT_VIOLATION,
        "severity must be in [0,100]",
        2,
        false,
        "e30=",
        "a".repeat(64));
  }
}
