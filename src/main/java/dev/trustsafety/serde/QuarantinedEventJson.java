package dev.trustsafety.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.trustsafety.model.QuarantinedEvent;
import java.io.IOException;
import java.util.Set;

/** Strict versioned JSON contract for the Kafka quarantine topic. */
public final class QuarantinedEventJson {
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final Set<String> FIELDS =
      Set.of(
          "schema_version",
          "quarantine_id",
          "source_topic",
          "source_partition",
          "source_offset",
          "source_timestamp",
          "failure_reason",
          "failure_message",
          "original_payload_bytes",
          "payload_truncated",
          "payload_base64",
          "payload_sha256");

  private QuarantinedEventJson() {}

  public static byte[] encode(QuarantinedEvent event) throws IOException {
    var n = MAPPER.createObjectNode();
    n.put("schema_version", event.schemaVersion());
    n.put("quarantine_id", event.quarantineId());
    n.put("source_topic", event.sourceTopic());
    n.put("source_partition", event.sourcePartition());
    n.put("source_offset", event.sourceOffset());
    n.put("source_timestamp", event.sourceTimestamp());
    n.put("failure_reason", event.failureReason().name());
    n.put("failure_message", event.failureMessage());
    n.put("original_payload_bytes", event.originalPayloadBytes());
    n.put("payload_truncated", event.payloadTruncated());
    n.put("payload_base64", event.payloadBase64());
    n.put("payload_sha256", event.payloadSha256());
    return MAPPER.writeValueAsBytes(n);
  }

  public static QuarantinedEvent decode(byte[] payload) throws IOException {
    JsonNode n = MAPPER.readTree(payload);
    if (n == null || !n.isObject()) throw new IOException("quarantine record must be an object");
    var fields = n.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      if (!FIELDS.contains(field)) throw new IOException("unknown field: " + field);
    }
    try {
      return new QuarantinedEvent(
          requiredInt(n, "schema_version"),
          requiredText(n, "quarantine_id"),
          requiredText(n, "source_topic"),
          requiredInt(n, "source_partition"),
          requiredLong(n, "source_offset"),
          requiredLong(n, "source_timestamp"),
          SafetyEventDecodingException.Reason.valueOf(requiredText(n, "failure_reason")),
          requiredText(n, "failure_message"),
          requiredInt(n, "original_payload_bytes"),
          requiredBoolean(n, "payload_truncated"),
          requiredTextAllowEmpty(n, "payload_base64"),
          requiredText(n, "payload_sha256"));
    } catch (RuntimeException e) {
      throw new IOException("invalid quarantine record: " + e.getMessage(), e);
    }
  }

  private static String requiredText(JsonNode n, String field) {
    String value = requiredTextAllowEmpty(n, field);
    if (value.isBlank()) throw new IllegalArgumentException(field + " must not be blank");
    return value;
  }

  private static String requiredTextAllowEmpty(JsonNode n, String field) {
    JsonNode value = n.get(field);
    if (value == null || !value.isTextual())
      throw new IllegalArgumentException(field + " must be text");
    return value.textValue();
  }

  private static int requiredInt(JsonNode n, String field) {
    JsonNode value = n.get(field);
    if (value == null || !value.canConvertToInt())
      throw new IllegalArgumentException(field + " must be integer");
    return value.intValue();
  }

  private static long requiredLong(JsonNode n, String field) {
    JsonNode value = n.get(field);
    if (value == null || !value.canConvertToLong())
      throw new IllegalArgumentException(field + " must be long");
    return value.longValue();
  }

  private static boolean requiredBoolean(JsonNode n, String field) {
    JsonNode value = n.get(field);
    if (value == null || !value.isBoolean())
      throw new IllegalArgumentException(field + " must be boolean");
    return value.booleanValue();
  }
}
