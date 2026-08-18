package dev.trustsafety.model;

import dev.trustsafety.serde.SafetyEventDecodingException.Reason;
import java.io.Serializable;
import java.util.Base64;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable poison-record evidence written to the quarantine topic. */
public record QuarantinedEvent(
    int schemaVersion,
    String quarantineId,
    String sourceTopic,
    int sourcePartition,
    long sourceOffset,
    long sourceTimestamp,
    Reason failureReason,
    String failureMessage,
    int originalPayloadBytes,
    boolean payloadTruncated,
    String payloadBase64,
    String payloadSha256)
    implements Serializable {
  private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

  public QuarantinedEvent {
    if (schemaVersion != 1) throw new IllegalArgumentException("unsupported quarantine schema");
    requireText(quarantineId, "quarantine_id");
    requireText(sourceTopic, "source_topic");
    if (sourcePartition < 0) throw new IllegalArgumentException("source_partition must be >= 0");
    if (sourceOffset < 0) throw new IllegalArgumentException("source_offset must be >= 0");
    if (!quarantineId.equals(sourceTopic + ":" + sourcePartition + ":" + sourceOffset))
      throw new IllegalArgumentException("quarantine_id must match the source coordinate");
    Objects.requireNonNull(failureReason, "failure_reason");
    requireText(failureMessage, "failure_message");
    if (failureMessage.length() > 512)
      throw new IllegalArgumentException("failure_message must be at most 512 characters");
    if (originalPayloadBytes < 0)
      throw new IllegalArgumentException("original_payload_bytes must be >= 0");
    Objects.requireNonNull(payloadBase64, "payload_base64");
    requireText(payloadSha256, "payload_sha256");
    if (!SHA256.matcher(payloadSha256).matches())
      throw new IllegalArgumentException("payload_sha256 must be 64 lowercase hex characters");
    int previewLength;
    try {
      previewLength = Base64.getDecoder().decode(payloadBase64).length;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("payload_base64 must be valid base64", e);
    }
    if ((!payloadTruncated && previewLength != originalPayloadBytes)
        || (payloadTruncated && previewLength >= originalPayloadBytes))
      throw new IllegalArgumentException(
          "payload preview length is inconsistent with truncation metadata");
  }

  private static void requireText(String value, String field) {
    if (value == null || value.isBlank())
      throw new IllegalArgumentException(field + " must not be blank");
  }
}
