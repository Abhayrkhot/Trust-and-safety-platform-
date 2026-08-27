package dev.trustsafety.serde;

import dev.trustsafety.model.IngestedSafetyRecord;
import dev.trustsafety.model.QuarantinedEvent;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class SafetyEventDeserializer
    implements KafkaRecordDeserializationSchema<IngestedSafetyRecord> {
  private static final long serialVersionUID = 1L;
  private static final int MAX_FAILURE_MESSAGE_LENGTH = 512;
  static final int MAX_QUARANTINE_PAYLOAD_BYTES = 256 * 1024;

  @Override
  public void deserialize(
      ConsumerRecord<byte[], byte[]> record, Collector<IngestedSafetyRecord> out)
      throws IOException {
    try {
      out.collect(IngestedSafetyRecord.accepted(SafetyEventJson.decode(record.value())));
    } catch (SafetyEventDecodingException e) {
      byte[] payload = record.value() == null ? new byte[0] : record.value();
      byte[] payloadPreview =
          payload.length <= MAX_QUARANTINE_PAYLOAD_BYTES
              ? payload
              : Arrays.copyOf(payload, MAX_QUARANTINE_PAYLOAD_BYTES);
      String id = record.topic() + ":" + record.partition() + ":" + record.offset();
      out.collect(
          IngestedSafetyRecord.rejected(
              new QuarantinedEvent(
                  1,
                  id,
                  record.topic(),
                  record.partition(),
                  record.offset(),
                  record.timestamp(),
                  e.reason(),
                  boundedMessage(e),
                  payload.length,
                  payloadPreview.length != payload.length,
                  Base64.getEncoder().encodeToString(payloadPreview),
                  sha256(payload))));
    }
  }

  @Override
  public TypeInformation<IngestedSafetyRecord> getProducedType() {
    return TypeInformation.of(IngestedSafetyRecord.class);
  }

  private static String boundedMessage(SafetyEventDecodingException e) {
    String message = e.getMessage() == null ? e.reason().name() : e.getMessage();
    return message.substring(0, Math.min(message.length(), MAX_FAILURE_MESSAGE_LENGTH));
  }

  private static String sha256(byte[] payload) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required by the Java platform", e);
    }
  }
}
