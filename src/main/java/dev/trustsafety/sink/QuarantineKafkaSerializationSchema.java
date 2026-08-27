package dev.trustsafety.sink;

import dev.trustsafety.model.QuarantinedEvent;
import dev.trustsafety.serde.QuarantinedEventJson;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Uses the source coordinate as a stable Kafka key so replays remain identifiable. */
public final class QuarantineKafkaSerializationSchema
    implements KafkaRecordSerializationSchema<QuarantinedEvent> {
  private static final long serialVersionUID = 1L;
  private final String topic;

  public QuarantineKafkaSerializationSchema(String topic) {
    if (topic == null || topic.isBlank())
      throw new IllegalArgumentException("quarantine topic must not be blank");
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      QuarantinedEvent event, KafkaSinkContext context, Long timestamp) {
    Objects.requireNonNull(event, "event");
    try {
      return new ProducerRecord<>(
          topic,
          null,
          timestamp,
          event.quarantineId().getBytes(StandardCharsets.UTF_8),
          QuarantinedEventJson.encode(event));
    } catch (java.io.IOException e) {
      throw new IllegalStateException("failed to encode quarantine record", e);
    }
  }
}
