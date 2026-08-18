package dev.trustsafety.serde;

import dev.trustsafety.model.SafetyEvent;
import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class SafetyEventDeserializer implements KafkaRecordDeserializationSchema<SafetyEvent> {
  @Override public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<SafetyEvent> out) throws IOException {
    out.collect(SafetyEventJson.decode(record.value()));
  }
  @Override public TypeInformation<SafetyEvent> getProducedType() { return TypeInformation.of(SafetyEvent.class); }
}
