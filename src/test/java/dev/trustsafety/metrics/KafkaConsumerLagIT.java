package dev.trustsafety.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Properties;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class KafkaConsumerLagIT {
  @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka-native:3.8.0");

  @Test
  @Timeout(30)
  void flinkPendingRecordsReportsRealKafkaOffsetLagAndDrainsToZero() throws Exception {
    String topic = "consumer-lag";
    produce(topic, 5);
    TopicPartition partition = new TopicPartition(topic, 0);

    try (KafkaConsumer<String, String> consumer = consumer()) {
      consumer.assign(java.util.List.of(partition));
      consumer.seekToBeginning(java.util.List.of(partition));
      assertThat(pollUntilAtLeast(consumer, 1)).isEqualTo(1);

      MetricListener listener = new MetricListener();
      var sourceMetricGroup = InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
      var connectorMetrics = new KafkaSourceReaderMetrics(sourceMetricGroup);
      connectorMetrics.maybeAddRecordsLagMetric(consumer, partition);
      Gauge<Long> pendingRecords = listener.<Long>getGauge("pendingRecords").orElseThrow();

      assertThat(pendingRecords.getValue()).isEqualTo(4);
      assertThat(pollUntilAtLeast(consumer, 4)).isEqualTo(4);
      assertThat(pendingRecords.getValue()).isZero();
      consumer.commitSync();
    }
  }

  private static KafkaConsumer<String, String> consumer() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-lag-test");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return new KafkaConsumer<>(properties);
  }

  private static int pollUntilAtLeast(KafkaConsumer<String, String> consumer, int expected) {
    int consumed = 0;
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (consumed < expected && System.nanoTime() < deadline) {
      consumed += consumer.poll(Duration.ofMillis(250)).count();
    }
    return consumed;
  }

  private static void produce(String topic, int count) throws Exception {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
      for (int i = 0; i < count; i++) {
        producer.send(new ProducerRecord<>(topic, 0, "key-" + i, "value-" + i)).get();
      }
      producer.flush();
    }
  }
}
