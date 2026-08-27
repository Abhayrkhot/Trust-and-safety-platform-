package dev.trustsafety.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.trustsafety.serde.QuarantinedEventJson;
import dev.trustsafety.serde.QuarantinedEventJsonTest;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class QuarantineKafkaSerializationSchemaTest {
  @Test
  void writesStableKeyAndVersionedValueToConfiguredTopic() throws Exception {
    var event = QuarantinedEventJsonTest.event();
    var record =
        new QuarantineKafkaSerializationSchema("events-quarantine").serialize(event, null, 9L);
    assertThat(record.topic()).isEqualTo("events-quarantine");
    assertThat(new String(record.key(), StandardCharsets.UTF_8)).isEqualTo(event.quarantineId());
    assertThat(record.timestamp()).isEqualTo(9L);
    assertThat(QuarantinedEventJson.decode(record.value())).isEqualTo(event);
  }

  @Test
  void rejectsBlankTopic() {
    assertThatThrownBy(() -> new QuarantineKafkaSerializationSchema(" "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("topic");
  }
}
