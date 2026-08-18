package dev.trustsafety;

import static org.assertj.core.api.Assertions.assertThat;
import dev.trustsafety.model.SafetyEvent;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SafetyStreamJobTest {
  @Test void watermarkUsesOccurredAt(){var e=new SafetyEvent(1,"e",Instant.ofEpochMilli(1234),Instant.ofEpochMilli(9999),"a",null,SafetyEvent.EventType.USER_BLOCK,1,Map.of());var assigner=SafetyStreamJob.watermarkStrategy().createTimestampAssigner(() -> null);assertThat(assigner.extractTimestamp(e,0)).isEqualTo(1234);}
}
