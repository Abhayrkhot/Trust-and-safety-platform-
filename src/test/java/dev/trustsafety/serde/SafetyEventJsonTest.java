package dev.trustsafety.serde;

import static org.assertj.core.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class SafetyEventJsonTest {
  private static final String VALID="""
      {"schema_version":1,"event_id":"evt-1","occurred_at":"2026-08-18T12:00:00Z","ingested_at":"2026-08-18T12:00:01Z","actor_id":"user-7","content_id":"video-9","event_type":"CONTENT_REPORT","severity":45,"attributes":{"country":"US"}}
      """;
  @Test void decodesV1(){var event=unchecked(VALID);assertThat(event.eventId()).isEqualTo("evt-1");assertThat(event.attributes()).containsEntry("country","US");}
  @Test void rejectsUnknownVersion(){assertThatThrownBy(()->decode(VALID.replace("\"schema_version\":1","\"schema_version\":2"))).hasMessageContaining("unsupported schema_version");}
  @Test void rejectsUnknownFields(){assertThatThrownBy(()->decode(VALID.replace("}",",\"surprise\":true}"))).hasMessageContaining("unknown field");}
  @Test void rejectsOutOfRangeSeverity(){assertThatThrownBy(()->decode(VALID.replace("\"severity\":45","\"severity\":101"))).hasMessageContaining("severity");}
  private static dev.trustsafety.model.SafetyEvent unchecked(String json){try{return decode(json);}catch(Exception e){throw new AssertionError(e);}}
  private static dev.trustsafety.model.SafetyEvent decode(String json)throws Exception{return SafetyEventJson.decode(json.getBytes(StandardCharsets.UTF_8));}
}
