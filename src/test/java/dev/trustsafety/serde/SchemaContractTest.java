package dev.trustsafety.serde;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.dialect.Draft202012;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class SchemaContractTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void publishedSchemasAcceptTheirVersionsAndRejectCrossVersionPayloads() throws Exception {
    Schema v1 = schema("schemas/safety-event-v1.schema.json"),
        v2 = schema("schemas/safety-event-v2.schema.json");
    JsonNode one = JSON.readTree(payload(1, false)), two = JSON.readTree(payload(2, true));
    assertThat(v1.validate(one)).isEmpty();
    assertThat(v2.validate(two)).isEmpty();
    assertThat(v1.validate(two)).isNotEmpty();
    assertThat(v2.validate(one)).isNotEmpty();
    assertThat(SafetyEventJson.decode(JSON.writeValueAsBytes(one)).schemaVersion()).isEqualTo(1);
    assertThat(SafetyEventJson.decode(JSON.writeValueAsBytes(two)).schemaVersion()).isEqualTo(2);
  }

  @Test
  void schemasRejectUnknownFieldsAndOutOfRangeSeverity() throws Exception {
    Schema v2 = schema("schemas/safety-event-v2.schema.json");
    assertThat(
            v2.validate(
                JSON.readTree(payload(2, true).replace("\"severity\":40", "\"severity\":101"))))
        .isNotEmpty();
    assertThat(v2.validate(JSON.readTree(payload(2, true).replace("}", ",\"unknown\":true}"))))
        .isNotEmpty();
  }

  @Test
  void publishedQuarantineSchemaMatchesTheStrictCodec() throws Exception {
    Schema quarantineSchema = schema("schemas/quarantine-event-v1.schema.json");
    byte[] encoded = QuarantinedEventJson.encode(QuarantinedEventJsonTest.event());
    JsonNode valid = JSON.readTree(encoded);
    assertThat(quarantineSchema.validate(valid)).isEmpty();
    assertThat(QuarantinedEventJson.decode(encoded)).isEqualTo(QuarantinedEventJsonTest.event());
    ObjectNode invalid = valid.deepCopy();
    invalid.put("payload_truncated", "no");
    assertThat(quarantineSchema.validate(invalid)).isNotEmpty();
  }

  private static Schema schema(String path) throws Exception {
    return SchemaRegistry.withDefaultDialect(Draft202012.getInstance())
        .getSchema(JSON.readTree(Path.of(path).toFile()));
  }

  private static String payload(int version, boolean tenant) {
    return "{\"schema_version\":"
        + version
        + ",\"event_id\":\"e\",\"occurred_at\":\"2026-08-18T00:00:00Z\",\"ingested_at\":\"2026-08-18T00:00:01Z\","
        + (tenant ? "\"tenant_id\":\"t\",\"trace_id\":\"trace\"," : "")
        + "\"actor_id\":\"a\",\"event_type\":\"CONTENT_REPORT\",\"severity\":40}";
  }
}
