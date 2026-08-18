package dev.trustsafety.rules;

import static org.assertj.core.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RuleConfigLoaderTest {
  @TempDir Path temp;

  @Test
  void loadsProductionConfig() throws Exception {
    var rules = RuleConfigLoader.load(Path.of("conf/safety-rules.json"));
    assertThat(rules)
        .singleElement()
        .satisfies(
            r -> {
              assertThat(r.ruleId()).isEqualTo("burst-severity-v1");
              assertThat(r.eventTypes()).hasSize(2);
            });
  }

  @Test
  void rejectsDuplicateIdsUnknownFieldsAndInvalidEnums() throws Exception {
    assertBad("{\"rules\":[{" + base() + "},{" + base() + "}]}", "duplicate rule_id");
    assertBad("{\"rules\":[{" + base() + ",\"surprise\":1}]}", "unknown rule field");
    assertBad("{\"rules\":[{" + base() + ",\"event_types\":[\"NOPE\"]}]}", "unknown event_type");
  }

  private String base() {
    return "\"rule_id\":\"r\",\"window_ms\":1000,\"minimum_events\":1,\"minimum_severity_sum\":0,\"risk_score\":10";
  }

  private void assertBad(String json, String message) throws Exception {
    Path file = temp.resolve(java.util.UUID.randomUUID() + ".json");
    Files.writeString(file, json);
    assertThatThrownBy(() -> RuleConfigLoader.load(file)).hasMessageContaining(message);
  }
}
