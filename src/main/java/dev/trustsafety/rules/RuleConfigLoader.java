package dev.trustsafety.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.trustsafety.model.SafetyEvent;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class RuleConfigLoader {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Set<String> FIELDS =
      Set.of(
          "rule_id",
          "window_ms",
          "minimum_events",
          "minimum_severity_sum",
          "risk_score",
          "event_types",
          "required_attributes");

  private RuleConfigLoader() {}

  public static List<RuleConfig> load(Path path) throws IOException {
    JsonNode root = JSON.readTree(path.toFile());
    if (root == null || !root.isObject()) throw new IOException("rule config must be an object");
    root.fieldNames()
        .forEachRemaining(
            f -> {
              if (!f.equals("rules"))
                throw new IllegalArgumentException("unknown root field: " + f);
            });
    JsonNode rules = root.get("rules");
    if (rules == null || !rules.isArray() || rules.isEmpty())
      throw new IOException("rules must be a non-empty array");
    List<RuleConfig> result = new ArrayList<>();
    Set<String> ids = new HashSet<>();
    for (JsonNode n : rules) {
      n.fieldNames()
          .forEachRemaining(
              f -> {
                if (!FIELDS.contains(f))
                  throw new IllegalArgumentException("unknown rule field: " + f);
              });
      String id = text(n, "rule_id");
      if (!ids.add(id)) throw new IOException("duplicate rule_id: " + id);
      Set<SafetyEvent.EventType> types = new HashSet<>();
      JsonNode typeNodes = n.get("event_types");
      if (typeNodes != null) {
        if (!typeNodes.isArray()) throw new IOException("event_types must be an array");
        for (JsonNode type : typeNodes)
          try {
            types.add(SafetyEvent.EventType.valueOf(type.asText()));
          } catch (IllegalArgumentException e) {
            throw new IOException("unknown event_type: " + type.asText(), e);
          }
      }
      Map<String, String> attributes = new HashMap<>();
      JsonNode attrs = n.get("required_attributes");
      if (attrs != null) {
        if (!attrs.isObject()) throw new IOException("required_attributes must be an object");
        attrs
            .fields()
            .forEachRemaining(
                e -> {
                  if (!e.getValue().isTextual())
                    throw new IllegalArgumentException("required attribute values must be strings");
                  attributes.put(e.getKey(), e.getValue().textValue());
                });
      }
      try {
        result.add(
            new RuleConfig(
                id,
                integer(n, "window_ms"),
                integer(n, "minimum_events"),
                integer(n, "minimum_severity_sum"),
                (int) integer(n, "risk_score"),
                types,
                attributes));
      } catch (IllegalArgumentException e) {
        throw new IOException("invalid rule " + id + ": " + e.getMessage(), e);
      }
    }
    return List.copyOf(result);
  }

  private static String text(JsonNode n, String f) throws IOException {
    JsonNode v = n.get(f);
    if (v == null || !v.isTextual() || v.textValue().isBlank())
      throw new IOException(f + " must be non-blank text");
    return v.textValue();
  }

  private static long integer(JsonNode n, String f) throws IOException {
    JsonNode v = n.get(f);
    if (v == null || !v.isIntegralNumber()) throw new IOException(f + " must be an integer");
    return v.longValue();
  }
}
