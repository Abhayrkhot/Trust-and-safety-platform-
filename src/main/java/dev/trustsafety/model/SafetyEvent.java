package dev.trustsafety.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Objects;

public record SafetyEvent(
    int schemaVersion,
    String eventId,
    Instant occurredAt,
    Instant ingestedAt,
    String actorId,
    String contentId,
    EventType eventType,
    int severity,
    Map<String, String> attributes) implements Serializable {

  public enum EventType { CONTENT_REPORT, POLICY_MATCH, USER_BLOCK, MODERATION_DECISION }

  public SafetyEvent {
    if (schemaVersion != 1) throw new IllegalArgumentException("unsupported schema_version: " + schemaVersion);
    requireText(eventId, "event_id"); requireText(actorId, "actor_id");
    Objects.requireNonNull(occurredAt, "occurred_at"); Objects.requireNonNull(ingestedAt, "ingested_at");
    Objects.requireNonNull(eventType, "event_type");
    if (severity < 0 || severity > 100) throw new IllegalArgumentException("severity must be in [0,100]");
    attributes = attributes == null ? new HashMap<>() : new HashMap<>(attributes);
  }

  @Override public Map<String,String> attributes() { return Collections.unmodifiableMap(attributes); }

  private static void requireText(String value, String name) {
    if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
  }
}
