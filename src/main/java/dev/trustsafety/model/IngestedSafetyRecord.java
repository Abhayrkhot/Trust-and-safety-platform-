package dev.trustsafety.model;

import java.io.Serializable;

/** Exactly one of {@code event} or {@code quarantine} is present. */
public record IngestedSafetyRecord(SafetyEvent event, QuarantinedEvent quarantine)
    implements Serializable {
  public IngestedSafetyRecord {
    if ((event == null) == (quarantine == null))
      throw new IllegalArgumentException("exactly one ingestion outcome is required");
  }

  public static IngestedSafetyRecord accepted(SafetyEvent event) {
    return new IngestedSafetyRecord(event, null);
  }

  public static IngestedSafetyRecord rejected(QuarantinedEvent quarantine) {
    return new IngestedSafetyRecord(null, quarantine);
  }

  public boolean accepted() {
    return event != null;
  }
}
