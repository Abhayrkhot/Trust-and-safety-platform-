package dev.trustsafety.rules;

import java.io.Serializable;

public record RuleConfig(String ruleId, long windowMillis, long minimumEvents,
                         long minimumSeveritySum, int riskScore) implements Serializable {
  public RuleConfig {
    if (ruleId == null || ruleId.isBlank()) throw new IllegalArgumentException("ruleId must not be blank");
    if (windowMillis <= 0 || minimumEvents <= 0 || minimumSeveritySum < 0) throw new IllegalArgumentException("invalid rule thresholds");
    if (riskScore < 0 || riskScore > 100) throw new IllegalArgumentException("riskScore must be in [0,100]");
  }
}
