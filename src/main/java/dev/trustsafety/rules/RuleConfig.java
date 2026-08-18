package dev.trustsafety.rules;

import java.io.Serializable;
import dev.trustsafety.model.SafetyEvent;
import java.util.Map;
import java.util.Set;

public record RuleConfig(String ruleId, long windowMillis, long minimumEvents,
                         long minimumSeveritySum, int riskScore,Set<SafetyEvent.EventType> eventTypes,
                         Map<String,String> requiredAttributes) implements Serializable {
  public RuleConfig {
    if (ruleId == null || ruleId.isBlank()) throw new IllegalArgumentException("ruleId must not be blank");
    if (windowMillis <= 0 || minimumEvents <= 0 || minimumSeveritySum < 0) throw new IllegalArgumentException("invalid rule thresholds");
    if (riskScore < 0 || riskScore > 100) throw new IllegalArgumentException("riskScore must be in [0,100]");
    eventTypes=eventTypes==null?Set.of():Set.copyOf(eventTypes);requiredAttributes=requiredAttributes==null?Map.of():Map.copyOf(requiredAttributes);
  }
  public RuleConfig(String ruleId,long windowMillis,long minimumEvents,long minimumSeveritySum,int riskScore){this(ruleId,windowMillis,minimumEvents,minimumSeveritySum,riskScore,Set.of(),Map.of());}
  public boolean matches(SafetyEvent event){return(eventTypes.isEmpty()||eventTypes.contains(event.eventType()))&&requiredAttributes.entrySet().stream().allMatch(e->e.getValue().equals(event.attributes().get(e.getKey())));}
}
