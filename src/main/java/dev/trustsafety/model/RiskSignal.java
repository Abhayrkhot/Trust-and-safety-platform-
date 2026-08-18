package dev.trustsafety.model;

import java.io.Serializable;
import java.time.Instant;

public record RiskSignal(
    String signalId,
    String actorId,
    String triggeringEventId,
    String ruleId,
    int riskScore,
    String reason,
    long observedEventCount,
    long observedSeveritySum,
    Instant emittedAt)
    implements Serializable {}
