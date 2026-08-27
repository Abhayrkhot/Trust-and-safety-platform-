package dev.trustsafety.serde;

import java.io.IOException;

/** A stable, matchable classification for producer-controlled decoding failures. */
public final class SafetyEventDecodingException extends IOException {
  private static final long serialVersionUID = 1L;

  public enum Reason {
    NULL_PAYLOAD,
    MALFORMED_JSON,
    UNSUPPORTED_SCHEMA_VERSION,
    CONTRACT_VIOLATION
  }

  private final Reason reason;

  public SafetyEventDecodingException(Reason reason, String message) {
    super(message);
    this.reason = reason;
  }

  public SafetyEventDecodingException(Reason reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }

  public Reason reason() {
    return reason;
  }
}
