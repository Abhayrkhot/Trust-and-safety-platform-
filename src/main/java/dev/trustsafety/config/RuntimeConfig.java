package dev.trustsafety.config;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Validated operational settings loaded at the job boundary. */
public record RuntimeConfig(
    Environment environment,
    Path rulesPath,
    Optional<URI> checkpointUri,
    Duration checkpointInterval,
    Duration checkpointTimeout,
    Duration checkpointMinPause,
    int restartAttempts,
    Duration restartDelay,
    Optional<Long> failureAfterEvents) {

  public RuntimeConfig {
    if (environment == null) throw new IllegalArgumentException("environment is required");
    if (rulesPath == null || rulesPath.toString().isBlank())
      throw new IllegalArgumentException("rulesPath is required");
    if (checkpointUri == null) throw new IllegalArgumentException("checkpointUri is required");
    requirePositive(checkpointInterval, "checkpointInterval");
    requirePositive(checkpointTimeout, "checkpointTimeout");
    requireNonNegative(checkpointMinPause, "checkpointMinPause");
    if (checkpointTimeout.compareTo(checkpointInterval) < 0)
      throw new IllegalArgumentException("checkpointTimeout must be at least checkpointInterval");
    if (restartAttempts < 0)
      throw new IllegalArgumentException("restartAttempts must be nonnegative");
    requireNonNegative(restartDelay, "restartDelay");
    if (failureAfterEvents == null)
      throw new IllegalArgumentException("failureAfterEvents is required");
    failureAfterEvents.ifPresent(
        value -> {
          if (value <= 0) throw new IllegalArgumentException("failureAfterEvents must be positive");
        });
    if (environment == Environment.PRODUCTION && checkpointUri.isEmpty())
      throw new IllegalArgumentException(
          "SAFETY_CHECKPOINT_URI is required in production environment");
    if (failureAfterEvents.isPresent() && restartAttempts == 0)
      throw new IllegalArgumentException("failure injection requires at least one restart attempt");
  }

  public static RuntimeConfig fromEnvironment(Map<String, String> environment) {
    return new RuntimeConfig(
        runtimeEnvironment(environment),
        Path.of(environment.getOrDefault("SAFETY_RULES_PATH", "conf/safety-rules.json")),
        optionalUri(environment, "SAFETY_CHECKPOINT_URI"),
        duration(environment, "SAFETY_CHECKPOINT_INTERVAL_MS", 30_000),
        duration(environment, "SAFETY_CHECKPOINT_TIMEOUT_MS", 120_000),
        duration(environment, "SAFETY_CHECKPOINT_MIN_PAUSE_MS", 10_000),
        integer(environment, "SAFETY_RESTART_ATTEMPTS", 3),
        duration(environment, "SAFETY_RESTART_DELAY_MS", 5_000),
        optionalLong(environment, "SAFETY_FAIL_AFTER_EVENTS"));
  }

  private static Environment runtimeEnvironment(Map<String, String> environment) {
    String value = environment.getOrDefault("SAFETY_ENVIRONMENT", "local");
    try {
      return Environment.valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "SAFETY_ENVIRONMENT must be either local or production", e);
    }
  }

  private static Optional<URI> optionalUri(Map<String, String> environment, String name) {
    String value = environment.get(name);
    if (value == null || value.isBlank()) return Optional.empty();
    try {
      URI uri = URI.create(value);
      if (!uri.isAbsolute()) throw new IllegalArgumentException("URI must be absolute");
      return Optional.of(uri);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(name + " must be a valid absolute URI", e);
    }
  }

  private static Optional<Long> optionalLong(Map<String, String> environment, String name) {
    String value = environment.get(name);
    if (value == null || value.isBlank()) return Optional.empty();
    return Optional.of(number(value, name));
  }

  private static Duration duration(
      Map<String, String> environment, String name, long defaultMillis) {
    return Duration.ofMillis(
        environment.containsKey(name) ? number(environment.get(name), name) : defaultMillis);
  }

  private static int integer(Map<String, String> environment, String name, int defaultValue) {
    long value = environment.containsKey(name) ? number(environment.get(name), name) : defaultValue;
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE)
      throw new IllegalArgumentException(name + " is outside the 32-bit integer range");
    return (int) value;
  }

  private static long number(String value, String name) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(name + " must be an integer", e);
    }
  }

  private static void requirePositive(Duration value, String name) {
    if (value == null || value.isZero() || value.isNegative())
      throw new IllegalArgumentException(name + " must be positive");
  }

  private static void requireNonNegative(Duration value, String name) {
    if (value == null || value.isNegative())
      throw new IllegalArgumentException(name + " must be nonnegative");
  }

  public enum Environment {
    LOCAL,
    PRODUCTION
  }
}
