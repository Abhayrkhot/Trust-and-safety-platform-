package dev.trustsafety.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RuntimeConfigTest {
  @Test
  void loadsSafeDefaultsWithoutPretendingCheckpointStorageIsDurable() {
    RuntimeConfig config = RuntimeConfig.fromEnvironment(Map.of());

    assertThat(config.environment()).isEqualTo(RuntimeConfig.Environment.LOCAL);
    assertThat(config.rulesPath()).hasToString("conf/safety-rules.json");
    assertThat(config.checkpointUri()).isEmpty();
    assertThat(config.checkpointInterval()).isEqualTo(Duration.ofSeconds(30));
    assertThat(config.checkpointTimeout()).isEqualTo(Duration.ofMinutes(2));
    assertThat(config.checkpointMinPause()).isEqualTo(Duration.ofSeconds(10));
    assertThat(config.restartAttempts()).isEqualTo(3);
    assertThat(config.restartDelay()).isEqualTo(Duration.ofSeconds(5));
    assertThat(config.failureAfterEvents()).isEmpty();
  }

  @Test
  void loadsDurableCheckpointAndFailureDrillConfiguration() {
    RuntimeConfig config =
        RuntimeConfig.fromEnvironment(
            Map.of(
                "SAFETY_ENVIRONMENT", "production",
                "SAFETY_RULES_PATH", "/etc/safety/rules.json",
                "SAFETY_CHECKPOINT_URI", "s3://safety-state/checkpoints",
                "SAFETY_CHECKPOINT_INTERVAL_MS", "45000",
                "SAFETY_CHECKPOINT_TIMEOUT_MS", "180000",
                "SAFETY_CHECKPOINT_MIN_PAUSE_MS", "15000",
                "SAFETY_RESTART_ATTEMPTS", "5",
                "SAFETY_RESTART_DELAY_MS", "7000",
                "SAFETY_FAIL_AFTER_EVENTS", "123"));

    assertThat(config.checkpointUri()).hasValueSatisfying(uri -> assertThat(uri).hasScheme("s3"));
    assertThat(config.environment()).isEqualTo(RuntimeConfig.Environment.PRODUCTION);
    assertThat(config.checkpointInterval()).isEqualTo(Duration.ofSeconds(45));
    assertThat(config.restartAttempts()).isEqualTo(5);
    assertThat(config.failureAfterEvents()).contains(123L);
  }

  @Test
  void rejectsRelativeUriInvalidNumbersAndUnsafeTiming() {
    assertThatThrownBy(
            () ->
                RuntimeConfig.fromEnvironment(
                    Map.of("SAFETY_CHECKPOINT_URI", "relative/checkpoints")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SAFETY_CHECKPOINT_URI");
    assertThatThrownBy(
            () -> RuntimeConfig.fromEnvironment(Map.of("SAFETY_RESTART_ATTEMPTS", "not-a-number")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SAFETY_RESTART_ATTEMPTS");
    assertThatThrownBy(
            () ->
                RuntimeConfig.fromEnvironment(
                    Map.of(
                        "SAFETY_CHECKPOINT_INTERVAL_MS", "60000",
                        "SAFETY_CHECKPOINT_TIMEOUT_MS", "10000")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("checkpointTimeout");
    assertThatThrownBy(() -> RuntimeConfig.fromEnvironment(Map.of("SAFETY_FAIL_AFTER_EVENTS", "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("failureAfterEvents");
    assertThatThrownBy(
            () -> RuntimeConfig.fromEnvironment(Map.of("SAFETY_ENVIRONMENT", "production")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SAFETY_CHECKPOINT_URI");
    assertThatThrownBy(
            () ->
                RuntimeConfig.fromEnvironment(
                    Map.of("SAFETY_FAIL_AFTER_EVENTS", "10", "SAFETY_RESTART_ATTEMPTS", "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("restart attempt");
    assertThatThrownBy(() -> RuntimeConfig.fromEnvironment(Map.of("SAFETY_ENVIRONMENT", "staging")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SAFETY_ENVIRONMENT");
    assertThatThrownBy(
            () ->
                RuntimeConfig.fromEnvironment(
                    Map.of("SAFETY_RESTART_ATTEMPTS", Long.toString(Long.MIN_VALUE))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("32-bit integer range");
    assertThatThrownBy(() -> RuntimeConfig.fromEnvironment(Map.of("SAFETY_RULES_PATH", "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rulesPath");
  }
}
