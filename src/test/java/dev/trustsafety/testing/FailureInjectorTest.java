package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

class FailureInjectorTest {
  @Test
  void onlyInitialExecutionAttemptInjectsAtConfiguredRecord() {
    assertThat(FailureInjector.shouldInject(1, 2, 0)).isFalse();
    assertThat(FailureInjector.shouldInject(2, 2, 0)).isTrue();
    assertThat(FailureInjector.shouldInject(3, 2, 0)).isFalse();
    assertThat(FailureInjector.shouldInject(2, 2, 1)).isFalse();
    assertThat(FailureInjector.shouldInject(2, 2, 7)).isFalse();
  }

  @Test
  void rejectsUnsafeConfiguration() {
    assertThatThrownBy(() -> new FailureInjector<>("", 1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new FailureInjector<>("x", 0))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
