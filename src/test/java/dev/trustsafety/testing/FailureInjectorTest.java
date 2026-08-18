package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.*;
import org.junit.jupiter.api.Test;

class FailureInjectorTest {
  @Test void firesOnlyOnceAcrossOperatorRecreation() throws Exception {
    String id="unit-fail-once";FailureInjector.reset(id);var first=new FailureInjector<Integer>(id,2);first.open((org.apache.flink.api.common.functions.OpenContext)null);
    assertThat(first.map(1)).isEqualTo(1);assertThatThrownBy(()->first.map(2)).isInstanceOf(FailureInjector.InjectedFailureException.class).hasMessageContaining(id);
    var restarted=new FailureInjector<Integer>(id,2);restarted.open((org.apache.flink.api.common.functions.OpenContext)null);assertThat(restarted.map(1)).isEqualTo(1);assertThat(restarted.map(2)).isEqualTo(2);
  }
  @Test void rejectsUnsafeConfiguration(){assertThatThrownBy(()->new FailureInjector<>("",1)).isInstanceOf(IllegalArgumentException.class);assertThatThrownBy(()->new FailureInjector<>("x",0)).isInstanceOf(IllegalArgumentException.class);}
}
