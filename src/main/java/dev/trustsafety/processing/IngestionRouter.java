package dev.trustsafety.processing;

import dev.trustsafety.model.IngestedSafetyRecord;
import dev.trustsafety.model.QuarantinedEvent;
import dev.trustsafety.model.SafetyEvent;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Separates valid events from poison records without dropping either outcome. */
public final class IngestionRouter extends ProcessFunction<IngestedSafetyRecord, SafetyEvent> {
  private static final long serialVersionUID = 1L;

  public static final OutputTag<QuarantinedEvent> QUARANTINE =
      new OutputTag<>("quarantined-safety-events", TypeInformation.of(QuarantinedEvent.class));

  // Flink calls open before processing; initial values also keep construction safe for analyzers.
  private transient Counter accepted = new SimpleCounter();
  private transient Counter quarantined = new SimpleCounter();

  @Override
  public void open(org.apache.flink.configuration.Configuration ignored) {
    var group = getRuntimeContext().getMetricGroup().addGroup("trust_safety");
    accepted = group.counter("accepted_records_total");
    quarantined = group.counter("quarantined_records_total");
  }

  @Override
  public void processElement(IngestedSafetyRecord record, Context ctx, Collector<SafetyEvent> out) {
    if (record.accepted()) {
      accepted.inc();
      out.collect(record.event());
    } else {
      quarantined.inc();
      ctx.output(QUARANTINE, record.quarantine());
    }
  }

  @Serial
  private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
    input.defaultReadObject();
    accepted = new SimpleCounter();
    quarantined = new SimpleCounter();
  }
}
