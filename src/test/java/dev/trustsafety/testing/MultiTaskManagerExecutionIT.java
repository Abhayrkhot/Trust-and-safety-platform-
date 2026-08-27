package dev.trustsafety.testing;

import static org.assertj.core.api.Assertions.assertThat;

import dev.trustsafety.SafetyStreamJob;
import dev.trustsafety.model.RiskSignal;
import dev.trustsafety.processing.SafetyProcessor;
import dev.trustsafety.rules.RuleConfig;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

class MultiTaskManagerExecutionIT {
  private static final String TRIGGERING_EVENT_IDS = "triggering-event-ids";

  @RegisterExtension
  static final MiniClusterExtension CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(2)
              .setNumberSlotsPerTaskManager(1)
              .build());

  @Test
  @Timeout(60)
  void productionRuleGraphUsesTwoTaskManagersWithoutLosingOrDuplicatingResults(
      @InjectMiniCluster MiniCluster miniCluster) throws Exception {
    var overview = waitForTaskManagers(miniCluster, 2);
    assertThat(overview.getNumTaskManagersConnected()).isEqualTo(2);
    assertThat(overview.getNumSlotsTotal()).isEqualTo(2);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var input =
        LongStream.rangeClosed(1, 200).mapToObj(i -> PipelineTestSupport.event(i, 20)).toList();
    env.fromData(input)
        .assignTimestampsAndWatermarks(SafetyStreamJob.watermarkStrategy())
        .name("multi-worker-watermarks")
        .keyBy(event -> event.actorId())
        .process(
            new SafetyProcessor(
                List.of(new RuleConfig("multi-worker-rule", 60_000, 1, 0, 10)),
                Duration.ofMinutes(2).toMillis()))
        .name("multi-worker-rules")
        .map(new TriggeringEventAccumulator())
        .name("multi-worker-result-oracle")
        .sinkTo(new DiscardingSink<>())
        .name("multi-worker-discard");

    var result = env.execute("multi-task-manager-execution-it");
    List<String> triggeringEventIds = result.getAccumulatorResult(TRIGGERING_EVENT_IDS);
    assertThat(triggeringEventIds)
        .containsExactlyInAnyOrderElementsOf(
            LongStream.rangeClosed(1, 200).mapToObj(i -> "event-" + i).toList());

    var archived = miniCluster.getArchivedExecutionGraph(result.getJobID()).get();
    Set<String> assignedTaskManagers =
        java.util.stream.StreamSupport.stream(
                archived.getAllExecutionVertices().spliterator(), false)
            .map(vertex -> vertex.getCurrentExecutionAttempt().getAssignedResourceLocation())
            .filter(java.util.Objects::nonNull)
            .map(location -> location.getResourceID().toString())
            .collect(Collectors.toSet());
    assertThat(assignedTaskManagers).hasSize(2);
  }

  private static org.apache.flink.runtime.messages.webmonitor.ClusterOverview waitForTaskManagers(
      MiniCluster miniCluster, int expected) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    var overview = miniCluster.requestClusterOverview().get();
    while (overview.getNumTaskManagersConnected() != expected && System.nanoTime() < deadline) {
      Thread.sleep(25);
      overview = miniCluster.requestClusterOverview().get();
    }
    return overview;
  }

  static final class TriggeringEventAccumulator extends RichMapFunction<RiskSignal, RiskSignal> {
    private static final long serialVersionUID = 1L;
    private final ListAccumulator<String> triggeringEventIds = new ListAccumulator<>();

    @Override
    public void open(OpenContext ignored) {
      getRuntimeContext().addAccumulator(TRIGGERING_EVENT_IDS, triggeringEventIds);
    }

    @Override
    public RiskSignal map(RiskSignal signal) {
      triggeringEventIds.add(signal.triggeringEventId());
      return signal;
    }
  }
}
