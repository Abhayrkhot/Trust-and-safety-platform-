package dev.trustsafety.benchmark;

import dev.trustsafety.model.SafetyEvent;
import dev.trustsafety.processing.SafetyProcessor;
import dev.trustsafety.rules.RuleConfig;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

public final class LocalLoadBenchmark {
  private LocalLoadBenchmark() {}

  public static void main(String[] args) throws Exception {
    int count = args.length == 0 ? 50_000 : Integer.parseInt(args[0]);
    int actors = 500;
    List<SafetyEvent> input =
        LongStream.range(0, count)
            .mapToObj(
                i ->
                    new SafetyEvent(
                        1,
                        "bench-" + i,
                        Instant.ofEpochMilli(1_700_000_000_000L + i),
                        Instant.ofEpochMilli(1_700_000_000_000L + i),
                        "actor-" + (i % actors),
                        null,
                        SafetyEvent.EventType.POLICY_MATCH,
                        (int) (i % 100),
                        Map.of()))
            .toList();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(Math.min(4, Runtime.getRuntime().availableProcessors()));
    env.fromData(input)
        .keyBy(SafetyEvent::actorId)
        .process(
            new SafetyProcessor(
                List.of(new RuleConfig("benchmark", 3_600_000, 1, 0, 10)), 7_200_000))
        .sinkTo(new DiscardingSink<>());
    long started = System.nanoTime();
    env.execute("local-load-benchmark");
    long elapsedNanos = System.nanoTime() - started;
    double seconds = elapsedNanos / 1_000_000_000.0;
    System.out.printf(
        java.util.Locale.ROOT,
        "{\"events\":%d,\"actors\":%d,\"parallelism\":%d,\"elapsed_seconds\":%.6f,\"events_per_second\":%.2f,\"available_processors\":%d,\"java_version\":\"%s\",\"os\":\"%s %s\"}%n",
        count,
        actors,
        env.getParallelism(),
        seconds,
        count / seconds,
        Runtime.getRuntime().availableProcessors(),
        System.getProperty("java.version"),
        System.getProperty("os.name"),
        System.getProperty("os.arch"));
  }
}
