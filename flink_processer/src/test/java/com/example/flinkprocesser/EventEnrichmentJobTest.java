package com.example.flinkprocesser;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.clock.RelativeClock;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnrichmentJobTest {
    // 捕获策略放错算子、使用 Kafka 时间或没有产生有界乱序水位线的回归。
    @Test
    @SuppressWarnings("unchecked")
    void sourceAssignsEventTimestampAndGeneratesWatermarksWithIdleTimeout() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EventEnrichmentJob.buildPipeline(env,
                new JobConfig("localhost:9092", "input", "output", "errors", "test-group", 1, 1500L));
        var source = env.getTransformations().stream()
                .flatMap(transformation -> transformation.getTransitivePredecessors().stream())
                .filter(transformation -> "kafka-source".equals(transformation.getUid()))
                .findFirst().orElseThrow();
        assertThat(source).isInstanceOf(SourceTransformation.class);
        WatermarkStrategy<EventRecord> strategy = (WatermarkStrategy<EventRecord>)
                ((SourceTransformation<?, ?, ?>) source).getWatermarkStrategy();
        EventRecord event = new EventRecord();
        event.setEventTimestamp(12000L);
        assertThat(strategy.createTimestampAssigner(() -> new UnregisteredMetricsGroup())
                .extractTimestamp(event, 999L)).isEqualTo(12000L);
        ManualClock clock = new ManualClock(1L);
        var generator = strategy.createWatermarkGenerator(new WatermarkGeneratorSupplier.Context() {
            public MetricGroup getMetricGroup() { return new UnregisteredMetricsGroup(); }
            public RelativeClock getInputActivityClock() { return clock; }
        });
        RecordingWatermarkOutput output = new RecordingWatermarkOutput();
        generator.onEvent(event, 12000L, output);
        generator.onPeriodicEmit(output);
        assertThat(output.timestamps).containsExactly(6999L);
        // 下一次无活动探测开始 idle 计时；时钟从非零值开始避开框架计时哨兵。
        generator.onPeriodicEmit(output);
        clock.advanceTime(Duration.ofMillis(60000));
        generator.onPeriodicEmit(output);
        assertThat(output.idle).isFalse();
        clock.advanceTime(Duration.ofMillis(1));
        generator.onPeriodicEmit(output);
        assertThat(output.idle).isTrue();
    }

    private static final class RecordingWatermarkOutput implements WatermarkOutput {
        private final List<Long> timestamps = new ArrayList<>();
        private boolean idle;
        public void emitWatermark(Watermark watermark) { timestamps.add(watermark.getTimestamp()); idle = false; }
        public void markIdle() { idle = true; }
        public void markActive() { idle = false; }
    }

    // 捕获旧窗口算子或不稳定 UID，并确认正常与异常流各有一个 Sink 分支。
    @Test
    void buildsKeyedProcessWithBothKafkaSinkBranches() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig config = new JobConfig("localhost:9092", "input", "output", "errors", "test-group", 3, 1500L);

        EventEnrichmentJob.buildPipeline(env, config);
        StreamGraph graph = env.getStreamGraph();

        assertThat(graph.getStreamNodes()).extracting(StreamNode::getTransformationUID)
                .contains("kafka-source", "parse-json", "common-state-enrichment",
                        "kafka-output-sink", "kafka-error-sink");
        assertThat(graph.getStreamNodes()).allSatisfy(node -> assertThat(node.getOperatorName())
                .doesNotContainIgnoringCase("window"));
        assertThat(env.getParallelism()).isEqualTo(3);
        assertThat(graph.getStreamNodes()).allSatisfy(node -> assertThat(node.getParallelism()).isEqualTo(3));
        assertThat(env.getCheckpointConfig().getCheckpointInterval()).isEqualTo(1500L);
        assertThat(env.getCheckpointConfig().getCheckpointingConsistencyMode()).isEqualTo(CheckpointingMode.AT_LEAST_ONCE);
    }

    // 捕获保留 sink UID 却意外接入其他类型 Sink 的回归。
    @Test
    void connectsBothKafkaSinkInstances() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig config = new JobConfig("localhost:9092", "input", "output", "errors", "test-group", 3, 1500L);

        EventEnrichmentJob.buildPipeline(env, config);

        var sinks = env.getTransformations().stream()
                .filter(transformation -> "kafka-output-sink".equals(transformation.getUid())
                        || "kafka-error-sink".equals(transformation.getUid()))
                .toList();
        assertThat(sinks).extracting(Transformation::getUid)
                .containsExactlyInAnyOrder("kafka-output-sink", "kafka-error-sink");
        assertThat(sinks).allSatisfy(transformation -> {
            assertThat(transformation).isInstanceOf(SinkTransformation.class);
            assertThat(((SinkTransformation<?, ?>) transformation).getSink()).isInstanceOf(KafkaSink.class);
        });
    }
}
