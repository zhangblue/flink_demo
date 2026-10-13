package com.example.flinkprocesser;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnrichmentJobTest {
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
