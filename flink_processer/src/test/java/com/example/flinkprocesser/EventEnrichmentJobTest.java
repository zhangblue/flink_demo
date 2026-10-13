package com.example.flinkprocesser;

import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnrichmentJobTest {
    // 捕获旧窗口算子或不稳定 UID，并确认正常与异常流各自写入 Kafka。
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
        assertThat(graph.getStreamNodes()).filteredOn(node ->
                "kafka-output-sink".equals(node.getTransformationUID())
                        || "kafka-error-sink".equals(node.getTransformationUID()))
                .extracting(StreamNode::getOperatorName)
                .containsExactlyInAnyOrder("Sink: Writer", "Sink: Writer");
        assertThat(env.getParallelism()).isEqualTo(3);
        assertThat(graph.getStreamNodes()).allSatisfy(node -> assertThat(node.getParallelism()).isEqualTo(3));
        assertThat(env.getCheckpointConfig().getCheckpointInterval()).isEqualTo(1500L);
        assertThat(env.getCheckpointConfig().getCheckpointingConsistencyMode()).isEqualTo(CheckpointingMode.AT_LEAST_ONCE);
    }
}
