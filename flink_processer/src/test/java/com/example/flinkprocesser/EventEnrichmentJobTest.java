package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnrichmentJobTest {
    // Catches a sliding/event-time window or an incorrect two-minute boundary.
    @ParameterizedTest
    @CsvSource({"119999,0,120000", "120000,120000,240000"})
    void assignsTwoMinuteTumblingProcessingTimeWindows(long processingTime, long start, long end) {
        var assigner = EventEnrichmentJob.createWindowAssigner();

        assertThat(assigner).isInstanceOf(TumblingProcessingTimeWindows.class);
        assertThat(assigner.isEventTime()).isFalse();
        assertThat(assigner.assignWindows(new Object(), 999_999L, new WindowAssigner.WindowAssignerContext() {
            @Override
            public long getCurrentProcessingTime() {
                return processingTime;
            }
        })).containsExactly(new TimeWindow(start, end));
    }

    // Catches losing savepoint-stable UIDs or ignoring configured parallelism/checkpoint interval.
    @Test
    void buildsBothKafkaBranchesWithStableUidsAndConfiguredExecutionSettings() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        JobConfig config = new JobConfig("localhost:9092", "input", "output", "errors", "test-group", 3, 1500L);

        EventEnrichmentJob.buildPipeline(env, config);
        StreamGraph graph = env.getStreamGraph();

        assertThat(graph.getStreamNodes()).extracting(StreamNode::getTransformationUID)
                .contains("kafka-source", "parse-json", "app-enrichment-window", "kafka-output-sink", "kafka-error-sink");
        assertThat(env.getParallelism()).isEqualTo(3);
        assertThat(graph.getStreamNodes()).allSatisfy(node -> assertThat(node.getParallelism()).isEqualTo(3));
        assertThat(env.getCheckpointConfig().getCheckpointInterval()).isEqualTo(1500L);
        assertThat(env.getCheckpointConfig().getCheckpointingConsistencyMode()).isEqualTo(CheckpointingMode.AT_LEAST_ONCE);
    }

    // Catches reversing success/error routes or accessing extra keyed/global state in the adapter.
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void windowFunctionRoutesEnrichmentResultsToMainAndErrorOutputs(boolean completeMetadata) throws Exception {
        String key = "device-1:1791614142000";
        EventRecordParser parser = new EventRecordParser();
        String appJson = "{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":\"app\",\"payload\":"
                + (completeMetadata
                ? "{\"self_package_name\":\"self\",\"apps\":[{\"package_name\":\"self\",\"app_name\":\"App\",\"version_name\":\"1.0\"}]}"
                : "{}") + "}";
        EventRecord app = parser.map(appJson);
        EventRecord start = parser.map("{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":\"start\",\"sequence\":1}");
        AppEnrichmentWindowFunction function = new AppEnrichmentWindowFunction();
        List<OutputEvent> main = new ArrayList<>();
        List<OutputEvent> errors = new ArrayList<>();
        var context = function.new Context() {
            @Override
            public TimeWindow window() { return new TimeWindow(0, 120_000); }

            @Override
            public long currentProcessingTime() { return 120_000; }

            @Override
            public long currentWatermark() { return Long.MIN_VALUE; }

            @Override
            public KeyedStateStore windowState() { throw new AssertionError("No extra window state expected"); }

            @Override
            public KeyedStateStore globalState() { throw new AssertionError("No global state expected"); }

            @Override
            public <X> void output(OutputTag<X> tag, X value) {
                assertThat(tag).isEqualTo(AppEnrichmentWindowFunction.ERROR_OUTPUT_TAG);
                errors.add((OutputEvent) value);
            }
        };

        function.process(key, context, List.of(start, app), new Collector<OutputEvent>() {
            @Override
            public void collect(OutputEvent value) { main.add(value); }

            @Override
            public void close() { }
        });

        assertThat(main).contains(new OutputEvent(key, appJson));
        assertThat(main).hasSize(completeMetadata ? 2 : 1);
        assertThat(errors).hasSize(completeMetadata ? 0 : 1);
        OutputEvent enriched = completeMetadata ? main.get(0) : errors.get(0);
        assertThat(enriched.key()).isEqualTo(key);
        var json = new ObjectMapper().readTree(enriched.json());
        assertThat(json.path("app_name").textValue()).isEqualTo(completeMetadata ? "App" : null);
        assertThat(json.path("app_version").textValue()).isEqualTo(completeMetadata ? "1.0" : null);
    }
}
