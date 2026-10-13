package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

class CommonStateCheckpointRestoreTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // 抓住 checkpoint 丢失 timer 或截止状态：不输入新事件，恢复后的 timer 仍必须清理状态。
    @Test
    void restoredTimerExpiresCommonStateWithoutAnotherInput() throws Exception {
        OperatorSubtaskState snapshot = snapshotCompleteCommon();

        try (var restored = newHarness()) {
            restored.setup();
            restored.initializeState(snapshot);
            restored.open();

            assertThat(restored.numKeyedStateEntries()).isEqualTo(2);
            assertThat(restored.numEventTimeTimers()).isEqualTo(1);
            assertThat(restored.numProcessingTimeTimers()).isZero();
            restored.processWatermark(new Watermark(30_999L));
            assertThat(restored.numKeyedStateEntries()).isEqualTo(2);
            assertThat(restored.numEventTimeTimers()).isEqualTo(1);

            restored.processWatermark(new Watermark(31_000L));
            assertThat(restored.numKeyedStateEntries()).isZero();
            assertThat(restored.numEventTimeTimers()).isZero();
            assertThat(restored.numProcessingTimeTimers()).isZero();
            assertThat(restored.extractOutputValues()).isEmpty();
            assertThat(errorOutput(restored)).isEmpty();
        }
    }

    // 抓住 common 恢复丢字段、旧 timer 未删、恢复后不能续期或清理后仍复用 common。
    @Test
    void restoredCommonEnrichesNewEventAndRenewsItsCleanupTimer() throws Exception {
        OperatorSubtaskState snapshot = snapshotCompleteCommon();

        try (var restored = newHarness()) {
            restored.setup();
            restored.initializeState(snapshot);
            restored.open();

            assertThat(restored.numEventTimeTimers()).isEqualTo(1);
            restored.processElement(nonCommon("app", 20_000L), 20_000L);

            List<OutputEvent> output = restored.extractOutputValues();
            assertThat(output).hasSize(1);
            assertThat(output.get(0).key()).isEqualTo("device-1_1");
            assertThat(MAPPER.readTree(output.get(0).json())).isEqualTo(MAPPER.readTree("""
                    {"udid":"device-1","start_id":1,"protol_type":"app",
                     "payload":{"marker":"preserved"},
                     "self_app_package":"com.example.demo","self_app_name":"Demo",
                     "model":"Pixel 10","brand":"Google","os":"Android","os_version":"17"}
                    """));
            assertThat(errorOutput(restored)).isEmpty();
            assertThat(restored.numEventTimeTimers()).isEqualTo(1);
            assertThat(restored.numProcessingTimeTimers()).isZero();

            restored.processWatermark(new Watermark(31_000L));
            assertThat(restored.numKeyedStateEntries()).isEqualTo(2);
            assertThat(restored.numEventTimeTimers()).isEqualTo(1);
            restored.processWatermark(new Watermark(49_999L));
            assertThat(restored.numKeyedStateEntries()).isEqualTo(2);

            restored.processWatermark(new Watermark(50_000L));
            assertThat(restored.numKeyedStateEntries()).isZero();
            assertThat(restored.numEventTimeTimers()).isZero();
            assertThat(restored.numProcessingTimeTimers()).isZero();
            assertThat(restored.extractOutputValues()).containsExactlyElementsOf(output);
            assertThat(errorOutput(restored)).isEmpty();

            restored.processElement(nonCommon("environment", 50_000L), 50_000L);
            assertThat(restored.extractOutputValues()).containsExactlyElementsOf(output);
            List<OutputEvent> errors = errorOutput(restored);
            assertThat(errors).hasSize(1);
            assertThat(errors.get(0).key()).isEqualTo("device-1_1");
            assertThat(MAPPER.readTree(errors.get(0).json())).isEqualTo(MAPPER.readTree("""
                    {"udid":"device-1","start_id":1,"protol_type":"environment",
                     "payload":{"marker":"preserved"},
                     "self_app_package":null,"self_app_name":null,"model":null,
                     "brand":null,"os":null,"os_version":null}
                    """));
        }
    }

    private static OperatorSubtaskState snapshotCompleteCommon() throws Exception {
        // 此 harness 在返回 snapshot 前关闭；恢复测试只能依赖 OperatorSubtaskState。
        try (var original = newHarness()) {
            original.setup();
            original.open();
            original.processElement(event("common", 1_000L, """
                    {"udid":"device-1","start_id":1,"protol_type":"common",
                     "payload":{"self_app_package":"com.example.demo","self_app_name":"Demo",
                                "model":"Pixel 10","brand":"Google","os":"Android","os_version":"17"}}
                    """), 1_000L);
            assertThat(original.extractOutputValues()).isEmpty();
            assertThat(errorOutput(original)).isEmpty();
            assertThat(original.numKeyedStateEntries()).isEqualTo(2);
            assertThat(original.numEventTimeTimers()).isEqualTo(1);
            return original.snapshot(1L, 1L);
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, EventRecord, OutputEvent> newHarness()
            throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new CommonStateEnrichmentFunction()),
                EventRecord::recordKey, Types.STRING);
    }

    private static EventRecord nonCommon(String type, long timestamp) {
        return event(type, timestamp, "{\"udid\":\"device-1\",\"start_id\":1,\"protol_type\":\""
                + type + "\",\"payload\":{\"marker\":\"preserved\"}}");
    }

    private static EventRecord event(String type, long timestamp, String json) {
        EventRecord event = new EventRecord();
        event.setUdid("device-1");
        event.setStartId(1L);
        event.setProtolType(type);
        event.setEventTimestamp(timestamp);
        event.setJson(json);
        return event;
    }

    private static List<OutputEvent> errorOutput(
            KeyedOneInputStreamOperatorTestHarness<String, EventRecord, OutputEvent> harness) {
        Queue<StreamRecord<OutputEvent>> output =
                harness.getSideOutput(CommonStateEnrichmentFunction.ERROR_OUTPUT_TAG);
        return output == null ? List.of() : output.stream().map(StreamRecord::getValue).toList();
    }
}
