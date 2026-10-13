package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommonStateEnrichmentFunctionTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COMPLETE_PAYLOAD = """
            {"self_app_package":"com.example.demo","self_app_name":"Demo",
             "model":"Pixel 10","brand":"Google","os":"Android","os_version":"17"}
            """;
    private static final String INCOMPLETE_PAYLOAD = """
            {"self_app_package":"com.example.partial","self_app_name":"Partial",
             "model":42,"brand":null,"os":"   "}
            """;

    private KeyedOneInputStreamOperatorTestHarness<String, EventRecord, OutputEvent> harness;

    @BeforeEach
    void openHarness() throws Exception {
        harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                new CommonStateEnrichmentFunction(), EventRecord::recordKey, Types.STRING);
    }

    @AfterEach
    void closeHarness() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    // 抓住完整 common 泄漏到任一输出分支，或未写入真实 managed state。
    @Test
    void completeCommonWritesManagedStateWithoutOutput() throws Exception {
        harness.processElement(common("device-1", 1L), 0L);

        assertThat(mainOutput()).isEmpty();
        assertThat(errorOutput()).isEmpty();
        assertThat(harness.numKeyedStateEntries()).isEqualTo(2);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
    }

    // 抓住遗漏 app/任一非 common 类型、延迟输出、错误分支双写或六字段映射错误。
    @ParameterizedTest
    @ValueSource(strings = {"start", "devinfo", "environment", "app"})
    void immediatelyEnrichesEveryNonCommonTypeFromManagedState(String type) throws Exception {
        harness.processElement(common("device-1", 1L), 0L);
        harness.processElement(nonCommon("device-1", 1L, type), 0L);

        assertThat(mainOutput()).hasSize(1);
        assertThat(errorOutput()).isEmpty();
        assertCompleteMetadata(mainOutput().get(0), "device-1_1", type);
    }

    // 抓住无状态非 common 进入主流、app 特判，或用输入旧字段代替六个显式 null。
    @ParameterizedTest
    @ValueSource(strings = {"start", "devinfo", "environment", "app"})
    void nonCommonWithoutStateGoesOnlyToErrorsWithExplicitNulls(String type) throws Exception {
        harness.processElement(nonCommon("device-1", 1L, type), 0L);

        assertThat(mainOutput()).isEmpty();
        assertThat(errorOutput()).hasSize(1);
        assertNullMetadata(errorOutput().get(0), "device-1_1", type);
    }

    // 抓住不完整 common 被丢弃、无效字段未归一化，或错误地建立可用状态。
    @Test
    void incompleteCommonEmitsPartialMetadataAndDoesNotEstablishState() throws Exception {
        harness.processElement(incompleteCommon("device-1", 1L), 0L);
        assertThat(mainOutput()).isEmpty();
        assertThat(errorOutput()).hasSize(1);
        JsonNode output = assertEnvelope(errorOutput().get(0), "device-1_1", "common");
        assertField(output, "self_app_package", "com.example.partial");
        assertField(output, "self_app_name", "Partial");
        assertField(output, "model", null);
        assertField(output, "brand", null);
        assertField(output, "os", null);
        assertField(output, "os_version", null);

        harness.processElement(nonCommon("device-1", 1L, "start"), 0L);
        assertThat(mainOutput()).isEmpty();
        assertThat(errorOutput()).hasSize(2);
        assertNullMetadata(errorOutput().get(1), "device-1_1", "start");
    }

    // 抓住不完整 common 清空或覆盖已有完整 common 状态。
    @Test
    void incompleteCommonPreservesPreviousCompleteState() throws Exception {
        harness.processElement(common("device-1", 1L), 0L);
        harness.processElement(incompleteCommon("device-1", 1L), 0L);
        assertThat(mainOutput()).isEmpty();
        assertThat(errorOutput()).hasSize(1);
        assertEnvelope(errorOutput().get(0), "device-1_1", "common");

        harness.processElement(nonCommon("device-1", 1L, "app"), 0L);
        assertThat(mainOutput()).hasSize(1);
        assertThat(errorOutput()).hasSize(1);
        assertCompleteMetadata(mainOutput().get(0), "device-1_1", "app");
    }

    // 抓住首次 common 后不再更新，或第二条 common 仅更新部分字段。
    @Test
    void laterCompleteCommonReplacesAllSixFields() throws Exception {
        harness.processElement(common("device-1", 1L), 0L);
        harness.processElement(event("device-1", 1L, "common", """
                {"self_app_package":"com.example.next","self_app_name":"Next",
                 "model":"Phone 2","brand":"Brand 2","os":"OS 2","os_version":"2"}
                """), 0L);
        assertThat(mainOutput()).isEmpty();
        assertThat(errorOutput()).isEmpty();

        harness.processElement(nonCommon("device-1", 1L, "devinfo"), 0L);
        assertThat(mainOutput()).hasSize(1);
        assertThat(errorOutput()).isEmpty();
        JsonNode output = assertEnvelope(mainOutput().get(0), "device-1_1", "devinfo");
        assertField(output, "self_app_package", "com.example.next");
        assertField(output, "self_app_name", "Next");
        assertField(output, "model", "Phone 2");
        assertField(output, "brand", "Brand 2");
        assertField(output, "os", "OS 2");
        assertField(output, "os_version", "2");
    }

    // 抓住 common 状态跨设备或跨 start_id 泄漏，以及组合输出 key 错误。
    @Test
    void isolatesDevicesAndStartupCycles() throws Exception {
        harness.processElement(common("device-1", 1L), 0L);
        harness.processElement(nonCommon("device-2", 1L, "start"), 0L);
        harness.processElement(nonCommon("device-1", 2L, "app"), 0L);
        harness.processElement(nonCommon("device-1", 1L, "environment"), 0L);

        assertThat(mainOutput()).hasSize(1);
        assertThat(errorOutput()).hasSize(2);
        assertNullMetadata(errorOutput().get(0), "device-2_1", "start");
        assertNullMetadata(errorOutput().get(1), "device-1_2", "app");
        assertCompleteMetadata(mainOutput().get(0), "device-1_1", "environment");
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(3);
    }

    // 抓住任意分支漏续期、旧 timer 未删、到旧时刻提前清理或当前 timer 未清两个状态。
    @ParameterizedTest
    @ValueSource(strings = {"start", "devinfo", "environment", "app", "common", "incomplete-common"})
    void everyMessageRenewsSingleProcessingTimerAndExpiresAtCurrentDeadline(String renewingType)
            throws Exception {
        harness.setProcessingTime(1_000L);
        harness.processElement(common("device-1", 1L), -100L);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

        harness.setProcessingTime(20_000L);
        EventRecord renewing = switch (renewingType) {
            case "common" -> common("device-1", 1L);
            case "incomplete-common" -> incompleteCommon("device-1", 1L);
            default -> nonCommon("device-1", 1L, renewingType);
        };
        harness.processElement(renewing, -200L);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        int mainCount = renewingType.endsWith("common") ? 0 : 1;
        int errorCount = renewingType.equals("incomplete-common") ? 1 : 0;
        assertThat(mainOutput()).hasSize(mainCount);
        assertThat(errorOutput()).hasSize(errorCount);
        if (mainCount == 1) {
            assertCompleteMetadata(mainOutput().get(0), "device-1_1", renewingType);
        }
        if (errorCount == 1) {
            assertEnvelope(errorOutput().get(0), "device-1_1", "common");
        }

        harness.setProcessingTime(31_000L);
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
        assertThat(harness.numKeyedStateEntries()).isEqualTo(2);
        harness.setProcessingTime(49_999L);
        assertThat(harness.numKeyedStateEntries()).isEqualTo(2);

        harness.setProcessingTime(50_000L);
        assertThat(harness.numProcessingTimeTimers()).isZero();
        assertThat(harness.numKeyedStateEntries()).isZero();
        assertThat(mainOutput()).hasSize(mainCount);
        assertThat(errorOutput()).hasSize(errorCount);

        harness.processElement(nonCommon("device-1", 1L, "environment"), 50_000L);
        assertThat(mainOutput()).hasSize(mainCount);
        assertThat(errorOutput()).hasSize(errorCount + 1);
        assertNullMetadata(errorOutput().get(errorCount), "device-1_1", "environment");
        assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
    }

    // 抓住用普通加法把清理时间溢出为过去值并继续产生业务输出。
    @Test
    void failsOnCleanupTimestampOverflowWithoutRegisteringTimer() throws Exception {
        harness.setProcessingTime(Long.MAX_VALUE - 10_000L);

        assertThatThrownBy(() -> harness.processElement(nonCommon("device-1", 1L, "start"), 0L))
                .isInstanceOf(ArithmeticException.class);
        assertThat(harness.numProcessingTimeTimers()).isZero();
        assertThat(mainOutput()).isEmpty();
        assertThat(errorOutput()).isEmpty();
    }

    // 抓住陈旧 callback 未比较当前时间戳，误清最新状态；真实 operator 调用真实函数。
    @Test
    void staleTimerCallbackCannotClearRenewedMetadata() throws Exception {
        harness.setProcessingTime(1_000L);
        harness.processElement(common("device-1", 1L), 0L);
        harness.setProcessingTime(20_000L);
        harness.processElement(nonCommon("device-1", 1L, "start"), 0L);
        @SuppressWarnings("unchecked")
        KeyedProcessOperator<String, EventRecord, OutputEvent> operator =
                (KeyedProcessOperator<String, EventRecord, OutputEvent>) harness.getOperator();
        operator.onProcessingTime(new TimerHeapInternalTimer<>(31_000L, "device-1_1", VoidNamespace.INSTANCE));
        assertThat(harness.numKeyedStateEntries()).isEqualTo(2);
        assertThat(mainOutput()).hasSize(1);
        assertThat(errorOutput()).isEmpty();

        harness.processElement(nonCommon("device-1", 1L, "app"), 0L);
        assertThat(mainOutput()).hasSize(2);
        assertThat(errorOutput()).isEmpty();
        assertCompleteMetadata(mainOutput().get(0), "device-1_1", "start");
        assertCompleteMetadata(mainOutput().get(1), "device-1_1", "app");
    }

    private List<OutputEvent> mainOutput() {
        return harness.extractOutputValues();
    }

    private List<OutputEvent> errorOutput() {
        Queue<StreamRecord<OutputEvent>> output =
                harness.getSideOutput(CommonStateEnrichmentFunction.ERROR_OUTPUT_TAG);
        return output == null ? List.of() : output.stream().map(StreamRecord::getValue).toList();
    }

    private static EventRecord common(String device, long startId) {
        return event(device, startId, "common", COMPLETE_PAYLOAD);
    }

    private static EventRecord incompleteCommon(String device, long startId) {
        return event(device, startId, "common", INCOMPLETE_PAYLOAD);
    }

    private static EventRecord nonCommon(String device, long startId, String type) {
        return event(device, startId, type, "{\"marker\":\"preserved\"}");
    }

    private static EventRecord event(String device, long startId, String type, String payload) {
        EventRecord event = new EventRecord();
        event.setUdid(device);
        event.setStartId(startId);
        event.setProtolType(type);
        event.setJson("{\"udid\":\"" + device + "\",\"start_id\":" + startId
                + ",\"protol_type\":\"" + type + "\",\"model\":\"old\",\"payload\":" + payload + "}");
        return event;
    }

    private static JsonNode assertEnvelope(OutputEvent event, String key, String type) throws Exception {
        assertThat(event.key()).isEqualTo(key);
        JsonNode json = MAPPER.readTree(event.json());
        assertThat(json.path("protol_type").textValue()).isEqualTo(type);
        return json;
    }

    private static void assertCompleteMetadata(OutputEvent event, String key, String type) throws Exception {
        JsonNode json = assertEnvelope(event, key, type);
        assertField(json, "self_app_package", "com.example.demo");
        assertField(json, "self_app_name", "Demo");
        assertField(json, "model", "Pixel 10");
        assertField(json, "brand", "Google");
        assertField(json, "os", "Android");
        assertField(json, "os_version", "17");
    }

    private static void assertNullMetadata(OutputEvent event, String key, String type) throws Exception {
        JsonNode json = assertEnvelope(event, key, type);
        assertField(json, "self_app_package", null);
        assertField(json, "self_app_name", null);
        assertField(json, "model", null);
        assertField(json, "brand", null);
        assertField(json, "os", null);
        assertField(json, "os_version", null);
    }

    private static void assertField(JsonNode json, String field, String expected) {
        assertThat(json.has(field)).isTrue();
        if (expected == null) {
            assertThat(json.get(field).isNull()).isTrue();
        } else {
            assertThat(json.get(field).textValue()).isEqualTo(expected);
        }
    }
}
