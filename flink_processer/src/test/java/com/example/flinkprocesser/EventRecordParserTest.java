package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventRecordParserTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final EventRecordParser parser = new EventRecordParser();

    @Test
    void parsesBusinessKeyAndPreservesOriginalJson() throws Exception {
        String json = """
                {"udid":"device-1","start_id":1791614142000,"protol_type":"start",
                 "sequence":1,"event_timestamp":1791614142381,"event_id":"event-1","payload":{"unknown":true}}
                """;

        EventRecord event = parser.parseAt(json, 1_791_614_142_000L);

        assertThat(event.getUdid()).isEqualTo("device-1");
        assertThat(event.getStartId()).isEqualTo(1_791_614_142_000L);
        assertThat(event.recordKey()).isEqualTo("device-1_1791614142000");
        assertThat(event.getProtolType()).isEqualTo("start");
        assertThat(event.getJson()).isEqualTo(json);
        assertThat(event.getSequence()).isEqualTo(1L);
        assertThat(event.getEventTimestamp()).isEqualTo(1_791_614_142_381L);
        assertThat(event.getEventId()).isEqualTo("event-1");
    }

    @Test
    void missingSequenceUsesStableFallback() throws Exception {
        EventRecord event = parser.map("""
                {"udid":"device-1","start_id":1791614142000,"protol_type":"start","event_timestamp":1791614142000}
                """);

        assertThat(event.getSequence()).isEqualTo(Long.MAX_VALUE);
        assertThat(event.getEventId()).isEmpty();
    }

    @Test
    void clampsTimestampBeyondFutureToleranceAndUpdatesJson() throws Exception {
        EventRecord corrected = parser.parseAt(eventJson(106_001L), 100_000L);

        assertThat(corrected.getEventTimestamp()).isEqualTo(100_000L);
        assertThat(MAPPER.readTree(corrected.getJson()).path("event_timestamp").longValue())
                .isEqualTo(100_000L);
    }

    @Test
    void preservesTimestampAtFutureToleranceBoundary() throws Exception {
        String json = eventJson(105_000L);

        EventRecord boundary = parser.parseAt(json, 100_000L);

        assertThat(boundary.getEventTimestamp()).isEqualTo(105_000L);
        assertThat(boundary.getJson()).isEqualTo(json);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not-json", "{}", "[]", "null",
            "{\"start_id\":1791614142000,\"protol_type\":\"start\"}",
            "{\"udid\":\"device-1\",\"protol_type\":\"start\"}",
            "{\"udid\":\"device-1\",\"start_id\":1791614142000}",
            "{\"udid\":\"\",\"start_id\":1791614142000,\"protol_type\":\"start\"}",
            "{\"udid\":null,\"start_id\":1791614142000,\"protol_type\":\"start\"}",
            "{\"udid\":42,\"start_id\":1791614142000,\"protol_type\":\"start\"}",
            "{\"udid\":\"device-1\",\"start_id\":\"1791614142000\",\"protol_type\":\"start\"}",
            "{\"udid\":\"device-1\",\"start_id\":1.5,\"protol_type\":\"start\"}",
            "{\"udid\":\"device-1\",\"start_id\":9223372036854775808,\"protol_type\":\"start\"}",
            "{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":\"  \"}",
            "{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":true}"
    })
    void rejectsInvalidJsonOrMissingTypedBusinessFields(String json) {
        assertThatThrownBy(() -> parser.map(json)).isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":\"start\"}",
            "{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":\"start\",\"event_timestamp\":\"100000\"}",
            "{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":\"start\",\"event_timestamp\":100000.5}",
            "{\"udid\":\"device-1\",\"start_id\":1791614142000,\"protol_type\":\"start\",\"event_timestamp\":9223372036854775808}"
    })
    void rejectsMissingOrNonLongEventTimestamp(String json) {
        assertThatThrownBy(() -> parser.parseAt(json, 100_000L)).isInstanceOf(Exception.class);
    }

    @Test
    void rejectsTrailingContentAfterAnEvent() {
        String json = """
                {"udid":"device-1","start_id":1791614142000,"protol_type":"start"} {}
                """;
        assertThatThrownBy(() -> parser.map(json)).isInstanceOf(Exception.class);
    }

    private static String eventJson(long eventTimestamp) {
        return "{\"udid\":\"device-1\",\"start_id\":1000,\"protol_type\":\"start\",\"event_timestamp\":"
                + eventTimestamp + "}";
    }
}
