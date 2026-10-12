package com.example.flinkprocesser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventRecordParserTest {
    private final EventRecordParser parser = new EventRecordParser();

    @Test
    void parsesBusinessKeyAndPreservesOriginalJson() throws Exception {
        String json = """
                {"udid":"device-1","start_id":1791614142000,"protol_type":"start",
                 "sequence":1,"event_timestamp":1791614142381,"event_id":"event-1","payload":{"unknown":true}}
                """;

        EventRecord event = parser.map(json);

        assertThat(event.getUdid()).isEqualTo("device-1");
        assertThat(event.getStartId()).isEqualTo(1_791_614_142_000L);
        assertThat(event.recordKey()).isEqualTo("device-1:1791614142000");
        assertThat(event.getProtolType()).isEqualTo("start");
        assertThat(event.getJson()).isEqualTo(json);
        assertThat(event.getSequence()).isEqualTo(1L);
        assertThat(event.getEventTimestamp()).isEqualTo(1_791_614_142_381L);
        assertThat(event.getEventId()).isEqualTo("event-1");
    }

    @Test
    void missingSortFieldsUseStableFallbacks() throws Exception {
        EventRecord event = parser.map("""
                {"udid":"device-1","start_id":1791614142000,"protol_type":"start"}
                """);

        assertThat(event.getSequence()).isEqualTo(Long.MAX_VALUE);
        assertThat(event.getEventTimestamp()).isEqualTo(Long.MAX_VALUE);
        assertThat(event.getEventId()).isEmpty();
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

    @Test
    void rejectsTrailingContentAfterAnEvent() {
        String json = """
                {"udid":"device-1","start_id":1791614142000,"protol_type":"start"} {}
                """;
        assertThatThrownBy(() -> parser.map(json)).isInstanceOf(Exception.class);
    }
}
