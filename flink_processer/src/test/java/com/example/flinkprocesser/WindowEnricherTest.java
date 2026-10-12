package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WindowEnricherTest {
    private static final String KEY = "device-1:1791614142000";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COMPLETE_PAYLOAD = """
            {"self_package_name":"com.tencent.mm","apps":[
              {"package_name":"other","app_name":"Other","version_name":"1.0"},
              {"package_name":"com.tencent.mm","app_name":"WeChat","version_name":"8.0.62"}]}
            """;
    private final EventRecordParser parser = new EventRecordParser();
    private final WindowEnricher enricher = new WindowEnricher();

    // Catches dropping pre-app events, choosing an unrelated installed app, or mutating input JSON.
    @Test
    void enrichesEveryNonAppEventAndPreservesOriginalAppJson() throws Exception {
        List<EventRecord> events = List.of(
                event("start", 1, 101, "start-1", "{\"app_version\":\"fallback\",\"unknown\":true}"),
                event("devinfo", 2, 102, "devinfo-1", "{\"memory\":42}"),
                event("app", 3, 103, "app-1", COMPLETE_PAYLOAD),
                event("environment", 4, 104, "environment-1", "{\"network\":\"wifi\"}"));
        List<String> originals = events.stream().map(EventRecord::getJson).toList();

        EnrichmentBatch batch = enricher.enrich(KEY, events);

        assertThat(batch.errors()).isEmpty();
        assertThat(batch.success()).hasSize(4).extracting(OutputEvent::key).containsOnly(KEY);
        assertThat(sequences(batch.success())).containsExactly(1L, 2L, 3L, 4L);
        assertThat(batch.success().get(2).json()).isEqualTo(events.get(2).getJson());
        for (int index : List.of(0, 1, 3)) {
            JsonNode output = MAPPER.readTree(batch.success().get(index).json());
            assertThat(output.path("app_name").asText()).isEqualTo("WeChat");
            assertThat(output.path("app_version").asText()).isEqualTo("8.0.62");
            JsonNode source = MAPPER.readTree(events.get(index).getJson());
            source.fields().forEachRemaining(field -> assertThat(output.get(field.getKey())).isEqualTo(field.getValue()));
        }
        JsonNode app = MAPPER.readTree(batch.success().get(2).json());
        assertThat(app.has("app_name")).isFalse();
        assertThat(app.has("app_version")).isFalse();
        assertThat(events).extracting(EventRecord::getJson).containsExactlyElementsOf(originals);
    }

    // Catches routing incomplete enrichment to success, using the start version, or omitting explicit nulls.
    @ParameterizedTest(name = "{0}")
    @MethodSource("incompleteMetadata")
    void routesIncompleteNonAppEventsOnlyToErrors(String scenario, String appPayload,
                                                 String expectedName, String expectedVersion) throws Exception {
        List<EventRecord> events = new ArrayList<>(List.of(
                event("start", 1, 101, "start-1", "{\"app_version\":\"fallback\"}"),
                event("devinfo", 2, 102, "devinfo-1", "{}"),
                event("environment", 4, 104, "environment-1", "{}")));
        if (appPayload != null) {
            events.add(event("app", 3, 103, "app-1", appPayload));
        }

        EnrichmentBatch batch = enricher.enrich(KEY, events);

        assertThat(batch.errors()).hasSize(3).extracting(OutputEvent::key).containsOnly(KEY);
        assertThat(sequences(batch.errors())).containsExactly(1L, 2L, 4L);
        for (OutputEvent output : batch.errors()) {
            JsonNode json = MAPPER.readTree(output.json());
            assertField(json, "app_name", expectedName);
            assertField(json, "app_version", expectedVersion);
            assertThat(json.path("protol_type").asText()).isNotEqualTo("app");
        }
        if (appPayload == null) {
            assertThat(batch.success()).isEmpty();
        } else {
            assertThat(batch.success()).containsExactly(new OutputEvent(KEY, events.get(3).getJson()));
        }
        List<String> routedIds = new ArrayList<>(eventIds(batch.success()));
        routedIds.addAll(eventIds(batch.errors()));
        assertThat(routedIds).containsExactlyInAnyOrderElementsOf(events.stream().map(EventRecord::getEventId).toList());
    }

    static Stream<Arguments> incompleteMetadata() {
        return Stream.of(
                Arguments.of("no app", null, null, null),
                Arguments.of("no package match", "{\"self_package_name\":\"com.tencent.mm\",\"apps\":[{\"package_name\":\"other\",\"app_name\":\"Other\",\"version_name\":\"1.0\"}]}", null, null),
                Arguments.of("missing app name", matchingPayload("{\"version_name\":\"8.0.62\"}"), null, "8.0.62"),
                Arguments.of("missing version", matchingPayload("{\"app_name\":\"WeChat\"}"), "WeChat", null),
                Arguments.of("empty app name", matchingPayload("{\"app_name\":\"\",\"version_name\":\"8.0.62\"}"), null, "8.0.62"),
                Arguments.of("empty version", matchingPayload("{\"app_name\":\"WeChat\",\"version_name\":\"\"}"), "WeChat", null),
                Arguments.of("null app name", matchingPayload("{\"app_name\":null,\"version_name\":\"8.0.62\"}"), null, "8.0.62"),
                Arguments.of("null version", matchingPayload("{\"app_name\":\"WeChat\",\"version_name\":null}"), "WeChat", null),
                Arguments.of("missing self package", "{\"apps\":[{\"package_name\":\"com.tencent.mm\",\"app_name\":\"WeChat\",\"version_name\":\"8.0.62\"}]}", null, null),
                Arguments.of("missing apps", "{\"self_package_name\":\"com.tencent.mm\"}", null, null));
    }

    // Catches choosing the first app encountered or prioritizing timestamp above sequence.
    @ParameterizedTest(name = "{0}")
    @MethodSource("appPriorities")
    void selectsAppBySequenceThenTimestampRegardlessOfInputOrder(String scenario,
                                                               long oldSequence, long oldTimestamp,
                                                               long selectedSequence, long selectedTimestamp) throws Exception {
        List<EventRecord> events = new ArrayList<>(List.of(
                event("app", oldSequence, oldTimestamp, "old-app", matchingPayload("{\"app_name\":\"Old\",\"version_name\":\"1.0\"}")),
                event("start", 1, 101, "start-1", "{}"),
                event("app", selectedSequence, selectedTimestamp, "new-app", COMPLETE_PAYLOAD)));

        EnrichmentBatch batch = enricher.enrich(KEY, events);
        Collections.reverse(events);
        EnrichmentBatch reversed = enricher.enrich(KEY, events);

        assertThat(reversed).isEqualTo(batch);
        assertThat(batch.errors()).isEmpty();
        assertThat(batch.success()).hasSize(3);
        JsonNode enriched = MAPPER.readTree(batch.success().get(0).json());
        assertThat(enriched.path("app_name").asText()).isEqualTo("WeChat");
        assertThat(enriched.path("app_version").asText()).isEqualTo("8.0.62");
    }

    static Stream<Arguments> appPriorities() {
        return Stream.of(
                Arguments.of("greater sequence wins even with older timestamp", 2L, 999L, 3L, 100L),
                Arguments.of("equal sequence uses greater timestamp", 3L, 100L, 3L, 999L));
    }

    // Catches missing secondary or tertiary ordering of equal-sequence events.
    @Test
    void ordersOutputsBySequenceThenTimestampThenEventId() throws Exception {
        List<EventRecord> events = List.of(
                event("devinfo", 2, 200, "event-b", "{}"),
                event("environment", 2, 200, "event-a", "{}"),
                event("start", 2, 100, "event-z", "{}"),
                event("app", 3, 300, "app-1", COMPLETE_PAYLOAD));

        EnrichmentBatch batch = enricher.enrich(KEY, events);

        assertThat(eventIds(batch.success())).containsExactly("event-z", "event-a", "event-b", "app-1");
    }

    // Catches cross-device or cross-launch enrichment when a caller supplies mixed keys.
    @ParameterizedTest
    @ValueSource(strings = {"device-2:1791614142000", "device-1:1791614142001"})
    void rejectsAnyRecordWithAnotherBusinessKey(String otherKey) throws Exception {
        EventRecord other = event("app", 3, 103, "app-1", COMPLETE_PAYLOAD);
        String[] parts = otherKey.split(":");
        other.setUdid(parts[0]);
        other.setStartId(Long.parseLong(parts[1]));

        assertThatThrownBy(() -> enricher.enrich(KEY, List.of(event("start", 1, 101, "start-1", "{}"), other)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private EventRecord event(String type, long sequence, long timestamp, String id, String payload) throws Exception {
        return parser.map("""
                {"udid":"device-1","start_id":1791614142000,"protol_type":"%s",
                 "sequence":%d,"event_timestamp":%d,"event_id":"%s","payload":%s}
                """.formatted(type, sequence, timestamp, id, payload));
    }

    private static String matchingPayload(String metadata) {
        return "{\"self_package_name\":\"com.tencent.mm\",\"apps\":[{\"package_name\":\"com.tencent.mm\","
                + metadata.substring(1) + "]}";
    }

    private static void assertField(JsonNode json, String field, String expected) {
        assertThat(json.has(field)).isTrue();
        if (expected == null) {
            assertThat(json.get(field).isNull()).isTrue();
        } else {
            assertThat(json.get(field).asText()).isEqualTo(expected);
        }
    }

    private static List<Long> sequences(List<OutputEvent> outputs) throws Exception {
        List<Long> result = new ArrayList<>();
        for (OutputEvent output : outputs) {
            result.add(MAPPER.readTree(output.json()).path("sequence").asLong());
        }
        return result;
    }

    private static List<String> eventIds(List<OutputEvent> outputs) throws Exception {
        List<String> result = new ArrayList<>();
        for (OutputEvent output : outputs) {
            result.add(MAPPER.readTree(output.json()).path("event_id").asText());
        }
        return result;
    }
}
