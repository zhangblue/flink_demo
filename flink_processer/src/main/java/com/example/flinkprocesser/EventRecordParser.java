package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;

/** 将 Kafka JSON value 解析成窗口计算所需的轻量事件对象。 */
public class EventRecordParser implements MapFunction<String, EventRecord> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @Override
    public EventRecord map(String json) throws Exception {
        return parseAt(json, System.currentTimeMillis());
    }

    EventRecord parseAt(String json, long now) throws Exception {
        ObjectNode root = requireObject(MAPPER.readTree(json));
        String udid = requiredText(root, "udid");
        String protolType = requiredText(root, "protol_type");
        JsonNode startId = root.get("start_id");
        if (startId == null || !startId.isIntegralNumber() || !startId.canConvertToLong()) {
            throw new IllegalArgumentException("start_id must be a 64-bit integer");
        }
        long timestamp = requiredLong(root, "event_timestamp");
        long futureLimit = Math.addExact(now, EventTimePolicy.FUTURE_TOLERANCE_MILLIS);
        if (timestamp > futureLimit) {
            timestamp = now;
            root.put("event_timestamp", now);
            json = MAPPER.writeValueAsString(root);
        }
        return createEvent(root, json, timestamp, udid, protolType, startId);
    }

    private static ObjectNode requireObject(JsonNode root) {
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("Event must be a JSON object");
        }
        return (ObjectNode) root;
    }

    private static long requiredLong(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new IllegalArgumentException(field + " must be a 64-bit integer");
        }
        return value.longValue();
    }

    private static EventRecord createEvent(JsonNode root, String json, long timestamp,
                                           String udid, String protolType, JsonNode startId) {
        EventRecord event = new EventRecord();
        event.setJson(json);
        event.setUdid(udid);
        event.setStartId(startId.longValue());
        event.setProtolType(protolType);
        // 排序字段缺失时放到窗口末尾；它们不是业务分组的必填字段。
        event.setSequence(root.path("sequence").asLong(Long.MAX_VALUE));
        event.setEventTimestamp(timestamp);
        event.setEventId(root.path("event_id").asText(""));
        return event;
    }

    private static String requiredText(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException(field + " must be a nonblank string");
        }
        return value.textValue();
    }
}
