package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

/** 将 Kafka JSON value 解析成窗口计算所需的轻量事件对象。 */
public class EventRecordParser implements MapFunction<String, EventRecord> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    @Override
    public EventRecord map(String json) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("Event must be a JSON object");
        }
        String udid = requiredText(root, "udid");
        String protolType = requiredText(root, "protol_type");
        JsonNode startId = root.get("start_id");
        if (startId == null || !startId.isIntegralNumber() || !startId.canConvertToLong()) {
            throw new IllegalArgumentException("start_id must be a 64-bit integer");
        }

        EventRecord event = new EventRecord();
        event.setJson(json);
        event.setUdid(udid);
        event.setStartId(startId.longValue());
        event.setProtolType(protolType);
        // 排序字段缺失时放到窗口末尾；它们不是业务分组的必填字段。
        event.setSequence(root.path("sequence").asLong(Long.MAX_VALUE));
        event.setEventTimestamp(root.path("event_timestamp").asLong(Long.MAX_VALUE));
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
