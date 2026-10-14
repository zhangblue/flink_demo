package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;

/** 将 Kafka JSON value 解析成事件时间分配与状态补全所需的轻量事件对象。 */
public class EventRecordParser implements MapFunction<String, EventRecord> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    /**
     * 触发条件：作为 Flink {@link MapFunction} 使用时，每收到一条字符串输入调用一次；
     * 当前生产拓扑的 Kafka 反序列化器也直接调用本方法，复用同一解析器。
     *
     * <p>作用：以当前系统时间为校正基准，检查 JSON 结构和必填信封字段，校正事件时间，
     * 生成下游状态补全使用的 {@link EventRecord}。解析或校验异常向调用方传播。</p>
     */
    @Override
    public EventRecord map(String json) throws Exception {
        return parseAt(json, System.currentTimeMillis());
    }

    /**
     * 可注入当前时间的解析核心，供 {@link #map(String)} 调用，并便于测试固定时间边界。
     *
     * <p>要求输入为 JSON 对象且必填字段类型合法；事件时间比 {@code now} 超前超过 5 秒时，
     * 将事件对象的时间改写为 {@code now}，同时更新原始 JSON 的对应顶层 {@code event_timestamp}
     * 字段并重新序列化。未触发校正时保留传入的 JSON 文本。</p>
     */
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
        // sequence 缺失时使用 Long.MAX_VALUE 作为约定默认值。
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
