package com.example.flinkprocesser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 执行单个业务 key、单个滚动窗口内的 App 信息补全。
 *
 * <p>该类不持有跨窗口状态，便于独立测试，也明确保留固定滚动窗口的边界语义。</p>
 */
public class WindowEnricher {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Comparator<EventRecord> APP_PRIORITY = Comparator
            .comparingLong(EventRecord::getSequence)
            .thenComparingLong(EventRecord::getEventTimestamp);
    private static final Comparator<EventRecord> OUTPUT_ORDER = APP_PRIORITY
            .thenComparing(EventRecord::getEventId);

    public EnrichmentBatch enrich(String key, Iterable<EventRecord> events) throws JsonProcessingException {
        List<EventRecord> ordered = new ArrayList<>();
        for (EventRecord event : events) {
            // 防止错误调用把其他启动周期的数据混入当前窗口结果。
            if (!Objects.equals(key, event.recordKey())) {
                throw new IllegalArgumentException("Window contains an event with another business key");
            }
            ordered.add(event);
        }
        ordered.sort(OUTPUT_ORDER);

        Optional<EventRecord> selectedApp = ordered.stream()
                .filter(WindowEnricher::isApp)
                // 异常出现多条 app 时选择序号和事件时间最大的记录，保证结果确定。
                .max(APP_PRIORITY);
        AppMetadata metadata = selectedApp.isPresent()
                ? extractMetadata(selectedApp.get()) : new AppMetadata(null, null);

        List<OutputEvent> success = new ArrayList<>();
        List<OutputEvent> errors = new ArrayList<>();
        for (EventRecord event : ordered) {
            if (isApp(event)) {
                // app 本身无需补全，保持原 JSON 并进入正常输出。
                success.add(new OutputEvent(key, event.getJson()));
                continue;
            }
            ObjectNode output = ((ObjectNode) MAPPER.readTree(event.getJson())).deepCopy();
            setField(output, "app_name", metadata.name());
            setField(output, "app_version", metadata.version());
            OutputEvent enriched = new OutputEvent(key, MAPPER.writeValueAsString(output));
            if (metadata.name() != null && metadata.version() != null) {
                success.add(enriched);
            } else {
                // 任一字段缺失都视为补全失败，但仍输出带 null 字段的完整事件便于排查。
                errors.add(enriched);
            }
        }
        return new EnrichmentBatch(success, errors);
    }

    private static boolean isApp(EventRecord event) {
        return "app".equals(event.getProtolType());
    }

    private static AppMetadata extractMetadata(EventRecord app) throws JsonProcessingException {
        JsonNode payload = MAPPER.readTree(app.getJson()).path("payload");
        String selfPackage = nonemptyText(payload.path("self_package_name"));
        JsonNode apps = payload.path("apps");
        if (selfPackage != null && apps.isArray()) {
            for (JsonNode installed : apps) {
                if (selfPackage.equals(nonemptyText(installed.path("package_name")))) {
                    return new AppMetadata(nonemptyText(installed.path("app_name")),
                            nonemptyText(installed.path("version_name")));
                }
            }
        }
        return new AppMetadata(null, null);
    }

    private static String nonemptyText(JsonNode node) {
        return node.isTextual() && !node.textValue().isEmpty() ? node.textValue() : null;
    }

    private static void setField(ObjectNode output, String field, String value) {
        if (value == null) {
            output.putNull(field);
        } else {
            output.put(field, value);
        }
    }

    private record AppMetadata(String name, String version) {
    }
}
