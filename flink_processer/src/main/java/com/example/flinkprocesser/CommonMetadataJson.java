package com.example.flinkprocesser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** common 元数据的 JSON 提取与事件顶层补全边界。 */
public final class CommonMetadataJson {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CommonMetadataJson() {
    }

    public static CommonMetadata extract(String json) throws JsonProcessingException {
        JsonNode payload = MAPPER.readTree(json).path("payload");
        return new CommonMetadata(
                nonblankText(payload.path("self_app_package")),
                nonblankText(payload.path("self_app_name")),
                nonblankText(payload.path("model")),
                nonblankText(payload.path("brand")),
                nonblankText(payload.path("os")),
                nonblankText(payload.path("os_version")));
    }

    public static String enrich(String json, CommonMetadata metadata) throws JsonProcessingException {
        ObjectNode output = ((ObjectNode) MAPPER.readTree(json)).deepCopy();
        setField(output, "self_app_package", metadata.getSelfAppPackage());
        setField(output, "self_app_name", metadata.getSelfAppName());
        setField(output, "model", metadata.getModel());
        setField(output, "brand", metadata.getBrand());
        setField(output, "os", metadata.getOs());
        setField(output, "os_version", metadata.getOsVersion());
        return MAPPER.writeValueAsString(output);
    }

    private static String nonblankText(JsonNode node) {
        return node != null && node.isTextual() && !node.textValue().isBlank()
                ? node.textValue() : null;
    }

    private static void setField(ObjectNode output, String name, String value) {
        if (value == null) {
            output.putNull(name);
        } else {
            output.put(name, value);
        }
    }
}
