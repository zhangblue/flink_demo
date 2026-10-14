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

    /**
     * 从 common 事件的 {@code payload} 提取六个元数据字段，不读取顶层同名字段。
     *
     * <p>缺失、非字符串或空白值归一化为 null，是否可用由 {@link CommonMetadata#isComplete()}
     * 判断；JSON 解析失败向调用方传播。</p>
     */
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

    /**
     * 将元数据写入事件 JSON 顶层，始终覆盖六个同名字段；缺失值写为显式 JSON null。
     *
     * <p>输入应为 JSON 对象；复制原始对象以保留 payload 和其他未知字段，仅补全顶层字段。
     * JSON 解析或生成失败向调用方传播。</p>
     */
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
