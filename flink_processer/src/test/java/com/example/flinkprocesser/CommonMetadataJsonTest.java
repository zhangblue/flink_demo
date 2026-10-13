package com.example.flinkprocesser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class CommonMetadataJsonTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String COMMON_JSON = """
            {"udid":"device-1","start_id":1791614142000,"protol_type":"common",
             "payload":{"self_app_package":"com.example.demo","self_app_name":"Demo",
                        "model":"Pixel 10","brand":"Google","os":"Android",
                        "os_version":"17"}}
            """;

    // 抓住字段来源读错、字段映射错位，或把完整元数据误判为不完整。
    @Test
    void extractsAllSixCommonPayloadFields() throws Exception {
        CommonMetadata metadata = CommonMetadataJson.extract(COMMON_JSON);

        assertThat(metadata.isComplete()).isTrue();
        assertThat(metadata.getSelfAppPackage()).isEqualTo("com.example.demo");
        assertThat(metadata.getSelfAppName()).isEqualTo("Demo");
        assertThat(metadata.getModel()).isEqualTo("Pixel 10");
        assertThat(metadata.getBrand()).isEqualTo("Google");
        assertThat(metadata.getOs()).isEqualTo("Android");
        assertThat(metadata.getOsVersion()).isEqualTo("17");
    }

    // 抓住补全漏写、未覆盖旧值，以及重建根对象时丢掉未知字段或原 payload。
    @Test
    void enrichesSixTopLevelFieldsAndPreservesOtherInput() throws Exception {
        String input = """
                {"protol_type":"start","unknown":true,
                 "self_app_package":"old-package","self_app_name":"old-name",
                 "model":"old-model","brand":"old-brand","os":"old-os",
                 "os_version":"old-version","payload":{"nested":[1,2]}}
                """;
        CommonMetadata metadata = CommonMetadataJson.extract(COMMON_JSON);

        JsonNode output = MAPPER.readTree(CommonMetadataJson.enrich(input, metadata));

        assertThat(output.path("self_app_package").textValue()).isEqualTo("com.example.demo");
        assertThat(output.path("self_app_name").textValue()).isEqualTo("Demo");
        assertThat(output.path("model").textValue()).isEqualTo("Pixel 10");
        assertThat(output.path("brand").textValue()).isEqualTo("Google");
        assertThat(output.path("os").textValue()).isEqualTo("Android");
        assertThat(output.path("os_version").textValue()).isEqualTo("17");
        assertThat(output.path("unknown").booleanValue()).isTrue();
        assertThat(output.path("protol_type").textValue()).isEqualTo("start");
        assertThat(output.path("payload")).isEqualTo(MAPPER.readTree("{\"nested\":[1,2]}"));
    }

    // 抓住任一字段将缺失、null、数字、空串或空白串当作有效文本的缺陷。
    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("invalidFields")
    void normalizesInvalidPayloadFieldAndWritesExplicitTopLevelNull(String field, String caseName,
                                                                      String invalidJson) throws Exception {
        ObjectNode input = (ObjectNode) MAPPER.readTree(COMMON_JSON);
        ObjectNode payload = (ObjectNode) input.path("payload");
        if (invalidJson == null) {
            payload.remove(field);
        } else {
            payload.set(field, MAPPER.readTree(invalidJson));
        }

        CommonMetadata metadata = CommonMetadataJson.extract(MAPPER.writeValueAsString(input));
        JsonNode output = MAPPER.readTree(CommonMetadataJson.enrich("{\"protol_type\":\"start\"}", metadata));

        assertThat(metadata.isComplete()).isFalse();
        assertThat(metadata.getSelfAppPackage()).isEqualTo(field.equals("self_app_package") ? null : "com.example.demo");
        assertThat(metadata.getSelfAppName()).isEqualTo(field.equals("self_app_name") ? null : "Demo");
        assertThat(metadata.getModel()).isEqualTo(field.equals("model") ? null : "Pixel 10");
        assertThat(metadata.getBrand()).isEqualTo(field.equals("brand") ? null : "Google");
        assertThat(metadata.getOs()).isEqualTo(field.equals("os") ? null : "Android");
        assertThat(metadata.getOsVersion()).isEqualTo(field.equals("os_version") ? null : "17");
        assertField(output, "self_app_package", field.equals("self_app_package") ? null : "com.example.demo");
        assertField(output, "self_app_name", field.equals("self_app_name") ? null : "Demo");
        assertField(output, "model", field.equals("model") ? null : "Pixel 10");
        assertField(output, "brand", field.equals("brand") ? null : "Google");
        assertField(output, "os", field.equals("os") ? null : "Android");
        assertField(output, "os_version", field.equals("os_version") ? null : "17");
    }

    static Stream<Arguments> invalidFields() {
        return Stream.of("self_app_package", "self_app_name", "model", "brand", "os", "os_version")
                .flatMap(field -> Stream.of(
                        Arguments.of(field, "missing", null),
                        Arguments.of(field, "JSON null", "null"),
                        Arguments.of(field, "number", "42"),
                        Arguments.of(field, "empty", "\"\""),
                        Arguments.of(field, "blank", "\"   \"")));
    }

    // 抓住从旧 App payload 取值，或补全时覆盖输入自带 App 顶层字段的缺陷。
    @Test
    void leavesLegacyAppFieldsUntouched() throws Exception {
        String appJson = """
                {"udid":"device-1","start_id":1791614142000,"protol_type":"app",
                 "app_name":"input-name","app_version":"input-version",
                 "payload":{"self_package_name":"com.old",
                            "apps":[{"package_name":"com.old","app_name":"Old","version_name":"1.0"}]}}
                """;

        JsonNode output = MAPPER.readTree(CommonMetadataJson.enrich(appJson, CommonMetadataJson.extract(COMMON_JSON)));

        assertThat(output.path("app_name").textValue()).isEqualTo("input-name");
        assertThat(output.path("app_version").textValue()).isEqualTo("input-version");
        assertThat(output.path("payload")).isEqualTo(MAPPER.readTree(appJson).path("payload"));
        assertThat(output.path("self_app_package").textValue()).isEqualTo("com.example.demo");
        assertThat(output.path("self_app_name").textValue()).isEqualTo("Demo");
        assertThat(output.path("model").textValue()).isEqualTo("Pixel 10");
        assertThat(output.path("brand").textValue()).isEqualTo("Google");
        assertThat(output.path("os").textValue()).isEqualTo("Android");
        assertThat(output.path("os_version").textValue()).isEqualTo("17");
    }

    private static void assertField(JsonNode output, String name, String expected) {
        assertThat(output.has(name)).isTrue();
        if (expected == null) {
            assertThat(output.get(name).isNull()).isTrue();
        } else {
            assertThat(output.get(name).textValue()).isEqualTo(expected);
        }
    }
}
