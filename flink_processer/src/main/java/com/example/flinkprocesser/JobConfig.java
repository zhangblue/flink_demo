package com.example.flinkprocesser;

import java.net.URI;
import org.apache.flink.util.ParameterTool;

/** Flink 作业的命令行配置及集中校验规则。 */
public record JobConfig(
        String bootstrapServers,
        String inputTopic,
        String outputTopic,
        String errorTopic,
        String groupId,
        int parallelism,
        long checkpointIntervalMs,
        String checkpointStorageUri,
        String savepointStorageUri) {

    public JobConfig {
        bootstrapServers = requiredString(bootstrapServers, "bootstrap-servers");
        inputTopic = requiredString(inputTopic, "input-topic");
        outputTopic = requiredString(outputTopic, "output-topic");
        errorTopic = requiredString(errorTopic, "error-topic");
        groupId = requiredString(groupId, "group-id");
        checkpointStorageUri = absoluteFileUri(checkpointStorageUri, "checkpoint-storage-uri");
        savepointStorageUri = absoluteFileUri(savepointStorageUri, "savepoint-storage-uri");
        if (inputTopic.equals(outputTopic) || inputTopic.equals(errorTopic) || outputTopic.equals(errorTopic)) {
            throw new IllegalArgumentException("Input, output and error topics must be distinct");
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        if (checkpointIntervalMs <= 0) {
            throw new IllegalArgumentException("checkpoint-interval-ms must be positive");
        }
    }

    public static JobConfig fromArgs(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        return new JobConfig(
                stringParameter(parameters, "bootstrap-servers", "192.168.250.106:9092"),
                stringParameter(parameters, "input-topic", "flink_demo"),
                stringParameter(parameters, "output-topic", "flink_output"),
                stringParameter(parameters, "error-topic", "flink_error"),
                stringParameter(parameters, "group-id", "flink-processer"),
                parameters.getInt("parallelism", 1),
                parameters.getLong("checkpoint-interval-ms", 60_000L),
                stringParameter(parameters, "checkpoint-storage-uri", "file:///home/zhangdi/workspace/flink_data/checkpoints"),
                stringParameter(parameters, "savepoint-storage-uri", "file:///home/zhangdi/workspace/flink_data/savepoints"));
    }

    private static String stringParameter(ParameterTool parameters, String name, String defaultValue) {
        String value = parameters.get(name, defaultValue);
        // ParameterTool 会用该占位符表示“参数存在但没有值”，必须显式拒绝以免写错 topic。
        if ("__NO_VALUE_KEY".equals(value)) {
            throw new IllegalArgumentException(name + " requires a value");
        }
        return value;
    }

    private static String requiredString(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    private static String absoluteFileUri(String value, String name) {
        String validated = requiredString(value, name);
        URI uri = URI.create(validated);
        // Flink Path(String) 不解码百分号编码，也不把 query/fragment 作为 URI 元数据。
        // 限定为无歧义的本地目录，避免配置指向不同目录或在运行时才无法解析文件系统。
        if (!"file".equals(uri.getScheme()) || uri.getPath() == null || !uri.getPath().startsWith("/")
                || uri.getRawAuthority() != null || uri.getRawQuery() != null || uri.getRawFragment() != null
                || validated.contains("%")) {
            throw new IllegalArgumentException(name + " must be an absolute local URI with lowercase file scheme"
                    + " without authority, query, fragment or percent-encoding");
        }
        return validated;
    }
}
