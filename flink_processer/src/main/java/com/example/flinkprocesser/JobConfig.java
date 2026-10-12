package com.example.flinkprocesser;

import org.apache.flink.util.ParameterTool;

public record JobConfig(
        String bootstrapServers,
        String inputTopic,
        String outputTopic,
        String errorTopic,
        String groupId,
        int parallelism,
        long checkpointIntervalMs) {

    public JobConfig {
        bootstrapServers = requiredString(bootstrapServers, "bootstrap-servers");
        inputTopic = requiredString(inputTopic, "input-topic");
        outputTopic = requiredString(outputTopic, "output-topic");
        errorTopic = requiredString(errorTopic, "error-topic");
        groupId = requiredString(groupId, "group-id");
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
                parameters.getLong("checkpoint-interval-ms", 60_000L));
    }

    private static String stringParameter(ParameterTool parameters, String name, String defaultValue) {
        String value = parameters.get(name, defaultValue);
        // ParameterTool stores this marker when an option is present without a value.
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
}
