package com.example.flinkprocesser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobConfigTest {
    @Test
    void usesRequestedDefaults() {
        JobConfig config = JobConfig.fromArgs(new String[0]);

        assertThat(config.bootstrapServers()).isEqualTo("192.168.250.106:9092");
        assertThat(config.inputTopic()).isEqualTo("flink_demo");
        assertThat(config.outputTopic()).isEqualTo("flink_output");
        assertThat(config.errorTopic()).isEqualTo("flink_error");
        assertThat(config.groupId()).isEqualTo("flink-processer");
        assertThat(config.parallelism()).isEqualTo(1);
        assertThat(config.checkpointIntervalMs()).isEqualTo(60_000L);
    }

    @Test
    void readsOverridesAndTrimsStringValues() {
        JobConfig config = JobConfig.fromArgs(new String[]{
                "--bootstrap-servers", " broker:9092 ", "--input-topic", " in ",
                "--output-topic", " out ", "--error-topic", " errors ",
                "--group-id", " group ", "--parallelism", "4",
                "--checkpoint-interval-ms", "1500"
        });

        assertThat(config).isEqualTo(new JobConfig("broker:9092", "in", "out", "errors", "group", 4, 1500L));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bootstrap-servers", "input-topic", "output-topic", "error-topic", "group-id"})
    void rejectsEmptyStrings(String option) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option, ""}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"bootstrap-servers", "input-topic", "output-topic", "error-topic", "group-id"})
    void rejectsWhitespaceOnlyStrings(String option) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option, "  "}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"bootstrap-servers", "input-topic", "output-topic", "error-topic", "group-id"})
    void rejectsStringOptionWithoutValueAtEnd(String option) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"bootstrap-servers", "input-topic", "output-topic", "error-topic", "group-id"})
    void rejectsStringOptionWithoutValueBeforeAnotherOption(String option) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option, "--parallelism", "2"}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"input-topic,flink_output", "input-topic,flink_error", "output-topic,flink_error"})
    void rejectsEachDuplicateTopicPair(String option, String value) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option, value}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"parallelism,0", "parallelism,-1", "checkpoint-interval-ms,0", "checkpoint-interval-ms,-1"})
    void rejectsNonPositiveNumbers(String option, String value) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option, value}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
