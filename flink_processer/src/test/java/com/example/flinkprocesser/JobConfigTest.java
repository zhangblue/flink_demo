package com.example.flinkprocesser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobConfigTest {
    // 捕获 Flink 不支持的 authority/scheme，或 URI 经 Path(String) 后目录语义改变。
    @ParameterizedTest
    @CsvSource({
            "checkpoint-storage-uri,file://remote-host/tmp/checkpoints",
            "savepoint-storage-uri,file://remote-host/tmp/savepoints",
            "checkpoint-storage-uri,FILE:///tmp/checkpoints",
            "savepoint-storage-uri,FILE:///tmp/savepoints",
            "checkpoint-storage-uri,file:///tmp/my%20checkpoints",
            "savepoint-storage-uri,file:///tmp/my%20savepoints",
            "checkpoint-storage-uri,file:///tmp/my%25checkpoints",
            "savepoint-storage-uri,file:///tmp/my%25savepoints",
            "checkpoint-storage-uri,file:///tmp/checkpoints?ignored",
            "savepoint-storage-uri,file:///tmp/savepoints?ignored",
            "checkpoint-storage-uri,file:///tmp/checkpoints#ignored",
            "savepoint-storage-uri,file:///tmp/savepoints#ignored",
            "checkpoint-storage-uri,file:///tmp/checkpoints?",
            "savepoint-storage-uri,file:///tmp/savepoints?",
            "checkpoint-storage-uri,file:///tmp/checkpoints#",
            "savepoint-storage-uri,file:///tmp/savepoints#"
    })
    void rejectsUrisThatCannotPreserveLocalFlinkPathSemantics(String option, String uri) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option, uri}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(option)
                .hasMessageContaining("lowercase file scheme")
                .hasMessageContaining("without authority, query, fragment or percent-encoding");
    }

    // 捕获忽略存储目录参数、允许非绝对本地 file URI 的回归。
    @ParameterizedTest
    @ValueSource(strings = {"", " ", "relative/path", "file:relative/path", "file://relative",
            "hdfs:///checkpoints", "s3:///checkpoints", "file:///bad path"})
    void rejectsInvalidCheckpointStorageUri(String uri) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--checkpoint-storage-uri", uri}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "relative/path", "file:relative/path", "file://relative",
            "hdfs:///savepoints", "s3:///savepoints", "file:///bad path"})
    void rejectsInvalidSavepointStorageUri(String uri) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--savepoint-storage-uri", uri}))
                .isInstanceOf(IllegalArgumentException.class);
    }

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
        assertThat(config.checkpointStorageUri()).isEqualTo("file:///home/zhangdi/workspace/flink_data/checkpoints");
        assertThat(config.savepointStorageUri()).isEqualTo("file:///home/zhangdi/workspace/flink_data/savepoints");
    }

    @Test
    void readsOverridesAndTrimsStringValues() {
        JobConfig config = JobConfig.fromArgs(new String[]{
                "--bootstrap-servers", " broker:9092 ", "--input-topic", " in ",
                "--output-topic", " out ", "--error-topic", " errors ",
                "--group-id", " group ", "--parallelism", "4",
                "--checkpoint-interval-ms", "1500",
                "--checkpoint-storage-uri", " file:///tmp/checkpoints ",
                "--savepoint-storage-uri", " file:///tmp/savepoints "
        });

        assertThat(config).isEqualTo(new JobConfig("broker:9092", "in", "out", "errors", "group", 4, 1500L,
                "file:///tmp/checkpoints", "file:///tmp/savepoints"));
    }

    @Test
    void rejectsNullStorageUrisAtConstruction() {
        assertThatThrownBy(() -> new JobConfig("broker:9092", "in", "out", "errors", "group", 1, 1500L,
                null, "file:///tmp/savepoints")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new JobConfig("broker:9092", "in", "out", "errors", "group", 1, 1500L,
                "file:///tmp/checkpoints", null)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"checkpoint-storage-uri", "savepoint-storage-uri"})
    void rejectsStorageOptionWithoutValue(String option) {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[]{"--" + option, "--parallelism", "2"}))
                .isInstanceOf(IllegalArgumentException.class);
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
