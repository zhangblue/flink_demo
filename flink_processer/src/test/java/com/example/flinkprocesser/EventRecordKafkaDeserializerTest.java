package com.example.flinkprocesser;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventRecordKafkaDeserializerTest {
    // 捕获忽略 value、使用错误编码或未调用严格解析器的回归。
    @Test
    void deserializesUtf8ValueIntoTypedEvent() throws Exception {
        String json = "{\"udid\":\"device-1\",\"start_id\":1000,\"protol_type\":\"测试\",\"event_timestamp\":12000}";
        List<EventRecord> output = new ArrayList<>();

        new EventRecordKafkaDeserializer().deserialize(record(json.getBytes(StandardCharsets.UTF_8)), collector(output));

        assertThat(output).singleElement().satisfies(event -> {
            assertThat(event.recordKey()).isEqualTo("device-1_1000");
            assertThat(event.getProtolType()).isEqualTo("测试");
            assertThat(event.getEventTimestamp()).isEqualTo(12000L);
            assertThat(event.getJson()).isEqualTo(json);
        });
    }

    // 捕获 Kafka tombstone 被静默丢弃的回归。
    @Test
    void rejectsNullValueWithoutEmittingEvent() {
        List<EventRecord> output = new ArrayList<>();
        assertThatThrownBy(() -> new EventRecordKafkaDeserializer().deserialize(record(null), collector(output)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(output).isEmpty();
    }

    // 捕获反序列化路径绕开严格时间字段校验的回归。
    @Test
    void rejectsInvalidTimestampWithoutEmittingEvent() {
        String json = "{\"udid\":\"device-1\",\"start_id\":1000,\"protol_type\":\"event\",\"event_timestamp\":\"12000\"}";
        List<EventRecord> output = new ArrayList<>();
        assertThatThrownBy(() -> new EventRecordKafkaDeserializer().deserialize(
                record(json.getBytes(StandardCharsets.UTF_8)), collector(output)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(output).isEmpty();
    }

    private static ConsumerRecord<byte[], byte[]> record(byte[] value) {
        return new ConsumerRecord<>("input", 0, 1L, null, value);
    }

    private static Collector<EventRecord> collector(List<EventRecord> output) {
        return new Collector<>() {
            public void collect(EventRecord event) { output.add(event); }
            public void close() { }
        };
    }
}
