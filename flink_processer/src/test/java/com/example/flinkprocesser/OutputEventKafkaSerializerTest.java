package com.example.flinkprocesser;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputEventKafkaSerializerTest {
    // Catches dropping the business key, changing the JSON, or writing both branches to one topic.
    @ParameterizedTest
    @ValueSource(strings = {"flink_output", "flink_error"})
    void preservesTopicBusinessKeyAndUtf8Json(String topic) {
        OutputEvent event = new OutputEvent(
                "device-1_1791614142000",
                "{\"self_app_name\":\"微信\"}");

        ProducerRecord<byte[], byte[]> record = new OutputEventKafkaSerializer(topic)
                .serialize(event, null, null);

        assertThat(record.topic()).isEqualTo(topic);
        assertThat(new String(record.key(), UTF_8))
                .isEqualTo("device-1_1791614142000");
        assertThat(new String(record.value(), UTF_8)).isEqualTo(event.json());
    }

    // Catches letting Kafka receive an invalid destination.
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void rejectsBlankTopic(String topic) {
        assertThatThrownBy(() -> new OutputEventKafkaSerializer(topic))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // Catches silently producing an unkeyed record, which breaks the partitioning contract.
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void rejectsBlankBusinessKey(String key) {
        OutputEventKafkaSerializer serializer = new OutputEventKafkaSerializer("flink_output");

        assertThatThrownBy(() -> serializer.serialize(new OutputEvent(key, "{}"), null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
