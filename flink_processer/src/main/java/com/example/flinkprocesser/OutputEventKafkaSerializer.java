package com.example.flinkprocesser;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class OutputEventKafkaSerializer implements KafkaRecordSerializationSchema<OutputEvent> {
    private final String topic;

    public OutputEventKafkaSerializer(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(OutputEvent event, KafkaSinkContext context, Long timestamp) {
        if (event.key() == null || event.key().isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        return new ProducerRecord<>(topic,
                event.key().getBytes(StandardCharsets.UTF_8),
                event.json().getBytes(StandardCharsets.UTF_8));
    }
}
