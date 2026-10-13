package com.example.flinkprocesser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** 在 Source 内解析事件，让 Kafka 分区水位线使用校正后的事件时间。 */
public final class EventRecordKafkaDeserializer implements KafkaRecordDeserializationSchema<EventRecord> {
    private final EventRecordParser parser = new EventRecordParser();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EventRecord> output) throws IOException {
        if (record.value() == null) {
            throw new IllegalArgumentException("Kafka event value must not be null");
        }
        try {
            output.collect(parser.map(new String(record.value(), StandardCharsets.UTF_8)));
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IOException("Cannot parse Kafka event value", exception);
        }
    }

    @Override
    public TypeInformation<EventRecord> getProducedType() {
        return TypeInformation.of(EventRecord.class);
    }
}
