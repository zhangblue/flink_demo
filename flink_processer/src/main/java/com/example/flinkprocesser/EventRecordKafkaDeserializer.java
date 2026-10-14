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

    /**
     * 触发条件：Kafka Source reader 每交付一个 {@link ConsumerRecord} 时调用一次。
     *
     * <p>作用：只读取 record 的 value，以 UTF-8 解码并复用事件解析器，成功后向 Collector 发送
     * 一条 {@link EventRecord}。空 value 或解析异常会使当前 Source 处理失败，并交由 Flink 容错机制
     * 处理；运行时异常直接传播，其他解析异常包装为 {@link IOException}。</p>
     */
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

    /**
     * 触发条件：Flink 构建类型系统和序列化器时查询反序列化器的产出类型。
     *
     * <p>作用：返回稳定的 {@link EventRecord} 类型信息，显式声明 Source 的输出类型，
     * 避免仅依赖泛型推断。</p>
     */
    @Override
    public TypeInformation<EventRecord> getProducedType() {
        return TypeInformation.of(EventRecord.class);
    }
}
