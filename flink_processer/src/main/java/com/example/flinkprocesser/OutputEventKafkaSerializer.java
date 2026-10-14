package com.example.flinkprocesser;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

/** 将补全结果序列化为带组合 key 的 Kafka ProducerRecord。 */
public class OutputEventKafkaSerializer implements KafkaRecordSerializationSchema<OutputEvent> {
    private final String topic;

    public OutputEventKafkaSerializer(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be blank");
        }
        this.topic = topic;
    }

    /**
     * 触发条件：Kafka Sink 写出每个 {@link OutputEvent} 前调用一次。
     *
     * <p>作用：将组合 key 和 JSON value 均以 UTF-8 编码，写入构造时指定的固定 topic。
     * 当前 {@link KafkaSinkContext} 和输入时间戳不参与分区选择或 record timestamp 设置；
     * 返回的 record 未显式指定分区或时间戳。空或空白 key 通过异常阻止错误记录发送。</p>
     */
    @Override
    public ProducerRecord<byte[], byte[]> serialize(OutputEvent event, KafkaSinkContext context, Long timestamp) {
        if (event.key() == null || event.key().isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
        // key 与 value 都显式使用 UTF-8，确保两个输出 topic 的分区规则一致。
        return new ProducerRecord<>(topic,
                event.key().getBytes(StandardCharsets.UTF_8),
                event.json().getBytes(StandardCharsets.UTF_8));
    }
}
