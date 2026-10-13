package com.example.flinkprocesser;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * 手机事件公共信息补全作业入口。
 *
 * <p>作业从 Kafka 读取原始事件，按组合键维护补全状态，再将正常和异常数据
 * 分别写入两个 Kafka topic。</p>
 */
public final class EventEnrichmentJob {
    private EventEnrichmentJob() { }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(env, config);
        env.execute("mobile-event-common-enrichment");
    }

    public static void buildPipeline(StreamExecutionEnvironment env, JobConfig config) {
        env.setParallelism(config.parallelism());
        // Source 与两个 Sink 均采用至少一次语义，checkpoint 用于提交消费进度和恢复状态。
        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.AT_LEAST_ONCE);

        KafkaSource<EventRecord> source = KafkaSource.<EventRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.inputTopic())
                .setGroupId(config.groupId())
                // 优先恢复消费组已提交位置；新消费组从最早 offset 开始，避免漏掉历史数据。
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new EventRecordKafkaDeserializer())
                .build();

        SingleOutputStreamOperator<OutputEvent> output = env
                .fromSource(source, EventTimePolicy.watermarkStrategy(), "Kafka input").uid("kafka-source")
                // 保留既有 UID；解析已在 Source 内完成，供分区水位线使用。
                .map(event -> event).returns(EventRecord.class).uid("parse-json")
                // Flink 内部分组和下游 Kafka record 使用完全相同的组合 key。
                .keyBy(EventRecord::recordKey)
                .process(new CommonStateEnrichmentFunction()).uid("common-state-enrichment");

        output.sinkTo(createSink(config, config.outputTopic())).uid("kafka-output-sink");
        output.getSideOutput(CommonStateEnrichmentFunction.ERROR_OUTPUT_TAG)
                .sinkTo(createSink(config, config.errorTopic())).uid("kafka-error-sink");
    }

    private static KafkaSink<OutputEvent> createSink(JobConfig config, String topic) {
        return KafkaSink.<OutputEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new OutputEventKafkaSerializer(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
