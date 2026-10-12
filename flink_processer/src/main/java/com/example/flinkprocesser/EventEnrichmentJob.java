package com.example.flinkprocesser;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

public final class EventEnrichmentJob {
    private EventEnrichmentJob() { }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(env, config);
        env.execute("mobile-event-app-enrichment");
    }

    public static void buildPipeline(StreamExecutionEnvironment env, JobConfig config) {
        env.setParallelism(config.parallelism());
        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.AT_LEAST_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.inputTopic())
                .setGroupId(config.groupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<OutputEvent> output = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka input").uid("kafka-source")
                .map(new EventRecordParser()).uid("parse-json")
                .keyBy(EventRecord::recordKey)
                .window(createWindowAssigner())
                .process(new AppEnrichmentWindowFunction()).uid("app-enrichment-window");

        output.sinkTo(createSink(config, config.outputTopic())).uid("kafka-output-sink");
        output.getSideOutput(AppEnrichmentWindowFunction.ERROR_OUTPUT_TAG)
                .sinkTo(createSink(config, config.errorTopic())).uid("kafka-error-sink");
    }

    static TumblingProcessingTimeWindows createWindowAssigner() {
        return TumblingProcessingTimeWindows.of(Duration.ofMinutes(2));
    }

    private static KafkaSink<OutputEvent> createSink(JobConfig config, String topic) {
        return KafkaSink.<OutputEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new OutputEventKafkaSerializer(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
