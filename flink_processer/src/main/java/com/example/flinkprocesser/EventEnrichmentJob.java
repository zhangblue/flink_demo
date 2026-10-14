package com.example.flinkprocesser;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
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

    /**
     * 由 Flink CLI 或 Java 进程入口调用，解析参数、取得执行环境并提交补全作业。
     *
     * <p>{@code env.execute} 之前只构建执行图，不处理输入数据；提交或执行失败向调用方传播。</p>
     */
    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        buildPipeline(env, config);
        env.execute("mobile-event-common-enrichment");
    }

    /**
     * 由入口在提交前调用，为执行环境配置 checkpoint 并构建 Source、状态补全和双 Sink 拓扑。
     *
     * <p>具有可用 common 状态的非 common 事件进入主输出，不完整 common 或缺少状态的事件
     * 进入错误侧输出；此方法仅注册算子。</p>
     */
    public static void buildPipeline(StreamExecutionEnvironment env, JobConfig config) {
        env.setParallelism(config.parallelism());
        Configuration flinkConfig = new Configuration();
        // 使用校验后的绝对本地目录保存 checkpoint/savepoint；恢复时仍须能访问这些文件。
        flinkConfig.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointStorageUri());
        flinkConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, config.savepointStorageUri());
        env.configure(flinkConfig);
        // Source 与两个 Sink 均采用至少一次语义，checkpoint 用于提交消费进度和恢复状态。
        env.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.AT_LEAST_ONCE);
        // 取消作业后保留外部化 checkpoint，供后续显式恢复使用。
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        KafkaSource<EventRecord> source = KafkaSource.<EventRecord>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.inputTopic())
                .setGroupId(config.groupId())
                // 恢复快照中的 Source offset 优先，其次使用 consumer group committed offset；
                // 某分区缺失有效 committed offset 时，仅该分区从最新位置开始。
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new EventRecordKafkaDeserializer())
                .build();

        SingleOutputStreamOperator<OutputEvent> output = env
                // Source 使用解析后校正的事件时间分配 watermark，并检测空闲分区。
                .fromSource(source, EventTimePolicy.watermarkStrategy(), "Kafka input").uid("kafka-source")
                // 保留既有 UID；解析已在 Source 内完成，供分区水位线使用。
                .map(event -> event).returns(EventRecord.class).uid("parse-json")
                // udid_startId 隔离每个设备启动周期的 keyed state，也用于下游 Kafka record key。
                .keyBy(EventRecord::recordKey)
                .process(new CommonStateEnrichmentFunction()).uid("common-state-enrichment");

        // 两条互斥输出各自写入独立 topic；主输出携带已补全的顶层 common 字段。
        output.sinkTo(createSink(config, config.outputTopic())).uid("kafka-output-sink");
        // 错误侧输出同样写入顶层字段，供下游处理不完整 common 或缺少可用状态的事件。
        output.getSideOutput(CommonStateEnrichmentFunction.ERROR_OUTPUT_TAG)
                .sinkTo(createSink(config, config.errorTopic())).uid("kafka-error-sink");
    }

    /**
     * 为每个目标 topic 创建独立 Kafka Sink，由 {@link OutputEventKafkaSerializer} 统一生成 record key。
     *
     * <p>采用至少一次投递语义，故障恢复边界可能重复写出已投递记录。</p>
     */
    private static KafkaSink<OutputEvent> createSink(JobConfig config, String topic) {
        return KafkaSink.<OutputEvent>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setRecordSerializer(new OutputEventKafkaSerializer(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
