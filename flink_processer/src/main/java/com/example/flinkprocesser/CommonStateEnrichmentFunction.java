package com.example.flinkprocesser;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 按 {@code udid + start_id} 启动周期即时补全事件，并在该启动周期连续 30 秒无新消息后清理状态。
 *
 * <p>该函数在上游完成 {@code keyBy(EventRecord::recordKey)} 后运行，因此 Flink 会为每个组合 key
 * 隔离 {@link ValueState}。完整的 {@code common} 消息只负责写入公共元数据状态，不产生业务输出；
 * 其他类型消息读取该状态完成顶层字段补全。无法建立或读取完整状态的消息进入错误侧输出。</p>
 *
 * <p>状态生命周期使用处理时间定时器管理。每收到任意类型消息都会把清理时刻顺延到当前处理时间
 * 之后 30 秒；只有最新定时器触发时才会真正删除该 key 的状态。</p>
 */
public class CommonStateEnrichmentFunction extends KeyedProcessFunction<String, EventRecord, OutputEvent> {
    private static final long serialVersionUID = 1L;
    private static final long CLEANUP_DELAY_MILLIS = 30_000L;

    /** 补全失败的数据使用该侧输出标签发送到 flink_error 对应的数据流。 */
    public static final OutputTag<OutputEvent> ERROR_OUTPUT_TAG =
            new OutputTag<OutputEvent>("common-enrichment-errors") { };

    /** 当前组合 key 最近一条合法 common 消息提供的六个公共字段。 */
    private transient ValueState<CommonMetadata> metadataState;

    /** 当前组合 key 最新的处理时间清理定时器时间戳，用于续期和识别陈旧回调。 */
    private transient ValueState<Long> cleanupTimerState;

    /**
     * Flink 创建并初始化该算子实例时触发一次，早于该实例处理任何输入消息。
     *
     * <p>此时 RuntimeContext 已可用，因此在这里注册两个托管 {@link ValueState}：一个保存 common
     * 元数据，另一个保存最新清理定时器的时间戳。状态描述符只初始化一次，但实际状态值由 Flink
     * 按当前 keyed stream 的 key 隔离，并参与 checkpoint/savepoint。</p>
     */
    @Override
    public void open(OpenContext openContext) throws Exception {
        metadataState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("common-metadata", CommonMetadata.class));
        cleanupTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cleanup-timer", Long.class));
    }

    /**
     * 同一算子实例每收到一条已经按组合 key 分组的事件时触发。
     *
     * <p>处理顺序如下：</p>
     * <ol>
     *   <li>无论消息类型是什么，先把该 key 的状态清理时间顺延到当前处理时间之后 30 秒。</li>
     *   <li>如果是 {@code common}，从 payload 提取六个公共字段。字段完整时覆盖元数据状态且不输出；
     *       字段不完整时不修改已有元数据状态，把有效字段和显式 null 写到事件顶层后仅输出到错误侧流。</li>
     *   <li>如果不是 {@code common}，读取当前 key 的元数据状态并在事件顶层写入六个字段。状态存在时
     *       只写主输出；状态不存在时写入六个 null，并且只输出到错误侧流。</li>
     * </ol>
     *
     * <p>主输出和错误侧输出互斥，一条输入消息最多进入其中一个；合法 {@code common} 不进入任何
     * 输出流。</p>
     */
    @Override
    public void processElement(EventRecord event, Context ctx, Collector<OutputEvent> out) throws Exception {
        // 任意消息都代表该启动周期仍然活跃，因此业务分支执行前先刷新不活跃清理时间。
        refreshCleanupTimer(ctx.timerService());
        if ("common".equals(event.getProtolType())) {
            CommonMetadata candidate = CommonMetadataJson.extract(event.getJson());
            if (candidate.isComplete()) {
                // 后到的合法 common 会完整覆盖旧状态，但 common 本身不进入正常输出。
                metadataState.update(candidate);
            } else {
                // 不完整 common 仅用于错误排查，不能污染当前 key 已存在的合法元数据。
                ctx.output(ERROR_OUTPUT_TAG, new OutputEvent(
                        event.recordKey(), CommonMetadataJson.enrich(event.getJson(), candidate)));
            }
            return;
        }

        CommonMetadata metadata = metadataState.value();
        OutputEvent result = new OutputEvent(event.recordKey(), CommonMetadataJson.enrich(
                event.getJson(), metadata == null ? new CommonMetadata() : metadata));
        if (metadata == null) {
            // 没有 common 状态时仍补齐字段结构，但六个值均为 JSON null，并路由到错误流。
            ctx.output(ERROR_OUTPUT_TAG, result);
        } else {
            // 只有成功读取完整 common 状态的非 common 事件才进入正常输出。
            out.collect(result);
        }
    }

    /**
     * 每次 {@link #processElement(EventRecord, Context, Collector)} 处理消息时调用，用于续期当前 key 的
     * 不活跃清理定时器。
     *
     * <p>如果该 key 已注册定时器，先删除旧定时器；随后基于 Flink 当前处理时间计算 30 秒后的新时间，
     * 注册新定时器并把时间戳写入状态。这里使用处理时间而不是事件中的 event timestamp，因此消息内容、
     * watermark 和发送时间不会改变清理边界。{@link Math#addExact(long, long)} 会在极端时间戳溢出时显式
     * 抛出异常，避免注册错误的过去时间。</p>
     */
    private void refreshCleanupTimer(TimerService timerService) throws Exception {
        Long previous = cleanupTimerState.value();
        if (previous != null) {
            timerService.deleteProcessingTimeTimer(previous);
        }
        long next = Math.addExact(timerService.currentProcessingTime(), CLEANUP_DELAY_MILLIS);
        timerService.registerProcessingTimeTimer(next);
        cleanupTimerState.update(next);
    }

    /**
     * Flink 的处理时间达到某个已注册时间戳时触发，并自动恢复注册该定时器的 key 上下文。
     *
     * <p>回调时间戳只有与 {@link #cleanupTimerState} 保存的最新时间戳一致时，才表示该 key 确实已经
     * 连续 30 秒没有新消息，此时同时清除 common 元数据和定时器时间戳状态。若旧定时器在被删除后仍因
     * 竞态或恢复过程产生回调，时间戳不一致，函数会忽略它，避免误删后来续期的状态。定时器只负责清理，
     * 不产生主输出或错误输出。</p>
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutputEvent> out) throws Exception {
        Long current = cleanupTimerState.value();
        if (current != null && current == timestamp) {
            metadataState.clear();
            cleanupTimerState.clear();
        }
    }
}
