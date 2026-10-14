package com.example.flinkprocesser;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 按 {@code udid + start_id} 启动周期即时补全事件，并在 watermark 到达该周期最新事件时间加 30 秒后清理状态。
 *
 * <p>该函数在上游完成 {@code keyBy(EventRecord::recordKey)} 后运行，因此 Flink 会为每个组合 key
 * 隔离 {@link ValueState}。完整的 {@code common} 消息只负责写入公共元数据状态，不产生业务输出；
 * 其他类型消息读取该状态完成顶层字段补全。无法建立或读取完整状态的消息进入错误侧输出。</p>
 *
 * <p>状态生命周期使用事件时间定时器管理。任意类型消息的事件时间加 30 秒都会成为候选清理时刻，
 * 只有候选时刻比当前截止时间更晚时才续期，乱序消息不会缩短生命周期。watermark 推进到最新截止时间
 * 时才会删除该 key 的状态，处理时间推进本身不触发清理。</p>
 */
public class CommonStateEnrichmentFunction extends KeyedProcessFunction<String, EventRecord, OutputEvent> {
    private static final long serialVersionUID = 1L;

    /** 补全失败的数据使用该侧输出标签发送到 flink_error 对应的数据流。 */
    public static final OutputTag<OutputEvent> ERROR_OUTPUT_TAG =
            new OutputTag<OutputEvent>("common-enrichment-errors") { };

    /** 当前组合 key 最近一条合法 common 消息提供的六个公共字段。 */
    private transient ValueState<CommonMetadata> metadataState;

    /** 当前组合 key 最大的事件时间清理截止时间戳，用于单调续期和识别陈旧回调。 */
    private transient ValueState<Long> cleanupTimerState;

    /**
     * Flink 创建并初始化该算子实例时触发一次，早于该实例处理任何输入消息。
     *
     * <p>此时 RuntimeContext 已可用，因此在这里注册两个托管 {@link ValueState}：一个保存 common
     * 元数据，另一个保存最新事件时间清理定时器的时间戳。状态描述符只初始化一次，但实际状态值由 Flink
     * 按当前 keyed stream 的 key 隔离，并与事件时间定时器一起参与 checkpoint/savepoint。</p>
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
     *   <li>无论消息类型是什么，先计算事件时间加 30 秒的清理候选值，只在候选值更晚时续期。
     *       如果最终截止值已经被当前 watermark 到达，则移除过去的定时器，并在本条消息完成即时路由后
     *       清空状态。截止时间加法溢出会在任何状态变更和业务输出之前抛出异常。</li>
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
        // 任意消息都参与事件时间生命周期管理；已到期状态仍完成本条消息的即时路由，随后立即清除。
        boolean cleanupAfterProcessing = refreshCleanupTimer(event, ctx.timerService());
        try {
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
        } finally {
            if (cleanupAfterProcessing) {
                metadataState.clear();
            }
        }
    }

    /**
     * 每次 {@link #processElement(EventRecord, Context, Collector)} 处理消息时调用，用于续期当前 key 的
     * 事件时间清理定时器。
     *
     * <p>先基于事件的 {@code event timestamp} 计算 30 秒后的候选截止值；只有当前 key 尚未注册定时器，
     * 或候选截止值比已保存的截止值更晚时，才删除旧事件时间定时器、注册新定时器并更新状态。
     * 相同时间或乱序事件保留原截止值，因此清理边界只会单调推进。如果候选值与已有截止值中的较晚者
     * 已经小于或等于当前 watermark，则删除已有的过去定时器并清空截止值状态，返回 {@code true}
     * 通知调用方在完成本条消息的即时业务路由后清除元数据。这样不会依赖 Flink 对过去事件时间定时器
     * 的下一次 watermark 回调，也不会因为单条迟到消息删除截止值仍在未来的已有状态。
     * {@link Math#addExact(long, long)} 在读取和改变状态之前校验加法溢出，避免在失败时产生状态或输出。</p>
     *
     * @return 本条消息完成即时业务路由后是否必须清除元数据状态
     */
    private boolean refreshCleanupTimer(EventRecord event, TimerService timerService) throws Exception {
        long candidate = Math.addExact(event.getEventTimestamp(), EventTimePolicy.STATE_CLEANUP_DELAY_MILLIS);
        Long previous = cleanupTimerState.value();
        long finalDeadline = previous == null ? candidate : Math.max(previous, candidate);
        if (finalDeadline <= timerService.currentWatermark()) {
            if (previous != null) {
                timerService.deleteEventTimeTimer(previous);
            }
            cleanupTimerState.clear();
            return true;
        }
        if (previous == null || candidate > previous) {
            if (previous != null) {
                timerService.deleteEventTimeTimer(previous);
            }
            timerService.registerEventTimeTimer(candidate);
            cleanupTimerState.update(candidate);
        }
        return false;
    }

    /**
     * Flink 的 watermark 达到某个已注册的事件时间截止值时触发，并自动恢复注册该定时器的 key 上下文。
     *
     * <p>回调时间戳只有与 {@link #cleanupTimerState} 保存的最新时间戳一致时，才表示该 key 的最新
     * 事件时间之后 30 秒的边界已被 watermark 到达，此时同时清除 common 元数据和定时器时间戳状态。
     * 若旧定时器在被删除后仍因竞态或恢复过程产生回调，时间戳不一致，函数会忽略它，避免误删后来续期的状态。
     * 定时器只负责清理，
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
