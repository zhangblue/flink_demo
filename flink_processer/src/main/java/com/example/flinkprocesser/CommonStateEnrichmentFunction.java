package com.example.flinkprocesser;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** 按启动周期即时补全事件，并在最后一条消息 30 秒后清理 common 状态。 */
public class CommonStateEnrichmentFunction extends KeyedProcessFunction<String, EventRecord, OutputEvent> {
    private static final long serialVersionUID = 1L;
    private static final long CLEANUP_DELAY_MILLIS = 30_000L;

    public static final OutputTag<OutputEvent> ERROR_OUTPUT_TAG =
            new OutputTag<OutputEvent>("common-enrichment-errors") { };

    private transient ValueState<CommonMetadata> metadataState;
    private transient ValueState<Long> cleanupTimerState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        metadataState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("common-metadata", CommonMetadata.class));
        cleanupTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cleanup-timer", Long.class));
    }

    @Override
    public void processElement(EventRecord event, Context ctx, Collector<OutputEvent> out) throws Exception {
        refreshCleanupTimer(ctx.timerService());
        if ("common".equals(event.getProtolType())) {
            CommonMetadata candidate = CommonMetadataJson.extract(event.getJson());
            if (candidate.isComplete()) {
                metadataState.update(candidate);
            } else {
                ctx.output(ERROR_OUTPUT_TAG, new OutputEvent(
                        event.recordKey(), CommonMetadataJson.enrich(event.getJson(), candidate)));
            }
            return;
        }

        CommonMetadata metadata = metadataState.value();
        OutputEvent result = new OutputEvent(event.recordKey(), CommonMetadataJson.enrich(
                event.getJson(), metadata == null ? new CommonMetadata() : metadata));
        if (metadata == null) {
            ctx.output(ERROR_OUTPUT_TAG, result);
        } else {
            out.collect(result);
        }
    }

    private void refreshCleanupTimer(TimerService timerService) throws Exception {
        Long previous = cleanupTimerState.value();
        if (previous != null) {
            timerService.deleteProcessingTimeTimer(previous);
        }
        long next = Math.addExact(timerService.currentProcessingTime(), CLEANUP_DELAY_MILLIS);
        timerService.registerProcessingTimeTimer(next);
        cleanupTimerState.update(next);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutputEvent> out) throws Exception {
        Long current = cleanupTimerState.value();
        if (current != null && current == timestamp) {
            metadataState.clear();
            cleanupTimerState.clear();
        }
    }
}
