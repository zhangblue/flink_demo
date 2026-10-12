package com.example.flinkprocesser;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 将窗口补全结果拆分为正常主流和补全失败的侧输出流。
 */
public class AppEnrichmentWindowFunction extends ProcessWindowFunction<EventRecord, OutputEvent, String, TimeWindow> {
    /** 补全失败事件使用固定标签输出，作业入口通过同一标签连接 flink_error Sink。 */
    public static final OutputTag<OutputEvent> ERROR_OUTPUT_TAG = new OutputTag<OutputEvent>("app-enrichment-errors") { };

    @Override
    public void process(String key, Context context, Iterable<EventRecord> events, Collector<OutputEvent> output)
            throws Exception {
        EnrichmentBatch batch = new WindowEnricher().enrich(key, events);
        // 主输出只接收成功事件；失败事件走侧输出，保证一条输入不会同时进入两个 topic。
        batch.success().forEach(output::collect);
        batch.errors().forEach(event -> context.output(ERROR_OUTPUT_TAG, event));
    }
}
