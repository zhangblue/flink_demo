package com.example.flinkprocesser;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class AppEnrichmentWindowFunction extends ProcessWindowFunction<EventRecord, OutputEvent, String, TimeWindow> {
    public static final OutputTag<OutputEvent> ERROR_OUTPUT_TAG = new OutputTag<OutputEvent>("app-enrichment-errors") { };

    @Override
    public void process(String key, Context context, Iterable<EventRecord> events, Collector<OutputEvent> output)
            throws Exception {
        EnrichmentBatch batch = new WindowEnricher().enrich(key, events);
        batch.success().forEach(output::collect);
        batch.errors().forEach(event -> context.output(ERROR_OUTPUT_TAG, event));
    }
}
