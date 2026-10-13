package com.example.flinkprocesser;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/** 事件时间、水位线与状态清理使用的时间策略。 */
public final class EventTimePolicy {
    public static final long FUTURE_TOLERANCE_MILLIS = 5_000L;
    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    public static final Duration IDLE_TIMEOUT = Duration.ofMinutes(1);
    public static final long CLEANUP_INTERVAL_MILLIS = 30_000L;

    private EventTimePolicy() {
    }

    public static WatermarkStrategy<EventRecord> watermarkStrategy() {
        return WatermarkStrategy.<EventRecord>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, timestamp) -> event.getEventTimestamp())
                .withIdleness(IDLE_TIMEOUT);
    }
}
