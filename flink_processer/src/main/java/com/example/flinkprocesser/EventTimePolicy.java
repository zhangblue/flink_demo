package com.example.flinkprocesser;

import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/** 事件时间、水位线与状态清理使用的时间策略。 */
public final class EventTimePolicy {
    /** 允许事件时间比处理时刻最多超前 5 秒，超过时由解析器校正。 */
    public static final long FUTURE_TOLERANCE_MILLIS = 5_000L;
    /** watermark 容忍事件相对已观察最大事件时间落后 5 秒的乱序范围。 */
    public static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
    /** 分区连续 1 分钟无事件时标记为空闲，避免它阻塞其他分区的 watermark。 */
    public static final Duration IDLE_TIMEOUT = Duration.ofMinutes(1);
    /** 当前 key 的最新事件时间后 30 秒为清理边界，以 watermark 判定到期，而非等待墙钟 30 秒。 */
    public static final long STATE_CLEANUP_DELAY_MILLIS = 30_000L;

    private EventTimePolicy() {
    }

    /**
     * 作业构建时调用以创建 Source 的事件时间策略。
     *
     * <p>运行时逐条提取解析器校正后的 {@code eventTimestamp}；watermark 按已观察最大事件时间
     * 减 5 秒乱序容忍推进（毫秒边界再减 1）。空闲分区超时后暂不参与全局最小 watermark，
     * 恢复接收数据后重新参与。</p>
     */
    public static WatermarkStrategy<EventRecord> watermarkStrategy() {
        return WatermarkStrategy.<EventRecord>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                .withTimestampAssigner((event, timestamp) -> event.getEventTimestamp())
                .withIdleness(IDLE_TIMEOUT);
    }
}
