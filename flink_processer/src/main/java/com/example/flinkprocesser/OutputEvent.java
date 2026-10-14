package com.example.flinkprocesser;

/**
 * 状态补全算子交给 Kafka Sink 的 record key 与 JSON value。
 *
 * <p>{@code key} 沿用事件的 udid_startId 组合键；{@code json} 在主输出和错误侧输出中都已写入
 * 六个顶层 common 字段，缺失值为显式 JSON null。原始 JSON 的未知字段保留，
 * Kafka 序列化器只负责将 key/value 编码写出。</p>
 */
public record OutputEvent(String key, String json) {
}
