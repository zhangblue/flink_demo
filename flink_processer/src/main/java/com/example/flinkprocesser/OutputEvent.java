package com.example.flinkprocesser;

/** 准备写入 Kafka 的 record key 与 JSON value。 */
public record OutputEvent(String key, String json) {
}
