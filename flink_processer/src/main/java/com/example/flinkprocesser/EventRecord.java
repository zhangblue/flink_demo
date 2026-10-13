package com.example.flinkprocesser;

/**
 * Flink 流转使用的轻量事件对象。
 *
 * <p>保留原始 JSON 以避免丢失未知字段，只额外解析分组、排序和路由所需的信封字段。</p>
 */
public class EventRecord {
    private String json;
    private String udid;
    private long startId;
    private String protolType;
    private long sequence;
    private long eventTimestamp;
    private String eventId;

    public EventRecord() {
        // Flink POJO 序列化器需要公开的无参构造器。
    }

    /** 返回状态分组和两个输出 Kafka record 共用的启动周期组合 key。 */
    public String recordKey() {
        return udid + "_" + startId;
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }

    public String getUdid() {
        return udid;
    }

    public void setUdid(String udid) {
        this.udid = udid;
    }

    public long getStartId() {
        return startId;
    }

    public void setStartId(long startId) {
        this.startId = startId;
    }

    public String getProtolType() {
        return protolType;
    }

    public void setProtolType(String protolType) {
        this.protolType = protolType;
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
}
