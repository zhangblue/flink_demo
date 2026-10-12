package com.example.flinkprocesser;

public class EventRecord {
    private String json;
    private String udid;
    private long startId;
    private String protolType;
    private long sequence;
    private long eventTimestamp;
    private String eventId;

    public EventRecord() {
    }

    public String recordKey() {
        return udid + ":" + startId;
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
