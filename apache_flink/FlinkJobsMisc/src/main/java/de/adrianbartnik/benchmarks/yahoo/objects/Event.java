package de.adrianbartnik.benchmarks.yahoo.objects;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Input data
 */
public class Event implements Serializable {

    public final String userId;
    public final String pageId;
    public final String advertisingId;
    public final String advertisingType;
    public final String eventType;
    public final Timestamp eventTime;
    public final String ipAddress;

    public Event(String userId, String pageId, String advertisingId, String advertisingType, String eventType, Timestamp eventTime, String ipAddress) {
        this.userId = userId;
        this.pageId = pageId;
        this.advertisingId = advertisingId;
        this.advertisingType = advertisingType;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.ipAddress = ipAddress;
    }
}
