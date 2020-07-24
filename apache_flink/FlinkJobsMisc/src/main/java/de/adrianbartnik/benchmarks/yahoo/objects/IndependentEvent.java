package de.adrianbartnik.benchmarks.yahoo.objects;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Input data
 */
public class IndependentEvent extends Event {

    private final long id;

    public IndependentEvent(long id, String userId, String pageId, String advertisingId, String advertisingType, String eventType, Timestamp eventTime, String ipAddress) {
        super(userId, pageId, advertisingId, advertisingType, eventType, eventTime, ipAddress);
        this.id = id;
    }
}
