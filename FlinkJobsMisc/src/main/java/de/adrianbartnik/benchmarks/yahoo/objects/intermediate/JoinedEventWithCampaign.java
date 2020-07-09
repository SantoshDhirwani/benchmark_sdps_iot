package de.adrianbartnik.benchmarks.yahoo.objects.intermediate;

import java.sql.Timestamp;

public class JoinedEventWithCampaign {
    public final String campaignId;
    public final String advertisingId;
    public final Timestamp eventTime;

    public JoinedEventWithCampaign(String campaignId, String advertisingId, Timestamp eventTime) {
        this.campaignId = campaignId;
        this.advertisingId = advertisingId;
        this.eventTime = eventTime;
    }
}
