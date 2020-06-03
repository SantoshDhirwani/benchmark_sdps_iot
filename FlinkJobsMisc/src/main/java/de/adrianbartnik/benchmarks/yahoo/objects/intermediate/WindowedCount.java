package de.adrianbartnik.benchmarks.yahoo.objects.intermediate;

import java.io.Serializable;
import java.sql.Timestamp;


public class WindowedCount implements Serializable {
    public final Timestamp timeWindow;
    public final String campaignId;
    public Long count;
    public Timestamp lastUpdate;

    /**
     * Class used to aggregate windowed counts.
     *
     * @param lastUpdate Event time of the last record received for tmp given `campaignId` and `timeWindow`
     */
    public WindowedCount(Timestamp timeWindow, String campaignId, long count, Timestamp lastUpdate) {
        this.timeWindow = timeWindow;
        this.campaignId = campaignId;
        this.count = count;
        this.lastUpdate = lastUpdate;
    }

    @Override
    public String toString() {
        return "WindowedCount - timeWindow: " + timeWindow + " campaignId: " + campaignId
                + " count: " + count + " last_update: " + lastUpdate;
    }
}
