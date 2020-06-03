package de.adrianbartnik.benchmarks.yahoo.objects;

import java.io.Serializable;
import java.sql.Timestamp;

public class Output implements Serializable {
    public final Timestamp time_window;
    public final String campaign_id;
    public final Long count;
    public final Timestamp lastUpdate;

    public Output(Timestamp time_window, String campaign_id, Long count, Timestamp lastUpdate) {
        this.time_window = time_window;
        this.campaign_id = campaign_id;
        this.count = count;
        this.lastUpdate = lastUpdate;
    }
}

