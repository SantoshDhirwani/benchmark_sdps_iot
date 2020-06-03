package de.adrianbartnik.benchmarks.yahoo.operators;

import de.adrianbartnik.benchmarks.yahoo.objects.Event;
import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.JoinedEventWithCampaign;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Map;

/**
 * Joins event id with advertising id and the event timestamp
 */
public class StaticJoinMapper<T extends Event> implements MapFunction<T, JoinedEventWithCampaign> {

    private final Map<String, String> campaigns;

    public StaticJoinMapper(Map<String, String> campaigns) {
        this.campaigns = campaigns;
    }

    @Override
    public JoinedEventWithCampaign map(T value) {

        String campaignID = campaigns.get(value.advertisingId);

        if (campaignID == null) {
            throw new IllegalStateException("Could not find corresponding campaign");
        }

        return new JoinedEventWithCampaign(campaignID, value.advertisingId, value.eventTime);
    }
}
