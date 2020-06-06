package de.adrianbartnik.benchmarks.yahoo.operators;

import de.adrianbartnik.benchmarks.yahoo.objects.Event;
import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.JoinedEventWithCampaign;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.UUID;

/**
 * Joins event id with advertising id and the event timestamp without map
 */
public class IndependentJoinMapper<T extends Event> implements MapFunction<T, JoinedEventWithCampaign> {

    private UUID currentCampaignID = UUID.randomUUID();
    private long currentCount = 0;

    @Override
    public JoinedEventWithCampaign map(T value) {

        if (currentCount++ % 10 == 0) {
            currentCampaignID = UUID.randomUUID();
        }

        return new JoinedEventWithCampaign(currentCampaignID.toString(), value.advertisingId, value.eventTime);
    }
}
