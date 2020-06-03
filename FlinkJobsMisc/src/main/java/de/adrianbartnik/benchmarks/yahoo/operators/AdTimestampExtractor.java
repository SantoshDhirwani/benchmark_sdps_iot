package de.adrianbartnik.benchmarks.yahoo.operators;

import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.JoinedEventWithCampaign;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class AdTimestampExtractor extends AscendingTimestampExtractor<JoinedEventWithCampaign> {
    @Override
    public long extractAscendingTimestamp(JoinedEventWithCampaign element) {
        return element.eventTime.getTime();
    }
}
