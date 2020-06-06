package de.adrianbartnik.job.timestampextractor;

import de.adrianbartnik.data.nexmark.NewPersonEvent;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class PersonEventTimestampExtractor extends AscendingTimestampExtractor<NewPersonEvent> {
    @Override
    public long extractAscendingTimestamp(NewPersonEvent element) {
        return element.getTimestamp();
    }
}
