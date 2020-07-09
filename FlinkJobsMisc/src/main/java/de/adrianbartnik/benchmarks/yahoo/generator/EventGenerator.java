package de.adrianbartnik.benchmarks.yahoo.generator;

import de.adrianbartnik.benchmarks.yahoo.objects.CampaignAd;
import de.adrianbartnik.benchmarks.yahoo.objects.Constants;
import de.adrianbartnik.benchmarks.yahoo.objects.Event;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;
import java.util.UUID;

/**
 * Flink internal generator for the YahooStreamingBenchmark
 */
public class EventGenerator extends RichParallelSourceFunction<Event> {

    private final String uuid = UUID.randomUUID().toString(); // used as tmp dummy value for all events, based on ref code

    private static final int AD_TYPE_LENGTH = Constants.AD_TYPES.size();
    private static final int EVENT_TYPE_LENGTH = Constants.EVENT_TYPES.size();

    private final CampaignAd[] campaingsArray;
    private final int campaignLength;
    private final long maxNumberOfEvents;
    private final int artificialDelay;

    private volatile boolean running = true;

    private long currentNumberOfEvents = 0;

    public EventGenerator(List<CampaignAd> campaigns, long maxNumberOfEvents, int artificialDelay) {
        this.campaingsArray = campaigns.toArray(new CampaignAd[campaigns.size()]);
        this.campaignLength = campaigns.size();
        this.maxNumberOfEvents = maxNumberOfEvents;
        this.artificialDelay = artificialDelay;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {

        int i = 0, j = 0, k = 0, t = 0;
        long ts = System.currentTimeMillis();

        while (running) {
            i += 1;
            j += 1;
            k += 1;
            t += 1;

            if (i >= campaignLength) {
                i = 0;
            }

            if (j >= AD_TYPE_LENGTH) {
                j = 0;
            }

            if (k >= EVENT_TYPE_LENGTH) {
                k = 0;
            }

            if (t >= 1000) {
                t = 0;
                ts = System.currentTimeMillis();
            }

            String ad_id = campaingsArray[i].ad_id; // ad id for the current event index
            String ad_type = Constants.AD_TYPES.get(j); // current adtype for event index
            String event_type = Constants.EVENT_TYPES.get(k); // current event type for event index

            Event event = new Event(
                    uuid, // random user, irrelevant
                    uuid, // random page, irrelevant
                    ad_id,
                    ad_type,
                    event_type,
                    new java.sql.Timestamp(ts),
                    "255.255.255.255"); // generic ipaddress, irrelevant
            ctx.collect(event);

            Thread.sleep(artificialDelay);

            currentNumberOfEvents++;

            if (currentNumberOfEvents > maxNumberOfEvents) {
                break;
            }
        }

        ctx.close();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
