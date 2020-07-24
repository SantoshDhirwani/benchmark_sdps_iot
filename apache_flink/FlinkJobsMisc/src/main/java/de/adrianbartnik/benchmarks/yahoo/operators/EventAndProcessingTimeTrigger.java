package de.adrianbartnik.benchmarks.yahoo.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class EventAndProcessingTimeTrigger extends Trigger<Object, TimeWindow> {

    private final int triggerIntervalMs;
    private long nextTimer = 0L;

    public EventAndProcessingTimeTrigger(int triggerIntervalMs) {
        this.triggerIntervalMs = triggerIntervalMs;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        // register system timer only for the first time
        ValueState<Boolean> firstTimerSet = ctx.getKeyValueState("firstTimerSet", Boolean.class, Boolean.FALSE);
        if (!firstTimerSet.value()) {
            nextTimer = System.currentTimeMillis() + triggerIntervalMs;
            ctx.registerProcessingTimeTimer(nextTimer);
            firstTimerSet.update(true);
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
        // schedule next timer
        nextTimer = System.currentTimeMillis() + triggerIntervalMs;
        ctx.registerProcessingTimeTimer(nextTimer);
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ctx.deleteProcessingTimeTimer(nextTimer);
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
