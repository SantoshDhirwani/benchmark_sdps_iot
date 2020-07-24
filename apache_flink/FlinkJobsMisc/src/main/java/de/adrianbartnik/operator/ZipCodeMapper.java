package de.adrianbartnik.operator;

import de.adrianbartnik.job.data.GeoEvent;
import de.adrianbartnik.job.data.ZipCodeGeoEvent;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.concurrent.ThreadLocalRandom;

public class ZipCodeMapper extends RichMapFunction<GeoEvent, ZipCodeGeoEvent> {
    @Override
    public ZipCodeGeoEvent map(GeoEvent value) {
        return new ZipCodeGeoEvent(ThreadLocalRandom.current().nextInt(16), value.category);
    }
}
