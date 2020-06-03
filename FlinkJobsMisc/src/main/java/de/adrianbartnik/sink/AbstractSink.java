package de.adrianbartnik.sink;

import de.adrianbartnik.factory.AbstractNode;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class AbstractSink<T> extends AbstractNode {

    public AbstractSink() {}

    public AbstractSink(int parallelism) {
        super(parallelism);
    }

    public abstract void createSink(String arguments[], DataStream<T> dataSource);
}

