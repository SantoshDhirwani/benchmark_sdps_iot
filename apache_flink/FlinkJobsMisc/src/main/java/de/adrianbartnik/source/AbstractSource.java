package de.adrianbartnik.source;

import de.adrianbartnik.factory.AbstractNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class AbstractSource<T> extends AbstractNode {

    public AbstractSource() {
        super();
    }

    public AbstractSource(int parallelism) {
        super(parallelism);
    }

    public abstract DataStream<T> createSource(String arguments[], StreamExecutionEnvironment executionEnvironment);
}
