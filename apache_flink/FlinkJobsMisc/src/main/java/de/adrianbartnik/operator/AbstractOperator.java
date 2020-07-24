package de.adrianbartnik.operator;

import de.adrianbartnik.factory.AbstractNode;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class AbstractOperator<IN, OUT> extends AbstractNode {

    public AbstractOperator() {}

    public AbstractOperator(int parallelism) {
        super(parallelism);
    }

    public abstract DataStream<OUT> createOperator(String arguments[], DataStream<IN> dataSource);
}
