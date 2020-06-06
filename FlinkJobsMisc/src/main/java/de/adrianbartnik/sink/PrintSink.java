package de.adrianbartnik.sink;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.streaming.api.datastream.DataStream;

public class PrintSink<T> extends AbstractSink {

    public PrintSink() {
        super();
    }

    public PrintSink(int parallelism) {
        super(parallelism);
    }

    @Override
    public void createSink(String[] arguments, DataStream dataSource) {
        dataSource.print().setParallelism(parallelism);
    }
}
