package de.adrianbartnik.operator;

import de.adrianbartnik.factory.FlinkJobFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class TextAppender extends AbstractOperator<String, String> {

    private static final String OPERATOR_NAME = "TextAppender";

    public TextAppender() {
        super();
    }

    public TextAppender(int parallelism) {
        super(parallelism);
    }

    @Override
    public DataStream<String> createOperator(String[] arguments, DataStream<String> dataSource) {
        return dataSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return input + " - appended text";
            }
        }).name(OPERATOR_NAME).setParallelism(parallelism);
    }
}