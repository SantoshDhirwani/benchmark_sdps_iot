package de.adrianbartnik.operator;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Collections;
import java.util.List;

public class FilterLongOperator extends AbstractOperator<Long, Long> {

    private static final String OPERATOR_NAME = "FilterLongOperator";

    public FilterLongOperator() {
        super();
    }

    public FilterLongOperator(int parallelism) {
        super(parallelism);
    }

    @Override
    public DataStream<Long> createOperator(String[] arguments, DataStream<Long> dataSource) {
        return dataSource.filter(new InnerMap()).name(OPERATOR_NAME).setParallelism(parallelism);
    }

    public class InnerMap extends RichFilterFunction<Long> {
        @Override
        public boolean filter(Long value) {
            return value % 2 == 0;
        }
    }
}