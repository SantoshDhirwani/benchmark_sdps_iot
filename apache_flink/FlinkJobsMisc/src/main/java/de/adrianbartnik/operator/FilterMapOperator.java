package de.adrianbartnik.operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class FilterMapOperator extends AbstractOperator<String, String> {

    private static final String MAP_OPERATOR_NAME = "AppendMapper";
    private static final String FILTER_OPERATOR_NAME = "AppendMapper";

    private final int mapParallelism;

    public FilterMapOperator() {
        super();
        this.mapParallelism = 3;
    }

    public FilterMapOperator(int filterParallelism, int mapParallelism) {
        super(filterParallelism);
        this.mapParallelism = mapParallelism;
    }

    @Override
    public DataStream<String> createOperator(String[] arguments, DataStream<String> dataSource) {
        SingleOutputStreamOperator<String> filterOperator = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String string) throws Exception {
                return Long.valueOf(string.substring(10)) % 2 == 0;
            }
        }).setParallelism(parallelism).name(MAP_OPERATOR_NAME);

        return filterOperator.map(new MapFunction<String, String>() {
            @Override
            public String map(String input) throws Exception {
                return "Received: " + input;
            }
        }).setParallelism(mapParallelism).name(FILTER_OPERATOR_NAME);
    }
}