package de.adrianbartnik.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class CountingMap<Input> extends AbstractOperator<Input, String> {

    private static final String OPERATOR_NAME = "CountingMap";

    public CountingMap() {
        super();
    }

    public CountingMap(int parallelism) {
        super(parallelism);
    }

    @Override
    public DataStream<String> createOperator(String[] arguments, DataStream<Input> dataSource) {
        return dataSource.map(new InnerMap<Input>()).name(OPERATOR_NAME).setParallelism(parallelism);
    }

    /**
     * Each mapper counts how many items it has processed.
     */
    public class InnerMap<INPUT> extends RichMapFunction<INPUT, String> implements ListCheckpointed<Long> {

        private long numberOfProcessedElements = 0;

        private String taskNameWithSubtasks;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
        }

        @Override
        public String map(INPUT value) {
            numberOfProcessedElements++;
            return value + " - " + taskNameWithSubtasks + " - " + numberOfProcessedElements;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(numberOfProcessedElements);
        }

        @Override
        public void restoreState(List<Long> state) {
            for (Long number : state) {
                numberOfProcessedElements += number;
            }
        }
    }
}