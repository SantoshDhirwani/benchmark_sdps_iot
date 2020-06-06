package de.adrianbartnik.operator;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

public class CountingTupleMap extends AbstractOperator<Tuple2<Timestamp, Long>, Tuple4<Timestamp, Long, String, Long>> {

    private static final String OPERATOR_NAME = "CountingMap";

    public CountingTupleMap() {
        super();
    }

    public CountingTupleMap(int parallelism) {
        super(parallelism);
    }

    @Override
    public DataStream<Tuple4<Timestamp, Long, String, Long>> createOperator(String[] arguments, DataStream<Tuple2<Timestamp, Long>> dataSource) {
        return dataSource.map(new InnerMap()).name(OPERATOR_NAME).setParallelism(parallelism);
    }

    /**
     * Each mapper counts how many items it has processed.
     */
    public class InnerMap extends RichMapFunction<Tuple2<Timestamp, Long>, Tuple4<Timestamp, Long, String, Long>> implements ListCheckpointed<Long> {

        private long numberOfProcessedElements = 0;

        private String taskNameWithSubtasks;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            taskNameWithSubtasks = getRuntimeContext().getTaskNameWithSubtasks();
        }

        @Override
        public Tuple4<Timestamp, Long, String, Long> map(Tuple2<Timestamp, Long> value) {
            numberOfProcessedElements++;
            return new Tuple4<>(value.f0, value.f1, taskNameWithSubtasks, numberOfProcessedElements);
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