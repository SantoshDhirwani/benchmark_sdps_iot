package de.adrianbartnik.source.socket;

import de.adrianbartnik.source.AbstractSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class TimestampedNumberParallelSocketSource extends AbstractSource<Tuple2<Timestamp, Long>> implements Serializable {

    private static final String OPERATOR_NAME = "ParallelSocketSource";

    private final List<String> hostnames;
    private final List<Integer> ports;

    public TimestampedNumberParallelSocketSource(List<String> hostnames, List<Integer> ports, int parallelism) {
        super(parallelism);
        this.hostnames = hostnames;
        this.ports = ports;
    }

    @Override
    public DataStream<Tuple2<Timestamp, Long>> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        TypeInformation<Tuple2<Timestamp, Long>> typeInformation = TypeInformation.of(new TypeHint<Tuple2<Timestamp, Long>>() {});

        TimestampNumberSocketSocketFunction function = new TimestampNumberSocketSocketFunction(hostnames, ports);

        return new DataStreamSource<>(executionEnvironment,
                typeInformation,
                new StreamSource<>(function),
                true,
                OPERATOR_NAME)
                .setParallelism(parallelism);
    }

    public class TimestampNumberSocketSocketFunction extends AbstractSocketSourceFunction<Tuple2<Timestamp, Long>> {

        public TimestampNumberSocketSocketFunction(List<String> hostnames, List<Integer> ports) {
            super(hostnames, ports);
        }

        @Override
        protected Tuple2<Timestamp, Long> stringToRecord(String record) {
            if (record == null || !record.contains("#")) {
                throw new IllegalArgumentException("Malformed input from sockets: " + record);
            }

            String[] split = record.split("#");

            if (split.length != 2 || split[0].isEmpty() || split[1].isEmpty()) {
                throw new IllegalArgumentException("Malformed input from sockets: " + record);
            }

            return new Tuple2<>(new Timestamp(Long.valueOf(split[0])), Long.valueOf(split[1]));
        }

        @Override
        protected String getStartCommand() {
            return getRuntimeContext().getIndexOfThisSubtask() + ":from:" + numberProcessedMessages + ":reconnect\n";
        }
    }
}
