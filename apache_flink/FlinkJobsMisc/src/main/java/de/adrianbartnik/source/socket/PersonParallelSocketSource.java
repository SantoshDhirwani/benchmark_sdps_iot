package de.adrianbartnik.source.socket;

import de.adrianbartnik.data.nexmark.NewPersonEvent;
import de.adrianbartnik.source.AbstractSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.io.Serializable;
import java.util.List;

public class PersonParallelSocketSource extends AbstractSource<NewPersonEvent> implements Serializable {

    private static final String OPERATOR_NAME = "PersonParallelSocketSource";

    private final List<String> hostnames;
    private final List<Integer> ports;

    public PersonParallelSocketSource(List<String> hostnames, List<Integer> ports, int parallelism) {
        super(parallelism);
        this.hostnames = hostnames;
        this.ports = ports;
    }

    @Override
    public DataStream<NewPersonEvent> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        TypeInformation<NewPersonEvent> typeInformation = TypeInformation.of(new TypeHint<NewPersonEvent>() {});

        PersonSocketSourceFunction function = new PersonSocketSourceFunction(hostnames, ports);

        return new DataStreamSource<>(executionEnvironment,
                typeInformation,
                new StreamSource<>(function),
                true,
                OPERATOR_NAME)
                .setParallelism(parallelism);
    }

    public class PersonSocketSourceFunction extends AbstractSocketSourceFunction<NewPersonEvent> {

        PersonSocketSourceFunction(List<String> hostnames, List<Integer> ports) {
            super(hostnames, ports);
        }

        @Override
        protected NewPersonEvent stringToRecord(String record) {
            String[] split = record.split(",");
            return new NewPersonEvent(
                    Long.valueOf(split[0]),
                    Long.valueOf(split[1]),
                    split[2],
                    split[3],
                    split[4],
                    split[5],
                    split[6],
                    split[7],
                    split[8],
                    split[9]);
        }

        @Override
        protected String getStartCommand() {
            return getRuntimeContext().getIndexOfThisSubtask() + ":persons\n";
        }
    }
}
