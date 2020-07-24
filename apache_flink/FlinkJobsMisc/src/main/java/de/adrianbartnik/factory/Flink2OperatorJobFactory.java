package de.adrianbartnik.factory;

import de.adrianbartnik.operator.AbstractOperator;
import de.adrianbartnik.sink.AbstractSink;
import de.adrianbartnik.source.AbstractSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink2OperatorJobFactory<INPUT, INTERMEDIATE, OUTPUT> extends FlinkJobFactory<INPUT, OUTPUT> {

    public Flink2OperatorJobFactory(String[] arguments, boolean chaining, boolean checkpointing) {
        super(arguments, chaining, checkpointing);
    }

    public StreamExecutionEnvironment createJob(AbstractSource<INPUT> sourceCreator,
                                                AbstractOperator<INPUT, INTERMEDIATE> operator1,
                                                AbstractOperator<INTERMEDIATE, OUTPUT> operator2,
                                                AbstractSink<OUTPUT> sinkCreator) {

        StreamExecutionEnvironment executionEnvironment = setupExecutionEnvironment();

        executionEnvironment.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        DataStream<INPUT> source = sourceCreator.createSource(arguments, executionEnvironment);

        DataStream<INTERMEDIATE> intermediate1 = operator1.createOperator(arguments, source);
        DataStream<OUTPUT> intermediate2 = operator2.createOperator(arguments, intermediate1);

        sinkCreator.createSink(arguments, intermediate2);

        return executionEnvironment;
    }
}
