package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.Flink2OperatorJobFactory;
import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.operator.FilterLongOperator;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.IntervalSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IndependentOperatorWithFilterStateJob {

    private static final String JOB_NAME = "IndependentOperatorWithFilterStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int maxNumberOfMessages = params.getInt("maxNumberOfMessages", 100_000);
        final int pauseDuration = params.getInt("pauseDuration", 50);
        final int sourceParallelism = params.getInt("sourceParallelism", 3);
        final int filterParallelism = params.getInt("filterParallelism", 4);
        final int mapParallelism = params.getInt("mapParallelism", 4);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final boolean chaining = params.getBoolean("chaining", false);
        final String output_path = params.get("path", "benchmarkOutput");

        Flink2OperatorJobFactory<Long, Long, String> creator = new Flink2OperatorJobFactory<>(args, chaining, true);

        StreamExecutionEnvironment job =
                creator.createJob(new IntervalSequenceSource(0, maxNumberOfMessages, pauseDuration, sourceParallelism),
                        new FilterLongOperator(filterParallelism),
                        new CountingMap<>(mapParallelism),
                        new TextOutputSink<>(sinkParallelism, output_path));

        job.execute(JOB_NAME);
    }
}
