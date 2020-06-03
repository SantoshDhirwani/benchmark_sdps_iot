package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.IntervalSequenceSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IndependentOperatorStateJob {

    private static final String JOB_NAME = "IndependentOperatorStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int maxNumberOfMessages = params.getInt("maxNumberOfMessages", 100_000);
        final int pauseDuration = params.getInt("pauseDuration", 50);
        final int sourceParallelism = params.getInt("sourceParallelism", 3);
        final int mapParallelism = params.getInt("mapParallelism", 4);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final String output_path = params.get("path", "benchmarkOutput");

        StreamExecutionEnvironment env = new FlinkJobFactory<>(args, false, true)
                .setupExecutionEnvironmentWithStateBackend(params);

        DataStream<Long> source = new IntervalSequenceSource(0, maxNumberOfMessages, pauseDuration, sourceParallelism)
                .createSource(args, env);

        DataStream<String> coutingMap = new CountingMap<Long>(mapParallelism).createOperator(args, source);

        new TextOutputSink<String>(sinkParallelism, output_path).createSink(args, coutingMap);

        env.execute(JOB_NAME);
    }
}