package de.adrianbartnik.factory;

import de.adrianbartnik.operator.AbstractOperator;
import de.adrianbartnik.sink.AbstractSink;
import de.adrianbartnik.source.AbstractSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkJobFactory<INPUT, OUTPUT> {

    private static final int CHECKPOINTING_INTERVAL = 1000;

    protected final String[] arguments;

    private final boolean chaining;
    private final boolean checkpointing;

    public FlinkJobFactory(String arguments[], boolean chaining, boolean checkpointing) {
        this.arguments = arguments;
        this.checkpointing = checkpointing;
        this.chaining = chaining;
    }

    public StreamExecutionEnvironment createJob(AbstractSource<INPUT> sourceCreator,
                                                AbstractOperator<INPUT, OUTPUT> jobCreator,
                                                AbstractSink<OUTPUT> sinkCreator) {
        StreamExecutionEnvironment executionEnvironment = setupExecutionEnvironment();

        DataStream<INPUT> source = sourceCreator.createSource(arguments, executionEnvironment);

        DataStream<OUTPUT> stream = jobCreator.createOperator(arguments, source);
        sinkCreator.createSink(arguments, stream);

        return executionEnvironment;
    }

    public StreamExecutionEnvironment setupExecutionEnvironment() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        if (!chaining) {
            executionEnvironment.disableOperatorChaining();
        }

        if (checkpointing) {
            executionEnvironment.enableCheckpointing(CHECKPOINTING_INTERVAL);
            executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        }

        return executionEnvironment;
    }

    public StreamExecutionEnvironment setupExecutionEnvironmentWithStateBackend(ParameterTool params) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        final String external_backend = params.get("backend", "memory");
        final String backendPath = params.get("backendPath", "hdfs://ibm-power-1.dima.tu-berlin.de:44000/user/bartnik");
        final boolean asynchronousCheckpoints = params.getBoolean("asynchronousCheckpoints", true);
        final boolean incrementalCheckpoints = params.getBoolean("incrementalCheckpoints", true);
        final int checkpointingInterval = params.getInt("checkpointingInterval", 1000);
        final long checkpointingTimeout = params.getLong("checkpointingTimeout", CheckpointConfig.DEFAULT_TIMEOUT);
        final int concurrentCheckpoints = params.getInt("concurrentCheckpoints", 10);
        final long latencyTrackingInterval = params.getLong("latencyTrackingInterval", 2000);
        final boolean chaining = params.getBoolean("chaining", true);

        env.getCheckpointConfig().setCheckpointInterval(checkpointingInterval);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointingTimeout);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(concurrentCheckpoints);
        env.getConfig().setLatencyTrackingInterval(latencyTrackingInterval);

        if (!chaining) {
            env.disableOperatorChaining();
        }

        switch (external_backend) {
            case "memory":
                env.setStateBackend(new MemoryStateBackend());
                break;
            case "filesystem":
                env.setStateBackend(new FsStateBackend(backendPath, asynchronousCheckpoints));
                break;
            case "rocksdb":
                env.setStateBackend(new RocksDBStateBackend(backendPath, incrementalCheckpoints));
                break;
            default:
                throw new IllegalArgumentException();
        }

        return env;
    }
}
