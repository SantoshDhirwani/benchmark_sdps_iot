package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.job.parser.ParallelSocketArgumentParser;
import de.adrianbartnik.operator.CountingTupleMap;
import de.adrianbartnik.sink.latency.LatencySink;
import de.adrianbartnik.source.socket.TimestampedNumberParallelSocketSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;

public class ParallelSocketOperatorStateWithFilterJob {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelSocketOperatorStateWithFilterJob.class);

    private static final String JOB_NAME = "ParallelSocketOperatorStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int mapParallelism = params.getInt("mapParallelism", 4);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final String hostnames_string = params.get("hostnames");
        final String ports_string = params.get("ports");
        final String output_path = params.get("path", "benchmarkOutput");
        final boolean onlyLatency = params.getBoolean("onlyLatency", true);
        final int onlyNthLatencyOutput = params.getInt("onlyNthLatencyOutput", 1);

        if (hostnames_string == null || hostnames_string.isEmpty() || ports_string == null || ports_string.isEmpty()) {
            throw new IllegalArgumentException("Hostname and Ports must not be empty");
        }

        List<String> hostnames = ParallelSocketArgumentParser.ParseHostnames(hostnames_string);
        List<Integer> ports = ParallelSocketArgumentParser.ParsePorts(ports_string);

        if (ports.size() != hostnames.size()) {
            throw new IllegalArgumentException("Hostname and Ports must be of equal size");
        }

        final int sourceParallelism = hostnames.size();
        for (int i = 0; i < hostnames.size(); i++) {
            LOG.debug("Connecting to socket {}:{}", hostnames.get(i), ports.get(i));
        }

        StreamExecutionEnvironment environment =
                new FlinkJobFactory<>(args, false, true)
                        .setupExecutionEnvironment();

        DataStream<Tuple2<Timestamp, Long>> sourceFunction =
                new TimestampedNumberParallelSocketSource(hostnames, ports, sourceParallelism)
                        .createSource(args, environment);

        SingleOutputStreamOperator<Tuple2<Timestamp, Long>> filter =
                sourceFunction.filter((FilterFunction<Tuple2<Timestamp, Long>>) value -> value.f1 % 5 == 0);

        DataStream<Tuple4<Timestamp, Long, String, Long>> countingTupleMap =
                new CountingTupleMap(mapParallelism).createOperator(args, filter);

        new LatencySink(sinkParallelism, output_path, onlyLatency, onlyNthLatencyOutput).createSink(args, countingTupleMap);

        environment.execute(JOB_NAME);
    }
}