package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.job.data.GeoEvent;
import de.adrianbartnik.job.data.GeoWindow;
import de.adrianbartnik.job.data.ZipCodeGeoEvent;
import de.adrianbartnik.job.parser.ParallelSocketArgumentParser;
import de.adrianbartnik.operator.ZipCodeMapper;
import de.adrianbartnik.sink.latency.GeoWindowSink;
import de.adrianbartnik.source.socket.GeoEventParallelSocketSource;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.List;

public class GeoEventJob {

    private static final Logger LOG = LoggerFactory.getLogger(GeoEventJob.class);

    private static final String JOB_NAME = "ParallelSocketOperatorStateJob";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final String hostnames_string = params.get("hostnames");
        final String ports_string = params.get("ports");
        final String output_path = params.get("path", "benchmarkOutput");
        final int windowDuration = params.getInt("windowDuration", 60);
        final int slideWindowDuration = params.getInt("slideWindowDuration", 30);

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

        FlinkJobFactory<Tuple2<Timestamp, Long>, Tuple4<Timestamp, Long, String, Long>> creator =
                new FlinkJobFactory<>(args, false, true);
        StreamExecutionEnvironment env = creator.setupExecutionEnvironmentWithStateBackend(params);

        DataStream<GeoEvent> source = new GeoEventParallelSocketSource(hostnames, ports, sourceParallelism)
                .createSource(args, env);

        SingleOutputStreamOperator<ZipCodeGeoEvent> map = source.map(new ZipCodeMapper());

        SingleOutputStreamOperator<GeoWindow> apply = map
                .keyBy((KeySelector<ZipCodeGeoEvent, Integer>) value -> value.stateID)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(windowDuration), Time.seconds(slideWindowDuration)))
                .apply((WindowFunction<ZipCodeGeoEvent, GeoWindow, Integer, TimeWindow>) (key, window, input, out) -> {
                    long count = 0;
                    for (ZipCodeGeoEvent ignored : input) {
                        count += 1;
                    }
                    out.collect(new GeoWindow(count, key));
                });

        new GeoWindowSink(sinkParallelism, output_path).createSink(args, apply);

        env.execute(JOB_NAME);
    }
}