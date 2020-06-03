package de.adrianbartnik.job.stateful;

import de.adrianbartnik.data.nexmark.AuctionEvent;
import de.adrianbartnik.data.nexmark.NewPersonEvent;
import de.adrianbartnik.data.nexmark.intermediate.Query8WindowOutput;
import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.job.parser.ParallelSocketArgumentParser;
import de.adrianbartnik.job.timestampextractor.AuctionEventTimestampExtractor;
import de.adrianbartnik.job.timestampextractor.PersonEventTimestampExtractor;
import de.adrianbartnik.operator.JoiningNewUsersWithAuctionsCoGroupFunction;
import de.adrianbartnik.sink.latency.Nexmark8Sink;
import de.adrianbartnik.source.socket.AuctionParallelSocketSource;
import de.adrianbartnik.source.socket.PersonParallelSocketSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class NexmarkQuery8 {

    private static final Logger LOG = LoggerFactory.getLogger(NexmarkQuery8.class);

    private static final String JOB_NAME = "NexmarkQuery8";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostnames_string = params.get("hostnames");
        final String ports_string = params.get("ports");
        final int windowParallelism = params.getInt("windowParallelism", 3);
        final int windowDuration = params.getInt("windowDuration", 30);
        final int sinkParallelism = params.getInt("sinkParallelism", 2);
        final String output_path = params.get("path", "query8Output");

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

        StreamExecutionEnvironment streamExecutionEnvironment =
                new FlinkJobFactory(args, true, true).setupExecutionEnvironmentWithStateBackend(params);

        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        AuctionParallelSocketSource auctionSource = new AuctionParallelSocketSource(hostnames, ports, sourceParallelism);

        PersonParallelSocketSource personSource = new PersonParallelSocketSource(hostnames, ports, sourceParallelism);

        SingleOutputStreamOperator<Query8WindowOutput> job =
                personSource.createSource(args, streamExecutionEnvironment)
                        .assignTimestampsAndWatermarks(new PersonEventTimestampExtractor())
                        .coGroup(auctionSource.createSource(args, streamExecutionEnvironment)
                                .assignTimestampsAndWatermarks(new AuctionEventTimestampExtractor()))
                        .where(NewPersonEvent::getPersonId).equalTo(AuctionEvent::getPersonId)
                        .window(TumblingEventTimeWindows.of(Time.seconds(windowDuration)))
                        .with(new JoiningNewUsersWithAuctionsCoGroupFunction())
                        .name("WindowOperator")
                        .setParallelism(windowParallelism);

        new Nexmark8Sink(sinkParallelism, output_path).createSink(args, job);

        streamExecutionEnvironment.execute(JOB_NAME);
    }
}
