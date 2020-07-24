package de.adrianbartnik.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSource extends AbstractSource<String> {

    private static final String OPERATOR_NAME = "SocketSource";

    @Override
    public DataStream<String> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {
        // the host and the port to connect to
        final ParameterTool params = ParameterTool.fromArgs(arguments);
        final String hostname = params.get("hostname", "localhost");
        final int port = params.getInt("port", 9000);
        final String delimiter = params.get("delimiter", "\n");

        return executionEnvironment.socketTextStream(hostname, port, delimiter).name(OPERATOR_NAME);
    }
}
