package de.adrianbartnik.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class RabbitMQSource extends AbstractSource<String> {

    private static final String OPERATOR_NAME = "RabbitMQSource";

    @Override
    public DataStream<String> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        final ParameterTool params = ParameterTool.fromArgs(arguments);
        final String hostname = params.get("hostname", "localhost");
        final int port = params.getInt("port", 5672);
        final String queueName = params.get("queuename", "defaultQueue");
        final String username = params.get("username", "guest");
        final String password = params.get("password", "guest");

        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(hostname)
                .setPort(port)
                .setVirtualHost("/")
                .setUserName(username)
                .setPassword(password)
                .build();

        return executionEnvironment
                .addSource(new RMQSource<>(
                        connectionConfig,
                        queueName,
                        true,       // use correlation ids; can be false if only at-least-once is required
                        new SimpleStringSchema())) // deserialization schema to turn messages into Java objects
                .name(OPERATOR_NAME)
                .setParallelism(1);      // non-parallel source is only required for exactly-once
    }
}
