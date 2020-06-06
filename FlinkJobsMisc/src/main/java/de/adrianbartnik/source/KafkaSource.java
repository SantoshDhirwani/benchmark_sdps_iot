package de.adrianbartnik.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaSource extends AbstractSource<String> {

    private static final String OPERATOR_NAME = "KafkaSource";

    public KafkaSource() {
        super();
    }

    public KafkaSource(int parallelism) {
        super(parallelism);
    }

    @Override
    public DataStream<String> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        final ParameterTool params = ParameterTool.fromArgs(arguments);
        final String topic = params.get("topic", "defaultTopic");
        final String servers = params.get("servers", "localhost:9092");
        final String groupid = params.get("groupid", "defaultGroupID");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
//        only required for Kafka 0.8
//        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", groupid);

        return executionEnvironment
                .addSource(new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), properties))
                .name(OPERATOR_NAME)
                .setParallelism(parallelism);
    }
}