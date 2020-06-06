package de.adrianbartnik.job.stateful;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.CountingMap;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorStateJob {

    private static final String JOB_NAME = "OperatorStateJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String, String> creator = new FlinkJobFactory<>(args, false, true);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new CountingMap<>(), new TextOutputSink<String>(1, "dummy"));

        job.execute(JOB_NAME);
    }
}
