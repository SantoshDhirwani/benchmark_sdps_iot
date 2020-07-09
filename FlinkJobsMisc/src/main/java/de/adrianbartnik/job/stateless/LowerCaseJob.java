package de.adrianbartnik.job.stateless;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.LowercaseMapper;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LowerCaseJob {

    private static final String JOB_NAME = "LowerCaseMapJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String, String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new LowercaseMapper(), new TextOutputSink<String>(1, "dummy"));

        job.execute(JOB_NAME);
    }
}
