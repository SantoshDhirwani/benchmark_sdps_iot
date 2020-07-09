package de.adrianbartnik.job.stateless;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.TextAppender;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Simple job with one mapper, that appends text to all incoming tuples.
 */
public class AppendTextMapJob {

    private static final String JOB_NAME = "AppendTextMapJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String, String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new TextAppender(), new TextOutputSink<String>(1, "dummy"));

        job.execute(JOB_NAME);
    }
}
