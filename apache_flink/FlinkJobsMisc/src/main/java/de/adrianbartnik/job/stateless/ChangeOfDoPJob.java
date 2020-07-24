package de.adrianbartnik.job.stateless;

import de.adrianbartnik.factory.FlinkJobFactory;
import de.adrianbartnik.operator.FilterMapOperator;
import de.adrianbartnik.sink.TextOutputSink;
import de.adrianbartnik.source.RabbitMQSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Modified the SocketWindowWordCount example from the flink project.
 * <p>
 * <p>This program connects to a rabbitMQ instance and reads strings.
 */
public class ChangeOfDoPJob {

    private static final String JOB_NAME = "ChangeOfDoPJob";

    public static void main(String[] args) throws Exception {

        FlinkJobFactory<String, String> creator = new FlinkJobFactory<>(args, false, false);

        StreamExecutionEnvironment job =
                creator.createJob(new RabbitMQSource(), new FilterMapOperator(), new TextOutputSink<String>(1, "dummy"));

        job.execute(JOB_NAME);
    }
}
