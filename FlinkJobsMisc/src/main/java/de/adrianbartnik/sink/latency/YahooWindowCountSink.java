package de.adrianbartnik.sink.latency;

import de.adrianbartnik.benchmarks.yahoo.objects.intermediate.WindowedCount;
import de.adrianbartnik.data.nexmark.intermediate.Query8WindowOutput;
import de.adrianbartnik.sink.AbstractSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class YahooWindowCountSink extends AbstractSink<WindowedCount> implements Serializable {

    private static final String OPERATOR_NAME = "YahooWindowCountSink";

    private static final long serialVersionUID = 1L;

    private final String path;

    public YahooWindowCountSink(String path) {
        super();
        this.path = path;
    }

    public YahooWindowCountSink(int parallelism, String path) {
        super(parallelism);
        this.path = path;
    }

    @Override
    public void createSink(String[] arguments, DataStream<WindowedCount> dataSource) {
        dataSource
                .writeUsingOutputFormat(new WindowLatencyOutputFormat(new Path(path)))
                .setParallelism(parallelism)
                .name(OPERATOR_NAME);
    }

    private class WindowLatencyOutputFormat extends AbstractOutputFormat<WindowedCount> {

        WindowLatencyOutputFormat(Path outputPath) {
            super(outputPath);
        }

        @Override
        StringBuilder getOutputString(WindowedCount count) {

            this.stringBuilder.append(count.campaignId);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(count.count);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(System.currentTimeMillis() - count.lastUpdate.getTime());
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(count.timeWindow.getTime());

            return this.stringBuilder;
        }
    }
}
