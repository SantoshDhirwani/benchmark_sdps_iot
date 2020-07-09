package de.adrianbartnik.sink.latency;

import de.adrianbartnik.data.nexmark.intermediate.Query8WindowOutput;
import de.adrianbartnik.job.data.GeoWindow;
import de.adrianbartnik.sink.AbstractSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public class GeoWindowSink extends AbstractSink<GeoWindow> implements Serializable {

    private static final String OPERATOR_NAME = "GeoWindowSink";

    private static final long serialVersionUID = 1L;

    private final String path;

    public GeoWindowSink(int parallelism, String path) {
        super(parallelism);
        this.path = path;
    }

    @Override
    public void createSink(String[] arguments, DataStream<GeoWindow> dataSource) {
        dataSource
                .writeUsingOutputFormat(new WindowLatencyOutputFormat(new Path(path)))
                .setParallelism(parallelism)
                .name(OPERATOR_NAME);
    }

    /**
     * This is an OutputFormat to serialize {@link org.apache.flink.api.java.tuple.Tuple}s to text. The output is
     * structured by record delimiters and field delimiters as common in CSV files.
     * Record delimiter separate records from each other ('\n' is common). Field
     * delimiters separate fields within a record.
     */
    private class WindowLatencyOutputFormat extends AbstractOutputFormat<GeoWindow> {

        WindowLatencyOutputFormat(Path outputPath) {
            super(outputPath);
        }

        @Override
        StringBuilder getOutputString(GeoWindow window) {

            long timeMillis = System.currentTimeMillis();

            this.stringBuilder.append(timeMillis);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(window.state);
            this.stringBuilder.append(AbstractOutputFormat.FIELD_DELIMITER);
            this.stringBuilder.append(window.state);

            return this.stringBuilder;
        }
    }

}
