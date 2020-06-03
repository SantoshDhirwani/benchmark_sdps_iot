package de.adrianbartnik.sink.latency;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.*;

/**
 * This is an OutputFormat to serialize {@link org.apache.flink.api.java.tuple.Tuple}s to text. The output is
 * structured by record delimiters and field delimiters as common in CSV files.
 * Record delimiter separate records from each other ('\n' is common). Field
 * delimiters separate fields within a record.
 */
public abstract class AbstractOutputFormat<T> extends FileOutputFormat<T> implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    private static final String RECORD_DELIMITER = CsvInputFormat.DEFAULT_LINE_DELIMITER;

    private static final String CHARSET = "UTF-8";

    static final String FIELD_DELIMITER = CsvInputFormat.DEFAULT_FIELD_DELIMITER;

    private final int onlyNthLatencyOutput;

    private long numberOfRecords = 0;

    private transient Writer wrt;

    final StringBuilder stringBuilder = new StringBuilder();

    /**
     * Creates an instance of AbstractOutputFormat.
     *
     * @param outputPath The path where the file is written.
     */
    AbstractOutputFormat(Path outputPath) {
        super(outputPath);
        onlyNthLatencyOutput = 1;
    }

    AbstractOutputFormat(Path outputPath, int onlyNthLatencyOutput) {
        super(outputPath);
        this.onlyNthLatencyOutput = onlyNthLatencyOutput;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        this.wrt = new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), CHARSET);
    }

    @Override
    public void close() throws IOException {
        if (wrt != null) {
            this.wrt.flush();
            this.wrt.close();
        }
        super.close();

        FileSystem fileSystem = getOutputFilePath().getFileSystem();

        //Path newDestination = new Path(actualFilePath.toUri().toString() + "-" + System.currentTimeMillis());
	Path newDestination = new Path(outputFilePath.toUri().toString() + "-" + System.currentTimeMillis());

        //boolean successfullyRenamedOutput = fileSystem.rename(actualFilePath, newDestination);
	boolean successfullyRenamedOutput = fileSystem.rename(outputFilePath, newDestination);

        if (!successfullyRenamedOutput) {
            throw new IllegalStateException("Failed to move output directory");
        }
    }

    @Override
    public void writeRecord(T element) throws IOException {

        numberOfRecords++;

        if (numberOfRecords % onlyNthLatencyOutput == 0) {
            stringBuilder.setLength(0);
            this.wrt.write(getOutputString(element).toString());
            this.wrt.write(RECORD_DELIMITER);
            this.wrt.flush();
        }
    }

    abstract StringBuilder getOutputString(T record);

    @Override
    public String toString() {
        return "AbstractOutputFormat (path: " + this.getOutputFilePath() + ", delimiter: " + FIELD_DELIMITER + ")";
    }

    /**
     * The purpose of this method is solely to check whether the data type to be processed
     * is in fact a tuple type.
     */
    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        // Ignore
    }
}

