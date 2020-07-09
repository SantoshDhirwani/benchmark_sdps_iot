package de.adrianbartnik.source;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

public class IntervalSequenceSource extends AbstractSource<Long> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IntervalSequenceSource.class);

    private static final String OPERATOR_NAME = "StatefulIntervalSequenceSource";

    private final long start;
    private final long end;
    private final long pauseDuration;

    public IntervalSequenceSource(long start, long end) {
        this(start, end, 0);
    }

    public IntervalSequenceSource(long start, long end, long pauseDuration) {
        this.start = start;
        this.end = end;
        this.pauseDuration = pauseDuration;
    }

    public IntervalSequenceSource(long start, long end, long pauseDuration, int parallelism) {
        super(parallelism);
        this.start = start;
        this.end = end;
        this.pauseDuration = pauseDuration;
    }


    @Override
    public DataStream<Long> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {
        return new DataStreamSource<>(executionEnvironment,
                TypeInformation.of(Long.class),
                new StreamSource<>(new StatefulIntervalSequenceSource(start, end, pauseDuration)),
                true,
                OPERATOR_NAME)
                .setParallelism(parallelism);
    }

    /**
     * A stateful streaming source that emits each number from a given interval exactly once with a custom
     * pause between each number, possibly in parallel.
     * <p>
     * <p>For the source to be re-scalable, the first time the job is run, we precompute all the elements
     * that each of the tasks should emit and upon checkpointing, each element constitutes its own
     * partition. When rescaling, these partitions will be randomly re-assigned to the new tasks.
     * <p>
     * <p>This strategy guarantees that each element will be emitted exactly-once, but elements will not
     * necessarily be emitted in ascending order, even for the same tasks.
     */
    private class StatefulIntervalSequenceSource extends RichParallelSourceFunction<Long>
            implements CheckpointedFunction, StoppableFunction {

        private static final long serialVersionUID = 1L;

        private final long start;
        private final long end;
        private final long pauseDuration;

        private volatile boolean isRunning = true;

        private transient Deque<Long> valuesToEmit;

        private transient ListState<Long> checkpointedState;

        /**
         * Creates a source that emits all numbers from the given interval exactly once.
         *
         * @param start Start of the range of numbers to emit.
         * @param end   End of the range of numbers to emit.
         */
        public StatefulIntervalSequenceSource(long start, long end) {
            this(start, end, 0);
        }

        /**
         * Creates a source that emits all numbers from the given interval exactly once.
         *
         * @param start         Start of the range of numbers to emit.
         * @param end           End of the range of numbers to emit.
         * @param pauseDuration The amount to pause between each element in ms.
         */
        public StatefulIntervalSequenceSource(long start, long end, long pauseDuration) {
            Preconditions.checkArgument(end > start, "End must be larger than start");
            Preconditions.checkArgument(pauseDuration >= 0, "Pause duration must be larger or equal to 0");

            this.start = start;
            this.end = end;
            this.pauseDuration = pauseDuration;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            Preconditions.checkState(this.checkpointedState == null,
                    "The " + getClass().getSimpleName() + " has already been initialized.");

            this.checkpointedState = context.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>(
                            "stateful-sequence-source-state",
                            LongSerializer.INSTANCE
                    )
            );

            this.valuesToEmit = new ArrayDeque<>();
            if (context.isRestored()) {
                // upon restoring

                for (Long v : this.checkpointedState.get()) {
                    this.valuesToEmit.add(v);
                }
            } else {
                // the first time the job is executed

                final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
                final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
                final long congruence = start + taskIdx;

                long totalNoOfElements = Math.abs(end - start + 1);
                final int baseSize = safeDivide(totalNoOfElements, stepSize);
                final int toCollect = (totalNoOfElements % stepSize > taskIdx) ? baseSize + 1 : baseSize;

                for (long collected = 0; collected < toCollect; collected++) {
                    this.valuesToEmit.add(collected * stepSize + congruence);
                }
            }
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning && !this.valuesToEmit.isEmpty()) {
                synchronized (ctx.getCheckpointLock()) {

                    if (!isRunning) {
                        return;
                    }

                    Long record = this.valuesToEmit.poll();
                    ctx.collect(record);
                    LOG.debug("Source {} Emitting: {}", getRuntimeContext().getIndexOfThisSubtask(), record);
                }

                Thread.sleep(pauseDuration);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            Preconditions.checkState(this.checkpointedState != null,
                    "The " + getClass().getSimpleName() + " state has not been properly initialized.");

            this.checkpointedState.clear();
            for (Long v : this.valuesToEmit) {
                this.checkpointedState.add(v);
            }
        }

        @Override
        public void stop() {
            isRunning = false;
        }
    }

    private static int safeDivide(long left, long right) {
        Preconditions.checkArgument(right > 0);
        Preconditions.checkArgument(left >= 0);
        Preconditions.checkArgument(left <= Integer.MAX_VALUE * right);
        return (int) (left / right);
    }
}
