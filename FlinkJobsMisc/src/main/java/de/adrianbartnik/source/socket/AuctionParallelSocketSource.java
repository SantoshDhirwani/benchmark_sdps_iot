package de.adrianbartnik.source.socket;

import de.adrianbartnik.data.nexmark.AuctionEvent;
import de.adrianbartnik.source.AbstractSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.io.Serializable;
import java.util.List;

public class AuctionParallelSocketSource extends AbstractSource<AuctionEvent> implements Serializable {

    private static final String OPERATOR_NAME = "AuctionParallelSocketSource";

    private final List<String> hostnames;
    private final List<Integer> ports;

    public AuctionParallelSocketSource(List<String> hostnames, List<Integer> ports, int parallelism) {
        super(parallelism);
        this.hostnames = hostnames;
        this.ports = ports;
    }

    @Override
    public DataStream<AuctionEvent> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        TypeInformation<AuctionEvent> typeInformation = TypeInformation.of(new TypeHint<AuctionEvent>() {});

        AuctionSocketSourceFunction function = new AuctionSocketSourceFunction(hostnames, ports);

        return new DataStreamSource<>(executionEnvironment,
                typeInformation,
                new StreamSource<>(function),
                true,
                OPERATOR_NAME)
                .setParallelism(parallelism);
    }

    /**
     * Schema: timestamp,auction_id,item_id,seller_id,category_id,quantity,type,start,end
     */
    public class AuctionSocketSourceFunction extends AbstractSocketSourceFunction<AuctionEvent> {

        AuctionSocketSourceFunction(List<String> hostnames, List<Integer> ports) {
            super(hostnames, ports);
        }

        @Override
        protected AuctionEvent stringToRecord(String record) {
            String[] split = record.split(",");
            return new AuctionEvent(
                    Long.valueOf(split[0]),
                    Long.valueOf(split[1]),
                    Long.valueOf(split[2]),
                    Long.valueOf(split[3]),
                    Double.valueOf(split[4]),
                    Long.valueOf(split[5]),
                    Long.valueOf(split[6]),
                    Long.valueOf(split[7]));
        }

        /**
         * Add 1000 to start command in order to not interfere with {@link PersonParallelSocketSource}.
         * Currently, the generator assigns the type of record type to the index of the source function.
         * Since both sources request the same index simultaneously, this approach will fail.
         *
         * @return The start command
         */
        @Override
        protected String getStartCommand() {
            return (getRuntimeContext().getIndexOfThisSubtask() + 1000) + ":auctions\n";
        }
    }
}
