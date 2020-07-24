package de.adrianbartnik.source.socket;

import de.adrianbartnik.benchmarks.yahoo.objects.Event;
import de.adrianbartnik.benchmarks.yahoo.objects.IndependentEvent;
import de.adrianbartnik.data.nexmark.AuctionEvent;
import de.adrianbartnik.source.AbstractSource;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamSource;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class IndependentYahooEventParallelSocketSource extends AbstractSource<IndependentEvent> implements Serializable {

    private static final String OPERATOR_NAME = "IndependentYahooEventParallelSocketSource";

    private final List<String> hostnames;
    private final List<Integer> ports;

    public IndependentYahooEventParallelSocketSource(List<String> hostnames, List<Integer> ports, int parallelism) {
        super(parallelism);
        this.hostnames = hostnames;
        this.ports = ports;
    }

    @Override
    public DataStream<IndependentEvent> createSource(String[] arguments, StreamExecutionEnvironment executionEnvironment) {

        TypeInformation<IndependentEvent> typeInformation = TypeInformation.of(new TypeHint<IndependentEvent>() {
        });

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
    public class AuctionSocketSourceFunction extends AbstractSocketSourceFunction<IndependentEvent> {

        AuctionSocketSourceFunction(List<String> hostnames, List<Integer> ports) {
            super(hostnames, ports);
        }

        @Override
        protected IndependentEvent stringToRecord(String record) {
            String[] split = record.split(",");
            return new IndependentEvent(
                    Long.valueOf(split[0]),
                    split[1],
                    split[2],
                    split[3],
                    split[4],
                    split[5],
                    new Timestamp(Long.valueOf(split[6])),
                    split[7]);
        }

        @Override
        protected String getStartCommand() {
            return getRuntimeContext().getIndexOfThisSubtask() + ":yahoo\n";
        }
    }
}
