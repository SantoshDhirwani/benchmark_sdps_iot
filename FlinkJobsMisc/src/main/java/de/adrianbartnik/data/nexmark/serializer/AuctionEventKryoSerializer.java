package de.adrianbartnik.data.nexmark.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.adrianbartnik.data.nexmark.AuctionEvent;

/**
 * We need another set of Kryo Serializers for the Benchmark Events for Flink-internal serialization. In Flink, we need to maintain the
 * ingestion timestamp (Time of entry into Flink), which we do not require on sending to Kafka.
 */
public class AuctionEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<AuctionEvent> {

    @Override
    public void write(Kryo kryo, Output output, AuctionEvent event) {
        output.writeLong(event.timestamp);
        output.writeLong(event.auctionId);
        output.writeLong(event.itemId);
        output.writeLong(event.personId);
        output.writeDouble(event.initialPrice);
        output.writeLong(event.start);
        output.writeLong(event.end);
        output.writeLong(event.categoryId);
        output.writeLong(event.ingestionTimestamp);
    }

    @Override
    public AuctionEvent read(Kryo kryo, Input input, Class<AuctionEvent> aClass) {
        Long timestamp = input.readLong();
        long auctionId = input.readLong();
        long itemId = input.readLong();
        long personId = input.readLong();
        Double initialPrice = input.readDouble();
        Long start = input.readLong();
        Long end = input.readLong();
        Long categoryId = input.readLong();
        Long ingestionTimestamp = input.readLong();

        return new AuctionEvent(timestamp, auctionId, itemId, personId, initialPrice, categoryId, start, end, ingestionTimestamp);
    }
}
