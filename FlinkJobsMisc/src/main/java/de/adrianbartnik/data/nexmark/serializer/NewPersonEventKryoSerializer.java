package de.adrianbartnik.data.nexmark.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.adrianbartnik.data.nexmark.NewPersonEvent;

/**
 * We need another set of Kryo Serializers for the Benchmark Events for Flink-internal serialization. In Flink, we need to maintain the
 * ingestion timestamp (Time of entry into Flink), which we do not require on sending to Kafka.
 */
public class NewPersonEventKryoSerializer extends com.esotericsoftware.kryo.Serializer<NewPersonEvent> {
    @Override
    public void write(Kryo kryo, Output output, NewPersonEvent event) {
        output.writeLong(event.timestamp);
        output.writeLong(event.personId);
        output.writeString(event.name);
        output.writeString(event.email);
        output.writeString(event.city);
        output.writeString(event.country);
        output.writeString(event.province);
        output.writeString(event.zipcode);
        output.writeString(event.homepage);
        output.writeString(event.creditcard);
        output.writeLong(event.ingestionTimestamp);
    }

    @Override
    public NewPersonEvent read(Kryo kryo, Input input, Class<NewPersonEvent> aClass) {
        long timestamp = input.readLong();
        long personId = input.readLong();
        String name = input.readString();
        String email = input.readString();
        String city = input.readString();
        String country = input.readString();
        String province = input.readString();
        String zipcode = input.readString();
        String homepage = input.readString();
        String creditcard = input.readString();
        long ingestionTimestamp = input.readLong();

        return new NewPersonEvent(timestamp, personId, name, email, city, country, province, zipcode, homepage, creditcard, ingestionTimestamp);
    }
}
