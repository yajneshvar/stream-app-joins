package avro;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * A serde that reads/writes using Avro's data file serialization.
 *
 * @param <T> the data type
 * @see AvroDataFileSerializer
 * @see AvroDataFileDeserializer
 */
public class AvroDataFileSerde<T extends SpecificRecord> implements Serde<T> {

    private Serde<T> inner = Serdes.serdeFrom(new AvroDataFileSerializer<>(), new AvroDataFileDeserializer<>());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void close() {
        inner.close();
    }

}
