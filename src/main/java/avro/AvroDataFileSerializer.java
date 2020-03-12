package avro;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A serializer that uses Avro's {@link DataFileWriter} to transform an object into bytes.
 * <p>
 * The schema is serialized along with the value, thus a schema registry is not necessary.
 * <p>
 * This serialization method is only recommended when the schema size is small, otherwise
 * too much space can be wasted in the payload, and a schema registry option should be
 * considered.
 * <p>
 * This serializer also support custom codecs through the configuration parameter {@code "codec"}.
 *
 * @param <T> the data type (must be a {@link SpecificRecord)})
 * @see AvroDataFileDeserializer
 * @see AvroDataFileSerde
 */
public class AvroDataFileSerializer<T extends SpecificRecord> implements Serializer<T> {

    public static final String CODEC_CONFIG = "codec";
    private static final String DEFAULT_CODEC = DataFileConstants.NULL_CODEC;
    private CodecFactory codec;

    public AvroDataFileSerializer() {
        configure(null, false);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> serializerConfig = configs != null ? new HashMap<>(configs) : Collections.emptyMap();
        this.codec = CodecFactory.fromString((String) serializerConfig.getOrDefault(CODEC_CONFIG, DEFAULT_CODEC));
    }

    @Override
    public byte[] serialize(String topic, T data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
        SpecificDatumWriter<SpecificRecord> datumWriter = new SpecificDatumWriter<>();
        try (DataFileWriter<SpecificRecord> writer = new DataFileWriter<>(datumWriter)) {
            writer.setCodec(codec);
            writer.create(data.getSchema(), out);
            writer.append(data);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
        return out.toByteArray();
    }

}
