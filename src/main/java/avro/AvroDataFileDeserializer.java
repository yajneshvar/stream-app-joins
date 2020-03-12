package avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A deserializer that reads bytes that were written with
 * Avro's {@link org.apache.avro.file.DataFileWriter}.
 *
 * @param <T> the resulting type (must be a {@link SpecificRecord)}
 * @see AvroDataFileSerializer
 * @see AvroDataFileSerde
 */
public class AvroDataFileDeserializer<T extends SpecificRecord> implements Deserializer<T> {

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, byte[] data) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
            byte[] buffer = new byte[1024];
            int len;
            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
            byte[] bytes = out.toByteArray();
            SpecificDatumReader<SpecificRecord> datumReader = new SpecificDatumReader<>();
            try (DataFileReader<SpecificRecord> reader = new DataFileReader<>(new SeekableByteArrayInput(bytes), datumReader)) {
                return (T) reader.next();
            }
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
