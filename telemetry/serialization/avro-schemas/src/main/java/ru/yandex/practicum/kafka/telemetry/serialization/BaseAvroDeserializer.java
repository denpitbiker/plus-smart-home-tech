package ru.yandex.practicum.kafka.telemetry.serialization;

import java.io.IOException;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class BaseAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    private final DecoderFactory decoderFactory;
    private final Schema schema;

    public BaseAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    public BaseAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        this.decoderFactory = Objects.requireNonNull(decoderFactory);
        this.schema = Objects.requireNonNull(schema);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            var decoder = decoderFactory.binaryDecoder(data, null);
            T record = new SpecificDatumReader<T>(schema).read(null, decoder);
            if (!decoder.isEnd()) {
                throw new IOException("Trailing bytes after Avro record");
            }
            return record;
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("Cannot deserialize " + schema.getFullName() + " from " + topic, e);
        }
    }
}
