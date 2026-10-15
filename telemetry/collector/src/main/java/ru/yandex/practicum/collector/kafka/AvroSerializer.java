package ru.yandex.practicum.collector.kafka;
import java.io.ByteArrayOutputStream;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
public class AvroSerializer implements Serializer<SpecificRecord> {
    @Override
    public byte[] serialize(String topic, SpecificRecord data) {
        if (data == null) return null;
        try (var output = new ByteArrayOutputStream()) {
            var encoder = EncoderFactory.get().binaryEncoder(output, null);
            new SpecificDatumWriter<SpecificRecord>(data.getSchema()).write(data, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Cannot serialize " + data.getSchema().getFullName(), e);
        }
    }
}
