package ru.yandex.practicum.kafka.telemetry.event;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class SchemaContractTest {
    @Test void preservesAssignmentTemperatureFields() {
        assertEquals(List.of("id", "hubId", "timestamp", "temperature_c", "temperature_f"),
                TemperatureSensorAvro.getClassSchema().getFields().stream().map(Schema.Field::name).toList());
        assertEquals("timestamp-millis", SensorEventAvro.getClassSchema().getField("timestamp").schema().getLogicalType().getName());
        assertEquals(List.of(Schema.Type.NULL, Schema.Type.INT, Schema.Type.BOOLEAN),
                ScenarioConditionAvro.getClassSchema().getField("value").schema().getTypes().stream().map(Schema::getType).toList());
    }

    @Test void documentsRawBinaryIncompatibilityWithBundledRouter() throws Exception {
        Schema reference;
        try (var input = getClass().getResourceAsStream("/hub-router-sensor-schema.json")) {
            reference = new Schema.Parser().parse(input);
        }
        var payloadSchema = reference.getField("payload").schema().getTypes().get(4);
        assertEquals(List.of("temperature_c", "temperature_f"), payloadSchema.getFields().stream().map(Schema.Field::name).toList());
        var time = Instant.ofEpochMilli(1722957084157L);
        var record = new SensorEventAvro("sensor-1", "hub-1", time,
                new TemperatureSensorAvro("sensor-1", "hub-1", time, 21, 70));
        var bytes = new ByteArrayOutputStream();
        var encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        new SpecificDatumWriter<SensorEventAvro>(record.getSchema()).write(record, encoder); encoder.flush();
        var decoder = DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null);
        // Hub Router receives only bytes and assumes its own writer schema.
        var decoded = new GenericDatumReader<GenericRecord>(reference).read(null, decoder);
        var payload = (GenericRecord) decoded.get("payload");
        assertNotEquals(21, payload.get("temperature_c"));
        assertFalse(decoder.isEnd(), "Unexpected extra metadata cannot be decoded by the bundled schema");
    }
}
