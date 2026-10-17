package ru.yandex.practicum.kafka.telemetry.serialization;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroDeserializerTest {
    private static final Instant TIME = Instant.parse("2024-08-06T15:11:24.157Z");
    private final AvroSerializer serializer = new AvroSerializer();
    private final SensorEventDeserializer events = new SensorEventDeserializer();
    private final SensorsSnapshotDeserializer snapshots = new SensorsSnapshotDeserializer();

    private List<SensorEventAvro> events() {
        return List.of(
                new SensorEventAvro("climate", "hub-1", TIME, new ClimateSensorAvro(-10, 60, 400)),
                new SensorEventAvro("light", "hub-1", TIME, new LightSensorAvro(75, 0)),
                new SensorEventAvro("motion", "hub-1", TIME, new MotionSensorAvro(100, false, 220)),
                new SensorEventAvro("switch", "hub-1", TIME, new SwitchSensorAvro(false)),
                new SensorEventAvro("temperature", "hub-1", TIME,
                        new TemperatureSensorAvro("temperature", "hub-1", TIME, -20, -4)));
    }

    private SensorsSnapshotAvro snapshot() {
        Map<String, SensorStateAvro> states = new LinkedHashMap<>();
        for (var event : events()) {
            states.put(event.getId(), new SensorStateAvro(event.getTimestamp(), event.getPayload()));
        }
        return new SensorsSnapshotAvro("hub-1", TIME.plusMillis(1), states);
    }

    @Test
    void roundTripsEveryPayloadInEventsAndSnapshots() {
        for (var event : events()) {
            assertEquals(event, events.deserialize("sensors", serializer.serialize("sensors", event)));
            var snapshot = new SensorsSnapshotAvro(event.getHubId(), TIME,
                    Map.of(event.getId(), new SensorStateAvro(TIME, event.getPayload())));
            assertEquals(snapshot, snapshots.deserialize("snapshots", serializer.serialize("snapshots", snapshot)));
        }
        var snapshot = snapshot();
        assertEquals(snapshot, snapshots.deserialize("snapshots", serializer.serialize("snapshots", snapshot)));
    }

    @Test
    void preservesSnapshotContract() {
        var schema = SensorsSnapshotAvro.getClassSchema();
        assertEquals("ru.yandex.practicum.kafka.telemetry.event", schema.getNamespace());
        assertEquals(List.of("hubId", "timestamp", "sensorsState"),
                schema.getFields().stream().map(Schema.Field::name).toList());
        assertEquals("timestamp-millis", schema.getField("timestamp").schema().getLogicalType().getName());
        assertEquals(Schema.Type.MAP, schema.getField("sensorsState").schema().getType());
        assertEquals(SensorStateAvro.getClassSchema(), schema.getField("sensorsState").schema().getValueType());
        var state = SensorStateAvro.getClassSchema();
        assertEquals(List.of("timestamp", "data"), state.getFields().stream().map(Schema.Field::name).toList());
        assertEquals("timestamp-millis", state.getField("timestamp").schema().getLogicalType().getName());
        assertEquals(List.of("ClimateSensorAvro", "LightSensorAvro", "MotionSensorAvro", "SwitchSensorAvro",
                        "TemperatureSensorAvro"),
                state.getField("data").schema().getTypes().stream().map(Schema::getName).toList());
    }

    @Test
    void supportsExplicitDecoderFactoryAndNullValues() {
        var decoder = new BaseAvroDeserializer<SensorEventAvro>(DecoderFactory.get(), SensorEventAvro.getClassSchema());
        var event = events().getFirst();
        assertEquals(event, decoder.deserialize("sensors", serializer.serialize("sensors", event)));
        assertNull(events.deserialize("sensors", null));
        assertNull(snapshots.deserialize("snapshots", null));
    }

    @Test
    void rejectsTruncatedAndTrailingData() {
        var eventBytes = serializer.serialize("sensors", events().getFirst());
        var snapshotBytes = serializer.serialize("snapshots", snapshot());
        for (int length = 0; length < eventBytes.length; length++) {
            byte[] truncated = Arrays.copyOf(eventBytes, length);
            assertThrows(SerializationException.class, () -> events.deserialize("sensors", truncated));
        }
        for (int length = 0; length < snapshotBytes.length; length++) {
            byte[] truncated = Arrays.copyOf(snapshotBytes, length);
            assertThrows(SerializationException.class, () -> snapshots.deserialize("snapshots", truncated));
        }
        assertThrows(SerializationException.class,
                () -> events.deserialize("sensors", Arrays.copyOf(eventBytes, eventBytes.length + 1)));
        assertThrows(SerializationException.class,
                () -> snapshots.deserialize("snapshots", Arrays.copyOf(snapshotBytes, snapshotBytes.length + 1)));
        // Empty IDs and epoch timestamp followed by an invalid union index (5).
        assertThrows(SerializationException.class, () -> events.deserialize("sensors", new byte[]{0, 0, 0, 10}));
    }

    @Test
    void deserializesConcurrentlyWithoutSharingMutableReaders() throws Exception {
        var expected = snapshot();
        byte[] bytes = serializer.serialize("snapshots", expected);
        try (var executor = Executors.newFixedThreadPool(4)) {
            List<Callable<Void>> tasks = IntStream.range(0, 100).mapToObj(i -> (Callable<Void>) () -> {
                var event = events().get(i % 5);
                assertEquals(event, events.deserialize("sensors", serializer.serialize("sensors", event)));
                assertEquals(expected, snapshots.deserialize("snapshots", bytes));
                return null;
            }).toList();
            for (var future : executor.invokeAll(tasks)) {
                future.get();
            }
        }
    }
}
