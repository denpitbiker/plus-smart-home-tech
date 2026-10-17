package ru.yandex.practicum.aggregator.state;

import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.yandex.practicum.kafka.telemetry.event.ClimateSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.kafka.telemetry.event.TemperatureSensorAvro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotStoreTest {
    private static final Instant TIME = Instant.parse("2024-08-06T15:11:24.157Z");
    private final SnapshotStore store = new SnapshotStore();

    private static SensorEventAvro event(String hub, String sensor, Instant time, Object data) {
        var event = new SensorEventAvro();
        event.setId(sensor);
        event.setHubId(hub);
        if (time != null) {
            event.setTimestamp(time);
        }
        event.setPayload(data);
        return event;
    }

    private static Stream<Arguments> payloads() {
        return Stream.of(
                Arguments.of(new ClimateSensorAvro(0, 60, 400), new ClimateSensorAvro(1, 60, 400)),
                Arguments.of(new LightSensorAvro(75, 0), new LightSensorAvro(75, 10)),
                Arguments.of(new MotionSensorAvro(100, false, 220), new MotionSensorAvro(100, true, 220)),
                Arguments.of(new SwitchSensorAvro(false), new SwitchSensorAvro(true)),
                Arguments.of(new TemperatureSensorAvro("sensor", "hub", TIME, 0, 32),
                        new TemperatureSensorAvro("sensor", "hub", TIME, 1, 34)));
    }

    @ParameterizedTest
    @MethodSource("payloads")
    void createsAndUpdatesEveryPayload(Object initial, Object changed) {
        var first = store.updateState(event("hub", "sensor", TIME, initial)).orElseThrow();
        assertEquals("hub", first.getHubId());
        assertEquals(TIME, first.getTimestamp());
        assertEquals(1, first.getSensorsState().size());
        assertEquals(initial, first.getSensorsState().get("sensor").getData());
        assertEquals(TIME, first.getSensorsState().get("sensor").getTimestamp());

        assertTrue(store.updateState(event("hub", "sensor", TIME, initial)).isEmpty());
        assertTrue(store.updateState(event("hub", "sensor", TIME.minusMillis(1), changed)).isEmpty());
        // Equal timestamp with changed data must be accepted.
        var equalTime = store.updateState(event("hub", "sensor", TIME, changed)).orElseThrow();
        assertEquals(changed, equalTime.getSensorsState().get("sensor").getData());
        assertEquals(TIME, equalTime.getTimestamp());
        assertTrue(store.updateState(event("hub", "sensor", TIME.plusSeconds(2), changed)).isEmpty());
        // Ignoring unchanged data must not advance the stored sensor timestamp.
        var later = store.updateState(event("hub", "sensor", TIME.plusSeconds(1), initial)).orElseThrow();
        assertEquals(TIME.plusSeconds(1), later.getTimestamp());
        assertEquals(TIME.plusSeconds(1), later.getSensorsState().get("sensor").getTimestamp());
        assertEquals(initial, first.getSensorsState().get("sensor").getData());
    }

    @Test
    void isolatesHubsAndUsesIncomingTimeEvenWhenSnapshotTimeMovesBackwards() {
        store.updateState(event("hub-1", "same-id", TIME, new SwitchSensorAvro(false)));
        var otherHub = store.updateState(event("hub-2", "same-id", TIME, new SwitchSensorAvro(true))).orElseThrow();
        var result = store.updateState(event("hub-1", "light", TIME.minusSeconds(1), new LightSensorAvro(0, 0)))
                .orElseThrow();
        assertEquals(2, result.getSensorsState().size());
        assertEquals(TIME.minusSeconds(1), result.getTimestamp());
        assertEquals(TIME, result.getSensorsState().get("same-id").getTimestamp());
        assertEquals(new SwitchSensorAvro(false), result.getSensorsState().get("same-id").getData());
        assertEquals(1, otherHub.getSensorsState().size());
        assertEquals(new SwitchSensorAvro(true), otherHub.getSensorsState().get("same-id").getData());
    }

    @Test
    void copiesInputPayloadAndReturnedSnapshotsDeeply() {
        var input = event("hub", "sensor", TIME, new SwitchSensorAvro(false));
        var first = store.updateState(input).orElseThrow();
        ((SwitchSensorAvro) input.getPayload()).setState(true);
        ((SwitchSensorAvro) first.getSensorsState().get("sensor").getData()).setState(true);
        first.getSensorsState().clear();
        first.setTimestamp(TIME.plusSeconds(99));

        var second = store.updateState(event("hub", "light", TIME.plusSeconds(1), new LightSensorAvro(1, 2)))
                .orElseThrow();
        assertEquals(new SwitchSensorAvro(false), second.getSensorsState().get("sensor").getData());
        store.updateState(event("hub", "sensor", TIME.plusSeconds(2), new SwitchSensorAvro(true)));
        assertEquals(new SwitchSensorAvro(false), second.getSensorsState().get("sensor").getData());
        assertEquals(TIME.plusSeconds(1), second.getTimestamp());
    }

    @Test
    void comparesTemperatureMetadataAsPartOfPayload() {
        store.updateState(event("hub", "sensor", TIME, new TemperatureSensorAvro("sensor", "hub", TIME, 20, 68)));
        var updated = store.updateState(event("hub", "sensor", TIME.plusSeconds(1),
                new TemperatureSensorAvro("sensor", "hub", TIME.plusSeconds(1), 20, 68))).orElseThrow();
        assertEquals(TIME.plusSeconds(1), updated.getSensorsState().get("sensor").getTimestamp());
    }

    private static Stream<Arguments> invalidEvents() {
        return Stream.of(
                Arguments.of((Object) null),
                Arguments.of(event(null, "sensor", TIME, new SwitchSensorAvro(false))),
                Arguments.of(event("", "sensor", TIME, new SwitchSensorAvro(false))),
                Arguments.of(event(" \t", "sensor", TIME, new SwitchSensorAvro(false))),
                Arguments.of(event("hub", null, TIME, new SwitchSensorAvro(false))),
                Arguments.of(event("hub", "", TIME, new SwitchSensorAvro(false))),
                Arguments.of(event("hub", " \t", TIME, new SwitchSensorAvro(false))),
                Arguments.of(event("hub", "sensor", null, new SwitchSensorAvro(false))),
                Arguments.of(event("hub", "sensor", TIME, null)));
    }

    @ParameterizedTest
    @MethodSource("invalidEvents")
    void rejectsInvalidMetadataWithoutChangingState(SensorEventAvro invalid) {
        store.updateState(event("hub", "sensor", TIME, new SwitchSensorAvro(false)));
        assertThrows(IllegalArgumentException.class, () -> store.updateState(invalid));
        var snapshot = store.updateState(event("hub", "light", TIME, new LightSensorAvro(1, 2))).orElseThrow();
        assertEquals(2, snapshot.getSensorsState().size());
        assertEquals(new SwitchSensorAvro(false), snapshot.getSensorsState().get("sensor").getData());
    }
}
